from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts.deploy_contract import (
    DeployContract,
    classify_changed_paths,
    evaluate_deployment_outcome,
    parse_name_status_z,
    resolve_deploy_contract,
    resolve_push_deploy_trigger,
    write_success_marker,
)


class DeployContractTests(unittest.TestCase):
    def test_push_without_explicit_mode_is_rejected_before_deploy(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "push deploy requires a Deploy-Mode trailer"
        ):
            resolve_deploy_contract(event_name="push")

    def test_normal_push_requires_explicit_retention_trailer(self) -> None:
        contract = resolve_deploy_contract(
            event_name="push",
            commit_message=(
                "deploy application\n\n"
                "Deploy-Mode: normal\n"
                "Retention-Enabled: true\n"
            ),
        )
        self.assertEqual(contract.mode, "normal")
        self.assertTrue(contract.retention_enabled)

    def test_diagnostic_push_hard_disables_retention(self) -> None:
        contract = resolve_deploy_contract(
            event_name="push",
            commit_message="Deploy-Mode: diagnostic\nRetention-Enabled: false\n",
        )
        self.assertEqual(contract.mode, "diagnostic")
        self.assertFalse(contract.retention_enabled)

    def test_diagnostic_push_rejects_retention(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "diagnostic deploy cannot enable retention"
        ):
            resolve_deploy_contract(
                event_name="push",
                commit_message=(
                    "Deploy-Mode: diagnostic\nRetention-Enabled: true\n"
                ),
            )

    def test_manual_dispatch_uses_explicit_inputs(self) -> None:
        contract = resolve_deploy_contract(
            event_name="workflow_dispatch",
            input_mode="diagnostic",
            input_retention_enabled="false",
        )
        self.assertEqual(contract.mode, "diagnostic")
        self.assertFalse(contract.retention_enabled)

    def test_cli_outputs_only_valid_github_step_outputs(self) -> None:
        script = Path(__file__).parents[1] / "scripts" / "deploy_contract.py"
        result = subprocess.run(
            [
                sys.executable,
                str(script),
                "--event-name",
                "push",
            ],
            input="Deploy-Mode: diagnostic\nRetention-Enabled: false\n",
            text=True,
            capture_output=True,
            check=True,
        )
        self.assertEqual(
            result.stdout.splitlines(),
            ["deploy_mode=diagnostic", "retention_enabled=false"],
        )
        self.assertEqual(result.stderr, "")

    def test_invalid_or_duplicate_trailer_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid deploy mode"):
            resolve_deploy_contract(
                event_name="push",
                commit_message="Deploy-Mode: unsafe\n",
            )
        with self.assertRaisesRegex(ValueError, "duplicate Deploy-Mode"):
            resolve_deploy_contract(
                event_name="push",
                commit_message=(
                    "Deploy-Mode: normal\nDeploy-Mode: diagnostic\n"
                ),
            )

    def test_mode_health_outcome_matrix(self) -> None:
        cases = (
            ("normal", True, True, True, True),
            ("normal", True, False, False, False),
            ("diagnostic", False, True, True, False),
            ("diagnostic", False, False, False, False),
        )
        for mode, retention, health, succeeded, retention_runs in cases:
            with self.subTest(mode=mode, health=health):
                outcome = evaluate_deployment_outcome(
                    DeployContract(mode, retention), health_passed=health
                )
                self.assertEqual(outcome.succeeded, succeeded)
                self.assertEqual(outcome.marker_advances, succeeded)
                self.assertEqual(outcome.retention_runs, retention_runs)
                self.assertFalse(outcome.automatic_rollback)

    def test_failed_deploy_preserves_last_successful_marker(self) -> None:
        with TemporaryDirectory() as directory:
            marker = Path(directory) / ".deployed-sha"
            marker.write_bytes(b"last-successful\n")
            outcome = evaluate_deployment_outcome(
                DeployContract("diagnostic", False), health_passed=False
            )
            if outcome.marker_advances:
                write_success_marker(marker, "failed-source")
            self.assertEqual(marker.read_bytes(), b"last-successful\n")
            self.assertFalse((Path(directory) / ".deployed-sha.tmp").exists())

    def test_success_marker_replaces_atomically(self) -> None:
        with TemporaryDirectory() as directory:
            marker = Path(directory) / ".deployed-sha"
            marker.write_text("old\n", encoding="utf-8")
            write_success_marker(marker, "new-success")
            self.assertEqual(marker.read_text(encoding="utf-8"), "new-success\n")
            self.assertFalse((Path(directory) / ".deployed-sha.tmp").exists())


class DeployTriggerContractTests(unittest.TestCase):
    DIAGNOSTICS_PATHS = (
        "src/executor.py",
        "src/helius_rpc.py",
        "src/monitor.py",
        "src/observation_analysis.py",
        "src/observation_tracker.py",
        "src/phase_memory_telemetry.py",
        "src/research/alpha_discovery.py",
        "src/research/coverage_telemetry.py",
        "src/research/future_validation.py",
        "src/research/memory_workload_diagnostic.py",
        "src/research/monitor_memory_profile.py",
        "src/research/paper_mvp_review.py",
        "src/research_archive.py",
        "src/risk_manager.py",
        "src/shadow_trade_ledger.py",
        "src/solana_rpc.py",
        "src/state_lock_diagnostics.py",
        "src/state_store.py",
        "src/wallet_performance.py",
        "tests/test_shadow_trade_ledger.py",
        "tests/test_state_lock_diagnostics.py",
    )

    def assert_deploy(self, expected: bool, *paths: str) -> None:
        decision = classify_changed_paths(tuple(paths))
        self.assertEqual(decision.deploy, expected)

    def test_non_runtime_only_matrix_skips_deploy(self) -> None:
        cases = (
            ("tests/test_observation_mode.py",),
            ("docs/foo.md",),
            ("README.md",),
            (".github/workflows/deploy.yml",),
            ("scripts/deploy_contract.py",),
        )
        for paths in cases:
            with self.subTest(paths=paths):
                self.assert_deploy(False, *paths)

    def test_runtime_mixed_diagnostics_and_unknown_paths_deploy(self) -> None:
        cases = (
            ("src/foo.py",),
            ("src/foo.py", "tests/test_foo.py"),
            ("unknown/new-path.txt",),
            ("unknown/new-path.txt", "tests/test_foo.py"),
        )
        for paths in cases:
            with self.subTest(paths=paths):
                self.assert_deploy(True, *paths)
        self.assertEqual(len(self.DIAGNOSTICS_PATHS), 21)
        self.assert_deploy(True, *self.DIAGNOSTICS_PATHS)

    def test_baseline_fix_shape_skips_deploy(self) -> None:
        self.assert_deploy(False, "tests/test_observation_mode.py")

    def test_large_diff_with_runtime_path_last_still_deploys(self) -> None:
        paths = tuple(
            f"tests/test_generated_{index}.py" for index in range(3_001)
        )
        self.assert_deploy(True, *paths, "src/runtime_last.py")

    def test_rename_and_delete_include_runtime_paths(self) -> None:
        rename = parse_name_status_z(
            b"R100\0src/old.py\0tests/test_old.py\0"
        )
        deleted = parse_name_status_z(b"D\0src/deleted.py\0")
        self.assertEqual(rename, ("src/old.py", "tests/test_old.py"))
        self.assert_deploy(True, *rename)
        self.assert_deploy(True, *deleted)

    def test_empty_diff_is_non_deploy(self) -> None:
        self.assert_deploy(False)

    def test_unresolved_or_malformed_diff_fails_safe_to_deploy(self) -> None:
        invalid = resolve_push_deploy_trigger(
            before_sha="0" * 40,
            after_sha="1" * 40,
        )
        self.assertTrue(invalid.deploy)
        self.assertEqual(invalid.reason, "resolver_unknown")

        completed = subprocess.CompletedProcess(
            args=["git", "diff"],
            returncode=0,
            stdout=b"Q\0unknown\0",
        )
        with patch(
            "scripts.deploy_contract.subprocess.run", return_value=completed
        ):
            malformed = resolve_push_deploy_trigger(
                before_sha="1" * 40,
                after_sha="2" * 40,
            )
        self.assertTrue(malformed.deploy)
        self.assertEqual(malformed.reason, "resolver_unknown")

    def test_git_diff_failure_fails_safe_to_deploy(self) -> None:
        completed = subprocess.CompletedProcess(
            args=["git", "diff"],
            returncode=128,
            stdout=b"",
        )
        with patch(
            "scripts.deploy_contract.subprocess.run", return_value=completed
        ):
            decision = resolve_push_deploy_trigger(
                before_sha="1" * 40,
                after_sha="2" * 40,
            )
        self.assertTrue(decision.deploy)
        self.assertEqual(decision.reason, "resolver_unknown")

    def test_classify_cli_outputs_fixed_github_step_outputs(self) -> None:
        script = Path(__file__).parents[1] / "scripts" / "deploy_contract.py"
        result = subprocess.run(
            [
                sys.executable,
                str(script),
                "--classify-push",
                "--before-sha",
                "0" * 40,
                "--after-sha",
                "1" * 40,
            ],
            text=True,
            capture_output=True,
            check=True,
        )
        self.assertEqual(
            result.stdout.splitlines(),
            ["deploy=true", "trigger_reason=resolver_unknown"],
        )
        self.assertEqual(result.stderr, "")

if __name__ == "__main__":
    unittest.main()
