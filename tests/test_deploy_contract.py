from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.deploy_contract import (
    DeployContract,
    evaluate_deployment_outcome,
    resolve_deploy_contract,
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
        import subprocess
        import sys
        from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
