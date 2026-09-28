from __future__ import annotations

import ast
import re
import textwrap
import unittest
from pathlib import Path

from scripts.observer_health_gate import (
    MAX_ADDITIONAL_WAIT_SECONDS,
    MAX_HEARTBEAT_AGE_SECONDS,
    POLL_INTERVAL_SECONDS,
    ObserverHealthGateError,
    wait_for_observer_health,
)


ROOT = Path(__file__).resolve().parents[1]


class DeployWorkflowTests(unittest.TestCase):
    def test_normal_deploy_does_not_wait_for_market_traffic(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("sleep 360", workflow)
        self.assertIn("RPC_CONFIG providers=", workflow)
        self.assertNotIn('workload="deployment_smoke"', workflow)
        self.assertIn("src.research.future_validation", workflow)
        self.assertIn("--registry-mode prospective-five", workflow)
        self.assertIn("FUTURE_H1_H5", workflow)
        self.assertIn("PAPER_LEDGER", workflow)
        self.assertNotIn("src.research.alpha_discovery", workflow)

    def test_monitor_memory_ceiling_remains_unchanged(self) -> None:
        ecosystem = (ROOT / "ecosystem.config.js").read_text(encoding="utf-8")
        monitor_block = ecosystem.split('name: "aibot-monitor"', 1)[1].split(
            "},", 1
        )[0]
        self.assertIn('max_memory_restart: "260M"', monitor_block)

    def test_observer_gate_precedes_deployed_sha_marker(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        gate = "metrics = wait_for_observer_health(read_observer_metrics)"
        marker = "--success-marker .deployed-sha"
        self.assertIn(gate, workflow)
        self.assertLess(workflow.index(gate), workflow.index(marker))

    def test_deploy_ownership_avoids_live_tree_recursion(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        self.assertNotRegex(
            workflow,
            re.compile(
                r"^\s*sudo chown -R deploy:deploy /var/www/aibot\s*$",
                re.MULTILINE,
            ),
        )
        self.assertNotIn("find /var/www/aibot -xdev", workflow)
        self.assertEqual(
            workflow.count("for static_path in src scripts venv; do"), 2
        )
        self.assertEqual(
            workflow.count(
                "for static_file in requirements.txt ecosystem.config.js .env; do",
            ),
            2,
        )
        self.assertEqual(workflow.count("for runtime_dir in data logs; do"), 2)
        self.assertEqual(
            workflow.count(
                'sudo chown deploy:deploy "/var/www/aibot/$runtime_dir"'
            ),
            2,
        )
        self.assertNotIn(
            "sudo chown -R deploy:deploy \"/var/www/aibot/$runtime_dir\"",
            workflow,
        )

    def test_static_and_runtime_root_ownership_verification_remains(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("find src scripts venv -xdev", workflow)
        self.assertIn("-path '*/__pycache__' -prune", workflow)
        for path in (
            "/var/www/aibot/requirements.txt",
            "/var/www/aibot/ecosystem.config.js",
            "/var/www/aibot/.env",
            "/var/www/aibot/data",
            "/var/www/aibot/logs",
        ):
            self.assertIn(path, workflow)
        self.assertIn("stat -c '%U:%G'", workflow)

    def test_backup_and_job_timeouts_cover_bounded_observer_startup(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        backup = workflow.split(
            "- name: Back up ledgers and prepare deploy ownership", 1
        )[1].split("- name: Upload application source", 1)[0]
        self.assertIn("command_timeout: 10m", backup)
        install = workflow.split(
            "- name: Install, validate, and reload PM2 services", 1
        )[1].split("- name: Post Check out main", 1)[0]
        self.assertIn("command_timeout: 20m", install)
        self.assertIn("timeout-minutes: 30", workflow)

    def test_backup_integrity_semantics_remain(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        backup = workflow.split(
            "- name: Back up ledgers and prepare deploy ownership", 1
        )[1].split("- name: Upload application source", 1)[0]
        for invariant in (
            'fcntl.flock(lock.fileno(), fcntl.LOCK_EX)',
            'output.flush()',
            'os.fsync(output.fileno())',
            'archive_files = sorted(archive_source.glob("*/*.json"))',
            'manifest["rpc_provider_states"] = {}',
            'os.replace(temporary, manifest_path)',
            '"hypothesis_registry.json"',
            '"future_validation.json"',
            '"future_validation_manifest.json"',
        ):
            self.assertIn(invariant, backup)
        self.assertIn('RESEARCH_STATE_RESTORED name={name}', workflow)

    def test_deploy_contract_change_does_not_trigger_production(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn('paths-ignore:', workflow)
        self.assertIn('- ".github/workflows/deploy.yml"', workflow)
        self.assertIn('- "scripts/deploy_contract.py"', workflow)
        self.assertIn('- "tests/test_deploy_contract.py"', workflow)
        self.assertIn('- "tests/test_deploy_workflows.py"', workflow)
        self.assertIn('- "tests/test_pm2_topology_check.py"', workflow)
        self.assertIn("github.ref == 'refs/heads/main'", workflow)

    def test_changed_path_classifier_is_unprivileged_and_gates_deploy(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        classifier = workflow.split("  classify:\n", 1)[1].split(
            "\n  deploy:\n", 1
        )[0]
        deploy = workflow.split("\n  deploy:\n", 1)[1]

        self.assertIn("fetch-depth: 0", classifier)
        self.assertIn("github.event.before", classifier)
        self.assertIn("--classify-push", classifier)
        self.assertNotIn("environment:", classifier)
        self.assertNotIn("secrets.", classifier)
        self.assertNotIn("appleboy/", classifier)

        self.assertIn("needs: classify", deploy)
        self.assertIn("always()", deploy)
        self.assertIn("needs.classify.result != 'success'", deploy)
        self.assertIn("needs.classify.outputs.deploy == 'true'", deploy)
        self.assertIn("github.event_name == 'workflow_dispatch'", deploy)
        self.assertIn("environment: production", deploy)

    def test_production_upload_excludes_known_non_runtime_paths(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        upload = workflow.split("- name: Upload application source", 1)[1].split(
            "- name: Install, validate, and reload PM2 services", 1
        )[0]
        self.assertIn(
            'source: "src,requirements.txt,ecosystem.config.js,scripts"',
            upload,
        )
        self.assertNotIn("tests", upload)
        self.assertNotIn("docs", upload)

    def test_normal_deploy_requires_explicit_retention_intent(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        resolver = (ROOT / "scripts" / "deploy_contract.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("default: false", workflow)
        self.assertIn("python scripts/deploy_contract.py", workflow)
        self.assertIn('or "false"', resolver)
        self.assertIn(
            "push deploy requires a Deploy-Mode trailer", resolver
        )
        self.assertIn('"$DEPLOY_MODE" == "normal" && \\', workflow)
        self.assertIn('"$RETENTION_ENABLED" == "true"', workflow)
        self.assertEqual(workflow.count("scripts/storage_retention.py"), 1)

    def test_health_failure_cannot_reach_retention_or_marker(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        gate = "metrics = wait_for_observer_health(read_observer_metrics)"
        retention = "scripts/storage_retention.py"
        marker = "--success-marker .deployed-sha"
        self.assertIn("set -Eeuo pipefail", workflow)
        self.assertLess(workflow.index(gate), workflow.index(retention))
        self.assertLess(workflow.index(gate), workflow.index(marker))
        self.assertLess(workflow.index("JUPITER_POSITION_HEALTH=OK"), workflow.index(retention))

    def test_diagnostic_mode_hard_disables_retention(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        resolver = (ROOT / "scripts" / "deploy_contract.py").read_text(
            encoding="utf-8"
        )
        self.assertIn('- diagnostic', workflow)
        self.assertIn('diagnostic deploy cannot enable retention', resolver)
        self.assertIn('RETENTION_SKIPPED mode=$DEPLOY_MODE', workflow)
        retention_block = workflow.split(
            'if [[ "$DEPLOY_MODE" == "normal" &&', 1
        )[1].split("fi", 1)[0]
        self.assertNotIn(
            '"$DEPLOY_MODE" == "diagnostic"', retention_block
        )

    def test_diagnostic_mode_has_no_production_tmp_cleanup(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("/tmp/aibot-pm2-before.json", workflow)
        self.assertNotIn("/tmp/aibot-pm2-after.json", workflow)
        self.assertNotRegex(workflow, re.compile(r"(?m)^\s*rm\s"))
        self.assertNotRegex(workflow, re.compile(r"find\b[^\n]*-delete"))
        self.assertNotIn("shutil.rmtree", workflow)
        self.assertNotRegex(workflow, re.compile(r"\.unlink\s*\("))
        self.assertIn('DIAGNOSTIC_SKIP maintenance=research_archive', workflow)
        self.assertIn('DIAGNOSTIC_SKIP maintenance=research_state_restore', workflow)
        self.assertIn('DIAGNOSTIC_SKIP maintenance=future_validation', workflow)
        self.assertIn('DIAGNOSTIC_SKIP maintenance=nginx_reconfigure', workflow)
        self.assertIn('DIAGNOSTIC_SKIP maintenance=pm2_reset', workflow)
        self.assertIn('DIAGNOSTIC_SKIP maintenance=pm2_save', workflow)
        normal_maintenance = workflow.split(
            'if [[ "$DEPLOY_MODE" == "normal" ]]; then', 4
        )[4].split("else", 1)[0]
        self.assertIn("pm2 save", normal_maintenance)

    def test_projected_disk_gate_is_fail_closed_before_backup(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        disk_gate = 'if projected_percent >= 90.0:'
        backup = 'backup_dir="$backup_root/predeploy-'
        upload = '- name: Upload application source'
        reload = 'pm2 startOrReload ecosystem.config.js --update-env'
        self.assertIn('projected_available < 512 * 1024 * 1024', workflow)
        self.assertIn('metadata.st_blocks * 512', workflow)
        self.assertIn('PREDEPLOY_DISK_GATE', workflow)
        self.assertLess(workflow.index(disk_gate), workflow.index(backup))
        self.assertLess(workflow.index(disk_gate), workflow.index(upload))
        self.assertLess(workflow.index(disk_gate), workflow.index(reload))

    def test_projected_disk_gate_enforces_boundaries(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        lines = workflow.splitlines()
        start = next(
            index for index, line in enumerate(lines) if "python3 - <<'PY'" in line
        )
        end = next(
            index
            for index in range(start + 1, len(lines))
            if lines[index].strip() == "PY"
        )
        module = ast.parse(textwrap.dedent("\n".join(lines[start + 1 : end])))
        projection = next(
            node
            for node in module.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "require_safe_projection"
        )
        namespace: dict[str, object] = {}
        exec(compile(ast.Module([projection], []), "disk-gate", "exec"), namespace)
        require_safe_projection = namespace["require_safe_projection"]

        gib = 1024**3
        self.assertEqual(require_safe_projection(7 * gib, 3 * gib, 0)[1], 70.0)
        with self.assertRaisesRegex(SystemExit, "90% safety gate"):
            require_safe_projection(8 * gib, 2 * gib, 1 * gib)
        with self.assertRaisesRegex(SystemExit, "90% safety gate"):
            require_safe_projection(8 * gib, 2 * gib, 2 * gib)
        with self.assertRaisesRegex(SystemExit, "below 512MiB"):
            require_safe_projection(1 * gib, 600 * 1024**2, 100 * 1024**2)
        with self.assertRaisesRegex(SystemExit, "no usable bytes"):
            require_safe_projection(0, 0, 0)

    def test_success_marker_is_atomic_last_and_preserves_failure_marker(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        marker = "--success-marker .deployed-sha"
        self.assertEqual(workflow.count(marker), 1)
        self.assertLess(workflow.index("JUPITER_POSITION_HEALTH=OK"), workflow.index(marker))
        self.assertLess(workflow.index("scripts/storage_retention.py"), workflow.index(marker))
        self.assertIn("NO_AUTOMATIC_ROLLBACK=true", workflow)
        self.assertIn("LAST_SUCCESSFUL_DEPLOY_SHA=", workflow)
        tail = workflow.split(marker, 1)[1]
        self.assertNotIn("exit 1", tail)
        self.assertNotIn("scripts/storage_retention.py", tail)

    def test_diagnostic_mode_keeps_one_backup_reload_and_health_gate(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        self.assertEqual(workflow.count('backup_dir="$backup_root/predeploy-'), 1)
        self.assertEqual(
            workflow.count("pm2 startOrReload ecosystem.config.js --update-env"),
            1,
        )
        self.assertEqual(
            workflow.count("metrics = wait_for_observer_health(read_observer_metrics)"),
            1,
        )
        self.assertIn('automatic_rollback=false', workflow)

    def test_embedded_python_blocks_compile(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        lines = workflow.splitlines()
        blocks: list[str] = []
        index = 0
        while index < len(lines):
            if "<<'PY'" not in lines[index]:
                index += 1
                continue
            block: list[str] = []
            index += 1
            while index < len(lines) and lines[index].strip() != "PY":
                block.append(lines[index])
                index += 1
            self.assertLess(index, len(lines), "unterminated Python heredoc")
            blocks.append(textwrap.dedent("\n".join(block)))
            index += 1
        self.assertGreater(len(blocks), 5)
        for sequence, block in enumerate(blocks):
            compile(block, f"deploy.yml:python-heredoc-{sequence}", "exec")

    def test_extended_observation_is_manual_and_bounded(self) -> None:
        workflow = (
            ROOT / ".github" / "workflows" / "research-observation.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("workflow_dispatch:", workflow)
        self.assertIn("github.ref == 'refs/heads/main'", workflow)
        self.assertIn("OBSERVATION_SECONDS < 60", workflow)
        self.assertIn("OBSERVATION_SECONDS > 1800", workflow)
        self.assertIn("--write-baseline", workflow)
        self.assertIn("--baseline", workflow)
        self.assertIn("RESEARCH_REPORT_SCOPE=cumulative", workflow)
        self.assertIn("RESEARCH_REPORT_SCOPE=observation_window", workflow)
        self.assertIn("ALPHA_SMART_MONEY_SOURCES", workflow)
        self.assertIn("src.research.monitor_memory_profile", workflow)
        self.assertIn("src.research.memory_workload_diagnostic", workflow)
        self.assertIn("src.research.alpha_review", workflow)
        self.assertIn("src.research.future_validation", workflow)
        self.assertIn("--registry-mode prospective-five", workflow)
        self.assertIn("python -m src.observation_analysis", workflow)
        self.assertNotIn('sleep "$OBSERVATION_SECONDS"', workflow)
        self.assertIn("src.research.restart_forensics", workflow)
        self.assertIn("MONITOR_RESTART_FORENSICS", (
            ROOT / "src" / "research" / "restart_forensics.py"
        ).read_text(encoding="utf-8"))
        self.assertLess(
            workflow.index("src.research.restart_forensics"),
            workflow.index('exit "$pm2_health_status"'),
        )


class FakeClock:
    def __init__(self, epoch: float = 1_000.0) -> None:
        self.epoch = epoch
        self.elapsed = 0.0
        self.sleeps: list[float] = []
        self.read_count = 0

    def now(self) -> float:
        return self.epoch + self.elapsed

    def monotonic(self) -> float:
        return self.elapsed

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.elapsed += seconds


class ObserverHealthGateTests(unittest.TestCase):
    @staticmethod
    def metrics(
        state: str,
        *,
        heartbeat: float = 1_000.0,
        health_state: str | None = None,
        maintenance_completed_at: float | None = 995.0,
    ) -> dict:
        metrics = {
            "observer_state": state,
            "observer_heartbeat_at": heartbeat,
            "observer_started_at": 990.0,
            "observer_last_error_type": "StateLockTimeout",
            "observer_last_error_at": 980.0,
        }
        if state == "RUNNING" and health_state is None:
            health_state = "RUNNING_HEALTHY"
        if health_state is not None:
            metrics["observer_health_state"] = health_state
        if maintenance_completed_at is not None:
            metrics["observer_archive_maintenance_completed_at"] = (
                maintenance_completed_at
            )
        return metrics

    def run_gate(self, states: list[dict], clock: FakeClock, **kwargs):
        remaining = list(states)
        latest = remaining[-1]

        def read_metrics() -> dict:
            nonlocal latest
            clock.read_count += 1
            if remaining:
                latest = remaining.pop(0)
            return latest

        return wait_for_observer_health(
            read_metrics,
            now=clock.now,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            report=lambda _message: None,
            **kwargs,
        )

    def test_starting_polls_until_running_with_fresh_heartbeat(self) -> None:
        clock = FakeClock()
        result = self.run_gate([
            self.metrics("STARTING", heartbeat=800.0),
            self.metrics("STARTING", heartbeat=800.0),
            self.metrics("RUNNING", heartbeat=1_010.0),
        ], clock)

        self.assertEqual(result["observer_state"], "RUNNING")
        self.assertEqual(clock.sleeps, [5.0, 5.0])

    def test_starting_times_out_after_exact_bounded_wait(self) -> None:
        clock = FakeClock()
        with self.assertRaisesRegex(
            ObserverHealthGateError,
            r"STARTING timed out:.*waited_seconds=600\.0.*last_error_type=StateLockTimeout",
        ):
            self.run_gate([self.metrics("STARTING", heartbeat=800.0)], clock)

        self.assertEqual(sum(clock.sleeps), MAX_ADDITIONAL_WAIT_SECONDS)
        expected_polls = int(
            MAX_ADDITIONAL_WAIT_SECONDS / POLL_INTERVAL_SECONDS
        )
        self.assertEqual(clock.sleeps, [POLL_INTERVAL_SECONDS] * expected_polls)
        self.assertEqual(clock.read_count, expected_polls + 1)

    def test_restarting_fails_immediately(self) -> None:
        clock = FakeClock()
        with self.assertRaisesRegex(
            ObserverHealthGateError,
            r"state=RESTARTING.*waited_seconds=5\.0",
        ):
            self.run_gate([
                self.metrics("STARTING"),
                self.metrics("RESTARTING"),
            ], clock)

        self.assertEqual(clock.sleeps, [5.0])

    def test_running_with_stale_heartbeat_fails_immediately(self) -> None:
        clock = FakeClock()
        with self.assertRaisesRegex(
            ObserverHealthGateError,
            "heartbeat is missing or stale",
        ):
            self.run_gate([
                self.metrics(
                    "RUNNING",
                    heartbeat=clock.now() - MAX_HEARTBEAT_AGE_SECONDS - 0.1,
                )
            ], clock)

        self.assertEqual(clock.sleeps, [])

    def test_running_with_fresh_heartbeat_passes_immediately(self) -> None:
        clock = FakeClock()
        result = self.run_gate([
            self.metrics(
                "RUNNING",
                heartbeat=clock.now(),
                health_state="RUNNING_HEALTHY",
            )
        ], clock)

        self.assertEqual(result["observer_state"], "RUNNING")
        self.assertEqual(clock.sleeps, [])

    def test_running_waits_for_current_startup_maintenance(self) -> None:
        clock = FakeClock()
        result = self.run_gate([
            self.metrics(
                "RUNNING",
                heartbeat=clock.now(),
                health_state="RUNNING_STARTING",
                maintenance_completed_at=None,
            ),
            self.metrics(
                "RUNNING",
                heartbeat=clock.now() + 5.0,
                health_state="RUNNING_MAINTENANCE_RETRYING",
                maintenance_completed_at=None,
            ),
            self.metrics(
                "RUNNING",
                heartbeat=clock.now() + 10.0,
                health_state="RUNNING_HEALTHY",
                maintenance_completed_at=1_009.0,
            ),
        ], clock)

        self.assertEqual(result["observer_health_state"], "RUNNING_HEALTHY")
        self.assertEqual(clock.sleeps, [5.0, 5.0])

    def test_running_healthy_rejects_missing_current_maintenance(self) -> None:
        clock = FakeClock()
        with self.assertRaisesRegex(
            ObserverHealthGateError,
            "startup maintenance is missing or stale",
        ):
            self.run_gate([
                self.metrics(
                    "RUNNING",
                    heartbeat=clock.now(),
                    health_state="RUNNING_HEALTHY",
                    maintenance_completed_at=None,
                )
            ], clock)

        self.assertEqual(clock.sleeps, [])

    def test_running_stalled_fails_even_with_fresh_heartbeat(self) -> None:
        clock = FakeClock()
        with self.assertRaisesRegex(
            ObserverHealthGateError,
            r"health state is not healthy:.*health_state=RUNNING_STALLED",
        ):
            self.run_gate([
                self.metrics(
                    "RUNNING",
                    heartbeat=clock.now(),
                    health_state="RUNNING_STALLED",
                )
            ], clock)

        self.assertEqual(clock.sleeps, [])


if __name__ == "__main__":
    unittest.main()
