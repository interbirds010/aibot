from __future__ import annotations

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
        self.assertIn("src.research.collection_stability", workflow)
        self.assertIn("RPC_ROUTER_SMOKE", workflow)

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
        marker = 'printf \'%s\\n\' "$DEPLOY_SHA" > .deployed-sha.tmp'
        self.assertIn(gate, workflow)
        self.assertLess(workflow.index(gate), workflow.index(marker))

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
    def metrics(state: str, *, heartbeat: float = 1_000.0) -> dict:
        return {
            "observer_state": state,
            "observer_heartbeat_at": heartbeat,
            "observer_started_at": 990.0,
            "observer_last_error_type": "StateLockTimeout",
            "observer_last_error_at": 980.0,
        }

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
            r"STARTING timed out:.*waited_seconds=120\.0.*last_error_type=StateLockTimeout",
        ):
            self.run_gate([self.metrics("STARTING", heartbeat=800.0)], clock)

        self.assertEqual(sum(clock.sleeps), MAX_ADDITIONAL_WAIT_SECONDS)
        self.assertEqual(clock.sleeps, [POLL_INTERVAL_SECONDS] * 24)
        self.assertEqual(clock.read_count, 25)

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
            self.metrics("RUNNING", heartbeat=clock.now())
        ], clock)

        self.assertEqual(result["observer_state"], "RUNNING")
        self.assertEqual(clock.sleeps, [])


if __name__ == "__main__":
    unittest.main()
