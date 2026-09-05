from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DeployWorkflowTests(unittest.TestCase):
    def test_normal_deploy_does_not_wait_for_market_traffic(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("sleep 360", workflow)
        self.assertIn("src.research.collection_stability", workflow)
        self.assertIn("RPC_ROUTER_SMOKE", workflow)

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
        self.assertIn("src.research.restart_forensics", workflow)
        self.assertIn("MONITOR_RESTART_FORENSICS", (
            ROOT / "src" / "research" / "restart_forensics.py"
        ).read_text(encoding="utf-8"))
        self.assertLess(
            workflow.index("src.research.restart_forensics"),
            workflow.index('exit "$pm2_health_status"'),
        )


if __name__ == "__main__":
    unittest.main()
