import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DeploySudoRefreshTests(unittest.TestCase):
    def test_late_nginx_step_refreshes_sudo_credential(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "sudo -S -p '' bash ./scripts/configure_nginx_aibot.sh",
            workflow,
        )


if __name__ == "__main__":
    unittest.main()
