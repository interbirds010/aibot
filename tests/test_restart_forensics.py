from __future__ import annotations

import unittest

from src.research.restart_forensics import (
    build_restart_forensics,
    safe_log_tail,
)


def pm2_app(
    *, pid: int, restarts: int, exit_code: int | None, signal: str | None,
    memory: int,
) -> dict:
    return {
        "name": "aibot-monitor",
        "pid": pid,
        "pm2_env": {
            "status": "online",
            "restart_time": restarts,
            "unstable_restarts": 0,
            "exit_code": exit_code,
            "exit_signal": signal,
            "pm_uptime": 1_788_600_000_000,
        },
        "monit": {"memory": memory},
    }


class RestartForensicsTests(unittest.TestCase):
    def test_restart_delta_and_pm2_exit_fields_are_preserved(self) -> None:
        report = build_restart_forensics(
            [pm2_app(pid=10, restarts=2, exit_code=0, signal=None, memory=100)],
            [pm2_app(
                pid=11,
                restarts=3,
                exit_code=137,
                signal="SIGKILL",
                memory=200,
            )],
            deployed_sha="a" * 40,
            available_memory_bytes=300,
            oom_evidence="UNKNOWN",
            stderr_tail="Traceback\nRuntimeError: stopped",
        )

        self.assertTrue(report["restart_detected"])
        self.assertEqual(report["restart_delta"], 1)
        self.assertEqual(report["before"]["pid"], 10)
        self.assertEqual(report["after"]["pid"], 11)
        self.assertEqual(report["after"]["previous_process_exit_code"], 137)
        self.assertEqual(
            report["after"]["previous_process_exit_signal"], "SIGKILL"
        )
        self.assertEqual(report["after"]["current_rss_bytes"], 200)
        self.assertEqual(report["system_available_memory_bytes"], 300)

    def test_forensic_tail_redacts_urls_and_credentials(self) -> None:
        tail = safe_log_tail(
            "connect wss://node.invalid/path/private?api-key=secret\n"
            "API_KEY=another-secret token=third-secret"
        )
        joined = "\n".join(tail)
        self.assertNotIn("node.invalid", joined)
        self.assertNotIn("secret", joined)
        self.assertIn("<URL_REDACTED>", joined)
        self.assertIn("HIDDEN_MASKED", joined)

    def test_invalid_deployed_sha_is_not_echoed(self) -> None:
        report = build_restart_forensics(
            [],
            [],
            deployed_sha="https://secret.invalid/?api-key=value",
            available_memory_bytes=None,
            oom_evidence="UNKNOWN",
        )
        self.assertEqual(report["deployed_sha"], "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
