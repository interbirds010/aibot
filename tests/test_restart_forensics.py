from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.research.restart_forensics import (
    _safe_pm2_file_tail,
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
            "max_memory_restart": 272_629_760,
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
        self.assertEqual(
            report["after"]["max_memory_restart_bytes"], 272_629_760
        )
        self.assertEqual(report["system_available_memory_bytes"], 300)

    def test_explicit_pm2_memory_restart_is_canonicalized(self) -> None:
        report = build_restart_forensics(
            [pm2_app(pid=10, restarts=2, exit_code=0, signal=None, memory=100)],
            [pm2_app(pid=11, restarts=3, exit_code=0, signal=None, memory=200)],
            deployed_sha="b" * 40,
            available_memory_bytes=300,
            oom_evidence="NONE",
            pm2_daemon_tail=(
                "[PM2][WORKER] Process 0 restarted because it exceeds "
                "--max-memory-restart value"
            ),
        )

        self.assertEqual(
            report["restart_reason_category"], "PM2_MAX_MEMORY_RESTART"
        )

    def test_unknown_reason_is_not_guessed(self) -> None:
        report = build_restart_forensics(
            [pm2_app(pid=10, restarts=2, exit_code=0, signal=None, memory=100)],
            [pm2_app(pid=11, restarts=3, exit_code=0, signal=None, memory=200)],
            deployed_sha="c" * 40,
            available_memory_bytes=300,
            oom_evidence="NONE",
            stdout_tail="ordinary monitor output",
            pm2_daemon_tail="App exited with code [0]",
        )

        self.assertEqual(report["restart_reason_category"], "UNKNOWN")

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

    def test_all_forensic_log_tails_are_redacted(self) -> None:
        report = build_restart_forensics(
            [],
            [],
            deployed_sha="d" * 40,
            available_memory_bytes=None,
            oom_evidence="UNKNOWN",
            stdout_tail="RPC https://node.invalid/?api-key=stdout-secret",
            pm2_daemon_tail="token=daemon-secret",
        )
        joined = "\n".join(
            report["recent_safe_stdout_tail"]
            + report["recent_safe_pm2_daemon_tail"]
        )
        self.assertNotIn("node.invalid", joined)
        self.assertNotIn("stdout-secret", joined)
        self.assertNotIn("daemon-secret", joined)

    def test_pm2_path_field_is_used_for_error_tail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            error_log = base / "aibot-monitor-error.log"
            error_log.write_text("safe traceback line", encoding="utf-8")
            apps = [pm2_app(
                pid=10, restarts=2, exit_code=0, signal=None, memory=100
            )]
            apps[0]["pm2_env"]["pm_err_log_path"] = str(error_log)

            with mock.patch.dict(os.environ, {"PM2_HOME": directory}):
                tail, status = _safe_pm2_file_tail(
                    apps, "pm_err_log_path", "pm_err_log"
                )

        self.assertEqual(status, "AVAILABLE")
        self.assertEqual(tail, "safe traceback line")

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
