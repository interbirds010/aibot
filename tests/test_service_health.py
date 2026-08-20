from __future__ import annotations

import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from src import service_health
from src.service_health import collection_metrics_are_fresh, service_is_fresh


class ServiceHealthTests(unittest.TestCase):
    def test_windows_pid_check_does_not_call_os_kill(self) -> None:
        with (
            patch.object(service_health.os, "name", "nt"),
            patch.object(
                service_health, "_windows_pid_is_alive", return_value=True
            ) as windows_check,
            patch.object(service_health.os, "kill") as kill,
        ):
            self.assertTrue(service_health._pid_is_alive(123))

        windows_check.assert_called_once_with(123)
        kill.assert_not_called()

    def test_collection_metrics_require_live_process_poll_and_subscription(self) -> None:
        metrics = {
            "monitor_started_at": 100,
            "monitor_process_heartbeat_at": 990,
            "route_b_last_poll_success_at": 980,
            "wallet_ws_state": "SUBSCRIBED",
            "wallet_ws_state_changed_at": 970,
            "wallet_ws_subscribed_at": 970,
        }
        self.assertTrue(collection_metrics_are_fresh(metrics, now=1_000))
        metrics["route_b_last_poll_success_at"] = 699
        self.assertFalse(collection_metrics_are_fresh(metrics, now=1_000))

    def test_collection_metrics_allow_short_reconnect_but_not_stuck_state(self) -> None:
        metrics = {
            "monitor_started_at": 100,
            "monitor_process_heartbeat_at": 990,
            "route_b_last_poll_success_at": 980,
            "wallet_ws_state": "RECONNECTING",
            "wallet_ws_state_changed_at": 900,
        }
        self.assertTrue(collection_metrics_are_fresh(metrics, now=1_000))
        metrics["wallet_ws_state_changed_at"] = 800
        self.assertFalse(collection_metrics_are_fresh(metrics, now=1_000))

    def test_collection_metrics_allow_startup_grace(self) -> None:
        self.assertTrue(collection_metrics_are_fresh(
            {"monitor_started_at": 900}, now=1_000
        ))

    def test_live_pm2_monitor_pid_is_authoritative(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pids = root / "pids"
            pids.mkdir()
            (pids / "aibot-monitor-1.pid").write_text(
                str(os.getpid()), encoding="utf-8"
            )

            self.assertTrue(service_is_fresh(root / "missing.log", pm2_home=root))

    def test_dead_pm2_pid_is_not_masked_by_fresh_log(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pids = root / "pids"
            logs = root / "logs"
            pids.mkdir()
            logs.mkdir()
            (pids / "aibot-monitor-1.pid").write_text("999999999", encoding="utf-8")
            (logs / "aibot-monitor-out.log").touch()

            self.assertFalse(service_is_fresh(root / "missing.log", pm2_home=root))

    def test_pm2_log_is_fallback_when_pid_metadata_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            logs = root / "logs"
            logs.mkdir()
            (logs / "aibot-monitor-out.log").touch()

            self.assertTrue(service_is_fresh(root / "missing.log", pm2_home=root))

    def test_legacy_log_remains_supported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy_log = root / "service.stderr.log"
            legacy_log.touch()

            self.assertTrue(
                service_is_fresh(legacy_log, pm2_home=root, now=time.time())
            )

    def test_stale_legacy_log_is_unhealthy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy_log = root / "service.stderr.log"
            legacy_log.touch()
            old = time.time() - 181
            os.utime(legacy_log, (old, old))

            self.assertFalse(
                service_is_fresh(legacy_log, pm2_home=root, now=time.time())
            )


if __name__ == "__main__":
    unittest.main()
