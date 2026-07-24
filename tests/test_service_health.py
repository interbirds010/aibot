from __future__ import annotations

import os
import tempfile
import time
import unittest
from pathlib import Path

from src.service_health import service_is_fresh


class ServiceHealthTests(unittest.TestCase):
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
