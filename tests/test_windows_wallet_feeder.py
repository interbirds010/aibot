"""공급기 복구 조건을 보존하고 플랫폼별 단발 실행 계약을 검증한다."""

import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

from src import monitor


class WindowsWalletFeederTests(unittest.TestCase):
    def setUp(self) -> None:
        for name, value in (
            ("get_active_wallets_count", 17),
            ("get_global_metric", 0),
            ("claim_global_interval", True),
        ):
            patcher = patch.object(monitor.state_store, name, return_value=value)
            setattr(self, name, patcher.start())
            self.addCleanup(patcher.stop)
        patcher = patch.object(monitor.subprocess, "Popen")
        self.popen = patcher.start()
        self.addCleanup(patcher.stop)

    def test_windows_uses_hidden_runner_current_python_and_inherits_environment(self) -> None:
        with (
            patch.object(monitor.sys, "platform", "win32"),
            patch.object(monitor.sys, "executable", "local-venv-python.exe"),
            patch.object(monitor.subprocess, "CREATE_NO_WINDOW", 0x08000000, create=True),
        ):
            self.assertTrue(monitor.trigger_wallet_feeder_if_needed(now=10_000))
        self.popen.assert_called_once_with(
            ["local-venv-python.exe",
             str(Path(monitor.__file__).resolve().parents[1] / "scripts" / "local_paper_runner.py"),
             "start", "wallet-feeder", "--python", "local-venv-python.exe"],
            cwd=Path(monitor.__file__).resolve().parents[1],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            creationflags=0x08000000,
        )
        self.claim_global_interval.assert_called_once_with(
            "last_wallet_feeder_run_time", 10_000.0,
            monitor.WALLET_FEEDER_COOLDOWN_SECONDS,
        )

    def test_linux_preserves_pm2_launch_contract(self) -> None:
        with patch.object(monitor.sys, "platform", "linux"):
            self.assertTrue(monitor.trigger_wallet_feeder_if_needed(now=10_000))
        self.popen.assert_called_once_with(
            ["pm2", "start", "wallet_feeder"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True,
        )

    def test_windows_does_not_launch_above_existing_wallet_threshold(self) -> None:
        self.get_active_wallets_count.return_value = 18
        with patch.object(monitor.sys, "platform", "win32"):
            self.assertFalse(monitor.trigger_wallet_feeder_if_needed(now=10_000))
        self.popen.assert_not_called()
        self.claim_global_interval.assert_not_called()

    def test_windows_does_not_launch_during_existing_two_hour_cooldown(self) -> None:
        self.get_global_metric.return_value = 5_000
        with patch.object(monitor.sys, "platform", "win32"):
            self.assertFalse(monitor.trigger_wallet_feeder_if_needed(now=10_000))
        self.popen.assert_not_called()
        self.claim_global_interval.assert_not_called()

    def test_windows_does_not_launch_when_another_process_claims_interval(self) -> None:
        self.claim_global_interval.return_value = False
        with patch.object(monitor.sys, "platform", "win32"):
            self.assertFalse(monitor.trigger_wallet_feeder_if_needed(now=10_000))
        self.popen.assert_not_called()
        self.claim_global_interval.assert_called_once()

    def test_missing_windows_interpreter_keeps_existing_failed_claim_semantics(self) -> None:
        self.popen.side_effect = FileNotFoundError("test executable unavailable")
        with (
            patch.object(monitor.sys, "platform", "win32"),
            patch.object(monitor.subprocess, "CREATE_NO_WINDOW", 0x08000000, create=True),
            self.assertLogs(monitor.logger, level="ERROR") as captured,
        ):
            self.assertFalse(monitor.trigger_wallet_feeder_if_needed(now=10_000))
        self.claim_global_interval.assert_called_once()
        self.popen.assert_called_once()
        self.assertIn("executable not found", captured.output[0])


if __name__ == "__main__":
    unittest.main()
