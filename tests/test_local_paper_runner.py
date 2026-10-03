from __future__ import annotations

import os
import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest import mock

from scripts import local_paper_runner as runner


class LocalPaperRunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "src").mkdir()
        (self.root / "src/monitor.py").touch()
        self.python = self.root / "python.exe"
        self.python.touch()

    def record(self, name="monitor", pid=100, created="123"):
        command = runner.service_command(self.root, self.python, name)
        return {"pid": pid, "created": created, "executable": str(self.python),
                "command": subprocess.list2cmdline(command)}

    def save_record(self, record):
        (self.root / "logs").mkdir(exist_ok=True)
        runner.atomic_write_json(self.root / "logs/local_runner.json",
                                 {"version": 1, "services": {"monitor": record}})

    def manage(self, action, names=("monitor",)):
        with mock.patch.object(runner.sys, "platform", "win32"):
            return runner.manage(self.root, self.python, action, names)

    def test_commands_use_absolute_runtime_and_one_shot_feeder(self):
        for name in runner.SERVICES:
            command = runner.service_command(self.root, self.python, name)
            self.assertEqual(command[0], str(self.python))
            self.assertTrue(any(str(self.root / "src") in item for item in command))
        self.assertEqual(runner.service_command(self.root, self.python, "wallet-feeder")[-1], "--once")

    def test_dashboard_is_loopback_and_preserves_base_path(self):
        command = runner.service_command(self.root, self.python, "dashboard")
        self.assertEqual(command[command.index("--server.address") + 1], "127.0.0.1")
        self.assertEqual(command[command.index("--server.baseUrlPath") + 1], "ai-bot")

    def test_environment_pins_paper_and_production_observation_contract(self):
        with mock.patch.dict(os.environ, {"SOLANA_KEY_ENCRYPTION_KEY": "do-not-print"}, clear=True):
            environment = runner.paper_environment(self.root)
        self.assertEqual(environment["TRADING_MODE"], "paper")
        self.assertEqual(environment["OBSERVATION_MODE"], "true")
        self.assertEqual(environment["APPROVED_SIGNAL_PAPER_MODE"], "true")
        self.assertEqual(environment["APPROVED_SIGNAL_MAX_OPEN_POSITIONS"], "8")
        self.assertEqual(environment["DASHBOARD_COOKIE_SECURE"], "false")
        self.assertNotIn("SOLANA_KEY_ENCRYPTION_KEY", environment)

    def test_live_dotenv_refused_without_exposing_other_values(self):
        (self.root / ".env").write_text("TRADING_MODE=live\nPRIVATE=do-not-print\n", encoding="utf-8")
        with mock.patch.dict(os.environ, {}, clear=True), self.assertRaises(RuntimeError) as caught:
            runner.paper_environment(self.root)
        self.assertNotIn("do-not-print", str(caught.exception))

    def test_live_inherited_environment_refused(self):
        with mock.patch.dict(os.environ, {"TRADING_MODE": "live"}, clear=True):
            with self.assertRaises(RuntimeError):
                runner.paper_environment(self.root)

    def test_identity_requires_creation_command_and_executable(self):
        record = self.record()
        self.assertTrue(runner.process_matches(record, record))
        for key, value in (("created", "124"), ("command", "other.py"),
                           ("executable", "other.exe"), ("pid", 101)):
            self.assertFalse(runner.process_matches({**record, key: value}, record))

    def test_start_existing_identity_does_not_launch_duplicate(self):
        record = self.record()
        self.save_record(record)
        with mock.patch.object(runner, "process_snapshot", return_value=[record]), \
                mock.patch.object(runner, "start_process") as start:
            result = self.manage("start")
        start.assert_not_called()
        self.assertEqual(result[0]["state"], "running")

    def test_unmanaged_process_is_neither_started_nor_stopped(self):
        record = self.record()
        for action in ("start", "stop"):
            with mock.patch.object(runner, "process_snapshot", return_value=[record]), \
                    mock.patch.object(runner, "start_process") as start, \
                    mock.patch.object(runner, "terminate_record") as stop:
                with self.assertRaises(RuntimeError):
                    self.manage(action)
            start.assert_not_called()
            stop.assert_not_called()

    def test_reused_pid_refuses_stop(self):
        record = self.record()
        self.save_record(record)
        with mock.patch.object(runner, "process_snapshot", return_value=[{**record, "created": "reused"}]), \
                mock.patch.object(runner, "terminate_record") as stop:
            with self.assertRaises(RuntimeError):
                self.manage("stop")
        stop.assert_not_called()

    def test_known_stop_updates_registry_without_trading_state(self):
        record = self.record()
        self.save_record(record)
        with mock.patch.object(runner, "process_snapshot", return_value=[record]), \
                mock.patch.object(runner, "process_creation", return_value=record["created"]), \
                mock.patch.object(runner, "terminate_record") as stop:
            result = self.manage("stop")
        stop.assert_called_once_with(record)
        self.assertEqual(result[0]["state"], "stopped")
        state = runner.read_json(self.root / "logs/local_runner.json", {})
        self.assertEqual(state["services"], {})
        self.assertEqual(state["version"], 2)
        self.assertFalse((self.root / "data").exists())

    def test_venv_launcher_and_known_child_are_one_service(self):
        record = self.record()
        child = {**record, "pid": 101, "created": "124", "parent_pid": 100,
                 "executable": "base-python.exe"}
        record["children"] = [child]
        self.save_record(record)
        with mock.patch.object(runner, "process_snapshot", return_value=[record, child]), \
                mock.patch.object(runner, "start_process") as start:
            result = self.manage("start")
        start.assert_not_called()
        self.assertEqual(result[0]["state"], "running")
        self.assertEqual(result[0]["pid"], 101)
        self.assertEqual(result[0]["pids"], [100, 101])

    def test_extra_launcher_is_refused(self):
        record = self.record()
        self.save_record(record)
        with mock.patch.object(runner, "process_snapshot", return_value=[record, self.record(pid=102)]), \
                mock.patch.object(runner, "start_process") as start:
            with self.assertRaises(RuntimeError):
                self.manage("start")
        start.assert_not_called()

    def test_stop_rechecks_creation_on_the_open_handle(self):
        kernel = mock.Mock()
        kernel.OpenProcess.return_value = 1
        with mock.patch.object(runner, "_kernel32", return_value=kernel), \
                mock.patch.object(runner, "_handle_creation", return_value="reused"):
            with self.assertRaises(RuntimeError):
                runner.terminate_record(self.record())
        kernel.TerminateProcess.assert_not_called()
        kernel.CloseHandle.assert_called_once_with(1)

    def test_already_exited_process_does_not_fail_stop(self):
        kernel = mock.Mock()
        kernel.OpenProcess.return_value = 1
        kernel.WaitForSingleObject.return_value = 0
        with mock.patch.object(runner, "_kernel32", return_value=kernel), \
                mock.patch.object(runner, "_handle_creation", return_value="123"):
            runner.terminate_record(self.record())
        kernel.TerminateProcess.assert_not_called()
        kernel.CloseHandle.assert_called_once_with(1)

    def test_start_records_identity(self):
        record = self.record()
        with mock.patch.object(runner, "process_snapshot", return_value=[]), \
                mock.patch.object(runner, "start_process", return_value=record):
            self.manage("start")
        state = runner.read_json(self.root / "logs/local_runner.json", {})
        self.assertEqual(state["services"]["monitor"], record)

    def test_occupied_dashboard_port_refuses_launch(self):
        with mock.patch.object(runner, "process_snapshot", return_value=[]), \
                mock.patch.object(runner, "dashboard_port_available", return_value=False), \
                mock.patch.object(runner, "start_process") as start:
            with self.assertRaises(RuntimeError):
                self.manage("start", ("dashboard",))
        start.assert_not_called()

    def test_partial_start_failure_preserves_started_process_identity(self):
        record = self.record()
        with mock.patch.object(runner, "process_snapshot", return_value=[]), \
                mock.patch.object(runner, "start_process", side_effect=[record, RuntimeError("failed")]):
            with self.assertRaises(RuntimeError):
                self.manage("start", ("monitor", "risk-manager"))
        state = runner.read_json(self.root / "logs/local_runner.json", {})
        self.assertEqual(state["services"], {"monitor": record})

    def test_status_does_not_require_secret_environment_or_start(self):
        with mock.patch.object(runner, "process_snapshot", return_value=[]), \
                mock.patch.object(runner, "paper_environment") as environment, \
                mock.patch.object(runner, "start_process") as start:
            self.assertEqual(self.manage("status")[0]["state"], "not_started")
        environment.assert_not_called()
        start.assert_not_called()

    def test_inventory_uses_case_insensitive_windows_path_matching(self):
        row = {"pid": 100, "command": str(self.root).swapcase() + "\\src\\monitor.py", "executable": str(self.python)}
        result = mock.Mock(returncode=0, stdout=json.dumps([row]))
        with mock.patch.object(runner.subprocess, "run", return_value=result) as query, \
                mock.patch.object(runner, "process_creation", return_value="created"):
            rows = runner.process_snapshot(self.root)
        command = query.call_args.args[0][-1]
        self.assertIn("[System.StringComparison]::OrdinalIgnoreCase", command)
        self.assertEqual(rows[0]["created"], "created")

    def test_cutover_inventory_selects_ambiguous_modules_and_registered_pids(self):
        result = mock.Mock(returncode=0, stdout="[]")
        with mock.patch.object(runner.subprocess, "run", return_value=result) as query:
            runner.process_snapshot(self.root, include_ambiguous=True, registered_pids=(100, 101))
        command = query.call_args.args[0][-1]
        self.assertIn("-match '(?i)", command)
        self.assertIn("monitor|risk_manager|wallet_feeder", command)
        self.assertIn("$_.ProcessId -in @(100,101)", command)

    def test_registered_pid_inventory_rejects_injection_and_unbounded_input(self):
        for identities in (("100);throw 'x'",), (True,), tuple(range(1, 34))):
            with mock.patch.object(runner.subprocess, "run") as query, self.assertRaises(RuntimeError):
                runner.process_snapshot(self.root, registered_pids=identities)
            query.assert_not_called()


if __name__ == "__main__":
    unittest.main()
