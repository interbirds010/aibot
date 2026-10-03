"""Cutover는 경로가 생략된 실행도 보수적으로 차단한다. 실제 PID를 건드리지 않는다."""
import os
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from scripts import entry_telemetry_cutover as cutover


class CutoverInventoryTests(unittest.TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def test_owned_module_launch_and_children_prevent_false_stopped_proof(self):
        registry = {"services": {"monitor": {"pid": 100, "children": [{"pid": 101}]}}}
        rows = [{"pid": 101, "command": "python -m src.monitor"}]
        with patch.object(cutover.runner, "read_json", return_value=registry), \
                patch.object(cutover.runner, "process_snapshot", return_value=rows) as query:
            with self.assertRaisesRegex(RuntimeError, "still active"):
                cutover.stopped_proof(self.root)
        query.assert_called_once_with(self.root, include_ambiguous=True, registered_pids=(100, 101))

    def test_unknown_core_module_launch_conservatively_blocks_cutover(self):
        with patch.object(cutover.runner, "read_json", return_value={}), \
                patch.object(cutover.runner, "process_snapshot", return_value=[{"pid": 100}]) as query:
            with self.assertRaises(RuntimeError):
                cutover.stopped_proof(self.root)
        query.assert_called_once_with(self.root, include_ambiguous=True, registered_pids=())

    def test_only_this_tool_process_is_excluded_from_inventory(self):
        with patch.object(cutover.runner, "read_json", return_value={}), \
                patch.object(cutover.runner, "process_snapshot", return_value=[{"pid": os.getpid()}]):
            self.assertEqual(cutover.stopped_proof(self.root)["active_process_count"], 0)

    def test_corrupt_or_unbounded_registry_refuses_snapshot(self):
        for registry in ([], {"services": []}, {"services": {"monitor": {"pid": 100, "children": "invalid"}}},
                         {"services": {"monitor": {"pid": True}}}):
            with self.subTest(registry=registry), patch.object(cutover.runner, "read_json", return_value=registry), \
                    patch.object(cutover.runner, "process_snapshot") as query, self.assertRaises(RuntimeError):
                cutover.stopped_proof(self.root)
            query.assert_not_called()

    def launcher(self, *, arguments=None, creation="created", parent=100):
        argv = arguments or [str(Path(cutover.__file__).resolve()), "status", "--root", str(self.root)]
        row = {"pid": parent, "created": creation, "executable": sys.executable,
               "command": subprocess.list2cmdline([sys.executable, *argv])}
        return row, argv

    def test_identical_owned_cutover_redirector_is_excluded(self):
        row, argv = self.launcher()
        with patch.object(cutover.os, "getppid", return_value=100), patch.object(cutover.sys, "argv", argv), \
                patch.object(cutover.runner, "process_creation", return_value="created"), \
                patch.object(cutover, "_windows_arguments", return_value=[sys.executable, *argv]), \
                patch.object(cutover.runner, "read_json", return_value={}), \
                patch.object(cutover.runner, "process_snapshot", return_value=[row, {"pid": os.getpid()}]):
            self.assertEqual(cutover.stopped_proof(self.root)["active_process_count"], 0)

    def test_parent_process_with_other_command_is_never_excluded(self):
        row, argv = self.launcher()
        for other in ([str(Path(cutover.__file__).resolve()), "resume", "--root", str(self.root)],
                      [str(Path(cutover.__file__).resolve()), "status", "--root", str(self.root / "other")],
                      ["-m", "src.monitor"]):
            with self.subTest(other=other), patch.object(cutover.os, "getppid", return_value=100), \
                    patch.object(cutover.sys, "argv", argv), \
                    patch.object(cutover.runner, "process_creation", return_value="created"), \
                    patch.object(cutover, "_windows_arguments", return_value=[sys.executable, *other]):
                self.assertFalse(cutover._own_launcher(row))

    def test_redirector_pid_reuse_or_executable_mismatch_is_not_excluded(self):
        row, argv = self.launcher()
        for current, candidate in (("reused", row), ("created", {**row, "executable": str(self.root / "other-python.exe")})):
            with self.subTest(candidate=candidate), patch.object(cutover.os, "getppid", return_value=100), \
                    patch.object(cutover.sys, "argv", argv), patch.object(cutover.runner, "process_creation", return_value=current):
                self.assertFalse(cutover._own_launcher(candidate))

    @unittest.skipUnless(sys.platform == "win32", "Windows venv redirector inventory")
    def test_actual_windows_venv_status_excludes_both_cli_processes(self):
        # 임시 경로 status만 실행한다. 서비스 시작/중지/원장 쓰기는 없다.
        result = subprocess.run([sys.executable, str(Path(cutover.__file__).resolve()), "status", "--root", str(self.root)],
                                cwd=str(Path(__file__).resolve().parents[1]), capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["runtime_process_count"], 0)
        self.assertFalse(report["epoch_created"])
        self.assertFalse((self.root / "data").exists())
