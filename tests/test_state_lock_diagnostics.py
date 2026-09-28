from __future__ import annotations

import ast
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from src import state_lock_diagnostics, state_store

if sys.platform.startswith("linux"):
    import fcntl


LINUX = sys.platform.startswith("linux")


class StateLockDiagnosticContractTests(unittest.TestCase):
    def _operations_by_function(self, path: Path) -> dict[str, list[str]]:
        operations: dict[str, list[str]] = {}

        class Visitor(ast.NodeVisitor):
            current_function = "<module>"

            def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
                previous = self.current_function
                self.current_function = node.name
                self.generic_visit(node)
                self.current_function = previous

            visit_AsyncFunctionDef = visit_FunctionDef

            def visit_Call(self, node: ast.Call) -> None:
                name = (
                    node.func.id
                    if isinstance(node.func, ast.Name)
                    else node.func.attr
                    if isinstance(node.func, ast.Attribute)
                    else ""
                )
                if (
                    name == "to_thread"
                    and node.args
                    and isinstance(node.args[0], ast.Name)
                    and node.args[0].id
                    in {"exclusive_file_lock", "update_json", "migrate_json"}
                ):
                    name = node.args[0].id
                if name in {"exclusive_file_lock", "update_json", "migrate_json"}:
                    operation = next(
                        (keyword.value for keyword in node.keywords if keyword.arg == "operation"),
                        None,
                    )
                    if isinstance(operation, ast.Constant) and isinstance(
                        operation.value, str
                    ):
                        operations.setdefault(self.current_function, []).append(
                            operation.value
                        )
                self.generic_visit(node)

        Visitor().visit(ast.parse(path.read_text(encoding="utf-8")))
        return operations

    def test_normal_acquire_release_preserves_update_semantics(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / "state.json"
            snapshot = root / "diagnostic.json"

            def mutate(document: dict[str, object]) -> str:
                document["value"] = 7
                return "saved"

            with patch.object(state_lock_diagnostics, "SNAPSHOT_PATH", snapshot):
                result, document = state_store.update_json(
                    target,
                    {"version": 0},
                    mutate,
                    operation="normal_update",
                )

            self.assertEqual(result, "saved")
            self.assertEqual(document["version"], 1)
            self.assertEqual(state_store.read_json(target, {}), document)
            self.assertFalse(snapshot.exists())

    def test_diagnostic_failure_is_non_fatal_for_normal_lock(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "state.json"
            with patch.object(
                state_lock_diagnostics,
                "begin",
                side_effect=RuntimeError("diagnostic failed"),
            ):
                with state_store.exclusive_file_lock(target):
                    target.write_text("{}", encoding="utf-8")
            self.assertTrue(target.exists())

    def test_traceback_and_encoded_snapshot_are_bounded(self) -> None:
        secret = "SECRET_SENTINEL_MUST_NOT_APPEAR"
        self.assertTrue(secret)
        trace = state_lock_diagnostics._capture_same_process_traceback()
        self.assertLessEqual(len(trace), state_lock_diagnostics.MAX_TRACE_THREADS)
        self.assertTrue(all(
            len(thread["frames"]) <= state_lock_diagnostics.MAX_TRACE_FRAMES
            for thread in trace
        ))
        encoded = state_lock_diagnostics._bounded_diagnostic_bytes({
            "event": "TIMEOUT",
            "same_process_traceback": trace,
        })
        self.assertLessEqual(
            len(encoded), state_lock_diagnostics.MAX_SNAPSHOT_BYTES
        )
        self.assertNotIn(secret.encode(), encoded)
        self.assertNotIn(b"locals", encoded)

    def test_snapshot_overwrites_with_fixed_storage_bound(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            snapshot = Path(temporary) / "diagnostic.json"
            huge_trace = [
                {
                    "thread_id": thread_id,
                    "frames": [
                        {
                            "file": "x" * 128,
                            "function": "y" * 128,
                            "line": frame,
                        }
                        for frame in range(100)
                    ],
                }
                for thread_id in range(100)
            ]
            with patch.object(state_lock_diagnostics, "SNAPSHOT_PATH", snapshot):
                for sequence in range(5):
                    state_lock_diagnostics._write_snapshot({
                        "event": "TIMEOUT",
                        "sequence": sequence,
                        "same_process_traceback": huge_trace,
                    })
            saved = json.loads(snapshot.read_text(encoding="utf-8"))
            self.assertEqual(saved["latest_timeout"]["sequence"], 4)
            total = sum(path.stat().st_size for path in snapshot.parent.iterdir())
            self.assertLessEqual(total, (2 * 64 * 1024) + 1)

    def test_snapshot_process_guard_wait_is_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            snapshot = Path(temporary) / "diagnostic.json"
            state_lock_diagnostics._SNAPSHOT_GUARD.acquire()
            try:
                started = time.monotonic()
                with (
                    patch.object(state_lock_diagnostics, "SNAPSHOT_PATH", snapshot),
                    patch.object(
                        state_lock_diagnostics,
                        "SNAPSHOT_LOCK_TIMEOUT_SECONDS",
                        0.02,
                    ),
                ):
                    persisted = state_lock_diagnostics._write_snapshot({
                        "event": "TIMEOUT"
                    })
                elapsed = time.monotonic() - started
            finally:
                state_lock_diagnostics._SNAPSHOT_GUARD.release()
            self.assertFalse(persisted)
            self.assertLess(elapsed, 0.5)
            self.assertFalse(snapshot.exists())

    def test_slow_event_does_not_overwrite_latest_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            snapshot = Path(temporary) / "diagnostic.json"
            with patch.object(state_lock_diagnostics, "SNAPSHOT_PATH", snapshot):
                state_lock_diagnostics._write_snapshot({
                    "event": "TIMEOUT",
                    "attempt_id": "timeout-1",
                })
                state_lock_diagnostics._write_snapshot({
                    "event": "RELEASED",
                    "attempt_id": "holder-1",
                    "hold_ms": 16000.0,
                })
            saved = json.loads(snapshot.read_text(encoding="utf-8"))
            self.assertEqual(saved["latest_timeout"]["attempt_id"], "timeout-1")
            self.assertEqual(saved["recent_events"][-1]["attempt_id"], "holder-1")

    def test_holder_thread_is_prioritized_in_bounded_trace(self) -> None:
        frame = sys._getframe()
        fake_frames = {thread_id: frame for thread_id in range(40)}
        fake_frames[999] = frame
        with patch.object(
            state_lock_diagnostics.sys,
            "_current_frames",
            return_value=fake_frames,
        ):
            trace = state_lock_diagnostics._capture_same_process_traceback(999)
        self.assertEqual(trace[0]["thread_id"], 999)
        self.assertLessEqual(len(trace), state_lock_diagnostics.MAX_TRACE_THREADS)

    def test_after_fork_reset_replaces_inherited_guards(self) -> None:
        old_local_guard = state_lock_diagnostics._LOCAL_HOLDER_GUARD
        old_snapshot_guard = state_lock_diagnostics._SNAPSHOT_GUARD
        old_local_guard.acquire()
        old_snapshot_guard.acquire()
        try:
            state_lock_diagnostics._reset_after_fork()
            self.assertIsNot(
                state_lock_diagnostics._LOCAL_HOLDER_GUARD, old_local_guard
            )
            self.assertIsNot(
                state_lock_diagnostics._SNAPSHOT_GUARD, old_snapshot_guard
            )
            self.assertEqual(state_lock_diagnostics._LOCAL_HOLDERS, {})
        finally:
            old_local_guard.release()
            old_snapshot_guard.release()

    def test_observation_startup_labels_are_explicit(self) -> None:
        operations = self._operations_by_function(
            Path(__file__).parents[1] / "src/observation_tracker.py"
        )
        self.assertEqual(operations["ensure_observations_migrated"], [
            "startup_migration"
        ])
        self.assertEqual(operations["reconcile_interrupted_discoveries"], [
            "startup_reconciliation"
        ])

    def test_runtime_observation_labels_are_explicit(self) -> None:
        operations = self._operations_by_function(
            Path(__file__).parents[1] / "src/observation_tracker.py"
        )
        expected = {
            "record_candidate_discovery": "candidate_discovery",
            "finalize_candidate_without_quote": "finalize_without_quote",
            "record_observation_decision": "observation_decision",
            "mark_paper_experiment_status": "paper_experiment_status",
            "record_sample_attempt": "sample_attempt",
            "record_sample_batch": "sample_batch",
        }
        for function, operation in expected.items():
            self.assertEqual(operations[function], [operation])

    def test_archive_marker_label_is_explicit(self) -> None:
        operations = self._operations_by_function(
            Path(__file__).parents[1] / "src/observation_tracker.py"
        )
        self.assertEqual(operations["_persist_archive_markers"], [
            "archive_marker_persist"
        ])

    def test_all_runtime_lock_callsites_have_explicit_operations(self) -> None:
        root = Path(__file__).parents[1] / "src"
        missing: list[str] = []
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                name = (
                    node.func.id
                    if isinstance(node.func, ast.Name)
                    else node.func.attr
                    if isinstance(node.func, ast.Attribute)
                    else ""
                )
                if (
                    name == "to_thread"
                    and node.args
                    and isinstance(node.args[0], ast.Name)
                    and node.args[0].id
                    in {"exclusive_file_lock", "update_json", "migrate_json"}
                ):
                    name = node.args[0].id
                if name not in {"exclusive_file_lock", "update_json", "migrate_json"}:
                    continue
                if not any(keyword.arg == "operation" for keyword in node.keywords):
                    missing.append(f"{path.relative_to(root)}:{node.lineno}:{name}")
        self.assertEqual(missing, [])

    def test_runtime_update_propagates_operation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "state.json"
            operations: list[str] = []
            original = state_lock_diagnostics.acquired

            def capture(
                attempt: state_lock_diagnostics.LockAttempt, handle: object
            ) -> None:
                operations.append(attempt.operation)
                original(attempt, handle)  # type: ignore[arg-type]

            with patch.object(state_lock_diagnostics, "acquired", capture):
                state_store.update_json(
                    target,
                    {"version": 0},
                    lambda document: document.update({"updated": True}),
                    operation="sample_attempt",
                )
            self.assertEqual(operations, ["sample_attempt"])


@unittest.skipUnless(LINUX, "requires Linux flock and /proc/locks")
class LinuxStateLockDiagnosticTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.target = self.root / "state.json"
        self.snapshot = self.root / "diagnostic.json"
        self.snapshot_patch = patch.object(
            state_lock_diagnostics, "SNAPSHOT_PATH", self.snapshot
        )
        self.snapshot_patch.start()

    def tearDown(self) -> None:
        self.snapshot_patch.stop()
        self.temporary.cleanup()

    def _thread_holder(
        self,
        *,
        operation: str = "thread_holder",
    ) -> tuple[threading.Thread, threading.Event, threading.Event]:
        acquired = threading.Event()
        release = threading.Event()

        def hold() -> None:
            with state_store.exclusive_file_lock(
                self.target, operation=operation
            ):
                acquired.set()
                release.wait(5)

        thread = threading.Thread(target=hold, daemon=True)
        thread.start()
        self.assertTrue(acquired.wait(2))
        return thread, acquired, release

    def _fork_raw_holder(
        self,
        *,
        copy_and_fsync: bool = False,
    ) -> tuple[int, int]:
        ready_read, ready_write = os.pipe()
        release_read, release_write = os.pipe()
        pid = os.fork()
        if pid == 0:
            try:
                os.close(ready_read)
                os.close(release_write)
                lock_path = self.target.with_name(f"{self.target.name}.lock")
                lock_path.parent.mkdir(parents=True, exist_ok=True)
                with open(lock_path, "a+b") as lock:
                    if lock.seek(0, os.SEEK_END) == 0:
                        lock.write(b"\0")
                        lock.flush()
                    lock.seek(0)
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
                    if copy_and_fsync:
                        backup = self.root / "backup.json"
                        with open(backup, "wb") as output:
                            output.write(b"{}\n")
                            output.flush()
                            os.fsync(output.fileno())
                    os.write(ready_write, b"1")
                    action = os.read(release_read, 1)
                    if action == b"x":
                        os._exit(0)
                    fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            finally:
                os._exit(0)
        os.close(ready_write)
        os.close(release_read)
        self.assertEqual(os.read(ready_read, 1), b"1")
        os.close(ready_read)
        return pid, release_write

    def _finish_child(self, pid: int, release_write: int, action: bytes = b"r") -> None:
        try:
            os.write(release_write, action)
        finally:
            os.close(release_write)
        waited_pid, status = os.waitpid(pid, 0)
        self.assertEqual(waited_pid, pid)
        self.assertTrue(os.WIFEXITED(status))

    def test_contention_waits_then_acquires(self) -> None:
        thread, _, release = self._thread_holder()
        timer = threading.Timer(0.12, release.set)
        timer.start()
        started = time.monotonic()
        with state_store.exclusive_file_lock(
            self.target,
            timeout_seconds=1.0,
            poll_seconds=0.01,
            operation="waiter",
        ):
            pass
        elapsed = time.monotonic() - started
        thread.join(2)
        timer.cancel()
        self.assertGreaterEqual(elapsed, 0.08)

    def test_snapshot_cross_process_lock_wait_is_bounded(self) -> None:
        lock_path = self.snapshot.with_name(f"{self.snapshot.name}.write.lock")
        lock_path.write_bytes(b"\0")
        with open(lock_path, "r+b") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            started = time.monotonic()
            with patch.object(
                state_lock_diagnostics,
                "SNAPSHOT_LOCK_TIMEOUT_SECONDS",
                0.02,
            ):
                persisted = state_lock_diagnostics._write_snapshot({
                    "event": "TIMEOUT"
                })
            elapsed = time.monotonic() - started
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        self.assertFalse(persisted)
        self.assertLess(elapsed, 0.5)
        self.assertFalse(self.snapshot.exists())

    def test_same_process_thread_timeout_captures_holder_and_stack(self) -> None:
        thread, _, release = self._thread_holder(operation="candidate_discovery")
        try:
            with self.assertRaises(state_store.StateLockTimeout) as raised:
                with state_store.exclusive_file_lock(
                    self.target,
                    timeout_seconds=0.08,
                    poll_seconds=0.01,
                    operation="sample_batch",
                ):
                    pass
            diagnostic = raised.exception.diagnostics
            self.assertTrue(diagnostic["verified"])
            self.assertEqual(diagnostic["holder_pid"], os.getpid())
            self.assertEqual(
                diagnostic["holder_operation"], "candidate_discovery"
            )
            self.assertGreater(diagnostic["holder_hold_ms"], 0)
            snapshot = json.loads(self.snapshot.read_text(encoding="utf-8"))[
                "latest_timeout"
            ]
            self.assertLessEqual(
                len(snapshot["same_process_traceback"]),
                state_lock_diagnostics.MAX_TRACE_THREADS,
            )
        finally:
            release.set()
            thread.join(2)

    def test_cross_process_timeout_verifies_holder_pid(self) -> None:
        pid, release_write = self._fork_raw_holder()
        try:
            with self.assertRaises(state_store.StateLockTimeout) as raised:
                with state_store.exclusive_file_lock(
                    self.target,
                    timeout_seconds=0.08,
                    poll_seconds=0.01,
                    operation="sample_attempt",
                ):
                    pass
            diagnostic = raised.exception.diagnostics
            self.assertTrue(diagnostic["verified"])
            self.assertEqual(diagnostic["holder_pid"], pid)
            self.assertEqual(
                diagnostic["holder_operation"], "external_or_unknown"
            )
            self.assertIsNotNone(diagnostic["holder_process_start"])
        finally:
            self._finish_child(pid, release_write)

    def test_holder_crash_releases_kernel_lock(self) -> None:
        pid, release_write = self._fork_raw_holder()
        self._finish_child(pid, release_write, b"x")
        with state_store.exclusive_file_lock(
            self.target,
            timeout_seconds=0.5,
            poll_seconds=0.01,
            operation="retry_after_crash",
        ):
            pass

    def test_holder_release_during_diagnosis_is_reported(self) -> None:
        lock_path = self.target.with_name(f"{self.target.name}.lock")
        lock_path.write_bytes(b"\0")
        metadata = {
            "holder_process_start": "boot:10",
            "holder_state": "S",
            "holder_wchan": "wait",
            "holder_uid": os.getuid(),
        }
        with open(lock_path, "a+b") as handle, patch.object(
            state_lock_diagnostics,
            "_find_linux_lock_holder",
            side_effect=[(123, True), (None, True)],
        ), patch.object(
            state_lock_diagnostics,
            "_read_linux_process_metadata",
            return_value=(metadata, "ok"),
        ), patch.object(
            state_lock_diagnostics,
            "_process_start_identity",
            return_value=(None, "missing"),
        ):
            result = state_lock_diagnostics._inspect_linux_holder(
                handle, waiter_pid=os.getpid()
            )
        self.assertTrue(result["released_during_diagnosis"])
        self.assertFalse(result["verified"])

    def test_stale_local_registry_cannot_override_kernel_pid(self) -> None:
        lock_path = self.target.with_name(f"{self.target.name}.lock")
        lock_path.write_bytes(b"\0")
        with open(lock_path, "a+b") as handle:
            descriptor = os.fstat(handle.fileno())
            key = (int(descriptor.st_dev), int(descriptor.st_ino))
            state_lock_diagnostics._LOCAL_HOLDERS[key] = {
                "pid": os.getpid(),
                "operation": "stale_operation",
                "hold_started_ns": time.monotonic_ns(),
            }
            metadata = {
                "holder_process_start": "boot:10",
                "holder_state": "S",
                "holder_wchan": "wait",
                "holder_uid": os.getuid(),
            }
            with patch.object(
                state_lock_diagnostics,
                "_find_linux_lock_holder",
                side_effect=[(999, True), (999, True)],
            ), patch.object(
                state_lock_diagnostics,
                "_read_linux_process_metadata",
                return_value=(metadata, "ok"),
            ), patch.object(
                state_lock_diagnostics,
                "_process_start_identity",
                return_value=("boot:10", "ok"),
            ):
                result = state_lock_diagnostics._inspect_linux_holder(
                    handle, waiter_pid=os.getpid()
                )
            state_lock_diagnostics._LOCAL_HOLDERS.pop(key, None)
        self.assertTrue(result["verified"])
        self.assertEqual(result["holder_operation"], "external_or_unknown")

    def test_pid_reuse_simulation_is_not_verified(self) -> None:
        lock_path = self.target.with_name(f"{self.target.name}.lock")
        lock_path.write_bytes(b"\0")
        metadata = {
            "holder_process_start": "boot:10",
            "holder_state": "S",
            "holder_wchan": "wait",
            "holder_uid": os.getuid(),
        }
        with open(lock_path, "a+b") as handle, patch.object(
            state_lock_diagnostics,
            "_find_linux_lock_holder",
            side_effect=[(123, True), (123, True)],
        ), patch.object(
            state_lock_diagnostics,
            "_read_linux_process_metadata",
            return_value=(metadata, "ok"),
        ), patch.object(
            state_lock_diagnostics,
            "_process_start_identity",
            return_value=("boot:11", "ok"),
        ):
            result = state_lock_diagnostics._inspect_linux_holder(
                handle, waiter_pid=os.getpid()
            )
        self.assertTrue(result["pid_reused"])
        self.assertFalse(result["verified"])

    def test_proc_locks_unavailable_is_bounded(self) -> None:
        holder, usable = state_lock_diagnostics._find_linux_lock_holder(
            1,
            1,
            proc_root=self.root / "missing-proc",
        )
        self.assertIsNone(holder)
        self.assertFalse(usable)

    def test_malformed_and_blocked_proc_entries_are_ignored(self) -> None:
        proc_root = self.root / "proc"
        proc_root.mkdir()
        (proc_root / "locks").write_text(
            "malformed\n"
            "1: -> FLOCK ADVISORY WRITE 123 08:01:99 0 EOF\n"
            "2: OFDLCK ADVISORY WRITE -1 08:01:99 0 EOF\n",
            encoding="utf-8",
        )
        holder, usable = state_lock_diagnostics._find_linux_lock_holder(
            os.makedev(8, 1),
            99,
            proc_root=proc_root,
        )
        self.assertIsNone(holder)
        self.assertTrue(usable)

    def test_timeout_diagnostic_exception_does_not_mask_primary_error(self) -> None:
        thread, _, release = self._thread_holder()
        try:
            with patch.object(
                state_lock_diagnostics,
                "timeout",
                side_effect=RuntimeError("diagnostic failed"),
            ):
                with self.assertRaises(state_store.StateLockTimeout) as raised:
                    with state_store.exclusive_file_lock(
                        self.target,
                        timeout_seconds=0.05,
                        poll_seconds=0.01,
                    ):
                        pass
            self.assertTrue(raised.exception.diagnostics["unavailable"])
        finally:
            release.set()
            thread.join(2)

    def test_retry_after_timeout_succeeds(self) -> None:
        thread, _, release = self._thread_holder()
        with self.assertRaises(state_store.StateLockTimeout):
            with state_store.exclusive_file_lock(
                self.target,
                timeout_seconds=0.05,
                poll_seconds=0.01,
                operation="first_waiter",
            ):
                pass
        release.set()
        thread.join(2)
        with state_store.exclusive_file_lock(
            self.target,
            timeout_seconds=0.5,
            poll_seconds=0.01,
            operation="retry_waiter",
        ):
            pass

    def test_backup_style_external_holder_is_detected(self) -> None:
        pid, release_write = self._fork_raw_holder(copy_and_fsync=True)
        try:
            with self.assertRaises(state_store.StateLockTimeout) as raised:
                with state_store.exclusive_file_lock(
                    self.target,
                    timeout_seconds=0.08,
                    poll_seconds=0.01,
                    operation="startup_migration",
                ):
                    pass
            self.assertEqual(raised.exception.diagnostics["holder_pid"], pid)
            self.assertEqual(
                raised.exception.diagnostics["holder_operation"],
                "external_or_unknown",
            )
        finally:
            self._finish_child(pid, release_write)


if __name__ == "__main__":
    unittest.main()
