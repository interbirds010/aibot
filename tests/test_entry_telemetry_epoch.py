"""임시 Windows 경로에서 epoch, 상태 연속성, cutover fail-closed를 검증한다."""
from __future__ import annotations
import hashlib
import os
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest
from unittest import mock

from src.research import entry_telemetry_epoch as epoch
from src.research.n3_shadow import immutable, read_immutable, digest
from src.state_store import atomic_write_json
from src.state_store import exclusive_file_lock, StateLockTimeout
from scripts import entry_telemetry_cutover as cutover


class EpochTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="epoch path ")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.proof = {"runtime_root": str(self.root.resolve()), "active_process_count": 0}
        self.config = {"paper_buy_basis_points": 50}
        self.build = {"git_sha": "a" * 40, "source_digest": "b" * 64, "source_files": {"src/monitor.py": "c" * 64}}
        self.provenance = {"git_sha": self.build["git_sha"], "config_fingerprint": digest(self.config)}
        patcher = mock.patch.object(epoch, "build_identity", return_value=self.build)
        patcher.start()
        self.addCleanup(patcher.stop)
        side = self.root / "data/n3_shadow"
        immutable(side / "manifest.json", {"cohort_id": "n3-cohort"})
        closure = {"cohort_id": "n3-cohort", "end_event_seq": 15}
        immutable(side / "closure.json", closure)
        immutable(side / "final_report.json", {"cohort_id": "n3-cohort", "closure": closure})
        atomic_write_json(side / "observer_state.json", {"cohort_id": "n3-cohort", "status": "CLOSED", "cursor": 15})
        atomic_write_json(self.root / "data/paper_trades.json", {"schema_version": 2, "next_event_seq": 16, "positions": {}})
        epoch.create_snapshot(self.root, stopped_proof=self.proof)

    def create(self, **values):
        return epoch.create_epoch(self.root, config=self.config, stopped_proof=self.proof, **values)

    def test_immutable_marker_restart_same_epoch(self):
        marker = self.create()
        path = epoch.directory(self.root) / "epochs" / marker["telemetry_epoch_id"] / "epoch.json"
        before = path.read_bytes()
        self.assertEqual(self.create(), marker)
        self.assertEqual(epoch.require_epoch(self.root, config=self.config), marker)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(marker["start_event_seq"], 16)
        self.assertIn("+09:00", marker["start_kst"])
        with self.assertRaises(RuntimeError):
            immutable(path, {**marker, "start_event_seq": 17})

    def test_new_activation_has_new_id_preserves_old(self):
        first = self.create()
        second = self.create(new_activation=True)
        self.assertNotEqual(first["telemetry_epoch_id"], second["telemetry_epoch_id"])
        path = epoch.directory(self.root) / "epochs" / first["telemetry_epoch_id"] / "epoch.json"
        self.assertEqual(read_immutable(path), first)
        self.assertEqual(epoch.require_epoch(self.root, config=self.config), second)

    def test_build_config_schema_platform_mismatch(self):
        self.create()
        for patcher, config, schemas in (
                (mock.patch.object(epoch, "build_identity", return_value={**self.build, "source_digest": "changed"}), self.config, epoch.SCHEMAS),
                (mock.patch.object(epoch, "build_identity", return_value=self.build), {"paper_buy_basis_points": 51}, epoch.SCHEMAS),
                (mock.patch.object(epoch, "build_identity", return_value=self.build), self.config, {"predictor": 1}),
                (mock.patch.object(epoch.platform, "platform", return_value="other-os"), self.config, epoch.SCHEMAS)):
            with patcher, self.assertRaises(RuntimeError):
                epoch.require_epoch(self.root, config=config, schemas=schemas)

    def test_config_rejects_unknown_secret_and_nonfinite(self):
        for config in ({"api_key": "secret"}, {"paper_buy_basis_points": "https://secret"}, {"paper_buy_basis_points": float("nan")}, {"paper_buy_basis_points": True}):
            with self.assertRaises(RuntimeError):
                epoch.create_epoch(self.root, config=config, stopped_proof=self.proof)
        self.assertFalse((epoch.directory(self.root) / "active.json").exists())

    def test_missing_sha_refuses_activation(self):
        with mock.patch.object(epoch, "build_identity", return_value={**self.build, "git_sha": None}), self.assertRaises(RuntimeError):
            self.create()

    def test_legacy_runtime_rows_refuses_conversion(self):
        path = epoch.directory(self.root) / "rows" / "legacy.json"
        atomic_write_json(path, {"schema_version": 1, "predictors": {}, "execution_receipt": {}})
        before = path.read_bytes()
        with self.assertRaises(RuntimeError):
            self.create()
        self.assertEqual(path.read_bytes(), before)

    def test_session_binding_restart_preserves_epoch(self):
        marker = self.create()
        first = epoch.register_session(self.root, marker, "first-session", 123, self.provenance)
        self.assertEqual(epoch.register_session(self.root, marker, "first-session", 123, self.provenance), first)
        second = epoch.register_session(self.root, marker, "second-session", 124, self.provenance)
        self.assertEqual(first["telemetry_epoch_id"], second["telemetry_epoch_id"])
        with self.assertRaises(RuntimeError):
            epoch.register_session(self.root, marker, "first-session", 999, self.provenance)

    def test_active_n3_or_processes_refuses_snapshot_and_epoch(self):
        with self.assertRaises(RuntimeError):
            epoch.create_snapshot(self.root, stopped_proof={**self.proof, "active_process_count": 1})
        atomic_write_json(self.root / "data/n3_shadow/observer_state.json", {"status": "RUNNING"})
        with self.assertRaises(RuntimeError):
            self.create()

    def test_registered_epoch_restart_refuses_reopened_n3(self):
        self.create()
        atomic_write_json(self.root / "data/n3_shadow/observer_state.json", {"cohort_id": "n3-cohort", "status": "RUNNING", "cursor": 15})
        with self.assertRaises(RuntimeError):
            epoch.require_epoch(self.root, config=self.config)

    def test_snapshot_exact_bytes_and_nested_n3_continuity(self):
        marker = epoch.validate_snapshot(self.root)
        source = self.root / "data/paper_trades.json"
        copy = epoch.directory(self.root) / "snapshots" / marker["snapshot_id"] / "bytes" / (hashlib.sha256("paper_trades.json".encode()).hexdigest() + ".bin")
        self.assertEqual(source.read_bytes(), copy.read_bytes())
        self.assertIn(str(Path("n3_shadow") / "manifest.json"), marker["state_hashes"])
        atomic_write_json(source, {"schema_version": 2, "next_event_seq": 17})
        with self.assertRaises(RuntimeError):
            epoch.validate_snapshot(self.root)

    def test_torn_marker_and_copied_bytes_detected(self):
        marker = self.create()
        path = epoch.directory(self.root) / "epochs" / marker["telemetry_epoch_id"] / "epoch.json"
        path.write_bytes(b'{"partial":')
        with self.assertRaises(ValueError):
            epoch.require_epoch(self.root, config=self.config)

    def test_corrupt_backup_detected_even_when_live_state_unchanged(self):
        marker = epoch.validate_snapshot(self.root)
        copy = next((epoch.directory(self.root) / "snapshots" / marker["snapshot_id"] / "bytes").glob("*.bin"))
        copy.write_bytes(b"corrupt")
        with self.assertRaises(RuntimeError):
            epoch.validate_snapshot(self.root)

    def test_snapshot_reopen_second_snapshot_keeps_previous_artifact(self):
        first = epoch.validate_snapshot(self.root)
        second = epoch.create_snapshot(self.root, stopped_proof=self.proof)
        self.assertNotEqual(first["snapshot_id"], second["snapshot_id"])
        self.assertEqual(epoch.validate_snapshot(self.root), second)
        self.assertEqual(read_immutable(epoch.directory(self.root) / "snapshots" / first["snapshot_id"] / "snapshot.json"), first)

    def test_epoch_reopen_ignores_stale_empty_lock_file(self):
        first = self.create()
        pointer = epoch.directory(self.root) / "active.json"
        pointer.with_name(pointer.name + ".lock").write_bytes(b"\0")
        self.assertEqual(self.create(new_activation=True)["start_event_seq"], first["start_event_seq"])

    def test_actual_process_restart_binds_new_session_without_epoch_mutation(self):
        marker = self.create()
        path = epoch.directory(self.root) / "epochs" / marker["telemetry_epoch_id"] / "epoch.json"
        before = path.read_bytes()
        source = ("import os,sys;from pathlib import Path;from src.research import entry_telemetry_epoch as e;"
                  "from src.research.n3_shadow import read_immutable;"
                  "m=read_immutable(Path(sys.argv[2]));e.register_session(Path(sys.argv[1]),m,sys.argv[3],os.getpid(),{'git_sha':m['build_sha'],'config_fingerprint':m['config_fingerprint']})")
        for session in ("process-restart-one", "process-restart-two"):
            result = subprocess.run([sys.executable, "-c", source, str(self.root), str(path), session],
                                    capture_output=True, text=True, timeout=20)
            self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(path.read_bytes(), before)
        sessions = list((path.parent / "sessions").glob("*.json"))
        self.assertEqual(len(sessions), 2)
        self.assertEqual({read_immutable(value)["telemetry_epoch_id"] for value in sessions}, {marker["telemetry_epoch_id"]})

    def test_os_file_lock_release_after_writer_process_exit(self):
        lock = epoch.directory(self.root) / "restart-lock"
        source = ("import sys,time;from pathlib import Path;from src.state_store import exclusive_file_lock;"
                  "ctx=exclusive_file_lock(Path(sys.argv[1]));ctx.__enter__();print('locked',flush=True);time.sleep(30)")
        child = subprocess.Popen([sys.executable, "-c", source, str(lock)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertEqual(child.stdout.readline().strip(), "locked")
            with self.assertRaises(StateLockTimeout):
                with exclusive_file_lock(lock, timeout_seconds=0.1, poll_seconds=0.01):
                    self.fail("Child lock must exclude another process")
        finally:
            child.terminate()
            child.wait(timeout=10)
            child.stdout.close()
            child.stderr.close()
        with exclusive_file_lock(lock, timeout_seconds=1):
            self.assertTrue(lock.with_name(lock.name + ".lock").exists())

    def test_clean_stop_refuses_unbound_process_no_force_fallback(self):
        marker = self.create()
        row = {"pid": 123, "created": "created", "executable": "python.exe", "command": "monitor.py"}
        with mock.patch.object(epoch, "require_epoch", return_value=marker), \
                mock.patch.object(cutover.runner, "read_json", return_value={"services": {"monitor": row}}), \
                mock.patch.object(cutover.runner, "process_snapshot", return_value=[row]), \
                mock.patch.object(cutover.runner, "terminate_record") as terminate:
            with self.assertRaises(RuntimeError):
                cutover.clean_stop(self.root, Path("python.exe"), timeout=0)
        terminate.assert_not_called()

    def test_clean_stop_timeout_preserves_control_process(self):
        marker = self.create()
        epoch.register_session(self.root, marker, "session", 123, self.provenance)
        row = {"pid": 123, "created": "created", "executable": "python.exe", "command": "monitor.py"}
        with mock.patch.object(epoch, "require_epoch", return_value=marker), \
                mock.patch.object(cutover.runner, "read_json", return_value={"services": {"monitor": row}}), \
                mock.patch.object(cutover.runner, "process_snapshot", return_value=[row]), \
                mock.patch.object(cutover.runner, "process_creation", return_value="created"), \
                mock.patch.object(cutover.runner, "terminate_record") as terminate:
            with self.assertRaises(RuntimeError):
                cutover.clean_stop(self.root, Path("python.exe"), timeout=0)
        terminate.assert_not_called()

    def test_missing_epoch_refuses_worker_registration(self):
        with self.assertRaises(RuntimeError):
            epoch.require_epoch(self.root, config=self.config)

    def test_marker_session_config_mismatch(self):
        marker = self.create()
        with self.assertRaises(RuntimeError):
            epoch.register_session(self.root, marker, "session", 123, {**self.provenance, "config_fingerprint": "changed"})

    def test_missing_git_provenance_refuses_valid_epoch_start(self):
        marker = self.create()
        with self.assertRaises(RuntimeError):
            epoch.require_epoch(self.root, config=self.config, provenance={**self.provenance, "git_sha": None})
        with self.assertRaises(RuntimeError):
            epoch.register_session(self.root, marker, "session", 123, {**self.provenance, "git_sha": None})

    def test_resume_never_starts_active_or_changed_state(self):
        with mock.patch.object(cutover, "stopped_proof", side_effect=RuntimeError("active")), mock.patch.object(cutover.runner, "manage") as start:
            with self.assertRaises(RuntimeError):
                cutover.resume(self.root, Path("python.exe"))
        start.assert_not_called()

    def test_stop_request_pid_creation_session_and_ack(self):
        from scripts import local_paper_runner as runner
        request = {"request_id": "request", "process_id": os.getpid(), "created": "created", "session_id": "session"}
        path = epoch.stop_request_path(self.root, "monitor", os.getpid(), "created", "session")
        immutable(path, request)
        with mock.patch.object(epoch.os, "name", "nt"), mock.patch.object(runner, "process_creation", return_value="created"):
            self.assertTrue(epoch.stop_requested(self.root, "monitor", session_id="session"))
            self.assertFalse(epoch.stop_requested(self.root, "monitor", session_id="other"))
            epoch.acknowledge_stopped(self.root, "monitor", session_id="session", drained=True)
        self.assertTrue(read_immutable(epoch.directory(self.root) / "stop_acknowledgements/request.json")["drained"])

    def test_snapshot_forbidden_symlink_and_limits(self):
        with mock.patch.object(Path, "is_symlink", return_value=True), self.assertRaises(RuntimeError):
            epoch.state_hashes(self.root)

    def test_cli_no_execute_never_mutates(self):
        with mock.patch("sys.argv", ["cutover", "create-epoch", "--root", str(self.root)]), mock.patch.object(epoch, "create_epoch") as create:
            with self.assertRaises(SystemExit):
                cutover.main()
        create.assert_not_called()


if __name__ == "__main__":
    unittest.main()
