"""임시 저장소에서 recorder 장애와 재시작을 검증한다. 운영 데이터는 읽지 않는다."""
from __future__ import annotations

import asyncio
from contextlib import ExitStack
import json
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from src import monitor, state_store
from src.research import entry_telemetry as telemetry

STAMP = "2026-10-03T00:00:00+00:00"


class StopWorker(BaseException):
    """실제 worker 루프를 한 행 처리 뒤 테스트에서 종료한다."""


class SingleRowQueue:
    maxsize = 16

    def __init__(self, capture=None):
        self.capture = capture
        self.done = 0

    def get(self, **kwargs):
        if self.capture is None:
            raise StopWorker()
        capture, self.capture = self.capture, None
        return capture

    def task_done(self):
        self.done += 1

    def qsize(self):
        return int(self.capture is not None)


class EntryTelemetryReadinessFailureTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.directory = self.root / "data/research/entry_telemetry"
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for name, value in {
            "_root": self.root, "_queue": queue.Queue(maxsize=16),
            "_provenance": {"session_id": "test", "git_sha": "offline"},
            "_health": {"status": "TELEMETRY_READY", "dropped_row_count": 0,
                        "write_error_count": 0, "duplicate_count": 0,
                        "conflict_count": 0, "last_error": None},
        }.items():
            self.stack.enter_context(patch.object(telemetry, name, value))

    def capture(self, mint="MINT", outcome="BUY"):
        capture = telemetry.begin_signal(mint=mint, route_type="A", signal_detected_at=STAMP)
        telemetry.finish(capture, outcome=outcome,
                         trade_id="POSITION" if outcome == "BUY" else None)
        return capture

    def run_one(self, capture, persist_effect=None):
        rows = SingleRowQueue(capture)
        with ExitStack() as stack:
            stack.enter_context(patch.object(telemetry, "_queue", rows))
            stack.enter_context(patch.object(telemetry, "_build_provenance", return_value={"session_id": "test"}))
            if persist_effect is not None:
                stack.enter_context(patch.object(telemetry, "_persist", side_effect=persist_effect))
            with self.assertRaises(StopWorker):
                telemetry._run({})
        self.assertEqual(rows.done, int(capture is not None))

    def health(self):
        return json.loads((self.directory / "health.json").read_text(encoding="utf-8"))

    def test_missing_directory_is_created_by_actual_worker(self):
        self.assertFalse(self.directory.exists())
        self.run_one(self.capture())
        self.assertEqual(len(list((self.directory / "rows").glob("*.json"))), 1)
        self.assertEqual(self.health()["status"], "TELEMETRY_READY")

    def test_permission_failure_is_dropped_and_published_degraded(self):
        self.run_one(self.capture(), PermissionError("injected permission failure"))
        health = self.health()
        self.assertEqual(health["status"], "TELEMETRY_DEGRADED")
        self.assertEqual(health["write_error_count"], 1)
        self.assertEqual(health["dropped_row_count"], 1)
        self.assertEqual(health["last_error"], "record_write_error")

    def test_actual_row_write_failure_keeps_reserved_budget_until_restart(self):
        original = telemetry.atomic_write_json
        def fail_row(path, document):
            if path.parent.name == "rows":
                raise PermissionError("injected row write failure")
            return original(path, document)
        with patch.object(telemetry, "atomic_write_json", side_effect=fail_row):
            self.run_one(self.capture())
        self.assertEqual(json.loads((self.directory / "storage.json").read_text())["row_count"], 1)
        self.run_one(None)
        self.assertEqual(json.loads((self.directory / "storage.json").read_text())["row_count"], 0)

    def test_malformed_existing_row_is_preserved_and_fails_closed(self):
        capture = self.capture()
        path = self.directory / "rows" / (capture.identity["signal_id"] + ".json")
        path.parent.mkdir(parents=True)
        partial = b'{"schema_version":1,"identity":'
        path.write_bytes(partial)
        self.run_one(capture)
        self.assertEqual(path.read_bytes(), partial)
        self.assertEqual(self.health()["dropped_row_count"], 1)
        self.assertEqual(self.health()["last_error"], "record_write_error")

    def test_valid_json_with_invalid_seal_is_preserved_and_reports_conflict(self):
        capture = self.capture()
        path = self.directory / "rows" / (capture.identity["signal_id"] + ".json")
        path.parent.mkdir(parents=True)
        path.write_text('{"content_hash":"wrong"}', encoding="utf-8")
        before = path.read_bytes()
        self.run_one(capture)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(self.health()["conflict_count"], 1)
        self.assertEqual(self.health()["last_error"], "immutable_content_corrupt")

    def test_writer_exception_does_not_escape_worker_loop(self):
        self.run_one(self.capture(), RuntimeError("injected writer exception"))
        self.assertEqual(self.health()["write_error_count"], 1)

    def test_serialization_failure_is_dropped_without_partial_row(self):
        capture = self.capture()
        capture.identity["offline_invalid_value"] = object()
        self.run_one(capture)
        self.assertEqual(list((self.directory / "rows").glob("*.json")), [])
        self.assertEqual(self.health()["dropped_row_count"], 1)

    def test_queue_full_preserves_finished_buy_and_reject_capture(self):
        for index in range(16):
            self.capture(str(index))
        buy = self.capture("overflow-buy")
        reject = self.capture("overflow-reject", "REJECT_ANALYZER")
        self.assertEqual(telemetry._queue.qsize(), 16)
        self.assertEqual(telemetry._health["dropped_row_count"], 2)
        self.assertTrue(buy.finished and reject.finished)
        self.assertEqual(buy.sections["decision"]["outcome"], "BUY")
        self.assertEqual(reject.sections["decision"]["outcome"], "REJECT_ANALYZER")

    def test_monitor_control_outcomes_survive_queue_overflow(self):
        for index in range(16):
            self.capture(str(index))
        executed = []
        async def control(*args, **kwargs):
            outcome = "BUY" if args[0] == "buy" else "REJECT_ANALYZER"
            executed.append(outcome)
            monitor._entry_telemetry_decision(outcome, [])
            if outcome == "BUY":
                telemetry.mark("paper_buy_created_at", trade_id="POSITION")
        with patch.object(monitor, "_process_paper_signal_control", side_effect=control):
            for mint in ("buy", "reject"):
                asyncio.run(monitor.process_paper_signal(mint, 1000, 6, 2_000_000_000,
                                                       "WALLET", "SIGNATURE", STAMP))
        self.assertEqual(executed, ["BUY", "REJECT_ANALYZER"])
        self.assertEqual(telemetry._health["dropped_row_count"], 2)

    def test_health_write_failure_is_reported_in_memory_without_exception(self):
        with patch.object(telemetry, "_publish_health", side_effect=PermissionError("health failure")):
            self.run_one(self.capture())
        self.assertEqual(telemetry._health["status"], "TELEMETRY_DEGRADED")
        self.assertEqual(telemetry._health["last_error"], "health_write_error")
        self.assertEqual(telemetry._health["dropped_row_count"], 0)

    def test_existing_stale_lock_file_has_no_live_ownership(self):
        self.directory.mkdir(parents=True)
        lock = self.directory / "recorder.lock"
        lock.write_bytes(b"\0stale file")
        telemetry._persist(self.capture())
        self.assertEqual(len(list((self.directory / "rows").glob("*.json"))), 1)
        self.assertTrue(lock.exists())

    def test_malformed_storage_budget_is_not_silently_reset(self):
        self.directory.mkdir(parents=True)
        path = self.directory / "storage.json"
        path.write_bytes(b'{"version":')
        self.run_one(self.capture())
        self.assertEqual(path.read_bytes(), b'{"version":')
        health = self.health()
        self.assertEqual(health["write_error_count"], 2)
        self.assertEqual(health["dropped_row_count"], 1)
        self.assertFalse(list((self.directory / "rows").glob("*.json")))

    def test_atomic_replace_failure_leaves_no_partial_final_row_or_temp_file(self):
        capture = self.capture()
        original = state_store.os.replace
        def fail_row_replace(source, destination):
            if Path(destination).parent.name == "rows":
                raise PermissionError("injected Windows replace denial")
            return original(source, destination)
        with patch.object(state_store.os, "replace", side_effect=fail_row_replace):
            self.run_one(capture)
        self.assertFalse(list((self.directory / "rows").glob("*.json")))
        self.assertFalse(list((self.directory / "rows").glob("*.tmp")))
        self.assertEqual(self.health()["dropped_row_count"], 1)

    def test_stranded_partial_temporary_file_is_not_treated_as_complete_row(self):
        rows = self.directory / "rows"
        rows.mkdir(parents=True)
        fragment = rows / ".offline.json.crash.tmp"
        fragment.write_bytes(b'{"schema_version":')
        self.run_one(self.capture())
        self.assertEqual(len(list(rows.glob("*.json"))), 1)
        self.assertEqual(json.loads((self.directory / "storage.json").read_text())["row_count"], 1)
        # 다른 이름의 최근 임시 파일은 보존되며 budget에는 포함하지 않는다.
        self.assertTrue(fragment.exists())

    def child(self, code, *args):
        result = subprocess.run([sys.executable, "-c", code, *args],
                                cwd=str(Path(__file__).resolve().parents[1]),
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_actual_process_restart_changes_session_and_preserves_duplicates_and_append(self):
        code = '''import json,sys
from pathlib import Path
from src.research import entry_telemetry as t
t._root=Path(sys.argv[1]); t._provenance=t._build_provenance({})
for mint in sys.argv[2:]:
    c=t.begin_signal(mint=mint,route_type="A",signal_detected_at="2026-10-03T00:00:00+00:00")
    t.finish(c,outcome="BUY",trade_id="POSITION"); t._persist(c)
print(json.dumps({"session":t._session,"schema":t.SCHEMA_VERSION,"duplicate":t._health["duplicate_count"]}))
'''
        first = self.child(code, str(self.root), "first")
        rows = self.directory / "rows"
        first_path = next(rows.glob("*.json"))
        before = first_path.read_bytes()
        second = self.child(code, str(self.root), "first", "second")
        self.assertNotEqual(first["session"], second["session"])
        self.assertEqual(first["schema"], second["schema"])
        self.assertEqual(second["duplicate"], 1)
        self.assertEqual(first_path.read_bytes(), before)
        documents = [json.loads(path.read_text(encoding="utf-8")) for path in rows.glob("*.json")]
        self.assertEqual(len(documents), 2)
        self.assertEqual({row["provenance"]["session_id"] for row in documents},
                         {first["session"], second["session"]})

    def test_crashed_process_releases_os_lock_without_deleting_lock_file(self):
        code = '''import json,os,sys
from pathlib import Path
from src.state_store import exclusive_file_lock
with exclusive_file_lock(Path(sys.argv[1])):
    print(json.dumps({"locked":True}),flush=True)
    os._exit(0)
'''
        self.assertTrue(self.child(code, str(self.directory / "recorder"))["locked"])
        with state_store.exclusive_file_lock(self.directory / "recorder", timeout_seconds=0.2):
            self.assertTrue((self.directory / "recorder.lock").exists())


if __name__ == "__main__":
    unittest.main()
