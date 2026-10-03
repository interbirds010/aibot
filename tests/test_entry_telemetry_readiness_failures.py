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
        self.directory = self.root / "data/research/entry_telemetry/epochs/offline-epoch"
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for name, value in {
            "_root": self.root, "_queue": queue.Queue(maxsize=16),
            "_epoch": {"telemetry_epoch_id": "offline-epoch", "start_utc": STAMP, "start_event_seq": 0, "build": {"source_digest": "offline-source"}},
            "_receipt_queue": queue.Queue(maxsize=16), "_outcome_queue": queue.Queue(maxsize=16),
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
            stack.enter_context(patch.object(telemetry, "_load_epoch", return_value=telemetry._epoch))
            stack.enter_context(patch.object(telemetry, "_receipt_queue", queue.Queue(maxsize=16)))
            stack.enter_context(patch.object(telemetry, "_build_provenance", return_value={"session_id": "test"}))
            if persist_effect is not None:
                stack.enter_context(patch.object(telemetry, "_persist_stream", side_effect=persist_effect))
            with self.assertRaises(StopWorker):
                telemetry._run({})
        self.assertEqual(rows.done, int(capture is not None))

    def health(self):
        return json.loads((self.directory / "health/monitor.json").read_text(encoding="utf-8"))

    def test_missing_directory_is_created_by_actual_worker(self):
        self.assertFalse(self.directory.exists())
        self.run_one(self.capture())
        self.assertEqual(len(list((self.directory / "predictors/2026-10-03").glob("*.json"))), 1)
        self.assertEqual(self.health()["status"], "TELEMETRY_READY")

    def test_permission_failure_is_dropped_and_published_degraded(self):
        self.run_one(self.capture(), PermissionError("injected permission failure"))
        health = self.health()
        self.assertEqual(health["status"], "TELEMETRY_DEGRADED")
        self.assertEqual(health["write_error_count"], 1)
        self.assertEqual(health["dropped_row_count"], 1)
        self.assertEqual(health["last_error"], "record_write_error")

    def test_actual_row_write_failure_keeps_identity_reservation_after_restart(self):
        original = telemetry.atomic_write_json
        def fail_row(path, document):
            if path.parent.name == "2026-10-03":
                raise PermissionError("injected row write failure")
            return original(path, document)
        with patch.object(telemetry, "atomic_write_json", side_effect=fail_row):
            self.run_one(self.capture())
        index = next((self.directory / "predictors/_identity").glob("*/*.json"))
        before = index.read_bytes()
        self.run_one(None)
        self.assertEqual(index.read_bytes(), before)
        self.assertEqual(json.loads(before)["index_schema_version"], 1)

    def test_malformed_existing_row_is_preserved_and_fails_closed(self):
        capture = self.capture()
        path = self.directory / "predictors/2026-10-03" / (capture.identity["signal_id"] + ".json")
        path.parent.mkdir(parents=True)
        partial = b'{"schema_version":1,"identity":'
        path.write_bytes(partial)
        self.run_one(capture)
        self.assertEqual(path.read_bytes(), partial)
        self.assertEqual(self.health()["dropped_row_count"], 1)
        self.assertEqual(self.health()["last_error"], "immutable_content_corrupt")

    def test_valid_json_with_invalid_seal_is_preserved_and_reports_conflict(self):
        capture = self.capture()
        path = self.directory / "predictors/2026-10-03" / (capture.identity["signal_id"] + ".json")
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
        capture.identity["mint"] = object()
        self.run_one(capture)
        self.assertEqual(list((self.directory / "predictors/2026-10-03").glob("*.json")), [])
        self.assertEqual(self.health()["dropped_row_count"], 1)

    def test_queue_full_preserves_finished_buy_and_reject_capture(self):
        for index in range(16):
            self.capture(str(index))
        buy = self.capture("overflow-buy")
        reject = self.capture("overflow-reject", "REJECT_ANALYZER")
        self.assertEqual(telemetry._queue.qsize(), 16)
        self.assertEqual(telemetry._health["streams"]["predictors"]["dropped_row_count"], 2)
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
        self.assertEqual(telemetry._health["streams"]["predictors"]["dropped_row_count"], 2)

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
        self.assertEqual(len(list((self.directory / "predictors/2026-10-03").glob("*.json"))), 1)
        self.assertTrue(lock.exists())

    def test_obsolete_storage_budget_is_preserved_and_not_used(self):
        path = self.directory / "predictors/storage.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        before = b'{"version":'
        path.write_bytes(before)
        self.run_one(self.capture())
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(self.health()["write_error_count"], 0)
        self.assertEqual(self.health()["dropped_row_count"], 0)
        self.assertEqual(len(list((self.directory / "predictors/2026-10-03").glob("*.json"))), 1)

    def test_atomic_replace_failure_leaves_no_partial_final_row_or_temp_file(self):
        capture = self.capture()
        original = state_store.os.replace
        def fail_row_replace(source, destination):
            if Path(destination).parent.name == "2026-10-03":
                raise PermissionError("injected Windows replace denial")
            return original(source, destination)
        with patch.object(state_store.os, "replace", side_effect=fail_row_replace):
            self.run_one(capture)
        self.assertFalse(list((self.directory / "predictors/2026-10-03").glob("*.json")))
        self.assertFalse(list((self.directory / "predictors/2026-10-03").glob("*.tmp")))
        self.assertEqual(self.health()["dropped_row_count"], 1)

    def test_stranded_partial_temporary_file_is_not_treated_as_complete_row(self):
        rows = self.directory / "predictors/2026-10-03"
        rows.mkdir(parents=True)
        fragment = rows / ".offline.json.crash.tmp"
        fragment.write_bytes(b'{"schema_version":')
        self.run_one(self.capture())
        self.assertEqual(len(list(rows.glob("*.json"))), 1)
        self.assertEqual(len(list((self.directory / "predictors/_identity").glob("*/*.json"))), 1)
        # 다른 이름의 임시 파일은 보존되며 정상 row로 읽지 않는다.
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
t._epoch={"telemetry_epoch_id":"offline-epoch","start_utc":"2026-10-03T00:00:00+00:00","start_event_seq":0}
for mint in sys.argv[2:]:
    c=t.begin_signal(mint=mint,route_type="A",signal_detected_at="2026-10-03T00:00:00+00:00")
    t.finish(c,outcome="BUY",trade_id="POSITION"); t._persist(c)
print(json.dumps({"session":t._session,"schema":t.SCHEMA_VERSION,"duplicate":t._health["duplicate_count"]}))
'''
        first = self.child(code, str(self.root), "first")
        rows = self.directory / "predictors/2026-10-03"
        first_path = next(rows.glob("*.json"))
        before = first_path.read_bytes()
        second = self.child(code, str(self.root), "first", "second")
        self.assertNotEqual(first["session"], second["session"])
        self.assertEqual(first["schema"], second["schema"])
        self.assertEqual(second["duplicate"], 2)
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

    def test_predictor_and_receipt_write_failures_are_independent(self):
        original = telemetry._persist_stream
        for failed, succeeding in (("predictors", "receipts"), ("receipts", "predictors")):
            with self.subTest(failed=failed):
                capture = self.capture("independent-" + failed)
                def write(stream, document):
                    if stream == failed:
                        raise PermissionError("offline stream denied")
                    return original(stream, document)
                with patch.object(telemetry, "_persist_stream", side_effect=write):
                    telemetry._persist(capture)
                builder = telemetry._row if succeeding == "predictors" else telemetry._receipt_row
                self.assertTrue(telemetry._record_path(succeeding, builder(capture)).exists())
                builder = telemetry._row if failed == "predictors" else telemetry._receipt_row
                self.assertFalse(telemetry._record_path(failed, builder(capture)).exists())
                self.assertEqual(telemetry._health["streams"][failed]["write_error_count"], 1)

    def test_outcome_failure_does_not_change_predictor_or_receipt(self):
        capture = self.capture()
        telemetry._persist(capture)
        paths = [telemetry._record_path(stream, builder(capture)) for stream, builder in
            (("predictors", telemetry._row), ("receipts", telemetry._receipt_row))]
        before = [path.read_bytes() for path in paths]
        value = {"trade_id": "POSITION", "mint": "MINT", "signal_detected_at": STAMP, "buy_event_seq": 1}
        with patch.object(telemetry, "_persist_stream", side_effect=PermissionError("outcome denied")):
            telemetry._write_stream("outcomes", value)
        self.assertEqual([path.read_bytes() for path in paths], before)
        self.assertEqual(telemetry._health["streams"]["outcomes"]["write_error_count"], 1)

    def test_predictor_queue_full_still_enqueues_receipt(self):
        telemetry._queue = queue.Queue(maxsize=1)
        telemetry.finish(telemetry.begin_signal(mint="filled", route_type="A", signal_detected_at=STAMP))
        capture = self.capture("buy-survives")
        self.assertTrue(capture.finished)
        self.assertEqual(telemetry._receipt_queue.qsize(), 1)
        self.assertEqual(telemetry._health["streams"]["predictors"]["dropped_row_count"], 1)
        self.assertNotIn("receipts", telemetry._health["streams"])

    def test_disabled_epoch_collects_no_queued_rows_and_launch_error_cannot_raise(self):
        with patch.object(telemetry, "_epoch", None):
            self.capture()
            self.assertEqual(telemetry._queue.qsize(), 0)
            self.assertEqual(telemetry._receipt_queue.qsize(), 0)
        with patch.object(telemetry, "_load_epoch", side_effect=ValueError("invalid epoch")):
            telemetry._run({})
        self.assertFalse(telemetry.is_enabled())
        self.assertEqual(telemetry._health["status"], "TELEMETRY_DISABLED")

    def test_role_health_publishers_keep_both_process_counters(self):
        for role in ("monitor", "risk-manager"):
            with patch.object(telemetry, "_role", role):
                telemetry._publish_health()
        self.assertEqual({path.name for path in (self.directory / "health").glob("*.json")},
                         {"monitor.json", "risk-manager.json"})

    def test_pre_epoch_signal_and_legacy_buy_receipt_are_excluded(self):
        with patch.object(telemetry, "_epoch", {"telemetry_epoch_id": "offline-epoch",
                "start_utc": "2026-10-03T00:00:01+00:00", "start_event_seq": 10}):
            self.assertIsNone(telemetry.begin_signal(mint="legacy", route_type="A", signal_detected_at=STAMP))
            self.assertEqual(telemetry._health["last_error"], "signal_before_epoch_start")
        capture = self.capture()
        capture.receipt["event_seq"] = 9
        with patch.object(telemetry, "_epoch", {"telemetry_epoch_id": "offline-epoch",
                "start_utc": STAMP, "start_event_seq": 10}):
            telemetry._write_stream("receipts", capture)
        self.assertFalse(telemetry._record_path("receipts", telemetry._receipt_row(capture)).exists())
        self.assertEqual(telemetry._health["last_error"], "record_outside_epoch")

    def test_startup_validates_epoch_before_starting_worker_thread(self):
        order = []
        def initialize(config):
            order.append("validate")
            return True
        with patch.object(telemetry, "_worker", None), patch.object(telemetry, "_initialize", side_effect=initialize), \
                patch.object(telemetry.threading, "Thread") as thread:
            thread.return_value.start.side_effect = lambda: order.append("start")
            telemetry.start_worker(self.root, {})
            self.assertEqual(order, ["validate", "start"])
            self.assertEqual(thread.call_args.kwargs["args"], ({}, True))

    def test_startup_invalid_epoch_keeps_control_usable_and_does_not_launch_writer(self):
        with patch.object(telemetry, "_worker", None), patch.object(telemetry, "_initialize", return_value=False), \
                patch.object(telemetry.threading, "Thread") as thread:
            telemetry.start_worker(self.root, {})
            thread.assert_not_called()

    def test_health_snapshot_nested_counters_do_not_change_after_lock_release(self):
        telemetry._degrade("offline_injected", stream="predictors")
        real = telemetry.atomic_write_json
        def concurrent_update(path, document):
            telemetry._health["streams"]["predictors"]["write_error_count"] = 10
            return real(path, document)
        with patch.object(telemetry, "atomic_write_json", side_effect=concurrent_update):
            telemetry._publish_health()
        self.assertEqual(self.health()["streams"]["predictors"]["write_error_count"], 0)
        self.assertEqual(telemetry._health["streams"]["predictors"]["write_error_count"], 10)


if __name__ == "__main__":
    unittest.main()
