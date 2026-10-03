from __future__ import annotations

import asyncio
import contextvars
import copy
import json
import multiprocessing
from pathlib import Path
import queue
import tempfile
import time
import unittest
from unittest.mock import patch

from src.research import entry_telemetry as telemetry

STAMP = "2026-10-03T00:00:00+00:00"


def race_writer(root, start, connection):
    telemetry._root = Path(root)
    telemetry._epoch = {"telemetry_epoch_id": "offline-epoch", "start_utc": STAMP, "start_event_seq": 0, "build": {"source_digest": "offline-source"}}
    telemetry._provenance = {"session_id": "race"}
    capture = telemetry.begin_signal(mint="mint", route_type="B", signal_detected_at=STAMP)
    telemetry.finish(capture, outcome="BUY", trade_id="trade", event_seq=1)
    connection.send("READY")
    start.wait(30)
    try:
        telemetry._persist(capture)
        connection.send("OK")
    except Exception as error:
        connection.send(type(error).__name__)
    connection.close()


class EntryTelemetryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.patches = [patch.object(telemetry, "_root", self.root),
            patch.object(telemetry, "_epoch", {"telemetry_epoch_id": "offline-epoch", "start_utc": STAMP, "start_event_seq": 0, "build": {"source_digest": "offline-source"}}),
            patch.object(telemetry, "_load_epoch", return_value={"telemetry_epoch_id": "offline-epoch", "start_utc": STAMP, "start_event_seq": 0, "build": {"source_digest": "offline-source"}}),
            patch.object(telemetry, "_receipt_queue", queue.Queue(maxsize=16)),
            patch.object(telemetry, "_outcome_queue", queue.Queue(maxsize=16)),
            patch.object(telemetry, "_queue", queue.Queue(maxsize=16)),
            patch.object(telemetry, "_provenance", {"session_id": "test", "git_sha": "sha"}),
            patch.object(telemetry, "_health", {"status": "TELEMETRY_READY", "dropped_row_count": 0,
                "write_error_count": 0, "duplicate_count": 0, "conflict_count": 0, "last_error": None})]
        for item in self.patches:
            item.start()
        self.addCleanup(self.tmp.cleanup)
        for item in self.patches:
            self.addCleanup(item.stop)

    def capture(self, **kwargs):
        return telemetry.begin_signal(mint=kwargs.pop("mint", "mint"), route_type="B", signal_detected_at=STAMP, **kwargs)

    def write(self, capture):
        telemetry._persist(capture)
        return json.loads(next((self.root / "data/research/entry_telemetry/epochs/offline-epoch/predictors/2026-10-03").glob("*.json")).read_text(encoding="utf-8"))

    def test_signal_identity_is_deterministic_and_signature_is_hashed(self):
        a = self.capture(source_wallet="wallet", source_signature="signature")
        b = self.capture(source_wallet="wallet", source_signature="signature")
        self.assertEqual(a.identity, b.identity)
        self.assertNotEqual(a.identity["signal_id"], self.capture(source_signature="another").identity["signal_id"])
        self.assertNotIn("signature\"", json.dumps(a.identity))
        self.assertNotIn("wallet\"", json.dumps(a.identity))

    def test_missing_values_fixed_schema_not_synthetic_zero(self):
        capture = self.capture()
        self.assertIsNone(capture.counters["rpc_attempt_count"])
        self.assertIsNone(capture.timestamps["analysis_started_at"])
        self.assertIsNone(capture.sections["short_flow"]["windows"])
        self.assertEqual(capture.sections["short_flow"]["missing_reason"], "not_available_in_existing_flow")
        self.assertIsNone(capture.timestamps["signal_detected_at"]["monotonic_ns"])

    def test_large_identity_is_rejected_before_hash_encoding(self):
        for key in ("source_wallet", "source_signature", "signal_id"):
            with self.subTest(key=key), patch.object(telemetry.hashlib, "sha256") as digest:
                self.assertIsNone(self.capture(**{key: "x" * 100_000}))
                digest.assert_not_called()

    def test_partial_field_or_health_failure_does_not_count_as_dropped_row(self):
        telemetry._degrade("section_capture_error")
        telemetry._degrade("health_write_error", write=True)
        self.assertEqual(telemetry._health["dropped_row_count"], 0)
        self.assertEqual(telemetry._health["write_error_count"], 1)
        self.assertEqual(telemetry._health["degraded_event_count"], 2)

    def test_explicit_enqueue_clock_is_preserved_and_ordering_is_monotonic(self):
        capture = self.capture()
        with telemetry.bind(capture):
            telemetry.mark("signal_enqueued_at", wall_clock=STAMP, monotonic=12.25)
            telemetry.mark("analysis_started_at")
            telemetry.mark("analysis_completed_at")
        self.assertEqual(capture.timestamps["signal_enqueued_at"]["wall_utc"], STAMP)
        self.assertEqual(capture.timestamps["signal_enqueued_at"]["monotonic_ns"], 12250000000)
        self.assertLessEqual(capture.timestamps["analysis_started_at"]["monotonic_ns"],
            capture.timestamps["analysis_completed_at"]["monotonic_ns"])

    def test_predictors_freeze_at_decision_receipt_is_separate(self):
        capture = self.capture()
        with telemetry.bind(capture):
            telemetry.set_section("trajectory", {"price_now": 1})
            telemetry.mark("entry_decision_at")
            telemetry.set_section("trajectory", {"price_now": 999})
            telemetry.add_counter("rpc_attempt_count")
            telemetry.mark("paper_buy_created_at", trade_id="position")
            telemetry.set_section("decision", {"outcome": "BUY"})
        telemetry.finish(capture)
        row = self.write(capture)
        self.assertEqual(row["predictors"]["sections"]["trajectory"]["price_now"], 1)
        self.assertIsNone(row["predictors"]["counters"]["rpc_attempt_count"])
        self.assertNotIn("paper_buy_created_at", row["predictors"]["timestamps"])
        self.assertNotIn("decision", row["predictors"]["sections"])
        self.assertEqual(telemetry._receipt_row(capture)["execution_receipt"]["trade_id"], "position")
        self.assertEqual(row["decision"]["outcome"], "BUY")

    def test_future_and_secret_fields_recursively_rejected(self):
        capture = self.capture()
        with telemetry.bind(capture):
            telemetry.set_section("scores", {"raw_components": {"volume": 10, "nested": {
                "final_pnl": 12, "winner": True, "MFE": 20, "private_key": "bad"}},
                "future_horizon_return": 5})
            telemetry.set_section("rpc_errors", {"last_error_type": "https://host/?api_key=bad"})
        row = telemetry._row(capture)
        text = json.dumps(row)
        for forbidden in ("final_pnl", "winner", "MFE", "private_key", "api_key", "future_horizon_return"):
            self.assertNotIn(forbidden, text)
        self.assertEqual(capture.sections["scores"]["raw_components"]["volume"], 10)

    def test_capped_and_uncapped_raw_scores_counts_preserved(self):
        capture = self.capture()
        with telemetry.bind(capture):
            telemetry.set_section("scores", {"momentum": {"capped_total": 100, "uncapped_total": None,
                "uncapped_total_missing_reason": "upstream_not_exposed", "raw_components": {"volume": 25}}})
            telemetry.set_section("wallets", {"capped_whale_count": 3, "uncapped_whale_count": 7})
        self.assertEqual(capture.sections["wallets"]["uncapped_whale_count"], 7)
        self.assertIsNone(capture.sections["scores"]["momentum"]["uncapped_total"])

    def test_rejected_signal_uses_same_envelope_without_receipt_trade(self):
        capture = self.capture()
        telemetry.finish(capture, outcome="REJECT_ANALYZER")
        row = self.write(capture)
        self.assertEqual(row["decision"]["outcome"], "REJECT_ANALYZER")
        self.assertIsNone(row["identity"]["trade_id"])
        self.assertIsNone(row["identity"]["event_seq"])

    def test_actual_buy_receipt_cannot_be_reclassified_by_later_control_error(self):
        capture = self.capture()
        with telemetry.bind(capture):
            telemetry.mark("entry_decision_at")
            telemetry.mark("paper_buy_created_at", trade_id="position")
            telemetry.set_section("decision", {"outcome": "OTHER"})
        telemetry.finish(capture, outcome="OTHER")
        self.assertEqual(telemetry._row(capture)["decision"]["outcome"], "BUY")

    def test_prebuy_ledger_risk_rejection_after_decision_remains_available(self):
        capture = self.capture()
        with telemetry.bind(capture):
            telemetry.mark("entry_decision_at")
            telemetry.set_section("decision", {"outcome": "REJECT_RISK"})
        telemetry.finish(capture)
        self.assertEqual(telemetry._row(capture)["decision"]["outcome"], "REJECT_RISK")

    def test_recording_and_finish_do_no_disk_or_serialization(self):
        with patch.object(telemetry, "atomic_write_json", side_effect=AssertionError("disk")), \
                patch.object(telemetry, "_persist", side_effect=AssertionError("disk")):
            capture = self.capture()
            with telemetry.bind(capture):
                telemetry.mark("analysis_started_at")
                telemetry.duration("rpc_request_duration_sec", .1)
            with patch.object(telemetry, "_canonical", side_effect=AssertionError("serialization")):
                telemetry.finish(capture, outcome="BUY")
        self.assertEqual(telemetry._queue.qsize(), 1)

    def test_all_api_errors_are_isolated_by_safe_hook(self):
        for name in ("begin_signal", "mark", "set_section", "duration", "add_counter", "finish", "start_worker"):
            with patch.object(telemetry, name, side_effect=OSError("no output secret")):
                self.assertIsNone(telemetry.safe_hook(name))
        self.assertEqual(telemetry._health["last_error"], "hook_error")

    def test_queue_overflow_drops_only_telemetry(self):
        for index in range(17):
            telemetry.finish(self.capture(mint=str(index)), outcome="BUY", trade_id=str(index))
        self.assertEqual(telemetry._queue.qsize(), 16)
        self.assertEqual(telemetry._health["streams"]["predictors"]["dropped_row_count"], 1)
        self.assertEqual(telemetry._health["status"], "TELEMETRY_DEGRADED")
        self.assertEqual(telemetry._health["last_error"], "queue_full")

    def test_duplicate_is_idempotent_across_provenance_restart(self):
        capture = self.capture()
        telemetry.finish(capture, outcome="BUY", trade_id="trade", event_seq=1)
        first = self.write(capture)
        with patch.object(telemetry, "_provenance", {"session_id": "new-session"}):
            second = self.capture()
            telemetry.finish(second, outcome="BUY", trade_id="trade", event_seq=1)
            telemetry._persist(second)
        path = next((self.root / "data/research/entry_telemetry/epochs/offline-epoch/predictors/2026-10-03").glob("*.json"))
        self.assertEqual(json.loads(path.read_text(encoding="utf-8")), first)
        self.assertEqual(telemetry._health["streams"]["predictors"]["duplicate_count"], 1)
        self.assertEqual(len(list((path.parent.parent / "_identity").glob("*/*.json"))), 1)

    def test_conflicting_identity_receipt_never_overwrites(self):
        capture = self.capture()
        telemetry.finish(capture, outcome="BUY", trade_id="first", event_seq=1)
        first = self.write(capture)
        another = self.capture()
        telemetry.finish(another, outcome="BUY", trade_id="second", event_seq=2)
        self.assertEqual(self.write(another), first)
        self.assertEqual(telemetry._health["conflict_count"], 1)

    def test_corrupt_existing_record_is_not_silently_replaced(self):
        capture = self.capture()
        telemetry.finish(capture, outcome="BUY")
        self.write(capture)
        path = next((self.root / "data/research/entry_telemetry/epochs/offline-epoch/predictors/2026-10-03").glob("*.json"))
        row = json.loads(path.read_text(encoding="utf-8"))
        row["identity"]["mint"] = "changed"
        path.write_text(json.dumps(row), encoding="utf-8")
        telemetry._persist(capture)
        self.assertEqual(telemetry._health["last_error"], "immutable_content_corrupt")

    def test_large_row_dropped_without_creating_record(self):
        capture = self.capture()
        capture.sections["scores"]["raw_components"] = {"large": "x" * telemetry.MAX_ROW_BYTES}
        telemetry.finish(capture)
        with patch.object(telemetry, "MAX_ROW_BYTES", 1):
            telemetry._persist(capture)
        self.assertEqual(telemetry._health["last_error"], "row_too_large")
        self.assertFalse((self.root / "data").exists())

    def test_large_nested_inputs_nonfinite_values_are_bounded(self):
        capture = self.capture()
        with telemetry.bind(capture):
            telemetry.set_section("wallets", {"wallet_ids": list(range(2000))})
            telemetry.set_section("scores", {"raw_components": {"volume": float("nan"), "volume_m5_usd": "x" * 10000}})
        self.assertEqual(len(capture.sections["wallets"]["wallet_ids"]), telemetry.MAX_ITEMS)
        self.assertIsNone(capture.sections["scores"]["raw_components"]["volume"])
        self.assertEqual(len(capture.sections["scores"]["raw_components"]["volume_m5_usd"]), telemetry.MAX_STRING)

    def test_identity_reservation_failure_prevents_row_write(self):
        capture = self.capture()
        telemetry.finish(capture)
        with patch.object(telemetry, "atomic_write_json", side_effect=OSError("write")) as writer:
            telemetry._persist(capture)
        self.assertEqual(writer.call_count, 1)
        self.assertIn("_identity", writer.call_args.args[0].parts)
        self.assertFalse(telemetry._record_path("predictors", telemetry._row(capture)).exists())

    def test_row_failure_preserves_immutable_identity_reservation(self):
        capture = self.capture()
        telemetry.finish(capture)
        document = telemetry._row(capture)
        real = telemetry.atomic_write_json
        def writer(path, value):
            if path.parent.name == "2026-10-03":
                raise OSError("record failed")
            real(path, value)
        with patch.object(telemetry, "atomic_write_json", side_effect=writer):
            telemetry._persist(capture)
        index = telemetry._identity_index_path("predictors", document)
        before = index.read_bytes()
        reservation = json.loads(before)
        self.assertEqual(reservation["record_content_hash"], telemetry._digest(document))
        telemetry._persist(capture)
        self.assertEqual(index.read_bytes(), before)
        self.assertTrue(telemetry._record_path("predictors", document).exists())

    def test_old_epoch_storage_cap_metadata_does_not_limit_new_records(self):
        directory = self.root / "data/research/entry_telemetry/epochs/offline-epoch/predictors"
        directory.mkdir(parents=True)
        old = directory / "storage.json"
        old.write_text('{"version":1,"row_count":4096,"bytes":134217728}', encoding="utf-8")
        before = old.read_bytes()
        for mint in ("first", "second"):
            capture = self.capture(mint=mint)
            telemetry.finish(capture)
            telemetry._persist(capture)
        self.assertEqual(old.read_bytes(), before)
        self.assertEqual(len(list((directory / "2026-10-03").glob("*.json"))), 2)
        self.assertEqual(telemetry._health["dropped_row_count"], 0)

    def test_row_byte_bound_includes_actual_atomic_file_newline(self):
        capture = self.capture()
        telemetry.finish(capture)
        self.write(capture)
        row = telemetry._row(capture)
        document = {**row, "content_hash": telemetry._digest(row)}
        actual_size = telemetry._record_path("predictors", row).stat().st_size
        self.assertEqual(actual_size, len(telemetry._canonical(document).encode()) + (2 if telemetry.os.name == "nt" else 1))
        another = self.capture(mint="another")
        telemetry.finish(another)
        row = telemetry._row(another)
        document = {**row, "content_hash": telemetry._digest(row)}
        size = len(telemetry._canonical(document).encode()) + (2 if telemetry.os.name == "nt" else 1)
        with patch.object(telemetry, "MAX_ROW_BYTES", size - 1):
            telemetry._persist(another)
        self.assertEqual(telemetry._health["last_error"], "row_too_large")

    def test_provenance_config_hash_excludes_secret_environment(self):
        config = {"TRADING_MODE": "paper", "API_KEY": "secret", "RPC_URL": "https://secret"}
        with patch.object(telemetry.subprocess, "run", side_effect=OSError("git")):
            a = telemetry._build_provenance(config)
            b = telemetry._build_provenance({"TRADING_MODE": "paper", "API_KEY": "other"})
        self.assertEqual(a["config_fingerprint"], b["config_fingerprint"])
        self.assertNotIn("API_KEY", a["safe_config"])
        self.assertNotIn("RPC_URL", a["safe_config"])
        self.assertNotIn('"secret"', json.dumps(a))
        self.assertEqual(a["process_start_semantics"], "module initialization UTC")
        self.assertEqual(a["telemetry_schema_version"], 2)

    def test_health_storage_is_bounded_across_sessions_and_versioned(self):
        telemetry._publish_health()
        with patch.object(telemetry, "_session", "restarted"):
            telemetry._publish_health()
        directory = self.root / "data/research/entry_telemetry/epochs/offline-epoch"
        self.assertEqual(len(list((directory / "health").glob("*.json"))), 1)
        health = json.loads((directory / "health/monitor.json").read_text())
        self.assertEqual(health["version"], 2)
        self.assertEqual(health["session_id"], "restarted")

    def test_actual_safe_constants_change_config_fingerprint(self):
        with patch.object(telemetry.subprocess, "run", side_effect=OSError("git")):
            a = telemetry._build_provenance({"paper_buy_basis_points": 50})
            b = telemetry._build_provenance({"paper_buy_basis_points": 51})
        self.assertNotEqual(a["config_fingerprint"], b["config_fingerprint"])

    def test_quote_age_uses_actual_monotonic_local_receipt_before_decision(self):
        capture = self.capture()
        with telemetry.bind(capture), patch.object(telemetry.time, "monotonic_ns", side_effect=[1000000000, 3000000000]):
            telemetry.mark("quote_buy_received_at")
            telemetry.mark("entry_decision_at")
        self.assertEqual(capture.sections["quote_buy"]["quote_age_at_decision_sec"], 2)
        self.assertEqual(capture.sections["quote_buy"]["quote_age_ms"], 2000)
        self.assertEqual(capture.timestamps["quote_received_at"]["monotonic_ns"], 1000000000)

    def test_worker_write_failure_publishes_degraded_without_control_dependency(self):
        capture = self.capture()
        telemetry.finish(capture)
        class OneQueue:
            def __init__(self):
                self.called = False
            def get(self, **kwargs):
                if self.called:
                    raise KeyboardInterrupt()
                self.called = True
                return capture
            def task_done(self):
                pass
        with patch.object(telemetry, "_queue", OneQueue()), \
                patch.object(telemetry, "_build_provenance", return_value={}), \
                patch.object(telemetry, "_persist_stream", side_effect=OSError("sensitive text")), \
                patch.object(telemetry, "_publish_health") as health:
            with self.assertRaises(KeyboardInterrupt):
                telemetry._run({})
        self.assertTrue(health.called)
        self.assertEqual(telemetry._health["write_error_count"], 1)
        self.assertEqual(telemetry._health["last_error"], "record_write_error")

    def test_shared_capture_sanitizer_budget_bounds_repeated_nested_values(self):
        capture = self.capture()
        with telemetry.bind(capture):
            for index in range(8):
                telemetry.set_section("wallets", {"wallet_ids": ["x" * 256] * 128})
        self.assertGreaterEqual(capture.safe_budget[0], 0)
        self.assertGreaterEqual(capture.safe_budget[1], 0)
        self.assertTrue(telemetry._row(capture)["collection_limits"]["capture_budget_exhausted"])

    def test_copied_context_late_task_cannot_change_finished_capture(self):
        capture = self.capture()
        with telemetry.bind(capture):
            copied = contextvars.copy_context()
        telemetry.finish(capture)
        before = copy.deepcopy(capture.sections)
        copied.run(telemetry.set_section, "scores", {"uncapped_total": 666})
        self.assertEqual(capture.sections, before)

    def test_async_contexts_are_isolated(self):
        captures = [self.capture(mint=str(index)) for index in range(2)]
        async def task(capture, value):
            with telemetry.bind(capture):
                await asyncio.sleep(0)
                telemetry.set_section("pressure", {"open_positions_count": value})
        async def run():
            await asyncio.gather(task(captures[0], 1), task(captures[1], 2))
        asyncio.run(run())
        self.assertEqual([capture.sections["pressure"]["open_positions_count"] for capture in captures], [1, 2])

    def test_capture_hot_path_latency_and_no_io(self):
        started = time.perf_counter()
        for index in range(1000):
            capture = self.capture(mint=str(index))
            with telemetry.bind(capture):
                telemetry.mark("analysis_started_at")
                telemetry.set_section("scores", {"capped_total": 100})
                telemetry.mark("entry_decision_at")
        elapsed = time.perf_counter() - started
        self.assertLess(elapsed, 2, "1000 in-memory captures must remain lightweight")
        self.assertFalse((self.root / "data").exists())

    def test_windows_spawn_atomic_duplicate_race(self):
        ctx = multiprocessing.get_context("spawn")
        start = ctx.Event()
        pairs = [ctx.Pipe() for _ in range(4)]
        workers = [ctx.Process(target=race_writer, args=(str(self.root), start, child)) for _, child in pairs]
        try:
            for worker in workers:
                worker.start()
            for parent, _ in pairs:
                self.assertTrue(parent.poll(30))
                self.assertEqual(parent.recv(), "READY")
            start.set()
            for parent, _ in pairs:
                self.assertTrue(parent.poll(30))
                self.assertEqual(parent.recv(), "OK")
            for worker in workers:
                worker.join(30)
                self.assertEqual(worker.exitcode, 0)
            directory = self.root / "data/research/entry_telemetry/epochs/offline-epoch"
            self.assertEqual(len(list((directory / "predictors/2026-10-03").glob("*.json"))), 1)
            self.assertEqual(len(list((directory / "predictors/_identity").glob("*/*.json"))), 1)
        finally:
            for worker in workers:
                if worker.is_alive():
                    worker.terminate()
                    worker.join(10)
            for parent, child in pairs:
                parent.close()
                child.close()


if __name__ == "__main__":
    unittest.main()
