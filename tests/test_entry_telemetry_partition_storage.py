from copy import deepcopy
import errno
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from src.research import entry_telemetry as telemetry


class PartitionStorageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for item in (
            patch.object(telemetry, "_root", self.root),
            patch.object(telemetry, "_epoch", {"telemetry_epoch_id": "storage-test", "start_utc": "2026-10-01T00:00:00+00:00", "start_event_seq": 1}),
            patch.object(telemetry, "_health", {"status": "TELEMETRY_READY", "dropped_row_count": 0,
                "write_error_count": 0, "duplicate_count": 0, "conflict_count": 0, "last_error": None}),
        ):
            item.start()
            self.addCleanup(item.stop)

    def row(self, stream, key="one", stamp="2026-10-03T14:59:59+00:00"):
        identity = {"signal_id": key, "trade_id": key if stream == "outcomes" else None,
            "mint": "mint", "route_type": "B", "signal_detected_at": stamp}
        return {"telemetry_epoch_id": "storage-test", "schema_version": 2 if stream == "predictors" else 1,
            "identity": identity, "decision": {"outcome": "BUY"}}

    def test_more_than_4096_each_stream_persists_without_cumulative_cap(self):
        started = time.perf_counter()
        for stream in ("predictors", "receipts", "outcomes"):
            for number in range(4100):
                stamp = "2026-10-03T14:59:59+00:00" if number < 2050 else "2026-10-03T15:00:00+00:00"
                telemetry._persist_stream(stream, self.row(stream, f"row-{number}", stamp))
            directory = telemetry._directory() / stream
            paths = [path for day in ("2026-10-03", "2026-10-04") for path in (directory / day).glob("*.json")]
            self.assertEqual(len(paths), 4100)
            self.assertEqual(len(list((directory / "_identity").glob("*/*.json"))), 4100)
            self.assertFalse((directory / "storage.json").exists())
            self.assertEqual(telemetry._health["streams"][stream]["written_count"], 4100)
            for path in (paths[0], paths[-1]):
                self.assertEqual(telemetry._read_sealed(path)["telemetry_epoch_id"], "storage-test")
        self.assertEqual(telemetry._health["dropped_row_count"], 0)
        print(f"PARTITION_STORAGE_REAL_WRITES rows=12300 elapsed_seconds={time.perf_counter() - started:.3f}")

    def test_daily_rollover_and_same_identity_other_day_conflict(self):
        for stream in ("predictors", "receipts", "outcomes"):
            original = self.row(stream)
            telemetry._persist_stream(stream, original)
            path = telemetry._record_path(stream, original)
            before = path.read_bytes()
            changed = self.row(stream, stamp="2026-10-03T15:00:00+00:00")
            telemetry._persist_stream(stream, changed)
            self.assertEqual(path.read_bytes(), before)
            self.assertFalse(telemetry._record_path(stream, changed).exists())
            self.assertEqual(telemetry._health["streams"][stream]["conflict_count"], 1)
            telemetry._persist_stream(stream, self.row(stream, "different", changed["identity"]["signal_detected_at"]))
            self.assertTrue(telemetry._record_path(stream, self.row(stream, "different", changed["identity"]["signal_detected_at"])).exists())

    def test_restart_reopen_duplicate_preserves_original_bytes(self):
        row = self.row("predictors")
        telemetry._persist_stream("predictors", row)
        path = telemetry._record_path("predictors", row)
        before = path.read_bytes()
        restarted = deepcopy(row)
        restarted["session_id"] = "restarted"
        telemetry._persist_stream("predictors", restarted)
        self.assertEqual(before, path.read_bytes())
        self.assertEqual(telemetry._health["duplicate_count"], 1)

    def test_crash_reservation_recovers_only_original_payload_and_ignores_temp(self):
        row = self.row("predictors")
        path = telemetry._record_path("predictors", row)
        real = telemetry.atomic_write_json
        def fail_final(target, value):
            if target == path:
                raise OSError(errno.ENOSPC, "synthetic disk full")
            return real(target, value)
        with patch.object(telemetry, "atomic_write_json", side_effect=fail_final):
            with self.assertRaises(OSError):
                telemetry._persist_stream("predictors", row)
        self.assertTrue(telemetry._identity_index_path("predictors", row).exists())
        path.parent.mkdir(parents=True, exist_ok=True)
        orphan = path.parent / "crash.tmp"
        orphan.write_text('{"partial":', encoding="utf-8")
        changed = deepcopy(row)
        changed["session_id"] = "changed"
        telemetry._persist_stream("predictors", changed)
        self.assertEqual(telemetry._health["last_error"], "immutable_reservation_conflict")
        self.assertFalse(path.exists())
        telemetry._persist_stream("predictors", row)
        self.assertTrue(path.exists())
        self.assertEqual(orphan.read_text(encoding="utf-8"), '{"partial":')

    def test_partial_final_and_partial_index_refuse_overwrite(self):
        for damage in ("record", "index"):
            row = self.row("predictors", damage)
            telemetry._persist_stream("predictors", row)
            path = telemetry._record_path("predictors", row) if damage == "record" else telemetry._identity_index_path("predictors", row)
            path.write_text('{"partial":', encoding="utf-8")
            telemetry._persist_stream("predictors", row)
            self.assertEqual(path.read_text(encoding="utf-8"), '{"partial":')
            self.assertEqual(telemetry._health["last_error"], f"immutable_{'content' if damage == 'record' else 'index'}_corrupt")

    def test_existing_valid_daily_row_without_index_is_preserved(self):
        row = self.row("predictors")
        path = telemetry._record_path("predictors", row)
        telemetry.atomic_write_json(path, {**row, "content_hash": telemetry._digest(row)})
        before = path.read_bytes()
        telemetry._persist_stream("predictors", row)
        self.assertEqual(path.read_bytes(), before)
        self.assertTrue(telemetry._identity_index_path("predictors", row).exists())

    def test_resealed_row_cannot_break_original_index_linkage(self):
        row = self.row("predictors")
        telemetry._persist_stream("predictors", row)
        path = telemetry._record_path("predictors", row)
        changed = {**row, "unexpected": "tampered"}
        telemetry.atomic_write_json(path, {**changed, "content_hash": telemetry._digest(changed)})
        before = path.read_bytes()
        telemetry._persist_stream("predictors", row)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(telemetry._health["last_error"], "immutable_index_record_conflict")

    def test_disk_full_one_stream_does_not_block_other_stream(self):
        real = telemetry.atomic_write_json
        def fail_predictor(path, value):
            if "predictors" in path.parts:
                raise OSError(errno.ENOSPC, "synthetic disk full")
            return real(path, value)
        with patch.object(telemetry, "atomic_write_json", side_effect=fail_predictor):
            capture = telemetry.begin_signal(mint="mint", route_type="B", signal_detected_at="2026-10-03T00:00:00+00:00")
            telemetry._write_stream("predictors", capture)
            telemetry._persist_stream("receipts", self.row("receipts"))
        self.assertEqual(telemetry._health["streams"]["predictors"]["write_error_count"], 1)
        self.assertEqual(telemetry._health["streams"]["receipts"]["written_count"], 1)

    def test_health_counters_are_explicitly_session_local(self):
        telemetry._persist_stream("predictors", self.row("predictors"))
        telemetry._publish_health()
        health = json.loads((telemetry._directory() / "health/monitor.json").read_text(encoding="utf-8"))
        self.assertIsNone(health["storage_limits"]["rows"])
        self.assertIsNone(health["storage_limits"]["bytes"])
        self.assertEqual(health["storage_limits"]["row_bytes"], 65536)
        self.assertIn("persistent totals require offline enumeration", health["counter_semantics"])

    def test_worker_restart_does_not_scan_historical_partitions(self):
        class StopQueue:
            def get(self, **kwargs):
                raise KeyboardInterrupt()
        with patch.object(telemetry, "_queue", StopQueue()), \
                patch.object(Path, "glob", side_effect=AssertionError("historical scan")), \
                patch.object(Path, "rglob", side_effect=AssertionError("historical scan")):
            with self.assertRaises(KeyboardInterrupt):
                telemetry._run({}, initialized=True)
        self.assertFalse((self.root / "data").exists())


if __name__ == "__main__":
    unittest.main()
