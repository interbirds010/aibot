"""분리 stream을 offline에서만 결정적인 identity로 연결한다."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from src.research import entry_telemetry_join as join
from src.state_store import atomic_write_json

STAMP = "2026-10-04T00:00:00+00:00"


def seal(row):
    body = {key: value for key, value in row.items() if key != "content_hash"}
    return {**body, "content_hash": hashlib.sha256(join._canonical(body)).hexdigest()}


def row(stream, decision="BUY", signal="signal-1", trade="trade-1", epoch="epoch-1"):
    field, schema = join.SCHEMAS[stream]
    identity = {"mint": "MINT", "route_type": "A", "strategy_family": "whale_route_a",
                "signal_detected_at": STAMP, "signal_id": signal}
    value = {"schema_version": schema, field: schema, "telemetry_epoch_id": epoch,
             "build_sha": "f" * 40, "session_id": "session-1", "identity": identity}
    value["provenance"] = {"config_fingerprint": "c" * 64}
    if stream == "predictors":
        value["predictors"] = {}
        value["decision"] = {"outcome": decision}
    elif stream == "receipts":
        identity.update(trade_id=trade, event_seq=10)
        value["execution_receipt"] = {"trade_id": trade, "event_seq": 10}
    else:
        identity.update(signal_id=None, trade_id=trade, buy_event_seq=10, event_seq=12)
        value["outcome"] = {"exit_reason": "MANUAL_CLOSE", "realized_pnl_lamports": 5}
    return seal(value)


class EntryTelemetryOfflineJoinTests(unittest.TestCase):
    def test_predictor_receipt_completed_outcome_join(self):
        predictor, receipt, outcome = (row(stream) for stream in join.SCHEMAS)
        before = copy.deepcopy(predictor)
        result = join.join_streams([predictor], [receipt], [outcome])
        self.assertEqual(result["rows"][0]["join_status"], "COMPLETED")
        self.assertEqual(predictor, before)
        self.assertNotIn("outcome", predictor)
        self.assertEqual(result["orphan_outcomes"], [])

    def test_predictor_receipt_without_completed_outcome(self):
        result = join.join_streams([row("predictors")], [row("receipts")])
        self.assertEqual(result["rows"][0]["join_status"], "BUY_RECEIPT")

    def test_reject_and_rpc_skip_predictors_without_receipt_are_normal(self):
        for outcome in ("REJECT_ANALYZER", "REJECT_RISK", "QUOTE_FAILED", "RPC_SKIPPED"):
            with self.subTest(outcome=outcome):
                result = join.join_streams([row("predictors", decision=outcome)])
                self.assertEqual(result["rows"][0]["join_status"], "PREDICTOR_ONLY")
                self.assertIsNone(result["rows"][0]["outcome"])

    def test_buy_without_receipt_is_explicit_missing_not_fabricated(self):
        result = join.join_streams([row("predictors")], outcomes=[row("outcomes")])
        self.assertEqual(result["rows"][0]["join_status"], "MISSING_RECEIPT")
        self.assertEqual(len(result["orphan_outcomes"]), 1)

    def test_missing_trade_id_is_invalid_receipt(self):
        receipt = row("receipts")
        receipt["identity"]["trade_id"] = None
        with self.assertRaisesRegex(ValueError, "join identity missing"):
            join.join_streams([row("predictors")], [seal(receipt)])

    def test_duplicate_identity_rejected_even_when_rows_identical(self):
        for stream in join.SCHEMAS:
            values = {"predictors": [], "receipts": [], "outcomes": []}
            values[stream] = [row(stream), row(stream)]
            with self.subTest(stream=stream), self.assertRaisesRegex(ValueError, "duplicate"):
                join.join_streams(**values)

    def test_two_signal_ids_cannot_claim_one_trade_id(self):
        with self.assertRaisesRegex(ValueError, "multiple signals"):
            join.join_streams([row("predictors"), row("predictors", signal="signal-2")],
                              [row("receipts"), row("receipts", signal="signal-2")])

    def test_conflicting_mint_build_timestamp_or_buy_seq_is_rejected(self):
        for field in ("mint", "signal_detected_at", "buy_event_seq", "build_sha"):
            outcome = row("outcomes")
            if field == "build_sha":
                outcome[field] = "e" * 40
            else:
                outcome["identity"][field] = {"mint": "OTHER", "signal_detected_at": "2026-10-05T00:00:00Z",
                                              "buy_event_seq": 11}[field]
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "conflict"):
                join.join_streams([row("predictors")], [row("receipts")], [seal(outcome)])

    def test_equivalent_utc_signal_timestamps_match(self):
        outcome = row("outcomes")
        outcome["identity"]["signal_detected_at"] = "2026-10-04T09:00:00+09:00"
        result = join.join_streams([row("predictors")], [row("receipts")], [seal(outcome)])
        self.assertEqual(result["rows"][0]["join_status"], "COMPLETED")

    def test_no_cross_epoch_join(self):
        result = join.join_streams([row("predictors")], [row("receipts", epoch="epoch-2")])
        self.assertEqual(result["rows"][0]["join_status"], "MISSING_RECEIPT")
        self.assertEqual(len(result["orphan_receipts"]), 1)

    def test_corrupt_hash_or_wrong_schema_is_rejected(self):
        predictor = row("predictors")
        predictor["decision"]["outcome"] = "RPC_SKIPPED"
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            join.join_streams([predictor])
        predictor = row("predictors")
        predictor["predictor_schema_version"] = 99
        with self.assertRaisesRegex(ValueError, "schema mismatch"):
            join.join_streams([seal(predictor)])

    def test_predictor_cannot_contain_nested_receipt_or_outcome(self):
        for field in ("execution_receipt", "outcome"):
            predictor = row("predictors")
            predictor[field] = {}
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "separation"):
                join.join_streams([seal(predictor)])

    def test_joining_actual_paths_keeps_all_files_immutable_and_separate(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            paths = []
            for stream in join.SCHEMAS:
                path = root / "data/research/entry_telemetry/epochs/epoch-1" / stream / "2026-10-04" / "key.json"
                atomic_write_json(path, row(stream))
                paths.append(path)
            before = {path: path.read_bytes() for path in paths}
            result = join.join_epoch(root, "epoch-1")
            self.assertEqual(result["rows"][0]["join_status"], "COMPLETED")
            self.assertEqual(before, {path: path.read_bytes() for path in paths})
            self.assertEqual(len(set(paths)), 3)

    def test_missing_optional_stream_directory_is_empty(self):
        with tempfile.TemporaryDirectory() as temporary:
            self.assertEqual(join.load_stream(Path(temporary), "epoch-1", "outcomes"), [])

    def test_all_streams_join_more_than_previous_4096_ceiling(self):
        def records(stream):
            for number in range(4100):
                yield row(stream, signal=f"signal-{number}", trade=f"trade-{number}")
        result = join.join_streams(*(records(stream) for stream in join.SCHEMAS))
        self.assertEqual(len(result["rows"]), 4100)
        self.assertTrue(all(item["join_status"] == "COMPLETED" for item in result["rows"]))
        self.assertEqual(result["orphan_receipts"], [])
        self.assertEqual(result["orphan_outcomes"], [])

    def test_retained_files_beyond_4096_across_multiple_days_are_all_read(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            stream = root / "data/research/entry_telemetry/epochs/epoch-1/predictors"
            for day in ("2026-10-04", "2026-10-05"):
                (stream / day).mkdir(parents=True)
            for number in range(4097):
                path = stream / ("2026-10-04" if number < 2048 else "2026-10-05") / f"signal-{number}.json"
                path.write_bytes(join._canonical(row("predictors", decision="RPC_SKIPPED", signal=f"signal-{number}")))
            result = join.join_epoch(root, "epoch-1")
            self.assertEqual(len(result["rows"]), 4097)
            self.assertTrue(all(item["join_status"] == "PREDICTOR_ONLY" for item in result["rows"]))

    def test_multiday_completed_rejected_and_rpc_streams_remain_separate(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / "data/research/entry_telemetry/epochs/epoch-1"
            for stream in join.SCHEMAS:
                atomic_write_json(directory / stream / "2026-10-04" / "one.json", row(stream))
            for outcome in ("REJECT_RISK", "RPC_SKIPPED"):
                atomic_write_json(directory / "predictors/2026-10-05" / (outcome + ".json"),
                                  row("predictors", decision=outcome, signal=outcome))
            before = {path: path.read_bytes() for path in directory.rglob("*.json")}
            result = join.join_epoch(root, "epoch-1")
            self.assertCountEqual([item["join_status"] for item in result["rows"]],
                                  ["COMPLETED", "PREDICTOR_ONLY", "PREDICTOR_ONLY"])
            self.assertEqual(before, {path: path.read_bytes() for path in before})

    def test_duplicate_identity_in_different_dates_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for stream in join.SCHEMAS:
                for day in ("2026-10-04", "2026-10-05"):
                    atomic_write_json(root / "data/research/entry_telemetry/epochs/epoch-1" / stream / day / "one.json", row(stream))
                with self.subTest(stream=stream), self.assertRaisesRegex(ValueError, "duplicate"):
                    join._index(join.iter_stream(root, "epoch-1", stream), stream,
                                "trade_id" if stream == "outcomes" else "signal_id")

    def test_config_fingerprint_required_and_counterpart_conflict_rejected(self):
        receipt = row("receipts")
        receipt["provenance"]["config_fingerprint"] = "d" * 64
        with self.assertRaisesRegex(ValueError, "config fingerprint conflict"):
            join.join_streams([row("predictors")], [seal(receipt)])
        predictor = row("predictors")
        predictor["provenance"].clear()
        with self.assertRaisesRegex(ValueError, "config fingerprint missing"):
            join.join_streams([seal(predictor)])

    def test_restart_session_can_complete_same_epoch_build_and_config(self):
        outcome = row("outcomes")
        outcome["session_id"] = "restarted-risk-manager-session"
        result = join.join_streams([row("predictors")], [row("receipts")], [seal(outcome)])
        self.assertEqual(result["rows"][0]["join_status"], "COMPLETED")
        self.assertEqual(result["rows"][0]["outcome"]["session_id"], "restarted-risk-manager-session")

    def test_partial_json_fails_without_rewriting_and_temporary_file_is_ignored(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            day = root / "data/research/entry_telemetry/epochs/epoch-1/predictors/2026-10-04"
            day.mkdir(parents=True)
            temp = day / "one.json.partial"
            temp.write_bytes(b'{"partial":')
            self.assertEqual(join.load_stream(root, "epoch-1", "predictors"), [])
            path = day / "one.json"
            path.write_bytes(b'{"partial":')
            before = path.read_bytes()
            with self.assertRaises(json.JSONDecodeError):
                join.load_stream(root, "epoch-1", "predictors")
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(temp.read_bytes(), before)

    def test_sharded_index_is_not_a_row_and_linkage_is_validated(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / "data/research/entry_telemetry/epochs/epoch-1/predictors"
            predictor = row("predictors")
            path = directory / "2026-10-04/signal-1.json"
            atomic_write_json(path, predictor)
            digest = hashlib.sha256(b"signal-1").hexdigest()
            index_path = directory / "_identity" / digest[:2] / (digest + ".json")
            index = {"index_schema_version": 1, "telemetry_epoch_id": "epoch-1", "stream": "predictors",
                     "identity": predictor["identity"], "record_relative_path": "2026-10-04/signal-1.json",
                     "record_content_hash": predictor["content_hash"]}
            atomic_write_json(index_path, seal(index))
            self.assertEqual(join.load_stream(root, "epoch-1", "predictors"), [predictor])
            index["record_content_hash"] = "0" * 64
            atomic_write_json(index_path, seal(index))
            before = index_path.read_bytes()
            with self.assertRaisesRegex(ValueError, "index linkage"):
                join.load_stream(root, "epoch-1", "predictors")
            self.assertEqual(index_path.read_bytes(), before)

    def test_non_date_directory_and_unsafe_epoch_path_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "data/research/entry_telemetry/epochs/epoch-1/predictors/unexpected").mkdir(parents=True)
            with self.assertRaisesRegex(ValueError, "date partition"):
                join.load_stream(root, "epoch-1", "predictors")
            for value in ("..", "../epoch-1", "C:\\escape"):
                with self.subTest(value=value), self.assertRaisesRegex(ValueError, "epoch path"):
                    join.load_stream(root, value, "predictors")


if __name__ == "__main__":
    unittest.main()
