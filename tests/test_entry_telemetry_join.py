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


if __name__ == "__main__":
    unittest.main()
