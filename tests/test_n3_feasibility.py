"""경제 평가 없이 N3 feasibility 경계와 외부 불변 기록을 검증한다."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.research import n3_feasibility as n
from src.research.n3_shadow import CONTRACT


class ForbiddenEconomics(dict):
    def get(self, key, default=None):
        if any(word in key for word in ("pnl", "profit", "return", "cost", "proceeds", "winner")):
            raise AssertionError("economic field accessed: " + key)
        return super().get(key, default)

    def __getitem__(self, key):
        if any(word in key for word in ("pnl", "profit", "return", "cost", "proceeds", "winner")):
            raise AssertionError("economic field accessed: " + key)
        return super().__getitem__(key)


class N3FeasibilityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "runtime"
        self.sidecar = self.root / "data/n3_shadow"
        self.manifest = {"cohort_id": n.COHORT, "start_event_seq": n.START_SEQ,
                         "start_utc": "2026-10-03T01:04:59+00:00", "contract": CONTRACT,
                         "build": {"git_sha": n.BUILD, "source_digest": n.SOURCE_DIGEST}}
        self.guard = patch.object(n, "source_guard", return_value=self.manifest)
        self.guard.start()
        self.addCleanup(self.guard.stop)
        self.output = Path(self.temp.name) / "amendment.json"
        self.populate(17, 4, 0)
        self.amendment = n.create_amendment(self.root, self.output, "2026-10-07T03:00:00+00:00")

    def write(self, path, row, sealed=False):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({**row, "content_hash": n.digest(row)} if sealed else row), encoding="utf-8")

    def record(self, kind, identity, row):
        path = self.sidecar / kind / (hashlib.sha256(str(identity).encode()).hexdigest() + ".json")
        self.write(path, row, True)
        return path

    def populate(self, count, days, hits, unknown=0):
        for folder in ("entries", "completed", "sells", "snapshots"):
            target = self.sidecar / folder
            if target.exists():
                for path in target.glob("*.json"):
                    path.unlink()
        events = []
        for i in range(count):
            pid = "position-" + str(i)
            seq = n.START_SEQ + i * 2
            signal = datetime(2026, 10, 3, 2, tzinfo=timezone.utc) + timedelta(days=i % days)
            analysis = signal + timedelta(seconds=2)
            preflight = analysis + timedelta(seconds=1 if i < hits else 4.5)
            at = (preflight + timedelta(seconds=1)).isoformat()
            value = True if i < hits else (None if i >= count - unknown else False)
            inputs = {"analysis_duration_sec": 2, "quote_preflight_duration_sec": 1 if i < hits else 4.5}
            if value is None:
                inputs = {"analysis_duration_sec": None, "quote_preflight_duration_sec": None}
            snap = {"cohort_id": n.COHORT, "rule_hash": n.RULE_HASH, "build": self.manifest["build"],
                    "position_id": pid, "raw_inputs": inputs, "would_skip": value,
                    "signal_timestamp": signal.isoformat(), "analysis_end_timestamp": analysis.isoformat(),
                    "preflight_end_timestamp": preflight.isoformat(), "entry_decision_timestamp": at,
                    "missing_fields": ["decision_snapshot"] if value is None else []}
            entry = {"cohort_id": n.COHORT, "rule_hash": n.RULE_HASH, "position_id": pid,
                     "buy_event_seq": seq, "buy_event_id": "buy-" + pid,
                     "signal_timestamp": signal.isoformat(), "entry_token_amount_raw": 100,
                     "would_skip": value, "snapshot": snap, "entry_cost_lamports": 999}
            buy = {"event_seq": seq, "event_id": entry["buy_event_id"], "type": "BUY", "position_id": pid,
                   "signal_detected_at": signal.isoformat(), "analysis_completed_at": analysis.isoformat(),
                   "entry_quote_at": preflight.isoformat(), "at": at, "cost_lamports": 999, "token_amount_raw": 100}
            sell = {"event_seq": seq + 1, "event_id": "sell-" + pid, "type": "SELL", "position_id": pid,
                    "at": at, "token_amount_raw": 100, "realized_pnl_lamports": 888,
                    "proceeds_lamports": 1887}
            completed = {**entry, "closed_at": at, "sell_event_count": 1, "realized_pnl_lamports": 888,
                         "normalized_return": 888 / 999}
            self.record("entries", pid, entry)
            self.record("snapshots", pid, snap)
            self.record("completed", pid, completed)
            self.record("sells", seq + 1, sell)
            events.extend((buy, sell))
        state = {"cohort_id": n.COHORT, "status": "RUNNING", "cursor": n.START_SEQ + count * 2 - 1,
                 "version": count, "admitted_count": count, "completed_count": count,
                 "open_count": 0, "sell_record_count": count, "missing_snapshot_count": unknown,
                 "missing_field_counts": {"decision_snapshot": unknown} if unknown else {},
                 "recorder_health": "MISSING_SNAPSHOTS" if unknown else "OK"}
        self.write(self.sidecar / "observer_state.json", state)
        self.write(self.root / "data/paper_trades.json", {"schema_version": 2,
                   "events": events, "next_event_seq": state["cursor"] + 1})

    def status(self):
        return n.checkpoint_status(self.root, self.output)

    def test_disclosure_is_new_after_17_not_rewritten_protocol(self):
        self.assertEqual(self.amendment["already_observed_completed_at_request"], 17)
        self.assertEqual(self.amendment["observed_at_adoption"]["completed"], 17)
        self.assertIn("after 17", self.amendment["disclosure"])
        self.assertEqual(self.amendment["original_manifest_hash"], n.MANIFEST_HASH)
        self.assertEqual(CONTRACT["sample_gates"]["completed_control"], 200)

    def test_trade_and_day_boundaries(self):
        for completed, days, expected in ((29, 6, "WAITING_FOR_30_OR_7D"), (30, 6, "CHECKPOINT_REACHED"), (29, 7, "CHECKPOINT_REACHED")):
            with self.subTest(completed=completed, days=days):
                self.populate(completed, days, 0)
                self.assertEqual(self.status()["checkpoint_status"], expected)

    def test_hit_one_and_two_verdict(self):
        for hits, decision in ((1, "N3_LOW_FRESH_COVERAGE"), (2, "N3_CONTINUE_PROSPECTIVE_VALIDATION")):
            self.populate(30, 6, hits)
            report = self.status()
            self.assertEqual(report["decision"], decision)
            self.assertEqual(report["hit"], hits)
            self.assertEqual(report["non_hit"], 30 - hits)

    def test_unknown_recording_cannot_judge_coverage(self):
        self.populate(30, 6, 0, unknown=1)
        report = self.status()
        self.assertEqual(report["checkpoint_status"], "CHECKPOINT_REACHED")
        self.assertEqual(report["unknown"], 1)
        self.assertIsNone(report["decision"])
        self.assertEqual(report["recorder_health"], "RECORDING_REVIEW_REQUIRED")

    def test_economic_values_never_accessed(self):
        original = n.read_sealed
        original_document = n.read_document
        def forbid(value):
            if isinstance(value, dict):
                return ForbiddenEconomics({k: forbid(v) for k, v in value.items()})
            if isinstance(value, list):
                return [forbid(v) for v in value]
            return value
        with patch.object(n, "read_sealed", side_effect=lambda p: forbid(original(p))), patch.object(n, "read_document", side_effect=lambda p: forbid(original_document(p))), patch("src.research.n3_shadow.completed_trades", side_effect=AssertionError("economic aggregator forbidden")):
            report = self.status()
        self.assertEqual(report["completed"], 17)
        self.assertFalse(any(word in json.dumps(report) for word in ("pnl", "expectancy", "winner", "profit_factor")))

    def test_immutable_first_checkpoint_survives_later_hit_changes(self):
        output = Path(self.temp.name) / "checkpoint.json"
        self.assertIsNone(n.record_checkpoint(self.root, self.output, output))
        self.assertFalse(output.exists())
        self.populate(30, 6, 1)
        first = n.record_checkpoint(self.root, self.output, output)
        byte_value = output.read_bytes()
        self.populate(31, 7, 2)
        self.assertEqual(n.record_checkpoint(self.root, self.output, output), first)
        self.assertEqual(output.read_bytes(), byte_value)

    def test_output_inside_runtime_rejected(self):
        with self.assertRaises(n.FeasibilityBlocked):
            n.create_amendment(self.root, self.root / "data/amendment.json")
        with self.assertRaises(n.FeasibilityBlocked):
            n.record_checkpoint(self.root, self.output, self.root / "checkpoint.json")

    def test_corrupt_missing_and_resealed_identity_block(self):
        for mode in ("corrupt", "missing", "identity"):
            self.populate(17, 4, 0)
            target = next((self.sidecar / "completed").glob("*.json"))
            if mode == "corrupt":
                target.write_text("{broken", encoding="utf-8")
            elif mode == "missing":
                target.unlink()
            else:
                row = n.read_sealed(target)
                row["cohort_id"] = "foreign"
                self.write(target, row, True)
            self.assertEqual(self.status()["checkpoint_status"], "BLOCKED", mode)

    def test_source_guard_failure_is_blocked(self):
        with patch.object(n, "source_guard", side_effect=n.FeasibilityBlocked("source mismatch")):
            self.assertEqual(self.status()["checkpoint_status"], "BLOCKED")

    def test_real_source_guard_rejects_resealed_foreign_manifest(self):
        self.write(self.sidecar / "manifest.json", self.manifest, True)
        self.guard.stop()
        try:
            with self.assertRaisesRegex(n.FeasibilityBlocked, "manifest/cohort"):
                n.source_guard(self.root)
        finally:
            self.guard.start()

    def test_missing_sequence_blocks(self):
        path = self.root / "data/paper_trades.json"
        value = n.read_document(path)
        value["events"].pop(1)
        self.write(path, value)
        self.assertEqual(self.status()["checkpoint_status"], "BLOCKED")

    def test_timing_corruption_is_blocked_even_with_new_seals(self):
        path = next((self.sidecar / "entries").glob("*.json"))
        value = n.read_sealed(path)
        value["snapshot"]["analysis_end_timestamp"] = value["signal_timestamp"]
        self.write(path, value, True)
        self.record("snapshots", value["position_id"], value["snapshot"])
        self.assertEqual(self.status()["checkpoint_status"], "BLOCKED")

    def test_population_regression_is_blocked(self):
        self.populate(16, 4, 0)
        self.assertEqual(self.status()["checkpoint_status"], "BLOCKED")

    def test_rule_hash_and_policy_changes_blocked(self):
        value = dict(self.amendment)
        value["checkpoint_gate"] = {**n.GATES, "completed_control": 20}
        self.assertEqual(n.checkpoint_status(self.root, value)["checkpoint_status"], "BLOCKED")

    def test_read_only_status_preserves_runtime_bytes(self):
        before = {str(p): p.read_bytes() for p in self.root.rglob("*.json")}
        self.status()
        self.assertEqual({str(p): p.read_bytes() for p in self.root.rglob("*.json")}, before)


if __name__ == "__main__":
    unittest.main()
