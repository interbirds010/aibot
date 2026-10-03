"""첫 runtime acceptance 도구를 합성 immutable stream으로 검증한다."""
from contextlib import ExitStack
import hashlib
from pathlib import Path
import queue
import tempfile
import unittest
from unittest.mock import patch

from scripts import entry_telemetry_acceptance as acceptance
from src.research import entry_telemetry as t, entry_telemetry_epoch as epoch
from src.research.n3_shadow import immutable
from src.state_store import atomic_write_json

STAMP = "2026-10-03T00:00:00+00:00"


class EntryTelemetryAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.stack = ExitStack(); self.addCleanup(self.stack.close)
        self.marker = {"telemetry_epoch_id": "offline-acceptance", "build_sha": "a" * 40,
                       "start_utc": STAMP, "start_event_seq": 1,
                       "config_fingerprint": t._digest({})}
        self.provenance = {"git_sha": "a" * 40, "session_id": "offline-session",
                           "config_fingerprint": self.marker["config_fingerprint"]}
        for name, value in {"_root": self.root, "_epoch": self.marker, "_provenance": self.provenance,
                "_queue": queue.Queue(16), "_receipt_queue": queue.Queue(16), "_outcome_queue": queue.Queue(16),
                "_health": {"status": "TELEMETRY_READY", "dropped_row_count": 0, "write_error_count": 0,
                    "duplicate_count": 0, "conflict_count": 0, "last_error": None}}.items():
            self.stack.enter_context(patch.object(t, name, value))
        self.stack.enter_context(patch.object(epoch, "require_epoch", return_value=self.marker))
        directory = t._directory()
        immutable(directory / "sessions" / (hashlib.sha256(b"offline-session").hexdigest() + ".json"),
                  {"telemetry_epoch_id": self.marker["telemetry_epoch_id"], "build_sha": "a" * 40,
                   "session_id": "offline-session"})
        atomic_write_json(self.root / "data/paper_trades.json", {"schema_version": 2, "next_event_seq": 3,
            "positions": {}, "events": [{"type": "BUY", "event_seq": 1, "mint": "mint", "position_id": "trade"},
                                       {"type": "SELL", "event_seq": 2, "mint": "mint", "position_id": "trade",
                                        "at": STAMP, "reason": "MANUAL_CLOSE"}]})

    def write_candidate(self, outcome):
        capture = t.begin_signal(mint="mint", route_type="B", signal_detected_at=STAMP, signal_id=outcome)
        with t.bind(capture):
            t.mark("entry_decision_at")
            if outcome == "BUY":
                t.mark("paper_buy_created_at", trade_id="trade", event_seq=1)
        t.finish(capture, outcome=outcome); t._persist(capture)
        while not t._queue.empty(): t._queue.get_nowait(); t._queue.task_done()
        while not t._receipt_queue.empty(): t._receipt_queue.get_nowait(); t._receipt_queue.task_done()

    def health(self):
        for role in ("monitor", "risk-manager"):
            with patch.object(t, "_role", role): t._publish_health()

    def test_all_five_natural_cases_join_and_control_proof_pass(self):
        for outcome in ("BUY", "REJECT_ANALYZER", "RPC_SKIPPED"): self.write_candidate(outcome)
        t.submit_outcome({"trade_id": "trade", "mint": "mint", "route_type": "B",
            "strategy_family": "dex_momentum_b", "signal_detected_at": STAMP, "buy_event_seq": 1,
            "event_seq": 2, "completed_at": STAMP, "exit_reason": "MANUAL_CLOSE", "sell_legs": []})
        value = t._outcome_queue.get_nowait(); t._outcome_queue.task_done(); t._write_stream("outcomes", value)
        self.health()
        report = acceptance.inspect(self.root)
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(len(report["cases"]), 5)
        self.assertTrue(all(case["status"] == "PASS" for case in report["cases"].values()))

    def test_absent_natural_cases_remain_pending_without_fabrication(self):
        self.write_candidate("REJECT_RISK"); self.health()
        report = acceptance.inspect(self.root)
        self.assertEqual(report["status"], "PENDING")
        self.assertEqual(report["cases"]["first_buy_receipt"]["status"], "PENDING")

    def test_degraded_health_fails_acceptance(self):
        self.health()
        t._degrade("offline_injected_error", dropped=True, stream="predictors"); self.health()
        with self.assertRaisesRegex(ValueError, "degraded"):
            acceptance.inspect(self.root)

    def test_wrong_control_identity_fails_acceptance(self):
        self.write_candidate("BUY"); self.health()
        atomic_write_json(self.root / "data/paper_trades.json", {"positions": {}, "events": []})
        with self.assertRaisesRegex(ValueError, "Control BUY"):
            acceptance.inspect(self.root)

    def test_recursive_schema_violation_is_rejected_without_outcome_join(self):
        self.write_candidate("REJECT_RISK")
        row = t._row(t.begin_signal(mint="mint", route_type="B", signal_detected_at=STAMP))
        row["predictors"]["sections"]["scores"]["raw_components"] = {"SELL": {"realized_pnl": 1}}
        with self.assertRaisesRegex(ValueError, "allowlist"):
            acceptance.validate_predictor(row)

    def test_tampered_scalar_timestamp_identity_and_feature_envelope_fail(self):
        import copy
        row = t._row(t.begin_signal(mint="mint", route_type="B", signal_detected_at=STAMP))
        cases = [lambda r: r["identity"].update(result={"realized_pnl": 1}),
            lambda r: r["predictors"].update(future_data={"SELL": 1}),
            lambda r: r["predictors"]["counters"].update(rpc_attempt_count={"realized_pnl": 1}),
            lambda r: r["predictors"]["timestamps"].update(analysis_started_at={"wall_utc": STAMP, "SELL": 1}),
            lambda r: r["provenance"].update(os={"result": 1}),
            lambda r: r.update(decision_outcome="BUY"),
            lambda r: r["predictors"]["sections"]["trajectory"].update(pre_signal_snapshots=[{"snapshot_at_epoch": 9999999999}])]
        for tamper in cases:
            changed = copy.deepcopy(row); tamper(changed)
            with self.assertRaises(ValueError): acceptance.validate_predictor(changed)

    def test_actual_buy_creation_stamp_is_preserved_by_monitor_ack(self):
        capture = t.begin_signal(mint="mint", route_type="B", signal_detected_at=STAMP)
        with t.bind(capture):
            t.mark("paper_buy_created_at", trade_id="trade", event_seq=1,
                   wall_clock=STAMP, monotonic=10)
            first = dict(capture.receipt["paper_buy_created_at"])
            t.mark("paper_buy_created_at", trade_id="trade", wall_clock=STAMP, monotonic=20)
        self.assertEqual(capture.receipt["paper_buy_created_at"], first)
        self.assertEqual(capture.receipt["event_seq"], 1)
