"""실제 기록만 사용하는 baseline과 명시적 허용 정책을 검증한다."""
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import alpha_latency_baseline as baseline
from src.research import n3_shadow as n3
from src.state_store import atomic_write_json


class BaselineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="latency 한글 path ")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.manifest = {"cohort_id": "cohort", "build": {"git_sha": "old", "source_digest": "hash"}}
        self.entry = {"position_id": "position", "cohort_id": "cohort", "rule_hash": n3.RULE_HASH,
            "buy_event_seq": 1, "buy_event_id": "buy",
            "snapshot": {"signal_timestamp": "2026-10-04T00:00:00+00:00",
                "analysis_start_timestamp": "2026-10-04T00:00:01+00:00",
                "analysis_end_timestamp": "2026-10-04T00:00:02+00:00",
                "preflight_start_timestamp": "2026-10-04T00:00:03+00:00",
                "entry_decision_timestamp": "2026-10-04T00:00:05+00:00"}}
        self.buy = {"type": "BUY", "position_id": "position", "event_id": "buy", "event_seq": 1,
            "at": "2026-10-04T00:00:06+00:00"}
        atomic_write_json(self.root / "data/paper_trades.json", {"schema_version": 2, "events": [self.buy]})
        n3.immutable(n3.record_path(self.root / "data/n3_shadow", "entries", "position"), self.entry)

    def collect(self):
        with patch.object(baseline.feasibility, "source_guard", return_value=self.manifest), patch.object(baseline, "_paper_config", return_value={}):
            return baseline.collect(self.root)

    def test_actual_wall_clock_and_quote_missing(self):
        report = self.collect()
        self.assertEqual(report["clock_semantics"], "UTC_WALL_CLOCK")
        self.assertEqual(report["samples"][0]["metrics_ms"], {"signal_to_analysis_ms": 1000,
            "analysis_to_preflight_ms": 1000, "preflight_to_paper_buy_ms": 3000, "total_decision_ms": 5000})
        self.assertNotIn("quote_ms", report["samples"][0]["metrics_ms"])
        self.assertFalse(report["economic_evaluation"])

    def test_wrong_buy_identity_rejected(self):
        atomic_write_json(self.root / "data/paper_trades.json", {"schema_version": 2, "events": [{**self.buy, "position_id": "other"}]})
        with self.assertRaisesRegex(ValueError, "identity"):
            self.collect()

    def test_policy_has_explicit_tolerance(self):
        report = self.collect()
        policy = baseline.policy(report, metrics=["total_decision_ms"], minimum_samples=25,
            maximum_increase_ms=12.5, statistic="p95")
        self.assertEqual(policy["metrics"]["total_decision_ms"]["maximum_increase_ms"], 12.5)
        with self.assertRaises(ValueError):
            baseline.policy(report, metrics=["total_decision_ms"], minimum_samples=0,
                maximum_increase_ms=12.5, statistic="p95")

    def test_no_profitability_fields_are_extracted(self):
        self.buy["realized_pnl_lamports"] = "not a numeric economic value"
        atomic_write_json(self.root / "data/paper_trades.json", {"schema_version": 2, "events": [self.buy]})
        report = self.collect()
        self.assertNotIn("realized_pnl_lamports", str(report))

    def test_corrupt_baseline_state_not_replaced(self):
        path = self.root / "data/paper_trades.json"
        path.write_bytes(b'{bad')
        with self.assertRaises(ValueError):
            self.collect()
        self.assertEqual(path.read_bytes(), b'{bad')
