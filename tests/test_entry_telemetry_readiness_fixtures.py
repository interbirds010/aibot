"""실제 serializer의 offline fixture와 미해결 leakage 위험을 고정한다."""
from __future__ import annotations

import copy
from datetime import datetime
import json
import os
from pathlib import Path
import queue
import tempfile
import unittest
from unittest.mock import patch

from src.research import entry_telemetry as telemetry


STAMP = "2026-10-03T00:00:00+00:00"
FIXTURE_NAMES = (
    "normal_buy", "analyzer_reject", "risk_reject", "quote_failed",
    "rpc_skipped", "missing_optional", "partial_wallet_score",
    "wallet_rich", "maximum_bounded_input",
)
OUTCOMES = {
    "normal_buy": "BUY", "analyzer_reject": "REJECT_ANALYZER",
    "risk_reject": "REJECT_RISK", "quote_failed": "QUOTE_FAILED",
    "rpc_skipped": "RPC_SKIPPED", "missing_optional": "OTHER",
    "partial_wallet_score": "OTHER", "wallet_rich": "BUY",
    "maximum_bounded_input": "BUY",
}


def fixture_capture(name):
    """실시간 실행 없이 공개 capture API로 합성 입력만 공급한다."""
    if name not in FIXTURE_NAMES:
        raise ValueError(name)
    capture = telemetry.begin_signal(
        mint="offline-fixture-mint-" + name, route_type="B",
        signal_detected_at=STAMP, source_wallet="offline-source-wallet",
        source_signature="offline-source-signature", signal_id=name,
    )
    with telemetry.bind(capture):
        telemetry.mark("signal_enqueued_at", wall_clock=STAMP, monotonic=1)
        if name not in ("missing_optional", "rpc_skipped"):
            telemetry.mark("analysis_started_at", wall_clock=STAMP, monotonic=2)
            telemetry.set_section("scores", {"momentum": {
                "raw_components": {"volume_m5_usd": 32500.125, "volume_operand": 65.00025,
                    "volume_points": 60, "net_buy_count": 25, "net_buy_points": 40},
                "capped_total": 100, "uncapped_total": None,
                "uncapped_total_missing_reason": "upstream_not_exposed",
            }, "missing_reason": None})
            telemetry.set_section("wallets", {"observed_whale_count": 3,
                "observed_unique_wallet_count": 3, "count_is_lower_bound": True,
                "participating_wallet_ids": ["offline-wallet-1", "offline-wallet-2", "offline-wallet-3"],
                "paid_lamports_by_wallet": {"offline-wallet-1": 1500000000,
                    "offline-wallet-2": 1600000000, "offline-wallet-3": 1700000000},
                "missing_reason": "full_population_not_observed"})
            telemetry.add_counter("rpc_attempt_count", 2)
            telemetry.add_counter("rpc_retry_count", 1)
            telemetry.duration("rpc_retry_sleep_sec", .25)
            telemetry.duration("rpc_limiter_wait_duration_sec", .01)
            telemetry.mark("analysis_completed_at", wall_clock=STAMP, monotonic=3)
        if name == "partial_wallet_score":
            telemetry.set_section("scores", {"momentum": None,
                "missing_reason": "optional_momentum_unavailable"})
            telemetry.set_section("wallets", {"observed_whale_count": None,
                "participating_wallet_ids": None, "missing_reason": "optional_wallet_inputs_unavailable"})
        if name in ("normal_buy", "risk_reject", "quote_failed", "wallet_rich", "maximum_bounded_input"):
            telemetry.mark("risk_check_started_at", wall_clock=STAMP, monotonic=4)
            telemetry.mark("risk_check_completed_at", wall_clock=STAMP, monotonic=5)
        if name in ("normal_buy", "quote_failed", "wallet_rich", "maximum_bounded_input"):
            telemetry.mark("preflight_started_at", wall_clock=STAMP, monotonic=6)
            telemetry.mark("quote_buy_request_started_at", wall_clock=STAMP, monotonic=7)
            telemetry.add_counter("quote_buy_attempt_count", 2)
            telemetry.add_counter("quote_buy_retry_count", 1)
            telemetry.duration("quote_buy_retry_sleep_sec", .1)
            telemetry.duration("quote_limiter_wait_duration_sec", .02)
            if name == "quote_failed":
                telemetry.set_section("quote_buy", {"last_error_type": "TimeoutError",
                    "missing_reason": "quote_not_received"})
            else:
                telemetry.mark("quote_buy_received_at", wall_clock=STAMP, monotonic=8)
                telemetry.set_section("quote_buy", {"input_amount": 5000000,
                    "expected_output": 120000000, "price_impact_pct": .125,
                    "route_count": 2, "dex_identifiers": ["offline-dex-1", "offline-dex-2"],
                    "route_hash": "0" * 64, "missing_reason": None})
            telemetry.mark("preflight_completed_at", wall_clock=STAMP, monotonic=9)
        if name == "rpc_skipped":
            telemetry.set_section("rpc", {"last_error_type": "TimeoutError",
                "scope": "confirmation", "missing_reason": "confirmation_rpc_unavailable"})
            telemetry.add_counter("rpc_attempt_count", 3)
            telemetry.add_counter("rpc_retry_count", 2)
            telemetry.duration("rpc_retry_sleep_sec", .75)
        if name == "wallet_rich":
            telemetry.set_section("wallets", {"participating_wallet_ids": [
                "offline-wallet-" + str(index) for index in range(32)],
                "paid_lamports_by_wallet": {"offline-wallet-" + str(index): 1500000000 + index
                    for index in range(32)}, "observed_whale_count": 32})
        if name == "maximum_bounded_input":
            # 상한 동작 측정용 synthetic stress fixture이며 실거래 분포가 아니다.
            telemetry.set_section("quote_buy", {"dex_identifiers": ["x" * 256 for _ in range(128)]})
            telemetry.set_section("wallets", {"participating_wallet_ids": ["y" * 256 for _ in range(128)]})
        telemetry.set_section("decision", {"outcome": OUTCOMES[name],
            "reason": "offline_fixture_" + name, "missing_reason": None})
        telemetry.mark("entry_decision_at", wall_clock=STAMP, monotonic=10)
        if OUTCOMES[name] == "BUY":
            telemetry.mark("paper_buy_created_at", trade_id="offline-trade-" + name)
    # 큐 ownership 전환도 실제 finish로 검증하되 worker는 시작하지 않는다.
    with patch.object(telemetry, "_queue", queue.Queue(maxsize=1)):
        telemetry.finish(capture)
    return capture


def fixture_row(name):
    return telemetry._row(fixture_capture(name))


class EntryTelemetryReadinessFixtureTests(unittest.TestCase):
    def test_all_required_outcomes_use_actual_persisted_serializer(self):
        provenance = telemetry._build_provenance({"TRADING_MODE": "paper"})
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(telemetry, "_root", Path(directory)), \
                patch.object(telemetry, "_provenance", provenance):
            for name in FIXTURE_NAMES:
                with self.subTest(name=name):
                    capture = fixture_capture(name)
                    telemetry._persist(capture)
                    path = Path(directory) / "data/research/entry_telemetry/rows" / (capture.identity["signal_id"] + ".json")
                    record = json.loads(path.read_text(encoding="utf-8"))
                    digest = record.pop("content_hash")
                    self.assertEqual(digest, telemetry._digest(record))
                    self.assertEqual(record["schema_version"], telemetry.SCHEMA_VERSION)
                    self.assertEqual(record["decision"]["outcome"], OUTCOMES[name])
                    self.assertEqual(record["identity"]["signal_detected_at"], STAMP)
                    self.assertEqual(len(record["identity"]["signal_id"]), 64)
                    self.assertNotEqual(record["identity"]["source_wallet_hash"], "offline-source-wallet")
                    self.assertIn("entry_decision_at", record["predictors"]["timestamps"])
                    self.assertNotIn("paper_buy_created_at", record["predictors"]["timestamps"])
                    self.assertNotIn("decision", record["predictors"]["sections"])
                    self.assertIsNone(record["predictors"]["sections"]["wallet_performance"]["realized_pnl"])
                    self.assertEqual(record["predictors"]["sections"]["wallet_performance"]["missing_reason"],
                        "not_available_in_existing_flow")
                    self.assertEqual(record["provenance"]["telemetry_schema_version"], telemetry.SCHEMA_VERSION)
                    for field in ("git_sha", "source_digest", "config_fingerprint", "os", "platform", "session_id"):
                        self.assertTrue(record["provenance"][field])
                    self.assertLess(path.stat().st_size, telemetry.MAX_ROW_BYTES)
                    sealed = {**record, "content_hash": digest}
                    self.assertEqual(path.stat().st_size, len(telemetry._canonical(sealed).encode("utf-8"))
                        + (2 if os.name == "nt" else 1))

    def test_buy_raw_capped_retry_quote_and_wallet_fields(self):
        record = fixture_row("normal_buy")
        predictors = record["predictors"]
        momentum = predictors["sections"]["scores"]["momentum"]
        self.assertEqual(momentum["raw_components"]["volume_operand"], 65.00025)
        self.assertEqual(momentum["raw_components"]["volume_points"], 60)
        self.assertIsNone(momentum["uncapped_total"])
        self.assertEqual(momentum["capped_total"], 100)
        self.assertEqual(predictors["counters"]["rpc_retry_count"], 1)
        self.assertEqual(predictors["durations"]["rpc_limiter_wait_duration_sec"], .01)
        quote = predictors["sections"]["quote_buy"]
        self.assertEqual(quote["expected_output"], 120000000)
        self.assertEqual(quote["quote_age_at_decision_sec"], 2)
        self.assertEqual(predictors["sections"]["wallets"]["observed_whale_count"], 3)
        stamps = [predictors["timestamps"][key]["monotonic_ns"] for key in
            ("signal_enqueued_at", "analysis_started_at", "analysis_completed_at",
             "quote_buy_request_started_at", "quote_buy_received_at", "entry_decision_at")]
        self.assertEqual(stamps, sorted(stamps))

    def test_missing_optional_and_partial_inputs_remain_null_with_reason(self):
        minimal = fixture_row("missing_optional")["predictors"]
        partial = fixture_row("partial_wallet_score")["predictors"]
        self.assertIsNone(minimal["timestamps"]["analysis_started_at"])
        self.assertIsNone(minimal["counters"]["rpc_attempt_count"])
        self.assertIsNone(minimal["sections"]["quote_buy"]["expected_output"])
        self.assertIsNone(partial["sections"]["scores"]["momentum"])
        self.assertIsNone(partial["sections"]["wallets"]["observed_whale_count"])
        self.assertEqual(partial["sections"]["wallets"]["missing_reason"], "optional_wallet_inputs_unavailable")

    def test_post_decision_predictor_updates_are_ignored(self):
        capture = fixture_capture("normal_buy")
        before = copy.deepcopy(telemetry._row(capture)["predictors"])
        with telemetry.bind(capture):
            telemetry.set_section("wallet_performance", {"realized_pnl": 12345})
            telemetry.set_section("scores", {"raw_components": {"SELL": {"realized_pnl": 12345}}})
        self.assertEqual(telemetry._row(capture)["predictors"], before)

    def test_known_named_future_keys_are_recursively_removed(self):
        capture = telemetry.begin_signal(mint="offline", route_type="B", signal_detected_at=STAMP)
        forbidden = {"winner": True, "loser": False, "exit_reason": "synthetic",
            "MFE": 1, "MAE": 2, "future_horizon_roi": 3, "post_entry_research_status": "done",
            "future_wallet_performance": {"roi": 4}, "final_pnl": 5}
        with telemetry.bind(capture):
            telemetry.set_section("scores", {"raw_components": {"observed": 1, **forbidden}})
        raw = telemetry._row(capture)["predictors"]["sections"]["scores"]["raw_components"]
        self.assertEqual(raw, {"observed": 1})

    def test_characterize_unresolved_realized_pnl_sell_and_snapshot_leakage(self):
        # 보안 통과 assertion이 아니다. 아래 누출 가능성 때문에 activation은 NOT_READY다.
        capture = telemetry.begin_signal(mint="offline", route_type="B", signal_detected_at=STAMP)
        after_entry = "2026-10-04T00:00:00+00:00"
        with telemetry.bind(capture):
            telemetry.set_section("wallet_performance", {"snapshot_at": after_entry,
                "realized_pnl": 123, "roi": .5, "wins": 2, "losses": 1, "status": "closed"})
            telemetry.set_section("scores", {"raw_components": {
                "SELL": {"realized_pnl": 123, "roi": .5, "status": "closed"}}})
        sections = telemetry._row(capture)["predictors"]["sections"]
        self.assertEqual(sections["wallet_performance"]["realized_pnl"], 123)
        self.assertGreater(datetime.fromisoformat(sections["wallet_performance"]["snapshot_at"]),
            datetime.fromisoformat(capture.identity["signal_detected_at"]))
        self.assertEqual(sections["scores"]["raw_components"]["SELL"]["realized_pnl"], 123)

    def test_characterize_receipt_and_predictors_share_one_physical_file(self):
        # 현재는 JSON key 분리만 있으며 요청한 물리적 파일 분리를 충족하지 않는다.
        with tempfile.TemporaryDirectory() as directory, patch.object(telemetry, "_root", Path(directory)):
            capture = fixture_capture("normal_buy")
            telemetry._persist(capture)
            rows = list((Path(directory) / "data/research/entry_telemetry/rows").glob("*.json"))
            self.assertEqual(len(rows), 1)
            document = json.loads(rows[0].read_text(encoding="utf-8"))
            self.assertIn("predictors", document)
            self.assertIn("execution_receipt", document)
            self.assertEqual(document["execution_receipt"]["trade_id"], "offline-trade-normal_buy")


if __name__ == "__main__":
    unittest.main()
