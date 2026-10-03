"""실제 Control 호출과 연구 envelope의 격리를 검증한다."""

from __future__ import annotations

import asyncio
import queue
import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from src import analyzer, executor, monitor, observation_tracker, risk_manager
from src.research import entry_telemetry as telemetry


STAMP = "2026-10-03T01:04:59.425384+00:00"


class EntryMonitorTelemetryTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.rows = queue.Queue(maxsize=16)
        self.stack.enter_context(patch.object(telemetry, "_queue", self.rows))
        self.stack.enter_context(patch.object(monitor, "_analysis_limit", asyncio.Semaphore(2)))
        self.stack.enter_context(patch.dict("os.environ", {
            "OBSERVATION_MODE": "false", "JUPITER_API_KEY": "",
        }))
        for name in ("record_funnel_stage", "record_wallet_ws_activity", "record_memory_phase"):
            self.stack.enter_context(patch.object(monitor, name))
        self.stack.enter_context(patch.object(monitor, "current_rss_bytes", return_value=0))
        self.stack.enter_context(patch.object(monitor.state_store, "get_route_initial_stop_streak", return_value=(0, 0)))
        self.stack.enter_context(patch.object(monitor, "token_cooldown_is_active", return_value=False))
        self.report = SimpleNamespace(safety_score=100, route_type="A", reasons=[], liquidity_usd=10000,
                                      lp_locked_percent=80)
        self.analysis = self.stack.enter_context(patch.object(analyzer, "analyze_token", new=AsyncMock(return_value=self.report)))
        self.cash = self.stack.enter_context(patch.object(risk_manager, "paper_cash_balance", new=AsyncMock(return_value=10_000_000_000)))
        self.buy = self.stack.enter_context(patch.object(risk_manager, "record_paper_buy", new=AsyncMock(return_value="POSITION")))
        self.stack.enter_context(patch.object(risk_manager, "record_paper_rejection", new=AsyncMock()))
        self.rpc_skip = self.stack.enter_context(patch.object(risk_manager, "record_rpc_skip", new=AsyncMock()))
        self.quotes = self.stack.enter_context(patch.object(executor, "jupiter_quote", new=AsyncMock(side_effect=[
            {"outAmount": "2500", "routePlan": [{}], "priceImpactPct": "0.5", "slippageBps": "100"},
            {"outAmount": "990000", "routePlan": [{}], "priceImpactPct": "0.4", "slippageBps": "100"},
        ])))
        self.stack.enter_context(patch("src.wallet_performance.record_paper_buy_success", new=AsyncMock()))
        self.stack.enter_context(patch("src.wallet_performance.reject_unsafe_buy", new=AsyncMock()))
        self.stack.enter_context(patch.object(monitor.n3_shadow, "prepare_entry_snapshot", return_value={"would_skip": True}))
        self.n3_submit = self.stack.enter_context(patch.object(monitor.n3_shadow, "submit_control_entry"))

    def run_signal(self, **kwargs):
        asyncio.run(monitor.process_paper_signal("MINT", 1000, 6, 2_000_000_000, "WALLET", "SIGNATURE", STAMP, **kwargs))
        return telemetry._row(self.rows.get_nowait())

    def test_actual_buy_is_unchanged_and_receipt_is_not_predictor(self):
        row = self.run_signal()
        self.buy.assert_awaited_once()
        self.assertEqual(self.buy.await_args.args, ("MINT", 50_000_000, 2500, 6))
        self.n3_submit.assert_called_once_with("POSITION", {"would_skip": True})
        self.assertEqual(row["decision"]["outcome"], "BUY")
        self.assertEqual(row["execution_receipt"]["trade_id"], "POSITION")
        self.assertIsNone(row["execution_receipt"]["event_seq"])
        self.assertIn("paper_buy_created_at", row["execution_receipt"])
        self.assertNotIn("paper_buy_created_at", row["predictors"]["timestamps"])
        for key in ("final_pnl", "winner", "exit_reason", "mfe", "mae"):
            self.assertNotIn(key, str(row["predictors"]).lower())

    def test_broken_telemetry_hook_does_not_block_control(self):
        with patch.object(telemetry, "safe_hook", side_effect=RuntimeError("broken telemetry")):
            asyncio.run(monitor.process_paper_signal("MINT", 1000, 6, 2_000_000_000, "WALLET", "SIGNATURE", STAMP))
        self.buy.assert_awaited_once()
        self.n3_submit.assert_called_once()

    def test_broken_context_binding_does_not_block_control(self):
        with patch.object(telemetry, "bind", side_effect=RuntimeError("broken binding")):
            asyncio.run(monitor.process_paper_signal("MINT", 1000, 6, 2_000_000_000, "WALLET", "SIGNATURE", STAMP))
        self.buy.assert_awaited_once()

    def test_bad_trajectory_projection_does_not_block_control(self):
        with patch("src.research.prospective_features.normalize_prospective_feature_collection", side_effect=RuntimeError("bad projection")):
            row = self.run_signal(prospective_feature_collection={"schema_version": 1})
        self.buy.assert_awaited_once()
        self.assertEqual(row["decision"]["outcome"], "BUY")

    def test_prefilter_rejected_has_same_envelope(self):
        row = self.run_signal(prefilter_reasons=("WHALE_AMOUNT_FILTER_REJECTED",))
        self.analysis.assert_not_awaited()
        self.buy.assert_not_awaited()
        self.assertEqual(row["decision"], {"outcome": "OTHER", "reasons": ["WHALE_AMOUNT_FILTER_REJECTED"],
                                           "reason": None, "missing_reason": "not_available_in_existing_flow"})

    def test_analyzer_rejection_does_not_call_quotes(self):
        self.analysis.return_value = SimpleNamespace(safety_score=20, route_type="REJECTED", reasons=["unsafe"])
        row = self.run_signal()
        self.quotes.assert_not_awaited()
        self.buy.assert_not_awaited()
        self.assertEqual(row["decision"]["outcome"], "REJECT_ANALYZER")

    def test_risk_size_rejection_preserves_missing_later_timing(self):
        self.cash.return_value = 0
        row = self.run_signal()
        self.buy.assert_not_awaited()
        self.assertEqual(row["decision"]["outcome"], "REJECT_RISK")
        self.assertIsNone(row["predictors"]["timestamps"]["preflight_started_at"])

    def test_quote_failure_is_recorded_without_buy(self):
        self.quotes.side_effect = RuntimeError("quote failed")
        row = self.run_signal()
        self.buy.assert_not_awaited()
        self.assertEqual(row["decision"]["outcome"], "QUOTE_FAILED")

    def test_rpc_failure_is_recorded_without_buy(self):
        self.analysis.side_effect = RuntimeError("getTokenSupply failed")
        row = self.run_signal()
        self.buy.assert_not_awaited()
        self.rpc_skip.assert_awaited_once()
        self.assertEqual(row["decision"]["outcome"], "RPC_SKIPPED")

    def test_ledger_admission_rejection_changes_only_outcome_metadata(self):
        self.buy.side_effect = RuntimeError("capacity reached")
        row = self.run_signal()
        self.assertEqual(row["decision"]["outcome"], "REJECT_RISK")
        self.assertIsNone(row["execution_receipt"]["trade_id"])
        self.assertIsNotNone(row["predictors"]["timestamps"]["entry_decision_at"])

    def test_actual_semaphore_wait_and_timestamp_order_are_preserved(self):
        async def scenario():
            gate = asyncio.Semaphore(0)
            with patch.object(monitor, "_analysis_limit", gate):
                enqueued = (datetime.now(timezone.utc).isoformat(), __import__("time").monotonic())
                task = asyncio.create_task(monitor.process_paper_signal("MINT", 1000, 6, 2_000_000_000,
                    "WALLET", "SIGNATURE", STAMP, _entry_telemetry_enqueued=enqueued))
                await asyncio.sleep(0.02)
                gate.release()
                await task
        asyncio.run(scenario())
        row = telemetry._row(self.rows.get_nowait())
        stamps = row["predictors"]["timestamps"]
        self.assertGreater(stamps["analysis_started_at"]["monotonic_ns"] - stamps["signal_enqueued_at"]["monotonic_ns"], 10_000_000)
        keys = ("signal_enqueued_at", "analysis_started_at", "analysis_completed_at", "risk_check_started_at",
                "risk_check_completed_at", "preflight_started_at", "preflight_completed_at", "entry_decision_at")
        values = [stamps[key]["monotonic_ns"] for key in keys]
        self.assertEqual(values, sorted(values))

    def test_scheduler_uses_actual_enqueue_clock_without_relabeling_detection(self):
        detected = datetime.fromisoformat(STAMP)
        enqueued = datetime.fromisoformat("2026-10-03T01:05:00.425384+00:00")
        process = AsyncMock()
        async def scenario():
            with patch.object(monitor, "process_paper_signal", new=process), \
                    patch.object(monitor, "track_signal_task"), \
                    patch.object(monitor, "datetime", wraps=datetime) as clock, \
                    patch.object(monitor.time, "monotonic", return_value=123.0):
                clock.now.side_effect = [detected, enqueued]
                monitor.schedule_paper_signal("MINT", 1000, 6, 2_000_000_000, "WALLET", "SIGNATURE")
                await asyncio.sleep(0)
        asyncio.run(scenario())
        self.assertEqual(process.call_args.args[6], detected.isoformat())
        captured = process.call_args.kwargs["_entry_telemetry_enqueued"]
        self.assertEqual(captured, (enqueued.isoformat(), 123.0))
        self.assertNotEqual(captured[0], process.call_args.args[6])

    def test_momentum_operands_distinguish_saturated_values_without_new_score(self):
        components = {}
        self.assertEqual(monitor.momentum_score(60_000, 40, 1, _raw_components=components), 100)
        self.assertEqual(components["volume_operand_before_cap"], 120)
        self.assertEqual(components["volume_points"], 60)
        self.assertEqual(components["imbalance_operand_before_cap"], 78)
        candidate = monitor.MomentumCandidate("MINT", "PAIR", 60_000, 40, 1, 10000, 100,
                                             momentum_score_components=components)
        row = self.run_signal(_entry_telemetry_scores=monitor._momentum_entry_telemetry_scores(candidate))
        momentum = row["predictors"]["sections"]["scores"]["momentum"]
        self.assertIsNone(momentum["uncapped_total"])
        self.assertEqual(momentum["raw_components"]["volume_operand_before_cap"], 120)

    def test_known_wallet_set_count_preserved_without_claiming_full_market(self):
        wallets = {"participating_wallet_ids": ["w1", "w2", "w3", "w4"],
                   "observed_whale_count": 4, "observed_unique_wallet_count": 4,
                   "count_is_lower_bound": True,
                   "missing_reason": "CONFIRMATION_EARLY_EXIT_FULL_MARKET_COUNT_UNKNOWN"}
        row = self.run_signal(_entry_telemetry_wallets=wallets)
        stored = row["predictors"]["sections"]["wallets"]
        self.assertEqual(stored["observed_whale_count"], 4)
        self.assertEqual(len(stored["participating_wallet_ids"]), 4)
        self.assertTrue(stored["count_is_lower_bound"])

    def test_pre_confirmation_rpc_failure_has_standalone_envelope(self):
        candidate = monitor.MomentumCandidate("MINT", "PAIR", 20000, 30, 1, 10000, 100)
        with patch.object(monitor, "_confirm_unknown_whales_with_memory_telemetry", new=AsyncMock(side_effect=RuntimeError("RPC failed"))):
            with self.assertRaises(RuntimeError):
                asyncio.run(monitor._confirm_unknown_whales_with_telemetry(None, "", candidate, set()))
        row = telemetry._row(self.rows.get_nowait())
        self.assertEqual(row["decision"]["outcome"], "RPC_SKIPPED")
        self.assertEqual(row["identity"]["route_type"], "B")
        self.buy.assert_not_awaited()

    def test_successful_confirmation_does_not_duplicate_accepted_envelope(self):
        candidate = monitor.MomentumCandidate("MINT", "PAIR", 20000, 30, 1, 10000, 100)
        whales = [monitor.UnknownWhaleBuy("W", "S", 2_000_000_000, 1000, 6)]
        with patch.object(monitor, "_confirm_unknown_whales_with_memory_telemetry", new=AsyncMock(return_value=whales)):
            result = asyncio.run(monitor._confirm_unknown_whales_with_telemetry(None, "", candidate, set()))
        self.assertEqual(result, whales)
        self.assertTrue(self.rows.empty())


if __name__ == "__main__":
    unittest.main()
