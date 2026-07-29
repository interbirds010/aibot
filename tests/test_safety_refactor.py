from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from src import analyzer, executor, monitor, risk_manager, state_store


class ExitPreflightTests(unittest.TestCase):
    def test_exit_impact_rejects_exact_threshold(self) -> None:
        self.assertEqual(
            executor.validate_exit_price_impact(
                {"priceImpactPct": "3.4999"}
            ),
            3.4999,
        )
        with self.assertRaisesRegex(RuntimeError, "ENTRY_REJECTED"):
            executor.validate_exit_price_impact(
                {"priceImpactPct": "3.5"}
            )

    def test_signal_uses_buy_output_for_exit_preflight_and_fails_closed(
        self,
    ) -> None:
        report = SimpleNamespace(
            safety_score=100,
            route_type="A",
            reasons=[],
            liquidity_usd="10000",
            lp_locked_percent="80",
        )
        buy_quote = {
            "outAmount": "2500",
            "routePlan": [{}],
            "priceImpactPct": "0.5",
            "slippageBps": "100",
        }
        rejection = AsyncMock()
        record_buy = AsyncMock()
        quote = AsyncMock(
            side_effect=[
                buy_quote,
                RuntimeError("Jupiter returned no executable route"),
            ]
        )
        with (
            patch.object(
                analyzer,
                "analyze_token",
                new=AsyncMock(return_value=report),
            ),
            patch.object(
                monitor.state_store,
                "get_route_initial_stop_streak",
                return_value=(0, 0),
            ),
            patch.object(
                monitor,
                "token_cooldown_is_active",
                return_value=False,
            ),
            patch.object(
                risk_manager,
                "paper_cash_balance",
                new=AsyncMock(return_value=10_000_000_000),
            ),
            patch.object(
                risk_manager,
                "record_paper_rejection",
                new=rejection,
            ),
            patch.object(
                risk_manager,
                "record_paper_buy",
                new=record_buy,
            ),
            patch.object(executor, "jupiter_quote", new=quote),
        ):
            asyncio.run(
                monitor.process_paper_signal(
                    "MINT",
                    1_000,
                    6,
                    2_000_000_000,
                    "WALLET",
                    "SIGNATURE",
                    "2026-07-28T00:00:00+00:00",
                    "A",
                )
            )
        self.assertEqual(quote.await_count, 2)
        self.assertEqual(quote.await_args_list[1].args[4], 2500)
        rejection.assert_awaited_once()
        record_buy.assert_not_awaited()

    def test_route_a_cooldown_blocks_before_analysis(self) -> None:
        analyze = AsyncMock()
        with (
            patch.object(
                monitor.state_store,
                "get_route_initial_stop_streak",
                return_value=(0, 0),
            ),
            patch.object(
                monitor,
                "token_cooldown_is_active",
                return_value=True,
            ),
            patch.object(analyzer, "analyze_token", new=analyze),
        ):
            asyncio.run(
                monitor.process_paper_signal(
                    "MINT",
                    1_000,
                    6,
                    2_000_000_000,
                    "WALLET",
                    "SIGNATURE",
                    "2026-07-28T00:00:00+00:00",
                    "A",
                )
            )
        analyze.assert_not_awaited()


class StopLossBlacklistLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "paper_trades.json"
        self.original_path = state_store.PAPER_TRADES_PATH
        state_store.PAPER_TRADES_PATH = self.path

    def tearDown(self) -> None:
        state_store.PAPER_TRADES_PATH = self.original_path
        self.temporary.cleanup()

    def test_recent_stop_loss_lookup_uses_unique_token_cap(self) -> None:
        events = [
            {
                "type": "SELL",
                "reason": "ROUTE_B_STOP_LOSS_10",
                "mint": f"MINT-{index}",
                "at": f"2026-07-28T00:{index:02d}:00+00:00",
            }
            for index in range(3)
        ]
        self.path.write_text(
            json.dumps({"events": events}),
            encoding="utf-8",
        )
        expected = state_store.datetime_from_iso(events[-1]["at"])
        self.assertEqual(
            state_store.get_recent_stop_loss_time(
                "MINT-2",
                maximum_tokens=2,
            ),
            expected,
        )
        self.assertEqual(
            state_store.get_recent_stop_loss_time(
                "MINT-0",
                maximum_tokens=2,
            ),
            0.0,
        )

    def test_route_a_initial_stop_streak_resets_on_non_stop_exit(self) -> None:
        events = [
            {
                "type": "BUY",
                "position_id": "OLD",
                "route_type": "A",
                "at": "2026-07-28T00:00:00+00:00",
            },
            {
                "type": "SELL",
                "position_id": "OLD",
                "reason": "POST_TP_TRAILING_STOP_50",
                "at": "2026-07-28T00:10:00+00:00",
            },
            {
                "type": "BUY",
                "position_id": "LOSS-1",
                "route_type": "A",
                "at": "2026-07-28T01:00:00+00:00",
            },
            {
                "type": "SELL",
                "position_id": "LOSS-1",
                "reason": "STOP_LOSS_15",
                "at": "2026-07-28T01:10:00+00:00",
            },
            {
                "type": "BUY",
                "position_id": "LOSS-2",
                "route_type": "A",
                "at": "2026-07-28T02:00:00+00:00",
            },
            {
                "type": "SELL",
                "position_id": "LOSS-2",
                "reason": "STOP_LOSS_15",
                "at": "2026-07-28T02:10:00+00:00",
            },
        ]
        self.path.write_text(json.dumps({"events": events}), encoding="utf-8")
        streak, latest = state_store.get_route_initial_stop_streak("A")
        self.assertEqual(streak, 2)
        self.assertEqual(
            latest,
            state_store.datetime_from_iso("2026-07-28T02:10:00+00:00"),
        )


class PaperExitOptimizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "paper_trades.json"
        self.original_path = risk_manager.LEDGER_PATH
        risk_manager.LEDGER_PATH = self.path

    def tearDown(self) -> None:
        risk_manager.LEDGER_PATH = self.original_path
        self.temporary.cleanup()

    def test_stop_loss_reuses_detection_quote_and_records_latency(self) -> None:
        asyncio.run(
            risk_manager.record_paper_buy(
                "MINT",
                100_000,
                1_000,
                route_type="B",
            )
        )
        position = risk_manager.read_ledger()["positions"]["MINT"]
        quote = {
            "outAmount": "90000",
            "routePlan": [{}],
            "slippageBps": "1000",
        }
        quote_mock = AsyncMock(return_value=quote)
        with patch.object(
            risk_manager,
            "jupiter_quote",
            new=quote_mock,
        ):
            asyncio.run(
                risk_manager.evaluate_paper_position(
                    object(),
                    "key",
                    position,
                )
            )
        ledger = risk_manager.read_ledger()
        self.assertNotIn("MINT", ledger["positions"])
        sell = ledger["events"][-1]
        self.assertEqual(sell["proceeds_lamports"], 90_000)
        self.assertIsNotNone(sell["exit_trigger_latency_ms"])
        self.assertEqual(quote_mock.await_count, 1)

    def test_stale_detection_amount_releases_claim_without_requote(self) -> None:
        position = {
            "mint": "MINT",
            "position_id": "POSITION",
        }
        release = AsyncMock()
        quote_mock = AsyncMock()
        with (
            patch.object(
                risk_manager,
                "claim_position_exit",
                new=AsyncMock(return_value=("EXIT", 999)),
            ),
            patch.object(
                risk_manager,
                "release_exit_claim",
                new=release,
            ),
            patch.object(
                risk_manager,
                "jupiter_quote",
                new=quote_mock,
            ),
        ):
            result = asyncio.run(
                risk_manager._execute_paper_exit(
                    object(),
                    "key",
                    position,
                    "ROUTE_B_STOP_LOSS_10",
                    -10.0,
                    detected_quote={"outAmount": "90000"},
                    detected_amount=1_000,
                )
            )
        self.assertFalse(result)
        release.assert_awaited_once()
        quote_mock.assert_not_awaited()

    def test_first_take_profit_quotes_exactly_eighty_percent(self) -> None:
        record = AsyncMock(return_value=True)
        quote = AsyncMock(return_value={"outAmount": "104000", "routePlan": [{}]})
        with (
            patch.object(
                risk_manager,
                "claim_position_exit",
                new=AsyncMock(return_value=("EXIT", 1_000)),
            ),
            patch.object(risk_manager, "record_paper_sell", new=record),
            patch.object(risk_manager, "jupiter_quote", new=quote),
        ):
            self.assertTrue(
                asyncio.run(
                    risk_manager._execute_paper_exit(
                        object(),
                        "key",
                        {"mint": "MINT", "position_id": "POSITION"},
                        "TAKE_PROFIT_30_SELL_80",
                        30.0,
                    )
                )
            )
        self.assertEqual(quote.await_args.args[4], 800)
        self.assertEqual(record.await_args.args[1], 800)

    def test_first_take_profit_leaves_twenty_percent_and_resets_peak(self) -> None:
        asyncio.run(
            risk_manager.record_paper_buy(
                "MINT",
                100_000,
                1_000,
                route_type="B",
            )
        )
        position_id = risk_manager.read_ledger()["positions"]["MINT"][
            "position_id"
        ]
        self.assertTrue(
            asyncio.run(
                risk_manager.record_paper_sell(
                    "MINT",
                    800,
                    104_000,
                    "TAKE_PROFIT_30_SELL_80",
                    position_id=position_id,
                )
            )
        )
        position = risk_manager.read_ledger()["positions"]["MINT"]
        self.assertEqual(position["token_amount_raw"], 200)
        self.assertEqual(position["remaining_cost_lamports"], 20_000)
        self.assertEqual(position["post_tp_peak_exit_value_lamports"], 0)

    def test_legacy_position_uses_original_entry_price_fallback(self) -> None:
        position = {
            "token_amount_raw": 400,
            "remaining_cost_lamports": 40_000,
            "entry_token_amount_raw": 1_000,
            "entry_cost_lamports": 100_000,
            "take_profit_done": True,
        }
        self.assertEqual(
            risk_manager.break_even_required_value(position),
            40_000,
        )

    def test_post_take_profit_trailing_stop_triggers_at_half_peak(self) -> None:
        asyncio.run(
            risk_manager.record_paper_buy(
                "MINT",
                100_000,
                1_000,
                route_type="B",
            )
        )
        position_id = risk_manager.read_ledger()["positions"]["MINT"][
            "position_id"
        ]
        asyncio.run(
            risk_manager.record_paper_sell(
                "MINT",
                800,
                104_000,
                "TAKE_PROFIT_30_SELL_80",
                position_id=position_id,
            )
        )
        position = risk_manager.read_ledger()["positions"]["MINT"]
        first_quote = {"outAmount": "40_000", "routePlan": [{}], "slippageBps": "1000"}
        with patch.object(risk_manager, "jupiter_quote", new=AsyncMock(return_value=first_quote)):
            asyncio.run(risk_manager.evaluate_paper_position(object(), "key", position))
        position = risk_manager.read_ledger()["positions"]["MINT"]
        self.assertEqual(position["post_tp_peak_exit_value_lamports"], 40_000)
        quote_mock = AsyncMock(
            return_value={"outAmount": "20000", "routePlan": [{}], "slippageBps": "1000"}
        )
        with patch.object(
            risk_manager,
            "jupiter_quote",
            new=quote_mock,
        ):
            asyncio.run(
                risk_manager.evaluate_paper_position(
                    object(),
                    "key",
                    position,
                )
            )
        ledger = risk_manager.read_ledger()
        self.assertNotIn("MINT", ledger["positions"])
        self.assertEqual(
            ledger["events"][-1]["reason"],
            "POST_TP_TRAILING_STOP_50",
        )
        self.assertEqual(quote_mock.await_count, 1)


if __name__ == "__main__":
    unittest.main()
