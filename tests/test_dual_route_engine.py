from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from src import analyzer, executor, monitor, risk_manager


class DualRouteSelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = analyzer.AnalyzerSettings(rpc_url="https://rpc.invalid")

    def route(self, lp: str, liquidity: str, *, authority: bool = True) -> str | None:
        return analyzer.select_route_type(
            safety_score=100 if Decimal(lp) >= 80 else 65,
            developer_below_ten_percent=True,
            mint_authority_renounced=authority,
            lp_locked_percent=Decimal(lp),
            liquidity=Decimal(liquidity),
            settings=self.settings,
        )

    def test_route_a_keeps_full_size(self) -> None:
        self.assertEqual(self.route("80", "10000"), "A")
        self.assertEqual(executor.route_sized_amount(10_000, "A"), 10_000)

    def test_route_b_uses_fifteen_percent_size(self) -> None:
        self.assertEqual(self.route("0", "10000"), "B")
        self.assertEqual(self.route("0", "10000"), "B")
        self.assertEqual(executor.route_sized_amount(10_000, "B"), 1_500)
        with self.assertRaises(ValueError):
            executor.route_sized_amount(10_000, "UNKNOWN")

    def test_route_b_fails_closed_for_low_liquidity_or_live_authority(self) -> None:
        self.assertIsNone(self.route("0", "9999.99"))
        self.assertIsNone(self.route("0", "10000", authority=False))


class RouteBRiskTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.ledger_path = Path(self.temporary.name) / "paper_trades.json"
        self.original_ledger_path = risk_manager.LEDGER_PATH
        risk_manager.LEDGER_PATH = self.ledger_path

    def tearDown(self) -> None:
        risk_manager.LEDGER_PATH = self.original_ledger_path
        self.temporary.cleanup()

    def test_route_b_position_persists_tight_stop(self) -> None:
        asyncio.run(
            risk_manager.record_paper_buy(
                "MINT_B", 100_000, 1_000, route_type="B"
            )
        )
        ledger = json.loads(self.ledger_path.read_text(encoding="utf-8"))
        position = ledger["positions"]["MINT_B"]
        self.assertEqual(position["route_type"], "B")
        self.assertEqual(position["stop_loss_ratio"], 0.90)
        self.assertEqual(ledger["events"][-1]["route_type"], "B")
        self.assertEqual(position["dex_momentum_score"], 0.0)

    def test_experiment_strategy_metadata_and_capacity_are_atomic(self) -> None:
        asyncio.run(risk_manager.record_paper_buy(
            "MINT_1",
            100_000,
            1_000,
            route_type="B",
            strategy_version="broad_discovery_v1",
            observation_id="OBS_1",
            max_strategy_open_positions=1,
        ))
        with self.assertRaisesRegex(RuntimeError, "capacity reached"):
            asyncio.run(risk_manager.record_paper_buy(
                "MINT_2",
                100_000,
                1_000,
                route_type="B",
                strategy_version="broad_discovery_v1",
                observation_id="OBS_2",
                max_strategy_open_positions=1,
            ))
        ledger = json.loads(self.ledger_path.read_text(encoding="utf-8"))
        self.assertEqual(list(ledger["positions"]), ["MINT_1"])
        self.assertEqual(
            ledger["positions"]["MINT_1"]["strategy_version"],
            "broad_discovery_v1",
        )
        self.assertEqual(
            ledger["events"][-1]["observation_id"], "OBS_1"
        )

    def test_full_experiment_sell_closes_linked_observation(self) -> None:
        asyncio.run(risk_manager.record_paper_buy(
            "MINT_1",
            100_000,
            1_000,
            route_type="B",
            strategy_version="broad_discovery_v1",
            observation_id="OBS_1",
        ))
        ledger = json.loads(self.ledger_path.read_text(encoding="utf-8"))
        position_id = ledger["positions"]["MINT_1"]["position_id"]
        with patch(
            "src.observation_tracker.mark_paper_experiment_status",
            return_value=True,
        ) as mark_status:
            self.assertTrue(asyncio.run(risk_manager.record_paper_sell(
                "MINT_1",
                1_000,
                110_000,
                "STOP_LOSS",
                position_id=position_id,
            )))
        mark_status.assert_called_once_with(
            "OBS_1",
            "CLOSED",
            position_id=position_id,
        )

    def test_route_b_takes_eighty_percent_profit_at_thirty_percent(self) -> None:
        position = {
            "mint": "MINT_B",
            "position_id": "position-b",
            "token_amount_raw": 1_000,
            "remaining_cost_lamports": 100_000,
            "route_type": "B",
            "risk_state": "NORMAL",
            "take_profit_done": False,
        }
        quote = {"outAmount": "130000", "routePlan": [{}]}
        with (
            patch.object(risk_manager, "jupiter_quote", new=AsyncMock(return_value=quote)),
            patch.object(risk_manager, "record_position_mark", new=AsyncMock()),
            patch.object(risk_manager, "_execute_paper_exit", new=AsyncMock(return_value=True)) as exit_mock,
        ):
            asyncio.run(risk_manager.evaluate_paper_position(object(), "key", position))
        self.assertEqual(exit_mock.await_args.args[3], "TAKE_PROFIT_30_SELL_80")

    def test_route_a_uses_same_thirty_percent_take_profit(self) -> None:
        position = {
            "mint": "MINT_A",
            "position_id": "position-a",
            "token_amount_raw": 1_000,
            "remaining_cost_lamports": 100_000,
            "route_type": "A",
            "risk_state": "NORMAL",
            "take_profit_done": False,
        }
        quote = {"outAmount": "130000", "routePlan": [{}]}
        with (
            patch.object(risk_manager, "jupiter_quote", new=AsyncMock(return_value=quote)),
            patch.object(risk_manager, "record_position_mark", new=AsyncMock()),
            patch.object(risk_manager, "_execute_paper_exit", new=AsyncMock(return_value=True)) as exit_mock,
        ):
            asyncio.run(risk_manager.evaluate_paper_position(object(), "key", position))
        self.assertEqual(exit_mock.await_args.args[3], "TAKE_PROFIT_30_SELL_80")

    def test_rpc_skip_does_not_lock_cash(self) -> None:
        before = risk_manager.empty_ledger()["cash_lamports"]
        asyncio.run(
            risk_manager.record_rpc_skip(
                "MINT_B", "WALLET", "SIGNATURE", "getTokenSupply failed"
            )
        )
        ledger = json.loads(self.ledger_path.read_text(encoding="utf-8"))
        self.assertEqual(ledger["cash_lamports"], before)
        self.assertEqual(ledger["positions"], {})
        self.assertEqual(ledger["events"][-1]["type"], "SKIPPED_BY_RPC_ERROR")


class MarketMomentumTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.ledger_path = Path(self.temporary.name) / "paper_trades.json"
        self.original_ledger_path = risk_manager.LEDGER_PATH
        risk_manager.LEDGER_PATH = self.ledger_path

    def tearDown(self) -> None:
        risk_manager.LEDGER_PATH = self.original_ledger_path
        self.temporary.cleanup()

    def test_route_b_revalidation_rejects_weakened_snapshot(self) -> None:
        original = monitor.MomentumCandidate(
            "MINT", "PAIR", 20_000, 30, 10, 15_000, 80.0
        )
        weaker = monitor.MomentumCandidate(
            "MINT", "PAIR", 19_999, 30, 10, 15_000, 79.99
        )
        stronger = monitor.MomentumCandidate(
            "MINT", "PAIR", 21_000, 31, 10, 15_000, 82.0
        )
        self.assertFalse(monitor.momentum_is_still_strong(original, weaker))
        self.assertTrue(monitor.momentum_is_still_strong(original, stronger))

    def test_pair_requires_burst_and_minimum_liquidity(self) -> None:
        pair = {
            "chainId": "solana",
            "pairAddress": "PAIR",
            "baseToken": {"address": "MINT"},
            "txns": {"m5": {"buys": 25, "sells": 10}},
            "volume": {"m5": 15_000},
            "liquidity": {"usd": 10_000},
            "pairCreatedAt": 1_000_000,
        }
        candidate = monitor.momentum_candidate_from_pair(
            pair,
            now_ms=1_900_000,
        )
        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(candidate.mint, "MINT")
        self.assertGreater(candidate.momentum_score, 0)

        pair["liquidity"]["usd"] = 9_999
        self.assertIsNone(
            monitor.momentum_candidate_from_pair(pair, now_ms=1_900_000)
        )

    def test_pair_requires_all_burst_conditions_and_minimum_age(self) -> None:
        pair = {
            "chainId": "solana",
            "pairAddress": "PAIR",
            "baseToken": {"address": "MINT"},
            "txns": {"m5": {"buys": 34, "sells": 10}},
            "volume": {"m5": 15_000},
            "liquidity": {"usd": 10_000},
            "pairCreatedAt": 1_000_000,
        }
        self.assertIsNotNone(
            monitor.momentum_candidate_from_pair(pair, now_ms=1_900_000)
        )
        pair["volume"]["m5"] = 14_999.99
        self.assertIsNone(
            monitor.momentum_candidate_from_pair(pair, now_ms=1_900_000)
        )
        pair["volume"]["m5"] = 15_000
        pair["txns"]["m5"] = {"buys": 24, "sells": 10}
        self.assertIsNone(
            monitor.momentum_candidate_from_pair(pair, now_ms=1_900_000)
        )
        pair["txns"]["m5"] = {"buys": 34, "sells": 10}
        self.assertIsNone(
            monitor.momentum_candidate_from_pair(pair, now_ms=1_899_999)
        )
        pair["txns"]["m5"] = {"buys": 35, "sells": 20}
        self.assertIsNone(
            monitor.momentum_candidate_from_pair(pair, now_ms=1_900_000)
        )
        pair["txns"]["m5"] = {"buys": 36, "sells": 20}
        self.assertIsNotNone(
            monitor.momentum_candidate_from_pair(pair, now_ms=1_900_000)
        )

    def test_near_miss_preserves_all_momentum_filter_reasons(self) -> None:
        pair = {
            "chainId": "solana",
            "pairAddress": "PAIR",
            "baseToken": {"address": "MINT"},
            "txns": {"m5": {"buys": 10, "sells": 10}},
            "volume": {"m5": 14_999},
            "liquidity": {"usd": 9_999},
            "pairCreatedAt": 1_000_001,
        }
        evaluated = monitor.momentum_shadow_candidate_from_pair(
            pair,
            now_ms=1_900_000,
        )
        self.assertIsNotNone(evaluated)
        assert evaluated is not None
        self.assertEqual(set(evaluated.rejection_reasons), {
            "PAIR_TOO_YOUNG",
            "MOMENTUM_LIQUIDITY_UNDER_MIN",
            "MOMENTUM_VOLUME_UNDER_MIN",
            "MOMENTUM_NET_BUYS_UNDER_MIN",
            "MOMENTUM_BUY_SELL_RATIO_UNDER_MIN",
        })
        self.assertIsNone(
            monitor.momentum_candidate_from_pair(pair, now_ms=1_900_000)
        )

    def test_invalid_pair_identity_is_not_a_shadow_candidate(self) -> None:
        base = {
            "chainId": "solana",
            "pairAddress": "PAIR",
            "baseToken": {"address": "MINT"},
            "pairCreatedAt": 1_000_000,
        }
        for override in (
            {"chainId": "ethereum"},
            {"pairAddress": ""},
            {"baseToken": {}},
            {"baseToken": {"address": monitor.WSOL_MINT}},
        ):
            pair = {**base, **override}
            self.assertIsNone(
                monitor.momentum_shadow_candidate_from_pair(
                    pair,
                    now_ms=1_900_000,
                )
            )

    def test_malformed_momentum_metrics_fail_closed_into_shadow_reason(self) -> None:
        pair = {
            "chainId": "solana",
            "pairAddress": "PAIR",
            "baseToken": {"address": "MINT"},
            "txns": {"m5": {"buys": "bad", "sells": 10}},
            "volume": {"m5": float("nan")},
            "liquidity": {"usd": 10_000},
            "pairCreatedAt": "bad",
        }
        evaluated = monitor.momentum_shadow_candidate_from_pair(
            pair,
            now_ms=1_900_000,
        )
        self.assertIsNotNone(evaluated)
        assert evaluated is not None
        self.assertIn("MOMENTUM_METRIC_INVALID", evaluated.rejection_reasons)
        self.assertIn("PAIR_CREATED_AT_INVALID", evaluated.rejection_reasons)
        self.assertIsNone(
            monitor.momentum_candidate_from_pair(pair, now_ms=1_900_000)
        )

    def test_market_shadow_scheduler_enforces_interval_and_mint_cooldown(self) -> None:
        candidate = monitor.MomentumCandidate(
            "MINT", "PAIR", 14_000, 20, 10, 9_000, 50, 1_000
        )
        fake_task = MagicMock()

        def create_task(coroutine):
            coroutine.close()
            return fake_task

        process = AsyncMock()
        with (
            patch.object(monitor, "_market_shadow_cooldowns", {}),
            patch.object(monitor, "_shadow_signal_tasks", set()),
            patch.object(monitor, "_signal_tasks", set()),
            patch.object(monitor, "_last_market_shadow_capture_at", 0.0),
            patch.object(monitor.asyncio, "create_task", side_effect=create_task),
            patch.object(monitor, "process_paper_signal", new=process),
        ):
            self.assertTrue(monitor.schedule_market_shadow(
                candidate,
                ("MOMENTUM_VOLUME_UNDER_MIN",),
                now=100,
            ))
            self.assertFalse(monitor.schedule_market_shadow(
                candidate,
                ("MOMENTUM_VOLUME_UNDER_MIN",),
                now=200,
            ))
        self.assertEqual(
            process.call_args.kwargs["prefilter_reasons"],
            ("MOMENTUM_VOLUME_UNDER_MIN",),
        )

    def test_fetch_cohorts_keeps_near_miss_out_of_trading_candidates(self) -> None:
        approved_pair = {
            "chainId": "solana",
            "pairAddress": "APPROVED_PAIR",
            "baseToken": {"address": "APPROVED_MINT"},
            "txns": {"m5": {"buys": 36, "sells": 20}},
            "volume": {"m5": 15_000},
            "liquidity": {"usd": 10_000},
            "pairCreatedAt": 1,
        }
        shadow_pair = {
            **approved_pair,
            "pairAddress": "SHADOW_PAIR",
            "baseToken": {"address": "SHADOW_MINT"},
            "volume": {"m5": 14_999},
        }
        with (
            patch.object(monitor.time, "time", return_value=2_000),
            patch.object(
                monitor,
                "_dexscreener_json",
                new=AsyncMock(side_effect=[
                    {"pairs": [approved_pair, shadow_pair]},
                    [],
                    [],
                ]),
            ),
        ):
            approved, shadows = asyncio.run(
                monitor.fetch_momentum_candidate_cohorts(object())
            )
        self.assertEqual([row.mint for row in approved], ["APPROVED_MINT"])
        self.assertEqual(
            [row.candidate.mint for row in shadows], ["SHADOW_MINT"]
        )
        self.assertEqual(
            shadows[0].rejection_reasons,
            ("MOMENTUM_VOLUME_UNDER_MIN",),
        )
    def test_route_a_never_inherits_relaxed_route_b_analysis(self) -> None:
        self.assertTrue(monitor.route_report_allowed("A", "A"))
        self.assertFalse(monitor.route_report_allowed("A", "B"))
        self.assertTrue(monitor.route_report_allowed("B", "A"))
        self.assertTrue(monitor.route_report_allowed("B", "B"))

    def test_unknown_whale_must_be_unregistered_signer_with_token_inflow(self) -> None:
        transaction = {
            "transaction": {
                "signatures": ["SIG"],
                "message": {
                    "accountKeys": [
                        {"pubkey": "UNKNOWN", "signer": True},
                        {"pubkey": "KNOWN", "signer": True},
                    ]
                },
            },
            "meta": {
                "fee": 5_000,
                "preBalances": [3_000_005_000, 3_000_000_000],
                "postBalances": [1_000_000_000, 1_000_000_000],
                "preTokenBalances": [],
                "postTokenBalances": [
                    {
                        "owner": "UNKNOWN",
                        "mint": "MINT",
                        "uiTokenAmount": {"amount": "500", "decimals": 6},
                    },
                    {
                        "owner": "KNOWN",
                        "mint": "MINT",
                        "uiTokenAmount": {"amount": "500", "decimals": 6},
                    },
                ],
            },
        }
        buys = monitor.unknown_whale_buy_from_transaction(
            transaction, "MINT", {"KNOWN"}
        )
        self.assertEqual(len(buys), 1)
        self.assertEqual(buys[0].wallet, "UNKNOWN")
        self.assertEqual(buys[0].paid_lamports, 2_000_000_000)

    def test_momentum_metadata_is_isolated_in_position_and_event(self) -> None:
        asyncio.run(
            risk_manager.record_paper_buy(
                "MINT_B",
                100_000,
                1_000,
                route_type="B",
                dex_momentum_score=87.12567,
            )
        )
        ledger = json.loads(self.ledger_path.read_text(encoding="utf-8"))
        position = ledger["positions"]["MINT_B"]
        event = ledger["events"][-1]
        self.assertEqual(position["route_type"], "B")
        self.assertEqual(position["dex_momentum_score"], 87.1257)
        self.assertEqual(event["dex_momentum_score"], 87.1257)

    def test_v2_ledger_backfills_route_metadata(self) -> None:
        document = {
            "schema_version": 2,
            "positions": {"MINT": {"mint": "MINT"}},
            "events": [
                {"type": "BUY", "mint": "MINT"},
                {"type": "SELL", "mint": "MINT"},
            ],
        }
        self.assertTrue(risk_manager.migrate_ledger_document(document))
        self.assertEqual(document["positions"]["MINT"]["route_type"], "A")
        self.assertEqual(document["events"][0]["dex_momentum_score"], 0.0)
        self.assertEqual(
            document["positions"]["MINT"]["strategy_version"],
            "baseline_v1",
        )
        self.assertIsNone(document["positions"]["MINT"]["observation_id"])
        self.assertEqual(document["events"][1]["strategy_version"], "baseline_v1")
        self.assertIsNone(document["events"][1]["observation_id"])
        self.assertFalse(risk_manager.migrate_ledger_document(document))

    def test_v1_ledger_adds_strategy_metadata_in_one_migration(self) -> None:
        document = {
            "schema_version": 1,
            "positions": {
                "MINT": {
                    "mint": "MINT",
                    "token_amount_raw": 1_000,
                    "remaining_cost_lamports": 100_000,
                }
            },
            "events": [
                {"type": "BUY", "mint": "MINT"},
                {"type": "SELL", "mint": "MINT"},
            ],
        }
        self.assertTrue(risk_manager.migrate_ledger_document(document))
        self.assertEqual(
            document["positions"]["MINT"]["strategy_version"],
            "baseline_v1",
        )
        self.assertIsNone(document["positions"]["MINT"]["observation_id"])
        self.assertEqual(document["events"][0]["strategy_version"], "baseline_v1")
        self.assertIsNone(document["events"][1]["observation_id"])


if __name__ == "__main__":
    unittest.main()
