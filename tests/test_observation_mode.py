from __future__ import annotations

import asyncio
import concurrent.futures
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from src import analyzer, executor, monitor, observation_tracker, risk_manager


class ObservationLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.original_path = observation_tracker.OBSERVATION_PATH
        observation_tracker.OBSERVATION_PATH = (
            Path(self.temporary.name) / "signal_observations.json"
        )

    def tearDown(self) -> None:
        observation_tracker.OBSERVATION_PATH = self.original_path
        self.temporary.cleanup()

    def record(
        self,
        *,
        mint: str = "MINT",
        signature: str = "SIGNATURE",
        route: str = "B",
        score: float = 95.0,
    ) -> bool:
        return asyncio.run(observation_tracker.record_observation(
            mint=mint,
            route_type=route,
            source_wallet="WALLET",
            source_signature=signature,
            safety_score=100,
            entry_cost_lamports=1_000,
            token_amount_raw=500,
            token_decimals=6,
            entry_price_impact_pct=0.1,
            exit_price_impact_pct=0.2,
            expected_slippage_bps=100,
            dex_momentum_score=score,
            signal_detected_at="2026-07-30T00:00:00+00:00",
            analysis_completed_at="2026-07-30T00:00:01+00:00",
            entry_quote_at="2026-07-30T00:00:02+00:00",
            entry_latency_ms=2_000,
            momentum_metrics={
                "volume_m5_usd": 20_000,
                "buys_m5": 30,
                "sells_m5": 10,
                "net_buys_m5": 20,
                "buy_sell_ratio_m5": 3,
                "liquidity_usd": 12_000,
                "pair_age_seconds": 1_000,
                "unknown_whale_count": 3,
            },
        ))

    def test_observation_is_idempotent_and_does_not_create_position(self) -> None:
        self.assertTrue(self.record())
        self.assertFalse(self.record())
        document = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )
        self.assertEqual(len(document["observations"]), 1)
        self.assertNotIn("positions", document)
        self.assertNotIn("cash_lamports", document)
        self.assertTrue(document["observations"][0]["candidate_v2_eligible"])
        self.assertEqual(document["schema_version"], 2)

    def test_candidate_v2_score_boundaries(self) -> None:
        cases = (
            (89.9999, False),
            (90.0, True),
            (99.9999, True),
            (100.0, False),
        )
        for index, (score, eligible) in enumerate(cases):
            self.record(
                mint=f"MINT-{index}",
                signature=f"SIGNATURE-{index}",
                score=score,
            )
        rows = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"]
        self.assertEqual(
            [row["candidate_v2_eligible"] for row in rows],
            [eligible for _, eligible in cases],
        )

    def test_candidate_v2_limits_same_mint_to_once_per_24_hours(self) -> None:
        with patch.object(observation_tracker.time, "time", return_value=1_000):
            self.record(signature="FIRST")
        with patch.object(
            observation_tracker.time,
            "time",
            return_value=1_000 + 86_399,
        ):
            self.record(signature="TOO-SOON")
        with patch.object(
            observation_tracker.time,
            "time",
            return_value=1_000 + 86_400,
        ):
            self.record(signature="ALLOWED")
        rows = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"]
        self.assertEqual(
            [row["candidate_v2_eligible"] for row in rows],
            [True, False, True],
        )
        self.assertEqual(
            rows[1]["candidate_v2_filter_reasons"],
            ["MINT_SEEN_WITHIN_24H"],
        )

    def test_concurrent_same_mint_has_only_one_candidate(self) -> None:
        def write(index: int) -> bool:
            return self.record(signature=f"CONCURRENT-{index}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(write, range(2)))
        self.assertEqual(results, [True, True])
        rows = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"]
        self.assertEqual(
            sum(row["candidate_v2_eligible"] is True for row in rows),
            1,
        )

    def test_samples_capture_interval_returns_and_extremes(self) -> None:
        self.record()
        row = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        observation_id = row["observation_id"]
        self.assertTrue(observation_tracker.record_sample(
            observation_id, "1m", proceeds_lamports=900
        ))
        self.assertTrue(observation_tracker.record_sample(
            observation_id, "5m", proceeds_lamports=1_200
        ))
        self.assertTrue(observation_tracker.record_sample(
            observation_id, "15m", proceeds_lamports=1_100
        ))
        updated = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        self.assertEqual(updated["status"], "COMPLETE")
        self.assertEqual(updated["min_return_percent"], -10.0)
        self.assertEqual(updated["max_return_percent"], 20.0)
        self.assertTrue(updated["candidate_v2_early_failure"])

    def test_no_route_at_one_minute_is_not_an_early_failure(self) -> None:
        self.record()
        row = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        observation_tracker.record_sample(
            row["observation_id"],
            "1m",
            proceeds_lamports=None,
            error="no route",
        )
        updated = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        self.assertIsNone(updated["candidate_v2_early_failure"])
        self.assertEqual(updated["status"], "PENDING")


class ObservationEntryGateTests(unittest.TestCase):
    def test_approved_signal_is_observed_without_paper_buy(self) -> None:
        report = SimpleNamespace(
            safety_score=100,
            route_type="A",
            reasons=[],
            liquidity_usd="10000",
            lp_locked_percent="80",
        )
        quote = AsyncMock(side_effect=[
            {
                "outAmount": "2500",
                "routePlan": [{}],
                "priceImpactPct": "0.5",
                "slippageBps": "100",
            },
            {
                "outAmount": "990000",
                "routePlan": [{}],
                "priceImpactPct": "0.4",
                "slippageBps": "100",
            },
        ])
        observed = AsyncMock(return_value=True)
        record_buy = AsyncMock()
        record_buy_success = AsyncMock()
        with (
            patch.dict("os.environ", {"OBSERVATION_MODE": "true"}),
            patch.object(
                analyzer, "analyze_token", new=AsyncMock(return_value=report)
            ),
            patch.object(
                monitor.state_store,
                "get_route_initial_stop_streak",
                return_value=(0, 0),
            ),
            patch.object(
                monitor, "token_cooldown_is_active", return_value=False
            ),
            patch.object(
                risk_manager,
                "paper_cash_balance",
                new=AsyncMock(return_value=10_000_000_000),
            ),
            patch.object(
                risk_manager, "record_paper_buy", new=record_buy
            ),
            patch.object(executor, "jupiter_quote", new=quote),
            patch.object(
                observation_tracker, "record_observation", new=observed
            ),
            patch(
                "src.wallet_performance.record_paper_buy_success",
                new=record_buy_success,
            ),
        ):
            asyncio.run(monitor.process_paper_signal(
                "MINT",
                1_000,
                6,
                2_000_000_000,
                "WALLET",
                "SIGNATURE",
                "2026-07-30T00:00:00+00:00",
                "A",
            ))
        observed.assert_awaited_once()
        record_buy.assert_not_awaited()
        record_buy_success.assert_not_awaited()
        self.assertEqual(quote.await_count, 2)


if __name__ == "__main__":
    unittest.main()
