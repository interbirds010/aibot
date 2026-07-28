import asyncio
import json
import tempfile
import unittest
from pathlib import Path

import src.monitor as monitor
import src.wallet_performance as performance
from src.wallet_feeder import FeederSettings, WalletScore, score_rank, select_active_wallets


SOL = 1_000_000_000


class WalletPerformanceIsolationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.original_path = performance.PERFORMANCE_PATH
        performance.PERFORMANCE_PATH = Path(self.temporary.name) / "wallet_performance.json"
        monitor._whale_buy_history.clear()
        monitor._last_history_cleanup_at = 0.0

    def tearDown(self):
        performance.PERFORMANCE_PATH = self.original_path
        self.temporary.cleanup()

    def test_low_intensity_buy_is_observed_but_not_copy_approved(self):
        asyncio.run(
            performance.observe_buy(
                "wallet-a", "mint-a", 2_000_000, int(0.5 * SOL), "signature-a"
            )
        )
        approved = monitor.whale_buy_amount_allowed(
            int(0.5 * SOL),
            wallet="wallet-a",
            mint="mint-a",
            signature="signature-a",
            observed_at=1_000.0,
        )

        state = json.loads(performance.PERFORMANCE_PATH.read_text(encoding="utf-8"))
        row = state["wallets"]["wallet-a"]
        self.assertFalse(approved)
        self.assertEqual(row["processed_count"], 1)
        self.assertEqual(row["evaluation_pending_count"], 1)
        self.assertEqual(row["pending"][0]["status"], "PENDING")

    def test_processed_count_is_once_per_signature_but_each_mint_is_observed(self):
        asyncio.run(
            performance.observe_buy("wallet-a", "mint-a", 10, SOL, "signature-a")
        )
        asyncio.run(
            performance.observe_buy("wallet-a", "mint-b", 20, SOL, "signature-a")
        )

        state = json.loads(performance.PERFORMANCE_PATH.read_text(encoding="utf-8"))
        row = state["wallets"]["wallet-a"]
        self.assertEqual(row["processed_count"], 1)
        self.assertEqual(row["evaluation_pending_count"], 2)

    def test_no_route_skip_removes_pending_without_changing_performance(self):
        asyncio.run(
            performance.observe_buy(
                "wallet-a",
                "mint-a",
                10,
                SOL,
                "signature-a",
            )
        )
        state = json.loads(
            performance.PERFORMANCE_PATH.read_text(encoding="utf-8")
        )
        sample = dict(state["wallets"]["wallet-a"]["pending"][0])

        self.assertTrue(
            performance.skip_observation("wallet-a", sample)
        )
        self.assertFalse(
            performance.skip_observation("wallet-a", sample)
        )

        state = json.loads(
            performance.PERFORMANCE_PATH.read_text(encoding="utf-8")
        )
        row = state["wallets"]["wallet-a"]
        self.assertEqual(row["processed_count"], 1)
        self.assertEqual(row["evaluation_pending_count"], 0)
        self.assertEqual(row["evaluation_skipped_count"], 1)
        self.assertEqual(row["wins"], 0)
        self.assertEqual(row["losses"], 0)
        self.assertEqual(row["samples"], [])
        self.assertEqual(
            row["last_evaluation_skip"]["reason"],
            "NO_ROUTE_PERFORMANCE",
        )


class WalletFeederRankingTests(unittest.TestCase):
    def test_rank_combines_copy_success_and_raw_onchain_performance(self):
        proven = WalletScore(
            "proven", 1.0, 10, wins=8, losses=2, win_rate=80,
            average_return_percent=100, cumulative_return_percent=1_000,
            processed_count=40, paper_buy_count=4,
        )
        activity_only = WalletScore(
            "activity", 100.0, 10, processed_count=200,
        )
        self.assertGreater(score_rank(proven), score_rank(activity_only))

    def test_elite_incumbent_remains_locked_and_selected(self):
        settings = FeederSettings(
            rpc_url="https://example.invalid", max_wallets=2, elite_reserved_slots=1
        )
        elite = WalletScore(
            "elite", 1.0, 1, wins=4, losses=1, win_rate=80,
            cumulative_return_percent=500, processed_count=10,
            paper_buy_count=2, is_elite=True, locked=True,
        )
        newcomers = [
            WalletScore("new-a", 500.0, 1),
            WalletScore("new-b", 400.0, 1),
        ]
        selected = select_active_wallets(
            [*newcomers, elite], {"elite"}, settings
        )
        self.assertIn("elite", [row.address for row in selected])
        self.assertTrue(next(row for row in selected if row.address == "elite").locked)


if __name__ == "__main__":
    unittest.main()
