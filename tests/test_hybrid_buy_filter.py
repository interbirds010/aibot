import logging
import unittest

import src.monitor as monitor


SOL = 1_000_000_000


class HybridBuyFilterTests(unittest.TestCase):
    def setUp(self):
        monitor._whale_buy_history.clear()
        monitor._last_history_cleanup_at = 0.0

    def allowed(self, sol: float, *, wallet="wallet-a", mint="mint-a", at=1_000.0):
        return monitor.whale_buy_amount_allowed(
            int(sol * SOL),
            wallet=wallet,
            mint=mint,
            signature=f"sig-{sol}-{at}",
            observed_at=at,
        )

    def test_exactly_1_5_sol_is_approved_by_single_strength(self):
        with self.assertLogs("smart-money-monitor", level=logging.INFO) as logs:
            self.assertTrue(self.allowed(1.5))
        self.assertTrue(any("Approved by Single Strength (1.50 SOL)" in x for x in logs.output))

    def test_one_sol_or_less_is_always_rejected_and_not_accumulated(self):
        for index in range(10):
            self.assertFalse(self.allowed(1.0, at=1_000.0 + index))
        self.assertNotIn(("wallet-a", "mint-a"), monitor._whale_buy_history)

    def test_sub_1_5_sol_trades_approve_at_five_sol_accumulated(self):
        for index in range(3):
            self.assertFalse(self.allowed(1.25, at=1_000.0 + index * 10))
        with self.assertLogs("smart-money-monitor", level=logging.INFO) as logs:
            self.assertTrue(self.allowed(1.25, at=1_030.0))
        self.assertTrue(any("Approved by 3Min Accumulation (5.00 SOL Total)" in x
                            for x in logs.output))
        self.assertNotIn(("wallet-a", "mint-a"), monitor._whale_buy_history)

    def test_trades_older_than_180_seconds_are_removed(self):
        self.assertFalse(self.allowed(1.25, at=1_000.0))
        self.assertFalse(self.allowed(1.25, at=1_181.0))
        buffered = monitor._whale_buy_history[("wallet-a", "mint-a")]
        self.assertEqual(list(buffered), [(1_181.0, int(1.25 * SOL))])

    def test_accumulation_is_isolated_by_wallet_and_mint(self):
        for index in range(3):
            self.assertFalse(self.allowed(1.25, wallet="wallet-a", at=1_000.0 + index))
        self.assertFalse(self.allowed(1.25, wallet="wallet-b", at=1_003.0))
        self.assertFalse(self.allowed(1.25, mint="mint-b", at=1_004.0))
        self.assertTrue(self.allowed(1.25, wallet="wallet-a", mint="mint-a", at=1_005.0))


if __name__ == "__main__":
    unittest.main()
