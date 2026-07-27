from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src import monitor, wallet_performance


class RouteBPrecisionFilterTests(unittest.TestCase):
    def report(
        self,
        *,
        safety_score: float = 55,
        liquidity_usd: float = 7_500,
        lp_locked_percent: float = 40,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            safety_score=safety_score,
            liquidity_usd=str(liquidity_usd),
            lp_locked_percent=str(lp_locked_percent),
        )

    def test_route_b_accepts_exact_lower_bounds(self) -> None:
        self.assertTrue(
            monitor.route_b_safety_filter(
                self.report(),
                "MINT",
            )
        )

    def test_route_b_rejects_safety_score_under_55(self) -> None:
        with self.assertLogs("smart-money-monitor", level="WARNING") as captured:
            allowed = monitor.route_b_safety_filter(
                self.report(safety_score=54.99),
                "MINT",
            )
        self.assertFalse(allowed)
        self.assertIn("FAIL_SAFETY_SCORE_UNDER_55", "\n".join(captured.output))

    def test_route_b_rejects_liquidity_under_7500(self) -> None:
        with self.assertLogs("smart-money-monitor", level="WARNING") as captured:
            allowed = monitor.route_b_safety_filter(
                self.report(liquidity_usd=7_499.99),
                "MINT",
            )
        self.assertFalse(allowed)
        self.assertIn("FAIL_LIQUIDITY_UNDER_7500", "\n".join(captured.output))

    def test_route_b_rejects_lp_under_40(self) -> None:
        with self.assertLogs("smart-money-monitor", level="WARNING") as captured:
            allowed = monitor.route_b_safety_filter(
                self.report(lp_locked_percent=39.99),
                "MINT",
            )
        self.assertFalse(allowed)
        self.assertIn("FAIL_LP_LOCKED_UNDER_40", "\n".join(captured.output))

    def test_token_cooldown_blocks_trade_within_45_minutes(self) -> None:
        now = 10_000.0
        with (
            patch.object(
                monitor.state_store,
                "get_last_trade_time",
                return_value=now - 2_699,
            ),
            self.assertLogs("smart-money-monitor", level="INFO") as captured,
        ):
            active = monitor.token_cooldown_is_active("MINT", now)
        self.assertTrue(active)
        self.assertIn("FAIL_TOKEN_COOLDOWN_ACTIVE", "\n".join(captured.output))

    def test_token_cooldown_allows_trade_at_45_minutes(self) -> None:
        now = 10_000.0
        with patch.object(
            monitor.state_store,
            "get_last_trade_time",
            return_value=now - 2_700,
        ):
            self.assertFalse(monitor.token_cooldown_is_active("MINT", now))


class WalletFeederTriggerTests(unittest.TestCase):
    def test_feeder_starts_asynchronously_when_wallets_are_low(self) -> None:
        process = MagicMock()
        with (
            patch.object(
                monitor.state_store,
                "get_active_wallets_count",
                return_value=17,
            ),
            patch.object(
                monitor.state_store,
                "get_global_metric",
                return_value=0,
            ),
            patch.object(
                monitor.state_store,
                "claim_global_interval",
                return_value=True,
            ),
            patch.object(monitor.subprocess, "Popen", return_value=process) as popen,
        ):
            self.assertTrue(monitor.trigger_wallet_feeder_if_needed(now=10_000))
        self.assertEqual(popen.call_args.args[0], ["pm2", "start", "wallet_feeder"])

    def test_feeder_does_not_start_above_threshold(self) -> None:
        with (
            patch.object(
                monitor.state_store,
                "get_active_wallets_count",
                return_value=18,
            ),
            patch.object(monitor.subprocess, "Popen") as popen,
        ):
            self.assertFalse(monitor.trigger_wallet_feeder_if_needed(now=10_000))
        popen.assert_not_called()

    def test_feeder_respects_two_hour_cooldown(self) -> None:
        with (
            patch.object(
                monitor.state_store,
                "get_active_wallets_count",
                return_value=9,
            ),
            patch.object(
                monitor.state_store,
                "get_global_metric",
                return_value=5_000,
            ),
            patch.object(monitor.subprocess, "Popen") as popen,
        ):
            self.assertFalse(monitor.trigger_wallet_feeder_if_needed(now=10_000))
        popen.assert_not_called()


class CooldownSelfRecoveryTests(unittest.TestCase):
    def test_expired_wallet_recovers_without_metric_reset(self) -> None:
        now = datetime(2026, 7, 27, tzinfo=timezone.utc)
        cooling = [{
            "address": "WALLET",
            "status": "COOL_DOWN",
            "cooldown_start_time": (now - timedelta(seconds=86_400)).isoformat(),
            "wins": 9,
            "losses": 1,
            "cumulative_return_percent": 320.0,
        }]
        with (
            patch.object(
                wallet_performance.state_store,
                "get_wallets_by_status",
                return_value=cooling,
            ),
            patch.object(
                wallet_performance.state_store,
                "set_wallet_status",
                return_value=True,
            ) as setter,
        ):
            restored = wallet_performance.self_recovery_cooldown_wallets(now)
        self.assertEqual(restored, 1)
        setter.assert_called_once_with(
            "WALLET",
            "ACTIVE",
            reset_metrics=False,
            completed_at=now.isoformat(),
        )

    def test_unexpired_wallet_remains_in_cooldown(self) -> None:
        now = datetime(2026, 7, 27, tzinfo=timezone.utc)
        cooling = [{
            "address": "WALLET",
            "status": "COOL_DOWN",
            "cooldown_started_at": (
                now - timedelta(seconds=86_399)
            ).isoformat(),
        }]
        with (
            patch.object(
                wallet_performance.state_store,
                "get_wallets_by_status",
                return_value=cooling,
            ),
            patch.object(
                wallet_performance.state_store,
                "set_wallet_status",
            ) as setter,
        ):
            restored = wallet_performance.self_recovery_cooldown_wallets(now)
        self.assertEqual(restored, 0)
        setter.assert_not_called()


if __name__ == "__main__":
    unittest.main()
