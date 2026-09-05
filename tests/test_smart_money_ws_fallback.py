from __future__ import annotations

import asyncio
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from src import analyzer, monitor, observation_tracker, risk_manager


class FakeSocket:
    def __init__(self, messages: list[dict]) -> None:
        self.messages = list(messages)
        self.sent: list[str] = []

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.messages:
            raise StopAsyncIteration
        return __import__("json").dumps(self.messages.pop(0))


class AsyncContext:
    def __init__(self, value) -> None:
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_args):
        return False


class SmartMoneyWsFallbackTests(unittest.TestCase):
    def test_settings_default_to_keyless_public_standard_ws(self) -> None:
        environment = {
            "HELIUS_API_KEY": "test-key",
            "HELIUS_RPC_WS_URL": "wss://helius.invalid/?api-key=${HELIUS_API_KEY}",
            "HELIUS_RPC_HTTP_URL": "https://helius.invalid/",
        }
        with (
            patch.dict(os.environ, environment, clear=True),
            patch.object(monitor, "load_dotenv"),
        ):
            settings = monitor.MonitorSettings.from_env()
        self.assertEqual(
            settings.standard_ws_url,
            "wss://api.mainnet.solana.com",
        )
        self.assertNotIn("test-key", settings.standard_ws_url)

    def test_helius_failure_selects_standard_and_later_recovers_priority(self) -> None:
        route = monitor.WalletWsRouteState()
        route.record_failure(100.0)
        self.assertTrue(route.uses_standard)
        self.assertEqual(
            route.next_helius_probe_at,
            100.0 + monitor.HELIUS_RECOVERY_PROBE_SECONDS,
        )
        route.refresh(route.next_helius_probe_at - 1)
        self.assertTrue(route.uses_standard)
        route.refresh(route.next_helius_probe_at)
        self.assertFalse(route.uses_standard)

    def test_standard_failure_is_isolated_until_helius_probe_is_due(self) -> None:
        route = monitor.WalletWsRouteState()
        route.activate_standard(100.0)
        route.record_failure(200.0)
        self.assertTrue(route.uses_standard)
        route.record_failure(route.next_helius_probe_at)
        self.assertFalse(route.uses_standard)

    def test_duplicate_signatures_are_suppressed_with_bounded_fifo(self) -> None:
        signatures = monitor.SignatureWindow(maximum=2)
        self.assertTrue(signatures.add("one"))
        self.assertFalse(signatures.add("one"))
        self.assertTrue(signatures.add("two"))
        self.assertTrue(signatures.add("three"))
        self.assertTrue(signatures.add("one"))

    def test_standard_monitor_uses_public_ws_and_fetches_each_signature_once(self) -> None:
        monitor.reset_wallet_ws_activity(now_epoch=10.0)
        program = next(iter(monitor.DEX_PROGRAMS.values()))
        notification = {
            "params": {
                "result": {
                    "value": {
                        "signature": "SIGNATURE",
                        "err": None,
                        "logs": [f"Program {program} invoke [1]"],
                    }
                }
            }
        }
        socket = FakeSocket([
            {"jsonrpc": "2.0", "id": 1, "result": 99},
            notification,
            notification,
        ])
        connect_mock = MagicMock(return_value=AsyncContext(socket))
        session = object()
        transaction = {"transaction": {}, "meta": {}}
        fetched = AsyncMock(return_value=transaction)
        parsed = MagicMock()
        settings = monitor.MonitorSettings(
            "wss://helius.invalid",
            "https://helius.invalid",
            standard_ws_url="wss://public.invalid",
        )
        with (
            patch.object(monitor, "connect", new=connect_mock),
            patch.object(
                monitor.aiohttp,
                "ClientSession",
                return_value=AsyncContext(session),
            ),
            patch.object(monitor, "fetch_transaction", new=fetched),
            patch.object(monitor, "print_buys", new=parsed),
            patch.object(monitor.state_store, "set_global_metrics"),
        ):
            asyncio.run(monitor.monitor_standard_once(settings, ("WALLET",)))
        self.assertEqual(connect_mock.call_args.args[0], "wss://public.invalid")
        fetched.assert_awaited_once()
        parsed.assert_called_once_with(
            transaction,
            next(iter(monitor.DEX_PROGRAMS)),
            {"WALLET"},
            discovery_source=monitor.DISCOVERY_SOURCE_SOLANA,
        )
        metrics = monitor.wallet_ws_activity_metrics()
        self.assertEqual(metrics["wallet_ws_unique_signature_process_count"], 1)
        self.assertEqual(
            metrics["wallet_ws_transaction_restore_success_process_count"], 1
        )
        self.assertEqual(
            metrics["wallet_ws_transaction_restore_failure_process_count"], 0
        )
        monitor.reset_wallet_ws_activity()

    def test_get_transaction_failure_is_not_misclassified_as_ws_failure(self) -> None:
        monitor.reset_wallet_ws_activity(now_epoch=10.0)
        program = next(iter(monitor.DEX_PROGRAMS.values()))
        socket = FakeSocket([
            {"jsonrpc": "2.0", "id": 1, "result": 99},
            {
                "params": {"result": {"value": {
                    "signature": "SIGNATURE",
                    "err": None,
                    "logs": [f"Program {program} invoke [1]"],
                }}},
            },
        ])
        settings = monitor.MonitorSettings(
            "wss://helius.invalid",
            "https://helius.invalid",
            standard_ws_url="wss://public.invalid",
        )
        with (
            patch.object(
                monitor, "connect", return_value=AsyncContext(socket)
            ),
            patch.object(
                monitor.aiohttp,
                "ClientSession",
                return_value=AsyncContext(object()),
            ),
            patch.object(
                monitor,
                "fetch_transaction",
                new=AsyncMock(side_effect=monitor.SolanaRpcExhaustedError(
                    "getTransaction", 6
                )),
            ),
            patch.object(monitor.state_store, "set_global_metrics"),
        ):
            asyncio.run(monitor.monitor_standard_once(settings, ("WALLET",)))
        metrics = monitor.wallet_ws_activity_metrics()
        self.assertEqual(
            metrics["wallet_ws_transaction_restore_failure_process_count"], 1
        )
        self.assertEqual(
            metrics["wallet_ws_transaction_restore_failure_reasons_by_source"],
            {
                monitor.DISCOVERY_SOURCE_SOLANA: {
                    "RPC_ALL_PROVIDERS_EXHAUSTED": 1,
                },
            },
        )
        monitor.reset_wallet_ws_activity()

    def test_activity_metrics_are_bounded_by_canonical_source(self) -> None:
        monitor.reset_wallet_ws_activity(now_epoch=10.0)
        monitor.record_wallet_ws_activity(
            "notification", monitor.DISCOVERY_SOURCE_SOLANA
        )
        monitor.record_wallet_ws_activity(
            "notification", monitor.DISCOVERY_SOURCE_SOLANA
        )
        metrics = monitor.wallet_ws_activity_metrics()
        self.assertEqual(metrics["wallet_ws_activity_started_at"], 10.0)
        self.assertEqual(metrics["wallet_ws_notification_process_count"], 2)
        self.assertEqual(
            metrics["wallet_ws_notification_counts_by_source"],
            {monitor.DISCOVERY_SOURCE_SOLANA: 2},
        )
        monitor.reset_wallet_ws_activity()

    def test_standard_source_uses_same_smart_money_signal_path(self) -> None:
        transaction = {
            "transaction": {
                "signatures": ["SIGNATURE"],
                "message": {"accountKeys": ["WALLET"]},
            },
            "meta": {
                "fee": 0,
                "preBalances": [2_000_000_000],
                "postBalances": [0],
                "preTokenBalances": [{
                    "owner": "WALLET",
                    "mint": "MINT",
                    "uiTokenAmount": {"amount": "0", "decimals": 6},
                }],
                "postTokenBalances": [{
                    "owner": "WALLET",
                    "mint": "MINT",
                    "uiTokenAmount": {"amount": "1000", "decimals": 6},
                }],
            },
        }

        async def run() -> None:
            with (
                patch.object(monitor, "whale_buy_amount_allowed", return_value=True),
                patch.object(monitor, "schedule_paper_signal") as schedule,
                patch(
                    "src.wallet_performance.observe_buy",
                    new=AsyncMock(),
                ),
            ):
                monitor.print_buys(
                    transaction,
                    "DEX",
                    {"WALLET"},
                    discovery_source=monitor.DISCOVERY_SOURCE_SOLANA,
                )
                await asyncio.sleep(0)
            schedule.assert_called_once()
            self.assertEqual(
                schedule.call_args.kwargs["discovery_source"],
                monitor.DISCOVERY_SOURCE_SOLANA,
            )
            self.assertEqual(schedule.call_args.args[:6], (
                "MINT", 1000, 6, 2_000_000_000, "WALLET", "SIGNATURE"
            ))

        asyncio.run(run())

    def test_research_observation_records_standard_discovery_source(self) -> None:
        monitor.reset_wallet_ws_activity(now_epoch=10.0)
        discovery = observation_tracker.ObservationDecision(
            True,
            "SIGNATURE:WALLET:MINT",
            False,
            ("baseline_v1", "route_a_baseline"),
        )
        record_discovery = AsyncMock(return_value=discovery)
        finalize = MagicMock()
        with (
            patch.dict(os.environ, {"OBSERVATION_MODE": "true"}),
            patch.object(
                observation_tracker,
                "record_candidate_discovery",
                new=record_discovery,
            ),
            patch.object(
                observation_tracker,
                "finalize_candidate_without_quote",
                new=finalize,
            ),
            patch.object(
                analyzer,
                "analyze_token",
                new=AsyncMock(side_effect=RuntimeError("test stop")),
            ),
            patch.object(
                monitor.state_store,
                "get_route_initial_stop_streak",
                return_value=(0, 0.0),
            ),
            patch.object(monitor, "token_cooldown_is_active", return_value=False),
            patch.object(risk_manager, "record_rpc_skip", new=AsyncMock()),
        ):
            asyncio.run(monitor.process_paper_signal(
                "MINT",
                1_000,
                6,
                2_000_000_000,
                "WALLET",
                "SIGNATURE",
                "2026-09-05T00:00:00+00:00",
                "A",
                discovery_source=monitor.DISCOVERY_SOURCE_SOLANA,
            ))
        metadata = record_discovery.call_args.kwargs["discovery_metadata"]
        self.assertEqual(
            metadata["discovery_source"],
            monitor.DISCOVERY_SOURCE_SOLANA,
        )
        finalize.assert_called_once()
        metrics = monitor.wallet_ws_activity_metrics()
        self.assertEqual(metrics["wallet_ws_analyzer_reached_process_count"], 1)
        self.assertEqual(metrics["wallet_ws_analyzer_failure_process_count"], 1)
        self.assertEqual(
            metrics["wallet_ws_analyzer_failure_counts_by_source"],
            {monitor.DISCOVERY_SOURCE_SOLANA: 1},
        )
        monitor.reset_wallet_ws_activity()


if __name__ == "__main__":
    unittest.main()
