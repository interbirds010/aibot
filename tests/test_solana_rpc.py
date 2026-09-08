from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
from collections import deque
from pathlib import Path
from unittest.mock import AsyncMock, patch

from src import analyzer, solana_rpc
from src.state_store import atomic_write_json


class FakeResponse:
    def __init__(
        self,
        status: int,
        payload: object,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status = status
        self.payload = payload
        self.headers = headers or {}

    async def __aenter__(self) -> "FakeResponse":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def json(self) -> object:
        return self.payload


class FakeSession:
    def __init__(self, responses: dict[str, list[FakeResponse]]) -> None:
        self.responses = {
            url: deque(rows) for url, rows in responses.items()
        }
        self.calls: list[tuple[str, str, float]] = []

    def post(self, url: str, *, json: dict[str, object]) -> FakeResponse:
        self.calls.append((url, str(json["method"]), time.monotonic()))
        return self.responses[url].popleft()


class FailingSession:
    def __init__(self, error: BaseException) -> None:
        self.error = error

    def post(self, _url: str, *, json: dict[str, object]) -> FakeResponse:
        del json
        raise self.error


def provider(name: str, *, max_rps: float = 1_000.0) -> solana_rpc.RpcProvider:
    return solana_rpc.RpcProvider(
        name=name,
        url=f"https://{name}.example.invalid/rpc",
        max_rps=max_rps,
        public_fallback=name == "solana_public",
    )


class SolanaRpcRouterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.original_state_dir = solana_rpc.RPC_PROVIDER_STATE_DIR
        solana_rpc.RPC_PROVIDER_STATE_DIR = Path(self.temporary.name)

    def tearDown(self) -> None:
        solana_rpc.RPC_PROVIDER_STATE_DIR = self.original_state_dir
        self.temporary.cleanup()

    def test_primary_success_updates_provider_state(self) -> None:
        primary = provider("alchemy")
        session = FakeSession({
            primary.url: [FakeResponse(200, {"result": {"value": 7}})],
        })

        result = asyncio.run(solana_rpc.solana_rpc_call(
            session,
            "getBalance",
            ["owner"],
            providers=[primary],
        ))

        self.assertEqual(result, {"value": 7})
        self.assertEqual([call[0] for call in session.calls], [primary.url])
        state = solana_rpc.provider_state("alchemy", [primary])
        self.assertEqual(state["request_count"], 1)
        self.assertEqual(state["success_count"], 1)
        self.assertEqual(state["failure_count"], 0)
        self.assertEqual(state["circuit_state"], "CLOSED")
        self.assertEqual(state["version"], 2)

    def test_primary_429_fails_over_to_secondary(self) -> None:
        primary, secondary = provider("alchemy"), provider("chainstack")
        session = FakeSession({
            primary.url: [FakeResponse(429, {}, {"Retry-After": "1"})],
            secondary.url: [FakeResponse(200, {"result": "secondary"})],
        })

        result = asyncio.run(solana_rpc.solana_rpc_call(
            session,
            "getAccountInfo",
            [],
            providers=[secondary, primary],
        ))

        self.assertEqual(result, "secondary")
        self.assertEqual(
            [call[0] for call in session.calls],
            [primary.url, secondary.url],
        )
        state = solana_rpc.provider_state("alchemy", [primary, secondary])
        self.assertEqual(state["rate_limit_count"], 1)
        self.assertEqual(state["last_failure_category"], "RATE_LIMIT")
        primary_metric = state["method_metrics"]["getAccountInfo"]
        self.assertEqual(primary_metric["request_count"], 1)
        self.assertEqual(primary_metric["failure_count"], 1)
        self.assertEqual(primary_metric["rate_limit_count"], 1)
        secondary_state = solana_rpc.provider_state(
            "chainstack", [primary, secondary]
        )
        secondary_metric = secondary_state["method_metrics"]["getAccountInfo"]
        self.assertEqual(secondary_metric["request_count"], 1)
        self.assertEqual(secondary_metric["success_count"], 1)
        self.assertEqual(secondary_metric["failover_count"], 1)
        self.assertEqual(secondary_metric["latency_sample_count"], 1)

    def test_three_transient_failures_open_circuit(self) -> None:
        target = provider("ankr")
        reservation = solana_rpc.ProviderReservation(half_open_probe=False)
        failure = solana_rpc.ProviderFailure(
            provider="ankr",
            transient=True,
            rate_limited=True,
            retry_delay_seconds=1.0,
            retry_source="retry-after",
            category="RATE_LIMIT",
        )
        for _ in range(solana_rpc.RPC_CIRCUIT_FAILURE_THRESHOLD):
            solana_rpc._record_provider_failure_sync(
                target,
                reservation,
                failure,
            )

        state = solana_rpc.provider_state("ankr", [target])
        self.assertEqual(state["consecutive_failures"], 3)
        self.assertEqual(state["circuit_state"], "OPEN")
        self.assertEqual(state["circuit_open_count"], 1)
        self.assertIsNone(solana_rpc._reserve_provider_slot_sync(target))

        solana_rpc._record_provider_failure_sync(
            target,
            reservation,
            failure,
        )
        state = solana_rpc.provider_state("ankr", [target])
        self.assertEqual(state["circuit_open_count"], 1)

    def test_cooldown_expiry_allows_one_probe_and_success_closes_circuit(self) -> None:
        target = provider("alchemy")
        state = solana_rpc._empty_provider_state("alchemy")
        state.update({
            "circuit_state": "OPEN",
            "cooldown_until_epoch": time.time() - 1,
            "consecutive_failures": 3,
        })
        atomic_write_json(solana_rpc._state_path("alchemy"), state)
        session = FakeSession({
            target.url: [FakeResponse(200, {"result": "probe-ok"})],
        })

        result = asyncio.run(solana_rpc.solana_rpc_call(
            session,
            "getHealth",
            [],
            providers=[target],
        ))

        self.assertEqual(result, "probe-ok")
        recovered = solana_rpc.provider_state("alchemy", [target])
        self.assertEqual(recovered["circuit_state"], "CLOSED")
        self.assertEqual(recovered["consecutive_failures"], 0)

    def test_provider_state_migrates_metrics_and_calculates_success_rate(self) -> None:
        target = provider("alchemy")
        atomic_write_json(solana_rpc._state_path("alchemy"), {
            "schema_version": 1,
            "version": 4,
            "provider": "alchemy",
            "request_count": 5,
            "success_count": 3,
            "failure_count": 1,
            "rate_limit_count": 1,
        })

        state = solana_rpc.provider_state("alchemy", [target])

        self.assertEqual(
            state["schema_version"],
            solana_rpc.RPC_PROVIDER_STATE_SCHEMA_VERSION,
        )
        self.assertEqual(state["circuit_open_count"], 0)
        self.assertEqual(state["success_rate_percent"], 75.0)

    def test_all_providers_fail_closed(self) -> None:
        primary, secondary = provider("alchemy"), provider("ankr")
        session = FakeSession({
            primary.url: [FakeResponse(500, {})],
            secondary.url: [FakeResponse(503, {})],
        })

        with self.assertRaises(solana_rpc.SolanaRpcExhaustedError) as caught:
            asyncio.run(solana_rpc.solana_rpc_call(
                session,
                "getTokenSupply",
                [],
                providers=[secondary, primary],
                provider_local_attempts=1,
            ))

        self.assertEqual(
            caught.exception.canonical_reason,
            solana_rpc.RPC_ALL_PROVIDERS_EXHAUSTED,
        )
        final_state = solana_rpc.provider_state("ankr", [primary, secondary])
        self.assertEqual(
            final_state["method_metrics"]["getTokenSupply"][
                "exhaustion_count"
            ],
            1,
        )
        self.assertEqual(caught.exception.last_provider, "ankr")
        self.assertEqual(caught.exception.last_category, "HTTP_TRANSIENT")

        async def fail_closed() -> bool:
            with patch.object(
                analyzer,
                "analyze_token",
                new=AsyncMock(side_effect=caught.exception),
            ):
                return await analyzer.should_enter_token(
                    "So11111111111111111111111111111111111111112"
                )

        self.assertFalse(asyncio.run(fail_closed()))

    def test_method_aware_order_is_deterministic(self) -> None:
        providers = [
            provider("solana_public"),
            provider("helius"),
            provider("ankr"),
            provider("chainstack"),
            provider("alchemy"),
        ]
        light = solana_rpc.ordered_providers(
            providers,
            "getAccountInfo",
            "analyzer",
        )
        heavy = solana_rpc.ordered_providers(
            tuple(reversed(providers)),
            "getTransaction",
            "transaction_history",
        )
        token_accounts = solana_rpc.ordered_providers(
            providers,
            "getTokenAccountsByOwner",
            "executor_read",
        )

        self.assertEqual(
            [row.name for row in light],
            ["alchemy", "chainstack", "ankr", "helius", "solana_public"],
        )
        self.assertEqual(
            [row.name for row in heavy],
            ["ankr", "chainstack", "alchemy", "helius", "solana_public"],
        )
        self.assertEqual(
            [row.name for row in token_accounts],
            ["alchemy", "ankr", "helius", "solana_public"],
        )

    def test_missing_endpoints_are_disabled_and_public_is_emergency_default(self) -> None:
        defaults = solana_rpc.provider_configs_from_env({})
        self.assertEqual([row.name for row in defaults], ["solana_public"])
        disabled = solana_rpc.provider_configs_from_env({
            "ALCHEMY_SOLANA_RPC_URL": "",
            "CHAINSTACK_SOLANA_RPC_URL": "",
            "ANKR_SOLANA_RPC_URL": "",
            "HELIUS_RPC_HTTP_URL": "https://rpc/?key=${HELIUS_API_KEY}",
            "SOLANA_PUBLIC_RPC_URL": "",
        })
        self.assertEqual(disabled, ())
        states = solana_rpc.provider_states(defaults)
        self.assertTrue(states["solana_public"]["enabled"])
        self.assertFalse(states["alchemy"]["enabled"])
        with self.assertRaises(
            solana_rpc.SolanaRpcConfigurationError
        ) as caught:
            asyncio.run(solana_rpc.solana_rpc_call(
                FakeSession({}),
                "getHealth",
                [],
                providers=disabled,
            ))
        self.assertEqual(
            caught.exception.canonical_reason,
            solana_rpc.RPC_NO_PROVIDER_CONFIGURED,
        )

    def test_per_provider_rate_slot_is_process_shared(self) -> None:
        target = provider("alchemy", max_rps=2.0)
        first = solana_rpc._reserve_provider_slot_sync(target, now_epoch=100.0)
        second = solana_rpc._reserve_provider_slot_sync(target, now_epoch=100.0)

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        state = solana_rpc.provider_state("alchemy", [target])
        self.assertEqual(state["last_request_at_epoch"], 100.5)
        self.assertEqual(state["request_count"], 2)

    def test_retry_after_controls_shared_provider_cooldown(self) -> None:
        primary, secondary = provider("alchemy"), provider("ankr")
        session = FakeSession({
            primary.url: [FakeResponse(429, {}, {"Retry-After": "2"})],
            secondary.url: [FakeResponse(200, {"result": True})],
        })
        before = time.time()

        asyncio.run(solana_rpc.solana_rpc_call(
            session,
            "getBalance",
            [],
            providers=[primary, secondary],
        ))

        state = solana_rpc.provider_state("alchemy", [primary, secondary])
        self.assertGreaterEqual(state["cooldown_until_epoch"], before + 2.0)

    def test_exponential_backoff_uses_jitter_and_stays_bounded(self) -> None:
        target = provider("alchemy")
        error = solana_rpc._ProviderRequestError(
            transient=True,
            rate_limited=False,
            status=503,
            category="HTTP_TRANSIENT",
        )
        with patch("src.helius_rpc.random.random", return_value=0.0):
            failure = solana_rpc._failure_from_error(target, error, 2)

        self.assertEqual(failure.retry_source, "exponential-fallback")
        self.assertEqual(failure.retry_delay_seconds, 3.0)
        self.assertLessEqual(
            failure.retry_delay_seconds,
            solana_rpc.RPC_MAX_INLINE_BACKOFF_SECONDS,
        )

    def test_timeout_and_connection_failures_have_stable_categories(self) -> None:
        target = provider("alchemy")
        cases = (
            (asyncio.TimeoutError(), "TIMEOUT"),
            (solana_rpc.aiohttp.ClientConnectionError(), "CONNECTION"),
        )
        for error, category in cases:
            with self.subTest(category=category):
                with self.assertRaises(
                    solana_rpc._ProviderRequestError
                ) as caught:
                    asyncio.run(solana_rpc._provider_request_once(
                        FailingSession(error),
                        target,
                        "getHealth",
                        [],
                    ))
                self.assertEqual(caught.exception.category, category)
                self.assertTrue(caught.exception.transient)

    def test_local_retry_is_counted_separately_from_failover(self) -> None:
        target = provider("alchemy")
        session = FakeSession({
            target.url: [
                FakeResponse(500, {}),
                FakeResponse(200, {"result": "ok"}),
            ],
        })
        immediate_failure = solana_rpc.ProviderFailure(
            provider="alchemy",
            transient=True,
            rate_limited=False,
            retry_delay_seconds=0.0,
            retry_source="exponential-fallback",
            category="HTTP_TRANSIENT",
        )
        with (
            patch.object(
                solana_rpc,
                "_failure_from_error",
                return_value=immediate_failure,
            ),
            patch.object(solana_rpc.asyncio, "sleep", new=AsyncMock()),
        ):
            result = asyncio.run(solana_rpc.solana_rpc_call(
                session,
                "getBalance",
                [],
                providers=[target],
            ))

        self.assertEqual(result, "ok")
        metric = solana_rpc.provider_state("alchemy", [target])[
            "method_metrics"
        ]["getBalance"]
        self.assertEqual(metric["request_count"], 2)
        self.assertEqual(metric["failure_count"], 1)
        self.assertEqual(metric["success_count"], 1)
        self.assertEqual(metric["retry_count"], 1)
        self.assertEqual(metric["failover_count"], 0)
        self.assertEqual(metric["latency_sample_count"], 2)
        self.assertEqual(sum(metric["latency_buckets"].values()), 2)

    def test_rate_limit_exhaustion_uses_method_canonical_reason(self) -> None:
        primary, secondary = provider("alchemy"), provider("ankr")
        session = FakeSession({
            primary.url: [FakeResponse(429, {}, {"Retry-After": "1"})],
            secondary.url: [FakeResponse(429, {}, {"Retry-After": "1"})],
        })

        with self.assertRaises(
            solana_rpc.SolanaRpcRateLimitExhaustedError
        ) as caught:
            asyncio.run(solana_rpc.solana_rpc_call(
                session,
                "getTokenSupply",
                [],
                providers=[primary, secondary],
            ))

        self.assertEqual(
            caught.exception.canonical_reason,
            "RPC_GET_TOKEN_SUPPLY_RATE_LIMIT_EXHAUSTED",
        )
        self.assertEqual(caught.exception.attempts, 2)

    def test_overall_attempt_budget_bounds_failover(self) -> None:
        providers = [provider("alchemy"), provider("chainstack"), provider("ankr")]
        session = FakeSession({
            row.url: [FakeResponse(500, {})] for row in providers
        })

        with self.assertRaises(solana_rpc.SolanaRpcExhaustedError) as caught:
            asyncio.run(solana_rpc.solana_rpc_call(
                session,
                "getAccountInfo",
                [],
                providers=providers,
                provider_local_attempts=1,
                overall_attempt_budget=2,
            ))

        self.assertEqual(caught.exception.attempts, 2)
        self.assertEqual(len(session.calls), 2)

    def test_default_budget_preserves_an_emergency_public_attempt(self) -> None:
        providers = [
            provider("alchemy"),
            provider("chainstack"),
            provider("ankr"),
            provider("helius"),
            provider("solana_public"),
        ]
        session = FakeSession({
            row.url: [FakeResponse(500, {}), FakeResponse(500, {})]
            for row in providers
        })
        immediate_failure = solana_rpc.ProviderFailure(
            provider="test",
            transient=True,
            rate_limited=False,
            retry_delay_seconds=0.0,
            retry_source="exponential-fallback",
            category="HTTP_TRANSIENT",
        )

        with (
            patch.object(
                solana_rpc,
                "_failure_from_error",
                return_value=immediate_failure,
            ),
            patch.object(solana_rpc.asyncio, "sleep", new=AsyncMock()),
            self.assertRaises(solana_rpc.SolanaRpcExhaustedError),
        ):
            asyncio.run(solana_rpc.solana_rpc_call(
                session,
                "getAccountInfo",
                [],
                providers=providers,
            ))

        self.assertEqual(len(session.calls), solana_rpc.RPC_OVERALL_ATTEMPT_BUDGET)
        self.assertEqual(session.calls[-1][0], providers[-1].url)

    def test_concurrent_requests_keep_provider_spacing(self) -> None:
        target = provider("alchemy", max_rps=20.0)
        session = FakeSession({
            target.url: [
                FakeResponse(200, {"result": 1}),
                FakeResponse(200, {"result": 2}),
            ],
        })

        async def scenario() -> list[object]:
            return await asyncio.gather(*[
                solana_rpc.solana_rpc_call(
                    session,
                    "getBalance",
                    [],
                    providers=[target],
                )
                for _ in range(2)
            ])

        self.assertEqual(asyncio.run(scenario()), [1, 2])
        call_times = sorted(call[2] for call in session.calls)
        self.assertGreaterEqual(call_times[1] - call_times[0], 0.035)

    def test_secret_endpoint_is_never_logged_or_persisted(self) -> None:
        secret = "super-secret-api-key"
        target = solana_rpc.RpcProvider(
            name="alchemy",
            url=f"https://alchemy.invalid/v2/{secret}",
            max_rps=1_000,
        )
        session = FakeSession({
            target.url: [FakeResponse(401, {"error": {"message": secret}})],
        })

        with self.assertLogs("solana-rpc", level="WARNING") as logs:
            with self.assertRaises(solana_rpc.SolanaRpcExhaustedError):
                asyncio.run(solana_rpc.solana_rpc_call(
                    session,
                    "getBalance",
                    [],
                    providers=[target],
                    provider_local_attempts=1,
                ))

        persisted = json.dumps(
            solana_rpc.provider_state("alchemy", [target]),
            sort_keys=True,
        )
        self.assertNotIn(secret, "\n".join(logs.output))
        self.assertNotIn(secret, persisted)
        self.assertNotIn(target.url, persisted)


if __name__ == "__main__":
    unittest.main()
