from __future__ import annotations

import asyncio
import json
import multiprocessing
import tempfile
import time
import unittest
from collections import deque
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import AsyncMock, patch

from src import analyzer, solana_rpc
from src.research import coverage_telemetry
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


def paced_reservation_worker(
    state_dir: str,
    start: object,
    results: object,
) -> None:
    """spawn worker에서 동일한 persisted method pacing state를 예약한다."""
    solana_rpc.RPC_PROVIDER_STATE_DIR = Path(state_dir)
    solana_rpc.RPC_METHOD_PACING_INTERVAL_SECONDS = {
        ("solana_public", "getTransaction"): 0.2,
    }
    target = provider("solana_public", max_rps=1_000.0)
    start.wait(timeout=10)
    reservation = solana_rpc._reserve_provider_slot_sync(
        target, method="getTransaction"
    )
    results.put(reservation.pacing_request_epoch)


class SolanaRpcRouterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.original_state_dir = solana_rpc.RPC_PROVIDER_STATE_DIR
        solana_rpc.RPC_PROVIDER_STATE_DIR = Path(self.temporary.name)
        self.original_telemetry_path = coverage_telemetry.TELEMETRY_PATH
        self.original_hourly_path = coverage_telemetry.HOURLY_TELEMETRY_PATH
        coverage_telemetry.TELEMETRY_PATH = (
            Path(self.temporary.name) / "coverage.json"
        )
        coverage_telemetry.HOURLY_TELEMETRY_PATH = (
            Path(self.temporary.name) / "coverage-hourly.json"
        )
        coverage_telemetry.reset_pending_telemetry()
        with solana_rpc._semantic_repetition_lock:
            solana_rpc._semantic_repetition_seen.clear()
        solana_rpc._reset_pacing_attribution()

    def tearDown(self) -> None:
        solana_rpc.RPC_PROVIDER_STATE_DIR = self.original_state_dir
        coverage_telemetry.reset_pending_telemetry()
        with solana_rpc._semantic_repetition_lock:
            solana_rpc._semantic_repetition_seen.clear()
        solana_rpc._reset_pacing_attribution()
        coverage_telemetry.TELEMETRY_PATH = self.original_telemetry_path
        coverage_telemetry.HOURLY_TELEMETRY_PATH = self.original_hourly_path
        self.temporary.cleanup()

    def _coverage_rpc_metrics(self) -> dict[str, dict[str, object]]:
        coverage_telemetry.flush_coverage_telemetry()
        document = json.loads(
            coverage_telemetry.TELEMETRY_PATH.read_text("utf-8")
        )
        return document["buckets"][-1]["rpc_methods"]

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

        coverage_telemetry.flush_coverage_telemetry()
        coverage = json.loads(
            coverage_telemetry.TELEMETRY_PATH.read_text("utf-8")
        )
        rpc = coverage["buckets"][-1]["rpc_methods"][
            "alchemy|getBalance"
        ]
        self.assertEqual(rpc["request_count"], 1)
        self.assertEqual(rpc["success_count"], 1)
        self.assertEqual(rpc["failure_count"], 0)
        self.assertEqual(rpc["latency_count"], 1)

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
        coverage_telemetry.flush_coverage_telemetry()
        coverage = json.loads(
            coverage_telemetry.TELEMETRY_PATH.read_text("utf-8")
        )["buckets"][-1]["rpc_methods"]
        self.assertEqual(
            coverage["alchemy|getAccountInfo"]["rate_limit_count"], 1
        )
        self.assertEqual(
            coverage["chainstack|getAccountInfo"]["success_count"], 1
        )
        self.assertEqual(
            coverage["chainstack|getAccountInfo"]["failover_count"], 1
        )

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

    def test_reservation_skip_reasons_are_exact_and_bounded(self) -> None:
        cases = (
            (
                "circuit_open_cooldown",
                {
                    "circuit_state": "OPEN",
                    "cooldown_until_epoch": time.time() + 60,
                    "last_circuit_open_method": "getTransaction",
                },
            ),
            (
                "half_open_lease",
                {
                    "circuit_state": "HALF_OPEN",
                    "half_open_lease_until_epoch": time.time() + 60,
                    "last_circuit_open_method": "getTransaction",
                },
            ),
            (
                "cooldown",
                {
                    "circuit_state": "CLOSED",
                    "cooldown_until_epoch": time.time() + 60,
                    "last_failure_method": "getTransaction",
                },
            ),
        )
        for reason, changes in cases:
            with self.subTest(reason=reason):
                coverage_telemetry.reset_pending_telemetry()
                coverage_telemetry.TELEMETRY_PATH.unlink(missing_ok=True)
                target = provider("solana_public")
                state = solana_rpc._empty_provider_state(target.name)
                state.update(changes)
                atomic_write_json(solana_rpc._state_path(target.name), state)

                self.assertIsNone(solana_rpc._reserve_provider_slot_sync(
                    target,
                    method="getSignaturesForAddress",
                ))

                metric = self._coverage_rpc_metrics()[
                    "solana_public|getSignaturesForAddress"
                ]
                self.assertEqual(metric["reservation_skip_count"], 1)
                self.assertEqual(metric[f"reservation_{reason}_count"], 1)
                self.assertEqual(
                    metric["reservation_skip_trigger_methods"],
                    {"getTransaction": 1},
                )

    def test_get_transaction_rate_limit_does_not_block_signatures(self) -> None:
        target = provider("solana_public")
        session = FakeSession({
            target.url: [
                FakeResponse(429, {}, {"Retry-After": "120"}),
                FakeResponse(200, {"result": ["healthy"]}),
            ],
        })

        with self.assertRaises(solana_rpc.SolanaRpcRateLimitExhaustedError):
            asyncio.run(solana_rpc.solana_rpc_call(
                session,
                "getTransaction",
                ["signature"],
                workload="transaction_history",
                providers=[target],
                provider_local_attempts=1,
            ))
        result = asyncio.run(solana_rpc.solana_rpc_call(
            session,
            "getSignaturesForAddress",
            ["address", {"limit": 12, "commitment": "confirmed"}],
            workload="transaction_history",
            providers=[target],
            provider_local_attempts=1,
        ))

        self.assertEqual(result, ["healthy"])
        self.assertEqual(
            [call[1] for call in session.calls],
            ["getTransaction", "getSignaturesForAddress"],
        )
        state = solana_rpc.provider_state(target.name, [target])
        transaction = state["method_availability"]["getTransaction"]
        signatures = state["method_availability"][
            "getSignaturesForAddress"
        ]
        self.assertEqual(transaction["last_failure_method"], "getTransaction")
        self.assertEqual(
            transaction["last_rate_limit_method"], "getTransaction"
        )
        self.assertGreater(transaction["cooldown_until_epoch"], time.time())
        self.assertEqual(signatures["circuit_state"], "CLOSED")
        self.assertEqual(state["circuit_state"], "CLOSED")
        metrics = self._coverage_rpc_metrics()
        signatures_metric = metrics[
            "solana_public|getSignaturesForAddress"
        ]
        self.assertEqual(signatures_metric["request_count"], 1)
        self.assertEqual(signatures_metric["success_count"], 1)
        self.assertEqual(signatures_metric["reservation_skip_count"], 0)
        self.assertEqual(
            metrics["router|getSignaturesForAddress"][
                "zero_attempt_exhaustion_count"
            ],
            0,
        )

    def test_signature_rate_limit_does_not_block_transaction(self) -> None:
        target = provider("solana_public")
        session = FakeSession({
            target.url: [
                FakeResponse(429, {}, {"Retry-After": "120"}),
                FakeResponse(200, {"result": {"slot": 7}}),
            ],
        })

        with self.assertRaises(solana_rpc.SolanaRpcRateLimitExhaustedError):
            asyncio.run(solana_rpc.solana_rpc_call(
                session,
                "getSignaturesForAddress",
                ["address"],
                providers=[target],
                provider_local_attempts=1,
            ))
        result = asyncio.run(solana_rpc.solana_rpc_call(
            session,
            "getTransaction",
            ["signature"],
            providers=[target],
            provider_local_attempts=1,
        ))

        self.assertEqual(result, {"slot": 7})
        self.assertEqual(
            [call[1] for call in session.calls],
            ["getSignaturesForAddress", "getTransaction"],
        )
        state = solana_rpc.provider_state(target.name, [target])
        signatures = state["method_availability"][
            "getSignaturesForAddress"
        ]
        transaction = state["method_availability"]["getTransaction"]
        self.assertGreater(signatures["cooldown_until_epoch"], time.time())
        self.assertEqual(transaction["circuit_state"], "CLOSED")

    def test_all_skipped_providers_have_one_mixed_zero_attempt_summary(self) -> None:
        helius = provider("helius")
        public = provider("solana_public")
        helius_state = solana_rpc._empty_provider_state(helius.name)
        helius_state.update({
            "circuit_state": "OPEN",
            "cooldown_until_epoch": time.time() + 60,
            "last_circuit_open_method": "getTransaction",
        })
        public_state = solana_rpc._empty_provider_state(public.name)
        public_state.update({
            "circuit_state": "CLOSED",
            "cooldown_until_epoch": time.time() + 60,
            "last_failure_method": "getTransaction",
        })
        atomic_write_json(solana_rpc._state_path(helius.name), helius_state)
        atomic_write_json(solana_rpc._state_path(public.name), public_state)

        with self.assertRaises(solana_rpc.SolanaRpcExhaustedError) as caught:
            asyncio.run(solana_rpc.solana_rpc_call(
                FakeSession({}),
                "getSignaturesForAddress",
                ["address"],
                workload="transaction_history",
                providers=[public, helius],
            ))

        self.assertEqual(caught.exception.attempts, 0)
        zero = self._coverage_rpc_metrics()[
            "router|getSignaturesForAddress"
        ]
        self.assertEqual(zero["zero_attempt_exhaustion_count"], 1)
        self.assertEqual(zero["zero_attempt_provider_count_sum"], 2)
        self.assertEqual(zero["zero_attempt_provider_counts"], {"2": 1})
        self.assertEqual(zero["zero_attempt_circuit_open_cooldown_count"], 1)
        self.assertEqual(zero["zero_attempt_cooldown_count"], 1)
        self.assertEqual(zero["zero_attempt_mixed_unavailable_count"], 1)

    def test_circuit_trigger_method_is_recorded_only_on_open_transition(self) -> None:
        target = provider("solana_public")
        reservation = solana_rpc.ProviderReservation(half_open_probe=False)
        failure = solana_rpc.ProviderFailure(
            provider=target.name,
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
                method="getTransaction",
            )
        solana_rpc._record_provider_failure_sync(
            target,
            reservation,
            failure,
            method="getSignaturesForAddress",
        )

        state = solana_rpc.provider_state(target.name, [target])
        transaction = state["method_availability"]["getTransaction"]
        signatures = state["method_availability"][
            "getSignaturesForAddress"
        ]
        self.assertEqual(transaction["last_failure_method"], "getTransaction")
        self.assertEqual(transaction["last_circuit_open_method"], "getTransaction")
        self.assertEqual(
            signatures["last_failure_method"], "getSignaturesForAddress"
        )
        self.assertEqual(
            signatures["last_rate_limit_method"],
            "getSignaturesForAddress",
        )
        self.assertEqual(state["last_failure_method"], "getSignaturesForAddress")
        self.assertEqual(
            state["last_rate_limit_method"], "getSignaturesForAddress"
        )
        self.assertEqual(state["last_circuit_open_method"], "getTransaction")

    def test_method_circuit_and_half_open_lease_are_isolated(self) -> None:
        target = provider("solana_public")
        reservation = solana_rpc.ProviderReservation(half_open_probe=False)
        failure = solana_rpc.ProviderFailure(
            provider=target.name,
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
                method="getTransaction",
            )

        state = solana_rpc.provider_state(target.name, [target])
        transaction = state["method_availability"]["getTransaction"]
        self.assertEqual(transaction["circuit_state"], "OPEN")
        self.assertIsNone(solana_rpc._reserve_provider_slot_sync(
            target,
            method="getTransaction",
        ))
        self.assertIsNotNone(solana_rpc._reserve_provider_slot_sync(
            target,
            method="getSignaturesForAddress",
        ))

        transaction["cooldown_until_epoch"] = 99.0
        transaction["half_open_lease_until_epoch"] = 0.0
        atomic_write_json(solana_rpc._state_path(target.name), state)
        probe = solana_rpc._reserve_provider_slot_sync(
            target,
            method="getTransaction",
            now_epoch=100.0,
        )
        self.assertIsNotNone(probe)
        self.assertTrue(probe.method_half_open_probe)
        self.assertIsNone(solana_rpc._reserve_provider_slot_sync(
            target,
            method="getTransaction",
            now_epoch=100.1,
        ))
        self.assertIsNotNone(solana_rpc._reserve_provider_slot_sync(
            target,
            method="getSignaturesForAddress",
            now_epoch=100.1,
        ))

    def test_method_success_does_not_close_other_method_circuit(self) -> None:
        target = provider("solana_public")
        state = solana_rpc._empty_provider_state(target.name)
        transaction = state["method_availability"]["getTransaction"]
        transaction.update({
            "circuit_state": "OPEN",
            "cooldown_until_epoch": time.time() + 60,
            "consecutive_failures": 3,
            "last_circuit_open_method": "getTransaction",
        })
        atomic_write_json(solana_rpc._state_path(target.name), state)

        solana_rpc._record_provider_success_sync(
            target,
            solana_rpc.ProviderReservation(half_open_probe=False),
            method="getSignaturesForAddress",
        )

        final_state = solana_rpc.provider_state(target.name, [target])
        self.assertEqual(
            final_state["method_availability"]["getTransaction"][
                "circuit_state"
            ],
            "OPEN",
        )

    def test_provider_wide_failure_still_blocks_all_methods(self) -> None:
        target = provider("solana_public")
        session = FakeSession({target.url: [FakeResponse(500, {})]})
        provider_failure = solana_rpc.ProviderFailure(
            provider=target.name,
            transient=True,
            rate_limited=False,
            retry_delay_seconds=120.0,
            retry_source="exponential-fallback",
            category="HTTP_TRANSIENT",
        )
        with (
            patch.object(
                solana_rpc,
                "_failure_from_error",
                return_value=provider_failure,
            ),
            self.assertRaises(solana_rpc.SolanaRpcExhaustedError),
        ):
            asyncio.run(solana_rpc.solana_rpc_call(
                session,
                "getTransaction",
                ["signature"],
                providers=[target],
                provider_local_attempts=1,
            ))
        with self.assertRaises(solana_rpc.SolanaRpcExhaustedError) as caught:
            asyncio.run(solana_rpc.solana_rpc_call(
                session,
                "getSignaturesForAddress",
                ["address"],
                providers=[target],
                provider_local_attempts=1,
            ))

        self.assertEqual(caught.exception.attempts, 0)
        self.assertEqual(len(session.calls), 1)
        state = solana_rpc.provider_state(target.name, [target])
        self.assertGreater(state["cooldown_until_epoch"], time.time())
        self.assertEqual(state["last_failure_method"], "getTransaction")
        telemetry = self._coverage_rpc_metrics()
        skipped = telemetry["solana_public|getSignaturesForAddress"]
        self.assertEqual(skipped["reservation_cooldown_count"], 1)
        self.assertEqual(
            skipped["reservation_skip_trigger_methods"],
            {"getTransaction": 1},
        )

    def test_failure_scope_classification_is_narrow(self) -> None:
        def failure(category: str, *, rate_limited: bool = False):
            return solana_rpc.ProviderFailure(
                provider="solana_public",
                transient=True,
                rate_limited=rate_limited,
                retry_delay_seconds=1.0,
                retry_source="test",
                category=category,
            )

        self.assertTrue(solana_rpc._failure_uses_method_availability(
            "getTransaction", failure("RATE_LIMIT", rate_limited=True)
        ))
        self.assertTrue(solana_rpc._failure_uses_method_availability(
            "getSignaturesForAddress", failure("RPC_TRANSIENT")
        ))
        for category in (
            "HTTP_TRANSIENT", "TIMEOUT", "CONNECTION", "TRANSPORT",
            "MALFORMED_RESPONSE",
        ):
            with self.subTest(category=category):
                self.assertFalse(
                    solana_rpc._failure_uses_method_availability(
                        "getTransaction", failure(category)
                    )
                )
        self.assertFalse(solana_rpc._failure_uses_method_availability(
            "getBalance", failure("RATE_LIMIT", rate_limited=True)
        ))

    def test_semantic_repetition_windows_and_capacity_are_bounded(self) -> None:
        params = ["private-address", {"commitment": "confirmed", "limit": 12}]
        for now in (0.0, 30.0, 120.0, 600.0):
            solana_rpc._record_semantic_repetition(
                "getSignaturesForAddress",
                params,
                now_monotonic=now,
            )
        with patch.object(
            solana_rpc,
            "RPC_SEMANTIC_REPETITION_MAX_ENTRIES",
            3,
        ):
            for index in range(4):
                solana_rpc._record_semantic_repetition(
                    "getSignaturesForAddress",
                    [f"unique-{index}"],
                    now_monotonic=700.0 + index,
                )
            solana_rpc._record_semantic_repetition(
                "getSignaturesForAddress",
                ["after-ttl"],
                now_monotonic=2_000.0,
            )

        self.assertEqual(len(solana_rpc._semantic_repetition_seen), 1)
        metric = self._coverage_rpc_metrics()[
            "router|getSignaturesForAddress"
        ]
        self.assertEqual(metric["semantic_request_count"], 9)
        self.assertEqual(metric["semantic_repeated_within_1m_count"], 1)
        self.assertEqual(metric["semantic_repeated_within_5m_count"], 2)
        self.assertEqual(metric["semantic_repeated_within_15m_count"], 3)
        self.assertGreaterEqual(metric["semantic_tracker_eviction_count"], 5)

    def test_public_transaction_pacing_attribution_is_bounded_and_outcome_linked(
        self,
    ) -> None:
        target = provider("solana_public")
        base = time.time()
        samples = (
            (base, "success"),
            (base + 0.05, "rate_limit"),
            (base + 0.30, "success"),
            (base + 0.80, "rate_limit"),
            (base + 2.0, "success"),
            (base + 4.5, "rate_limit"),
        )
        expected_intervals = (
            "no_previous",
            "lt_100_ms",
            "250_499_ms",
            "500_999_ms",
            "1_2_s",
            "gt_2_s",
        )

        for (timestamp, outcome), expected_interval in zip(
            samples, expected_intervals, strict=True
        ):
            interval, bursts = solana_rpc._physical_pacing_attribution(
                target,
                "getTransaction",
                now_epoch=timestamp,
            )
            self.assertEqual(interval, expected_interval)
            reservation = solana_rpc.ProviderReservation(
                half_open_probe=False,
                pacing_request_epoch=timestamp,
                pacing_interval_bucket=interval,
                pacing_burst_buckets=bursts,
            )
            solana_rpc._record_pacing_outcome(
                target, "getTransaction", reservation, outcome
            )

        metric = self._coverage_rpc_metrics()[
            "solana_public|getTransaction"
        ]
        self.assertEqual(
            sum(metric["pacing_interval_request_counts"].values()), 6
        )
        self.assertEqual(
            sum(metric["pacing_interval_success_counts"].values()), 3
        )
        self.assertEqual(
            sum(metric["pacing_interval_rate_limit_counts"].values()), 3
        )
        for window in ("1s", "5s", "10s"):
            self.assertEqual(
                sum(metric["pacing_burst_request_counts"][window].values()),
                6,
            )
        self.assertNotIn("pacing_request_epoch", json.dumps(metric))

        for index in range(solana_rpc.RPC_PACING_HISTORY_LIMIT + 20):
            solana_rpc._physical_pacing_attribution(
                target,
                "getTransaction",
                now_epoch=base + 20.0 + index / 1_000.0,
            )
        self.assertLessEqual(
            len(solana_rpc._pacing_request_times),
            solana_rpc.RPC_PACING_HISTORY_LIMIT,
        )
        ignored = solana_rpc._physical_pacing_attribution(
            provider("helius"),
            "getTransaction",
            now_epoch=base + 30.0,
        )
        self.assertEqual(ignored, (None, ()))

    def test_semantic_digest_and_runtime_config_do_not_expose_secrets(self) -> None:
        secret = "private-address-or-api-key"
        digest = solana_rpc._semantic_request_digest(
            "getSignaturesForAddress",
            [secret, {"limit": 12}],
        )
        self.assertEqual(len(digest), 32)
        self.assertNotIn(secret.encode("utf-8"), digest)
        self.assertEqual(
            digest,
            solana_rpc._semantic_request_digest(
                "getSignaturesForAddress",
                [secret, {"limit": 12}],
            ),
        )
        solana_rpc._record_semantic_repetition(
            "getSignaturesForAddress",
            [secret, {"limit": 12}],
            now_monotonic=1.0,
        )
        self._coverage_rpc_metrics()
        self.assertNotIn(
            secret,
            coverage_telemetry.TELEMETRY_PATH.read_text("utf-8"),
        )
        config = solana_rpc.safe_rpc_runtime_config({
            "HELIUS_RPC_HTTP_URL": f"https://helius.invalid/?api-key={secret}",
            "HELIUS_RPC_MAX_RPS": "7",
            "SOLANA_RPC_PROVIDER_ATTEMPTS": "2",
            "SOLANA_RPC_OVERALL_ATTEMPT_BUDGET": "6",
        })
        serialized = json.dumps(config, sort_keys=True)
        self.assertNotIn(secret, serialized)
        self.assertNotIn("https://", serialized)
        helius = next(
            item for item in config["providers"] if item["name"] == "helius"
        )
        self.assertTrue(helius["enabled"])
        self.assertEqual(helius["max_rps"], 7.0)
        self.assertEqual(config["provider_local_attempts"], 2)
        self.assertEqual(config["overall_attempt_budget"], 6)

    def test_telemetry_failure_does_not_change_rpc_result(self) -> None:
        target = provider("alchemy")
        session = FakeSession({
            target.url: [FakeResponse(200, {"result": "unchanged"})],
        })
        with (
            patch.object(
                solana_rpc,
                "record_rpc_method_metric",
                side_effect=RuntimeError("telemetry-only"),
            ),
            patch.object(solana_rpc.logger, "exception"),
        ):
            result = asyncio.run(solana_rpc.solana_rpc_call(
                session,
                "getSignaturesForAddress",
                ["address"],
                providers=[target],
            ))

        self.assertEqual(result, "unchanged")
        self.assertEqual(len(session.calls), 1)

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

    def test_v4_heavy_availability_migrates_without_resetting_history(self) -> None:
        target = provider("solana_public")
        atomic_write_json(solana_rpc._state_path(target.name), {
            "schema_version": 4,
            "version": 19,
            "provider": target.name,
            "request_count": 11,
            "success_count": 7,
            "failure_count": 4,
            "rate_limit_count": 4,
            "consecutive_failures": 3,
            "cooldown_until_epoch": time.time() + 60,
            "circuit_state": "OPEN",
            "circuit_open_count": 2,
            "half_open_lease_until_epoch": 0.0,
            "last_failure_method": "getTransaction",
            "last_failure_category": "RATE_LIMIT",
            "last_rate_limit_method": "getTransaction",
            "last_circuit_open_method": "getTransaction",
            "method_metrics": {
                "getTransaction": {"failure_count": 4},
            },
            "method_availability": {
                "getSignaturesForAddress": {"unexpected": 1},
                "unboundedMethod": {"x": 1},
            },
        })

        state = solana_rpc.provider_state(target.name, [target])

        self.assertEqual(
            state["schema_version"],
            solana_rpc.RPC_PROVIDER_STATE_SCHEMA_VERSION,
        )
        self.assertEqual(state["request_count"], 11)
        self.assertEqual(state["failure_count"], 4)
        self.assertEqual(state["rate_limit_count"], 4)
        self.assertEqual(state["circuit_open_count"], 2)
        self.assertEqual(state["circuit_state"], "CLOSED")
        self.assertEqual(state["cooldown_until_epoch"], 0.0)
        self.assertEqual(
            set(state["method_availability"]),
            solana_rpc.METHOD_SCOPED_AVAILABILITY_METHODS,
        )
        self.assertNotIn(
            "unexpected",
            state["method_availability"]["getSignaturesForAddress"],
        )
        transaction = state["method_availability"]["getTransaction"]
        self.assertEqual(transaction["circuit_state"], "OPEN")
        self.assertEqual(transaction["consecutive_failures"], 3)
        self.assertEqual(transaction["circuit_open_count"], 2)
        self.assertIsNotNone(solana_rpc._reserve_provider_slot_sync(
            target,
            method="getSignaturesForAddress",
        ))
        self.assertIsNone(solana_rpc._reserve_provider_slot_sync(
            target,
            method="getTransaction",
        ))

    def test_v4_provider_wide_failure_remains_global_after_migration(self) -> None:
        target = provider("solana_public")
        cooldown = time.time() + 60
        atomic_write_json(solana_rpc._state_path(target.name), {
            "schema_version": 4,
            "provider": target.name,
            "consecutive_failures": 1,
            "cooldown_until_epoch": cooldown,
            "circuit_state": "CLOSED",
            "last_failure_method": "getTransaction",
            "last_failure_category": "HTTP_TRANSIENT",
        })

        state = solana_rpc.provider_state(target.name, [target])

        self.assertEqual(state["cooldown_until_epoch"], cooldown)
        self.assertEqual(
            state["method_availability"]["getTransaction"][
                "cooldown_until_epoch"
            ],
            0.0,
        )
        self.assertIsNone(solana_rpc._reserve_provider_slot_sync(
            target,
            method="getTransaction",
        ))
        self.assertIsNone(solana_rpc._reserve_provider_slot_sync(
            target,
            method="getSignaturesForAddress",
        ))

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

    def test_public_get_transaction_uses_two_second_persisted_spacing(
        self,
    ) -> None:
        target = provider("solana_public", max_rps=2.0)

        first = solana_rpc._reserve_provider_slot_sync(
            target, method="getTransaction", now_epoch=100.0
        )
        second = solana_rpc._reserve_provider_slot_sync(
            target, method="getTransaction", now_epoch=100.0
        )

        self.assertEqual(first.pacing_request_epoch, 100.0)
        self.assertEqual(second.pacing_request_epoch, 102.0)
        state = solana_rpc.provider_state(target.name, [target])
        self.assertEqual(state["last_request_at_epoch"], 102.0)
        self.assertEqual(
            state["method_last_request_at_epoch"],
            {"getTransaction": 102.0},
        )
        self.assertEqual(state["request_count"], 2)

    def test_public_signatures_ignore_transaction_only_pacing_state(self) -> None:
        target = provider("solana_public", max_rps=2.0)
        solana_rpc._reserve_provider_slot_sync(
            target, method="getTransaction", now_epoch=100.0
        )

        signature = solana_rpc._reserve_provider_slot_sync(
            target, method="getSignaturesForAddress", now_epoch=100.0
        )

        self.assertIsNotNone(signature)
        state = solana_rpc.provider_state(target.name, [target])
        self.assertEqual(state["last_request_at_epoch"], 100.5)
        self.assertEqual(
            state["method_last_request_at_epoch"]["getTransaction"], 100.0
        )

    def test_waiting_transaction_does_not_starve_public_signatures(self) -> None:
        target = provider("solana_public", max_rps=1_000.0)

        async def exercise() -> tuple[object, object]:
            state = solana_rpc._empty_provider_state(target.name)
            state["method_last_request_at_epoch"]["getTransaction"] = time.time()
            atomic_write_json(solana_rpc._state_path(target.name), state)
            transaction_task = asyncio.create_task(
                solana_rpc._reserve_provider_slot(
                    target, method="getTransaction"
                )
            )
            await asyncio.sleep(0.01)
            signature = await asyncio.wait_for(
                solana_rpc._reserve_provider_slot(
                    target, method="getSignaturesForAddress"
                ),
                timeout=0.1,
            )
            return await transaction_task, signature

        with patch.dict(
            solana_rpc.RPC_METHOD_PACING_INTERVAL_SECONDS,
            {("solana_public", "getTransaction"): 0.2},
            clear=True,
        ):
            transaction, signature = asyncio.run(exercise())

        self.assertIsNotNone(transaction)
        self.assertIsNotNone(signature)

    def test_helius_methods_keep_provider_global_pacing_only(self) -> None:
        target = provider("helius", max_rps=2.0)

        first = solana_rpc._reserve_provider_slot_sync(
            target, method="getTransaction", now_epoch=100.0
        )
        second = solana_rpc._reserve_provider_slot_sync(
            target, method="getSignaturesForAddress", now_epoch=100.0
        )

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        state = solana_rpc.provider_state(target.name, [target])
        self.assertEqual(state["last_request_at_epoch"], 100.5)
        self.assertEqual(state["method_last_request_at_epoch"], {})

    def test_method_pacing_sleep_does_not_hold_provider_state_lock(self) -> None:
        target = provider("solana_public", max_rps=2.0)
        state_path = solana_rpc._state_path(target.name)
        state = solana_rpc._empty_provider_state(target.name)
        state["method_last_request_at_epoch"]["getTransaction"] = 99.0
        atomic_write_json(state_path, state)
        original_lock = solana_rpc.exclusive_file_lock
        active_paths: set[Path] = set()
        clock = [100.0]
        waits: list[float] = []

        @contextmanager
        def observed_lock(path: Path, **options: object):
            with original_lock(path, **options):
                active_paths.add(path)
                try:
                    yield
                finally:
                    active_paths.remove(path)

        def fake_sleep(delay: float) -> None:
            self.assertNotIn(state_path, active_paths)
            waits.append(delay)
            clock[0] += delay

        with (
            patch.object(solana_rpc, "exclusive_file_lock", observed_lock),
            patch.object(solana_rpc.time, "time", side_effect=lambda: clock[0]),
            patch.object(solana_rpc.time, "sleep", side_effect=fake_sleep),
        ):
            reservation = solana_rpc._reserve_provider_slot_sync(
                target, method="getTransaction"
            )

        self.assertIsNotNone(reservation)
        self.assertEqual(waits, [1.0])
        self.assertEqual(reservation.pacing_request_epoch, 101.0)

    def test_method_pacing_reconciles_interleaved_global_reservation(self) -> None:
        target = provider("solana_public", max_rps=2.0)
        state = solana_rpc._empty_provider_state(target.name)
        state["method_last_request_at_epoch"]["getTransaction"] = 99.0
        atomic_write_json(solana_rpc._state_path(target.name), state)
        clock = [100.0]
        waits: list[float] = []

        def fake_sleep(delay: float) -> None:
            waits.append(delay)
            if len(waits) == 1:
                signature = solana_rpc._reserve_provider_slot_core_sync(
                    target,
                    method="getSignaturesForAddress",
                    now_epoch=101.2,
                )
                self.assertEqual(signature.pacing_request_epoch, None)
            clock[0] += delay

        with (
            patch.object(solana_rpc.time, "time", side_effect=lambda: clock[0]),
            patch.object(solana_rpc.time, "sleep", side_effect=fake_sleep),
        ):
            reservation = solana_rpc._reserve_provider_slot_sync(
                target, method="getTransaction"
            )

        self.assertEqual(len(waits), 2)
        self.assertAlmostEqual(waits[0], 1.0)
        self.assertAlmostEqual(waits[1], 0.7)
        self.assertAlmostEqual(reservation.pacing_request_epoch, 101.7)
        final_state = solana_rpc.provider_state(target.name, [target])
        self.assertAlmostEqual(final_state["last_request_at_epoch"], 101.7)
        self.assertAlmostEqual(
            final_state["method_last_request_at_epoch"]["getTransaction"],
            101.7,
        )

    def test_v5_method_pacing_migration_is_bounded(self) -> None:
        target = provider("solana_public")
        atomic_write_json(solana_rpc._state_path(target.name), {
            "schema_version": 5,
            "provider": target.name,
            "request_count": 7,
            "method_last_request_at_epoch": {
                "getTransaction": 123.0,
                "unboundedMethod": 999.0,
            },
        })

        state = solana_rpc.provider_state(target.name, [target])

        self.assertEqual(
            state["schema_version"],
            solana_rpc.RPC_PROVIDER_STATE_SCHEMA_VERSION,
        )
        self.assertEqual(state["request_count"], 7)
        self.assertEqual(
            state["method_last_request_at_epoch"],
            {"getTransaction": 123.0},
        )

    def test_method_paced_retry_and_failover_metrics_are_preserved(self) -> None:
        target = provider("solana_public", max_rps=2.0)

        reservation = solana_rpc._reserve_provider_slot_sync(
            target,
            method="getTransaction",
            retry=True,
            failover=True,
            now_epoch=100.0,
        )

        self.assertIsNotNone(reservation)
        metric = solana_rpc.provider_state(target.name, [target])[
            "method_metrics"
        ]["getTransaction"]
        self.assertEqual(metric["request_count"], 1)
        self.assertEqual(metric["retry_count"], 1)
        self.assertEqual(metric["failover_count"], 1)

    def test_public_get_transaction_spacing_is_cross_process(self) -> None:
        context = multiprocessing.get_context("spawn")
        start = context.Event()
        results = context.Queue()
        processes = [
            context.Process(
                target=paced_reservation_worker,
                args=(self.temporary.name, start, results),
            )
            for _ in range(2)
        ]
        for process in processes:
            process.start()
        start.set()
        for process in processes:
            process.join(timeout=15)
            self.assertEqual(process.exitcode, 0)
        epochs = sorted(results.get(timeout=2) for _ in processes)

        self.assertGreaterEqual(epochs[1] - epochs[0], 0.18)
        state = solana_rpc.provider_state("solana_public", [
            provider("solana_public", max_rps=1_000.0),
        ])
        self.assertEqual(state["request_count"], 2)
        self.assertAlmostEqual(
            state["method_last_request_at_epoch"]["getTransaction"],
            epochs[1],
            places=3,
        )

    def test_evidence_fixture_reduces_burst_without_skips(self) -> None:
        logical_requests = 12

        def simulate(interval_seconds: float) -> dict[str, int]:
            starts = [index * interval_seconds for index in range(logical_requests)]
            rate_limits = sum(
                sum(
                    prior >= started - 10.0
                    for prior in starts[:index + 1]
                ) >= 9
                for index, started in enumerate(starts)
            )
            return {
                "logical_requests": logical_requests,
                "physical_requests": len(starts),
                "pacing_waits": logical_requests - 1,
                "reservation_skips": 0,
                "rate_limits": rate_limits,
                "successes": logical_requests - rate_limits,
            }

        before = simulate(0.5)
        after = simulate(2.0)

        self.assertEqual(before["rate_limits"], 4)
        self.assertEqual(before["successes"], 8)
        self.assertEqual(after["rate_limits"], 0)
        self.assertEqual(after["successes"], 12)
        self.assertEqual(before["physical_requests"], logical_requests)
        self.assertEqual(after["physical_requests"], logical_requests)
        self.assertEqual(after["reservation_skips"], 0)

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
