from __future__ import annotations

import asyncio
import multiprocessing
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from src import (
    analyzer,
    helius_rpc,
    monitor,
    observation_analysis,
    observation_tracker,
    risk_manager,
)


VALID_MINT = "So11111111111111111111111111111111111111112"


def limiter_worker(
    state_path: str,
    interval_seconds: float,
    start: multiprocessing.synchronize.Event,
    results: multiprocessing.queues.Queue,
) -> None:
    from src import helius_rpc as worker_rpc

    worker_rpc.HELIUS_RATE_LIMIT_PATH = Path(state_path)
    start.wait()
    worker_rpc._wait_for_helius_slot_sync(interval_seconds)
    results.put(time.time())


class _Response:
    def __init__(
        self,
        status: int,
        payload: dict,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status = status
        self.payload = payload
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise AssertionError(f"unexpected HTTP status {self.status}")

    async def json(self) -> dict:
        return self.payload


class _Session:
    def __init__(self, responses: list[_Response]) -> None:
        self.responses = responses
        self.request_times: list[float] = []

    def post(self, *_args, **_kwargs) -> _Response:
        self.request_times.append(time.monotonic())
        return self.responses.pop(0)


class HeliusGlobalRateLimitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.original_path = helius_rpc.HELIUS_RATE_LIMIT_PATH
        helius_rpc.HELIUS_RATE_LIMIT_PATH = (
            Path(self.temporary.name) / "helius_rate_limit.json"
        )

    def tearDown(self) -> None:
        helius_rpc.HELIUS_RATE_LIMIT_PATH = self.original_path
        self.temporary.cleanup()

    def test_shared_limiter_serializes_spawned_processes(self) -> None:
        context = multiprocessing.get_context("spawn")
        start = context.Event()
        results = context.Queue()
        interval = 0.2
        workers = [
            context.Process(
                target=limiter_worker,
                args=(
                    str(helius_rpc.HELIUS_RATE_LIMIT_PATH),
                    interval,
                    start,
                    results,
                ),
            )
            for _ in range(2)
        ]
        for worker in workers:
            worker.start()
        start.set()
        for worker in workers:
            worker.join(15)
            self.assertEqual(worker.exitcode, 0)
        timestamps = sorted(results.get(timeout=2) for _ in workers)
        self.assertGreaterEqual(timestamps[1] - timestamps[0], interval - 0.03)

    def test_concurrent_coroutines_share_request_spacing(self) -> None:
        session = _Session([
            _Response(200, {"result": index}) for index in range(3)
        ])

        async def wait_slot() -> None:
            await asyncio.to_thread(
                helius_rpc._wait_for_helius_slot_sync,
                0.04,
            )

        async def scenario() -> list[int]:
            with patch.object(
                helius_rpc,
                "_wait_for_global_helius_slot",
                new=wait_slot,
            ):
                return await asyncio.gather(*[
                    helius_rpc.helius_rpc_call(
                        session,
                        "https://rpc.invalid",
                        "getBalance",
                        [str(index)],
                        max_attempts=1,
                    )
                    for index in range(3)
                ])

        self.assertCountEqual(asyncio.run(scenario()), [0, 1, 2])
        timestamps = sorted(session.request_times)
        for earlier, later in zip(timestamps, timestamps[1:]):
            self.assertGreaterEqual(later - earlier, 0.025)

    def test_retry_after_is_shared_and_respected(self) -> None:
        session = _Session([
            _Response(429, {}, {"Retry-After": "2"}),
            _Response(200, {"result": {"value": 42}}),
        ])
        defer = MagicMock()
        sleep = AsyncMock()

        async def scenario() -> dict:
            with (
                patch.object(
                    helius_rpc,
                    "_wait_for_global_helius_slot",
                    new=AsyncMock(),
                ),
                patch.object(
                    helius_rpc,
                    "_defer_helius_until_sync",
                    new=defer,
                ),
                patch.object(helius_rpc.asyncio, "sleep", new=sleep),
                patch.object(helius_rpc.random, "random", return_value=0.0),
            ):
                return await helius_rpc.helius_rpc_call(
                    session,
                    "https://rpc.invalid",
                    "getBalance",
                    [],
                )

        before = time.time()
        self.assertEqual(asyncio.run(scenario()), {"value": 42})
        sleep.assert_awaited_once_with(2.0)
        self.assertTrue(defer.call_args.kwargs["rate_limited"])
        self.assertGreaterEqual(defer.call_args.args[0], before + 1.9)

    def test_backoff_uses_reset_then_bounded_exponential(self) -> None:
        delay, source = helius_rpc.helius_backoff_seconds(
            {"x-ratelimit-reset": "1005"},
            0,
            1000.0,
        )
        self.assertEqual((delay, source), (5.0, "rate-limit-reset"))
        delay, source = helius_rpc.helius_backoff_seconds({}, 3, 1000.0)
        self.assertEqual((delay, source), (8.0, "exponential-fallback"))
        delay, _ = helius_rpc.helius_backoff_seconds({}, 10, 1000.0)
        self.assertEqual(delay, helius_rpc.HELIUS_MAX_BACKOFF_SECONDS)

    def test_jitter_never_reduces_provider_delay(self) -> None:
        self.assertEqual(
            helius_rpc.jittered_backoff_seconds(
                4.0,
                "retry-after",
                random_value=0.0,
            ),
            4.0,
        )
        self.assertEqual(
            helius_rpc.jittered_backoff_seconds(
                4.0,
                "retry-after",
                random_value=1.0,
            ),
            5.0,
        )
        self.assertEqual(
            helius_rpc.jittered_backoff_seconds(
                4.0,
                "exponential-fallback",
                random_value=0.0,
            ),
            3.0,
        )

    def test_retry_exhaustion_has_canonical_method_reason(self) -> None:
        session = _Session([
            _Response(429, {}, {"Retry-After": "0"}) for _ in range(5)
        ])
        defer = MagicMock()

        async def scenario() -> None:
            with (
                patch.object(
                    helius_rpc,
                    "_wait_for_global_helius_slot",
                    new=AsyncMock(),
                ),
                patch.object(
                    helius_rpc,
                    "_defer_helius_until_sync",
                    new=defer,
                ),
                patch.object(
                    helius_rpc.asyncio,
                    "sleep",
                    new=AsyncMock(),
                ),
                patch.object(helius_rpc.random, "random", return_value=0.0),
            ):
                await helius_rpc.helius_rpc_call(
                    session,
                    "https://rpc.invalid",
                    "getAccountInfo",
                    [],
                )

        with self.assertRaises(helius_rpc.HeliusRpcRateLimitError) as captured:
            asyncio.run(scenario())
        self.assertEqual(
            captured.exception.canonical_reason,
            "RPC_GET_ACCOUNT_INFO_RATE_LIMIT_EXHAUSTED",
        )
        self.assertEqual(len(session.request_times), 5)
        self.assertEqual(defer.call_count, 5)

    def test_normal_rpc_path_does_not_retry_or_sleep(self) -> None:
        session = _Session([_Response(200, {"result": {"value": 7}})])
        sleep = AsyncMock()

        async def scenario() -> dict:
            with (
                patch.object(
                    helius_rpc,
                    "_wait_for_global_helius_slot",
                    new=AsyncMock(),
                ),
                patch.object(helius_rpc.asyncio, "sleep", new=sleep),
            ):
                return await helius_rpc.helius_rpc_call(
                    session,
                    "https://rpc.invalid",
                    "getBalance",
                    [],
                )

        self.assertEqual(asyncio.run(scenario()), {"value": 7})
        self.assertEqual(len(session.request_times), 1)
        sleep.assert_not_awaited()


class AnalyzerDeduplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        analyzer._analysis_cache.clear()
        analyzer._analysis_flights.clear()
        self.settings = analyzer.AnalyzerSettings(
            rpc_url="https://rpc.invalid"
        )

    def tearDown(self) -> None:
        analyzer._analysis_cache.clear()
        analyzer._analysis_flights.clear()

    def test_same_mint_single_flight_and_short_cache_remove_duplicates(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        async def analyze(*_args) -> analyzer.SafetyReport:
            started.set()
            await release.wait()
            return analyzer.SafetyReport(
                mint=VALID_MINT,
                safety_score=80,
                should_enter=True,
                reasons=["stable"],
            )

        call = AsyncMock(side_effect=analyze)

        async def scenario() -> list[analyzer.SafetyReport]:
            with patch.object(
                analyzer,
                "_analyze_token_uncached",
                new=call,
            ):
                tasks = [
                    asyncio.create_task(
                        analyzer.analyze_token(VALID_MINT, self.settings)
                    )
                    for _ in range(5)
                ]
                await started.wait()
                release.set()
                reports = await asyncio.gather(*tasks)
                reports.append(
                    await analyzer.analyze_token(VALID_MINT, self.settings)
                )
                return reports

        reports = asyncio.run(scenario())
        self.assertEqual(call.await_count, 1)
        self.assertEqual(len(reports), 6)
        reports[0].reasons.append("mutated")
        self.assertEqual(reports[-1].reasons, ["stable"])

    def test_cache_expiry_rechecks_the_token(self) -> None:
        call = AsyncMock(return_value=analyzer.SafetyReport(mint=VALID_MINT))

        async def scenario() -> None:
            with (
                patch.object(
                    analyzer,
                    "_analyze_token_uncached",
                    new=call,
                ),
                patch.object(analyzer, "ANALYZER_CACHE_TTL_SECONDS", 0.0),
            ):
                await analyzer.analyze_token(VALID_MINT, self.settings)
                await analyzer.analyze_token(VALID_MINT, self.settings)

        asyncio.run(scenario())
        self.assertEqual(call.await_count, 2)

    def test_rate_limit_failure_is_not_cached_and_remains_fail_closed(self) -> None:
        exhausted = helius_rpc.HeliusRpcRateLimitError(
            "getTokenSupply",
            5,
        )
        call = AsyncMock(side_effect=[
            exhausted,
            analyzer.SafetyReport(mint=VALID_MINT),
        ])

        async def scenario() -> None:
            with patch.object(
                analyzer,
                "_analyze_token_uncached",
                new=call,
            ):
                with self.assertRaises(helius_rpc.HeliusRpcRateLimitError):
                    await analyzer.analyze_token(VALID_MINT, self.settings)
                await analyzer.analyze_token(VALID_MINT, self.settings)

        asyncio.run(scenario())
        self.assertEqual(call.await_count, 2)

        async def fail_closed() -> bool:
            with patch.object(
                analyzer,
                "analyze_token",
                new=AsyncMock(side_effect=exhausted),
            ):
                return await analyzer.should_enter_token(VALID_MINT)

        self.assertFalse(asyncio.run(fail_closed()))


class ResearchFailureClassificationTests(unittest.TestCase):
    def test_analysis_keeps_bounded_rate_limit_reason(self) -> None:
        row = {
            "quote_status": "PROCESSING_FAILED",
            "decision_reasons": [
                "RPC_GET_ACCOUNT_INFO_RATE_LIMIT_EXHAUSTED"
            ],
        }
        self.assertEqual(
            observation_analysis._missing_outcome_reason(row, None),
            "RPC_GET_ACCOUNT_INFO_RATE_LIMIT_EXHAUSTED",
        )
        row["decision_reasons"] = ["raw exception with changing details"]
        self.assertEqual(
            observation_analysis._missing_outcome_reason(row, None),
            "PROCESSING_FAILED",
        )

    def test_analyzer_rate_limit_is_persisted_as_bounded_reason(self) -> None:
        decision = observation_tracker.ObservationDecision(
            True,
            "SIGNATURE:MINT",
            False,
            ("research_v1_60m",),
        )
        finalize = MagicMock(return_value=True)
        rpc_skip = AsyncMock()
        exhausted = helius_rpc.HeliusRpcRateLimitError(
            "getTokenSupply",
            5,
        )
        with (
            patch.object(
                observation_tracker,
                "observation_mode_enabled",
                return_value=True,
            ),
            patch.object(
                observation_tracker,
                "record_candidate_discovery",
                new=AsyncMock(return_value=decision),
            ),
            patch.object(
                observation_tracker,
                "finalize_candidate_without_quote",
                new=finalize,
            ),
            patch.object(
                analyzer,
                "analyze_token",
                new=AsyncMock(side_effect=exhausted),
            ),
            patch.object(
                monitor.state_store,
                "get_route_initial_stop_streak",
                return_value=(0, 0.0),
            ),
            patch.object(
                monitor,
                "token_cooldown_is_active",
                return_value=False,
            ),
            patch.object(
                risk_manager,
                "record_rpc_skip",
                new=rpc_skip,
            ),
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
            ))

        rpc_skip.assert_awaited_once()
        self.assertEqual(
            finalize.call_args.kwargs["decision_reasons"],
            ["RPC_GET_TOKEN_SUPPLY_RATE_LIMIT_EXHAUSTED"],
        )
        self.assertEqual(
            finalize.call_args.kwargs["quote_status"],
            "PROCESSING_FAILED",
        )


if __name__ == "__main__":
    unittest.main()
