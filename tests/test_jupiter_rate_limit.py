from __future__ import annotations

import asyncio
import multiprocessing
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from src import executor


def limiter_worker(
    state_path: str,
    interval_seconds: float,
    start: multiprocessing.synchronize.Event,
    results: multiprocessing.queues.Queue,
) -> None:
    from src import executor as worker_executor

    worker_executor.JUPITER_RATE_LIMIT_PATH = Path(state_path)
    start.wait()
    worker_executor._wait_for_jupiter_slot_sync(interval_seconds)
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

    def get(self, *_args, **_kwargs) -> _Response:
        return self.responses.pop(0)


class JupiterRateLimitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.state_path = Path(self.temporary.name) / "jupiter_rate_limit.json"
        executor.JUPITER_RATE_LIMIT_PATH = self.state_path

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_shared_limiter_serializes_spawned_processes(self) -> None:
        context = multiprocessing.get_context("spawn")
        start = context.Event()
        results = context.Queue()
        interval = 0.2
        workers = [
            context.Process(
                target=limiter_worker,
                args=(str(self.state_path), interval, start, results),
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

    def test_reset_header_and_fallback_backoff_are_bounded(self) -> None:
        now = 1_000.0
        delay, source = executor._jupiter_backoff_seconds("1005", 0, now)
        self.assertAlmostEqual(delay, 5.05)
        self.assertEqual(source, "rate-limit-reset")

        delay, source = executor._jupiter_backoff_seconds(None, 3, now)
        self.assertEqual(delay, 8.0)
        self.assertEqual(source, "exponential-fallback")

        delay, _ = executor._jupiter_backoff_seconds("999999", 0, now)
        self.assertEqual(delay, executor._JUPITER_MAX_RESET_WAIT_SECONDS)

    def test_429_retries_without_logging_api_key(self) -> None:
        api_key = "jup_super_secret_test_key"
        session = _Session([
            _Response(429, {}),
            _Response(200, {"routePlan": [{"swapInfo": {}}], "outAmount": "42"}),
        ])

        async def scenario() -> dict:
            with (
                patch.object(
                    executor,
                    "_wait_for_global_jupiter_slot",
                    new=AsyncMock(),
                ),
                patch.object(executor, "_defer_jupiter_until_sync"),
                patch.object(executor.asyncio, "sleep", new=AsyncMock()),
            ):
                return await executor.jupiter_quote(
                    session, api_key, "INPUT", "OUTPUT", 100
                )

        with self.assertLogs("executor", level="WARNING") as captured:
            quote = asyncio.run(scenario())

        self.assertEqual(quote["outAmount"], "42")
        logs = "\n".join(captured.output)
        self.assertIn("exponential-fallback", logs)
        self.assertNotIn(api_key, logs)


if __name__ == "__main__":
    unittest.main()
