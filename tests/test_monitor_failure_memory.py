from __future__ import annotations

import asyncio
import contextlib
import json
import tempfile
import unittest
import weakref
from pathlib import Path
from unittest import mock

from src import failure_memory_diagnostics as diagnostics, monitor


class WeakDict(dict):
    pass


class WeakList(list):
    pass


class Scope:
    def add_metadata(self, **_values):
        pass

    def set_metadata(self, _values):
        pass


class CandidateSession:
    def __init__(self):
        self.calls = []
        self.raw_refs = []

    def get(self, url, **_kwargs):
        self.calls.append(url)
        owner = self

        class Response:
            content_length = 128
            _body = b"x" * 128

            def raise_for_status(self):
                pass

            async def json(self):
                if "search" in url:
                    payload = WeakDict({"pairs": [
                        {
                            "chainId": "solana", "pairAddress": "PAIR",
                            "baseToken": {"address": "APPROVED"},
                            "txns": {"m5": {"buys": 40, "sells": 10}},
                            "volume": {"m5": 20_000}, "liquidity": {"usd": 10_000},
                            "pairCreatedAt": 1,
                        },
                        {
                            "chainId": "solana", "pairAddress": "SHADOW_PAIR",
                            "baseToken": {"address": "SHADOW"},
                            "txns": {"m5": {"buys": 40, "sells": 10}},
                            "volume": {"m5": 14_999}, "liquidity": {"usd": 10_000},
                            "pairCreatedAt": 1,
                        },
                    ]})
                else:
                    payload = WeakList()
                owner.raw_refs.append(weakref.ref(payload))
                return payload

        @contextlib.asynccontextmanager
        async def response():
            yield Response()

        return response()


class MonitorFailureMemoryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.recorder = diagnostics._Recorder(Path(self.temporary.name) / "failure.json")
        self.events = []
        self.stack = contextlib.ExitStack()
        self.stack.enter_context(mock.patch.object(diagnostics, "_recorder", self.recorder))
        self.stack.enter_context(mock.patch.object(monitor, "phase_memory", side_effect=lambda *_a, **_k: contextlib.nullcontext(Scope())))
        self.stack.enter_context(mock.patch.object(monitor, "record_funnel_stage"))
        self.stack.enter_context(mock.patch.object(monitor, "record_confirmation_result"))
        self.stack.enter_context(mock.patch.object(monitor, "add_current_phase_metadata"))
        self.stack.enter_context(mock.patch.object(monitor, "maybe_trim_allocator"))
        self.stack.enter_context(mock.patch.object(monitor, "_momentum_snapshot_store", monitor.MomentumSnapshotStore()))
        self.stack.enter_context(mock.patch.object(self.recorder, "capture", side_effect=self.capture))
        self.raw_refs = []

    def tearDown(self):
        self.stack.close()
        self.temporary.cleanup()

    def capture(self, *_args, **_kwargs):
        self.events.append({
            "phases": [
                (row["name"], row["stage"], dict(row["counts"]))
                for row in self.recorder.active.values()
            ],
            "raw_alive": [reference() is not None for reference in self.raw_refs],
        })

    async def test_candidate_stages_preserve_results_and_release_real_raw_graph(self):
        session = CandidateSession()
        self.raw_refs = session.raw_refs
        trim_phases = []

        def trim(**_values):
            trim_phases.extend((row["name"], row["stage"]) for row in self.recorder.active.values())

        with mock.patch.object(monitor, "maybe_trim_allocator", side_effect=trim):
            approved, shadows = await monitor.fetch_momentum_candidate_cohorts(session)
        self.assertEqual(trim_phases, [("candidate_fetch", "raw_released")])
        self.assertEqual([row.mint for row in approved], ["APPROVED"])
        self.assertEqual([row.candidate.mint for row in shadows], ["SHADOW"])
        self.assertEqual(shadows[0].rejection_reasons, ("MOMENTUM_VOLUME_UNDER_MIN",))
        self.assertEqual(session.calls, [monitor.DEX_SCREENER_SEARCH_URL, monitor.DEX_SCREENER_PROFILES_URL, monitor.DEX_SCREENER_BOOSTS_URL])
        stages = [event["phases"][0][1] for event in self.events]
        self.assertLess(stages.index("http_body_decoded"), stages.index("raw_materialized"))
        self.assertLess(stages.index("compact_projected"), stages.index("raw_released"))
        self.assertLess(stages.index("raw_released"), stages.index("sorted"))
        for event in self.events:
            _, stage, counts = event["phases"][0]
            if stage == "http_body_decoded":
                self.assertEqual(counts["response_body_bytes"], 128)
                self.assertTrue(counts["response_body_live"])
                self.assertTrue(event["raw_alive"][-1])
            if stage == "compact_projected" and counts.get("raw_graph_live"):
                self.assertFalse(counts["response_body_live"])
                self.assertTrue(event["raw_alive"][-1])
            if stage == "raw_released":
                self.assertEqual(counts["raw_payload_live_count"], 0)
                self.assertFalse(any(event["raw_alive"]))
            self.assertTrue(all(type(value) in (int, float, bool, type(None)) for value in counts.values()))
        self.assertFalse(self.recorder.active)
        self.assertFalse(any(reference() is not None for reference in session.raw_refs))

    async def test_cancelled_candidate_cleans_active_phase_without_release_claim(self):
        started = asyncio.Event()

        async def fetch(*_args, **_kwargs):
            started.set()
            await asyncio.Event().wait()

        with mock.patch.object(monitor, "_dexscreener_json", new=fetch):
            task = asyncio.create_task(monitor.fetch_momentum_candidate_cohorts(object()))
            await started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertFalse(self.recorder.active)
        stages = [row[1] for event in self.events for row in event["phases"]]
        self.assertIn("failed", stages)
        self.assertNotIn("raw_released", stages)

    async def test_whale_signature_graph_lives_with_one_raw_transaction_and_releases(self):
        methods = []
        trim_phases = []

        async def rpc(_session, _url, method, _params):
            methods.append(method)
            if method == "getSignaturesForAddress":
                rows = WeakList([{"signature": "SIG", "err": None}])
                self.raw_refs.append(weakref.ref(rows))
                return rows
            transaction = WeakDict({"transaction": {"message": {"accountKeys": []}}, "meta": {}})
            self.raw_refs.append(weakref.ref(transaction))
            return transaction

        candidate = monitor.MomentumCandidate("MINT", "PAIR", 20_000, 40, 10, 10_000, 1_000)
        def trim(**_values):
            trim_phases.extend((row["name"], row["stage"]) for row in self.recorder.active.values())

        with (
            mock.patch.object(monitor, "_solana_rpc", new=rpc),
            mock.patch.object(monitor, "maybe_trim_allocator", side_effect=trim),
        ):
            result = await monitor._confirm_unknown_whales_with_telemetry(object(), "", candidate, set())
        self.assertEqual(trim_phases, [("whale_confirmation", "raw_released")])
        self.assertEqual(result, [])
        self.assertEqual(methods, ["getSignaturesForAddress", "getTransaction"])
        materialized = [event for event in self.events if any(row[1] == "transaction_materialized" for row in event["phases"])]
        self.assertTrue(materialized)
        self.assertEqual(materialized[0]["raw_alive"], [True, True])
        self.assertEqual({row[0] for row in materialized[0]["phases"]}, {"whale_confirmation", "smart_get_transaction"})
        released = [event for event in self.events if any(row[1] == "transaction_released" for row in event["phases"])]
        self.assertEqual(released[0]["raw_alive"], [True, False])
        transaction_phase = next(row for row in released[0]["phases"] if row[0] == "smart_get_transaction")
        self.assertEqual(transaction_phase[2]["raw_payload_live_count"], 0)
        self.assertEqual(transaction_phase[2]["raw_transaction_count"], 0)
        self.assertEqual(transaction_phase[2]["transaction_fetch_active_count"], 0)
        final = next(event for event in self.events if any(row[1] == "raw_released" for row in event["phases"]))
        self.assertEqual(final["raw_alive"], [False, False])
        self.assertEqual(final["phases"][0][2]["raw_payload_live_count"], 0)
        self.assertEqual(final["phases"][0][2]["raw_signature_count"], 0)
        self.assertFalse(self.recorder.active)

    async def test_counter_loop_only_updates_scalar_counts_and_cancels(self):
        values = []
        ready = asyncio.Event()

        def update(**counts):
            values.append(counts)
            ready.set()

        with mock.patch.object(monitor, "update_runtime_counts", side_effect=update):
            task = asyncio.create_task(monitor.failure_memory_counter_loop())
            await ready.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(len(values), 1)
        self.assertEqual(diagnostics._scalars(values[0]), values[0])
        self.assertGreaterEqual(values[0]["asyncio_live_task_count"], 2)
        self.assertTrue(all(type(value) is int for value in values[0].values()))

    async def test_diagnostic_counts_do_not_reject_unused_malformed_balance_fields(self):
        transaction = {
            "transaction": {"message": {"accountKeys": []}},
            "meta": {"preTokenBalances": 1, "postTokenBalances": True},
        }
        self.assertEqual(monitor.unknown_whale_buy_from_transaction(transaction, "MINT", set()), [])

    async def test_restored_transaction_phase_covers_downstream_consumption(self):
        program = next(iter(monitor.DEX_PROGRAMS.values()))

        class Socket:
            def __init__(self):
                self.messages = iter([
                    {"id": 1, "result": 99},
                    {"params": {"result": {"value": {
                        "signature": "SIG", "err": None,
                        "logs": [f"Program {program} invoke [1]"],
                    }}}},
                ])

            async def send(self, _payload):
                pass

            def __aiter__(self):
                return self

            async def __anext__(self):
                try:
                    return json.dumps(next(self.messages))
                except StopIteration:
                    raise StopAsyncIteration

        @contextlib.asynccontextmanager
        async def connection(*_args, **_kwargs):
            yield Socket()

        @contextlib.asynccontextmanager
        async def session(*_args, **_kwargs):
            yield object()

        async def fetch(*_args):
            raw = WeakDict({"transaction": {}, "meta": {}})
            self.raw_refs.append(weakref.ref(raw))
            return raw

        consumed = []

        def consume(raw, *_args, **_kwargs):
            consumed.append(raw is self.raw_refs[0]())
            self.assertEqual([row["name"] for row in self.recorder.active.values()], ["smart_get_transaction"])

        with (
            mock.patch.object(monitor, "_connect_with_memory_phase", new=connection),
            mock.patch.object(monitor.aiohttp, "ClientSession", new=session),
            mock.patch.object(monitor, "fetch_transaction", new=fetch),
            mock.patch.object(monitor, "print_buys", new=consume),
            mock.patch.object(monitor.state_store, "set_global_metrics"),
        ):
            await monitor.monitor_standard_once(monitor.MonitorSettings("wss://invalid", "https://invalid"), ("WALLET",))
        self.assertEqual(consumed, [True])
        projected = next(event for event in self.events if event["phases"][0][1] == "transaction_projected")
        released = next(event for event in self.events if event["phases"][0][1] == "raw_released")
        self.assertEqual(projected["raw_alive"], [True])
        self.assertEqual(released["raw_alive"], [False])
        self.assertEqual(projected["phases"][0][2]["transaction_fetch_active_count"], 0)
        self.assertEqual(released["phases"][0][2]["raw_payload_live_count"], 0)
        self.assertEqual(released["phases"][0][2]["raw_transaction_count"], 0)
        self.assertFalse(self.recorder.active)

    async def test_transaction_retry_records_pending_and_none_result_accurately(self):
        with (
            mock.patch.object(monitor, "solana_rpc_call", new=mock.AsyncMock(side_effect=[None, {"meta": {}}])),
            mock.patch.object(monitor, "record_memory_phase"),
            mock.patch.object(monitor, "record_transaction_payload"),
            mock.patch.object(monitor.asyncio, "sleep", new=mock.AsyncMock()),
        ):
            with diagnostics.diagnostic_phase("smart_get_transaction"):
                result = await monitor.fetch_transaction(object(), "", "SIG")
        self.assertEqual(result, {"meta": {}})
        pending = [row[2]["transaction_fetch_active_count"] for event in self.events for row in event["phases"] if row[1] == "transaction_fetch"]
        self.assertEqual(pending, [1, 0, 1, 0])
        materialized = [row[2] for event in self.events for row in event["phases"] if row[1] == "transaction_materialized"]
        self.assertEqual([row["raw_transaction_count"] for row in materialized], [0, 1])
        self.assertEqual([row["raw_payload_live_count"] for row in materialized], [0, 1])
        self.assertEqual([row["transaction_fetch_active_count"] for row in materialized], [0, 0])

    async def test_service_cancels_counter_and_stops_optional_sampler(self):
        from src import observation_tracker, wallet_performance

        counter_stopped = asyncio.Event()

        async def counter():
            try:
                await asyncio.Event().wait()
            finally:
                counter_stopped.set()

        sampler = mock.Mock()
        old_sampler = mock.Mock()
        with (
            mock.patch.object(monitor.MonitorSettings, "from_env", return_value=monitor.MonitorSettings("wss://invalid", "https://invalid")),
            mock.patch.object(monitor.state_store, "set_global_metrics"),
            mock.patch.object(monitor, "start_memory_attribution_sampler", return_value=old_sampler),
            mock.patch.object(monitor, "start_failure_sampler", return_value=sampler),
            mock.patch.object(monitor, "failure_memory_counter_loop", new=counter),
            mock.patch.object(monitor, "run_forever", new=mock.AsyncMock()),
            mock.patch.object(monitor, "run_market_momentum_route", new=mock.AsyncMock()),
            mock.patch.object(monitor, "monitor_maintenance_loop", new=mock.AsyncMock()),
            mock.patch.object(wallet_performance, "performance_loop", new=mock.AsyncMock()),
            mock.patch.object(observation_tracker, "observation_mode_enabled", return_value=True),
            mock.patch.object(observation_tracker, "approved_signal_paper_mode_enabled", return_value=False),
            mock.patch.object(observation_tracker, "observation_supervisor", new=mock.AsyncMock()),
        ):
            await monitor.run_service()
        self.assertTrue(counter_stopped.is_set())
        sampler.stop.assert_called_once_with()
        old_sampler.stop.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
