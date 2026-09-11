from __future__ import annotations

import asyncio
import re
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from src import phase_memory_telemetry as telemetry
from src import state_store


MIB = 1024 * 1024
PROJECT_ROOT = Path(__file__).resolve().parents[1]


class SequenceClock:
    def __init__(self, *values: float) -> None:
        self.values = iter(values)

    def __call__(self) -> float:
        return next(self.values)


class SequenceMemory:
    def __init__(self, *values: tuple[int | None, int | None]) -> None:
        self.values = iter(values)

    def __call__(self) -> dict[str, int | None]:
        rss, hwm = next(self.values)
        return {"rss_bytes": rss, "hwm_bytes": hwm}


class PhaseMemoryTelemetryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "monitor_memory_phases.json"
        telemetry._active_phase_counts.clear()
        telemetry._active_contexts.clear()
        telemetry._active_context_overflow_count = 0
        telemetry._pending_batch = None
        telemetry._sampler_last_hwm_bytes = None
        telemetry._sampler_previous_contexts = []
        telemetry._sampler_previous_active_phases = []
        telemetry._sampler_thread = None
        telemetry._sampler_stop_event = None
        telemetry._detail_flush_requested = False
        telemetry._detail_flush_running = False
        telemetry._last_detail_flush_monotonic = 0.0
        self.path_patch = mock.patch.object(
            telemetry, "MEMORY_PHASE_PATH", self.path
        )
        self.path_patch.start()
        self.background_flush = telemetry._request_background_flush
        self.background_patch = mock.patch.object(
            telemetry, "_request_background_flush", return_value=False
        )
        self.background_patch.start()

    def tearDown(self) -> None:
        telemetry._active_phase_counts.clear()
        telemetry._active_contexts.clear()
        telemetry._active_context_overflow_count = 0
        telemetry._pending_batch = None
        telemetry._sampler_last_hwm_bytes = None
        telemetry._sampler_previous_contexts = []
        telemetry._sampler_previous_active_phases = []
        telemetry._sampler_thread = None
        telemetry._sampler_stop_event = None
        telemetry._detail_flush_requested = False
        telemetry._detail_flush_running = False
        telemetry._last_detail_flush_monotonic = 0.0
        self.background_patch.stop()
        self.path_patch.stop()
        self.temporary.cleanup()

    def test_proc_measurement_reads_rss_and_hwm_in_bytes(self) -> None:
        result = telemetry.process_memory_measurement(
            status_text="VmRSS: 123 kB\nVmHWM: 456 kB\nVmSize: 999 kB\n"
        )
        self.assertEqual(result["rss_bytes"], 123 * 1024)
        self.assertEqual(result["hwm_bytes"], 456 * 1024)

    def test_phase_allowlist_is_the_canonical_fixed_set(self) -> None:
        self.assertEqual(telemetry.ALLOWED_PHASES, frozenset({
            "candidate_fetch",
            "whale_confirmation",
            "whale_signature_retrieval",
            "whale_signature_projection",
            "whale_transaction_fetch",
            "whale_transaction_parse",
            "whale_transaction_matching",
            "whale_confirmation_aggregation",
            "whale_result_projection",
            "analyzer",
            "observation_due_scan",
            "coverage_telemetry_flush",
            "hourly_rollup",
            "archive_write",
            "archive_record_preparation",
            "archive_serialization_write",
            "archive_metric_write",
            "archive_retention_projection",
            "wallet_performance_refresh",
            "wallet_reload",
            "ws_refresh_reconnect",
        }))

    def test_nested_overlap_keeps_only_phase_names_and_counts(self) -> None:
        outer_memory = SequenceMemory((100 * MIB, 100 * MIB), (126 * MIB, 126 * MIB))
        inner_memory = SequenceMemory((101 * MIB, 101 * MIB), (124 * MIB, 124 * MIB))
        with telemetry.phase_memory(
            "analyzer",
            memory_reader=outer_memory,
            epoch_clock=SequenceClock(10.0, 12.0),
            monotonic_clock=SequenceClock(20.0, 22.0),
        ):
            with telemetry.phase_memory(
                "coverage_telemetry_flush",
                memory_reader=inner_memory,
                epoch_clock=SequenceClock(10.5, 11.5),
                monotonic_clock=SequenceClock(20.5, 21.5),
            ):
                pass

        self.assertTrue(telemetry.flush_phase_memory_telemetry())
        document = state_store.read_json(self.path, {})
        events = {event["phase"]: event for event in document["events"]}
        inner_start = events["coverage_telemetry_flush"]["active_at_start"]
        inner_end = events["coverage_telemetry_flush"]["active_at_end"]
        self.assertEqual(
            inner_start,
            [{"phase": "analyzer", "count": 1},
             {"phase": "coverage_telemetry_flush", "count": 1}],
        )
        self.assertEqual(inner_end, [{"phase": "analyzer", "count": 1}])
        self.assertEqual(
            events["coverage_telemetry_flush"]["context_stack_at_start"],
            ["analyzer", "coverage_telemetry_flush"],
        )
        self.assertEqual(
            events["coverage_telemetry_flush"]["context_stack_at_end"],
            ["analyzer"],
        )
        self.assertEqual(
            events["coverage_telemetry_flush"]["context_id"],
            events["analyzer"]["context_id"],
        )
        self.assertNotEqual(
            events["coverage_telemetry_flush"]["span_id"],
            events["analyzer"]["span_id"],
        )
        serialized = repr(document).lower()
        self.assertNotIn("token_id", serialized)
        self.assertNotIn("task_id", serialized)
        self.assertNotIn("phase_instance_id", serialized)

    def test_detail_ring_evicts_old_events_but_preserves_aggregate_max(self) -> None:
        for index in range(telemetry.DETAIL_EVENT_LIMIT + 5):
            base = 100 * MIB
            increase = (21 + index) * MIB
            with telemetry.phase_memory(
                "analyzer",
                memory_reader=SequenceMemory(
                    (base, base), (base + increase, base + increase)
                ),
                epoch_clock=SequenceClock(float(index), float(index) + 0.5),
                monotonic_clock=SequenceClock(float(index), float(index) + 0.25),
            ):
                pass

        self.assertTrue(telemetry.flush_phase_memory_telemetry())
        document = state_store.read_json(self.path, {})
        self.assertEqual(len(document["events"]), telemetry.DETAIL_EVENT_LIMIT)
        self.assertEqual(document["total_phase_count"], telemetry.DETAIL_EVENT_LIMIT + 5)
        self.assertEqual(document["detail_retained_count"], telemetry.DETAIL_EVENT_LIMIT + 5)
        self.assertEqual(document["events"][0]["started_at_epoch"], 5.0)
        self.assertEqual(
            document["phases"]["analyzer"]["max_rss_delta_bytes"],
            (21 + telemetry.DETAIL_EVENT_LIMIT + 4) * MIB,
        )

    def test_evicted_peak_keeps_timestamp_workload_and_overlap_context(self) -> None:
        with telemetry.phase_memory(
            "analyzer",
            metadata={"workload": "analyzer", "candidate_count": 1},
            memory_reader=SequenceMemory(
                (100 * MIB, 100 * MIB), (250 * MIB, 250 * MIB)
            ),
            epoch_clock=SequenceClock(1.0, 2.0),
            monotonic_clock=SequenceClock(1.0, 2.0),
        ):
            pass
        for index in range(telemetry.DETAIL_EVENT_LIMIT + 1):
            with telemetry.phase_memory(
                "candidate_fetch",
                metadata={"workload": "momentum", "candidate_count": 2},
                memory_reader=SequenceMemory(
                    (100 * MIB, 100 * MIB), (121 * MIB, 121 * MIB)
                ),
                epoch_clock=SequenceClock(3.0 + index, 3.5 + index),
                monotonic_clock=SequenceClock(3.0 + index, 3.5 + index),
            ):
                pass

        self.assertTrue(telemetry.flush_phase_memory_telemetry())
        document = state_store.read_json(self.path, {})
        self.assertNotIn(1.0, {
            event["started_at_epoch"] for event in document["events"]
        })
        peak = document["maxima"]["max_rss_bytes_context"]
        self.assertEqual(peak["phase"], "analyzer")
        self.assertEqual(peak["started_at_epoch"], 1.0)
        self.assertEqual(peak["metadata"]["workload"], "analyzer")

    def test_measurement_and_persistence_failures_are_fail_open(self) -> None:
        def broken_measurement() -> dict[str, int | None]:
            raise OSError("proc unavailable")

        completed: list[str] = []
        with mock.patch.object(
            telemetry.state_store, "update_json", side_effect=OSError("disk full")
        ):
            with telemetry.phase_memory(
                "analyzer",
                memory_reader=broken_measurement,
            ) as scope:
                completed.append("work")
            flushed = telemetry.flush_phase_memory_telemetry()
        self.assertEqual(completed, ["work"])
        self.assertIsNotNone(scope.result)
        self.assertFalse(scope.result["persisted"])
        self.assertTrue(scope.result["measurement_failed"])
        self.assertFalse(flushed)
        self.assertEqual(telemetry._active_phase_counts, {})
        self.assertTrue(telemetry.flush_phase_memory_telemetry())
        document = state_store.read_json(self.path, {})
        self.assertEqual(document["measurement_failure_count"], 1)
        self.assertEqual(document["persistence_failure_count"], 1)

    def test_scope_and_context_metadata_failures_are_fail_open(self) -> None:
        completed: list[str] = []
        with mock.patch.object(
            telemetry, "start_phase", side_effect=RuntimeError("telemetry start")
        ):
            with telemetry.phase_memory("analyzer"):
                completed.append("start-failed-open")

        with mock.patch.object(
            telemetry, "finish_phase", side_effect=RuntimeError("telemetry finish")
        ):
            with telemetry.phase_memory(
                "analyzer",
                memory_reader=SequenceMemory(
                    (100 * MIB, 100 * MIB), (101 * MIB, 101 * MIB)
                ),
            ) as scope:
                with mock.patch.object(
                    scope.token, "add_metadata", side_effect=RuntimeError("metadata")
                ):
                    self.assertFalse(scope.add_metadata(row_count=1))
                    self.assertFalse(
                        telemetry.add_current_phase_metadata(row_count=1)
                    )
                completed.append("finish-failed-open")

        self.assertEqual(
            completed, ["start-failed-open", "finish-failed-open"]
        )
        self.assertEqual(scope.result["persistence_status"], "FAILED")
        self.assertEqual(telemetry._active_phase_counts, {})

    def test_detailed_flush_signal_has_only_one_daemon_writer(self) -> None:
        with mock.patch.object(telemetry.threading, "Thread") as thread_type:
            self.assertTrue(self.background_flush())
            self.assertTrue(self.background_flush())
        thread_type.assert_called_once_with(
            target=telemetry._detail_flush_worker,
            name="phase-memory-flush",
            daemon=True,
        )
        thread_type.return_value.start.assert_called_once_with()

    def test_same_phase_nesting_reuses_outer_sample(self) -> None:
        memory = SequenceMemory((100 * MIB, 100 * MIB), (121 * MIB, 121 * MIB))
        with telemetry.phase_memory(
            "wallet_performance_refresh",
            metadata={"workload": "wallet_performance"},
            memory_reader=memory,
        ) as outer:
            with telemetry.phase_memory("wallet_performance_refresh") as inner:
                inner.add_metadata(replacement_count=1)
            outer.add_metadata(success_count=1)

        self.assertTrue(telemetry.flush_phase_memory_telemetry())
        document = state_store.read_json(self.path, {})
        aggregate = document["phases"]["wallet_performance_refresh"]
        self.assertEqual(aggregate["count"], 1)
        self.assertEqual(aggregate["metadata_totals"]["replacement_count"], 1)
        self.assertEqual(aggregate["metadata_totals"]["success_count"], 1)

    def test_sensitive_or_raw_metadata_is_rejected(self) -> None:
        invalid = (
            {"mint": "secret-identifier"},
            {"payload_bytes": {"raw": "payload"}},
            {"workload": "wallet-address"},
            {"row_count": [1, 2, 3]},
        )
        for metadata in invalid:
            with self.subTest(metadata=metadata):
                with self.assertRaises(ValueError):
                    telemetry.start_phase("analyzer", metadata=metadata)

    def test_atomic_roundtrip_retains_trigger_detail_and_all_aggregates(self) -> None:
        quiet = telemetry.start_phase(
            "observation_due_scan",
            metadata={"workload": "observation", "row_count": 123},
            memory_reader=SequenceMemory((50 * MIB, 60 * MIB), (51 * MIB, 60 * MIB)),
            epoch_clock=SequenceClock(1.0, 2.0),
            monotonic_clock=SequenceClock(5.0, 5.25),
            include_gc_counts=True,
            include_object_count=True,
        )
        quiet_result = telemetry.finish_phase(quiet)
        loud = telemetry.start_phase(
            "hourly_rollup",
            metadata={
                "workload": "coverage",
                "operation": "rebuild",
                "hour_count": 72,
                "rollup_needed": True,
            },
            memory_reader=SequenceMemory(
                (190 * MIB, 195 * MIB), (205 * MIB, 206 * MIB)
            ),
            epoch_clock=SequenceClock(3.0, 4.0),
            monotonic_clock=SequenceClock(6.0, 6.5),
        )
        loud_result = telemetry.finish_phase(loud)

        self.assertFalse(quiet_result["persisted"])
        self.assertEqual(quiet_result["persistence_status"], "DEFERRED")
        self.assertFalse(quiet_result["detail_retained"])
        self.assertFalse(loud_result["persisted"])
        self.assertEqual(loud_result["persistence_status"], "DEFERRED")
        self.assertTrue(loud_result["detail_retained"])
        self.assertTrue(telemetry.flush_phase_memory_telemetry())
        document = state_store.read_json(self.path, {})
        self.assertEqual(document["total_phase_count"], 2)
        self.assertEqual(len(document["events"]), 1)
        event = document["events"][0]
        self.assertEqual(event["started_at_epoch"], 3.0)
        self.assertEqual(event["ended_at_epoch"], 4.0)
        self.assertEqual(event["elapsed_ms"], 500.0)
        self.assertEqual(event["rss_start_bytes"], 190 * MIB)
        self.assertEqual(event["rss_end_bytes"], 205 * MIB)
        self.assertEqual(event["rss_delta_bytes"], 15 * MIB)
        self.assertEqual(event["hwm_start_bytes"], 195 * MIB)
        self.assertEqual(event["hwm_end_bytes"], 206 * MIB)
        self.assertEqual(event["hwm_delta_bytes"], 11 * MIB)
        self.assertEqual(document["events"][0]["metadata"]["hour_count"], 72)
        self.assertEqual(document["phases"]["observation_due_scan"]["count"], 1)
        self.assertEqual(
            document["phases"]["observation_due_scan"]["metadata_maxima"]["row_count"],
            123,
        )
        self.assertEqual(document["phases"]["hourly_rollup"]["count"], 1)
        self.assertGreater(document["version"], 0)

    def test_scope_and_contextvar_metadata_are_normalized_and_accumulated(self) -> None:
        with telemetry.phase_memory(
            "candidate_fetch",
            metadata={"workload": "momentum", "response_count": 1},
            memory_reader=SequenceMemory(
                (100 * MIB, 100 * MIB), (125 * MIB, 125 * MIB)
            ),
        ) as scope:
            self.assertTrue(scope.add_metadata(candidate_count=4))
            self.assertTrue(telemetry.add_current_phase_metadata(
                response_count=2,
                response_bytes=4096,
                missing_length_count=1,
                content_length_known=True,
            ))

        self.assertTrue(telemetry.flush_phase_memory_telemetry())
        document = state_store.read_json(self.path, {})
        metadata = document["events"][0]["metadata"]
        self.assertIsInstance(document["events"][0]["process_id"], int)
        self.assertEqual(
            document["last_process_id"], document["events"][0]["process_id"]
        )
        self.assertNotIn("command", repr(document).lower())
        self.assertNotIn("environment", repr(document).lower())
        self.assertEqual(metadata["response_count"], 3)
        self.assertEqual(metadata["candidate_count"], 4)
        self.assertEqual(metadata["response_bytes"], 4096)
        self.assertEqual(metadata["missing_length_count"], 1)
        self.assertTrue(metadata["content_length_known"])
        self.assertFalse(telemetry.add_current_phase_metadata(response_count=1))

    def test_contextvar_is_inherited_by_async_child_tasks(self) -> None:
        async def scenario() -> None:
            with telemetry.phase_memory(
                "analyzer",
                memory_reader=SequenceMemory(
                    (100 * MIB, 100 * MIB), (125 * MIB, 125 * MIB)
                ),
            ):
                await asyncio.gather(*(
                    asyncio.create_task(child()) for _ in range(3)
                ))

        async def child() -> None:
            self.assertTrue(telemetry.add_current_phase_metadata(
                response_count=1,
                response_bytes=100,
            ))

        asyncio.run(scenario())
        self.assertTrue(telemetry.flush_phase_memory_telemetry())
        document = state_store.read_json(self.path, {})
        metadata = document["events"][0]["metadata"]
        self.assertEqual(metadata["response_count"], 3)
        self.assertEqual(metadata["response_bytes"], 300)

    def test_concurrent_tasks_keep_distinct_context_stacks(self) -> None:
        async def scenario() -> None:
            first_ready = asyncio.Event()
            second_ready = asyncio.Event()
            release = asyncio.Event()

            async def worker(name, ready) -> None:
                with telemetry.phase_memory(
                    name,
                    memory_reader=SequenceMemory(
                        (100 * MIB, 100 * MIB), (230 * MIB, 230 * MIB)
                    ),
                ):
                    ready.set()
                    await release.wait()

            first = asyncio.create_task(worker("analyzer", first_ready))
            second = asyncio.create_task(worker("candidate_fetch", second_ready))
            await first_ready.wait()
            await second_ready.wait()
            release.set()
            await asyncio.gather(first, second)

        asyncio.run(scenario())
        self.assertTrue(telemetry.flush_phase_memory_telemetry())
        document = state_store.read_json(self.path, {})
        events = {event["phase"]: event for event in document["events"]}
        self.assertEqual(events["analyzer"]["context_stack_at_start"], ["analyzer"])
        self.assertEqual(
            events["candidate_fetch"]["context_stack_at_start"],
            ["candidate_fetch"],
        )
        self.assertNotEqual(
            events["analyzer"]["context_id"],
            events["candidate_fetch"]["context_id"],
        )

    def test_sampler_retains_hwm_and_high_rss_with_active_context(self) -> None:
        with telemetry.phase_memory(
            "whale_transaction_fetch",
            metadata={
                "workload": "transaction",
                "payload_bytes": 4096,
                "retained_count": 1,
            },
            memory_reader=SequenceMemory(
                (100 * MIB, 100 * MIB), (100 * MIB, 100 * MIB)
            ),
        ):
            baseline = telemetry.record_memory_attribution_sample(
                memory_reader=SequenceMemory((100 * MIB, 100 * MIB)),
                epoch_clock=lambda: 1.0,
            )
            event = telemetry.record_memory_attribution_sample(
                memory_reader=SequenceMemory((235 * MIB, 106 * MIB)),
                epoch_clock=lambda: 2.0,
            )

        self.assertEqual(baseline["trigger_reasons"], [])
        self.assertEqual(event["trigger_reasons"], ["RSS_HIGH", "HWM_INCREASE"])
        self.assertEqual(
            event["active_contexts"][0]["stack"],
            ["whale_transaction_fetch"],
        )
        self.assertEqual(
            event["active_contexts"][0]["metadata"]["payload_bytes"], 4096
        )
        self.assertIsInstance(event["active_contexts"][0]["span_id"], int)
        self.assertTrue(telemetry.flush_phase_memory_telemetry())
        document = state_store.read_json(self.path, {})
        self.assertEqual(document["sampler_sample_count"], 2)
        self.assertEqual(len(document["sampler_events"]), 1)
        serialized = repr(document["sampler_events"]).lower()
        self.assertNotIn("signature", serialized)
        self.assertNotIn("mint", serialized)
        self.assertNotIn("raw", serialized)

    def test_sampler_ring_is_bounded_and_measurement_failure_is_open(self) -> None:
        telemetry.record_memory_attribution_sample(
            memory_reader=SequenceMemory((100 * MIB, 100 * MIB))
        )
        for index in range(telemetry.SAMPLER_EVENT_LIMIT + 5):
            telemetry.record_memory_attribution_sample(
                memory_reader=SequenceMemory((231 * MIB, 100 * MIB)),
                epoch_clock=lambda index=index: float(index + 1),
            )

        def broken_reader():
            raise OSError("proc unavailable")

        result = telemetry.record_memory_attribution_sample(
            memory_reader=broken_reader
        )
        self.assertEqual(result["trigger_reasons"], [])
        self.assertTrue(telemetry.flush_phase_memory_telemetry())
        document = state_store.read_json(self.path, {})
        self.assertEqual(len(document["sampler_events"]), telemetry.SAMPLER_EVENT_LIMIT)
        self.assertEqual(document["sampler_event_evicted_count"], 5)
        self.assertEqual(document["sampler_measurement_failure_count"], 1)

    def test_active_context_registry_is_bounded(self) -> None:
        tokens = [
            telemetry.start_phase(
                "analyzer",
                memory_reader=SequenceMemory(
                    (100 * MIB, 100 * MIB), (100 * MIB, 100 * MIB)
                ),
            )
            for _ in range(telemetry.ACTIVE_CONTEXT_REGISTRY_LIMIT + 1)
        ]
        self.assertEqual(
            len(telemetry._active_contexts),
            telemetry.ACTIVE_CONTEXT_REGISTRY_LIMIT,
        )
        self.assertEqual(telemetry._active_context_overflow_count, 1)
        overflow_finished = tokens.pop()
        telemetry.finish_phase(overflow_finished)
        while len(tokens) > telemetry.SAMPLER_ACTIVE_CONTEXT_LIMIT:
            telemetry.finish_phase(tokens.pop())
        sample = telemetry.record_memory_attribution_sample(
            memory_reader=SequenceMemory((100 * MIB, 100 * MIB))
        )
        self.assertFalse(sample["active_contexts_truncated"])
        self.assertEqual(sample["active_context_overflow_count"], 1)
        for token in tokens:
            telemetry.finish_phase(token)
        self.assertEqual(telemetry._active_phase_counts, {})
        self.assertEqual(telemetry._active_contexts, {})

    def test_sampler_start_is_singleton_and_interval_is_bounded(self) -> None:
        with mock.patch.object(telemetry.threading, "Thread") as thread_type:
            thread = thread_type.return_value
            thread.is_alive.return_value = False
            first = telemetry.start_memory_attribution_sampler(interval_seconds=2)
            thread.is_alive.return_value = True
            second = telemetry.start_memory_attribution_sampler(interval_seconds=2)

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        thread_type.assert_called_once()
        thread.start.assert_called_once_with()
        self.assertIs(first.thread, second.thread)
        self.assertIsNone(
            telemetry.start_memory_attribution_sampler(interval_seconds=0.5)
        )
        self.assertIsNone(
            telemetry.start_memory_attribution_sampler(interval_seconds=6)
        )

    def test_sampler_concurrent_start_creates_only_one_thread(self) -> None:
        real_thread_type = threading.Thread
        entered_start = threading.Event()
        release_start = threading.Event()
        created: list[object] = []

        class BlockingThread:
            def __init__(self, **_options) -> None:
                self.alive = False
                created.append(self)

            def start(self) -> None:
                entered_start.set()
                release_start.wait(timeout=2.0)
                self.alive = True

            def is_alive(self) -> bool:
                return self.alive

            def join(self, timeout=None) -> None:
                del timeout
                self.alive = False

        handles: list[telemetry.MemoryAttributionSampler | None] = []

        def start_sampler() -> None:
            handles.append(telemetry.start_memory_attribution_sampler())

        with mock.patch.object(telemetry.threading, "Thread", BlockingThread):
            first_caller = real_thread_type(target=start_sampler)
            first_caller.start()
            self.assertTrue(entered_start.wait(timeout=1.0))
            second_caller = real_thread_type(target=start_sampler)
            second_caller.start()
            release_start.set()
            first_caller.join(timeout=2.0)
            second_caller.join(timeout=2.0)

        self.assertEqual(len(created), 1)
        self.assertEqual(len(handles), 2)
        self.assertIsNotNone(handles[0])
        self.assertIsNotNone(handles[1])
        self.assertIs(handles[0].thread, handles[1].thread)

    def test_sampler_loop_continues_after_unexpected_sample_failure(self) -> None:
        stop_event = mock.Mock()
        stop_event.wait.side_effect = [False, True]
        with mock.patch.object(
            telemetry,
            "record_memory_attribution_sample",
            side_effect=[RuntimeError("telemetry"), None],
        ) as sample:
            telemetry._memory_attribution_sampler_loop(stop_event, 2.0)

        self.assertEqual(sample.call_count, 2)

    def test_all_canonical_phases_are_wired_into_runtime_boundaries(self) -> None:
        source_paths = (
            PROJECT_ROOT / "src" / "monitor.py",
            PROJECT_ROOT / "src" / "analyzer.py",
            PROJECT_ROOT / "src" / "observation_tracker.py",
            PROJECT_ROOT / "src" / "research_archive.py",
            PROJECT_ROOT / "src" / "research" / "coverage_telemetry.py",
            PROJECT_ROOT / "src" / "solana_rpc.py",
            PROJECT_ROOT / "src" / "wallet_performance.py",
        )
        source = "\n".join(
            path.read_text(encoding="utf-8") for path in source_paths
        )
        for phase in telemetry.ALLOWED_PHASES:
            with self.subTest(phase=phase):
                self.assertRegex(
                    source,
                    rf"phase_memory\(\s*[\"']{re.escape(phase)}[\"']",
                )

    def test_payload_size_proxy_does_not_read_or_serialize_response_body(self) -> None:
        source_paths = (
            PROJECT_ROOT / "src" / "monitor.py",
            PROJECT_ROOT / "src" / "analyzer.py",
            PROJECT_ROOT / "src" / "solana_rpc.py",
            PROJECT_ROOT / "src" / "executor.py",
        )
        source = "\n".join(
            path.read_text(encoding="utf-8") for path in source_paths
        )
        self.assertGreaterEqual(source.count('getattr(response, "content_length"'), 4)
        self.assertNotIn("await response.read()", source)

    def test_heartbeat_flushes_phase_aggregates_after_coverage(self) -> None:
        source = (PROJECT_ROOT / "src" / "monitor.py").read_text(encoding="utf-8")
        heartbeat_start = source.index("async def monitor_heartbeat")
        heartbeat_end = source.index("\nasync def ", heartbeat_start + 1)
        heartbeat = source[heartbeat_start:heartbeat_end]
        coverage_flush = heartbeat.index("flush_coverage_telemetry")
        phase_flush = heartbeat.index("flush_phase_memory_telemetry")
        next_sleep = heartbeat.index("await asyncio.sleep", phase_flush)
        self.assertLess(coverage_flush, phase_flush)
        self.assertLess(phase_flush, next_sleep)


if __name__ == "__main__":
    unittest.main()
