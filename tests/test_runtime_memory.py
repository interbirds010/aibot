from __future__ import annotations

import asyncio
import inspect
import unittest
from unittest import mock

from src import monitor, observation_tracker, runtime_memory


class RuntimeMemoryTests(unittest.TestCase):
    def setUp(self) -> None:
        runtime_memory._malloc_trim_function = (
            runtime_memory._MALLOC_TRIM_UNINITIALIZED
        )
        runtime_memory._allocator_libc_handle = None
        runtime_memory._allocator_trim_last_attempt_monotonic = None
        runtime_memory._allocator_trim_in_progress = False
        runtime_memory._allocator_trim_stats.update({
            "attempt_count": 0,
            "success_count": 0,
            "failure_count": 0,
            "last_at_epoch_seconds": None,
            "last_rss_before_bytes": None,
            "last_rss_after_bytes": None,
            "last_latency_ms": None,
        })

    def test_full_analysis_is_not_in_observer_hot_path(self) -> None:
        source = inspect.getsource(observation_tracker.observation_loop)
        self.assertNotIn("refresh_observation_analysis", source)

    def test_proc_memory_values_are_normalized_to_bytes(self) -> None:
        result = runtime_memory.process_memory_snapshot(
            status_text="VmRSS: 123 kB\nVmSize: 456 kB\nThreads: 7\n",
            meminfo_text="MemAvailable: 789 kB\n",
        )
        self.assertEqual(result["rss_bytes"], 123 * 1024)
        self.assertEqual(result["vms_bytes"], 456 * 1024)
        self.assertEqual(result["thread_count"], 7)
        self.assertEqual(result["system_available_memory_bytes"], 789 * 1024)

    def test_hwm_and_cooldown_snapshot_do_not_wait_for_allocator_lock(self) -> None:
        memory = runtime_memory.process_memory_snapshot(status_text="VmHWM: 300000 kB\n", meminfo_text="")
        self.assertEqual(memory["hwm_bytes"], 300000 * 1024)
        runtime_memory._allocator_trim_last_attempt_monotonic = 100.0
        runtime_memory._allocator_trim_stats["last_at_epoch_seconds"] = 1000.0
        with mock.patch.object(runtime_memory.time, "monotonic", return_value=130.0), mock.patch.object(runtime_memory.sys, "platform", "linux"):
            with runtime_memory._allocator_trim_lock:
                snapshot = runtime_memory.trim_cooldown_snapshot(rss_bytes=251 * 1024 * 1024)
        self.assertEqual(snapshot["seconds_since_last_trim"], 30.0)
        self.assertEqual(snapshot["cooldown_remaining_seconds"], 30.0)
        self.assertFalse(snapshot["trim_eligible"])
        self.assertTrue(snapshot["rss_threshold_exceeded"])
        with mock.patch.object(runtime_memory.time, "monotonic", return_value=161.0), mock.patch.object(runtime_memory.sys, "platform", "linux"):
            self.assertTrue(runtime_memory.trim_cooldown_snapshot(rss_bytes=251 * 1024 * 1024)["trim_eligible"])
            runtime_memory._allocator_trim_in_progress = True
            self.assertFalse(runtime_memory.trim_cooldown_snapshot(rss_bytes=251 * 1024 * 1024)["trim_eligible"])

    def test_trim_events_include_release_phase_and_cooldown_skip_without_behavior_change(self) -> None:
        with (
            mock.patch.object(runtime_memory.sys, "platform", "linux"),
            mock.patch.object(runtime_memory, "current_rss_bytes", return_value=251 * 1024 * 1024),
            mock.patch.object(runtime_memory, "_load_malloc_trim", return_value=mock.Mock(return_value=1)),
            mock.patch("src.failure_memory_diagnostics.record_event") as record,
        ):
            self.assertTrue(runtime_memory.maybe_trim_allocator(phase="momentum_candidate_fetch", reason="raw_candidate_payload_released"))
            self.assertFalse(runtime_memory.maybe_trim_allocator(phase="momentum_candidate_fetch", reason="raw_candidate_payload_released"))
        self.assertEqual([call.args[0] for call in record.call_args_list], ["trim_attempt", "trim_success", "trim_skipped_cooldown"])
        self.assertEqual(record.call_args_list[-1].kwargs["phase_code"], 1)
        self.assertEqual(record.call_args_list[-1].kwargs["reason_code"], 1)
        self.assertFalse(runtime_memory._allocator_trim_in_progress)

    def test_payload_estimate_is_bounded_and_keeps_no_payload(self) -> None:
        size, truncated = runtime_memory.estimate_object_size_bytes(
            {"rows": [{"secret": "value"}] * 20}, maximum_nodes=3
        )
        self.assertGreater(size, 0)
        self.assertTrue(truncated)
        recorded = runtime_memory.record_transaction_payload(
            {"transaction": {"message": {"accountKeys": ["public"]}}}
        )
        self.assertGreater(recorded, 0)
        metrics = runtime_memory.runtime_memory_metrics(
            rss_ceiling_bytes=260 * 1024 * 1024
        )
        self.assertNotIn("public", repr(metrics))
        self.assertIn("monitor_transaction_payload_max_bytes", metrics)

    def test_allocator_trim_linux_glibc_available_and_rate_limited(self) -> None:
        malloc_trim = mock.Mock(return_value=1)
        clock = mock.Mock(
            side_effect=[100.0, 100.01, 100.02, 120.0, 161.0, 161.01, 161.02]
        )
        with (
            mock.patch.object(runtime_memory.sys, "platform", "linux"),
            mock.patch.object(
                runtime_memory, "current_rss_bytes", return_value=220 * 1024 * 1024
            ),
            mock.patch.object(
                runtime_memory, "_load_malloc_trim", return_value=malloc_trim
            ),
            mock.patch.object(runtime_memory.time, "monotonic", clock),
            mock.patch.object(runtime_memory.time, "time", return_value=1_000.0),
            mock.patch.object(runtime_memory.time, "perf_counter", clock),
        ):
            self.assertTrue(runtime_memory.maybe_trim_allocator())
            self.assertFalse(runtime_memory.maybe_trim_allocator())
            self.assertTrue(runtime_memory.maybe_trim_allocator())
        self.assertEqual(malloc_trim.call_count, 2)

    def test_allocator_trim_skips_below_threshold_and_unknown_rss(self) -> None:
        with (
            mock.patch.object(runtime_memory.sys, "platform", "linux"),
            mock.patch.object(
                runtime_memory,
                "current_rss_bytes",
                side_effect=[None, 199 * 1024 * 1024],
            ),
            mock.patch.object(runtime_memory, "_load_malloc_trim") as load,
        ):
            self.assertFalse(runtime_memory.maybe_trim_allocator())
            self.assertFalse(runtime_memory.maybe_trim_allocator())
        load.assert_not_called()

    def test_allocator_trim_is_noop_on_unsupported_platforms(self) -> None:
        for platform_name in ("win32", "darwin"):
            with (
                self.subTest(platform=platform_name),
                mock.patch.object(runtime_memory.sys, "platform", platform_name),
                mock.patch.object(runtime_memory, "current_rss_bytes") as rss,
            ):
                self.assertFalse(runtime_memory.maybe_trim_allocator())
                rss.assert_not_called()

    def test_allocator_trim_load_and_symbol_failures_are_non_fatal(self) -> None:
        with mock.patch.object(
            runtime_memory.ctypes, "CDLL", side_effect=OSError("load failed")
        ):
            self.assertIsNone(runtime_memory._load_malloc_trim())

        class VersionSymbol:
            argtypes = None
            restype = None

            def __call__(self):
                return b"2.39"

        class GlibcWithoutTrim:
            gnu_get_libc_version = VersionSymbol()

        runtime_memory._malloc_trim_function = (
            runtime_memory._MALLOC_TRIM_UNINITIALIZED
        )
        with mock.patch.object(
            runtime_memory.ctypes, "CDLL", return_value=GlibcWithoutTrim()
        ):
            self.assertIsNone(runtime_memory._load_malloc_trim())

    def test_allocator_trim_proc_read_failure_is_non_fatal(self) -> None:
        with (
            mock.patch.object(runtime_memory.sys, "platform", "linux"),
            mock.patch.object(
                runtime_memory,
                "current_rss_bytes",
                side_effect=OSError("proc unavailable"),
            ),
        ):
            self.assertFalse(runtime_memory.maybe_trim_allocator())

    def test_allocator_trim_call_exception_and_failure_are_non_fatal(self) -> None:
        for outcome in (RuntimeError("trim failed"), 0):
            trim = mock.Mock(side_effect=outcome) if isinstance(
                outcome, Exception
            ) else mock.Mock(return_value=outcome)
            with (
                self.subTest(outcome=repr(outcome)),
                mock.patch.object(runtime_memory.sys, "platform", "linux"),
                mock.patch.object(
                    runtime_memory,
                    "current_rss_bytes",
                    return_value=220 * 1024 * 1024,
                ),
                mock.patch.object(
                    runtime_memory, "_load_malloc_trim", return_value=trim
                ),
            ):
                runtime_memory._allocator_trim_last_attempt_monotonic = None
                self.assertFalse(runtime_memory.maybe_trim_allocator())

    def test_allocator_trim_signature_is_explicit(self) -> None:
        class Symbol:
            argtypes = None
            restype = None

            def __init__(self, result):
                self.result = result

            def __call__(self):
                return self.result

        class Libc:
            gnu_get_libc_version = Symbol(b"2.39")
            malloc_trim = Symbol(1)

        libc = Libc()
        with mock.patch.object(runtime_memory.ctypes, "CDLL", return_value=libc):
            resolved = runtime_memory._load_malloc_trim()
            self.assertIs(runtime_memory._load_malloc_trim(), resolved)
        self.assertIs(resolved, libc.malloc_trim)
        self.assertEqual(libc.gnu_get_libc_version.argtypes, [])
        self.assertIs(
            libc.gnu_get_libc_version.restype, runtime_memory.ctypes.c_char_p
        )
        self.assertEqual(libc.malloc_trim.argtypes, [runtime_memory.ctypes.c_size_t])
        self.assertIs(libc.malloc_trim.restype, runtime_memory.ctypes.c_int)

    @unittest.skipUnless(
        runtime_memory.sys.platform.startswith("linux"), "requires Linux glibc"
    )
    def test_real_linux_glibc_malloc_trim_is_available(self) -> None:
        self.assertIsNotNone(runtime_memory._load_malloc_trim())
        result = runtime_memory.maybe_trim_allocator(
            rss_threshold_bytes=0, minimum_interval_seconds=0
        )
        self.assertIsInstance(result, bool)
        self.assertEqual(runtime_memory._allocator_trim_stats["attempt_count"], 1)

    def test_trim_events_are_persisted_to_logs_with_phase_and_reason(self) -> None:
        malloc_trim = mock.Mock(return_value=1)
        with (
            mock.patch.object(runtime_memory.sys, "platform", "linux"),
            mock.patch.object(
                runtime_memory,
                "current_rss_bytes",
                side_effect=[220 * 1024 * 1024, 180 * 1024 * 1024],
            ),
            mock.patch.object(
                runtime_memory, "_load_malloc_trim", return_value=malloc_trim
            ),
            mock.patch.object(runtime_memory.logger, "info") as info,
        ):
            self.assertTrue(
                runtime_memory.maybe_trim_allocator(
                    minimum_interval_seconds=0,
                    phase="momentum_whale_confirmation",
                    reason="raw_confirmation_payload_released",
                )
            )
        rendered = " ".join(
            str(part)
            for call in info.call_args_list
            for part in call.args
        )
        self.assertIn("memory_trim_attempt", rendered)
        self.assertIn("memory_trim_success", rendered)
        self.assertIn("momentum_whale_confirmation", rendered)
        self.assertIn("raw_confirmation_payload_released", rendered)

    def test_candidate_fetch_trims_only_after_raw_payload_scope_ends(self) -> None:
        source = inspect.getsource(monitor.fetch_momentum_candidate_cohorts)
        fetch_index = source.index("_fetch_momentum_candidate_cohorts")
        trim_index = source.index("maybe_trim_allocator")
        return_index = source.rindex("return approved, shadows")
        self.assertLess(fetch_index, trim_index)
        self.assertLess(trim_index, return_index)
        self.assertIn("raw_candidate_payload_released", source)

    def test_whale_confirmation_trims_after_compact_projection(self) -> None:
        wrapper = inspect.getsource(monitor._confirm_unknown_whales_with_telemetry)
        self.assertIn("_confirm_unknown_whales_with_memory_telemetry", wrapper)
        source = inspect.getsource(monitor._confirm_unknown_whales_with_memory_telemetry)
        confirmation_index = source.index(
            "_confirm_unknown_whales_with_funnel_telemetry"
        )
        trim_index = source.index("maybe_trim_allocator")
        return_index = source.rindex("return whales")
        self.assertLess(confirmation_index, trim_index)
        self.assertLess(trim_index, return_index)
        self.assertIn("raw_confirmation_payload_released", source)

    def test_standard_get_transaction_drops_final_raw_reference_before_trim(self) -> None:
        source = inspect.getsource(monitor.monitor_standard_once)
        release_index = source.index("transaction = None")
        trim_index = source.index(
            'phase="smart_get_transaction"',
            release_index,
        )
        self.assertLess(release_index, trim_index)
        self.assertIn("restored_transaction_consumed", source)

    def test_completed_tracked_task_is_removed_from_both_registries(self) -> None:
        async def scenario() -> None:
            with (
                mock.patch.object(monitor, "_signal_tasks", set()),
                mock.patch.object(monitor, "_shadow_signal_tasks", set()),
                mock.patch.object(monitor, "_signal_task_created_count", 0),
                mock.patch.object(monitor, "_signal_task_completed_count", 0),
                mock.patch.object(
                    monitor, "_shadow_signal_task_created_count", 0
                ),
                mock.patch.object(
                    monitor, "_shadow_signal_task_completed_count", 0
                ),
            ):
                task = asyncio.create_task(asyncio.sleep(0))
                monitor.track_signal_task(task, shadow=True)
                self.assertIn(task, monitor._signal_tasks)
                self.assertIn(task, monitor._shadow_signal_tasks)
                await task
                await asyncio.sleep(0)
                self.assertNotIn(task, monitor._signal_tasks)
                self.assertNotIn(task, monitor._shadow_signal_tasks)
                self.assertEqual(monitor._signal_task_created_count, 1)
                self.assertEqual(monitor._signal_task_completed_count, 1)

        asyncio.run(scenario())

    def test_monitor_metrics_report_only_collection_sizes(self) -> None:
        async def scenario() -> None:
            with (
                mock.patch.object(
                    monitor,
                    "runtime_memory_metrics",
                    return_value={"monitor_memory_rss_bytes": 100},
                ),
                mock.patch.object(monitor, "_signal_tasks", set()),
                mock.patch.object(monitor, "_shadow_signal_tasks", set()),
                mock.patch.object(
                    monitor, "_whale_buy_history", {("wallet", "mint"): [1, 2]}
                ),
                mock.patch.object(
                    monitor, "_market_entry_cooldowns", {"mint": 1.0}
                ),
                mock.patch.object(monitor, "_market_shadow_cooldowns", {}),
            ):
                metrics = monitor.monitor_runtime_metrics(20)
            self.assertEqual(metrics["monitor_wallet_count"], 20)
            self.assertEqual(metrics["monitor_whale_history_key_count"], 1)
            self.assertEqual(metrics["monitor_whale_history_entry_count"], 2)
            self.assertEqual(metrics["monitor_market_entry_cooldown_count"], 1)
            values = repr(list(metrics.values()))
            self.assertNotIn("wallet", values)
            self.assertNotIn("mint", values)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
