from __future__ import annotations

import asyncio
import unittest
from unittest import mock

from src import monitor, runtime_memory


class RuntimeMemoryTests(unittest.TestCase):
    def test_proc_memory_values_are_normalized_to_bytes(self) -> None:
        result = runtime_memory.process_memory_snapshot(
            status_text="VmRSS: 123 kB\nVmSize: 456 kB\nThreads: 7\n",
            meminfo_text="MemAvailable: 789 kB\n",
        )
        self.assertEqual(result["rss_bytes"], 123 * 1024)
        self.assertEqual(result["vms_bytes"], 456 * 1024)
        self.assertEqual(result["thread_count"], 7)
        self.assertEqual(result["system_available_memory_bytes"], 789 * 1024)

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
