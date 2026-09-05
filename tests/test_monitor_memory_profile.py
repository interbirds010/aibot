from __future__ import annotations

import unittest

from src.research.monitor_memory_profile import summarize_memory_samples


class MonitorMemoryProfileTests(unittest.TestCase):
    def test_summary_reports_plateau_inputs_and_restart_delta(self) -> None:
        mib = 1024 * 1024
        samples = [
            {
                "sampled_at_epoch": 100.0,
                "pid": 10,
                "restart_count": 2,
                "rss_bytes": 100 * mib,
                "rss_ceiling_bytes": 260 * mib,
                "monitor_asyncio_live_task_count": 8,
                "monitor_signal_task_count": 1,
                "monitor_memory_vms_bytes": 500 * mib,
                "monitor_memory_system_available_bytes": 300 * mib,
                "wallet_ws_notification_process_count": 4,
            },
            {
                "sampled_at_epoch": 160.0,
                "pid": 11,
                "restart_count": 3,
                "rss_bytes": 80 * mib,
                "rss_ceiling_bytes": 260 * mib,
                "monitor_asyncio_live_task_count": 9,
                "monitor_signal_task_count": 2,
                "monitor_memory_vms_bytes": 480 * mib,
                "monitor_memory_system_available_bytes": 280 * mib,
                "wallet_ws_notification_process_count": 1,
            },
            {
                "sampled_at_epoch": 220.0,
                "pid": 11,
                "restart_count": 3,
                "rss_bytes": 90 * mib,
                "rss_ceiling_bytes": 260 * mib,
                "monitor_asyncio_live_task_count": 8,
                "monitor_signal_task_count": 0,
                "monitor_memory_vms_bytes": 485 * mib,
                "monitor_memory_system_available_bytes": 290 * mib,
                "wallet_ws_notification_process_count": 3,
            },
        ]

        report = summarize_memory_samples(samples, deployed_sha="a" * 40)

        self.assertEqual(report["restart_delta"], 1)
        self.assertEqual(report["pid_count"], 2)
        self.assertEqual(report["rss_bytes"]["start"], 100 * mib)
        self.assertEqual(report["rss_bytes"]["max"], 100 * mib)
        self.assertEqual(report["rss_bytes"]["end"], 90 * mib)
        self.assertEqual(
            report["rss_bytes"]["final_pid_slope_mib_per_min"], 10.0
        )
        self.assertEqual(
            report["series"]["signal_task_count"],
            {"start": 1, "max": 2, "end": 0},
        )
        self.assertEqual(
            report["counter_deltas"]["wallet_ws_notification_process_count"],
            3,
        )

    def test_invalid_sha_is_not_echoed(self) -> None:
        report = summarize_memory_samples(
            [], deployed_sha="https://secret.invalid/key"
        )
        self.assertEqual(report["deployed_sha"], "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
