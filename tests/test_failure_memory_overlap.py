from __future__ import annotations

import threading
import unittest
from unittest import mock

from src import failure_memory_diagnostics as failure
from src import phase_memory_telemetry as phase
from src import runtime_memory


class FailureMemoryOverlapTests(unittest.TestCase):
    def test_snapshot_combines_thread_work_and_existing_async_phases(self):
        recorder = failure._Recorder()
        memory = {"rss_bytes": 251 * 1024 * 1024, "hwm_bytes": 270 * 1024 * 1024}
        ready, release = threading.Event(), threading.Event()

        def write_phase():
            with failure.diagnostic_phase("shadow_ledger_write") as handle:
                handle.mark("serialize", shadow_trade_count=10_000)
                ready.set()
                release.wait(5)

        with (mock.patch.object(failure, "_recorder", recorder),
              mock.patch.object(runtime_memory, "process_memory_snapshot", return_value=memory),
              mock.patch.object(phase, "_request_background_flush", return_value=False),
              mock.patch.object(phase, "_pending_batch", None)):
            with failure.diagnostic_phase("candidate_fetch"):
                with phase.phase_memory("coverage_telemetry_flush"):
                    thread = threading.Thread(target=write_phase)
                    thread.start()
                    try:
                        self.assertTrue(ready.wait(5))
                        recorder.capture()
                        snapshot = recorder.pending[-1]
                        self.assertEqual(snapshot["active_counts"]["candidate_fetch"], 1)
                        self.assertEqual(snapshot["active_counts"]["shadow_ledger_write"], 1)
                        self.assertEqual(snapshot["existing_active_phase_counts"]["coverage_telemetry_flush"], 1)
                        self.assertTrue(snapshot["existing_phase_counts_available"])
                        self.assertNotIn("metadata", repr(snapshot["existing_active_phase_counts"]))
                    finally:
                        release.set()
                        thread.join(5)
                    self.assertFalse(thread.is_alive())
            self.assertEqual(recorder.active, {})

    def test_existing_phase_lock_contention_is_reported_without_wait(self):
        ready, release = threading.Event(), threading.Event()

        def lock_owner():
            with phase._active_lock:
                ready.set()
                release.wait(5)

        thread = threading.Thread(target=lock_owner)
        thread.start()
        try:
            self.assertTrue(ready.wait(5))
            self.assertIsNone(phase.active_phase_counts_snapshot())
        finally:
            release.set()
            thread.join(5)
        self.assertFalse(thread.is_alive())
