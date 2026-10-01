from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src import failure_memory_diagnostics as diagnostics, runtime_memory


class FailureMemoryDiagnosticsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "snapshots.json"
        self.recorder = diagnostics._Recorder(self.path)
        self.replace = mock.patch.object(diagnostics, "_recorder", self.recorder)
        self.replace.start()
        self.addCleanup(self.replace.stop)
        self.memory = mock.patch.object(runtime_memory, "process_memory_snapshot", return_value={"rss_bytes": 250 * 1024 * 1024, "hwm_bytes": 280 * 1024 * 1024})
        self.memory_mock = self.memory.start()
        self.addCleanup(self.memory.stop)

    def test_threshold_crossings_repeat_and_baseline(self) -> None:
        self.recorder.capture()
        first = self.recorder.pending[-1]
        self.assertEqual(first["kind"], "threshold_crossing")
        self.assertEqual(first["thresholds_exceeded_bytes"], list(diagnostics.THRESHOLDS))
        self.assertEqual(first["vmhwm_bytes"], 280 * 1024 * 1024)
        self.recorder.capture()
        self.assertEqual(len(self.recorder.pending), 1)
        self.recorder.last_high -= 11
        self.recorder.capture()
        self.assertEqual(self.recorder.pending[-1]["kind"], "high_rss_repeat")
        self.memory_mock.return_value["rss_bytes"] = 100 * 1024 * 1024
        self.recorder.capture()
        self.assertEqual(self.recorder.pending[-1]["kind"], "baseline")
        self.memory_mock.return_value["rss_bytes"] = 240 * 1024 * 1024
        self.recorder.capture()
        self.assertEqual(self.recorder.pending[-1]["kind"], "threshold_crossing")

    def test_unknown_rss_is_explicitly_unavailable(self) -> None:
        self.memory_mock.return_value = {"rss_bytes": None, "hwm_bytes": None}
        self.recorder.capture()
        event = self.recorder.pending[-1]
        self.assertIsNone(event["rss_bytes"])
        self.assertFalse(event["rss_measurement_available"])
        self.assertEqual(event["thresholds_exceeded_bytes"], [])
        self.assertFalse(event["trim"]["trim_eligible"])

    def test_phase_overlap_lifecycle_cleanup_and_no_payload_references(self) -> None:
        with diagnostics.diagnostic_phase("candidate_fetch", candidate_count=4, raw_payload={"secret": "payload"}) as candidate:
            candidate.mark("raw_materialized", raw_payload_live_count=1)
            with diagnostics.diagnostic_phase("smart_get_transaction", transaction_count=1):
                diagnostics.mark_current_phase("transaction_materialized", raw_transaction_count=1)
                event = self.recorder.pending[-1]
                self.assertEqual(event["active_counts"]["candidate_fetch"], 1)
                self.assertEqual(event["active_counts"]["smart_get_transaction"], 1)
            candidate.mark("compact_projected", compact_candidate_count=4)
            candidate.mark("raw_released", raw_payload_live_count=0)
        self.assertEqual(self.recorder.active, {})
        self.assertNotIn("secret", repr(list(self.recorder.pending)))
        self.assertEqual(self.recorder.pending[-1]["active_phases"][0]["stage"], "exit")

    def test_recent_lifecycle_updates_have_priority_with_bounded_phase_counts(self) -> None:
        initial = {f"original_{index}_count": 1 for index in range(8)}
        with diagnostics.diagnostic_phase("candidate_fetch", **initial) as phase:
            phase.mark("raw_materialized", **{f"recent_{index}_count": 1 for index in range(8)})
            phase.mark("compact_projected", original_0_count=2)
            phase.mark("raw_released", recent_1_count=0)
            counts = self.recorder.pending[-1]["active_phases"][0]["counts"]
            self.assertEqual(counts["original_0_count"], 2)
            self.assertEqual(counts["recent_1_count"], 0)
            self.assertEqual(len(counts), diagnostics.MAX_PHASE_SCALARS)

    def test_exception_and_cancellation_cleanup(self) -> None:
        with self.assertRaises(ValueError):
            with diagnostics.diagnostic_phase("whale_confirmation"):
                raise ValueError("trading exception unchanged")
        self.assertEqual(self.recorder.active, {})

        async def scenario() -> None:
            ready = asyncio.Event()

            async def run() -> None:
                with diagnostics.diagnostic_phase("smart_get_transaction"):
                    ready.set()
                    await asyncio.sleep(3600)

            task = asyncio.create_task(run())
            await ready.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        asyncio.run(scenario())
        self.assertEqual(self.recorder.active, {})

    def test_release_boundary_survives_drop_below_threshold(self) -> None:
        with diagnostics.diagnostic_phase("candidate_fetch") as phase:
            self.memory_mock.return_value["rss_bytes"] = 100 * 1024 * 1024
            phase.mark("raw_released", raw_payload_live_count=0)
        self.assertTrue(any(event["active_phases"][0]["stage"] == "raw_released" for event in self.recorder.pending))

    def test_critical_snapshot_survives_lower_rss_queue_overflow(self) -> None:
        self.recorder.capture()
        self.memory_mock.return_value["rss_bytes"] = 230 * 1024 * 1024
        for _ in range(100):
            self.recorder.capture("trim_success")
        self.assertTrue(all(event["rss_bytes"] < diagnostics.THRESHOLDS[-1] for event in self.recorder.pending))
        self.recorder.flush()
        document = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertEqual(document["processes"][0]["latest_critical_snapshot"]["rss_bytes"], 250 * 1024 * 1024)

    def test_new_hwm_preserves_gil_bound_peak_after_current_rss_falls(self) -> None:
        self.memory_mock.return_value = {"rss_bytes": 100 * 1024 * 1024, "hwm_bytes": 100 * 1024 * 1024}
        self.recorder.capture()
        with diagnostics.diagnostic_phase("shadow_ledger_write") as phase:
            self.memory_mock.return_value = {"rss_bytes": 150 * 1024 * 1024, "hwm_bytes": 280 * 1024 * 1024}
            phase.mark("ledger_loaded", shadow_trade_count=10)
        event = self.recorder.pending[-1]
        self.assertEqual(event["kind"], "hwm_peak")
        self.assertEqual(event["rss_bytes"], 150 * 1024 * 1024)
        self.assertEqual(event["hwm_delta_bytes"], 180 * 1024 * 1024)
        self.assertEqual(event["hwm_threshold_crossings_bytes"], list(diagnostics.THRESHOLDS))
        self.assertEqual(event["thresholds_exceeded_bytes"], [])
        self.assertFalse(event["hwm_first_observation"])
        self.assertEqual(event["active_phases"][0]["stage"], "ledger_loaded")
        for _ in range(100):
            self.recorder.capture("trim_success")
        self.recorder.flush()
        process = json.loads(self.path.read_text(encoding="utf-8"))["processes"][0]
        self.assertEqual(process["latest_hwm_snapshot"]["kind"], "hwm_peak")
        self.assertIsNone(process["latest_critical_snapshot"])

    def test_hwm_unchanged_is_not_a_repeated_current_rss_peak(self) -> None:
        self.memory_mock.return_value = {"rss_bytes": None, "hwm_bytes": 280 * 1024 * 1024}
        self.recorder.capture()
        self.assertEqual(self.recorder.pending[-1]["kind"], "hwm_peak")
        self.assertTrue(self.recorder.pending[-1]["hwm_first_observation"])
        self.assertIsNone(self.recorder.pending[-1]["hwm_delta_bytes"])
        self.recorder.capture()
        self.assertEqual(self.recorder.pending[-1]["kind"], "baseline")
        self.recorder.capture()
        self.assertEqual(len(self.recorder.pending), 2)
        self.memory_mock.return_value["hwm_bytes"] = 284 * 1024 * 1024
        self.recorder.capture()
        self.assertEqual(len(self.recorder.pending), 2)
        self.memory_mock.return_value["hwm_bytes"] = 285 * 1024 * 1024
        self.recorder.capture()
        self.assertEqual(self.recorder.pending[-1]["kind"], "hwm_peak")
        self.assertEqual(self.recorder.pending[-1]["hwm_delta_bytes"], 5 * 1024 * 1024)

    def test_active_and_pending_bounds(self) -> None:
        handles = [diagnostics.diagnostic_phase("candidate_fetch") for _ in range(30)]
        for handle in handles:
            handle.__enter__()
        self.assertEqual(len(self.recorder.active), diagnostics.MAX_ACTIVE_PHASES)
        self.assertEqual(self.recorder.overflow_active, 14)
        for index in range(100):
            self.recorder.capture("trim_success", {"attempt_count": index})
        self.assertEqual(len(self.recorder.pending), diagnostics.MAX_PENDING)
        self.assertGreater(self.recorder.dropped_events, 0)
        for handle in reversed(handles):
            handle.__exit__(None, None, None)
        self.assertEqual(self.recorder.active, {})
        self.assertEqual(self.recorder.overflow_active, 0)

    def test_process_and_event_retention_with_baseline_and_critical_slot(self) -> None:
        for generation in range(10):
            with mock.patch.object(runtime_memory, "_PROCESS_START_ID", f"test-{generation}"):
                for index in range(60):
                    self.recorder.capture("trim_success", {"attempt_count": index})
                self.recorder.flush()
                for index in range(12):
                    self.recorder.capture("baseline", {"sample_count": index})
                self.recorder.flush()
        document = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertEqual(len(document["processes"]), diagnostics.MAX_PROCESSES)
        for process in document["processes"]:
            self.assertLessEqual(len(process["events"]), diagnostics.MAX_EVENTS)
            self.assertLessEqual(len(process["baselines"]), diagnostics.MAX_BASELINES)
            self.assertEqual(process["baselines"][0]["event_counts"]["sample_count"], 0)
            self.assertEqual(process["latest_critical_snapshot"]["rss_bytes"], 250 * 1024 * 1024)
        self.assertLessEqual(self.path.stat().st_size, diagnostics.MAX_DOCUMENT_BYTES)

    def test_corrupt_and_failed_persistence_are_isolated_and_retry_bounded(self) -> None:
        self.path.write_text("{corrupt", encoding="utf-8")
        self.recorder.capture()
        self.recorder.flush()
        self.assertEqual(self.path.read_text(encoding="utf-8"), "{corrupt")
        self.assertEqual(self.recorder.persistence_failures, 1)
        self.assertEqual(len(self.recorder.pending), 1)
        with mock.patch("src.state_store.update_json", side_effect=OSError("disk full")):
            for _ in range(100):
                self.recorder.capture("trim_success")
                self.recorder.flush()
        self.assertLessEqual(len(self.recorder.pending), diagnostics.MAX_PENDING)

    def test_scalar_security_and_freshness(self) -> None:
        diagnostics.update_runtime_counts(signal_task_count=2, signature_window_size=3, api_key_count=123, url_count=1, payload_count=[1], giant_count=2**100, nan_count=float("nan"), token_count="secret-token")
        self.recorder.capture()
        event = self.recorder.pending[-1]
        self.assertEqual(event["runtime_counts"], {"signal_task_count": 2, "signature_window_size": 3})
        self.assertGreaterEqual(event["runtime_counts_age_seconds"], 0)
        self.assertNotIn("secret-token", repr(event))

    def test_diagnostic_failure_does_not_mask_original_exception(self) -> None:
        with mock.patch.object(self.recorder, "phase_mark", side_effect=OSError("proc unavailable")):
            with self.assertRaisesRegex(ValueError, "original"):
                with diagnostics.diagnostic_phase("candidate_fetch"):
                    raise ValueError("original")
        self.assertEqual(self.recorder.active, {})

    def test_enter_capture_failure_does_not_leak_active_registration(self) -> None:
        with mock.patch.object(self.recorder, "capture", side_effect=OSError("proc unavailable")):
            with diagnostics.diagnostic_phase("candidate_fetch"):
                self.assertEqual(len(self.recorder.active), 1)
        self.assertEqual(self.recorder.active, {})

    def test_nested_phase_selector_and_start_failure_are_safe(self) -> None:
        with diagnostics.diagnostic_phase("candidate_fetch"):
            diagnostics.mark_current_phase("serialize", phase_name="shadow_ledger_write")
            self.assertEqual(next(iter(self.recorder.active.values()))["stage"], "enter")
        with mock.patch.object(diagnostics, "_sampler", None), mock.patch.object(diagnostics.threading.Thread, "start", side_effect=RuntimeError("no threads")):
            self.assertIsNone(diagnostics.start_failure_sampler())

    def test_document_byte_bound_with_maximum_shape(self) -> None:
        keys = {"x" * 56 + f"{index:02d}_count": 10**15 for index in range(32)}
        diagnostics.update_runtime_counts(**keys)
        handles = [diagnostics.diagnostic_phase("candidate_fetch", **keys) for _ in range(16)]
        for handle in handles:
            handle.__enter__()
        for generation in range(10):
            with mock.patch.object(runtime_memory, "_PROCESS_START_ID", f"test-{generation}"):
                for _ in range(60):
                    self.recorder.capture("trim_success", keys)
                self.recorder.flush()
        self.assertLessEqual(self.path.stat().st_size, diagnostics.MAX_DOCUMENT_BYTES)
        document = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertEqual(len(document["processes"]), diagnostics.MAX_PROCESSES)
        self.assertTrue(all(process["latest_critical_snapshot"] for process in document["processes"]))
        for handle in reversed(handles):
            handle.__exit__(None, None, None)

    def test_sampler_persists_from_native_thread(self) -> None:
        with mock.patch.object(self.recorder, "flush", wraps=self.recorder.flush) as flush:
            sampler = diagnostics._Sampler()
            sampler.stop()
            sampler.thread.join(timeout=3)
        self.assertFalse(sampler.thread.is_alive())
        self.assertTrue(flush.called)
        self.assertTrue(self.path.exists())

    def _ordering_sample(self, timestamp: object, *, sequence: int | None = 1,
                         identity: str = "ordering-generation", rss_mib: int | None = 252,
                         hwm_mib: int | None = 280, hwm_peak: bool = False,
                         kind: str = "trim_success", elapsed: float = 1.0,
                         marker: int = 0) -> dict:
        event = {
            "timestamp": timestamp, "pid": 123,
            "process_start_id": identity, "kind": kind,
            "process_elapsed_seconds": elapsed,
            "rss_bytes": rss_mib * 1024 * 1024 if rss_mib is not None else None,
            "vmhwm_bytes": hwm_mib * 1024 * 1024 if hwm_mib is not None else None,
            "hwm_new_peak": hwm_peak, "event_counts": {"marker_count": marker},
        }
        if sequence is not None:
            event["sample_sequence"] = sequence
        return event

    def _ordering_process(self, identity: str = "ordering-generation") -> dict:
        document = json.loads(self.path.read_text(encoding="utf-8"))
        return next(process for process in document["processes"]
                    if process["process_start_id"] == identity)

    def test_protected_older_hwm_does_not_roll_back_newer_critical_after_overflow(self) -> None:
        older_hwm = self._ordering_sample(10.0, rss_mib=280, hwm_peak=True)
        newer_critical = self._ordering_sample(20.0, rss_mib=252)
        self.recorder.pending.extend([older_hwm, newer_critical])
        self.recorder.pending_hwm = older_hwm
        self.recorder.pending_critical = newer_critical
        for timestamp in range(21, 85):
            self.recorder.pending.append(self._ordering_sample(float(timestamp), rss_mib=230))
        self.recorder.flush()
        process = self._ordering_process()
        self.assertEqual(process["latest_critical_snapshot"]["timestamp"], 20.0)
        self.assertEqual(process["latest_critical_snapshot"]["rss_bytes"], 252 * 1024 * 1024)
        self.assertEqual(process["latest_hwm_snapshot"]["timestamp"], 10.0)
        self.assertEqual(process["latest_rss_snapshot"]["timestamp"], 84.0)
        self.assertEqual([event["timestamp"] for event in process["events"]], list(map(float, range(37, 85))))

    def test_mixed_out_of_order_events_and_baselines_use_time_retention(self) -> None:
        self.recorder.pending.extend(self._ordering_sample(float(value))
                                     for value in reversed(range(2, 101, 2)))
        self.recorder.flush()
        self.recorder.pending.extend(self._ordering_sample(float(value))
                                     for value in reversed(range(1, 100, 2)))
        self.recorder.pending.extend(self._ordering_sample(float(value), kind="baseline")
                                     for value in reversed(range(1, 15)))
        self.recorder.flush()
        process = self._ordering_process()
        self.assertEqual([event["timestamp"] for event in process["events"]], list(map(float, range(53, 101))))
        self.assertEqual([event["timestamp"] for event in process["baselines"]], [1.0] + list(map(float, range(8, 15))))
        self.assertEqual(process["latest_critical_snapshot"]["timestamp"], 100.0)
        self.assertEqual(process["latest_rss_snapshot"]["timestamp"], 100.0)

    def test_older_protected_hwm_does_not_roll_back_persisted_hwm(self) -> None:
        self.recorder.pending.append(self._ordering_sample(20.0, hwm_mib=290, hwm_peak=True))
        self.recorder.flush()
        self.recorder.pending.append(self._ordering_sample(30.0, rss_mib=230, hwm_mib=290))
        self.recorder.pending_hwm = self._ordering_sample(10.0, hwm_peak=True)
        self.recorder.flush()
        process = self._ordering_process()
        self.assertEqual(process["latest_hwm_snapshot"]["timestamp"], 20.0)
        self.assertEqual(process["latest_critical_snapshot"]["timestamp"], 20.0)
        self.assertEqual(process["latest_rss_snapshot"]["timestamp"], 30.0)

    def test_failed_first_writer_retry_preserves_newer_persisted_slots(self) -> None:
        older = self._ordering_sample(10.0, hwm_peak=True)
        self.recorder.pending.append(older)
        self.recorder.pending_hwm = older
        self.recorder.pending_critical = older
        with mock.patch("src.state_store.update_json", side_effect=OSError("first writer unavailable")):
            self.recorder.flush()
        self.assertEqual(self.recorder.persistence_failures, 1)
        second_writer = diagnostics._Recorder(self.path)
        second_writer.pending.append(self._ordering_sample(20.0, hwm_mib=290, hwm_peak=True))
        second_writer.flush()
        self.recorder.flush()
        process = self._ordering_process()
        for key in ("latest_critical_snapshot", "latest_hwm_snapshot", "latest_rss_snapshot"):
            self.assertEqual(process[key]["timestamp"], 20.0, key)
        self.assertEqual([event["timestamp"] for event in process["events"]], [10.0, 20.0])

    def test_generation_local_latest_and_generation_cap(self) -> None:
        for generation in range(10):
            identity = f"ordering-generation-{generation}"
            self.recorder.pending.append(self._ordering_sample(100.0 + generation, identity=identity, hwm_peak=True))
            self.recorder.flush()
            self.recorder.pending.append(self._ordering_sample(1.0, identity=identity, rss_mib=260, hwm_peak=True))
            self.recorder.flush()
        document = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertEqual(len(document["processes"]), 8)
        self.assertEqual({process["process_start_id"] for process in document["processes"]},
                         {f"ordering-generation-{value}" for value in range(2, 10)})
        for process in document["processes"]:
            generation = int(process["process_start_id"].rsplit("-", 1)[1])
            for key in ("latest_critical_snapshot", "latest_hwm_snapshot", "latest_rss_snapshot"):
                self.assertEqual(process[key]["timestamp"], 100.0 + generation)
            self.assertLessEqual(len(process["events"]), 48)
            self.assertLessEqual(len(process["baselines"]), 8)
        self.assertLessEqual(self.path.stat().st_size, 1024 * 1024)

    def test_generation_retention_uses_latest_time_not_incoming_order(self) -> None:
        for generation in reversed(range(10)):
            self.recorder.pending.append(self._ordering_sample(100.0 + generation,
                                         identity=f"ordering-generation-{generation}"))
            self.recorder.flush()
        # 이미 탈락한 과거 generation을 재전송해도 최근 generation을 밀어내지 않는다.
        self.recorder.pending.append(self._ordering_sample(1.0, identity="ordering-generation-0"))
        self.recorder.flush()
        processes = json.loads(self.path.read_text(encoding="utf-8"))["processes"]
        self.assertEqual([process["process_start_id"] for process in processes],
                         [f"ordering-generation-{value}" for value in range(2, 10)])

    def test_latest_slots_use_only_eligible_rss_and_hwm_samples(self) -> None:
        critical = self._ordering_sample(10.0, hwm_peak=True)
        hwm_only = self._ordering_sample(20.0, rss_mib=None, hwm_mib=290, hwm_peak=True)
        measured_rss = self._ordering_sample(30.0, rss_mib=230, hwm_mib=300, hwm_peak=False)
        unknown_hwm = self._ordering_sample(40.0, rss_mib=None, hwm_mib=None, hwm_peak=True)
        self.recorder.pending.extend([critical, hwm_only, measured_rss, unknown_hwm])
        self.recorder.flush()
        process = self._ordering_process()
        self.assertEqual(process["latest_critical_snapshot"]["timestamp"], 10.0)
        self.assertEqual(process["latest_hwm_snapshot"]["timestamp"], 20.0)
        self.assertEqual(process["latest_rss_snapshot"]["timestamp"], 30.0)

    def test_equal_timestamp_sequence_then_elapsed_then_legacy_fallback_is_deterministic(self) -> None:
        samples = [
            self._ordering_sample(10.0, sequence=2, elapsed=1.0, marker=2),
            self._ordering_sample(10.0, sequence=1, elapsed=100.0, marker=1),
            self._ordering_sample(10.0, sequence=2, elapsed=3.0, marker=3),
        ]
        self.recorder.pending.extend(samples)
        self.recorder.flush()
        process = self._ordering_process()
        self.assertEqual(process["latest_critical_snapshot"]["event_counts"]["marker_count"], 3)
        self.assertEqual([event["event_counts"]["marker_count"] for event in process["events"]], [1, 2, 3])
        legacy = [self._ordering_sample(20.0, sequence=None, elapsed=1.0, marker=value)
                  for value in (4, 5)]
        first = diagnostics._Recorder(Path(self.temporary.name) / "legacy-first.json")
        second = diagnostics._Recorder(Path(self.temporary.name) / "legacy-second.json")
        first.pending.extend(legacy)
        second.pending.extend(reversed(legacy))
        first.flush()
        second.flush()
        expected = max(legacy, key=lambda event: json.dumps(event, sort_keys=True, separators=(",", ":")))
        left = json.loads(first.path.read_text(encoding="utf-8"))["processes"][0]
        right = json.loads(second.path.read_text(encoding="utf-8"))["processes"][0]
        self.assertEqual(left["latest_critical_snapshot"], expected)
        self.assertEqual(right["latest_critical_snapshot"], expected)
        self.assertEqual(left["events"], right["events"])

    def test_invalid_timestamp_skips_only_bad_sample_and_flush_succeeds(self) -> None:
        invalid_values = (None, float("nan"), float("inf"), float("-inf"), -1, True, "10", [], {})
        bad = [self._ordering_sample(value, marker=99, hwm_peak=True) for value in invalid_values]
        missing = self._ordering_sample(1.0, marker=99)
        del missing["timestamp"]
        self.recorder.pending.extend([self._ordering_sample(10.0), *bad, missing,
                                     self._ordering_sample(20.0)])
        self.recorder.flush()
        process = self._ordering_process()
        self.assertEqual([event["timestamp"] for event in process["events"]], [10.0, 20.0])
        self.assertEqual(process["latest_critical_snapshot"]["timestamp"], 20.0)
        self.assertEqual(process["latest_rss_snapshot"]["timestamp"], 20.0)
        self.assertIsNone(process["latest_hwm_snapshot"])
        self.assertEqual(self.recorder.persistence_failures, 0)
        self.assertEqual(len(self.recorder.pending), 0)

    def test_capture_sample_sequence_increases_even_with_equal_timestamp(self) -> None:
        with mock.patch.object(diagnostics.time, "time", return_value=10.0):
            self.recorder.capture("trim_success")
            self.recorder.capture("trim_success")
        first, second = list(self.recorder.pending)[-2:]
        self.assertIsInstance(first["sample_sequence"], int)
        self.assertLess(first["sample_sequence"], second["sample_sequence"])


if __name__ == "__main__":
    unittest.main()
