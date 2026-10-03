"""후보 runtime의 종료 요청이 진행 중 Paper 작업을 취소하지 않는지 확인한다."""
import asyncio
from contextlib import nullcontext
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from src import monitor


class EntryTelemetryCleanStopTests(unittest.TestCase):
    def test_stop_closes_discovery_and_waits_for_existing_signal(self):
        async def scenario():
            discovered = asyncio.Event()
            finish = asyncio.Event()
            states = []
            async def signal():
                await finish.wait()
                states.append("paper-write-finished")
            existing = asyncio.create_task(signal())
            async def discovery():
                discovered.set()
                await asyncio.Event().wait()
            async def stop():
                await discovered.wait()
            with patch.object(monitor, "_wait_for_clean_stop", side_effect=stop), \
                    patch.object(monitor, "_signal_tasks", {existing}), \
                    patch.object(monitor, "_service_stopping", False):
                service = asyncio.create_task(monitor._run_service_tasks([discovery()]))
                for _ in range(10):
                    await asyncio.sleep(0)
                self.assertTrue(monitor._service_stopping)
                self.assertFalse(service.done())
                self.assertFalse(existing.cancelled())
                finish.set()
                await service
                self.assertEqual(states, ["paper-write-finished"])
        asyncio.run(scenario())

    def test_stop_state_prevents_new_entry_and_feeder_without_execution(self):
        with patch.object(monitor, "_service_stopping", True), \
                patch.object(monitor, "_process_paper_signal_control", new=AsyncMock()) as control, \
                patch.object(monitor.subprocess, "Popen") as process:
            asyncio.run(monitor.process_paper_signal("mint", 1, 6, 1, "wallet", "signature", "2026-10-03T00:00:00+00:00"))
            self.assertFalse(monitor.trigger_wallet_feeder_if_needed())
            control.assert_not_called()
            process.assert_not_called()

    def test_service_exception_is_not_hidden_by_stop_supervisor(self):
        async def scenario():
            async def failure():
                raise RuntimeError("existing service failure")
            async def stop():
                await asyncio.Event().wait()
            with patch.object(monitor, "_wait_for_clean_stop", side_effect=stop):
                with self.assertRaisesRegex(RuntimeError, "existing service failure"):
                    await monitor._run_service_tasks([failure()])
        asyncio.run(scenario())

    def test_already_registered_signal_runs_even_when_stop_precedes_first_task_tick(self):
        async def scenario():
            with patch.object(monitor, "_signal_tasks", set()), \
                    patch.object(monitor, "_service_stopping", True), \
                    patch.object(monitor, "_process_paper_signal_control", new=AsyncMock()) as control:
                queued = asyncio.create_task(monitor.process_paper_signal(
                    "mint", 1, 6, 1, "wallet", "signature", "2026-10-03T00:00:00+00:00",
                ))
                monitor.track_signal_task(queued)
                await queued
                control.assert_awaited_once()
                self.assertFalse(queued.cancelled())
        asyncio.run(scenario())

    def test_canceling_run_forever_cleans_its_four_connection_children_first(self):
        async def scenario(wallet_path):
            all_started = asyncio.Event()
            started, completed = [], []

            async def hold(*args):
                started.append(asyncio.current_task())
                if len(started) == 4:
                    all_started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    completed.append(asyncio.current_task())

            settings = monitor.MonitorSettings("wss://invalid", "https://invalid", wallets_path=wallet_path)
            scope = SimpleNamespace(add_metadata=lambda **kwargs: None)
            with patch.object(monitor, "load_wallets", return_value=("WALLET",)), \
                    patch.object(monitor.state_store, "set_global_metrics"), \
                    patch.object(monitor, "phase_memory", side_effect=lambda *args, **kwargs: nullcontext(scope)), \
                    patch.object(monitor, "monitor_once", side_effect=hold), \
                    patch.object(monitor, "monitor_standard_once", side_effect=hold), \
                    patch.object(monitor, "watch_wallet_file", side_effect=hold), \
                    patch.object(monitor, "monitor_heartbeat", side_effect=hold), \
                    patch.object(monitor, "subscription_refresh_timer", side_effect=hold):
                parent = asyncio.create_task(monitor.run_forever(settings))
                await asyncio.wait_for(all_started.wait(), timeout=2)
                parent.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await parent
                self.assertEqual(set(started), set(completed))
                self.assertTrue(all(task.done() and task.cancelled() for task in started))
        with tempfile.TemporaryDirectory() as temporary:
            wallet_path = Path(temporary) / "wallets.json"
            wallet_path.write_text("{}", encoding="utf-8")
            asyncio.run(scenario(wallet_path))

    def test_stop_monitor_failure_does_not_become_control_stop(self):
        from src.research import entry_telemetry_epoch as epoch

        with patch.object(epoch, "stop_requested", side_effect=[RuntimeError("injected stop monitor failure"), True]) as stop, \
                patch.object(monitor.asyncio, "sleep", new=AsyncMock()) as wait:
            asyncio.run(monitor._wait_for_clean_stop())
        self.assertEqual(stop.call_count, 2)
        wait.assert_awaited_once_with(1.0)

    def test_flush_failure_does_not_mask_original_control_exception(self):
        from src.research import entry_telemetry as telemetry, entry_telemetry_epoch as epoch

        with patch.object(monitor, "configure_safe_logging"), \
                patch.object(monitor, "load_dotenv"), \
                patch.object(telemetry, "start_worker"), \
                patch.object(epoch, "safe_runtime_config", return_value={}), \
                patch.object(monitor.n3_shadow, "start_capture_worker"), \
                patch.object(monitor, "run_service", new=AsyncMock(side_effect=RuntimeError("original Control failure"))), \
                patch.object(monitor, "flush_phase_memory_telemetry"), \
                patch.object(telemetry, "flush", side_effect=RuntimeError("injected telemetry flush failure")), \
                patch.object(monitor.logger, "warning"):
            with self.assertRaisesRegex(RuntimeError, "original Control failure"):
                monitor.main()
