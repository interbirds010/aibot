"""완료 outcome 계측 실패가 실제 Paper 원장 의미를 바꾸지 않는지 검증한다."""
from __future__ import annotations

import asyncio
from contextlib import ExitStack
import queue
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from src import risk_manager, state_store
from src.research import entry_telemetry as telemetry
from src.research import entry_telemetry_join as join

STAMP = "2026-10-04T00:00:00+00:00"


class EntryTelemetryOutcomeControlTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(risk_manager, "LEDGER_PATH", self.root / "data/paper_trades.json"))
        self.stack.enter_context(patch.object(telemetry, "is_enabled", return_value=True, create=True))
        self.submit = self.stack.enter_context(patch.object(telemetry, "submit_outcome", create=True))

    def buy(self):
        return asyncio.run(risk_manager.record_paper_buy(
            "MINT", 100, 1000, source_signature="sig", signal_detected_at=STAMP,
        ))

    def sell(self, amount, proceeds, trade_id=None):
        return asyncio.run(risk_manager.record_paper_sell(
            "MINT", amount, proceeds, "MANUAL_CLOSE", position_id=trade_id,
        ))

    def test_real_buy_and_completed_sell_generate_outcome_after_lock_release(self):
        trade_id = self.buy()
        self.submit.assert_not_called()

        def submit(payload):
            with state_store.exclusive_file_lock(risk_manager.LEDGER_PATH, timeout_seconds=0.1):
                self.assertNotIn("MINT", state_store.read_json(risk_manager.LEDGER_PATH, {})["positions"])
            self.assertEqual(payload["trade_id"], trade_id)

        self.submit.side_effect = submit
        self.assertTrue(self.sell(1000, 120, trade_id))
        self.submit.assert_called_once()
        payload = self.submit.call_args.args[0]
        self.assertEqual(payload["buy_event_seq"], 1)
        self.assertEqual(payload["event_seq"], 2)
        self.assertEqual(payload["realized_pnl_lamports"], 20)
        self.assertEqual(payload["signal_detected_at"], STAMP)
        self.assertTrue(payload["sell_legs_complete"])
        self.assertEqual(len(payload["sell_legs"]), 1)

    def test_partial_sell_has_no_completed_outcome(self):
        trade_id = self.buy()
        self.assertTrue(self.sell(400, 50, trade_id))
        self.submit.assert_not_called()
        self.assertTrue(self.sell(600, 70, trade_id))
        payload = self.submit.call_args.args[0]
        self.assertEqual([leg["event_seq"] for leg in payload["sell_legs"]], [2, 3])
        self.assertEqual(payload["cumulative_proceeds_lamports"], 120)
        self.assertEqual(payload["realized_pnl_lamports"], 20)

    def test_writer_exception_does_not_undo_sell_or_ledger_cost_invariant(self):
        trade_id = self.buy()
        self.submit.side_effect = RuntimeError("injected writer failure")
        self.assertTrue(self.sell(1000, 120, trade_id))
        ledger = state_store.read_json(risk_manager.LEDGER_PATH, {})
        self.assertEqual(ledger["cash_lamports"], risk_manager.INITIAL_PAPER_LAMPORTS + 20)
        self.assertEqual(ledger["events"][-1]["realized_pnl_lamports"], 20)
        self.assertEqual(ledger["positions"], {})

    def test_projection_failure_does_not_block_control(self):
        trade_id = self.buy()
        with patch.object(risk_manager, "_completed_telemetry", side_effect=RuntimeError("injected extraction failure")):
            self.assertTrue(self.sell(1000, 90, trade_id))
        self.submit.assert_not_called()
        self.assertEqual(state_store.read_json(risk_manager.LEDGER_PATH, {})["positions"], {})

    def test_wrong_trade_id_and_repeat_sell_do_not_generate_outcome(self):
        trade_id = self.buy()
        self.assertFalse(self.sell(1000, 120, "wrong-id"))
        self.submit.assert_not_called()
        self.assertTrue(self.sell(1000, 120, trade_id))
        self.assertFalse(self.sell(1000, 120, trade_id))
        self.submit.assert_called_once()

    def test_disabled_epoch_has_no_scan_or_outcome(self):
        trade_id = self.buy()
        with patch.object(telemetry, "is_enabled", return_value=False):
            self.assertTrue(self.sell(1000, 120, trade_id))
        self.submit.assert_not_called()

    def test_only_explicit_outcome_fields_leave_transaction(self):
        trade_id = self.buy()

        def mutate(document):
            document["positions"]["MINT"]["arbitrary_result"] = {"private_key": "excluded"}

        state_store.update_json(risk_manager.LEDGER_PATH, {}, mutate)
        self.assertTrue(self.sell(1000, 120, trade_id))
        payload = self.submit.call_args.args[0]
        self.assertNotIn("arbitrary_result", payload)
        self.assertNotIn("private_key", str(payload))

    def test_missing_buy_is_explicit_and_not_inferred_from_sell(self):
        trade_id = self.buy()

        def mutate(document):
            document["events"] = []

        state_store.update_json(risk_manager.LEDGER_PATH, {}, mutate)
        self.assertTrue(self.sell(1000, 120, trade_id))
        payload = self.submit.call_args.args[0]
        self.assertIsNone(payload["buy_event_seq"])
        self.assertFalse(payload["sell_legs_complete"])

    def test_sell_leg_copy_is_bounded(self):
        trade_id = self.buy()
        position = state_store.read_json(risk_manager.LEDGER_PATH, {})["positions"]["MINT"]
        events = [{"type": "BUY", "position_id": trade_id, "event_seq": 1, "event_id": "buy"}]
        events.extend({"type": "SELL", "position_id": trade_id, "event_seq": index,
                       "event_id": str(index), "at": STAMP} for index in range(2, 72))
        payload = risk_manager._completed_telemetry({"events": events}, position)
        self.assertEqual(len(payload["sell_legs"]), 64)
        self.assertTrue(payload["sell_legs_truncated"])
        self.assertFalse(payload["sell_legs_complete"])
        self.assertEqual(payload["buy_event_seq"], 1)

    def test_buy_receipt_hook_failure_preserves_control_buy(self):
        with patch.object(telemetry, "safe_hook", side_effect=RuntimeError("injected receipt hook failure")):
            trade_id = self.buy()
        ledger = state_store.read_json(risk_manager.LEDGER_PATH, {})
        self.assertEqual(ledger["positions"]["MINT"]["position_id"], trade_id)
        self.assertEqual(ledger["events"][0]["event_seq"], 1)


class EntryTelemetryActualSplitOutcomeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for name, value in {
            "_root": self.root,
            "_epoch": {"telemetry_epoch_id": "offline-epoch", "start_utc": STAMP, "start_event_seq": 1},
            "_queue": queue.Queue(maxsize=16), "_receipt_queue": queue.Queue(maxsize=16),
            "_outcome_queue": queue.Queue(maxsize=16),
            "_provenance": {"session_id": "offline-session", "git_sha": "f" * 40},
            "_health": {"status": "TELEMETRY_READY", "dropped_row_count": 0, "write_error_count": 0,
                        "duplicate_count": 0, "conflict_count": 0, "last_error": None},
        }.items():
            self.stack.enter_context(patch.object(telemetry, name, value))
        self.stack.enter_context(patch.object(risk_manager, "LEDGER_PATH", self.root / "data/paper_trades.json"))

    def test_real_control_buy_and_sell_use_three_separate_immutable_files_and_offline_join(self):
        capture = telemetry.begin_signal(mint="MINT", route_type="A", signal_detected_at=STAMP)
        with telemetry.bind(capture):
            telemetry.mark("entry_decision_at")
            trade_id = asyncio.run(risk_manager.record_paper_buy(
                "MINT", 100, 1000, signal_detected_at=STAMP,
            ))
        telemetry.finish(capture, outcome="BUY", trade_id=trade_id)
        self.assertEqual(capture.receipt["event_seq"], 1)
        telemetry._persist(capture)
        directory = self.root / "data/research/entry_telemetry/epochs/offline-epoch"
        predictor_path = next((directory / "predictors").glob("*/*.json"))
        receipt_path = next((directory / "receipts").glob("*/*.json"))
        before = predictor_path.read_bytes()
        self.assertTrue(asyncio.run(risk_manager.record_paper_sell(
            "MINT", 1000, 120, "MANUAL_CLOSE", position_id=trade_id,
        )))
        value = telemetry._outcome_queue.get_nowait()
        telemetry._write_stream("outcomes", value)
        outcome_path = next((directory / "outcomes").glob("*/*.json"))
        self.assertEqual(len({predictor_path, receipt_path, outcome_path}), 3)
        self.assertEqual(predictor_path.read_bytes(), before)
        result = join.join_epoch(self.root, "offline-epoch")
        self.assertEqual(result["rows"][0]["join_status"], "COMPLETED")
        self.assertEqual(result["rows"][0]["outcome"]["identity"]["buy_event_seq"], 1)
        self.assertNotIn("realized_pnl", before.decode("utf-8"))
        self.assertNotIn("execution_receipt", before.decode("utf-8"))
        telemetry._write_stream("outcomes", value)
        self.assertEqual(telemetry._health["streams"]["outcomes"]["duplicate_count"], 1)
        self.assertEqual(predictor_path.read_bytes(), before)

    def test_outcome_queue_failure_is_recorded_without_blocking_completed_sell(self):
        queue_full = queue.Queue(maxsize=1)
        queue_full.put_nowait({})
        with patch.object(telemetry, "_outcome_queue", queue_full):
            trade_id = asyncio.run(risk_manager.record_paper_buy("MINT", 100, 1000, signal_detected_at=STAMP))
            self.assertTrue(asyncio.run(risk_manager.record_paper_sell(
                "MINT", 1000, 120, "MANUAL_CLOSE", position_id=trade_id,
            )))
        self.assertEqual(telemetry._health["streams"]["outcomes"]["dropped_row_count"], 1)
        self.assertEqual(state_store.read_json(risk_manager.LEDGER_PATH, {})["positions"], {})

    def test_legacy_buy_seq_or_pre_epoch_timestamp_is_excluded_from_new_outcomes(self):
        for start_seq, stamp in ((2, STAMP), (1, "2026-10-03T23:59:59+00:00")):
            with self.subTest(start_seq=start_seq, stamp=stamp):
                root = self.root / str(start_seq)
                with patch.object(risk_manager, "LEDGER_PATH", root / "data/paper_trades.json"), \
                        patch.object(telemetry, "_epoch", {"telemetry_epoch_id": "offline-epoch", "start_utc": STAMP,
                                                         "start_event_seq": start_seq}):
                    trade_id = asyncio.run(risk_manager.record_paper_buy("MINT", 100, 1000, signal_detected_at=stamp))
                    self.assertTrue(asyncio.run(risk_manager.record_paper_sell(
                        "MINT", 1000, 120, "MANUAL_CLOSE", position_id=trade_id,
                    )))
                    self.assertTrue(telemetry._outcome_queue.empty())

    def test_outcome_write_failure_does_not_change_existing_predictor_or_receipt(self):
        capture = telemetry.begin_signal(mint="MINT", route_type="A", signal_detected_at=STAMP)
        telemetry.finish(capture, outcome="BUY", trade_id="trade", event_seq=1)
        telemetry._persist(capture)
        directory = self.root / "data/research/entry_telemetry/epochs/offline-epoch"
        paths = list((directory / "predictors").glob("*/*.json")) + list((directory / "receipts").glob("*/*.json"))
        before = {path: path.read_bytes() for path in paths}
        value = {"trade_id": "trade", "mint": "MINT", "signal_detected_at": STAMP, "buy_event_seq": 1}
        with patch.object(telemetry, "atomic_write_json", side_effect=PermissionError("injected outcome failure")):
            telemetry._write_stream("outcomes", value)
        self.assertEqual(before, {path: path.read_bytes() for path in paths})
        self.assertEqual(telemetry._health["streams"]["outcomes"]["write_error_count"], 1)


class EntryTelemetryRiskShutdownTests(unittest.TestCase):
    def test_stop_request_does_not_cancel_current_exit_or_start_next_position(self):
        async def evaluate(session, api_key, position):
            self.assertEqual(position["position_id"], "first")
            await asyncio.sleep(0)
            completed.append("first")

        completed = []
        settings = SimpleNamespace(jupiter_api_key="test-only")
        with patch.object(risk_manager.ExecutionSettings, "from_env", return_value=settings), \
                patch.object(risk_manager, "ensure_ledger_migrated"), \
                patch.object(risk_manager, "read_ledger", return_value={"positions": {
                    "MINT1": {"position_id": "first"}, "MINT2": {"position_id": "second"}}}), \
                patch.object(risk_manager, "evaluate_paper_position", side_effect=evaluate) as process, \
                patch.object(risk_manager, "_telemetry_stop_requested", side_effect=[False, False, True, True]), \
                patch.object(risk_manager.asyncio, "sleep", new=AsyncMock()):
            asyncio.run(risk_manager.run_risk_loop())
        self.assertEqual(completed, ["first"])
        self.assertEqual(process.await_count, 1)

    def test_main_flushes_then_acknowledges_only_after_control_returns(self):
        from src.research import entry_telemetry_epoch as epoch

        calls = []

        async def run(**kwargs):
            calls.append("control_returned")

        def flush(**kwargs):
            self.assertEqual(kwargs, {"timeout": 5.0})
            calls.append("queues_drained")
            return True

        def acknowledge(*args, **kwargs):
            self.assertEqual(calls, ["control_returned", "queues_drained"])
            self.assertTrue(kwargs["drained"])

        with patch.object(risk_manager, "configure_safe_logging"), \
                patch.object(telemetry, "start_worker"), \
                patch.object(epoch, "safe_runtime_config", return_value={}), \
                patch.object(risk_manager, "run_risk_loop", side_effect=run), \
                patch.object(telemetry, "flush", side_effect=flush), \
                patch.object(epoch, "acknowledge_stopped", side_effect=acknowledge) as ack, \
                patch("dotenv.load_dotenv"):
            risk_manager.main()
        ack.assert_called_once()

    def test_telemetry_start_and_flush_failures_do_not_block_control(self):
        from src.research import entry_telemetry_epoch as epoch

        with patch.object(risk_manager, "configure_safe_logging"), \
                patch.object(telemetry, "start_worker", side_effect=RuntimeError("injected startup failure")), \
                patch.object(epoch, "safe_runtime_config", return_value={}), \
                patch.object(risk_manager, "run_risk_loop", new=AsyncMock()) as control, \
                patch.object(telemetry, "flush", side_effect=RuntimeError("injected drain failure")), \
                patch("dotenv.load_dotenv"):
            risk_manager.main()
        control.assert_awaited_once_with(paper_trading=True)


if __name__ == "__main__":
    unittest.main()
