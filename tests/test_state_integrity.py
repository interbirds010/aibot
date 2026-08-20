from __future__ import annotations

import asyncio
import multiprocessing
import tempfile
import unittest
from pathlib import Path

from src import risk_manager, state_store


def competing_exit_worker(
    ledger_path: str, position_id: str, start: multiprocessing.synchronize.Event,
    results: multiprocessing.queues.Queue,
) -> None:
    from src import risk_manager as worker_risk

    worker_risk.LEDGER_PATH = Path(ledger_path)

    async def run() -> None:
        start.wait()
        claim = await worker_risk.claim_position_exit("MINT_A", position_id, "MANUAL_CLOSE")
        if not claim:
            results.put(False)
            return
        exit_id, amount = claim
        recorded = await worker_risk.record_paper_sell(
            "MINT_A", amount, 120_000_000, "MANUAL_CLOSE",
            position_id=position_id, exit_id=exit_id,
        )
        results.put(recorded)

    asyncio.run(run())


class StateIntegrityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.ledger_path = Path(self.temporary.name) / "paper_trades.json"
        risk_manager.LEDGER_PATH = self.ledger_path

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_same_mint_reentry_gets_a_new_position_id(self) -> None:
        async def scenario() -> tuple[str, str]:
            first = await risk_manager.record_paper_buy(
                "MINT_A", 100_000_000, 1_000_000, 6
            )
            claim = await risk_manager.claim_position_exit("MINT_A", first, "MANUAL_CLOSE")
            self.assertIsNotNone(claim)
            exit_id, amount = claim  # type: ignore[misc]
            self.assertTrue(await risk_manager.record_paper_sell(
                "MINT_A", amount, 100_000_000, "MANUAL_CLOSE",
                position_id=first, exit_id=exit_id,
            ))
            second = await risk_manager.record_paper_buy(
                "MINT_A", 100_000_000, 2_000_000, 6
            )
            return first, second

        first, second = asyncio.run(scenario())
        self.assertNotEqual(first, second)
        buys = [
            event for event in risk_manager.read_ledger()["events"]
            if event.get("type") == "BUY"
        ]
        self.assertEqual([event["round_index"] for event in buys], [1, 2])

    def test_cross_process_exit_competition_records_one_sell(self) -> None:
        position_id = asyncio.run(risk_manager.record_paper_buy(
            "MINT_A", 100_000_000, 1_000_000, 6
        ))
        context = multiprocessing.get_context("spawn")
        start = context.Event()
        results = context.Queue()
        workers = [
            context.Process(
                target=competing_exit_worker,
                args=(str(self.ledger_path), position_id, start, results),
            )
            for _ in range(2)
        ]
        for worker in workers:
            worker.start()
        start.set()
        for worker in workers:
            worker.join(15)
            self.assertEqual(worker.exitcode, 0)
        outcomes = [results.get(timeout=2) for _ in workers]
        self.assertEqual(outcomes.count(True), 1)
        sells = [
            event for event in risk_manager.read_ledger()["events"]
            if event.get("type") == "SELL"
        ]
        self.assertEqual(len(sells), 1)

    def test_cost_basis_accounting_invariant(self) -> None:
        async def scenario() -> None:
            position_id = await risk_manager.record_paper_buy(
                "MINT_A", 100_000_000, 1_000_000, 6
            )
            claim = await risk_manager.claim_position_exit(
                "MINT_A", position_id, "MANUAL_CLOSE"
            )
            self.assertIsNotNone(claim)
            exit_id, amount = claim  # type: ignore[misc]
            await risk_manager.record_paper_sell(
                "MINT_A", amount, 120_000_000, "MANUAL_CLOSE",
                position_id=position_id, exit_id=exit_id,
            )

        asyncio.run(scenario())
        ledger = risk_manager.read_ledger()
        remaining_cost = sum(
            int(position.get("remaining_cost_lamports", 0))
            for position in ledger["positions"].values()
        )
        realized_pnl = sum(
            int(event.get("realized_pnl_lamports", 0))
            for event in ledger["events"] if event.get("type") == "SELL"
        )
        # Cash already contains sale proceeds, so realized PnL must be subtracted
        # when reconciling the original cost-basis ledger.
        reconciled_initial = int(ledger["cash_lamports"]) + remaining_cost - realized_pnl
        self.assertEqual(reconciled_initial, risk_manager.INITIAL_PAPER_LAMPORTS)


class GlobalMetricsIntegrityTests(unittest.TestCase):
    def test_multiple_metrics_merge_without_losing_existing_values(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            original = state_store.GLOBAL_METRICS_PATH
            state_store.GLOBAL_METRICS_PATH = Path(directory) / "global_metrics.json"
            try:
                state_store.set_global_metric("existing", 1)
                state_store.set_global_metrics({"heartbeat": 2, "state": "READY"})
                self.assertEqual(state_store.get_global_metric("existing"), 1)
                self.assertEqual(state_store.get_global_metric("heartbeat"), 2)
                self.assertEqual(state_store.get_global_metric("state"), "READY")
            finally:
                state_store.GLOBAL_METRICS_PATH = original


if __name__ == "__main__":
    multiprocessing.freeze_support()
    unittest.main()
