from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

from src import analyzer, executor, risk_manager


class DualRouteSelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = analyzer.AnalyzerSettings(rpc_url="https://rpc.invalid")

    def route(self, lp: str, liquidity: str, *, authority: bool = True) -> str | None:
        return analyzer.select_route_type(
            safety_score=100 if Decimal(lp) >= 80 else 65,
            developer_below_ten_percent=True,
            mint_authority_renounced=authority,
            lp_locked_percent=Decimal(lp),
            liquidity=Decimal(liquidity),
            settings=self.settings,
        )

    def test_route_a_keeps_full_size(self) -> None:
        self.assertEqual(self.route("80", "10000"), "A")
        self.assertEqual(executor.route_sized_amount(10_000, "A"), 10_000)

    def test_route_b_uses_fifteen_percent_size(self) -> None:
        self.assertEqual(self.route("0", "10000"), "B")
        self.assertEqual(self.route("80", "5000"), "B")
        self.assertEqual(executor.route_sized_amount(10_000, "B"), 1_500)

    def test_route_b_fails_closed_for_low_liquidity_or_live_authority(self) -> None:
        self.assertIsNone(self.route("0", "4999.99"))
        self.assertIsNone(self.route("0", "10000", authority=False))


class RouteBRiskTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.ledger_path = Path(self.temporary.name) / "paper_trades.json"
        self.original_ledger_path = risk_manager.LEDGER_PATH
        risk_manager.LEDGER_PATH = self.ledger_path

    def tearDown(self) -> None:
        risk_manager.LEDGER_PATH = self.original_ledger_path
        self.temporary.cleanup()

    def test_route_b_position_persists_tight_stop(self) -> None:
        asyncio.run(
            risk_manager.record_paper_buy(
                "MINT_B", 100_000, 1_000, route_type="B"
            )
        )
        ledger = json.loads(self.ledger_path.read_text(encoding="utf-8"))
        position = ledger["positions"]["MINT_B"]
        self.assertEqual(position["route_type"], "B")
        self.assertEqual(position["stop_loss_ratio"], 0.90)
        self.assertEqual(ledger["events"][-1]["route_type"], "B")

    def test_route_b_takes_half_profit_at_thirty_percent(self) -> None:
        position = {
            "mint": "MINT_B",
            "position_id": "position-b",
            "token_amount_raw": 1_000,
            "remaining_cost_lamports": 100_000,
            "route_type": "B",
            "risk_state": "NORMAL",
            "take_profit_done": False,
        }
        quote = {"outAmount": "130000", "routePlan": [{}]}
        with (
            patch.object(risk_manager, "jupiter_quote", new=AsyncMock(return_value=quote)),
            patch.object(risk_manager, "record_position_mark", new=AsyncMock()),
            patch.object(risk_manager, "_execute_paper_exit", new=AsyncMock(return_value=True)) as exit_mock,
        ):
            asyncio.run(risk_manager.evaluate_paper_position(object(), "key", position))
        self.assertEqual(exit_mock.await_args.args[3], "ROUTE_B_TAKE_PROFIT_30")

    def test_rpc_skip_does_not_lock_cash(self) -> None:
        before = risk_manager.empty_ledger()["cash_lamports"]
        asyncio.run(
            risk_manager.record_rpc_skip(
                "MINT_B", "WALLET", "SIGNATURE", "getTokenSupply failed"
            )
        )
        ledger = json.loads(self.ledger_path.read_text(encoding="utf-8"))
        self.assertEqual(ledger["cash_lamports"], before)
        self.assertEqual(ledger["positions"], {})
        self.assertEqual(ledger["events"][-1]["type"], "SKIPPED_BY_RPC_ERROR")


if __name__ == "__main__":
    unittest.main()
