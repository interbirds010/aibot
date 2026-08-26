import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src import observation_analysis, observation_tracker, shadow_trade_ledger
from src.state_store import atomic_write_json


def completed_row(observation_id: str = "OBS-1") -> dict:
    return {
        "observation_id": observation_id,
        "mint": f"MINT-{observation_id}",
        "route_type": "B",
        "source_wallet": "market-near-miss",
        "source_signature": f"SIG-{observation_id}",
        "safety_score": 80,
        "entry_cost_lamports": 1_000,
        "token_amount_raw": 500,
        "token_decimals": 6,
        "entry_price_impact_pct": 0.1,
        "exit_price_impact_pct": 0.2,
        "expected_slippage_bps": 100,
        "dex_momentum_score": 95.0,
        "strategy_version": "broad_observation_v1",
        "strategy_variants": ["baseline_v1", "route_b_baseline"],
        "safety_metrics": {"liquidity_usd": 20_000},
        "momentum_metrics": {
            "volume_m5_usd": 20_000,
            "net_buys_m5": 20,
            "buy_sell_ratio_m5": 2.0,
            "pair_age_seconds": 1_000,
        },
        "candidate_v2_eligible": False,
        "candidate_v2_filter_reasons": [],
        "paper_experiment_status": "NOT_ELIGIBLE",
        "paper_experiment_position_id": None,
        "decision_status": "REJECTED",
        "decision_reasons": ["SHADOW_ONLY"],
        "quote_status": "EXECUTABLE",
        "discovery_metadata": {},
        "signal_detected_at": "2026-08-20T00:00:00+00:00",
        "analysis_completed_at": "2026-08-20T00:00:01+00:00",
        "entry_quote_at": "2026-08-20T00:00:02+00:00",
        "entry_latency_ms": 2_000,
        "started_at_epoch": 1_787_184_000.0,
        "started_at": "2026-08-20T00:00:00+00:00",
        "samples": [
            {"interval": "1m", "return_percent": -5.0},
            {"interval": "5m", "return_percent": -12.0},
            {"interval": "15m", "return_percent": 20.0},
        ],
        "sample_attempts": {},
        "status": "COMPLETE",
    }


class ShadowTradeLedgerTests(unittest.TestCase):
    def test_completed_executable_observation_becomes_closed_shadow_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "shadow_trades.json"
            with patch.object(shadow_trade_ledger, "SHADOW_TRADE_PATH", path):
                self.assertTrue(
                    shadow_trade_ledger.record_completed_shadow_trade(completed_row())
                )
                saved = json.loads(path.read_text(encoding="utf-8"))
                trade = saved["trades"][0]
                self.assertEqual(trade["entry_event"], "SHADOW_BUY")
                self.assertEqual(trade["exit_event"], "SHADOW_SELL")
                self.assertEqual(trade["trade_status"], "CLOSED")
                self.assertEqual(
                    trade["strategy_results"]["fixed_hold_15m"]["return_percent"],
                    20.0,
                )
                self.assertEqual(
                    trade["strategy_results"]["sampled_route_exit"]["exit_reason"],
                    "SAMPLED_STOP_LOSS",
                )
                self.assertEqual(
                    trade["strategy_results"]["sampled_route_exit"]["exit_interval"],
                    "5m",
                )

    def test_non_executable_observation_is_not_shadow_traded(self) -> None:
        row = completed_row()
        row["quote_status"] = "ENTRY_ONLY"
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "shadow_trades.json"
            with patch.object(shadow_trade_ledger, "SHADOW_TRADE_PATH", path):
                self.assertFalse(
                    shadow_trade_ledger.record_completed_shadow_trade(row)
                )
                self.assertFalse(path.exists())

    def test_backfill_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "shadow_trades.json"
            rows = [completed_row("OBS-1"), completed_row("OBS-2")]
            with patch.object(shadow_trade_ledger, "SHADOW_TRADE_PATH", path):
                self.assertEqual(
                    shadow_trade_ledger.backfill_completed_shadow_trades(rows), 2
                )
                self.assertEqual(
                    shadow_trade_ledger.backfill_completed_shadow_trades(rows), 0
                )
                saved = json.loads(path.read_text(encoding="utf-8"))
                self.assertEqual(len(saved["trades"]), 2)

    def test_fifteen_minute_sample_archives_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            observation_path = Path(tmp) / "signal_observations.json"
            shadow_path = Path(tmp) / "shadow_trades.json"
            row = completed_row()
            row["samples"] = row["samples"][:2]
            row["status"] = "PENDING"
            document = observation_tracker.empty_observations()
            document["observations"] = [row]
            atomic_write_json(observation_path, document)
            with (
                patch.object(observation_tracker, "OBSERVATION_PATH", observation_path),
                patch.object(shadow_trade_ledger, "SHADOW_TRADE_PATH", shadow_path),
            ):
                self.assertTrue(observation_tracker.record_sample(
                    row["observation_id"],
                    "15m",
                    proceeds_lamports=1_200,
                ))
            saved = json.loads(shadow_path.read_text(encoding="utf-8"))
            self.assertEqual(len(saved["trades"]), 1)
            self.assertEqual(
                saved["trades"][0]["strategy_results"]["fixed_hold_15m"][
                    "return_percent"
                ],
                20.0,
            )

    def test_refresh_prefers_long_lived_shadow_trades(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            observation_path = Path(tmp) / "signal_observations.json"
            shadow_path = Path(tmp) / "shadow_trades.json"
            analysis_path = Path(tmp) / "observation_analysis.json"
            atomic_write_json(
                observation_path,
                observation_tracker.empty_observations(),
            )
            shadow_document = shadow_trade_ledger.empty_shadow_trades()
            shadow_document["trades"] = [
                shadow_trade_ledger.completed_shadow_trade(completed_row())
            ]
            atomic_write_json(shadow_path, shadow_document)
            with (
                patch.object(observation_tracker, "OBSERVATION_PATH", observation_path),
                patch.object(shadow_trade_ledger, "SHADOW_TRADE_PATH", shadow_path),
                patch.object(observation_analysis, "ANALYSIS_PATH", analysis_path),
            ):
                report = observation_analysis.refresh_observation_analysis(
                    minimum_samples=1
                )
            self.assertEqual(report["input_source"], "shadow_trades")
            self.assertEqual(report["input_row_count"], 1)
            self.assertEqual(
                report["strategy_comparisons"]["sampled_route_exit"]["overall"]
                ["train"]["mean_roi_percent"],
                -12.0,
            )


if __name__ == "__main__":
    unittest.main()
