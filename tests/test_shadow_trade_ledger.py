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
        "signal_type": "MOMENTUM",
        "research_decision": "REJECTED",
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
            {"interval": "3m", "return_percent": 3.0},
            {"interval": "5m", "return_percent": -12.0},
            {"interval": "15m", "return_percent": 20.0},
            {"interval": "30m", "return_percent": 8.0},
            {"interval": "60m", "return_percent": 14.0},
        ],
        "mfe_percent": 20.0,
        "mae_percent": -12.0,
        "excursion_basis": "scheduled_jupiter_executable_quotes",
        "tracking_profile": "research_v1_60m",
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
                self.assertEqual(trade["signal_type"], "MOMENTUM")
                self.assertEqual(trade["research_decision"], "REJECTED")
                self.assertEqual(trade["mfe_percent"], 20.0)
                self.assertEqual(trade["mae_percent"], -12.0)
                self.assertEqual(
                    trade["excursion_basis"],
                    "scheduled_jupiter_executable_quotes",
                )
                self.assertEqual(trade["tracking_profile"], "research_v1_60m")
                self.assertEqual(
                    [
                        result.removeprefix("fixed_hold_")
                        for result in trade["strategy_results"]
                        if result.startswith("fixed_hold_")
                    ],
                    ["1m", "3m", "5m", "15m", "30m", "60m"],
                )
                self.assertEqual(
                    trade["strategy_results"]["fixed_hold_15m"]["return_percent"],
                    20.0,
                )
                self.assertEqual(
                    trade["strategy_results"]["fixed_hold_60m"]["return_percent"],
                    14.0,
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

    def test_only_sixty_minute_sample_completes_and_archives_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            observation_path = Path(tmp) / "signal_observations.json"
            shadow_path = Path(tmp) / "shadow_trades.json"
            row = completed_row()
            row["samples"] = row["samples"][:3]
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
                pending = json.loads(observation_path.read_text(encoding="utf-8"))
                self.assertEqual(pending["observations"][0]["status"], "PENDING")
                self.assertFalse(shadow_path.exists())
                self.assertTrue(observation_tracker.record_sample(
                    row["observation_id"],
                    "30m",
                    proceeds_lamports=1_100,
                ))
                self.assertTrue(observation_tracker.record_sample(
                    row["observation_id"],
                    "60m",
                    proceeds_lamports=1_140,
                ))
            saved = json.loads(shadow_path.read_text(encoding="utf-8"))
            self.assertEqual(len(saved["trades"]), 1)
            self.assertEqual(
                saved["trades"][0]["strategy_results"]["fixed_hold_15m"][
                    "return_percent"
                ],
                20.0,
            )
            self.assertEqual(
                saved["trades"][0]["strategy_results"]["fixed_hold_60m"][
                    "return_percent"
                ],
                14.0,
            )

    def test_completed_shadow_trade_requires_sixty_minute_horizon(self) -> None:
        row = completed_row()
        row["samples"] = [
            sample for sample in row["samples"]
            if sample["interval"] != "60m"
        ]
        self.assertIsNone(shadow_trade_ledger.completed_shadow_trade(row))

    def test_shadow_migration_fails_closed_for_malformed_trade(self) -> None:
        document = {"schema_version": 1, "trades": ["corrupt-trade"]}
        with self.assertRaisesRegex(RuntimeError, "ledger is malformed"):
            shadow_trade_ledger.migrate_shadow_trade_document(document)

    def test_legacy_completed_trade_keeps_fifteen_minute_terminal_horizon(self) -> None:
        row = completed_row()
        row["samples"] = [
            sample for sample in row["samples"]
            if sample["interval"] in {"1m", "5m", "15m"}
        ]
        row.pop("tracking_profile")
        document = {"schema_version": 1, "trades": [row]}
        self.assertTrue(shadow_trade_ledger.migrate_shadow_trade_document(document))
        migrated = document["trades"][0]
        self.assertEqual(migrated["tracking_profile"], "legacy_15m")
        self.assertEqual(migrated["mfe_percent"], 20.0)
        self.assertEqual(migrated["mae_percent"], -12.0)
        self.assertIsNotNone(shadow_trade_ledger.completed_shadow_trade(migrated))

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
            self.assertEqual(report["research_metrics"]["signal_count"], 0)
            self.assertEqual(
                report["long_term_outcome_metrics"]["signal_count"], 1
            )
            self.assertEqual(
                report["strategy_comparisons"]["sampled_route_exit"]["overall"]
                ["train"]["mean_roi_percent"],
                -12.0,
            )


if __name__ == "__main__":
    unittest.main()
