from __future__ import annotations

import math
import tempfile
import unittest
from pathlib import Path

from src.observation_analysis import (
    ABSOLUTE_MAX_ANALYSIS_ROWS,
    build_observation_analysis,
    performance_metrics,
    refresh_observation_analysis,
)
from src.state_store import update_json


def observation(
    started_at: float,
    roi: float,
    *,
    route: str = "B",
    eligible: bool = True,
    score: float = 95.0,
    mint: str | None = None,
) -> dict:
    return {
        "mint": mint or f"MINT-{started_at}",
        "started_at_epoch": started_at,
        "route_type": route,
        "decision_status": "APPROVED" if eligible else "REJECTED",
        "candidate_v2_eligible": eligible,
        "paper_experiment_status": "OPENED",
        "decision_reasons": [],
        "discovery_metadata": {"whale_paid_lamports": 2_000_000_000},
        "entry_price_impact_pct": 0.5,
        "exit_price_impact_pct": 0.7,
        "safety_score": 80,
        "dex_momentum_score": score,
        "safety_metrics": {
            "developer_supply_percent": 4,
            "lp_locked_percent": 85,
            "liquidity_usd": 12_000,
        },
        "momentum_metrics": {
            "volume_m5_usd": 20_000,
            "net_buys_m5": 12,
            "buy_sell_ratio_m5": 2,
            "unknown_whale_count": 3,
            "pair_age_seconds": 1_000,
        },
        "samples": [{"interval": "15m", "return_percent": roi}],
    }


class PerformanceMetricsTests(unittest.TestCase):
    def test_metrics_include_win_mean_median_and_downside_tail(self) -> None:
        metrics = performance_metrics(
            [-20.0, -10.0, 10.0, 20.0],
            minimum_samples=4,
        )
        self.assertEqual(metrics["sample_count"], 4)
        self.assertEqual(metrics["win_rate_percent"], 50.0)
        self.assertEqual(metrics["mean_roi_percent"], 0.0)
        self.assertEqual(metrics["median_roi_percent"], 0.0)
        self.assertEqual(metrics["downside_p10_roi_percent"], -17.0)
        self.assertEqual(metrics["worst_decile_mean_roi_percent"], -20.0)
        self.assertTrue(metrics["sufficient_samples"])

    def test_metrics_reject_nonfinite_values_from_output(self) -> None:
        metrics = performance_metrics(
            [1.0, math.nan, math.inf, -math.inf, "invalid"],
            minimum_samples=2,
        )
        self.assertEqual(metrics["sample_count"], 1)
        self.assertFalse(metrics["sufficient_samples"])
        self.assertEqual(metrics["mean_roi_percent"], 1.0)


class ObservationAnalysisTests(unittest.TestCase):
    def test_time_ordered_split_is_independent_of_input_order(self) -> None:
        rows = [
            observation(4, 40),
            observation(1, -10),
            observation(3, 30),
            observation(2, -20),
        ]
        report = build_observation_analysis(
            rows,
            minimum_samples=1,
            holdout_fraction=0.5,
        )
        self.assertEqual(report["split"]["train_count"], 2)
        self.assertEqual(report["split"]["holdout_count"], 2)
        self.assertEqual(report["overall"]["train"]["mean_roi_percent"], -15.0)
        self.assertEqual(report["overall"]["holdout"]["mean_roi_percent"], 35.0)
        self.assertEqual(report["overall"]["validation_status"], "SUFFICIENT_SAMPLES")

    def test_conditions_cover_route_decisions_safety_and_momentum(self) -> None:
        report = build_observation_analysis(
            [observation(1, 10), observation(2, -5)],
            minimum_samples=1,
            holdout_fraction=0.5,
        )
        keys = {
            (row["dimension"], row["condition"])
            for row in report["conditions"]
        }
        self.assertIn(("route", "B"), keys)
        self.assertIn(("entry_decision", "APPROVED"), keys)
        self.assertIn(("candidate_decision", "ELIGIBLE"), keys)
        self.assertIn(("paper_decision", "OPENED"), keys)
        self.assertIn(("safety_score", "70_to_below_90"), keys)
        self.assertIn(("lp_locked_percent", "80_or_more"), keys)
        self.assertIn(("liquidity_usd", "10000_or_more"), keys)
        self.assertIn(("dex_momentum_score", "90_to_below_100"), keys)
        self.assertIn(("volume_m5_usd", "15000_or_more"), keys)
        self.assertIn(("net_buys_m5", "10_to_below_15"), keys)
        self.assertIn(("buy_sell_ratio_m5", "1.8_or_more"), keys)
        self.assertIn(("pair_age_seconds", "900_or_more"), keys)
        self.assertIn(("unknown_whale_count", "3_or_more"), keys)
        self.assertIn(("source_buy_sol", "1.5_to_below_5"), keys)
        self.assertIn(("entry_price_impact_pct", "below_1"), keys)

    def test_insufficient_samples_are_explicit(self) -> None:
        report = build_observation_analysis(
            [observation(1, 10), observation(2, 20)],
            minimum_samples=2,
            holdout_fraction=0.5,
        )
        self.assertEqual(
            report["overall"]["validation_status"],
            "INSUFFICIENT_SAMPLES",
        )
        self.assertFalse(report["overall"]["train"]["sufficient_samples"])
        self.assertFalse(report["overall"]["holdout"]["sufficient_samples"])

    def test_latest_window_and_invalid_values_are_bounded_and_counted(self) -> None:
        rows = [observation(1, 1), observation(2, math.nan), "bad", observation(4, 4)]
        report = build_observation_analysis(
            rows,
            minimum_samples=1,
            holdout_fraction=0.5,
            max_rows=3,
        )
        self.assertEqual(report["bounded_row_count"], 3)
        self.assertEqual(report["eligible_outcome_count"], 1)
        self.assertEqual(report["excluded"]["outside_latest_window"], 1)
        self.assertEqual(report["excluded"]["invalid_row"], 1)
        self.assertEqual(report["excluded"]["missing_or_nonfinite_outcome"], 1)

    def test_requested_limit_is_capped_and_arguments_are_validated(self) -> None:
        report = build_observation_analysis(
            [],
            minimum_samples=1,
            max_rows=ABSOLUTE_MAX_ANALYSIS_ROWS + 100,
        )
        self.assertEqual(report["max_rows"], ABSOLUTE_MAX_ANALYSIS_ROWS)
        with self.assertRaises(ValueError):
            build_observation_analysis([], minimum_samples=1, holdout_fraction=math.nan)
        with self.assertRaises(ValueError):
            build_observation_analysis([], minimum_samples=2, max_rows=1)

    def test_positive_train_and_holdout_conditions_are_review_only_candidates(self) -> None:
        report = build_observation_analysis(
            [observation(index, 5 + index) for index in range(1, 5)],
            minimum_samples=1,
            holdout_fraction=0.5,
        )
        candidates = {
            (row["dimension"], row["condition"]): row
            for row in report["condition_candidates"]
        }
        self.assertIn(("route", "B"), candidates)
        self.assertEqual(
            candidates[("route", "B")]["status"],
            "TRAIN_SELECTED_REVIEW_ONLY",
        )
        self.assertEqual(
            candidates[("route", "B")]["holdout_evaluation"], "CONFIRMED"
        )
        self.assertFalse(report["automatic_config_changes"])

    def test_missing_outcome_coverage_blocks_review_candidate(self) -> None:
        rows = [observation(index, 10) for index in range(1, 5)]
        for index in (0, 2):
            rows[index]["samples"][0]["return_percent"] = None
        report = build_observation_analysis(
            rows,
            minimum_samples=1,
            holdout_fraction=0.5,
            minimum_outcome_coverage=0.75,
        )
        self.assertEqual(
            report["overall"]["validation_status"],
            "INSUFFICIENT_OUTCOME_COVERAGE",
        )
        self.assertEqual(report["condition_candidates"], [])

    def test_same_mint_is_counted_once_and_cannot_cross_split(self) -> None:
        rows = [
            observation(1, 10, mint="SAME"),
            observation(2, 20, mint="SAME"),
            observation(3, 30, mint="OTHER"),
        ]
        report = build_observation_analysis(
            rows,
            minimum_samples=1,
            holdout_fraction=0.5,
        )
        self.assertEqual(report["independent_mint_cohort_count"], 2)
        self.assertEqual(report["excluded"]["duplicate_mint_observation"], 1)

    def test_refresh_persists_bounded_review_report_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            observation_path = Path(temporary) / "observations.json"
            analysis_path = Path(temporary) / "analysis.json"

            def seed(document: dict) -> None:
                document["schema_version"] = 4
                document["observations"] = [
                    observation(index, 5 + index) for index in range(1, 5)
                ]

            update_json(
                observation_path,
                {"schema_version": 4, "observations": [], "version": 0},
                seed,
            )
            saved = refresh_observation_analysis(
                observation_path=observation_path,
                analysis_path=analysis_path,
                minimum_samples=1,
            )
        self.assertTrue(saved["condition_candidates"])
        self.assertFalse(saved["automatic_config_changes"])
        self.assertEqual(saved["version"], 1)

    def test_refresh_fails_closed_and_preserves_prior_report(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            observation_path = Path(temporary) / "observations.json"
            analysis_path = Path(temporary) / "analysis.json"
            update_json(
                observation_path,
                {"schema_version": 4, "observations": [], "version": 0},
                lambda document: document.__setitem__("observations", "corrupt"),
            )
            update_json(
                analysis_path,
                {"schema_version": 1, "version": 0},
                lambda document: document.__setitem__("marker", "preserve"),
            )
            with self.assertRaisesRegex(RuntimeError, "malformed"):
                refresh_observation_analysis(
                    observation_path=observation_path,
                    analysis_path=analysis_path,
                    minimum_samples=1,
                )
            from src.state_store import read_json

            preserved = read_json(analysis_path, {})
        self.assertEqual(preserved["marker"], "preserve")
        self.assertEqual(preserved["version"], 1)


if __name__ == "__main__":
    unittest.main()
