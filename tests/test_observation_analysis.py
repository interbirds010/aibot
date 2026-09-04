from __future__ import annotations

import math
import tempfile
import unittest
from pathlib import Path

from src.observation_analysis import (
    ABSOLUTE_MAX_ANALYSIS_ROWS,
    build_observation_analysis,
    build_research_metrics,
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

    def test_expectancy_profit_factor_and_win_loss_averages(self) -> None:
        metrics = performance_metrics(
            [-20.0, 0.0, 10.0, 30.0], minimum_samples=1
        )
        self.assertEqual(metrics["average_win_percent"], 20.0)
        self.assertEqual(metrics["average_loss_percent"], -20.0)
        self.assertEqual(metrics["expectancy_percent"], 5.0)
        self.assertEqual(metrics["profit_factor"], 2.0)

    def test_profit_factor_edges_are_json_safe(self) -> None:
        empty = performance_metrics([], minimum_samples=1)
        wins = performance_metrics([10.0, 20.0], minimum_samples=1)
        losses = performance_metrics([-10.0, -20.0], minimum_samples=1)
        self.assertIsNone(empty["profit_factor"])
        self.assertIsNone(wins["profit_factor"])
        self.assertEqual(losses["profit_factor"], 0.0)
        self.assertIsNone(wins["average_loss_percent"])
        self.assertIsNone(losses["average_win_percent"])


class ResearchMetricsTests(unittest.TestCase):
    @staticmethod
    def event(
        mint: str,
        decision: str,
        return_60m: float | None,
        *,
        reason: str | None = None,
        strategy: str = "baseline_v1",
        route: str = "A",
        signal_type: str = "SMART_MONEY",
        status: str = "COMPLETE",
    ) -> dict:
        samples = []
        if return_60m is not None:
            samples = [
                {"interval": "1m", "return_percent": -5.0},
                {"interval": "60m", "return_percent": return_60m},
            ]
        return {
            "observation_id": f"OBS-{mint}-{decision}",
            "mint": mint,
            "research_decision": decision,
            "decision_reasons": [reason] if reason else [],
            "strategy_version": strategy,
            "route_type": route,
            "signal_type": signal_type,
            "status": status,
            "samples": samples,
        }

    def test_empty_and_malformed_rows_are_fail_safe(self) -> None:
        empty = build_research_metrics([])
        malformed = build_research_metrics([
            "bad",
            {"research_decision": "invalid", "samples": "bad"},
        ])
        self.assertEqual(empty["signal_count"], 0)
        self.assertIsNone(empty["horizons"]["60m"]["average_return_percent"])
        self.assertEqual(malformed["invalid_row_count"], 1)
        self.assertEqual(malformed["signal_count"], 1)
        self.assertEqual(malformed["unknown_decision_count"], 1)
        self.assertEqual(malformed["groups"][0]["route_type"], "UNKNOWN")

    def test_counts_all_events_without_deduplicating_mints(self) -> None:
        rows = [
            self.event("SAME", "ENTERED", 10.0),
            self.event("SAME", "REJECTED", 20.0, reason="LOW_SCORE"),
            self.event("OTHER", "SHADOW", None, status="EXPIRED_UNSAMPLED"),
        ]
        metrics = build_research_metrics(rows)
        self.assertEqual(metrics["signal_count"], 3)
        self.assertEqual(metrics["entered_count"], 1)
        self.assertEqual(metrics["rejected_count"], 1)
        self.assertEqual(metrics["shadow_count"], 1)
        self.assertEqual(metrics["expired_count"], 1)
        self.assertEqual(metrics["horizons"]["60m"]["completed_outcome_count"], 2)
        self.assertEqual(metrics["horizons"]["60m"]["average_return_percent"], 15.0)
        self.assertEqual(metrics["horizons"]["60m"]["signal_count"], 3)
        self.assertEqual(metrics["horizons"]["60m"]["sampled_count"], 2)
        self.assertEqual(metrics["horizons"]["60m"]["missing_count"], 1)
        self.assertEqual(metrics["horizons"]["60m"]["coverage_rate_percent"], 66.6667)
        self.assertEqual(
            metrics["horizons"]["60m"]["trackable_coverage_rate_percent"],
            66.6667,
        )

    def test_grouping_and_sampled_excursions(self) -> None:
        rows = [
            self.event("A", "ENTERED", 20.0),
            self.event("B", "SHADOW", -10.0),
            self.event(
                "C", "SHADOW", 30.0,
                strategy="momentum_v1", route="B", signal_type="MOMENTUM",
            ),
        ]
        metrics = build_research_metrics(rows)
        groups = {
            (row["strategy_version"], row["route_type"], row["signal_type"]): row
            for row in metrics["groups"]
        }
        self.assertEqual(groups[("baseline_v1", "A", "SMART_MONEY")]["signal_count"], 2)
        self.assertEqual(groups[("momentum_v1", "B", "MOMENTUM")]["signal_count"], 1)
        self.assertEqual(metrics["average_sampled_mfe_percent"], 16.6667)
        self.assertEqual(metrics["average_sampled_mae_percent"], -6.6667)

    def test_sampled_excursions_include_entry_baseline(self) -> None:
        winner = self.event("WIN", "SHADOW", 20.0)
        winner["samples"] = [{"interval": "60m", "return_percent": 20.0}]
        loser = self.event("LOSS", "SHADOW", -20.0)
        loser["samples"] = [{"interval": "60m", "return_percent": -20.0}]
        metrics = build_research_metrics([winner, loser])
        self.assertEqual(metrics["average_sampled_mfe_percent"], 10.0)
        self.assertEqual(metrics["average_sampled_mae_percent"], -10.0)

    def test_rejection_reason_uses_sixty_minute_outcome(self) -> None:
        rows = [
            self.event("A", "REJECTED", 20.0, reason="low_score"),
            self.event("B", "REJECTED", -10.0, reason="LOW_SCORE"),
            self.event("C", "REJECTED", None, reason="LOW_SCORE"),
        ]
        rejection = build_research_metrics(rows)["rejection_reasons"][0]
        self.assertEqual(rejection["reason"], "LOW_SCORE")
        self.assertEqual(rejection["signal_count"], 3)
        self.assertEqual(rejection["completed_outcome_count"], 2)
        self.assertEqual(rejection["outcome_sample_count"], 2)
        self.assertEqual(rejection["outcome_missing_count"], 1)
        self.assertEqual(rejection["coverage_rate_percent"], 66.6667)
        self.assertEqual(rejection["outcome_trackable_count"], 3)
        self.assertEqual(rejection["outcome_untrackable_count"], 0)
        self.assertEqual(rejection["trackable_coverage_rate_percent"], 66.6667)
        self.assertEqual(rejection["average_return_percent"], 5.0)
        self.assertEqual(rejection["positive_rate_percent"], 50.0)

    def test_data_quality_normalizes_missing_reasons_and_keeps_no_route(self) -> None:
        sampled = self.event("OK", "SHADOW", 12.0)
        sampled["samples"][-1]["sample_lag_seconds"] = 1.0
        missed = self.event("MISSED", "SHADOW", None)
        missed["samples"] = [{
            "interval": "60m",
            "return_percent": None,
            "error": "HORIZON_MISSED",
            "sample_lag_seconds": 61.0,
        }]
        no_route = self.event("NO-ROUTE", "REJECTED", None)
        no_route["quote_status"] = "NO_ROUTE"
        api_failure = self.event("API", "SHADOW", None)
        api_failure["samples"] = [{
            "interval": "60m",
            "return_percent": None,
            "error": "HTTP 503: changing provider detail",
            "sample_lag_seconds": 3.0,
        }]
        unknown = self.event("UNKNOWN", "SHADOW", None)
        unknown["samples"] = [{
            "interval": "60m",
            "return_percent": math.nan,
            "error": None,
            "sample_lag_seconds": math.inf,
        }]

        metrics = build_research_metrics(
            [sampled, missed, no_route, api_failure, unknown]
        )
        horizon = metrics["horizons"]["60m"]
        self.assertEqual(horizon["signal_count"], 5)
        self.assertEqual(horizon["sampled_count"], 1)
        self.assertEqual(horizon["missing_count"], 4)
        self.assertEqual(horizon["coverage_rate_percent"], 20.0)
        self.assertEqual(horizon["trackable_coverage_rate_percent"], 25.0)
        self.assertEqual(horizon["missing_reasons"], {
            "API_FAILURE": 1,
            "ENTRY_NO_ROUTE": 1,
            "HORIZON_MISSED": 1,
            "UNKNOWN": 1,
        })
        self.assertEqual(metrics["outcome_trackable_count"], 4)
        self.assertEqual(metrics["outcome_untrackable_count"], 1)
        self.assertEqual(horizon["outcome_trackable_count"], 4)
        self.assertEqual(horizon["outcome_untrackable_count"], 1)
        self.assertEqual(horizon["lag_sample_count"], 3)
        self.assertEqual(horizon["mean_sample_lag_seconds"], 21.6667)
        self.assertEqual(horizon["median_sample_lag_seconds"], 3.0)
        self.assertEqual(horizon["p90_sample_lag_seconds"], 49.4)
        self.assertEqual(horizon["max_sample_lag_seconds"], 61.0)

    def test_group_horizons_include_coverage_and_lag_metrics(self) -> None:
        first = self.event("A", "ENTERED", 10.0)
        first["samples"][-1]["sample_lag_seconds"] = 2.0
        second = self.event("B", "SHADOW", None)
        second["samples"] = [{
            "interval": "60m",
            "return_percent": None,
            "error": "Jupiter returned no executable route",
            "sample_lag_seconds": 6.0,
        }]
        metrics = build_research_metrics([first, second])
        group = metrics["groups"][0]
        horizon = group["horizons"]["60m"]
        self.assertEqual(horizon["signal_count"], 2)
        self.assertEqual(horizon["sampled_count"], 1)
        self.assertEqual(horizon["coverage_rate_percent"], 50.0)
        self.assertEqual(horizon["trackable_coverage_rate_percent"], 50.0)
        self.assertEqual(horizon["missing_reasons"], {"EXIT_NO_ROUTE": 1})
        self.assertEqual(horizon["lag_sample_count"], 2)
        self.assertEqual(horizon["mean_sample_lag_seconds"], 4.0)

    def test_non_executable_quote_statuses_are_untrackable_not_zero_return(self) -> None:
        rows = []
        for quote_status in (
            "NO_ROUTE", "NOT_REQUESTED", "SIZE_UNUSABLE", "PROCESSING_FAILED",
        ):
            row = self.event(quote_status, "REJECTED", None, reason="NO_ENTRY")
            row["quote_status"] = quote_status
            rows.append(row)
        metrics = build_research_metrics(rows)
        horizon = metrics["horizons"]["60m"]
        self.assertEqual(metrics["signal_count"], 4)
        self.assertEqual(metrics["outcome_untrackable_count"], 4)
        self.assertEqual(horizon["sampled_count"], 0)
        self.assertEqual(horizon["missing_count"], 4)
        self.assertEqual(horizon["coverage_rate_percent"], 0.0)
        self.assertIsNone(horizon["trackable_coverage_rate_percent"])
        self.assertEqual(horizon["missing_reasons"], {
            "ENTRY_NOT_REQUESTED": 1,
            "ENTRY_NO_ROUTE": 1,
            "ENTRY_SIZE_UNUSABLE": 1,
            "PROCESSING_FAILED": 1,
        })
        self.assertIsNone(horizon["average_return_percent"])

        rejection = metrics["rejection_reasons"][0]
        self.assertEqual(rejection["coverage_rate_percent"], 0.0)
        self.assertIsNone(rejection["trackable_coverage_rate_percent"])

    def test_rejection_reason_separates_raw_and_trackable_coverage(self) -> None:
        sampled = self.event("SAMPLED", "REJECTED", 10.0, reason="LOW_SCORE")
        missing = self.event("MISSING", "REJECTED", None, reason="LOW_SCORE")
        no_entry = self.event("NO-ENTRY", "REJECTED", None, reason="LOW_SCORE")
        no_entry["quote_status"] = "NO_ROUTE"

        rejection = build_research_metrics(
            [sampled, missing, no_entry]
        )["rejection_reasons"][0]
        self.assertEqual(rejection["signal_count"], 3)
        self.assertEqual(rejection["outcome_sample_count"], 1)
        self.assertEqual(rejection["outcome_trackable_count"], 2)
        self.assertEqual(rejection["coverage_rate_percent"], 33.3333)
        self.assertEqual(rejection["trackable_coverage_rate_percent"], 50.0)

    def test_all_research_horizons_are_supported(self) -> None:
        for horizon in ("1m", "3m", "5m", "15m", "30m", "60m"):
            report = build_observation_analysis(
                [observation(1, 10), observation(2, 20)],
                outcome_interval=horizon,
                minimum_samples=1,
            )
            self.assertEqual(report["outcome_interval"], horizon)


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
        self.assertEqual(saved["research_metrics"]["signal_count"], 4)
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
