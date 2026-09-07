from __future__ import annotations

import json
import unittest
from pathlib import Path

from src.research.alpha_regime_review import (
    CANDIDATES,
    MAX_TIME_BLOCKS,
    _utc_time_blocks,
    build_alpha_regime_review,
    expanding_performance,
    extreme_sensitivity,
    holdout_concentration,
    pairwise_overlap,
    rolling_performance,
)
from src.research.alpha_discovery import _prepare_rows


def momentum_event(index: int, outcome: float) -> dict:
    return {
        "observation_id": f"OBS-{index:04d}",
        "mint": f"MINT-{index:04d}",
        "signal_type": "MOMENTUM",
        "route_type": "B",
        "started_at_epoch": float(index * 86_400),
        "quote_status": "EXECUTABLE",
        "tracking_profile": "research_v1_60m",
        "safety_score": 96.0,
        "safety_metrics": {
            "developer_supply_percent": 1.0,
            "lp_locked_percent": 90.0,
        },
        "momentum_metrics": {
            "volume_m5_usd": 20_000.0,
            "buys_m5": 60,
            "sells_m5": 5,
            "net_buys_m5": 55,
            "buy_sell_ratio_m5": 12.0,
            "liquidity_usd": 15_000.0,
            "pair_age_seconds": 120.0,
            "unknown_whale_count": 3,
        },
        "entry_latency_ms": 5_000,
        "samples": [{"interval": "60m", "return_percent": outcome}],
    }


class AlphaRegimeMetricTests(unittest.TestCase):
    def test_rolling_and_expanding_windows_are_chronological(self) -> None:
        values = [float(index) for index in range(1, 121)]
        rolling = rolling_performance(
            values, 30, timestamps=[float(index) for index in range(120)]
        )

        self.assertIsNotNone(rolling)
        self.assertEqual(rolling["window_count"], 91)
        self.assertEqual(rolling["latest"][-1]["mean_return_percent"], 105.5)
        self.assertEqual(rolling["latest"][-1]["ending_epoch"], 119.0)
        self.assertEqual(rolling["positive_expectancy_window_rate_percent"], 100.0)
        self.assertEqual(rolling["profit_factor_above_one_window_rate_percent"], 100.0)
        self.assertEqual(
            [row["through"] for row in expanding_performance(values)],
            [50, 100, 120],
        )

    def test_extreme_sensitivity_removes_large_winners_and_bottom_loss(self) -> None:
        report = extreme_sensitivity([-100.0, -10.0, 0.0, 10.0, 1_000.0])

        self.assertEqual(report["mean_return_percent"], 180.0)
        self.assertEqual(report["top_1_removed_expectancy_percent"], -25.0)
        self.assertEqual(report["top_3_removed_expectancy_percent"], -36.6667)
        self.assertEqual(report["top_5_percent_removed_count"], 1)
        self.assertEqual(report["bottom_1_removed_expectancy_percent"], 250.0)

        losses_only = extreme_sensitivity([-20.0, -10.0])
        self.assertEqual(losses_only["top_1_removed_expectancy_percent"], -15.0)
        self.assertEqual(losses_only["top_5_percent_removed_count"], 0)

    def test_holdout_concentration_separates_gross_positive_and_net(self) -> None:
        raw = [
            momentum_event(index, outcome)
            for index, outcome in enumerate([0.0] * 8 + [-50.0, 100.0])
        ]
        prepared, _ = _prepare_rows(
            raw, maximum_rows=10_000, cohort="research_v1_60m"
        )

        result = holdout_concentration(prepared)

        self.assertEqual(result["holdout_total_return_points"], 50.0)
        self.assertEqual(result["top_1_winner_contribution_percent"], 200.0)
        self.assertEqual(result["top_1_gross_positive_contribution_percent"], 100.0)

    def test_overlap_reports_counts_without_mint_identifiers(self) -> None:
        overlap = pairwise_overlap({"a": {"m1", "m2"}, "b": {"m2", "m3"}})

        self.assertEqual(overlap[0]["overlap_count"], 1)
        self.assertEqual(overlap[0]["jaccard"], 0.3333)
        self.assertNotIn("m1", json.dumps(overlap))

    def test_time_blocks_are_bounded(self) -> None:
        raw = [momentum_event(index, float(index)) for index in range(100)]
        prepared, _ = _prepare_rows(
            raw, maximum_rows=10_000, cohort="research_v1_60m"
        )

        result = _utc_time_blocks(prepared)

        self.assertEqual(result["total_block_count"], 50)
        self.assertEqual(len(result["latest_blocks"]), MAX_TIME_BLOCKS)
        self.assertEqual(result["omitted_older_block_count"], 26)


class AlphaRegimeReviewTests(unittest.TestCase):
    def test_observation_workflow_runs_read_only_review(self) -> None:
        workflow = Path(".github/workflows/research-observation.yml").read_text(
            encoding="utf-8"
        )

        self.assertIn("python -m src.research.alpha_regime_review", workflow)

    def test_existing_candidate_definitions_and_unique_mint_chronology(self) -> None:
        rows = [momentum_event(index, -5.0 if index < 80 else 10.0) for index in range(100)]
        duplicate = momentum_event(999, 1_000.0)
        duplicate["mint"] = "MINT-0000"
        duplicate["started_at_epoch"] = 999_999.0
        rows.append(duplicate)
        report = build_alpha_regime_review(
            rows,
            {"candidate_counts": {
                "PROMISING": 0,
                "UNSTABLE": 26,
                "INSUFFICIENT_DATA": 168,
            }},
        )

        momentum = report["momentum"]
        self.assertEqual(momentum["inventory"]["total_eligible_rows"], 101)
        self.assertEqual(momentum["inventory"]["unique_mint_count"], 100)
        self.assertEqual(
            momentum["inventory"]["successful_60m_unique_mint_count"], 100
        )
        self.assertEqual(momentum["unique_sampled_count"], 100)
        self.assertEqual(momentum["quartiles"][0]["mean_return_percent"], -5.0)
        self.assertEqual(momentum["quartiles"][-1]["mean_return_percent"], 7.0)
        self.assertEqual(set(momentum["candidates"]), {spec.key for spec in CANDIDATES})
        for candidate in momentum["candidates"].values():
            self.assertEqual(candidate["sampled_mint_count"], 100)
            self.assertEqual(candidate["pseudo_holdouts"]["80_20"]["holdout"]["count"], 20)
            self.assertEqual(candidate["holdout_extreme_sensitivity"]["count"], 20)
        self.assertEqual(
            report["basis"]["production_alpha_candidate_counts"],
            {"PROMISING": 0, "UNSTABLE": 26, "INSUFFICIENT_DATA": 168},
        )
        self.assertEqual(report["multiple_testing"]["new_candidates_added"], 0)
        self.assertEqual(
            momentum["candidates"]["pair_age_below_300"]
            ["multiple_testing_diagnostic"]["hypothesis_count"],
            194,
        )
        self.assertTrue(report["read_only"])

    def test_output_has_no_raw_mints_or_paths(self) -> None:
        report = build_alpha_regime_review(
            [momentum_event(index, 1.0) for index in range(30)],
            {},
        )
        serialized = json.dumps(report)

        self.assertNotIn("MINT-", serialized)
        self.assertNotIn("/var/www", serialized)
        self.assertNotIn("RPC_URL", serialized)


if __name__ == "__main__":
    unittest.main()
