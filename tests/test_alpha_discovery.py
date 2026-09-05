from __future__ import annotations

import copy
import hashlib
import json
import math
import tempfile
import unittest
from pathlib import Path

from src.research.alpha_discovery import (
    assign_bucket,
    build_alpha_discovery,
    refresh_alpha_discovery,
)
from src import research_archive


def alpha_event(
    index: int,
    *,
    family: str = "SMART_MONEY",
    mint: str | None = None,
    return_60m: float | None = 10.0,
    whale_paid_sol: float = 2.5,
    volume_m5_usd: float = 20_000.0,
    ratio_m5: float = 2.0,
    pair_age_seconds: float = 2_000.0,
    quote_status: str = "EXECUTABLE",
    timestamp: float | None = None,
) -> dict:
    route = "A" if family == "SMART_MONEY" else "B"
    samples = [
        {"interval": "5m", "return_percent": 2.0},
        {"interval": "15m", "return_percent": -3.0},
        {"interval": "30m", "return_percent": 4.0},
    ]
    if return_60m is not None:
        samples.append({"interval": "60m", "return_percent": return_60m})
    return {
        "observation_id": f"OBS-{index:04d}",
        "mint": mint or f"MINT-{index:04d}",
        "signal_type": family,
        "route_type": route,
        "started_at_epoch": float(index if timestamp is None else timestamp),
        "quote_status": quote_status,
        "tracking_profile": "research_v1_60m",
        "discovery_metadata": {
            "whale_paid_lamports": whale_paid_sol * 1_000_000_000,
        },
        "safety_score": 90.0,
        "safety_metrics": {
            "developer_supply_percent": 4.0,
            "lp_locked_percent": 85.0,
            "liquidity_usd": 25_000.0,
        },
        "momentum_metrics": {
            "volume_m5_usd": volume_m5_usd,
            "buys_m5": 20,
            "sells_m5": 8,
            "net_buys_m5": 12,
            "buy_sell_ratio_m5": ratio_m5,
            "liquidity_usd": 25_000.0,
            "pair_age_seconds": pair_age_seconds,
            "unknown_whale_count": 3,
        },
        "dex_momentum_score": 92.0,
        "entry_price_impact_pct": 0.4,
        "exit_price_impact_pct": 0.6,
        "entry_latency_ms": 2_000,
        "samples": samples,
    }


def single_feature(report: dict, family: str, feature: str) -> dict:
    return next(
        item for item in report["families"][family]["single_features"]
        if item["feature"] == feature
    )


def feature_bucket(report: dict, family: str, feature: str, label: str) -> dict:
    feature_result = single_feature(report, family, feature)
    return next(
        item for item in feature_result["buckets"]
        if item["labels"] == {feature: label}
    )


class BucketAssignmentTests(unittest.TestCase):
    def test_fixed_boundaries_are_lower_inclusive(self) -> None:
        self.assertEqual(
            assign_bucket("SMART_MONEY", "whale_paid_sol", 0.999), "below_1"
        )
        self.assertEqual(
            assign_bucket("SMART_MONEY", "whale_paid_sol", 1.0),
            "1_to_below_1.5",
        )
        self.assertEqual(
            assign_bucket("SMART_MONEY", "whale_paid_sol", 5.0), "5_or_more"
        )
        self.assertEqual(
            assign_bucket("MOMENTUM", "unknown_whale_count", 3), "3"
        )
        self.assertEqual(
            assign_bucket("MOMENTUM", "unknown_whale_count", 4), "4_or_more"
        )

    def test_none_nonfinite_and_invalid_negative_values_are_excluded(self) -> None:
        self.assertIsNone(assign_bucket("SMART_MONEY", "safety_score", None))
        self.assertIsNone(assign_bucket("SMART_MONEY", "safety_score", math.nan))
        self.assertIsNone(assign_bucket("MOMENTUM", "volume_m5_usd", -1))
        self.assertEqual(
            assign_bucket("MOMENTUM", "net_buys_m5", -20), "below_5"
        )


class AlphaDiscoveryTests(unittest.TestCase):
    def test_legacy_and_research_v1_cohorts_are_isolated(self) -> None:
        research = alpha_event(2, mint="SAME", return_60m=20.0)
        legacy = alpha_event(1, mint="SAME", return_60m=-90.0)
        legacy["tracking_profile"] = "legacy_15m"
        report = build_alpha_discovery([legacy, research])
        summary = report["families"]["SMART_MONEY"]["summary"]
        self.assertEqual(summary["event_signal_count"], 1)
        self.assertEqual(summary["unique_mint_count"], 1)
        self.assertEqual(
            summary["unique_mint_primary_outcome"]["mean_return_percent"],
            20.0,
        )
        self.assertEqual(report["input_summary"]["excluded"]["outside_cohort"], 1)

    def test_event_and_first_signal_per_mint_views_are_distinct(self) -> None:
        first = alpha_event(1, mint="SAME", whale_paid_sol=1.2)
        later = alpha_event(2, mint="SAME", whale_paid_sol=3.5)
        other = alpha_event(3, mint="OTHER", whale_paid_sol=2.5)
        report = build_alpha_discovery([later, other, first])

        summary = report["families"]["SMART_MONEY"]["summary"]
        self.assertEqual(summary["event_signal_count"], 3)
        self.assertEqual(summary["unique_mint_count"], 2)
        self.assertEqual(summary["repeated_mint_event_count"], 1)
        later_bucket = feature_bucket(
            report, "SMART_MONEY", "whale_paid_sol", "3_to_below_5"
        )
        self.assertEqual(
            later_bucket["event_level"]["horizons"]["60m"]["overall"][
                "signal_count"
            ],
            1,
        )
        self.assertEqual(
            later_bucket["first_signal_per_mint"]["horizons"]["60m"]
            ["overall"]["signal_count"],
            0,
        )

    def test_chronological_split_is_deterministic_without_randomness(self) -> None:
        rows = [
            alpha_event(index, return_60m=(-10.0 if index <= 8 else 20.0))
            for index in range(1, 11)
        ]
        forward = build_alpha_discovery(rows)
        reverse = build_alpha_discovery(list(reversed(rows)))
        self.assertEqual(forward, reverse)

        bucket = feature_bucket(
            forward, "SMART_MONEY", "whale_paid_sol", "2_to_below_3"
        )["first_signal_per_mint"]
        primary = bucket["horizons"]["60m"]
        self.assertEqual(bucket["split"]["train_signal_count"], 8)
        self.assertEqual(bucket["split"]["holdout_signal_count"], 2)
        self.assertEqual(primary["train"]["expectancy_percent"], -10.0)
        self.assertEqual(primary["holdout"]["expectancy_percent"], 20.0)
        self.assertEqual(bucket["split"]["cross_split_mint_count"], 0)

    def test_same_timestamp_uses_stable_identity_tie_break(self) -> None:
        rows = [
            alpha_event(index, timestamp=100.0, return_60m=float(index))
            for index in range(1, 11)
        ]
        first = build_alpha_discovery(rows)
        second = build_alpha_discovery(list(reversed(rows)))
        self.assertEqual(first, second)

    def test_family_split_is_reused_in_sparse_bucket(self) -> None:
        rows = [
            alpha_event(index, whale_paid_sol=(2.5 if index < 8 else 5.5))
            for index in range(10)
        ]
        bucket = feature_bucket(
            build_alpha_discovery(rows),
            "SMART_MONEY",
            "whale_paid_sol",
            "5_or_more",
        )["event_level"]
        self.assertEqual(bucket["split"]["train_signal_count"], 0)
        self.assertEqual(bucket["split"]["holdout_signal_count"], 2)

    def test_outcome_fields_never_control_duplicate_identity_split(self) -> None:
        rows = [
            alpha_event(index, timestamp=100.0, return_60m=float(index))
            for index in range(10)
        ]
        for row in rows:
            row["observation_id"] = "DUPLICATE"
            row["mint"] = "SAME"
        report = build_alpha_discovery(rows)
        reverse = build_alpha_discovery(list(reversed(rows)))
        self.assertEqual(report, reverse)
        summary = report["families"]["SMART_MONEY"]["summary"]
        self.assertEqual(summary["event_split"]["train_count"], 0)
        self.assertEqual(summary["event_split"]["holdout_count"], 10)
        self.assertEqual(summary["unique_mint_count"], 0)
        self.assertEqual(summary["ambiguous_first_signal_mint_count"], 1)

    def test_small_bucket_is_insufficient(self) -> None:
        report = build_alpha_discovery([alpha_event(index) for index in range(10)])
        bucket = feature_bucket(
            report, "SMART_MONEY", "whale_paid_sol", "2_to_below_3"
        )
        self.assertEqual(bucket["status"], "INSUFFICIENT_DATA")
        self.assertIn("TOTAL_SAMPLED_BELOW_MINIMUM", bucket["status_reasons"])

    def test_positive_train_and_holdout_can_be_promising(self) -> None:
        rows = [
            alpha_event(index, return_60m=(10.0 if index % 2 == 0 else -5.0))
            for index in range(80)
        ]
        report = build_alpha_discovery(rows)
        bucket = feature_bucket(
            report, "SMART_MONEY", "whale_paid_sol", "2_to_below_3"
        )
        self.assertEqual(bucket["status"], "PROMISING")
        self.assertGreaterEqual(report["candidate_counts"]["PROMISING"], 1)

    def test_lossless_profit_factor_has_explicit_status_interpretation(self) -> None:
        rows = [alpha_event(index, return_60m=10.0) for index in range(80)]
        bucket = feature_bucket(
            build_alpha_discovery(rows),
            "SMART_MONEY",
            "whale_paid_sol",
            "2_to_below_3",
        )
        holdout = bucket["first_signal_per_mint"]["horizons"]["60m"][
            "holdout"
        ]
        self.assertEqual(bucket["status"], "PROMISING")
        self.assertIsNone(holdout["profit_factor"])
        self.assertTrue(holdout["profit_factor_above_one"])
        self.assertEqual(
            holdout["profit_factor_interpretation"], "POSITIVE_WITH_NO_LOSSES"
        )

    def test_negative_holdout_cannot_be_promising(self) -> None:
        rows = [
            alpha_event(
                index,
                return_60m=(
                    10.0 if index < 64 and index % 2 == 0
                    else -5.0
                ),
            )
            for index in range(80)
        ]
        bucket = feature_bucket(
            build_alpha_discovery(rows),
            "SMART_MONEY",
            "whale_paid_sol",
            "2_to_below_3",
        )
        self.assertEqual(bucket["status"], "UNSTABLE")
        self.assertIn(
            "HOLDOUT_EXPECTANCY_NOT_POSITIVE", bucket["status_reasons"]
        )

    def test_low_trackable_coverage_is_insufficient(self) -> None:
        rows = [
            alpha_event(
                index,
                return_60m=(10.0 if index >= 20 else None),
            )
            for index in range(80)
        ]
        bucket = feature_bucket(
            build_alpha_discovery(rows),
            "SMART_MONEY",
            "whale_paid_sol",
            "2_to_below_3",
        )
        metrics = bucket["first_signal_per_mint"]["horizons"]["60m"][
            "overall"
        ]
        self.assertEqual(metrics["sampled_count"], 60)
        self.assertEqual(metrics["trackable_coverage_rate_percent"], 75.0)
        self.assertEqual(bucket["status"], "INSUFFICIENT_DATA")
        self.assertIn(
            "TRACKABLE_COVERAGE_BELOW_MINIMUM", bucket["status_reasons"]
        )

    def test_missing_outcomes_remain_in_coverage_with_canonical_reasons(self) -> None:
        missed = alpha_event(1, return_60m=None)
        missed["samples"].append({
            "interval": "60m", "return_percent": None,
            "error": "HORIZON_MISSED raw detail",
        })
        no_route = alpha_event(
            2, return_60m=None, quote_status="NO_ROUTE",
        )
        metrics = build_alpha_discovery([missed, no_route])["families"][
            "SMART_MONEY"
        ]["summary"]["event_primary_outcome"]
        self.assertEqual(metrics["signal_count"], 2)
        self.assertEqual(metrics["sampled_count"], 0)
        self.assertEqual(metrics["missing_count"], 2)
        self.assertEqual(metrics["missing_reasons"], {
            "ENTRY_NO_ROUTE": 1,
            "HORIZON_MISSED": 1,
        })

    def test_positive_holdout_collapse_is_unstable(self) -> None:
        rows = [
            alpha_event(
                index,
                return_60m=(
                    (20.0 if index % 2 == 0 else -10.0)
                    if index < 64
                    else (2.0 if index % 2 == 0 else -1.0)
                ),
            )
            for index in range(80)
        ]
        bucket = feature_bucket(
            build_alpha_discovery(rows),
            "SMART_MONEY",
            "whale_paid_sol",
            "2_to_below_3",
        )
        self.assertEqual(bucket["status"], "UNSTABLE")
        self.assertEqual(bucket["holdout_expectancy_retention_ratio"], 0.1)
        self.assertIn("HOLDOUT_EXPECTANCY_COLLAPSE", bucket["status_reasons"])

    def test_predefined_interaction_cell_has_both_views_and_metrics(self) -> None:
        rows = [
            alpha_event(
                index,
                family="MOMENTUM",
                volume_m5_usd=20_000,
                ratio_m5=2.0,
            )
            for index in range(5)
        ]
        report = build_alpha_discovery(rows)
        interaction = next(
            item for item in report["families"]["MOMENTUM"]["interactions"]
            if item["features"] == ["volume_m5_usd", "buy_sell_ratio_m5"]
        )
        self.assertEqual(interaction["observed_cell_count"], 1)
        cell = interaction["cells"][0]
        self.assertEqual(cell["labels"], {
            "volume_m5_usd": "15000_to_below_25000",
            "buy_sell_ratio_m5": "1.8_to_below_2.5",
        })
        self.assertEqual(
            cell["event_level"]["horizons"]["60m"]["overall"][
                "sampled_count"
            ],
            5,
        )
        self.assertIn("first_signal_per_mint", cell)

    def test_later_horizon_is_not_used_in_earlier_excursion(self) -> None:
        row = alpha_event(1, return_60m=100.0)
        report = build_alpha_discovery([row])
        bucket = feature_bucket(
            report, "SMART_MONEY", "whale_paid_sol", "2_to_below_3"
        )
        horizons = bucket["event_level"]["horizons"]
        self.assertEqual(horizons["5m"]["overall"]["sampled_mfe_percent"], 2.0)
        self.assertEqual(horizons["60m"]["overall"]["sampled_mfe_percent"], 100.0)

    def test_untrackable_residual_sample_is_excluded_and_reported(self) -> None:
        row = alpha_event(1, return_60m=50.0, quote_status="PROCESSING_FAILED")
        metrics = build_alpha_discovery([row])["families"]["SMART_MONEY"][
            "summary"
        ]["event_primary_outcome"]
        self.assertEqual(metrics["trackable_count"], 0)
        self.assertEqual(metrics["sampled_count"], 0)
        self.assertEqual(metrics["inconsistent_untrackable_sample_count"], 1)
        self.assertEqual(metrics["raw_coverage_rate_percent"], 0.0)
        self.assertIsNone(metrics["trackable_coverage_rate_percent"])
        self.assertIsNone(metrics["mean_return_percent"])

    def test_unlisted_future_and_wallet_fields_never_become_features(self) -> None:
        rows = [alpha_event(index) for index in range(10)]
        baseline = build_alpha_discovery(rows)
        contaminated = copy.deepcopy(rows)
        for row in contaminated:
            row.update({
                "candidate_v2_early_failure": -99.0,
                "mfe_percent": 999.0,
                "mae_percent": -999.0,
                "paper_experiment_status": "CLOSED",
                "strategy_results": {"future": {"return_percent": 999.0}},
                "wallet_performance": {"roi": 999.0, "win_rate": 100.0},
            })
        self.assertEqual(baseline, build_alpha_discovery(contaminated))

    def test_malformed_rows_are_excluded_without_creating_zero_returns(self) -> None:
        valid = alpha_event(1, return_60m=None)
        report = build_alpha_discovery([
            "bad",
            {"mint": "NO-FAMILY", "started_at_epoch": 1},
            {"mint": "NO-TIME", "route_type": "A"},
            {"route_type": "B", "started_at_epoch": 2},
            valid,
        ])
        excluded = report["input_summary"]["excluded"]
        self.assertEqual(excluded["invalid_row"], 1)
        self.assertEqual(excluded["unknown_signal_family"], 1)
        self.assertEqual(excluded["missing_or_invalid_timestamp"], 1)
        self.assertEqual(excluded["missing_mint"], 1)
        primary = report["families"]["SMART_MONEY"]["summary"][
            "event_primary_outcome"
        ]
        self.assertEqual(primary["signal_count"], 1)
        self.assertEqual(primary["sampled_count"], 0)
        self.assertIsNone(primary["mean_return_percent"])

    def test_naive_iso_timestamp_is_not_os_timezone_dependent(self) -> None:
        row = alpha_event(1)
        row.pop("started_at_epoch")
        row["signal_detected_at"] = "2026-01-01T00:00:00"
        report = build_alpha_discovery([row])
        self.assertEqual(report["input_summary"]["analyzed_row_count"], 0)
        self.assertEqual(
            report["input_summary"]["excluded"]["missing_or_invalid_timestamp"],
            1,
        )

    def test_refresh_writes_only_output_and_preserves_source_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "signal_observations.json"
            output = root / "alpha_discovery.json"
            source.write_text(json.dumps({
                "schema_version": 4,
                "version": 7,
                "observations": [alpha_event(1)],
            }), encoding="utf-8")
            before = hashlib.sha256(source.read_bytes()).hexdigest()
            saved = refresh_alpha_discovery(
                observation_path=source, output_path=output,
            )
            after = hashlib.sha256(source.read_bytes()).hexdigest()
            self.assertEqual(before, after)
            self.assertTrue(output.exists())
            self.assertEqual(saved["input_summary"]["source_schema_version"], 4)
            self.assertEqual(saved["input_summary"]["source_version"], 7)
            self.assertEqual(saved["version"], 1)

    def test_refresh_reads_archive_without_operational_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "research_archive"
            output = root / "alpha_discovery.json"
            research_archive.archive_observation(
                {**alpha_event(1), "status": "COMPLETE"},
                archive_path=archive,
                metrics_path=root / "research_archive_metrics.json",
            )
            saved = refresh_alpha_discovery(
                observation_path=archive, output_path=output,
            )
            summary = saved["input_summary"]
            self.assertEqual(summary["source_type"], "research_archive")
            self.assertEqual(summary["source_row_count"], 1)
            self.assertEqual(summary["cohort"], "research_v1_60m")
            self.assertEqual(summary["cohort_row_count"], 1)

    def test_empty_archive_does_not_silently_fallback_to_legacy(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "research_archive"
            output = root / "alpha_discovery.json"
            saved = refresh_alpha_discovery(
                observation_path=archive, output_path=output,
            )
            self.assertEqual(saved["input_summary"]["source_type"], "research_archive")
            self.assertEqual(saved["input_summary"]["source_row_count"], 0)
            self.assertEqual(saved["input_summary"]["analyzed_row_count"], 0)

    def test_refresh_rejects_output_path_equal_to_observation_path(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "signal_observations.json"
            source.write_text(json.dumps({
                "schema_version": 4,
                "version": 1,
                "observations": [alpha_event(1)],
            }), encoding="utf-8")
            before = source.read_bytes()
            with self.assertRaisesRegex(ValueError, "output must differ"):
                refresh_alpha_discovery(
                    observation_path=source, output_path=source,
                )
            self.assertEqual(source.read_bytes(), before)

    def test_refresh_rejects_future_or_malformed_source_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "signal_observations.json"
            output = root / "alpha_discovery.json"
            for document, expected in (
                ({"schema_version": 999, "version": 1, "observations": []},
                 "schema is unsupported"),
                ({"schema_version": "4", "version": 1, "observations": []},
                 "schema_version is malformed"),
                ({"schema_version": 4, "version": "bad", "observations": []},
                 "version is malformed"),
            ):
                with self.subTest(expected=expected):
                    source.write_text(json.dumps(document), encoding="utf-8")
                    with self.assertRaisesRegex(RuntimeError, expected):
                        refresh_alpha_discovery(
                            observation_path=source, output_path=output,
                        )
                    self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
