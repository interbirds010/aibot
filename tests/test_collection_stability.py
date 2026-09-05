from __future__ import annotations

import unittest

from src.research.collection_stability import (
    assess_alchemy_need,
    build_research_lifecycle,
)


def row(
    started: float,
    *,
    status: str = "PENDING",
    quote_status: str = "EXECUTABLE",
    reasons: list[str] | None = None,
    samples: list[dict] | None = None,
) -> dict:
    return {
        "tracking_profile": "research_v1_60m",
        "started_at_epoch": started,
        "status": status,
        "decision_status": "APPROVED",
        "decision_reasons": reasons or [],
        "quote_status": quote_status,
        "analysis_completed_at": "2026-09-05T00:00:00+00:00",
        "samples": samples or [],
    }


class CollectionStabilityTests(unittest.TestCase):
    def test_lifecycle_uses_scheduled_samples_without_reusing_horizons(self) -> None:
        report = build_research_lifecycle([
            row(100, samples=[{
                "interval": "1m",
                "return_percent": 2.0,
                "sample_lag_seconds": 4.0,
                "target_at_epoch": 160.0,
                "sampled_at_epoch": 164.0,
            }]),
            row(
                200,
                status="COMPLETE",
                samples=[{
                    "interval": "60m",
                    "return_percent": None,
                    "error": "HORIZON_MISSED",
                    "sample_lag_seconds": 65.0,
                }],
            ),
        ], since_epoch=50)

        self.assertEqual(report["signal_count"], 2)
        self.assertEqual(report["horizons"]["1m"]["sampled_count"], 1)
        self.assertEqual(report["horizons"]["60m"]["sampled_count"], 0)
        self.assertEqual(report["horizons"]["60m"]["sample_record_count"], 1)
        self.assertEqual(
            report["missing_60m_reasons"],
            {"HORIZON_MISSED": 1, "NOT_SAMPLED": 1},
        )
        self.assertEqual(report["completed_60m_count"], 1)

    def test_lifecycle_distinguishes_eligible_backlog_from_not_yet_due(self) -> None:
        report = build_research_lifecycle([
            row(100, samples=[{
                "interval": "1m",
                "return_percent": 2.0,
                "sample_lag_seconds": 4.0,
                "quote_latency_ms": 250.0,
            }]),
            row(200),
            row(500),
        ], now_epoch=400)

        horizon = report["horizons"]["1m"]
        self.assertEqual(horizon["target_eligible_count"], 2)
        self.assertEqual(horizon["successful_sample_count"], 1)
        self.assertEqual(horizon["eligible_missing_count"], 1)
        self.assertEqual(horizon["not_yet_due_count"], 1)
        self.assertEqual(horizon["due_backlog_count"], 1)
        self.assertEqual(horizon["oldest_due_lag_seconds"], 140.0)
        self.assertEqual(horizon["usable_rate_percent"], 50.0)
        self.assertEqual(horizon["not_sampled_count"], 1)
        self.assertEqual(report["due_backlog_depth"], 4)
        self.assertEqual(report["oldest_due_lag_seconds"], 140.0)
        self.assertEqual(
            report["overall_sampling"]["median_quote_latency_ms"],
            250.0,
        )

    def test_alchemy_gate_requires_sustained_failure_and_sufficient_window(self) -> None:
        lifecycle = {
            "signal_count": 50,
            "rpc_all_providers_exhausted_count": 13,
        }
        providers = {"solana_public": {
            "request_count_delta": 120,
            "window_success_rate_percent": 85.0,
        }}
        assessment = assess_alchemy_need(lifecycle, providers)
        self.assertTrue(assessment["recommend_alchemy_free"])
        self.assertFalse(assessment["automatic_provider_activation"])

        lifecycle["signal_count"] = 11
        assessment = assess_alchemy_need(lifecycle, providers)
        self.assertFalse(assessment["recommend_alchemy_free"])
        self.assertFalse(assessment["window_sufficient"])


if __name__ == "__main__":
    unittest.main()
