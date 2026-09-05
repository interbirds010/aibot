from __future__ import annotations

import unittest

from src.research.collection_stability import (
    assess_alchemy_need,
    build_research_lifecycle,
    build_smart_money_source_funnels,
    canonical_smart_money_failure,
)


def row(
    started: float,
    *,
    status: str = "PENDING",
    quote_status: str = "EXECUTABLE",
    reasons: list[str] | None = None,
    samples: list[dict] | None = None,
    source: str = "solana_logs_subscribe",
    analysis_completed: bool = True,
) -> dict:
    return {
        "tracking_profile": "research_v1_60m",
        "started_at_epoch": started,
        "status": status,
        "decision_status": "APPROVED",
        "decision_reasons": reasons or [],
        "quote_status": quote_status,
        "analysis_completed_at": (
            "2026-09-05T00:00:00+00:00" if analysis_completed else None
        ),
        "signal_type": "SMART_MONEY",
        "route_type": "A",
        "discovery_metadata": {"discovery_source": source},
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

    def test_source_funnel_uses_window_deltas_and_canonical_failures(self) -> None:
        successful = row(
            110,
            samples=[{"interval": "60m", "return_percent": 4.0}],
        )
        failed = row(
            120,
            status="COMPLETE",
            quote_status="PROCESSING_FAILED",
            reasons=["RPC_ALL_PROVIDERS_EXHAUSTED", "raw https://secret.invalid"],
            analysis_completed=False,
        )
        legacy = row(
            130,
            status="COMPLETE",
            quote_status="NO_ROUTE",
            source="legacy-value",
        )
        metrics = {
            "wallet_ws_activity_started_at": 10.0,
            "wallet_ws_active_source": "solana_logs_subscribe",
            "monitor_wallet_count": 20,
            "wallet_ws_dex_log_match_counts_by_source": {
                "solana_logs_subscribe": 12,
            },
            "wallet_ws_transaction_restore_success_counts_by_source": {
                "solana_logs_subscribe": 8,
            },
            "wallet_ws_smart_money_candidate_counts_by_source": {
                "solana_logs_subscribe": 4,
            },
            "wallet_ws_analyzer_success_counts_by_source": {
                "solana_logs_subscribe": 3,
            },
            "wallet_ws_transaction_restore_failure_reasons_by_source": {
                "solana_logs_subscribe": {
                    "RPC_ALL_PROVIDERS_EXHAUSTED": 2,
                },
            },
        }
        baseline = {"websocket": {
            "wallet_ws_activity_started_at": 10.0,
            "wallet_ws_dex_log_match_counts_by_source": {
                "solana_logs_subscribe": 2,
            },
            "wallet_ws_transaction_restore_success_counts_by_source": {
                "solana_logs_subscribe": 3,
            },
            "wallet_ws_smart_money_candidate_counts_by_source": {
                "solana_logs_subscribe": 1,
            },
            "wallet_ws_analyzer_success_counts_by_source": {
                "solana_logs_subscribe": 1,
            },
            "wallet_ws_transaction_restore_failure_reasons_by_source": {
                "solana_logs_subscribe": {
                    "RPC_ALL_PROVIDERS_EXHAUSTED": 1,
                },
            },
        }}
        report = build_smart_money_source_funnels(
            [successful, failed, legacy],
            metrics,
            since_epoch=100,
            baseline=baseline,
        )
        public = report["sources"]["solana_logs_subscribe"]
        self.assertEqual(public["dex_matches"], 10)
        self.assertEqual(public["get_transaction_successful"], 5)
        self.assertEqual(public["smart_money_candidates"], 3)
        self.assertEqual(public["research_discovered"], 2)
        self.assertEqual(public["analyzer_success"], 1)
        self.assertEqual(public["analyzer_failure"], 1)
        self.assertEqual(public["successful_60m"], 1)
        self.assertEqual(
            public["get_transaction_failure_distribution"],
            {"RPC_ALL_PROVIDERS_EXHAUSTED": 1},
        )
        self.assertEqual(
            public["canonical_failure_distribution"],
            {"RPC_ALL_PROVIDERS_EXHAUSTED": 1},
        )
        self.assertEqual(public["dex_match_to_transaction_restore_percent"], 50.0)
        self.assertEqual(
            public["candidate_to_analyzer_success_percent"], 66.6667
        )
        unknown = report["sources"]["unknown_legacy"]
        self.assertEqual(unknown["research_discovered"], 1)
        self.assertEqual(
            unknown["canonical_failure_distribution"],
            {"ENTRY_NO_ROUTE": 1},
        )
        self.assertEqual(
            canonical_smart_money_failure(failed),
            "RPC_ALL_PROVIDERS_EXHAUSTED",
        )


if __name__ == "__main__":
    unittest.main()
