from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from src import monitor, solana_rpc
from src.research import coverage_telemetry


class ResearchCoverageTelemetryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.original_path = coverage_telemetry.TELEMETRY_PATH
        coverage_telemetry.TELEMETRY_PATH = (
            Path(self.temporary.name) / "coverage.json"
        )
        coverage_telemetry.reset_pending_telemetry()

    def tearDown(self) -> None:
        coverage_telemetry.reset_pending_telemetry()
        coverage_telemetry.TELEMETRY_PATH = self.original_path
        self.temporary.cleanup()

    def test_event_and_unique_counts_remain_distinct_across_flushes(self) -> None:
        now = time.time()
        for mint in ("mint-a", "mint-a", "mint-b"):
            coverage_telemetry.record_funnel_stage(
                "poll_candidate_observed",
                mint=mint,
                family="MOMENTUM",
                timestamp=now,
            )
        coverage_telemetry.flush_coverage_telemetry(now_epoch=now)
        coverage_telemetry.reset_pending_telemetry()
        coverage_telemetry.record_funnel_stage(
            "poll_candidate_observed",
            mint="mint-c",
            family="B",
            timestamp=now,
        )

        report = coverage_telemetry.coverage_report()
        metric = report["families"]["MOMENTUM"]["stages"][
            "poll_candidate_observed"
        ]

        self.assertEqual(metric["event_count"], 4)
        self.assertEqual(metric["unique_mint_count_estimate"], 3)
        self.assertFalse(metric["unique_estimate_saturated"])
        persisted = coverage_telemetry.TELEMETRY_PATH.read_text("utf-8")
        self.assertNotIn("mint-a", persisted)
        self.assertNotIn("mint-b", persisted)

    def test_storage_caps_unique_bitmap_dimensions_and_old_buckets(self) -> None:
        latest = coverage_telemetry.BUCKET_SECONDS * (
            coverage_telemetry.MAX_BUCKETS + 3
        )
        for index in range(700):
            coverage_telemetry.record_funnel_stage(
                "candidate_considered",
                mint=f"mint-{index}",
                family="MOMENTUM",
                timestamp=latest,
            )
        combinations = [
            (method, provider, result)
            for method in sorted(coverage_telemetry.RPC_METHODS)
            for provider in sorted(coverage_telemetry.RPC_PROVIDERS)
            for result in sorted(coverage_telemetry.RPC_RESULTS)
        ]
        for index, (method, provider, result) in enumerate(
            combinations[: coverage_telemetry.MAX_RPC_DIMENSIONS_PER_BUCKET + 5]
        ):
            coverage_telemetry.record_confirmation_result(
                mint=f"rpc-{index}",
                family="MOMENTUM",
                method=method,
                provider=provider,
                result=result,
                timestamp=latest,
            )
        for index in range(coverage_telemetry.MAX_BUCKETS + 4):
            coverage_telemetry.record_funnel_stage(
                "analyzer_started",
                mint=f"bucket-{index}",
                family="MOMENTUM",
                timestamp=index * coverage_telemetry.BUCKET_SECONDS,
            )
        coverage_telemetry.flush_coverage_telemetry(now_epoch=latest)

        document = json.loads(
            coverage_telemetry.TELEMETRY_PATH.read_text("utf-8")
        )
        self.assertLessEqual(
            len(document["buckets"]), coverage_telemetry.MAX_BUCKETS
        )
        for bucket in document["buckets"]:
            self.assertLessEqual(
                len(bucket["rpc_confirmation"]),
                coverage_telemetry.MAX_RPC_DIMENSIONS_PER_BUCKET,
            )
        report = coverage_telemetry.coverage_report(now_epoch=latest)
        metric = report["families"]["MOMENTUM"]["stages"][
            "candidate_considered"
        ]
        self.assertGreater(metric["unique_mint_count_estimate"], 600)
        self.assertLess(metric["unique_mint_count_estimate"], 800)
        latest_bucket = next(
            bucket for bucket in document["buckets"]
            if bucket["bucket_start_epoch"] == latest
        )
        bitmap = latest_bucket["families"]["MOMENTUM"][
            "candidate_considered"
        ]["unique_bitmap_hex"]
        self.assertEqual(
            len(bitmap), coverage_telemetry.UNIQUE_BITMAP_HEX_LENGTH
        )

    def test_ratios_use_documented_unique_mint_denominators(self) -> None:
        for mint in ("a", "b"):
            coverage_telemetry.record_funnel_stage(
                "rpc_confirmation_started", mint=mint, family="MOMENTUM"
            )
            coverage_telemetry.record_funnel_stage(
                "candidate_considered", mint=mint, family="MOMENTUM"
            )
        coverage_telemetry.record_funnel_stage(
            "rpc_confirmation_succeeded", mint="a", family="MOMENTUM"
        )
        coverage_telemetry.record_funnel_stage(
            "observation_created", mint="a", family="MOMENTUM"
        )

        report = coverage_telemetry.coverage_report()
        ratios = report["families"]["MOMENTUM"][
            "unique_estimate_ratios_percent"
        ]

        self.assertEqual(ratios["confirmation_coverage"], 50.0)
        self.assertEqual(ratios["observation_creation_coverage"], 50.0)
        self.assertIn(
            "candidate_considered",
            report["ratio_denominators"]["observation_creation_coverage"],
        )

    def test_confirmation_wrapper_counts_success_before_observation(self) -> None:
        candidate = monitor.MomentumCandidate(
            "mint-success", "pair", 20_000.0, 40, 10, 20_000.0, 100.0
        )
        with patch.object(
            monitor,
            "confirm_unknown_whales",
            new=AsyncMock(return_value=[]),
        ):
            result = asyncio.run(
                monitor._confirm_unknown_whales_with_telemetry(
                    object(), "", candidate, set()
                )
            )

        self.assertEqual(result, [])
        report = coverage_telemetry.coverage_report()
        stages = report["families"]["MOMENTUM"]["stages"]
        self.assertEqual(stages["rpc_confirmation_started"]["event_count"], 1)
        self.assertEqual(stages["rpc_confirmation_succeeded"]["event_count"], 1)
        self.assertEqual(stages["rpc_confirmation_failed"]["event_count"], 0)

    def test_confirmation_wrapper_normalizes_rate_limit_failure(self) -> None:
        candidate = monitor.MomentumCandidate(
            "mint-failure", "pair", 20_000.0, 40, 10, 20_000.0, 100.0
        )
        failure = solana_rpc.SolanaRpcRateLimitExhaustedError(
            "getTransaction",
            2,
            last_provider="solana_public",
            last_category="RATE_LIMIT",
        )
        with patch.object(
            monitor,
            "confirm_unknown_whales",
            new=AsyncMock(side_effect=failure),
        ):
            with self.assertRaises(
                solana_rpc.SolanaRpcRateLimitExhaustedError
            ):
                asyncio.run(
                    monitor._confirm_unknown_whales_with_telemetry(
                        object(), "", candidate, set()
                    )
                )
        coverage_telemetry.flush_coverage_telemetry()
        document = json.loads(
            coverage_telemetry.TELEMETRY_PATH.read_text("utf-8")
        )
        dimensions = document["buckets"][0]["rpc_confirmation"]
        self.assertIn(
            "MOMENTUM|getTransaction|solana_public|rate_limit",
            dimensions,
        )

    def test_confirmation_failure_taxonomy_is_stable(self) -> None:
        timeout_error = solana_rpc.SolanaRpcExhaustedError(
            "getTransaction",
            1,
            last_provider="solana_public",
            last_category="TIMEOUT",
        )
        connection_error = solana_rpc.SolanaRpcExhaustedError(
            "getSignaturesForAddress",
            1,
            last_provider="helius",
            last_category="CONNECTION",
        )
        exhausted_error = solana_rpc.SolanaRpcExhaustedError(
            "getTransaction",
            0,
        )

        self.assertEqual(
            monitor._confirmation_failure_telemetry(timeout_error),
            ("getTransaction", "solana_public", "timeout"),
        )
        self.assertEqual(
            monitor._confirmation_failure_telemetry(connection_error),
            ("getSignaturesForAddress", "helius", "connection"),
        )
        self.assertEqual(
            monitor._confirmation_failure_telemetry(exhausted_error),
            ("getTransaction", "router", "exhausted"),
        )


if __name__ == "__main__":
    unittest.main()
