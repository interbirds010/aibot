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
        self.original_hourly_path = coverage_telemetry.HOURLY_TELEMETRY_PATH
        coverage_telemetry.TELEMETRY_PATH = (
            Path(self.temporary.name) / "coverage.json"
        )
        coverage_telemetry.HOURLY_TELEMETRY_PATH = (
            Path(self.temporary.name) / "coverage-hourly.json"
        )
        coverage_telemetry.reset_pending_telemetry()

    def tearDown(self) -> None:
        coverage_telemetry.reset_pending_telemetry()
        coverage_telemetry.TELEMETRY_PATH = self.original_path
        coverage_telemetry.HOURLY_TELEMETRY_PATH = self.original_hourly_path
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

    def test_legacy_raw_schema_migrates_without_count_loss(self) -> None:
        legacy = coverage_telemetry._empty_document()
        legacy["schema_version"] = 1
        legacy.pop("rpc_method_dimension_limit_per_bucket")
        bucket = coverage_telemetry._empty_bucket(0)
        bucket.pop("rpc_methods")
        bucket.pop("rpc_method_dimension_overflow_event_count")
        bucket.pop("heartbeat_seen")
        bucket["families"] = {
            "MOMENTUM": {
                "candidate_considered": {
                    "event_count": 2,
                    "unique_bitmap_hex": coverage_telemetry._bitmap_hex(
                        1 << coverage_telemetry._mint_bit("legacy-mint")
                    ),
                }
            }
        }
        legacy["buckets"] = [bucket]
        coverage_telemetry.state_store.atomic_write_json(
            coverage_telemetry.TELEMETRY_PATH, legacy
        )

        coverage_telemetry.flush_coverage_telemetry(now_epoch=1)

        migrated = json.loads(
            coverage_telemetry.TELEMETRY_PATH.read_text("utf-8")
        )
        self.assertEqual(
            migrated["schema_version"],
            coverage_telemetry.TELEMETRY_SCHEMA_VERSION,
        )
        metric = migrated["buckets"][0]["families"]["MOMENTUM"][
            "candidate_considered"
        ]
        self.assertEqual(metric["event_count"], 2)

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

    def test_four_quarters_roll_up_by_sum_and_bitmap_union(self) -> None:
        for index in range(4):
            timestamp = index * coverage_telemetry.BUCKET_SECONDS
            coverage_telemetry.record_funnel_stage(
                "candidate_considered",
                mint="same-mint",
                family="MOMENTUM",
                timestamp=timestamp,
            )
            coverage_telemetry.record_confirmation_result(
                mint="same-mint",
                family="MOMENTUM",
                method="getTransaction",
                provider="solana_public",
                result="rate_limit",
                timestamp=timestamp,
            )
            coverage_telemetry.record_rpc_method_metric(
                provider="solana_public",
                method="getTransaction",
                request_count=1,
                failure_count=1,
                rate_limit_count=1,
                latency_ms=100 + index,
                timestamp=timestamp,
            )
        coverage_telemetry.flush_coverage_telemetry(
            now_epoch=coverage_telemetry.HOUR_SECONDS
        )

        document = json.loads(
            coverage_telemetry.HOURLY_TELEMETRY_PATH.read_text("utf-8")
        )
        hour = next(
            item for item in document["hours"]
            if item["hour_start_epoch"] == 0
        )
        stage = hour["families"]["MOMENTUM"]["candidate_considered"]
        self.assertEqual(hour["status"], "COMPLETE")
        self.assertEqual(hour["source_bucket_count"], 4)
        self.assertEqual(stage["event_count"], 4)
        estimate = coverage_telemetry._unique_estimate(
            coverage_telemetry._bitmap_value(stage["unique_bitmap_hex"])
        )[0]
        self.assertEqual(estimate, 1)
        rpc = hour["rpc_methods"]["solana_public|getTransaction"]
        self.assertEqual(rpc["request_count"], 4)
        self.assertEqual(rpc["failure_count"], 4)
        self.assertEqual(rpc["rate_limit_count"], 4)
        self.assertEqual(rpc["latency_count"], 4)
        self.assertEqual(rpc["latency_sum_ms"], 406.0)
        self.assertEqual(rpc["latency_max_ms"], 103.0)
        self.assertEqual(rpc["latency_buckets"]["le_250_ms"], 4)
        raw_report = coverage_telemetry.coverage_report(
            now_epoch=coverage_telemetry.HOUR_SECONDS
        )
        raw_rpc = raw_report["rpc_methods"][0]
        self.assertEqual(raw_rpc["provider"], "solana_public")
        self.assertEqual(raw_rpc["request_count"], 4)
        self.assertEqual(raw_rpc["latency_average_ms"], 101.5)

    def test_partial_missing_and_current_hour_are_explicit(self) -> None:
        for index in range(3):
            coverage_telemetry.record_funnel_stage(
                "observation_created",
                mint=f"mint-{index}",
                family="MOMENTUM",
                timestamp=index * coverage_telemetry.BUCKET_SECONDS,
            )
        coverage_telemetry.flush_coverage_telemetry(
            now_epoch=coverage_telemetry.HOUR_SECONDS
        )

        document = json.loads(
            coverage_telemetry.HOURLY_TELEMETRY_PATH.read_text("utf-8")
        )
        indexed = {
            item["hour_start_epoch"]: item for item in document["hours"]
        }
        self.assertEqual(indexed[0]["status"], "PARTIAL")
        self.assertEqual(indexed[0]["source_bucket_count"], 3)
        self.assertEqual(len(indexed[0]["missing_source_bucket_starts"]), 1)
        self.assertEqual(indexed[-coverage_telemetry.HOUR_SECONDS]["status"], "MISSING")
        self.assertNotIn(coverage_telemetry.HOUR_SECONDS, indexed)

    def test_rebuild_is_idempotent_and_accepts_late_closed_bucket_data(self) -> None:
        for index in range(4):
            coverage_telemetry.record_funnel_stage(
                "analyzer_started",
                mint="mint-a",
                family="MOMENTUM",
                timestamp=index * coverage_telemetry.BUCKET_SECONDS,
            )
        coverage_telemetry.flush_coverage_telemetry(
            now_epoch=coverage_telemetry.HOUR_SECONDS
        )
        coverage_telemetry.reset_pending_telemetry()
        coverage_telemetry.flush_coverage_telemetry(
            now_epoch=coverage_telemetry.HOUR_SECONDS + 1
        )
        first = json.loads(
            coverage_telemetry.HOURLY_TELEMETRY_PATH.read_text("utf-8")
        )
        hour = next(x for x in first["hours"] if x["hour_start_epoch"] == 0)
        self.assertEqual(
            hour["families"]["MOMENTUM"]["analyzer_started"]["event_count"], 4
        )

        coverage_telemetry.record_funnel_stage(
            "analyzer_started",
            mint="mint-b",
            family="MOMENTUM",
            timestamp=3 * coverage_telemetry.BUCKET_SECONDS,
        )
        coverage_telemetry.flush_coverage_telemetry(
            now_epoch=coverage_telemetry.HOUR_SECONDS + 2
        )
        second = json.loads(
            coverage_telemetry.HOURLY_TELEMETRY_PATH.read_text("utf-8")
        )
        hour = next(x for x in second["hours"] if x["hour_start_epoch"] == 0)
        self.assertEqual(
            hour["families"]["MOMENTUM"]["analyzer_started"]["event_count"], 5
        )

    def test_hourly_write_failure_preserves_raw_and_retries(self) -> None:
        for index in range(4):
            coverage_telemetry.record_funnel_stage(
                "candidate_considered",
                mint=f"mint-{index}",
                family="MOMENTUM",
                timestamp=index * coverage_telemetry.BUCKET_SECONDS,
            )
        with patch.object(
            coverage_telemetry,
            "_refresh_hourly_rollups",
            side_effect=OSError("hourly unavailable"),
        ):
            with self.assertRaises(OSError):
                coverage_telemetry.flush_coverage_telemetry(
                    now_epoch=coverage_telemetry.HOUR_SECONDS
                )

        raw = json.loads(
            coverage_telemetry.TELEMETRY_PATH.read_text("utf-8")
        )
        closed = [
            bucket for bucket in raw["buckets"]
            if bucket["bucket_start_epoch"] < coverage_telemetry.HOUR_SECONDS
        ]
        self.assertEqual(len(closed), 4)
        coverage_telemetry.flush_coverage_telemetry(
            now_epoch=coverage_telemetry.HOUR_SECONDS + 1
        )
        hourly = json.loads(
            coverage_telemetry.HOURLY_TELEMETRY_PATH.read_text("utf-8")
        )
        hour = next(x for x in hourly["hours"] if x["hour_start_epoch"] == 0)
        self.assertEqual(hour["status"], "COMPLETE")
        self.assertEqual(
            hour["families"]["MOMENTUM"]["candidate_considered"]["event_count"],
            4,
        )

    def test_hourly_retention_and_review_eligibility_require_contiguous_hours(self) -> None:
        for hour_index in range(75):
            for quarter in range(4):
                coverage_telemetry.record_funnel_stage(
                    "prospective_eligible",
                    mint=f"mint-{hour_index}",
                    family="MOMENTUM",
                    timestamp=(
                        hour_index * coverage_telemetry.HOUR_SECONDS
                        + quarter * coverage_telemetry.BUCKET_SECONDS
                    ),
                )
            coverage_telemetry.flush_coverage_telemetry(
                now_epoch=(hour_index + 1) * coverage_telemetry.HOUR_SECONDS
            )
        document = json.loads(
            coverage_telemetry.HOURLY_TELEMETRY_PATH.read_text("utf-8")
        )
        self.assertEqual(
            len(document["hours"]), coverage_telemetry.MAX_HOURLY_ROLLUPS
        )
        status = coverage_telemetry.coverage_review_window_status(
            now_epoch=75 * coverage_telemetry.HOUR_SECONDS
        )
        self.assertTrue(status["eligible"])
        self.assertEqual(status["completed_hours"], 24)
        report = coverage_telemetry.coverage_review_report(
            now_epoch=75 * coverage_telemetry.HOUR_SECONDS
        )
        self.assertIsNotNone(report["summary"])
        self.assertEqual(
            report["summary"]["families"]["MOMENTUM"]["stages"][
                "prospective_eligible"
            ]["event_count"],
            96,
        )

        target = document["hours"][-2]
        target["status"] = "PARTIAL"
        target["complete"] = False
        coverage_telemetry.state_store.atomic_write_json(
            coverage_telemetry.HOURLY_TELEMETRY_PATH, document
        )
        status = coverage_telemetry.coverage_review_window_status(
            now_epoch=75 * coverage_telemetry.HOUR_SECONDS
        )
        self.assertFalse(status["eligible"])
        self.assertEqual(status["partial_hours"], 1)

        document["hours"] = [
            item for item in document["hours"]
            if item["hour_start_epoch"] != target["hour_start_epoch"]
        ]
        coverage_telemetry.state_store.atomic_write_json(
            coverage_telemetry.HOURLY_TELEMETRY_PATH, document
        )
        status = coverage_telemetry.coverage_review_window_status(
            now_epoch=75 * coverage_telemetry.HOUR_SECONDS
        )
        self.assertFalse(status["eligible"])
        self.assertEqual(status["missing_hours"], 1)

    def test_overflow_and_saturation_propagate_to_hour(self) -> None:
        raw = coverage_telemetry._empty_document()
        for quarter in range(4):
            start = quarter * coverage_telemetry.BUCKET_SECONDS
            bucket = coverage_telemetry._empty_bucket(start)
            bucket["heartbeat_seen"] = True
            bucket["families"] = {
                "MOMENTUM": {
                    "candidate_considered": {
                        "event_count": 1,
                        "unique_bitmap_hex": "f" * coverage_telemetry.UNIQUE_BITMAP_HEX_LENGTH,
                    }
                }
            }
            if quarter == 0:
                bucket["rpc_dimension_overflow_event_count"] = 2
            raw["buckets"].append(bucket)
        coverage_telemetry._refresh_hourly_rollups(
            raw, now_epoch=coverage_telemetry.HOUR_SECONDS
        )
        document = json.loads(
            coverage_telemetry.HOURLY_TELEMETRY_PATH.read_text("utf-8")
        )
        hour = next(x for x in document["hours"] if x["hour_start_epoch"] == 0)
        self.assertTrue(hour["saturation"])
        self.assertTrue(hour["overflow"])

    def test_review_report_is_withheld_until_window_is_complete(self) -> None:
        coverage_telemetry.record_funnel_stage(
            "candidate_considered",
            mint="mint",
            family="MOMENTUM",
            timestamp=0,
        )
        coverage_telemetry.flush_coverage_telemetry(
            now_epoch=coverage_telemetry.HOUR_SECONDS
        )
        report = coverage_telemetry.coverage_review_report(
            now_epoch=coverage_telemetry.HOUR_SECONDS
        )
        self.assertFalse(report["window_status"]["eligible"])
        self.assertEqual(
            report["window_status"]["eligibility_status"],
            "INCOMPLETE_WINDOW",
        )
        self.assertIsNone(report["summary"])

    def test_twenty_three_complete_hours_are_not_review_eligible(self) -> None:
        now = 100 * coverage_telemetry.HOUR_SECONDS
        document = coverage_telemetry._empty_hourly_document()
        for start in range(
            int(now - 23 * coverage_telemetry.HOUR_SECONDS),
            int(now),
            coverage_telemetry.HOUR_SECONDS,
        ):
            hour = coverage_telemetry._empty_hour(start)
            hour.update({
                "status": "COMPLETE",
                "complete": True,
                "source_bucket_count": 4,
                "missing_source_bucket_starts": [],
            })
            document["hours"].append(hour)
        coverage_telemetry.state_store.atomic_write_json(
            coverage_telemetry.HOURLY_TELEMETRY_PATH, document
        )

        status = coverage_telemetry.coverage_review_window_status(
            now_epoch=now
        )

        self.assertFalse(status["eligible"])
        self.assertEqual(status["completed_hours"], 23)
        self.assertEqual(status["missing_hours"], 1)

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
