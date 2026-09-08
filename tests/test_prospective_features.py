from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from src import observation_tracker, research_archive
from src.research.prospective_features import (
    FEATURE_COLLECTION_SCHEMA_VERSION,
    MAX_PRE_SIGNAL_SNAPSHOTS,
    MAX_TRACKED_MINT_PAIRS,
    MOMENTUM_COLLECTOR_VERSION,
    SNAPSHOT_FIELDS,
    MomentumSnapshotStore,
    normalize_prospective_feature_collection,
    prospective_feature_collection_eligible,
)


class MomentumSnapshotStoreTests(unittest.TestCase):
    def snapshot(self, timestamp: float, marker: int = 1) -> dict[str, object]:
        return {
            "snapshot_at_epoch": timestamp,
            "volume_m5_usd": 10_000 + marker,
            "buys_m5": 20 + marker,
            "sells_m5": 10,
            "liquidity_usd": 15_000,
            "price_usd": 0.001 * marker,
        }

    def test_signal_future_snapshot_is_excluded_and_projection_is_bounded(self) -> None:
        normalized = normalize_prospective_feature_collection(
            {
                "schema_version": FEATURE_COLLECTION_SCHEMA_VERSION,
                "collector_version": MOMENTUM_COLLECTOR_VERSION,
                "pre_signal_snapshots": [
                    {
                        **self.snapshot(100),
                        "raw_response": {"must_not": "survive"},
                    },
                    self.snapshot(200, 2),
                ],
            },
            signal_timestamp=150,
        )
        self.assertEqual(normalized["snapshot_count"], 1)
        snapshot = normalized["pre_signal_snapshots"][0]
        self.assertEqual(set(snapshot), SNAPSHOT_FIELDS)
        self.assertNotIn("raw_response", json.dumps(normalized))

    def test_per_series_and_global_counts_are_bounded(self) -> None:
        store = MomentumSnapshotStore()
        for index in range(MAX_PRE_SIGNAL_SNAPSHOTS + 4):
            self.assertTrue(store.record(
                mint="MINT",
                pair_address="PAIR",
                **self.snapshot(60 * index, index + 1),
            ))
        self.assertEqual(store.series_count, 1)
        self.assertEqual(store.snapshot_count, MAX_PRE_SIGNAL_SNAPSHOTS)

        for index in range(MAX_TRACKED_MINT_PAIRS + 4):
            self.assertTrue(store.record(
                mint=f"MINT-{index}",
                pair_address=f"PAIR-{index}",
                **self.snapshot(10_000 + index, index + 1),
            ))
        self.assertEqual(store.series_count, MAX_TRACKED_MINT_PAIRS)
        self.assertLessEqual(
            store.snapshot_count,
            MAX_TRACKED_MINT_PAIRS * MAX_PRE_SIGNAL_SNAPSHOTS,
        )

    def test_minute_bucket_replaces_and_ttl_evicts(self) -> None:
        store = MomentumSnapshotStore()
        self.assertTrue(store.record(
            mint="MINT", pair_address="PAIR", **self.snapshot(100)
        ))
        self.assertTrue(store.record(
            mint="MINT", pair_address="PAIR", **self.snapshot(110, 2)
        ))
        self.assertEqual(store.snapshot_count, 1)
        collection = store.collection(
            mint="MINT", pair_address="PAIR", signal_timestamp=111
        )
        self.assertEqual(
            collection["pre_signal_snapshots"][0]["snapshot_at_epoch"],
            110.0,
        )
        self.assertTrue(store.record(
            mint="OTHER", pair_address="PAIR", **self.snapshot(1_011)
        ))
        self.assertEqual(store.series_count, 1)
        self.assertEqual(store.snapshot_count, 1)

    def test_old_row_is_not_prospectively_eligible(self) -> None:
        self.assertFalse(prospective_feature_collection_eligible({
            "tracking_profile": "research_v1_60m",
            "signal_detected_at": "2026-09-08T00:00:00Z",
        }))

    def test_provenance_and_eligibility_are_deterministic(self) -> None:
        store = MomentumSnapshotStore()
        for timestamp in (100, 160):
            self.assertTrue(store.record(
                mint="MINT",
                pair_address="PAIR",
                **self.snapshot(timestamp, int(timestamp)),
            ))
        collection = store.collection(
            mint="MINT", pair_address="PAIR", signal_timestamp=170
        )
        self.assertEqual(collection["schema_version"], 1)
        self.assertEqual(
            collection["collector_version"], MOMENTUM_COLLECTOR_VERSION
        )
        self.assertEqual(collection["prospective_collection_start"],
                         "1970-01-01T00:01:40Z")
        self.assertEqual(collection["snapshot_count"], 2)
        self.assertTrue(prospective_feature_collection_eligible({
            "signal_detected_at": 170,
            "prospective_feature_collection": collection,
        }))


class ProspectiveObservationRoundTripTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.original_path = observation_tracker.OBSERVATION_PATH
        observation_tracker.OBSERVATION_PATH = self.root / "observations.json"

    def tearDown(self) -> None:
        observation_tracker.OBSERVATION_PATH = self.original_path
        self.temporary.cleanup()

    def test_new_row_preserves_copy_gap_and_projected_history(self) -> None:
        collection = {
            "schema_version": FEATURE_COLLECTION_SCHEMA_VERSION,
            "collector_version": MOMENTUM_COLLECTOR_VERSION,
            "pre_signal_snapshots": [
                {
                    "snapshot_at_epoch": 100,
                    "volume_m5_usd": 20_000,
                    "buys_m5": 30,
                    "sells_m5": 10,
                    "liquidity_usd": 12_000,
                    "price_usd": 0.001,
                    "raw_pair": {"not": "stored"},
                },
                {
                    "snapshot_at_epoch": 200,
                    "volume_m5_usd": 25_000,
                    "buys_m5": 40,
                    "sells_m5": 10,
                    "liquidity_usd": 13_000,
                    "price_usd": 0.002,
                },
            ],
        }
        signal = "1970-01-01T00:02:30Z"
        self.assertTrue(asyncio.run(observation_tracker.record_observation(
            mint="MINT",
            route_type="B",
            source_wallet="WALLET",
            source_signature="SIGNATURE",
            safety_score=100,
            entry_cost_lamports=1_000,
            token_amount_raw=500,
            token_decimals=6,
            entry_price_impact_pct=0.1,
            exit_price_impact_pct=0.2,
            expected_slippage_bps=100,
            dex_momentum_score=95,
            signal_detected_at=signal,
            analysis_completed_at="1970-01-01T00:02:31Z",
            entry_quote_at="1970-01-01T00:02:32Z",
            entry_latency_ms=2_000,
            copy_price_gap_pct=-1.23456789,
            prospective_feature_collection=collection,
        )))
        row = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        self.assertEqual(row["copy_price_gap_pct"], -1.23456789)
        saved_collection = row["prospective_feature_collection"]
        self.assertEqual(saved_collection["snapshot_count"], 1)
        self.assertNotIn("raw_pair", json.dumps(saved_collection))

        archive_path = self.root / "archive"
        metrics_path = self.root / "archive_metrics.json"
        row["status"] = "COMPLETE"
        created, _ = research_archive.archive_observation(
            row, archive_path=archive_path, metrics_path=metrics_path
        )
        self.assertTrue(created)
        loaded, _ = research_archive.load_research_archive(
            archive_path=archive_path,
            tracking_profile="research_v1_60m",
        )
        self.assertEqual(
            loaded[0]["prospective_feature_collection"], saved_collection
        )
        self.assertEqual(loaded[0]["copy_price_gap_pct"], -1.23456789)


if __name__ == "__main__":
    unittest.main()
