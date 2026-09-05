from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src import observation_tracker, research_archive


def terminal_row(identity: str, *, profile: str = "research_v1_60m") -> dict:
    return {
        "observation_id": identity,
        "status": "COMPLETE",
        "tracking_profile": profile,
        "signal_type": "SMART_MONEY",
        "route_type": "A",
        "mint": f"MINT-{identity}",
        "signal_detected_at": "2026-09-05T00:00:00+00:00",
        "started_at_epoch": 1_788_566_400.0,
        "decision_reasons": ["SAFE"],
        "samples": [
            {
                "interval": "60m",
                "return_percent": 5.0,
                "sample_lag_seconds": 3.0,
            }
        ],
    }


class ResearchArchiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.archive = self.root / "research_archive"
        self.metrics = self.root / "research_archive_metrics.json"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_append_preserves_complete_snapshot_and_identity(self) -> None:
        row = terminal_row("OBS-1")
        created, _ = research_archive.archive_observation(
            row, archive_path=self.archive, metrics_path=self.metrics,
        )
        self.assertTrue(created)
        path = research_archive.archive_record_path(
            "OBS-1", archive_path=self.archive,
        )
        saved = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(saved["archive_schema_version"], 1)
        self.assertEqual(saved["observation_id"], "OBS-1")
        self.assertEqual(saved["observation"], row)

    def test_duplicate_and_restart_replay_are_idempotent(self) -> None:
        row = terminal_row("OBS-1")
        first, _ = research_archive.archive_observation(
            row, archive_path=self.archive, metrics_path=self.metrics,
        )
        second, _ = research_archive.archive_observation(
            row, archive_path=self.archive, metrics_path=self.metrics,
        )
        loaded, stats = research_archive.load_research_archive(
            archive_path=self.archive,
        )
        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(len(loaded), 1)
        self.assertEqual(stats["archive_total_rows"], 1)
        metrics = json.loads(self.metrics.read_text(encoding="utf-8"))
        self.assertEqual(metrics["duplicate_prevented_count"], 1)

    def test_only_terminal_rows_are_archived(self) -> None:
        pending = terminal_row("PENDING")
        pending["status"] = "PENDING"
        result = research_archive.archive_terminal_rows(
            [pending, terminal_row("COMPLETE")],
            archive_path=self.archive,
            metrics_path=self.metrics,
        )
        self.assertEqual(result["eligible"], 1)
        rows, _ = research_archive.load_research_archive(
            archive_path=self.archive,
        )
        self.assertEqual([row["observation_id"] for row in rows], ["COMPLETE"])

    def test_backfill_is_idempotent_and_uses_research_shadow_only(self) -> None:
        shadow = terminal_row("SHADOW")
        shadow.pop("status")
        shadow["shadow_trade_id"] = shadow["observation_id"]
        legacy = terminal_row("LEGACY", profile="legacy_15m")
        legacy.pop("status")
        legacy["shadow_trade_id"] = legacy["observation_id"]
        first = research_archive.backfill_research_archive(
            [terminal_row("OPS")],
            shadow_rows=[shadow, legacy],
            archive_path=self.archive,
            metrics_path=self.metrics,
        )
        second = research_archive.backfill_research_archive(
            [terminal_row("OPS")],
            shadow_rows=[shadow, legacy],
            archive_path=self.archive,
            metrics_path=self.metrics,
        )
        self.assertEqual(first["operational_archived"], 1)
        self.assertEqual(first["shadow_archived"], 1)
        self.assertEqual(second["operational_archived"], 0)
        self.assertEqual(second["shadow_archived"], 0)
        rows, stats = research_archive.load_research_archive(
            archive_path=self.archive,
        )
        self.assertEqual({row["observation_id"] for row in rows}, {"OPS", "SHADOW"})
        self.assertEqual(stats["archive_total_rows"], 2)

    def test_archive_survives_operational_trim(self) -> None:
        observation_path = self.root / "signal_observations.json"
        old_path = observation_tracker.OBSERVATION_PATH
        observation_tracker.OBSERVATION_PATH = observation_path
        try:
            rows = [terminal_row("OLD"), terminal_row("NEW")]
            with patch.object(observation_tracker, "MAX_OBSERVATIONS", 1):
                retained = observation_tracker.archive_and_retain_observations(rows)
            self.assertEqual([row["observation_id"] for row in retained], ["NEW"])
            archived, stats = research_archive.load_research_archive(
                archive_path=self.archive,
            )
            self.assertEqual({row["observation_id"] for row in archived}, {"OLD", "NEW"})
            self.assertEqual(stats["archive_total_rows"], 2)
        finally:
            observation_tracker.OBSERVATION_PATH = old_path

    def test_archive_failure_keeps_terminal_row_outside_cap(self) -> None:
        observation_path = self.root / "signal_observations.json"
        old_path = observation_tracker.OBSERVATION_PATH
        observation_tracker.OBSERVATION_PATH = observation_path
        try:
            rows = [terminal_row("OLD"), terminal_row("NEW")]
            with patch.object(
                observation_tracker, "MAX_OBSERVATIONS", 1
            ), patch(
                "src.research_archive.archive_terminal_rows",
            ) as archive_rows:
                archive_rows.side_effect = lambda rows, **kwargs: {
                    "eligible": len(list(rows)), "archived": 0,
                    "duplicate": 0, "failed": 2,
                }
                retained = observation_tracker.archive_and_retain_observations(rows)
            self.assertEqual(len(retained), 2)
        finally:
            observation_tracker.OBSERVATION_PATH = old_path


if __name__ == "__main__":
    unittest.main()
