from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from src import failure_memory_diagnostics as diagnostics
from src import shadow_trade_ledger as shadow
from src import state_store


def completed_row(identity: str = "fixture-observation") -> dict:
    return {
        "observation_id": identity,
        "mint": "fixture-mint-not-telemetry",
        "source_signature": "fixture-signature-not-telemetry",
        "route_type": "B",
        "status": "COMPLETE",
        "quote_status": "EXECUTABLE",
        "entry_cost_lamports": 1000,
        "token_amount_raw": 500,
        "samples": [
            {"interval": interval, "return_percent": 20.0}
            for interval in shadow.SHADOW_HORIZONS
        ],
    }


class ShadowFailureMemoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.path = self.root / "shadow_trades.json"
        self.recorder = diagnostics._Recorder(self.root / "diagnostics.json")
        self.marks: list[tuple[str, dict]] = []
        original_mark = self.recorder.phase_mark

        def mark(identity, stage, values):
            self.marks.append((stage, dict(values)))
            return original_mark(identity, stage, values)

        for patcher in (
            patch.object(shadow, "SHADOW_TRADE_PATH", self.path),
            patch.object(shadow, "maybe_trim_allocator", return_value=False),
            patch.object(diagnostics, "_recorder", self.recorder),
            patch.object(self.recorder, "phase_mark", side_effect=mark),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_record_observes_streaming_boundaries_and_releases_graph(self) -> None:
        self.assertTrue(shadow.record_completed_shadow_trade(completed_row()))
        stages = [stage for stage, _ in self.marks]
        self.assertEqual(stages, [
            "ledger_loaded", "ledger_mutated", "tmp_write_start", "serialize",
            "serialized", "flushed", "file_buffer_closed", "buffer_released",
            "rename_start", "replaced", "ledger_released", "exit",
        ])
        counts = dict(self.marks)
        self.assertEqual(counts["ledger_loaded"]["shadow_trade_count"], 0)
        self.assertEqual(counts["ledger_mutated"]["shadow_trade_count"], 1)
        self.assertEqual(counts["ledger_mutated"]["shadow_position_count"], 0)
        self.assertEqual(counts["ledger_mutated"]["shadow_event_count"], 0)
        self.assertEqual(counts["serialized"]["serialized_size_bytes"], self.path.stat().st_size)
        self.assertFalse(counts["buffer_released"]["file_buffer_live"])
        self.assertFalse(counts["ledger_released"]["ledger_document_live"])
        self.assertEqual(self.recorder.active, {})
        self.assertNotIn("fixture-mint", repr(self.marks))
        self.assertNotIn("fixture-signature", repr(self.marks))
        for _, values in self.marks:
            self.assertTrue(all(type(value) in (bool, int, float) for value in values.values()))

    def test_loaded_size_and_backfill_duplicate_behavior(self) -> None:
        self.assertTrue(shadow.record_completed_shadow_trade(completed_row()))
        previous_bytes = self.path.stat().st_size
        self.marks.clear()
        self.assertEqual(shadow.backfill_completed_shadow_trades(
            [completed_row(), completed_row("second")], existing_ids=set(),
        ), 1)
        counts = dict(self.marks)
        self.assertEqual(counts["ledger_loaded"]["input_file_size_bytes"], previous_bytes)
        self.assertEqual(counts["ledger_loaded"]["shadow_trade_count"], 1)
        self.assertEqual(counts["ledger_mutated"]["shadow_trade_count"], 2)
        document = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertEqual(document["version"], 2)
        self.assertEqual([trade["shadow_trade_id"] for trade in document["trades"]],
                         ["fixture-observation", "second"])

    def test_serialization_failure_preserves_canonical_state_and_phase_cleanup(self) -> None:
        self.assertTrue(shadow.record_completed_shadow_trade(completed_row()))
        previous = self.path.read_bytes()
        self.marks.clear()
        with patch.object(state_store.json, "dump", side_effect=OSError("fixture failure")):
            with self.assertRaises(OSError):
                shadow.record_completed_shadow_trade(completed_row("second"))
        self.assertEqual(self.path.read_bytes(), previous)
        self.assertEqual(list(self.root.glob(".shadow_trades.json.*.tmp")), [])
        stages = [stage for stage, _ in self.marks]
        self.assertIn("serialize", stages)
        self.assertIn("file_buffer_closed", stages)
        self.assertIn("buffer_released", stages)
        self.assertEqual(stages[-1], "failed")
        self.assertNotIn("replaced", stages)
        self.assertNotIn("ledger_released", stages)
        self.assertEqual(self.recorder.active, {})
        self.assertIsNone(diagnostics._current_phase.get())

    def test_diagnostic_callback_failure_leaves_identical_ledger(self) -> None:
        instant = datetime(2026, 10, 1, tzinfo=timezone.utc)
        baseline_path = self.root / "baseline" / "shadow_trades.json"
        with patch.object(shadow, "datetime") as clock:
            clock.now.return_value = instant
            with patch.object(shadow, "SHADOW_TRADE_PATH", baseline_path):
                self.assertTrue(shadow.record_completed_shadow_trade(completed_row()))
            with (
                patch.object(diagnostics, "mark_current_phase", side_effect=RuntimeError("diagnostic")),
                patch.object(self.recorder, "phase_mark", side_effect=RuntimeError("diagnostic")),
            ):
                self.assertTrue(shadow.record_completed_shadow_trade(completed_row()))
        self.assertEqual(self.path.read_bytes(), baseline_path.read_bytes())
        self.assertEqual(self.recorder.active, {})
        document = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertEqual(document["schema_version"], 2)
        self.assertEqual(document["version"], 1)
        trade = document["trades"][0]
        self.assertEqual(trade["entry_cost_lamports"], 1000)
        self.assertEqual(trade["entry_event"], "SHADOW_BUY")
        self.assertEqual(trade["exit_event"], "SHADOW_SELL")

    def test_migration_return_does_not_claim_document_release(self) -> None:
        document = shadow.ensure_shadow_trades_migrated()
        self.assertEqual(document["trades"], [])
        self.assertIn("ledger_loaded", [stage for stage, _ in self.marks])
        self.assertNotIn("ledger_released", [stage for stage, _ in self.marks])
        self.assertEqual(self.recorder.active, {})

    def test_identity_projection_tracks_returned_graph_until_actual_release(self) -> None:
        self.assertTrue(shadow.record_completed_shadow_trade(completed_row()))
        self.marks.clear()
        self.assertEqual(shadow.current_shadow_trade_ids(), {"fixture-observation"})
        stages = [stage for stage, _ in self.marks]
        self.assertIn("compact_projected", stages)
        projected = dict(self.marks)["compact_projected"]
        self.assertEqual(projected["shadow_identity_count"], 1)
        self.assertTrue(projected["ledger_document_live"])
        self.assertEqual(stages[-2:], ["ledger_released", "exit"])
        self.assertEqual(self.recorder.active, {})

    def test_shadow_read_cannot_overwrite_unrelated_active_phase(self) -> None:
        state_store.atomic_write_json(self.path, {"trades": []})
        self.marks.clear()
        with diagnostics.diagnostic_phase("candidate_fetch"):
            state_store.read_json(self.path, {})
            state_store.atomic_write_json(self.path, {"trades": []})
        self.assertEqual([stage for stage, _ in self.marks], ["exit"])

    def test_nonshadow_atomic_writes_preserve_existing_observer_contract(self) -> None:
        stages: list[str] = []
        with patch.object(diagnostics, "mark_current_phase") as marker:
            state_store.atomic_write_json(
                self.root / "other.json", {"value": 1},
                lifecycle_observer=lambda stage, _size: stages.append(stage),
            )
        self.assertEqual(stages, ["serialize", "serialized", "flushed", "replaced"])
        marker.assert_not_called()


if __name__ == "__main__":
    unittest.main()
