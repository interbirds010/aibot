from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.research.memory_workload_diagnostic import (
    ledger_inventory,
    observation_allocator_profile,
)


class MemoryWorkloadDiagnosticTests(unittest.TestCase):
    def test_inventory_reports_sizes_without_exposing_path_or_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "private-ledger.json"
            path.write_text(
                json.dumps({"observations": [{"secret": "do-not-print"}]}),
                encoding="utf-8",
            )
            report = ledger_inventory(
                "signal_observations", path, "observations", {"observations": []}
            )

        self.assertEqual(report["row_count"], 1)
        self.assertGreater(report["largest_row_bytes"], 0)
        self.assertNotIn("path", report)
        self.assertNotIn("do-not-print", json.dumps(report))

    def test_allocator_profile_repeats_read_only_due_scan(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "signal_observations.json"
            path.write_text(
                json.dumps({
                    "observations": [{
                        "observation_id": "one",
                        "status": "PENDING",
                        "mint": "mint",
                        "started_at_epoch": 1,
                        "token_amount_raw": 1,
                        "samples": [],
                    }]
                }),
                encoding="utf-8",
            )
            report = observation_allocator_profile(path, repetitions=2)

        self.assertTrue(report["available"])
        self.assertEqual(report["repetitions"], 2)
        self.assertEqual(report["row_count"], 1)
        self.assertEqual(report["last_due_count"], 6)
        self.assertGreater(report["tracemalloc_peak_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
