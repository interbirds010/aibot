"""실행 중 N3와 새 계측의 저장·build 경계를 임시 경로에서 검증한다."""
from __future__ import annotations

import hashlib
from pathlib import Path
import queue
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from src.research import entry_telemetry as telemetry
from src.research import n3_shadow as n3


class EntryTelemetryReadinessIsolationTests(unittest.TestCase):
    def test_persistence_never_changes_frozen_n3_or_control_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            protected = {
                "data/n3_shadow/manifest.json": b'{"frozen":true}',
                "data/n3_shadow/closure.json": b'{"end_event_seq":11000}',
                "data/n3_shadow/entries/protected.json": b'{"legacy":false}',
                "data/paper_trades.json": b'{"next_event_seq":11001}',
            }
            for relative, content in protected.items():
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            with patch.object(telemetry, "_root", root), patch.object(
                telemetry, "_queue", queue.Queue(maxsize=16)
            ), patch.object(telemetry, "_provenance", {"session_id": "offline"}):
                capture = telemetry.begin_signal(
                    mint="offline-mint", route_type="B",
                    signal_detected_at="2026-10-03T00:00:00+00:00",
                )
                telemetry.finish(capture, outcome="BUY", trade_id="offline-trade")
                telemetry._persist(capture)
                telemetry._publish_health()
            for relative, content in protected.items():
                self.assertEqual((root / relative).read_bytes(), content)
            rows = list((root / "data/research/entry_telemetry/rows").glob("*.json"))
            self.assertEqual(len(rows), 1)

    def test_any_new_telemetry_source_changes_frozen_n3_build_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "src/research").mkdir(parents=True)
            result = subprocess.CompletedProcess([], 0, stdout="frozen-sha\n")
            with patch.object(n3.subprocess, "run", return_value=result):
                before = n3.build_identity(root)
                (root / "src/research/entry_telemetry.py").write_text(
                    "SCHEMA_VERSION = 1\n", encoding="utf-8"
                )
                after = n3.build_identity(root)
            self.assertEqual(before["git_sha"], after["git_sha"])
            self.assertNotEqual(before["source_digest"], after["source_digest"])
            self.assertIn("src/research/entry_telemetry.py", after["source_files"])

    def test_closed_cohort_recorder_does_not_read_manifest_or_add_snapshots(self):
        with tempfile.TemporaryDirectory() as directory:
            sidecar = Path(directory)
            (sidecar / "manifest.json").write_bytes(b"invalid frozen manifest sentinel")
            (sidecar / "closure.json").write_bytes(b"closed sentinel")
            with patch.object(n3, "load_manifest") as loader, patch.object(n3, "immutable") as writer:
                n3.persist_capture(sidecar, "new-epoch-trade", {}, {"git_sha": "new-build"})
                loader.assert_not_called()
                writer.assert_not_called()
            self.assertFalse((sidecar / "snapshots").exists())

    def test_frozen_eligibility_keeps_both_inclusive_boundaries(self):
        manifest = {"start_utc": "2026-10-03T01:04:59.425384+00:00", "start_event_seq": 10901}
        buy = {"signal_detected_at": manifest["start_utc"], "event_seq": 10901}
        self.assertTrue(n3.eligible(manifest, buy))
        self.assertFalse(n3.eligible(manifest, {**buy, "event_seq": 10900}))
        self.assertFalse(n3.eligible(manifest, {
            **buy, "signal_detected_at": "2026-10-03T01:04:59.425383+00:00"}))
        for legacy_seq in (9583, 9661):
            self.assertFalse(n3.eligible(manifest, {**buy, "event_seq": legacy_seq}))

    def test_provenance_has_session_but_no_activation_epoch_binding(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "src").mkdir()
            (root / "src/test.py").write_bytes(b"example = 1\n")
            result = subprocess.CompletedProcess([], 0, stdout="offline-sha\n")
            with patch.object(telemetry, "_root", root), patch.object(
                telemetry.subprocess, "run", return_value=result
            ), patch.object(telemetry, "_session", "offline-session"):
                document = telemetry._build_provenance({"TRADING_MODE": "paper"})
            self.assertEqual(document["session_id"], "offline-session")
            self.assertEqual(document["git_sha"], "offline-sha")
            self.assertEqual(document["telemetry_schema_version"], 1)
            self.assertEqual(document["config_scope"], "explicit non-secret allowlist only; not full environment")
            for missing in ("telemetry_epoch_id", "epoch_start_utc", "epoch_start_event_seq"):
                self.assertNotIn(missing, document)
            self.assertEqual(document["source_digest"], telemetry._digest({
                "src/test.py": hashlib.sha256(b"example = 1\n").hexdigest()}))


if __name__ == "__main__":
    unittest.main()
