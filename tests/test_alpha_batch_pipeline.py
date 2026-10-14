"""운영자 개입 없이 닫힌 표본만 forensic까지 이어지는 실제 파일 경로를 검증한다."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import alpha_batch_pipeline as pipeline
from src.research import alpha_telemetry_batch as batch
from tests.test_alpha_telemetry_batch import accepted, streams, START


class PipelineTests(unittest.TestCase):
    def test_freeze_join_import_forensic_and_idempotence(self):
        with tempfile.TemporaryDirectory(prefix="pipeline 한글 ") as temporary:
            root, evidence, research = [Path(temporary) / name for name in ("runtime", "batch", "research")]
            batch.register_batch(evidence, accepted(root), start_seq=1, start_utc=START)
            raw = streams(20)
            with patch.object(batch, "load_streams", return_value=raw):
                report = pipeline.run_once(root, evidence, research, as_of="2026-10-16T00:00:00+00:00", execute=True)
                self.assertEqual(report["status"], "BATCH_PIPELINE_COMPLETED")
                self.assertEqual(report["completed_control_trades"], 20)
                self.assertTrue((research / "reports/BATCH_001.json").is_file())
                before = (research / "reports/BATCH_001.json").read_bytes()
                repeat = pipeline.run_once(root, evidence, research, as_of="2026-10-17T00:00:00+00:00", execute=True)
                self.assertEqual(repeat, report)
                self.assertEqual((research / "reports/BATCH_001.json").read_bytes(), before)
                self.assertFalse((research / "challenger_registry.json").exists())

    def test_pending_does_not_create_dataset_or_forensic(self):
        with tempfile.TemporaryDirectory() as temporary:
            root, evidence, research = [Path(temporary) / name for name in ("runtime", "batch", "research")]
            batch.register_batch(evidence, accepted(root), start_seq=1, start_utc=START)
            with patch.object(batch, "load_streams", return_value=streams(1)):
                report = pipeline.run_once(root, evidence, research, as_of="2026-10-16T00:00:00+00:00", execute=True)
            self.assertEqual(report["status"], "PENDING")
            self.assertFalse((evidence / batch.BATCH_ID / "dataset.json").exists())
            self.assertFalse(research.exists())

    def test_unregistered_batch_blocked(self):
        with tempfile.TemporaryDirectory() as temporary:
            paths = [Path(temporary) / name for name in ("runtime", "batch", "research")]
            with self.assertRaises((ValueError, FileNotFoundError)):
                pipeline.run_once(*paths, as_of="2026-10-16T00:00:00+00:00", execute=True)
