"""실제 분리 stream을 shadow 입력으로 투영하며 outcome을 사용하지 않는다."""
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import alpha_shadow_stream as shadow
from src.research import alpha_telemetry_batch as batch
from src.research import accelerated_alpha as core
from tests.test_alpha_telemetry_batch import streams, seal, accepted, START, MARKER


class ShadowStreamTests(unittest.TestCase):
    def test_real_schema_projection_never_has_outcome(self):
        raw = streams()
        result = shadow.project(raw["predictors"][0], raw["receipts"][0])
        self.assertEqual(result["control_decision"], "BUY")
        self.assertEqual(result["features"]["route_type"]["value"], "B")
        self.assertNotIn("realized_pnl_lamports", str(result))
        self.assertNotIn("outcome", result)

    def test_mismatch_receipt_rejected(self):
        raw = streams()
        raw["receipts"][0]["identity"]["mint"] = "other"
        raw["receipts"][0] = seal(raw["receipts"][0])
        with self.assertRaisesRegex(ValueError, "conflict"):
            shadow.project(raw["predictors"][0], raw["receipts"][0])

    def test_consumer_reads_only_predictors_and_receipts(self):
        raw = streams()
        with tempfile.TemporaryDirectory() as temporary:
            root, output = Path(temporary) / "runtime", Path(temporary) / "shadow"
            def iterator(root_arg, epoch_id, name):
                self.assertNotEqual(name, "outcomes")
                return iter(raw[name])
            with patch.object(shadow.epoch, "require_epoch", return_value=MARKER), \
                    patch.object(shadow.offline, "iter_stream", side_effect=iterator), \
                    patch.object(shadow.core, "record_shadow_predictor", return_value={}) as recorder:
                report = shadow.consume(root, output, config={})
                self.assertEqual(report["buy_predictors_consumed"], 1)
                self.assertFalse(report["outcome_stream_read"])
                recorder.assert_called_once()

    def test_telemetry_import_preserves_signal_day_at_midnight(self):
        from datetime import timedelta, timezone
        raw = streams(7, days=True, completed=False)
        for receipt in raw["receipts"]:
            stamp = receipt["execution_receipt"]["paper_buy_created_at"]
            stamp["wall_utc"] = (core.utc(stamp["wall_utc"]) + timedelta(hours=16)).isoformat()
            receipt.update(seal(receipt))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "runtime"
            source = Path(temporary) / "source"
            output = Path(temporary) / "research"
            batch.register_batch(source, accepted(root), start_seq=1, start_utc=START)
            as_of = (core.utc(START) + timedelta(days=8)).isoformat()
            with patch.object(batch, "load_streams", return_value=raw):
                frozen = batch.freeze_batch(root, source, as_of=as_of)
            imported = core.import_frozen_telemetry_batch(source / batch.BATCH_ID / "dataset.json", output)
            expected = sorted({core.utc(r["identity"]["signal_detected_at"]).astimezone(timezone(timedelta(hours=9))).date().isoformat() for r in raw["predictors"]})
            self.assertEqual(imported["active_kst_days"], expected)
            self.assertEqual(frozen["completed_control_trades"], 0)
