"""연구 refresh는 명시적 외부 출력만 쓰고 열린 batch를 평가하지 않는다."""
from datetime import datetime, timedelta, timezone
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import accelerated_alpha_research as cli


def rows(count):
    result = []
    start = datetime(2026, 10, 7, tzinfo=timezone.utc)
    for i in range(count):
        stamp = (start + timedelta(minutes=i)).isoformat()
        result.append({"position_id": f"p{i}", "buy_event_seq": i + 1,
                       "signal_timestamp": stamp, "entry_decision_timestamp": stamp,
                       "closed_at": (start + timedelta(minutes=i, seconds=30)).isoformat(),
                       "entry_cost_lamports": 1000000, "realized_pnl_lamports": 100 if i % 2 else -100,
                       "features": {"liquidity_usd": {"value": 10000 + i, "observed_at": stamp,
                                                       "source": "pre_entry_test"}}, "forensic": {}})
    return result


class AcceleratedResearchCliTests(unittest.TestCase):
    def telemetry_fixture(self, base):
        from tests.test_alpha_telemetry_batch import accepted, streams, START
        from src.research import alpha_telemetry_batch as telemetry
        runtime = base / "runtime"
        runtime.mkdir()
        source = base / "telemetry evidence"
        telemetry.register_batch(source, accepted(runtime), start_seq=1, start_utc=START)
        physical = streams(7, days=True)
        physical["predictors"].append(streams(8, decision="RPC_SKIPPED")["predictors"][-1])
        with patch.object(telemetry, "load_streams", return_value=physical):
            frozen = telemetry.freeze_batch(runtime, source, as_of="2026-10-22T00:00:00+00:00")
        return source / "ALPHA_BATCH_001" / "dataset.json", frozen

    def test_import_seventh_day_contract_with_six_completed_and_open_position(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            source, frozen = self.telemetry_fixture(base)
            self.assertEqual(frozen["completed_control_trades"], 6)
            self.assertEqual(frozen["active_kst_days"], 7)
            original = source.read_bytes()
            output = base / "new research namespace"
            stdout = io.StringIO()
            with patch.object(cli.core, "freeze_batches", side_effect=AssertionError("completed-only batch closure must not run")), redirect_stdout(stdout):
                self.assertEqual(cli.main(["import-telemetry-batch", "--input", str(source), "--output", str(output)]), 0)
            result = json.loads(stdout.getvalue())
            self.assertEqual(result["completed_input"], 6)
            batch = cli.core.read_batch(output / "batches" / "BATCH_001.json")
            self.assertEqual(batch["source_batch_id"], "ALPHA_BATCH_001")
            self.assertEqual(batch["source_sample_contract"], frozen["sample_contract"])
            self.assertEqual(batch["frozen_at_utc"], frozen["end_utc"])
            self.assertGreaterEqual(batch["discovery_endpoint"]["max_buy_event_seq"], frozen["end_event_seq"])
            self.assertEqual(batch["source_population"]["open_or_uncompleted"], 1)
            self.assertEqual(batch["source_population"]["decision_outcomes"]["RPC_SKIPPED"], 1)
            report = cli.n3.read_immutable(output / "reports" / "BATCH_001.json")["report"]
            self.assertEqual(report["population"]["completed"], 6)
            self.assertEqual(len(report["trades"]), 6)
            self.assertLessEqual(len(report["candidate_generation"]["proposals"]), 10)
            candidate = {"candidate_id": "six-v1", "version": 1, "kind": "negative_filter",
                         "rationale": "작은 표본의 가설", "expected_mechanism": "미검증 손실 회피 가설",
                         "rule": {"feature": "liquidity_usd", "op": "lt", "threshold": 20000},
                         "creation_batch": "BATCH_001", "start_event_seq": frozen["end_event_seq"] + 1,
                         "start_timestamp": "2026-10-22T00:00:00+00:00", "discovery_evidence": {"question": "six trades"}}
            with self.assertRaisesRegex(ValueError, "REJECT_DISCOVERY"):
                cli.core.register_challenger(output, candidate)
            before = (output / "batches" / "BATCH_001.json").read_bytes()
            cli.import_telemetry_batch(source, output)
            self.assertEqual((output / "batches" / "BATCH_001.json").read_bytes(), before)
            self.assertEqual(source.read_bytes(), original)

    def test_import_conflicts_with_existing_discovery_and_invalid_stream_seal(self):
        from src.state_store import atomic_write_json
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            source, _ = self.telemetry_fixture(base)
            output = base / "existing N3 discovery"
            cli.core.freeze_batches(output, rows(20), "2026-10-07T02:00:00+00:00")
            before = (output / "origin.json").read_bytes()
            with self.assertRaisesRegex(RuntimeError, "existing research namespace"):
                cli.import_telemetry_batch(source, output)
            self.assertEqual((output / "origin.json").read_bytes(), before)
            stream_path = source.parent / "streams" / "predictors.json"
            damaged = json.loads(stream_path.read_text(encoding="utf-8"))
            damaged["rows"] = []
            atomic_write_json(stream_path, damaged)
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                cli.import_telemetry_batch(source, base / "other namespace")
            self.assertFalse((base / "other namespace").exists())

    def test_import_rebuild_rejects_self_consistent_fabricated_completed_economics(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            source, _ = self.telemetry_fixture(base)
            forged = cli.core._read_immutable(source)
            forged["rows"][0]["realized_pnl_lamports"] += 1
            forged["rows_hash"] = cli.core.digest(forged["rows"])
            cli.core.atomic_write_json(source, {**forged, "content_hash": cli.core.digest(forged)})
            with self.assertRaisesRegex(RuntimeError, "physical stream join"):
                cli.import_telemetry_batch(source, base / "other namespace")
            self.assertFalse((base / "other namespace").exists())

    def test_preview_does_not_create_output(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "research"
            result = cli.refresh(rows(20), output, "2026-10-07T02:00:00+00:00")
            self.assertFalse(output.exists())
            self.assertFalse(result["writes"])

    def test_open_batch_has_no_forensic_or_proposals(self):
        with tempfile.TemporaryDirectory() as temp, patch(
                "src.research.alpha_batch_forensics.build_forensic_report") as report:
            output = Path(temp) / "research"
            result = cli.refresh(rows(17), output, "2026-10-07T02:00:00+00:00", execute=True)
            self.assertEqual(result["closed_batches"], [])
            self.assertEqual(result["pending_completed"], 17)
            report.assert_not_called()

    def test_batch_report_is_sealed_and_restart_does_not_rewrite(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "research"
            first = cli.refresh(rows(20), output, "2026-10-07T02:00:00+00:00", execute=True)
            self.assertEqual(first["closed_batches"], ["BATCH_001"])
            path = output / "reports" / "BATCH_001.json"
            before = path.read_bytes()
            second = cli.refresh(rows(20), output, "2026-10-07T03:00:00+00:00", execute=True)
            self.assertEqual(second["closed_batches"], ["BATCH_001"])
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(cli.n3.read_immutable(path)["registration_mode"], "PROPOSAL_ONLY")
            self.assertFalse((output / "challenger_registry.json").exists())

    def test_output_inside_frozen_runtime_or_source_is_rejected(self):
        for path in (cli.ROOT / "outputs", cli.core.FROZEN_RUNTIME / "data/research"):
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, "outside"):
                cli.external_output(path)

    def test_malformed_input_fails_without_replacement(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "dataset.json"
            path.write_text("{broken", encoding="utf-8")
            before = path.read_bytes()
            with self.assertRaises(json.JSONDecodeError):
                cli.input_rows(path)
            self.assertEqual(path.read_bytes(), before)

    def test_sealed_definition_policy_tampering_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "candidate.json"
            definition = {"candidate_id": "A-v1", "discovery_evidence": {"minimum_coverage": 0.9}}
            cli.n3.immutable(path, definition)
            self.assertEqual(cli.input_definition(path), definition)
            tampered = json.loads(path.read_text(encoding="utf-8"))
            tampered["discovery_evidence"]["minimum_coverage"] = 0.0
            path.write_text(json.dumps(tampered), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "hash mismatch"):
                cli.input_definition(path)

    def test_unregistered_stage_definition_cannot_claim_fresh_status(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "absent-research"
            with self.assertRaisesRegex(RuntimeError, "registry required"):
                cli.core.registered_definition(output, "A-v1")
            self.assertFalse(output.exists())

    def test_registered_lookup_checks_batch_and_version(self):
        from tests.test_accelerated_alpha import SCREEN_POLICY
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "research"
            discovery = rows(20)
            for index, row in enumerate(discovery):
                row["realized_pnl_lamports"] = -100 if index < 10 else 100
                row["features"]["route_type"] = {"value": "A", "observed_at": row["signal_timestamp"], "source": "entry_snapshot"}
            cli.core.freeze_batches(output, discovery, "2026-10-07T02:00:00+00:00")
            definition = {"candidate_id": "A-v1", "version": 1, "kind": "negative_filter",
                          "rationale": "liquidity와 손실의 관계", "expected_mechanism": "해당 구간 손실 회피 가설",
                          "rule": {"feature": "liquidity_usd", "op": "lt", "threshold": 10010},
                          "creation_batch": "BATCH_001", "discovery_evidence": {"question": "liquidity", "discovery_screen_policy": SCREEN_POLICY},
                          "start_event_seq": 21, "start_timestamp": "2026-10-07T03:00:00+00:00"}
            candidate = cli.core.register_challenger(output, definition)
            self.assertEqual(cli.core.registered_definition(output, "A-v1"), candidate)
            with self.assertRaisesRegex(RuntimeError, "not registered"):
                cli.core.registered_definition(output, "A-v2")


if __name__ == "__main__":
    unittest.main()
