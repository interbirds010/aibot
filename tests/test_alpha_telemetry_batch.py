"""미래 telemetry batch 계약을 운영 data 없이 검증한다."""
from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
import hashlib
import io
from contextlib import redirect_stdout
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest
from unittest.mock import patch

from scripts import alpha_telemetry_batch as cli
from src.research import alpha_telemetry_batch as b, accelerated_alpha as core
from src.research import entry_telemetry as t
from src.state_store import atomic_write_json

START = "2026-10-15T00:00:00+00:00"
ACCEPTED = "2026-10-14T23:59:00+00:00"
MARKER = {"telemetry_epoch_id": "batch-fixture", "build_sha": "f" * 40,
    "config_fingerprint": t._digest({}), "start_utc": "2026-10-14T00:00:00+00:00", "start_event_seq": 1}


def seal(row):
    body = {k: v for k, v in row.items() if k != "content_hash"}
    return {**body, "content_hash": core.digest(body)}


def streams(count=1, *, days=False, completed=True, decision="BUY"):
    result = {name: [] for name in b.offline.SCHEMAS}
    for index in range(count):
        stamp = (core.utc(START) + timedelta(days=index if days else 0, seconds=index * 10)).isoformat()
        with patch.object(t, "_epoch", MARKER), patch.object(t, "_provenance", {"git_sha": MARKER["build_sha"], "session_id": "fixture-session", "config_fingerprint": MARKER["config_fingerprint"], "safe_config": {}}):
            capture = t.begin_signal(mint="mint" + str(index), route_type="B", signal_detected_at=stamp, signal_id="signal" + str(index))
            row = t._row(capture)
        row["decision"] = {"outcome": decision}
        row["decision_outcome"] = decision
        times = row["predictors"]["timestamps"]
        for name in ("signal_detected_at", "analysis_started_at", "analysis_completed_at", "preflight_started_at", "quote_buy_request_started_at", "quote_buy_received_at", "entry_decision_at"):
            offset = {"signal_detected_at": 0, "analysis_started_at": 1, "analysis_completed_at": 2, "preflight_started_at": 3, "quote_buy_request_started_at": 4, "quote_buy_received_at": 5, "entry_decision_at": 6}[name]
            times[name] = {"wall_utc": (core.utc(stamp) + timedelta(milliseconds=offset)).isoformat(), "monotonic_ns": (index * 10000 + offset) * 1000000, "semantics": "fixture"}
        row["predictors"]["sections"]["trajectory"] = {"liquidity_now": 10000, "history": [], "pre_signal_snapshots": []}
        row["predictors"]["sections"]["short_flow"] = {"sell_count": 3, "buy_count": 9}
        row = seal(row)
        result["predictors"].append(row)
        if decision != "BUY":
            continue
        identity = {**row["identity"], "trade_id": "trade" + str(index), "event_seq": index * 2 + 1}
        identity.pop("buy_event_seq", None)
        receipt = {k: row[k] for k in ("telemetry_epoch_id", "build_sha", "session_id", "provenance")}
        receipt.update(schema_version=1, receipt_schema_version=1, identity=identity,
            execution_receipt={"trade_id": identity["trade_id"], "event_seq": identity["event_seq"],
                "paper_buy_created_at": {"wall_utc": (core.utc(stamp) + timedelta(milliseconds=7)).isoformat(), "monotonic_ns": (index * 10000 + 7) * 1000000, "semantics": "fixture"}})
        result["receipts"].append(seal(receipt))
        if completed:
            closed = (core.utc(stamp) + timedelta(seconds=1)).isoformat()
            leg = {"event_seq": index * 2 + 2, "event_id": "sell" + str(index), "at": closed,
                "reason": "SL", "token_amount_raw": 100, "proceeds_lamports": 90, "realized_pnl_lamports": -10}
            outcome = {k: row[k] for k in ("telemetry_epoch_id", "build_sha", "session_id", "provenance")}
            outcome.update(schema_version=1, outcome_schema_version=1,
                identity={**identity, "signal_id": None, "buy_event_seq": index * 2 + 1, "event_seq": index * 2 + 2},
                outcome={"completed_at": closed, "entry_cost_lamports": 100, "cumulative_proceeds_lamports": 90,
                    "realized_pnl_lamports": -10, "exit_reason": "SL", "sell_legs": [leg], "sell_legs_complete": True, "sell_legs_truncated": False})
            result["outcomes"].append(seal(outcome))
    return result


def accepted(root):
    return {"status": "TELEMETRY_RUNTIME_ACCEPTED", "checked_utc": ACCEPTED, "runtime_root": str(root.resolve()),
        "epoch": MARKER, "latency": {"status": "PASS"}, "control_next_event_seq": 1}


def latency():
    baseline = {"control_fingerprint": MARKER["config_fingerprint"], "runtime_root": "baseline", "clock_semantics": "MONOTONIC",
        "build_sha": "a" * 40, "source_reference": "pre-control-clock-capture", "phase": "PRE_ACTIVATION_CONTROL",
        "captured_at_utc": "2026-10-13T00:00:00+00:00", "samples": [{"signal_id": "pre-1", "source_reference": "capture:1",
            "observed_at_utc": "2026-10-13T00:00:00+00:00", "metrics_ms": {"total_decision_ms": 5}}]}
    policy = {"registered_at_utc": "2026-10-13T00:00:00+00:00", "control_fingerprint": MARKER["config_fingerprint"],
        "baseline_runtime_root": "baseline", "metrics": {"total_decision_ms": {"statistic": "median", "minimum_samples": 1, "maximum_increase_ms": 2}}}
    return baseline, policy


class AlphaTelemetryBatchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "runtime"
        self.output = Path(self.temp.name) / "research output"
        self.root.mkdir()
        self.registration = b.register_batch(self.output, accepted(self.root), start_seq=1, start_utc=START)

    def test_registration_acceptance_gate_and_immutable_idempotence(self):
        self.assertEqual(self.registration, b.register_batch(self.output, accepted(self.root), start_seq=1, start_utc=START))
        with self.assertRaisesRegex(RuntimeError, "immutable conflict"):
            b.register_batch(self.output, accepted(self.root), start_seq=2, start_utc=START)
        for state in ("TELEMETRY_RUNTIME_BLOCKED", "TELEMETRY_RUNTIME_DEGRADED"):
            bad = accepted(self.root); bad["status"] = state
            with self.assertRaisesRegex(ValueError, "acceptance required"):
                b.register_batch(self.output, bad, start_seq=1, start_utc=START)
        with self.assertRaisesRegex(ValueError, "boundary"):
            b.register_batch(self.output, accepted(self.root), start_seq=0, start_utc=START)
        with self.assertRaisesRegex(ValueError, "outside runtime"):
            b.register_batch(self.root / "research", accepted(self.root), start_seq=1, start_utc=START)

    def test_real_latency_preregistered_baseline_missing_blocks_and_regression_degrades(self):
        raw = streams(); joined = b.offline.join_streams(*(raw[k] for k in b.offline.SCHEMAS))
        before, policy = latency()
        self.assertEqual(b.compare_latency(before, policy, MARKER, joined)["status"], "PASS")
        policy["metrics"]["total_decision_ms"]["maximum_increase_ms"] = 0
        self.assertEqual(b.compare_latency(before, policy, MARKER, joined)["status"], "DEGRADED")
        before["samples"] = []
        self.assertEqual(b.compare_latency(before, policy, MARKER, joined)["status"], "BLOCKED")
        policy["registered_at_utc"] = START
        with self.assertRaisesRegex(ValueError, "preregistered"):
            b.compare_latency(before, policy, MARKER, joined)

    def test_acceptance_natural_buy_and_latency_requirement(self):
        atomic_write_json(self.root / "data/paper_trades.json", {"next_event_seq": 3})
        natural = {"status": "PASS", "cases": {"first_rejected_predictor": {"status": "PASS"}}}
        with patch.object(b.epoch, "require_epoch", return_value=MARKER), patch.object(b, "load_streams", return_value=streams()), patch.object(b.natural, "inspect", return_value=copy.deepcopy(natural)):
            missing = b.acceptance_report(self.root, baseline=None, policy=None, config={})
            self.assertEqual(missing["status"], "TELEMETRY_RUNTIME_BLOCKED")
            before, policy = latency()
            result = b.acceptance_report(self.root, baseline=before, policy=policy, config={})
            self.assertEqual(result["status"], "TELEMETRY_RUNTIME_ACCEPTED")
            self.assertEqual(result["natural_acceptance"]["cases"]["first_buy_predictor"]["status"], "PASS")
        with patch.object(b.epoch, "require_epoch", return_value=MARKER), patch.object(b, "load_streams", return_value=streams(decision="RPC_SKIPPED")), patch.object(b.natural, "inspect", return_value=copy.deepcopy(natural)):
            before, policy = latency()
            self.assertEqual(b.acceptance_report(self.root, baseline=before, policy=policy, config={})["status"], "TELEMETRY_RUNTIME_BLOCKED")
        with patch.object(b.epoch, "require_epoch", side_effect=ValueError("recorder role degraded/drop/error/duplicate")):
            self.assertEqual(b.acceptance_report(self.root, baseline=None, policy=None, config={})["status"], "TELEMETRY_RUNTIME_DEGRADED")

    def test_pending_does_not_freeze_and_20_closes_first(self):
        self.assertEqual(b.build_dataset(streams(19), self.registration, "2026-10-16T00:00:00+00:00")["status"], "PENDING")
        result = b.build_dataset(streams(21), self.registration, "2026-10-16T00:00:00+00:00")
        self.assertEqual(result["close_reason"], "20_COMPLETED")
        self.assertEqual(len(result["rows"]), 20)
        self.assertEqual(result["end_event_seq"], 40)
        self.assertEqual(len(result["raw_streams"]["predictors"]), 20)

    def test_seven_active_days_include_open_trades_and_kst_date(self):
        result = b.build_dataset(streams(7, days=True, completed=False), self.registration, "2026-10-23T00:00:00+00:00")
        self.assertEqual(result["close_reason"], "7_ACTIVE_KST_DAYS")
        self.assertEqual(result["active_kst_days"], 7)
        self.assertEqual(result["rows"], [])
        self.assertTrue(all(r["join_status"] == "BUY_RECEIPT" for r in result["offline_join"]))
        rejects = streams(7, days=True, decision="REJECT_RISK")
        self.assertEqual(b.build_dataset(rejects, self.registration, "2026-10-23T00:00:00+00:00")["active_kst_days"], 0)

    def test_offline_dataset_preserves_rejects_and_exact_entry_sources(self):
        raw = streams(20)
        reject = streams(decision="REJECT_ANALYZER")["predictors"][0]
        reject["identity"]["signal_id"] = "rejected-independent"
        raw["predictors"].append(seal(reject))
        result = b.build_dataset(raw, self.registration, "2026-10-16T00:00:00+00:00")
        self.assertEqual(len(result["offline_join"]), 21)
        self.assertEqual(len(result["raw_streams"]["predictors"]), 21)
        feature = result["rows"][0]["features"]["flow_ask_count"]
        self.assertEqual(feature["value"], 3)
        self.assertTrue(feature["source"].endswith("short_flow.sell_count"))
        self.assertNotIn("sell_legs", result["rows"][0]["features"])

    def test_duplicates_future_timestamps_and_provenance_fail_closed(self):
        raw = streams(20); raw["predictors"].append(raw["predictors"][0])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            b.build_dataset(raw, self.registration, "2026-10-16T00:00:00+00:00")
        raw = streams(20)
        raw["predictors"][0]["predictors"]["timestamps"]["analysis_started_at"]["wall_utc"] = "2026-10-16T00:00:00+00:00"
        raw["predictors"][0] = seal(raw["predictors"][0])
        with self.assertRaisesRegex(ValueError, "future predictor"):
            b.build_dataset(raw, self.registration, "2026-10-16T00:00:00+00:00")
        raw = streams(20); raw["predictors"][0]["build_sha"] = "a" * 40
        raw["predictors"][0] = seal(raw["predictors"][0])
        with self.assertRaisesRegex(ValueError, "conflict"):
            b.build_dataset(raw, self.registration, "2026-10-16T00:00:00+00:00")

    def test_sell_leg_invariant_and_truncation_block_freeze(self):
        for field, value in (("sell_legs_complete", False), ("sell_legs_truncated", True), ("realized_pnl_lamports", -5)):
            raw = streams(20); raw["outcomes"][0]["outcome"][field] = value
            raw["outcomes"][0] = seal(raw["outcomes"][0])
            with self.subTest(field=field), self.assertRaises(ValueError):
                b.build_dataset(raw, self.registration, "2026-10-16T00:00:00+00:00")

    def test_windows_paths_physical_stream_freeze_and_cli_idempotence(self):
        raw = streams(20)
        paths = []
        for name, rows in raw.items():
            for index, row in enumerate(rows):
                path = self.root / "data/research/entry_telemetry/epochs" / MARKER["telemetry_epoch_id"] / name / "2026-10-15" / (str(index) + ".json")
                atomic_write_json(path, row); paths.append(path)
        before = {str(p): p.read_bytes() for p in paths}
        capture = io.StringIO()
        with redirect_stdout(capture):
            code = cli.main(["freeze", "--root", str(self.root), "--output", str(self.output), "--as-of", "2026-10-16T00:00:00+00:00"])
        self.assertEqual(code, 0, capture.getvalue())
        result = b.freeze_batch(self.root, self.output, as_of="2026-10-17T00:00:00+00:00")
        self.assertEqual(result["status"], "FROZEN")
        self.assertNotIn("raw_streams", result)
        for name in raw:
            sealed = b.read_evidence(self.output / b.BATCH_ID / "streams" / (name + ".json"))
            self.assertEqual(len(sealed["rows"]), 20)
        self.assertEqual(before, {str(p): p.read_bytes() for p in paths})
        b.write_evidence(self.output / "accepted.json", accepted(self.root), runtime_root=self.root)
        self.assertEqual(b.read_evidence(self.output / "accepted.json"), accepted(self.root))

    def test_stream_total_bound(self):
        with patch.object(b.offline, "iter_stream", return_value=iter(streams(2)["predictors"])), patch.object(b, "MAX_STREAM_ROWS", 1):
            with self.assertRaisesRegex(ValueError, "bound"):
                b.load_streams(self.root, "batch-fixture")

    def test_natural_acceptance_real_three_streams_sessions_and_control_ledger(self):
        raw = streams()
        for decision in ("REJECT_ANALYZER", "RPC_SKIPPED"):
            rejected = streams(decision=decision)["predictors"][0]
            rejected["identity"]["signal_id"] = decision
            raw["predictors"].append(seal(rejected))
        directory = self.root / "data/research/entry_telemetry/epochs" / MARKER["telemetry_epoch_id"]
        for name, rows in raw.items():
            for number, row in enumerate(rows):
                with patch.object(t, "_root", self.root), patch.object(t, "_epoch", MARKER):
                    atomic_write_json(t._record_path(name, row), row)
        session = "fixture-session"
        b.core._write_immutable(directory / "sessions" / (hashlib.sha256(session.encode()).hexdigest() + ".json"),
            {"telemetry_epoch_id": MARKER["telemetry_epoch_id"], "build_sha": MARKER["build_sha"], "session_id": session,
                "config_fingerprint": MARKER["config_fingerprint"]})
        for role in ("monitor", "risk-manager"):
            atomic_write_json(directory / "health" / (role + ".json"), {"status": "TELEMETRY_READY",
                "heartbeat_utc": datetime.now(timezone.utc).isoformat(), "telemetry_epoch_id": MARKER["telemetry_epoch_id"],
                "stream_queues": {name: {"depth": 0, "capacity": 16} for name in raw}})
        leg = raw["outcomes"][0]["outcome"]["sell_legs"][0]
        atomic_write_json(self.root / "data/paper_trades.json", {"schema_version": 2, "next_event_seq": 3, "positions": {},
            "events": [{"type": "BUY", "event_seq": 1, "mint": "mint0", "position_id": "trade0"},
                {"type": "SELL", "mint": "mint0", "position_id": "trade0", **leg}]})
        before, policy = latency()
        with patch.object(b.epoch, "require_epoch", return_value=MARKER):
            result = b.acceptance_report(self.root, baseline=before, policy=policy, config={})
        self.assertEqual(result["status"], "TELEMETRY_RUNTIME_ACCEPTED", result)
        self.assertEqual(len(result["natural_acceptance"]["cases"]), 6)
        self.assertEqual(result["physical_streams"]["predictors"]["count"], 3)

    def test_latency_clock_semantics_are_not_mixed(self):
        raw = streams(); joined = b.offline.join_streams(*(raw[k] for k in b.offline.SCHEMAS))
        before, policy = latency()
        before["clock_semantics"] = "UTC_WALL_CLOCK"
        raw["predictors"][0]["predictors"]["timestamps"]["entry_decision_at"]["monotonic_ns"] = 100000000
        result = b.compare_latency(before, policy, MARKER, joined)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["metrics"]["total_decision_ms"]["post_ms"], 6)
        self.assertEqual(result["clock_semantics"], "UTC_WALL_CLOCK")
        before.pop("clock_semantics")
        with self.assertRaisesRegex(ValueError, "clock semantics"):
            b.compare_latency(before, policy, MARKER, joined)

    def test_natural_reject_without_decision_clock_is_kept_without_invented_feature_times(self):
        raw = streams(20)
        reject = streams(decision="RPC_SKIPPED")["predictors"][0]
        reject["identity"]["signal_id"] = "natural-no-decision-clock"
        reject["predictors"]["timestamps"]["entry_decision_at"] = None
        reject = seal(reject)
        features = b.project_features(reject)
        self.assertEqual(set(features), {"route_type"})
        self.assertEqual(features["route_type"]["observed_at"], reject["identity"]["signal_detected_at"])
        raw["predictors"].append(reject)
        result = b.build_dataset(raw, self.registration, "2026-10-16T00:00:00+00:00")
        summary = next(row for row in result["offline_join"] if row["signal_id"] == "natural-no-decision-clock")
        self.assertFalse(summary["decision_timestamp_available"])
        self.assertIsNotNone(summary["decision_timestamp_missing_reason"])
        self.assertEqual(result["rows"][0]["features"]["route_type"]["value"], "B")

    def test_registration_process_race_has_one_immutable_boundary(self):
        accepted_path = self.output / "acceptance-for-process.json"
        b.write_evidence(accepted_path, accepted(self.root), runtime_root=self.root)
        destination = self.output / "concurrent"
        processes = []
        for sequence in (1, 2):
            processes.append(subprocess.Popen([sys.executable, str(Path(cli.__file__).resolve()), "register",
                "--acceptance", str(accepted_path), "--output", str(destination), "--start-seq", str(sequence),
                "--start-utc", START], stdout=subprocess.PIPE, stderr=subprocess.PIPE))
        results = []
        for process in processes:
            stdout, stderr = process.communicate(timeout=30)
            results.append(process.returncode)
            self.assertIn(process.returncode, (0, 1), (stdout, stderr))
        self.assertEqual(sorted(results), [0, 1])
        document = b.read_evidence(destination / b.BATCH_ID / "registration.json")
        self.assertIn(document["start_event_seq"], (1, 2))

    def test_duplicate_buy_sequences_and_frozen_stream_tampering_are_detected(self):
        raw = streams(20)
        raw["receipts"][1]["identity"]["event_seq"] = 1
        raw["receipts"][1]["execution_receipt"]["event_seq"] = 1
        raw["outcomes"][1]["identity"]["buy_event_seq"] = 1
        raw["receipts"][1] = seal(raw["receipts"][1]); raw["outcomes"][1] = seal(raw["outcomes"][1])
        with self.assertRaisesRegex(ValueError, "BUY sequence"):
            b.build_dataset(raw, self.registration, "2026-10-16T00:00:00+00:00")
        with patch.object(b, "load_streams", return_value=streams(20)):
            b.freeze_batch(self.root, self.output, as_of="2026-10-16T00:00:00+00:00")
        path = self.output / b.BATCH_ID / "streams/predictors.json"
        tampered = b.read_evidence(path); tampered["rows"] = []
        atomic_write_json(path, seal(tampered))
        with self.assertRaisesRegex(ValueError, "stream integrity"):
            b.read_dataset(self.output)

    def test_kst_activity_uses_signal_day_when_buy_receipt_crosses_midnight(self):
        raw = streams(7, days=True, completed=False)
        first = raw["predictors"][0]
        signal = "2026-10-15T14:59:00+00:00"  # KST 23:59
        first["identity"]["signal_detected_at"] = signal
        for stamp in first["predictors"]["timestamps"].values():
            if stamp is not None:
                stamp["wall_utc"] = (core.utc(signal) + timedelta(milliseconds=stamp["monotonic_ns"] / 1000000)).isoformat()
        receipt = raw["receipts"][0]
        receipt["identity"]["signal_detected_at"] = signal
        receipt["execution_receipt"]["paper_buy_created_at"]["wall_utc"] = "2026-10-15T15:01:00+00:00"  # KST next day
        raw["predictors"][0] = seal(first); raw["receipts"][0] = seal(receipt)
        result = b.build_dataset(raw, self.registration, "2026-10-23T00:00:00+00:00")
        self.assertEqual(result["status"], "FROZEN")
        self.assertEqual(result["active_kst_days"], 7)
        receipt_days = {b._receipt_time(row).astimezone(b.KST).date() for row in result["raw_streams"]["receipts"]}
        self.assertEqual(len(receipt_days), 6)
        self.assertEqual(result["end_utc"], b._receipt_time(raw["receipts"][-1]).isoformat())
        self.assertEqual(result["end_event_seq"], raw["receipts"][-1]["identity"]["event_seq"])
        raw = streams(7, days=True, completed=False)
        # 같은 signal date의 지연 BUY는 새 활동일을 만들지 않는다.
        raw["predictors"][-1]["identity"]["signal_detected_at"] = START
        raw["predictors"][-1]["predictors"]["timestamps"]["signal_detected_at"]["wall_utc"] = START
        raw["receipts"][-1]["identity"]["signal_detected_at"] = START
        raw["predictors"][-1] = seal(raw["predictors"][-1]); raw["receipts"][-1] = seal(raw["receipts"][-1])
        pending = b.build_dataset(raw, self.registration, "2026-10-23T00:00:00+00:00")
        self.assertEqual(pending["status"], "PENDING")
        self.assertEqual(pending["active_kst_days"], 6)


if __name__ == "__main__":
    unittest.main()
