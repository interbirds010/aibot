"""N3 분기 보고서와 별도 Windows root의 명시적 cutover 경계를 검증한다."""
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from scripts import n3_feasibility_checkpoint as watcher
from scripts import entry_telemetry_cutover as cutover
from src.research import alpha_transition as transition
from src.research import entry_telemetry_epoch as epoch
from src.research import n3_feasibility as feasibility
from src.research.n3_shadow import CONTRACT, immutable, read_immutable
from src.state_store import atomic_write_json, atomic_clone_json


class AlphaTransitionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="alpha Windows path ")
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.frozen = self.home / "frozen runtime"
        self.target = self.home / "telemetry runtime"
        self.manifest = {"cohort_id": feasibility.COHORT, "start_event_seq": feasibility.START_SEQ,
                         "start_utc": "2026-10-03T00:00:00+00:00", "contract": CONTRACT,
                         "build": {"git_sha": feasibility.BUILD, "source_digest": feasibility.SOURCE_DIGEST}}
        for patcher in (patch.object(feasibility, "MANIFEST_HASH", feasibility.digest(self.manifest)),
                        patch.object(feasibility, "source_guard", return_value=self.manifest)):
            patcher.start()
            self.addCleanup(patcher.stop)
        immutable(self.frozen / "data/n3_shadow/manifest.json", self.manifest)
        atomic_write_json(self.frozen / "data/n3_shadow/runtime_supervisor.disabled",
                          {"disabled": True, "at_kst": "2026-10-07T09:00:00+09:00"})
        self.populate(17, 0)
        self.amendment_path = self.home / "amendment.json"
        self.amendment = feasibility.create_amendment(self.frozen, self.amendment_path, "2026-10-07T00:00:00+00:00")
        self.populate(30, 1)
        self.checkpoint_path = self.home / "checkpoint.json"
        self.checkpoint = feasibility.record_checkpoint(self.frozen, self.amendment_path, self.checkpoint_path)
        self.copy_state()
        self.frozen_proof = {"runtime_root": str(self.frozen.resolve()), "active_process_count": 0}
        self.target_proof = {"runtime_root": str(self.target.resolve()), "active_process_count": 0}

    def populate(self, count, hits):
        side = self.frozen / "data/n3_shadow"
        events = []
        for i in range(count):
            pid = "p-" + str(i)
            seq = feasibility.START_SEQ + i * 2
            signal = datetime(2026, 10, 3, 2, tzinfo=timezone.utc) + timedelta(days=i % 6)
            analysis = signal + timedelta(seconds=2)
            preflight = analysis + timedelta(seconds=1 if i < hits else 4.5)
            at = (preflight + timedelta(seconds=1)).isoformat()
            value = i < hits
            snapshot = {"cohort_id": feasibility.COHORT, "rule_hash": feasibility.RULE_HASH, "build": self.manifest["build"], "position_id": pid,
                        "signal_timestamp": signal.isoformat(), "analysis_end_timestamp": analysis.isoformat(),
                        "preflight_end_timestamp": preflight.isoformat(), "entry_decision_timestamp": at,
                        "raw_inputs": {"analysis_duration_sec": 2, "quote_preflight_duration_sec": 1 if value else 4.5},
                        "would_skip": value, "missing_fields": []}
            entry = {"cohort_id": feasibility.COHORT, "rule_hash": feasibility.RULE_HASH, "position_id": pid,
                     "buy_event_seq": seq, "buy_event_id": "buy-" + pid, "signal_timestamp": signal.isoformat(),
                     "entry_token_amount_raw": 100, "would_skip": value, "snapshot": snapshot}
            buy = {"type": "BUY", "event_seq": seq, "event_id": "buy-" + pid, "position_id": pid,
                   "signal_detected_at": signal.isoformat(), "analysis_completed_at": analysis.isoformat(),
                   "entry_quote_at": preflight.isoformat(), "at": at, "token_amount_raw": 100}
            sell = {"type": "SELL", "event_seq": seq + 1, "event_id": "sell-" + pid,
                    "position_id": pid, "at": at, "token_amount_raw": 100}
            completed = {**entry, "closed_at": at, "sell_event_count": 1}
            for kind, identity, document in (("entries", pid, entry), ("snapshots", pid, snapshot),
                                             ("completed", pid, completed), ("sells", seq + 1, sell)):
                path = side / kind / (hashlib.sha256(str(identity).encode()).hexdigest() + ".json")
                # 재구성 fixture는 실제 운영 경로에 쓰지 않는다.
                atomic_write_json(path, {**document, "content_hash": feasibility.digest(document)})
            events.extend((buy, sell))
        atomic_write_json(side / "observer_state.json", {"cohort_id": feasibility.COHORT,
                          "status": "RUNNING", "cursor": feasibility.START_SEQ + count * 2 - 1,
                          "version": count, "admitted_count": count, "completed_count": count, "open_count": 0,
                          "sell_record_count": count, "missing_snapshot_count": 0,
                          "missing_field_counts": {}, "recorder_health": "OK"})
        atomic_write_json(self.frozen / "data/paper_trades.json", {"schema_version": 2, "events": events,
                          "next_event_seq": feasibility.START_SEQ + count * 2})

    def copy_state(self):
        document = feasibility.read_document(self.frozen / "data/paper_trades.json")
        atomic_write_json(self.target / "data/paper_trades.json", document)

    def prepare(self, checkpoint=None, **kwargs):
        return transition.prepare_boundary(self.frozen, checkpoint or self.checkpoint, self.amendment,
                                           self.target, frozen_stopped_proof=self.frozen_proof,
                                           target_stopped_proof=kwargs.get("target_proof", self.target_proof))

    def bytes(self, root):
        return {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()}

    def case_b(self):
        self.populate(30, 2)
        self.copy_state()
        path = self.home / "checkpoint-b.json"
        return feasibility.record_checkpoint(self.frozen, self.amendment, path)

    def test_case_a_and_case_b_reports_do_not_evaluate_economics(self):
        for checkpoint, case in ((self.checkpoint, "A"), (self.case_b(), "B")):
            with self.subTest(case=case):
                report = transition.transition_report(checkpoint)
                self.assertEqual(report["case"], case)
                self.assertEqual(report["completed"], 30)
                self.assertFalse(report["original_cohort_closed"])
                self.assertEqual(report["economic_evaluation"], "FORBIDDEN")
                self.assertEqual(report["process_actions"], [])

    def test_transition_is_immutable_external_and_frozen_bytes_unchanged(self):
        before = self.bytes(self.frozen)
        output = self.home / "reports/transition.json"
        first = transition.record_transition(self.frozen, self.checkpoint, output)
        raw = output.read_bytes()
        self.assertEqual(first, transition.record_transition(self.frozen, self.checkpoint_path, output))
        self.assertEqual(raw, output.read_bytes())
        self.assertEqual(before, self.bytes(self.frozen))
        self.assertEqual(first["boundary_annotation"]["scope"], "FEASIBILITY_AMENDMENT_ONLY")
        self.assertFalse(first["boundary_annotation"]["final_report_created"])
        with self.assertRaises(RuntimeError):
            transition.record_transition(self.frozen, self.checkpoint, self.frozen / "outputs/report.json")
        other = {**self.checkpoint, "hit": 2, "non_hit": 28, "coverage": 2 / 30,
                 "decision": "N3_CONTINUE_PROSPECTIVE_VALIDATION", "coverage_outcome": "CONTINUE_PROSPECTIVE_VALIDATION"}
        with self.assertRaises(RuntimeError):
            transition.record_transition(self.frozen, other, output)

    def test_gate_health_counts_and_verdict_mismatch_fail_closed(self):
        for change in ({"recorder_health": "UNVERIFIED"}, {"unknown": 1}, {"hit": True}, {"completed": 29},
                       {"coverage": float("nan")}, {"decision": "N3_CONTINUE_PROSPECTIVE_VALIDATION"},
                       {"days": 0}, {"amendment_hash": "foreign"}, {"checkpoint_status": "WAITING_FOR_30_OR_7D"}):
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                transition.transition_report({**self.checkpoint, **change})

    def test_unhealthy_recording_gets_external_review_report_without_case_decision(self):
        checkpoint = {**self.checkpoint, "unknown": 1, "non_hit": 28, "coverage": 1 / 29,
                      "recorder_health": "RECORDING_REVIEW_REQUIRED", "decision": None, "coverage_outcome": None}
        report = transition.record_transition(self.frozen, checkpoint, self.home / "review-report.json")
        self.assertIsNone(report["case"])
        self.assertIsNone(report["checkpoint_verdict"])
        self.assertEqual(report["activation_gate"], "BLOCKED_RECORDING_REVIEW_REQUIRED")
        self.assertNotIn("CLOSURE", report["boundary_annotation"]["artifact_type"])
        with self.assertRaises(RuntimeError):
            self.prepare(checkpoint)

    def test_plan_requires_separate_roots_and_never_starts_processes(self):
        before = self.bytes(self.frozen)
        with patch.object(cutover.runner, "manage", side_effect=AssertionError("process mutation")):
            plan = transition.cutover_plan(self.checkpoint, frozen_runtime=self.frozen, telemetry_runtime=self.target)
        self.assertTrue(plan["single_control_buy_owner"])
        self.assertFalse(plan["executable_now"])
        self.assertEqual(before, self.bytes(self.frozen))
        for root in (self.frozen, self.frozen / "nested", self.home, transition.PROTECTED_RUNTIME):
            with self.subTest(root=root), self.assertRaises(RuntimeError):
                transition.cutover_plan(self.checkpoint, frozen_runtime=self.frozen, telemetry_runtime=root)

    def test_explicit_case_a_boundary_new_epoch_preserves_original_running(self):
        before = self.bytes(self.frozen)
        boundary = self.prepare()
        raw = transition.boundary_path(self.target).read_bytes()
        self.assertEqual(boundary, self.prepare())
        self.assertEqual(raw, transition.boundary_path(self.target).read_bytes())
        self.assertEqual(before, self.bytes(self.frozen))
        self.assertFalse((self.target / "data/n3_shadow").exists())
        self.assertFalse(boundary["n3_original_closed"])
        marker = epoch.create_snapshot(self.target, stopped_proof=self.target_proof)
        self.assertEqual(marker["next_event_seq"], boundary["start_event_seq"])
        self.assertEqual(marker["n3_closure"]["artifact_type"], "N3_FEASIBILITY_CUTOVER_BOUNDARY")
        build = {"git_sha": "a" * 40, "source_digest": "b" * 64, "source_files": {}}
        with patch.object(epoch, "build_identity", return_value=build):
            value = epoch.create_epoch(self.target, config={"paper_buy_basis_points": 50}, stopped_proof=self.target_proof)
            self.assertEqual(epoch.require_epoch(self.target, config={"paper_buy_basis_points": 50}), value)
        self.assertEqual(value["n3_closure"]["case"], "A")
        self.assertEqual(before, self.bytes(self.frozen))

    def test_case_b_segmented_provenance_keeps_rule_and_one_ledger(self):
        boundary = self.prepare(self.case_b())
        provenance = epoch.n3_closed(self.target)
        self.assertEqual(boundary["case"], "B")
        self.assertTrue(provenance["segmented_evidence"])
        self.assertEqual(provenance["decision"], "N3_CONTINUE_PROSPECTIVE_VALIDATION")
        self.assertEqual(provenance["rule_hash"], feasibility.RULE_HASH)
        self.assertEqual(boundary["frozen_state_hashes"], boundary["target_state_hashes"])
        self.assertFalse((self.frozen / "data/n3_shadow/closure.json").exists())

    def test_existing_closed_gate_stays_closed_without_explicit_boundary(self):
        with self.assertRaises(RuntimeError):
            epoch.create_snapshot(self.frozen, stopped_proof=self.frozen_proof)
        with self.assertRaises(RuntimeError):
            epoch.create_snapshot(self.target, stopped_proof=self.target_proof)

    def test_backlog_and_stopped_proof_and_foreign_checkpoint_rejected(self):
        for checkpoint in ({**self.checkpoint, "amendment_hash": "f" * 64},
                           {**self.checkpoint, "hit": 0, "non_hit": 30, "coverage": 0}):
            with self.subTest(checkpoint=checkpoint), self.assertRaises(RuntimeError):
                self.prepare(checkpoint)
        for count in (1, False):
            with self.subTest(count=count), self.assertRaises(RuntimeError):
                self.prepare(target_proof={**self.target_proof, "active_process_count": count})
        self.assertFalse(transition.boundary_path(self.target).exists())

    def add_pending_event(self):
        path = self.frozen / "data/paper_trades.json"
        ledger = feasibility.read_document(path)
        seq = ledger["next_event_seq"]
        ledger["events"].append({"event_seq": seq, "event_id": "pending-rejection", "type": "SIGNAL_REJECTED"})
        ledger["next_event_seq"] += 1
        atomic_write_json(path, ledger)
        return seq

    def test_first_backlogged_checkpoint_can_cutover_after_current_observer_drains(self):
        seq = self.add_pending_event()
        path = self.home / "first backlogged checkpoint.json"
        first = feasibility.record_checkpoint(self.frozen, self.amendment_path, path)
        raw = path.read_bytes()
        self.assertEqual(first["backlog"], 1)
        self.assertEqual(first["recorder_health"], "OK")
        state_path = self.frozen / "data/n3_shadow/observer_state.json"
        state = feasibility.read_document(state_path)
        atomic_write_json(state_path, {**state, "cursor": seq, "version": state["version"] + 1})
        self.copy_state()
        boundary = self.prepare(first)
        self.assertEqual(boundary["checkpoint"]["backlog"], 1)
        self.assertEqual(boundary["end_event_seq"], seq)
        self.assertEqual(transition.read_boundary(self.target), boundary)
        snapshot = epoch.create_snapshot(self.target, stopped_proof=self.target_proof)
        self.assertEqual(snapshot["next_event_seq"], seq + 1)
        self.assertEqual(first, feasibility.record_checkpoint(self.frozen, self.amendment_path, path))
        self.assertEqual(raw, path.read_bytes())

    def test_current_backlog_still_blocks_cutover_even_when_historical_checkpoint_clean(self):
        raw = self.checkpoint_path.read_bytes()
        self.add_pending_event()
        self.copy_state()
        with self.assertRaisesRegex(RuntimeError, "healthy complete"):
            self.prepare()
        self.assertFalse(transition.boundary_path(self.target).exists())
        self.assertEqual(raw, self.checkpoint_path.read_bytes())

    def test_target_state_mismatch_and_active_n3_copy_rejected(self):
        ledger = feasibility.read_document(self.target / "data/paper_trades.json")
        atomic_write_json(self.target / "data/paper_trades.json", {**ledger, "next_event_seq": ledger["next_event_seq"] + 1})
        with self.assertRaises(RuntimeError):
            self.prepare()
        self.copy_state()
        immutable(self.target / "data/n3_shadow/manifest.json", self.manifest)
        with self.assertRaises(RuntimeError):
            self.prepare()

    def test_torn_and_resealed_boundary_metadata_rejected(self):
        self.prepare()
        path = transition.boundary_path(self.target)
        original = read_immutable(path)
        for change in ({"case": "B"}, {"n3_original_closed": True}, {"end_event_seq": 1},
                       {"target_state_hashes": {"../secret.json": "a" * 64}}, {"frozen_build": "foreign"}):
            document = {**original, **change}
            atomic_write_json(path, {**document, "content_hash": feasibility.digest(document)})
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                epoch.n3_closed(self.target)
        atomic_write_json(path, {**original, "content_hash": "0" * 64})
        with self.assertRaises(RuntimeError):
            epoch.n3_closed(self.target)

    def test_snapshot_refuses_state_change_after_boundary_install(self):
        self.prepare()
        atomic_write_json(self.target / "data/wallets.json", {"wallets": []})
        with self.assertRaises(RuntimeError):
            epoch.create_snapshot(self.target, stopped_proof=self.target_proof)

    def test_watcher_automatically_records_transition_and_repeat_is_immutable(self):
        output = self.home / "watch checkpoint.json"
        before = self.bytes(self.frozen)
        with redirect_stdout(io.StringIO()):
            result = watcher.main(["--runtime", str(self.frozen), "--amendment", str(self.amendment_path),
                                   "--record", "--output", str(output), "--watch", "--max-polls", "1"])
        transition_path = output.with_name(output.stem + "_transition.json")
        self.assertEqual(result, 0)
        original = transition_path.read_bytes()
        self.assertEqual(read_immutable(transition_path)["case"], "A")
        with redirect_stdout(io.StringIO()):
            self.assertEqual(watcher.main(["--runtime", str(self.frozen), "--amendment", str(self.amendment_path),
                                          "--record", "--output", str(output)]), 0)
        self.assertEqual(original, transition_path.read_bytes())
        self.assertEqual(before, self.bytes(self.frozen))

    def test_prepare_boundary_cli_default_is_readonly(self):
        before = self.bytes(self.home)
        argv = ["entry_telemetry_cutover.py", "prepare-boundary", "--root", str(self.target),
                "--frozen-root", str(self.frozen), "--checkpoint", str(self.checkpoint_path),
                "--amendment", str(self.amendment_path)]
        with patch.object(sys, "argv", argv), redirect_stdout(io.StringIO()), patch.object(cutover, "stopped_proof", side_effect=AssertionError("inventory not needed")):
            cutover.main()
        self.assertEqual(before, self.bytes(self.home))

    def test_real_identity_gap_evidence_external_idempotent_and_no_process_actions(self):
        before_path = self.home / "before-evidence.json"
        old = [{"pid": 101, "created": "111"}, {"pid": 102, "created": "112"}]
        original = self.bytes(self.frozen)
        with patch.object(cutover, "_active_processes", return_value=old):
            before = transition.capture_before_stop(self.frozen, self.checkpoint, before_path)
            self.assertEqual(before, transition.capture_before_stop(self.frozen, self.checkpoint, before_path))
        self.prepare()
        epoch.create_snapshot(self.target, stopped_proof=self.target_proof)
        config = {"paper_buy_basis_points": 50}
        build = {"git_sha": "a" * 40, "source_digest": "b" * 64, "source_files": {}}
        with patch.object(epoch, "build_identity", return_value=build):
            marker = epoch.create_epoch(self.target, config=config, stopped_proof=self.target_proof)
            sessions = {}
            services = {}
            rows = []
            for service, pid in (("monitor", 201), ("risk-manager", 202)):
                record = {"pid": pid, "created": str(pid * 10), "executable": "python.exe", "command": service}
                services[service] = record
                rows.append(record)
                sessions[pid] = {"process_id": pid, "session_id": "session-" + service,
                                 "telemetry_epoch_id": marker["telemetry_epoch_id"]}
            atomic_write_json(self.target / "logs/local_runner.json", {"services": services})
            output = self.home / "gap-evidence.json"
            with patch.object(cutover, "stopped_proof", return_value=self.frozen_proof), \
                    patch.object(cutover, "_paper_config", return_value=config), \
                    patch.object(cutover, "_sessions", return_value=sessions), \
                    patch.object(cutover, "_active_processes", return_value=rows), \
                    patch.object(cutover.runner, "manage", side_effect=AssertionError("process change")):
                gap = transition.record_cutover_gap(self.frozen, self.target, before_path, output)
                raw = output.read_bytes()
                self.assertEqual(gap, transition.record_cutover_gap(self.frozen, self.target, before_path, output))
                self.assertEqual(raw, output.read_bytes())
            self.assertEqual(gap["new_start_event_seq"], gap["old_end_event_seq"] + 1)
            self.assertEqual(len(gap["new_process_sessions"]), 2)
            self.assertIn("UNOBSERVED", gap["exact_stop_resume_instants"])
        self.assertEqual(original, self.bytes(self.frozen))

    def test_pre_stop_requires_live_creation_identity_and_external_output(self):
        for rows in ([], [{"pid": 101, "created": None}]):
            with patch.object(cutover, "_active_processes", return_value=rows), self.assertRaises(RuntimeError):
                transition.capture_before_stop(self.frozen, self.checkpoint, self.home / "before.json")
        with self.assertRaises(RuntimeError):
            transition.capture_before_stop(self.frozen, self.checkpoint, self.frozen / "before.json")

    def test_clone_state_preserves_exact_source_bytes_in_isolated_empty_target(self):
        path = self.frozen / "data/paper_trades.json"
        # 임시 fixture의 특이한 JSON 공백도 copy가 보존하는지 확인한다.
        path.write_text(json.dumps(feasibility.read_document(path), indent=2) + "\n", encoding="utf-8")
        atomic_write_json(self.frozen / "data/nested/state.json", {"version": 17, "historical_samples": [1, 2]})
        target = self.home / "new isolated Windows target"
        proof = {"runtime_root": str(target.resolve()), "active_process_count": 0}
        before = self.bytes(self.frozen)
        first = transition.clone_state(self.frozen, target, frozen_stopped_proof=self.frozen_proof, target_stopped_proof=proof)
        self.assertEqual(first["copied_files"], 2)
        self.assertEqual(path.read_bytes(), (target / "data/paper_trades.json").read_bytes())
        self.assertEqual((self.frozen / "data/nested/state.json").read_bytes(), (target / "data/nested/state.json").read_bytes())
        self.assertFalse((target / "data/n3_shadow").exists())
        self.assertFalse((target / "logs").exists())
        self.assertFalse((target / ".env").exists())
        second = transition.clone_state(self.frozen, target, frozen_stopped_proof=self.frozen_proof, target_stopped_proof=proof)
        self.assertEqual(second["copied_files"], 0)
        self.assertEqual(second["state_hashes"], first["state_hashes"])
        self.assertEqual(before, self.bytes(self.frozen))

    def test_clone_refuses_dirty_target_missing_maintenance_or_protected_root(self):
        atomic_write_json(self.target / "data/foreign.json", {"keep": True})
        before = self.bytes(self.target)
        with self.assertRaises(RuntimeError):
            transition.clone_state(self.frozen, self.target, frozen_stopped_proof=self.frozen_proof,
                                   target_stopped_proof=self.target_proof)
        self.assertEqual(before, self.bytes(self.target))
        marker = self.frozen / "data/n3_shadow/runtime_supervisor.disabled"
        marker.unlink()
        with self.assertRaises(OSError):
            self.prepare()
        with self.assertRaises(OSError):
            transition.clone_state(self.frozen, self.target, frozen_stopped_proof=self.frozen_proof,
                                   target_stopped_proof=self.target_proof)
        with self.assertRaises(RuntimeError):
            transition.clone_state(self.frozen, transition.PROTECTED_RUNTIME,
                                   frozen_stopped_proof=self.frozen_proof, target_stopped_proof=self.target_proof)

    def test_atomic_clone_rejects_corrupt_json_and_existing_conflict(self):
        source = self.home / "source.json"
        target = self.home / "cloned.json"
        for raw in (b'{"duplicate":1,"duplicate":2}', b'{"bad":NaN}', b'{"broken":'):
            source.write_bytes(raw)
            checksum = hashlib.sha256(raw).hexdigest()
            with self.assertRaises(ValueError):
                atomic_clone_json(source, target, expected_sha256=checksum)
            self.assertFalse(target.exists())
        atomic_write_json(source, {"sample": 1})
        atomic_write_json(target, {"preserve": 2})
        before = target.read_bytes()
        with self.assertRaises(RuntimeError):
            atomic_clone_json(source, target, expected_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
        self.assertEqual(before, target.read_bytes())

    def test_atomic_clone_windows_process_race_preserves_one_identical_file(self):
        source = self.home / "race source.json"
        target = self.home / "race target.json"
        atomic_write_json(source, {"events": [{"event_id": "once"}], "version": 19})
        checksum = hashlib.sha256(source.read_bytes()).hexdigest()
        command = "from pathlib import Path; from src.state_store import atomic_clone_json; print(atomic_clone_json(Path(" + repr(str(source)) + "), Path(" + repr(str(target)) + "), expected_sha256=" + repr(checksum) + "))"
        processes = [subprocess.Popen([sys.executable, "-c", command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(2)]
        outputs = []
        for process in processes:
            output, error = process.communicate(timeout=30)
            self.assertEqual(process.returncode, 0, error)
            outputs.append(output.strip())
        self.assertEqual(sorted(outputs), ["False", "True"])
        self.assertEqual(source.read_bytes(), target.read_bytes())

    def junction(self, link, target):
        link.parent.mkdir(parents=True, exist_ok=True)
        target.mkdir(parents=True, exist_ok=True)
        if os.name == "nt":
            command = "New-Item -ItemType Junction -Path '" + str(link).replace("'", "''") + "' -Target '" + str(target).replace("'", "''") + "' | Out-Null"
            subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", command],
                           check=True, capture_output=True, text=True)
        else:
            link.symlink_to(target, target_is_directory=True)

    def test_clone_refuses_data_junction_without_writing_external_destination(self):
        target = self.home / "junction target"
        outside = self.home / "outside isolated root"
        self.junction(target / "data", outside)
        proof = {"runtime_root": str(target.resolve()), "active_process_count": 0}
        original = self.bytes(self.frozen)
        with self.assertRaises(RuntimeError):
            transition.clone_state(self.frozen, target, frozen_stopped_proof=self.frozen_proof,
                                   target_stopped_proof=proof)
        self.assertEqual(list(outside.iterdir()), [])
        self.assertEqual(original, self.bytes(self.frozen))

    def test_atomic_clone_refuses_source_and_destination_parent_junctions(self):
        outside = self.home / "junction outside"
        linked = self.home / "junction parent"
        self.junction(linked, outside)
        source = self.home / "ordinary source.json"
        atomic_write_json(source, {"value": 1})
        checksum = hashlib.sha256(source.read_bytes()).hexdigest()
        with self.assertRaises(RuntimeError):
            atomic_clone_json(source, linked / "destination.json", expected_sha256=checksum)
        self.assertEqual(list(outside.iterdir()), [])
        atomic_write_json(outside / "source.json", {"value": 1})
        target = self.home / "ordinary target.json"
        with self.assertRaises(RuntimeError):
            atomic_clone_json(linked / "source.json", target,
                              expected_sha256=hashlib.sha256((outside / "source.json").read_bytes()).hexdigest())
        self.assertFalse(target.exists())


if __name__ == "__main__":
    unittest.main()
