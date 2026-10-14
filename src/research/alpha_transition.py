"""Frozen N3를 건드리지 않는 checkpoint 전환 보고서와 실행 전 계획."""
from __future__ import annotations

from datetime import datetime, timezone
import math
from pathlib import Path

from src.research import n3_feasibility as feasibility

CANDIDATE = "86d7ddc"
PROTECTED_RUNTIME = Path("C:/Users/user/Documents/aibot-local-paper").resolve()


def _checkpoint(value, *, recording_required=True):
    document = feasibility.read_sealed(value) if isinstance(value, (str, Path)) else dict(value)
    if "content_hash" in document:
        seal = document.pop("content_hash")
        if seal != feasibility.digest(document):
            raise feasibility.FeasibilityBlocked("checkpoint content hash mismatch")
    if (document.get("artifact_type") != "N3_FEASIBILITY_CHECKPOINT"
            or document.get("checkpoint_status") != "CHECKPOINT_REACHED"
            or document.get("cohort_id") != feasibility.COHORT
            or document.get("original_manifest_hash") != feasibility.MANIFEST_HASH
            or document.get("phase") != "FEASIBILITY_ONLY"
            or document.get("project_status") != "PAUSE_STRATEGY_REVIEW"):
        raise feasibility.FeasibilityBlocked("checkpoint identity/status mismatch")
    amendment_hash = document.get("amendment_hash")
    if not isinstance(amendment_hash, str) or len(amendment_hash) != 64 or any(c not in "0123456789abcdef" for c in amendment_hash):
        raise feasibility.FeasibilityBlocked("checkpoint amendment hash invalid")
    feasibility.timestamp(document.get("recorded_at"))
    counts = {key: feasibility.integer(document.get(key)) for key in ("completed", "days", "hit", "non_hit", "unknown", "backlog")}
    feasibility.integer(document.get("cursor"), feasibility.START_SEQ - 1)
    if (counts["completed"] != counts["hit"] + counts["non_hit"] + counts["unknown"]
            or not 1 <= counts["days"] <= counts["completed"]
            or not (counts["completed"] >= feasibility.GATES["completed_control"]
                    or counts["days"] >= feasibility.GATES["active_kst_days"])):
        raise feasibility.FeasibilityBlocked("checkpoint gate/counts invalid")
    recording_ok = document.get("recorder_health") == "OK" and counts["unknown"] == 0
    if recording_required and not recording_ok:
        raise feasibility.FeasibilityBlocked("checkpoint recording review required")
    coverage = document.get("coverage")
    valid = counts["hit"] + counts["non_hit"]
    expected = counts["hit"] / valid if valid else None
    if ((expected is None and coverage is not None) or (expected is not None and
            (isinstance(coverage, bool) or not isinstance(coverage, (int, float)) or not math.isfinite(coverage)
             or not math.isclose(coverage, expected, rel_tol=1e-12, abs_tol=1e-12)))):
        raise feasibility.FeasibilityBlocked("checkpoint coverage/count mismatch")
    if not recording_ok:
        if (document.get("recorder_health") != "RECORDING_REVIEW_REQUIRED"
                or document.get("decision") is not None or document.get("coverage_outcome") is not None):
            raise feasibility.FeasibilityBlocked("unhealthy checkpoint cannot carry a coverage verdict")
        return document
    verdict = "N3_LOW_FRESH_COVERAGE" if counts["hit"] <= 1 else "N3_CONTINUE_PROSPECTIVE_VALIDATION"
    outcome = "LOW_COVERAGE_IN_FRESH_REGIME" if counts["hit"] <= 1 else "CONTINUE_PROSPECTIVE_VALIDATION"
    if document.get("decision") != verdict or document.get("coverage_outcome") != outcome:
        raise feasibility.FeasibilityBlocked("checkpoint verdict/count mismatch")
    return document


def transition_report(checkpoint) -> dict:
    """봉인된 checkpoint의 recording/coverage만 판정하며 경제값은 읽지 않는다."""
    checkpoint = _checkpoint(checkpoint, recording_required=False)
    if checkpoint["recorder_health"] != "OK":
        return {"schema_version": 1, "artifact_type": "ALPHA_CHECKPOINT_TRANSITION",
                "checkpoint_hash": feasibility.digest(checkpoint), "amendment_hash": checkpoint["amendment_hash"],
                "cohort_id": feasibility.COHORT, "frozen_build": feasibility.BUILD,
                "source_digest": feasibility.SOURCE_DIGEST, "original_manifest_hash": feasibility.MANIFEST_HASH,
                "rule_hash": feasibility.RULE_HASH, "case": None,
                **{key: checkpoint[key] for key in ("completed", "hit", "non_hit", "unknown", "recorder_health", "cursor", "backlog")},
                "active_kst_days": checkpoint["days"], "checkpoint_verdict": None,
                "recommended_next_transition": "REVIEW_RECORDING_BEFORE_COVERAGE_DECISION",
                "n3_observation": "PRESERVE_CURRENT_RUNTIME", "original_cohort_closed": False,
                "economic_evaluation": "FORBIDDEN", "activation_gate": "BLOCKED_RECORDING_REVIEW_REQUIRED",
                "candidate": CANDIDATE, "activation_now": False, "process_actions": [],
                "telemetry_runtime": "NOT_STARTED", "live_trading": "DISABLED", "project_status": "PAUSE_STRATEGY_REVIEW"}
    case = "A" if checkpoint["hit"] <= 1 else "B"
    next_transition = ("CLOSE_FEASIBILITY_AMENDMENT_AND_PREPARE_ISOLATED_TELEMETRY"
                       if case == "A" else "PRESERVE_FROZEN_N3_OBSERVATION_AND_PREPARE_ISOLATED_TELEMETRY")
    return {"schema_version": 1, "artifact_type": "ALPHA_CHECKPOINT_TRANSITION",
            "checkpoint_hash": feasibility.digest(checkpoint), "amendment_hash": checkpoint["amendment_hash"],
            "cohort_id": feasibility.COHORT, "frozen_build": feasibility.BUILD,
            "source_digest": feasibility.SOURCE_DIGEST, "original_manifest_hash": feasibility.MANIFEST_HASH,
            "rule_hash": feasibility.RULE_HASH, "case": case,
            **{key: checkpoint[key] for key in ("completed", "hit", "non_hit", "unknown", "recorder_health", "cursor", "backlog")},
            "active_kst_days": checkpoint["days"], "checkpoint_verdict": checkpoint["decision"],
            "recommended_next_transition": next_transition,
            "n3_observation": "AMENDMENT_ADMISSIONS_CLOSED_EXTERNALLY" if case == "A" else "CONTINUE_UNCHANGED_FROZEN_STREAM",
            "original_cohort_closed": False, "economic_evaluation": "FORBIDDEN",
            "activation_gate": "PENDING_ISOLATED_BOUNDARY_AND_RUNTIME_ACCEPTANCE",
            "candidate": CANDIDATE, "activation_now": False, "process_actions": [],
            "telemetry_runtime": "NOT_STARTED", "live_trading": "DISABLED",
            "project_status": "PAUSE_STRATEGY_REVIEW"}


def _amendment_boundary(checkpoint, case):
    """원래 hard-cap 종료와 구분되는 외부 annotation을 만든다."""
    return {"artifact_type": "N3_FEASIBILITY_AMENDMENT_CLOSURE" if case == "A" else "N3_CONTINUITY_EPOCH_ANNOTATION",
            "cohort_id": feasibility.COHORT, "checkpoint_hash": feasibility.digest(checkpoint),
            "amendment_hash": checkpoint["amendment_hash"], "end_event_seq": checkpoint["cursor"],
            "scope": "FEASIBILITY_AMENDMENT_ONLY" if case == "A" else "FROZEN_OBSERVATIONAL_STREAM_ONLY",
            "original_manifest_hash": feasibility.MANIFEST_HASH, "original_cohort_closed": False,
            "observer_status_change": False, "final_report_created": False,
            "gap_annotation": "No runtime cutover occurred at this checkpoint; future stop/resume timestamps and event boundaries require separate verified evidence.",
            "open_positions": "PRESERVE; no forced sale and no historical state rewrite",
            "economic_evaluation": "FORBIDDEN"}


def cutover_plan(checkpoint, *, frozen_runtime, telemetry_runtime) -> dict:
    """외부 계획만 반환한다. 기존 cutover의 CLOSED 게이트를 우회하지 않는다."""
    checkpoint = _checkpoint(checkpoint)
    frozen, target = Path(frozen_runtime).resolve(), Path(telemetry_runtime).resolve()
    if target == PROTECTED_RUNTIME or frozen == target or frozen in target.parents or target in frozen.parents:
        raise feasibility.FeasibilityBlocked("telemetry runtime must be a separate non-nested root")
    report = transition_report(checkpoint)
    return {"schema_version": 1, "artifact_type": "ALPHA_TELEMETRY_CUTOVER_DRY_RUN",
            "case": report["case"], "checkpoint_hash": report["checkpoint_hash"],
            "frozen_runtime": str(frozen), "telemetry_runtime": str(target),
            "status": report["activation_gate"], "executable_now": False,
            "single_control_buy_owner": True, "separate_ledger_required": True,
            "state_copy": "verified stopped-boundary copy preserves positions, costs, sequence IDs and immutable history; never reset",
            "control_strategy_change": False, "process_actions": [],
            "candidate_limitation": "Unmodified 86d7ddc demands original closure + final_report + observer CLOSED. The prepared explicit feasibility boundary adapter must be included in the reviewed telemetry build.",
            "reuse_gate": "Existing snapshot/create-epoch/resume require genuine original hard-cap closure or the explicit validated boundary installed in the isolated target.",
            "steps": [
                "Verify checkpoint/amendment/source identities and retain immutable external report.",
                "Capture real Control latency baseline and preregister explicit tolerances with alpha_latency_baseline.py before activation.",
                "Use reviewed explicit feasibility boundary adapter; refuse fabricated CLOSED/final_report evidence.",
                "Operator puts recovery supervisor in verified maintenance before owned-stop; capture external pre-stop PID creation/cursor/KST evidence.",
                "Operator performs owned Control shutdown; frozen build cannot acknowledge telemetry cooperative stop.",
                "Stop old observer separately; verify all old runtime processes stopped, snapshot coherent state hashes and preserve old root unchanged.",
                "Prepare reviewed candidate in distinct telemetry root; copy stopped-boundary ledger and state without reset, excluding active data/n3_shadow.",
                "Record stop/resume gap, old/new event boundaries, builds, configuration and process/session identities.",
                "Compile and full suite; create new telemetry epoch with genuine closure or approved separate provenance adapter.",
                "For CASE B register the immutable external N3 continuity segment against the new epoch before resume.",
                "Resume only the new Control runtime; retain one BUY owner and never run old/new writers together.",
                "Record external gap evidence binding before/stop/after timestamps, PID creation, sessions and exact event boundaries.",
                "Require natural predictor/reject/RPC_SKIP/BUY receipt/completed outcome acceptance and real latency baseline.",
                "Register ALPHA_BATCH_001 only after TELEMETRY_RUNTIME_ACCEPTED."
            ],
            "case_specific": ("Stop original observer only by explicit operator decision; external amendment closure does not alter original cohort."
                              if report["case"] == "A" else
                              "Keep frozen N3 records unchanged. Continue the original rule in an external observational stream over the single new Control ledger using physical predictors and offline receipt/outcome joins; record separate epoch/build segments and latency annotation, never claim unchanged frozen-build measurement."),
            "live_trading": "DISABLED", "telemetry_runtime_status": "NOT_STARTED"}


def record_transition(runtime, checkpoint, output) -> dict:
    """최초 checkpoint의 외부 전환 기록을 멱등 봉인한다. runtime 쓰기는 없다."""
    path = feasibility._external(runtime, output)
    checkpoint = _checkpoint(checkpoint, recording_required=False)
    report = transition_report(checkpoint)
    if path.exists():
        existing = feasibility.read_sealed(path)
        if (existing.get("artifact_type") != "ALPHA_CHECKPOINT_TRANSITION"
                or existing.get("checkpoint_hash") != report["checkpoint_hash"]
                or existing.get("runtime_root") != str(Path(runtime).resolve())):
            raise feasibility.FeasibilityBlocked("transition artifact mismatch")
        return existing
    feasibility.source_guard(runtime)
    document = {**report, "runtime_root": str(Path(runtime).resolve()),
                "recorded_at": datetime.now(timezone.utc).isoformat(),
                "boundary_annotation": (_amendment_boundary(checkpoint, report["case"]) if report["case"] else
                                        {"artifact_type": "N3_CHECKPOINT_RECORDING_REVIEW", "original_cohort_closed": False})}
    return feasibility._write_new(path, document)


def capture_before_stop(runtime, checkpoint, output) -> dict:
    """운영자가 정지 전에 실제 PID identity와 원장 경계를 외부에 기록한다."""
    from scripts.entry_telemetry_cutover import _active_processes
    root = Path(runtime).resolve()
    path = feasibility._external(root, output)
    checkpoint = _checkpoint(checkpoint)
    feasibility.source_guard(root)
    if path.exists():
        existing = feasibility.read_sealed(path)
        if (existing.get("artifact_type") != "ALPHA_PRE_STOP_EVIDENCE"
                or existing.get("runtime_root") != str(root)
                or existing.get("checkpoint_hash") != feasibility.digest(checkpoint)):
            raise feasibility.FeasibilityBlocked("pre-stop evidence immutable identity mismatch")
        return existing
    before = feasibility.read_document(root / "data/paper_trades.json")
    rows = _active_processes(root)
    if not rows or len(rows) > 32:
        raise feasibility.FeasibilityBlocked("pre-stop evidence requires bounded live process identities")
    identities = []
    for row in rows:
        pid = feasibility.integer(row.get("pid"), 1)
        created = row.get("created")
        if not isinstance(created, str) or not created:
            raise feasibility.FeasibilityBlocked("process creation identity missing")
        identities.append({"process_id": pid, "process_created": created})
    after = feasibility.read_document(root / "data/paper_trades.json")
    if before != after or before.get("schema_version") != 2:
        raise feasibility.FeasibilityBlocked("ledger changed during pre-stop evidence capture")
    next_seq = feasibility.integer(before.get("next_event_seq"), checkpoint["cursor"] + 1)
    now = datetime.now(timezone.utc)
    from datetime import timedelta
    document = {"schema_version": 1, "artifact_type": "ALPHA_PRE_STOP_EVIDENCE",
                "runtime_root": str(root), "checkpoint_hash": feasibility.digest(checkpoint),
                "observed_utc": now.isoformat(), "observed_kst": now.astimezone(timezone(timedelta(hours=9))).isoformat(),
                "process_identities": sorted(identities, key=lambda row: row["process_id"]),
                "next_event_seq": next_seq, "ledger_hash": feasibility.digest(before),
                "frozen_build": feasibility.BUILD,
                "session_identity": "Frozen build has no telemetry epoch/session binding; PID creation is authoritative.",
                "process_actions": []}
    return feasibility._write_new(path, document)


def record_cutover_gap(runtime, telemetry_runtime, before_evidence, output) -> dict:
    """재개 후 관측 증거를 봉인한다. 실제 stop/resume instant를 추정하지 않는다."""
    from scripts import entry_telemetry_cutover as cutover
    from src.research import entry_telemetry_epoch as epoch
    frozen, target = Path(runtime).resolve(), Path(telemetry_runtime).resolve()
    path = feasibility._external(frozen, output)
    feasibility._external(target, path)
    before = feasibility.read_sealed(before_evidence)
    boundary = read_boundary(target)
    if (before.get("artifact_type") != "ALPHA_PRE_STOP_EVIDENCE"
            or before.get("runtime_root") != str(frozen) or boundary["frozen_runtime"] != str(frozen)
            or before.get("checkpoint_hash") != boundary["checkpoint_hash"]
            or feasibility.integer(before.get("next_event_seq"), 1) > boundary["start_event_seq"]
            or feasibility.timestamp(before.get("observed_utc")) > feasibility.timestamp(boundary["created_at"])):
        raise feasibility.FeasibilityBlocked("pre-stop evidence does not match cutover boundary")
    if path.exists():
        existing = feasibility.read_sealed(path)
        if (existing.get("artifact_type") != "ALPHA_CUTOVER_GAP_EVIDENCE"
                or existing.get("before_evidence_hash") != feasibility.digest(before)
                or existing.get("boundary_hash") != feasibility.digest(boundary)
                or existing.get("frozen_runtime") != str(frozen) or existing.get("telemetry_runtime") != str(target)):
            raise feasibility.FeasibilityBlocked("gap evidence immutable identity mismatch")
        return existing
    stopped = cutover.stopped_proof(frozen)
    marker = epoch.require_epoch(target, config=cutover._paper_config(target))
    if marker.get("start_event_seq") != boundary["start_event_seq"]:
        raise feasibility.FeasibilityBlocked("resumed telemetry epoch sequence does not match cutover boundary")
    sessions = cutover._sessions(target, marker)
    rows = cutover._active_processes(target)
    registry = cutover.runner.read_json(target / "logs/local_runner.json", {})
    bindings = []
    for service in ("monitor", "risk-manager"):
        record = registry.get("services", {}).get(service)
        if not isinstance(record, dict):
            raise feasibility.FeasibilityBlocked("resumed Control service registry missing")
        identities = [record, *record.get("children", [])]
        active = [row for row in rows if row["pid"] in sessions
                  and any(cutover.runner.process_matches(row, identity) for identity in identities)]
        if len(active) != 1:
            raise feasibility.FeasibilityBlocked("resumed Control service requires one active epoch/session owner")
        row = active[0]
        binding = sessions[row["pid"]]
        bindings.append({"service": service, "process_id": row["pid"], "process_created": row["created"],
                         "session_id": binding["session_id"], "telemetry_epoch_id": marker["telemetry_epoch_id"]})
    if bindings[0]["process_id"] == bindings[1]["process_id"]:
        raise feasibility.FeasibilityBlocked("Control service process identities overlap")
    ledger = feasibility.read_document(target / "data/paper_trades.json")
    next_seq = feasibility.integer(ledger.get("next_event_seq"), boundary["start_event_seq"])
    now = datetime.now(timezone.utc)
    from datetime import timedelta
    document = {"schema_version": 1, "artifact_type": "ALPHA_CUTOVER_GAP_EVIDENCE",
                "before_evidence_hash": feasibility.digest(before), "boundary_hash": feasibility.digest(boundary),
                "checkpoint_hash": boundary["checkpoint_hash"], "case": boundary["case"],
                "frozen_runtime": str(frozen), "telemetry_runtime": str(target),
                "before_observed_utc": before["observed_utc"], "before_observed_kst": before["observed_kst"],
                "stopped_observed_utc": boundary["created_at"],
                "after_observed_utc": now.isoformat(), "after_observed_kst": now.astimezone(timezone(timedelta(hours=9))).isoformat(),
                "exact_stop_resume_instants": "UNOBSERVED; these are verified observation bounds, not fabricated instants",
                "old_process_identities": before["process_identities"], "new_process_sessions": bindings,
                "old_end_event_seq": boundary["end_event_seq"], "new_start_event_seq": marker["start_event_seq"],
                "after_next_event_seq": next_seq, "frozen_still_stopped": stopped,
                "frozen_build": feasibility.BUILD, "telemetry_build": marker["build_sha"],
                "telemetry_epoch_id": marker["telemetry_epoch_id"], "control_fingerprint": marker["config_fingerprint"],
                "single_control_buy_owner": True, "segmented_evidence": True, "process_actions": []}
    return feasibility._write_new(path, document)


def record_cutover_plan(runtime, checkpoint, telemetry_runtime, output) -> dict:
    """새 runtime도 건드리지 않고 실행 전 계획만 외부에 봉인한다."""
    path = feasibility._external(runtime, output)
    feasibility._external(telemetry_runtime, path)
    document = cutover_plan(checkpoint, frozen_runtime=runtime, telemetry_runtime=telemetry_runtime)
    feasibility.source_guard(runtime)
    return feasibility._write_new(path, document)


def boundary_path(runtime) -> Path:
    return Path(runtime).resolve() / "data/research/entry_telemetry/feasibility_boundary.json"


def supervisor_maintenance(runtime) -> dict:
    """기존 harness의 실제 disabled marker를 읽기 전용으로 확인한다."""
    marker = feasibility.read_document(Path(runtime).resolve() / "data/n3_shadow/runtime_supervisor.disabled")
    if marker.get("disabled") is not True:
        raise feasibility.FeasibilityBlocked("original supervisor must be disabled before cutover")
    at = feasibility.timestamp(marker.get("at_kst"))
    if at > datetime.now(timezone.utc):
        raise feasibility.FeasibilityBlocked("supervisor maintenance timestamp is in the future")
    return marker


def clone_state(runtime, telemetry_runtime, *, frozen_stopped_proof, target_stopped_proof) -> dict:
    """정지된 원장을 새 root에 byte 그대로 복제하며 원본에는 쓰지 않는다."""
    from src.research.entry_telemetry_epoch import state_hashes
    from src.state_store import atomic_clone_json, _reject_linked_ancestors
    _reject_linked_ancestors(Path(runtime) / "data")
    _reject_linked_ancestors(Path(telemetry_runtime) / "data")
    frozen, target = Path(runtime).resolve(), Path(telemetry_runtime).resolve()
    if target == PROTECTED_RUNTIME or frozen == target or frozen in target.parents or target in frozen.parents:
        raise feasibility.FeasibilityBlocked("state clone requires separate non-protected isolated target")
    _stopped(frozen_stopped_proof, frozen)
    _stopped(target_stopped_proof, target)
    maintenance = supervisor_maintenance(frozen)
    manifest = feasibility.source_guard(frozen)
    counts = feasibility._counts(frozen, manifest)
    if counts["recorder_health"] != "OK" or counts["backlog"] != 0:
        raise feasibility.FeasibilityBlocked("state clone requires healthy drained observer")
    if (target / "data/n3_shadow").exists():
        raise feasibility.FeasibilityBlocked("isolated clone must not create active frozen N3 source path")
    source_data, target_data = (frozen / "data").resolve(), (target / "data").resolve()
    if frozen not in source_data.parents or target not in target_data.parents:
        raise feasibility.FeasibilityBlocked("state clone data directory escaped runtime root")
    source_hashes = state_hashes(frozen)
    expected = {name: checksum for name, checksum in source_hashes.items() if Path(name).parts[0] != "n3_shadow"}
    _hash_map(expected)
    if "paper_trades.json" not in expected:
        raise feasibility.FeasibilityBlocked("source ledger missing from state clone")
    current = state_hashes(target)
    if any(name not in expected or expected[name] != checksum for name, checksum in current.items()):
        raise feasibility.FeasibilityBlocked("state clone refuses conflicting/foreign target state")
    copied = 0
    for name, checksum in expected.items():
        source, destination = frozen / "data" / name, target / "data" / name
        _reject_linked_ancestors(source)
        _reject_linked_ancestors(destination)
        if target_data not in destination.resolve().parents or source_data not in source.resolve().parents:
            raise feasibility.FeasibilityBlocked("state clone destination escaped isolated root")
        copied += atomic_clone_json(source, destination, expected_sha256=checksum)
    if state_hashes(frozen) != source_hashes or state_hashes(target) != expected:
        raise feasibility.FeasibilityBlocked("state changed while cloning; target remains isolated and activation blocked")
    supervisor_maintenance(frozen)
    return {"schema_version": 1, "artifact_type": "ALPHA_ISOLATED_STATE_CLONE",
            "frozen_runtime": str(frozen), "telemetry_runtime": str(target), "start_event_seq": counts["cursor"] + 1,
            "state_hashes": expected, "copied_files": copied, "existing_identical_files": len(expected) - copied,
            "frozen_n3_archive": "Original root retained unchanged; no active N3 manifest copied to target",
            "supervisor_maintenance_hash": feasibility.digest(maintenance), "process_actions": [], "activation_now": False}


def _stopped(proof, root):
    if (not isinstance(proof, dict) or type(proof.get("active_process_count")) is not int
            or proof["active_process_count"] != 0 or proof.get("runtime_root") != str(Path(root).resolve())):
        raise feasibility.FeasibilityBlocked("verified stopped process proof required for both roots")


def _hash_map(value):
    if not isinstance(value, dict) or not value or len(value) > 20000:
        raise feasibility.FeasibilityBlocked("boundary state hashes invalid")
    for name, checksum in value.items():
        if not isinstance(name, str):
            raise feasibility.FeasibilityBlocked("boundary state hash path invalid")
        relative = Path(name)
        if (relative.is_absolute() or ".." in relative.parts
                or not isinstance(checksum, str) or len(checksum) != 64
                or any(c not in "0123456789abcdef" for c in checksum)):
            raise feasibility.FeasibilityBlocked("boundary state hash path/value invalid")


def read_boundary(runtime) -> dict:
    """epoch의 명시적 외부 provenance를 검증한다. 거래 중 원장 hash는 재비교하지 않는다."""
    document = feasibility.read_sealed(boundary_path(runtime))
    checkpoint = _checkpoint(document.get("checkpoint", {}))
    amendment = feasibility._amendment(document.get("amendment", {}))
    manifest = document.get("original_manifest")
    if (document.get("artifact_type") != "N3_FEASIBILITY_CUTOVER_BOUNDARY"
            or document.get("schema_version") != 1
            or document.get("telemetry_runtime") != str(Path(runtime).resolve())
            or document.get("checkpoint_hash") != feasibility.digest(checkpoint)
            or document.get("amendment_hash") != feasibility.digest(amendment)
            or checkpoint["amendment_hash"] != feasibility.digest(amendment)
            or document.get("original_manifest_hash") != feasibility.MANIFEST_HASH
            or feasibility.digest(manifest) != feasibility.MANIFEST_HASH
            or document.get("frozen_build") != feasibility.BUILD
            or document.get("source_digest") != feasibility.SOURCE_DIGEST
            or document.get("cohort_id") != feasibility.COHORT
            or document.get("rule_hash") != feasibility.RULE_HASH
            or document.get("case") != transition_report(checkpoint)["case"]
            or document.get("n3_original_closed") is not False
            or document.get("segmented_evidence") is not True
            or document.get("single_control_buy_owner") is not True
            or document.get("frozen_state_hashes") != document.get("target_state_hashes")):
        raise feasibility.FeasibilityBlocked("feasibility boundary provenance mismatch")
    if feasibility.timestamp(checkpoint["recorded_at"]) < feasibility.timestamp(amendment["adopted_at"]):
        raise feasibility.FeasibilityBlocked("checkpoint predates amendment")
    start = feasibility.integer(document.get("start_event_seq"), checkpoint["cursor"] + 1)
    if document.get("end_event_seq") != start - 1:
        raise feasibility.FeasibilityBlocked("cutover event boundary mismatch")
    _hash_map(document.get("target_state_hashes"))
    _hash_map(document.get("frozen_n3_archive_hashes"))
    frozen = document.get("frozen_runtime")
    if not isinstance(frozen, str):
        raise feasibility.FeasibilityBlocked("frozen runtime reference invalid")
    cutover_plan(checkpoint, frozen_runtime=frozen, telemetry_runtime=runtime)
    _stopped(document.get("frozen_stopped_proof"), frozen)
    _stopped(document.get("target_stopped_proof"), runtime)
    feasibility.timestamp(document.get("created_at"))
    maintenance = document.get("supervisor_maintenance")
    if not isinstance(maintenance, dict) or maintenance.get("disabled") is not True:
        raise feasibility.FeasibilityBlocked("boundary supervisor maintenance evidence missing")
    feasibility.timestamp(maintenance.get("at_kst"))
    if (Path(runtime) / "data/n3_shadow/manifest.json").exists():
        raise feasibility.FeasibilityBlocked("isolated target must not activate frozen N3 capture")
    return document


def boundary_provenance(runtime) -> dict:
    document = read_boundary(runtime)
    return {"artifact_type": document["artifact_type"], "boundary_hash": feasibility.digest(document),
            "decision": document["checkpoint"]["decision"],
            **{key: document[key] for key in ("case", "cohort_id", "rule_hash", "checkpoint_hash", "amendment_hash",
                                             "original_manifest_hash", "end_event_seq", "start_event_seq", "segmented_evidence")}}


def prepare_boundary(runtime, checkpoint, amendment, telemetry_runtime, *, frozen_stopped_proof, target_stopped_proof) -> dict:
    """정지 확인과 상태 연속성 검증 후 별도 target에만 boundary를 봉인한다."""
    from src.research.entry_telemetry_epoch import state_hashes
    frozen, target = Path(runtime).resolve(), Path(telemetry_runtime).resolve()
    checkpoint = _checkpoint(checkpoint)
    amendment = feasibility._amendment(amendment)
    cutover_plan(checkpoint, frozen_runtime=frozen, telemetry_runtime=target)
    _stopped(frozen_stopped_proof, frozen)
    _stopped(target_stopped_proof, target)
    maintenance = supervisor_maintenance(frozen)
    if checkpoint["amendment_hash"] != feasibility.digest(amendment):
        raise feasibility.FeasibilityBlocked("checkpoint/amendment mismatch")
    if (target / "data/n3_shadow").exists():
        raise feasibility.FeasibilityBlocked("target data/n3_shadow must remain absent; keep frozen archive separate")
    manifest = feasibility.source_guard(frozen)
    counts = feasibility._counts(frozen, manifest)
    if (counts["recorder_health"] != "OK" or counts["backlog"] != 0
            or counts["cursor"] < checkpoint["cursor"] or counts["completed"] < checkpoint["completed"]
            or counts["hit"] < checkpoint["hit"] or counts["non_hit"] < checkpoint["non_hit"]):
        raise feasibility.FeasibilityBlocked("cutover requires healthy complete non-regressing recorder")
    # 이후 완료 거래가 늘어도 최초 checkpoint의 당시 counts를 원본에서 재확인한다.
    from datetime import timedelta
    sidecar = frozen / "data/n3_shadow"
    entries = feasibility.records(sidecar, "entries", 408)
    completed = feasibility.records(sidecar, "completed", 408)
    sells = feasibility.records(sidecar, "sells", 1632)
    cutoff = checkpoint["cursor"]
    historical = []
    for pid, entry in entries.items():
        legs = [leg for leg in sells.values() if leg.get("position_id") == pid]
        if pid in completed and legs and max(leg["event_seq"] for leg in legs) <= cutoff:
            historical.append(entry)
    days = {feasibility.timestamp(entry["signal_timestamp"]).astimezone(timezone(timedelta(hours=9))).date().isoformat()
            for entry in historical}
    expected_counts = {"completed": len(historical), "days": len(days),
                       "hit": sum(entry["would_skip"] is True for entry in historical),
                       "non_hit": sum(entry["would_skip"] is False for entry in historical),
                       "unknown": sum(entry["would_skip"] is None for entry in historical)}
    if any(checkpoint[key] != value for key, value in expected_counts.items()):
        raise feasibility.FeasibilityBlocked("checkpoint counts do not match immutable frozen evidence at cursor")
    frozen_hashes = state_hashes(frozen)
    n3_hashes = {name: checksum for name, checksum in frozen_hashes.items() if Path(name).parts[0] == "n3_shadow"}
    copied_hashes = {name: checksum for name, checksum in frozen_hashes.items() if Path(name).parts[0] != "n3_shadow"}
    actual = state_hashes(target)
    if actual != copied_hashes or "paper_trades.json" not in actual:
        raise feasibility.FeasibilityBlocked("isolated target state does not match stopped frozen boundary")
    ledger = feasibility.read_document(target / "data/paper_trades.json")
    if ledger.get("schema_version") != 2 or ledger.get("next_event_seq") != counts["cursor"] + 1:
        raise feasibility.FeasibilityBlocked("target ledger cutover sequence/schema mismatch")
    if state_hashes(frozen) != frozen_hashes or state_hashes(target) != actual:
        raise feasibility.FeasibilityBlocked("state changed while preparing cutover boundary")
    path = boundary_path(target)
    if path.exists():
        existing = read_boundary(target)
        if (existing["checkpoint_hash"] != feasibility.digest(checkpoint)
                or existing["amendment_hash"] != feasibility.digest(amendment)
                or existing["frozen_state_hashes"] != copied_hashes):
            raise feasibility.FeasibilityBlocked("conflicting immutable cutover boundary")
        return existing
    document = {"schema_version": 1, "artifact_type": "N3_FEASIBILITY_CUTOVER_BOUNDARY",
                "case": transition_report(checkpoint)["case"], "cohort_id": feasibility.COHORT,
                "rule_hash": feasibility.RULE_HASH, "original_manifest_hash": feasibility.MANIFEST_HASH,
                "frozen_build": feasibility.BUILD, "source_digest": feasibility.SOURCE_DIGEST,
                "original_manifest": manifest, "amendment": amendment, "checkpoint": checkpoint,
                "checkpoint_hash": feasibility.digest(checkpoint), "amendment_hash": feasibility.digest(amendment),
                "frozen_runtime": str(frozen), "telemetry_runtime": str(target),
                "start_event_seq": counts["cursor"] + 1, "end_event_seq": counts["cursor"],
                "frozen_state_hashes": copied_hashes, "target_state_hashes": actual,
                "frozen_n3_archive_hashes": n3_hashes, "n3_original_closed": False,
                "segmented_evidence": True, "single_control_buy_owner": True,
                "frozen_stopped_proof": frozen_stopped_proof, "target_stopped_proof": target_stopped_proof,
                "supervisor_maintenance": maintenance,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "boundary_annotation": _amendment_boundary(checkpoint, transition_report(checkpoint)["case"])}
    _hash_map(copied_hashes)
    _hash_map(n3_hashes)
    return feasibility._write_new(path, document)
