"""Frozen N3를 수정하지 않는 외부 feasibility checkpoint. 경제 평가는 수행하지 않는다."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess

BUILD = "8f0b9429352c9961b895a9f9c0e2d1f3e6b19d05"
COHORT = "cba13fbb-c702-4b18-bcf7-20b78a8e252f"
RULE_HASH = "b219661c31c132e1b2f11901664981482a9e42dc847ab300f590072eb3271165"
SOURCE_DIGEST = "f048db0dd0799fbfb8e38ecd00e69ae2b9c5813ee2ab3ac0ced56790c83fab5e"
MANIFEST_HASH = "0798898d823ea44d341130a350acd2831f80514740fade1389679814e5c7ed3b"
START_SEQ = 10901
GATES = {"completed_control": 30, "active_kst_days": 7, "operator": "OR"}
POLICY = {"low_coverage_max_hits": 1, "continue_min_hits": 2,
          "economic_evaluation": "FORBIDDEN", "recording_required": "OK_WITH_NO_UNKNOWNS"}
DISCLOSURE = "Adopted after 17 completed Control trades and zero N3 hits had already been observed; this is a new feasibility amendment, not the original historical protocol."


class FeasibilityBlocked(RuntimeError):
    """기록/identity 확인 전에는 checkpoint 판정하지 않는다."""


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def read_document(path):
    def reject(value):
        raise FeasibilityBlocked("nonfinite JSON")
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise FeasibilityBlocked("duplicate JSON key")
            result[key] = value
        return result
    with Path(path).open(encoding="utf-8") as handle:
        value = json.load(handle, parse_constant=reject, object_pairs_hook=pairs)
    if not isinstance(value, dict):
        raise FeasibilityBlocked("document must be an object")
    return value


def read_sealed(path):
    value = read_document(path)
    seal = value.pop("content_hash", None)
    # 경제 필드도 불변 seal 검증에 포함하지만 값을 꺼내거나 평가하지 않는다.
    if seal != digest(value):
        raise FeasibilityBlocked("immutable content hash mismatch")
    return value


def source_guard(runtime):
    root = Path(runtime).resolve()
    manifest = read_sealed(root / "data/n3_shadow/manifest.json")
    if digest(manifest) != MANIFEST_HASH or manifest.get("cohort_id") != COHORT or manifest.get("start_event_seq") != START_SEQ:
        raise FeasibilityBlocked("frozen manifest/cohort mismatch")
    build = manifest.get("build", {})
    expected = build.get("source_files", {})
    if (build.get("git_sha") != BUILD or build.get("source_digest") != SOURCE_DIGEST
            or len(expected) != 56 or digest(expected) != SOURCE_DIGEST
            or manifest.get("contract", {}).get("rule_hash") != RULE_HASH
            or manifest.get("contract_hash") != digest(manifest.get("contract"))):
        raise FeasibilityBlocked("frozen contract/build mismatch")
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True,
                          text=True, check=True, timeout=15,
                          creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    paths = list((root / "src").rglob("*.py")) + list((root / "scripts").glob("*.py"))
    paths += [root / "requirements.txt", root / "ecosystem.config.js"]
    actual = {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths if p.is_file()}
    if head.stdout.strip() != BUILD or actual != expected:
        raise FeasibilityBlocked("frozen HEAD/source mismatch")
    return manifest


def integer(value, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise FeasibilityBlocked("invalid integer")
    return value


def timestamp(value):
    if not isinstance(value, str):
        raise FeasibilityBlocked("missing timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise FeasibilityBlocked("timestamp lacks timezone")
    return parsed


def classification(inputs, rule):
    if not isinstance(inputs, dict):
        raise FeasibilityBlocked("invalid classifier inputs")
    values = [inputs.get("quote_preflight_duration_sec"), inputs.get("analysis_duration_sec")]
    if any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) or x < 0 for x in values):
        return None
    outside, greater = rule["parts"]
    return (values[0] <= outside["low"] or values[0] > outside["high"]) and values[1] > greater["threshold"]


def records(sidecar, kind, limit):
    rows = {}
    for path in sorted((sidecar / kind).glob("*.json")):
        if len(rows) >= limit:
            raise FeasibilityBlocked("record bound exceeded")
        row = read_sealed(path)
        identity = str(integer(row.get("event_seq"), 1)) if kind == "sells" else row.get("position_id")
        if not isinstance(identity, str) or not identity or hashlib.sha256(identity.encode()).hexdigest() != path.stem or identity in rows:
            raise FeasibilityBlocked("record identity/filename mismatch")
        rows[identity] = row
    return rows


def _counts(runtime, manifest):
    root = Path(runtime).resolve()
    sidecar = root / "data/n3_shadow"
    # Observer가 sidecar를 먼저 쓰고 state를 마지막으로 교체하므로 version 재확인한다.
    for _ in range(3):
        before = read_document(sidecar / "observer_state.json")
        entries = records(sidecar, "entries", 408)
        completed = records(sidecar, "completed", 408)
        snapshots = records(sidecar, "snapshots", 408)
        sells = records(sidecar, "sells", 1632)
        ledger = read_document(root / "data/paper_trades.json")
        after = read_document(sidecar / "observer_state.json")
        if before == after:
            break
    else:
        raise FeasibilityBlocked("observer snapshot changed during read")
    if before.get("cohort_id") != COHORT or before.get("status") not in ("RUNNING", "CLOSED"):
        raise FeasibilityBlocked("observer identity/status invalid")
    cursor = integer(before.get("cursor"), START_SEQ - 1)
    next_seq = integer(ledger.get("next_event_seq"), START_SEQ)
    if ledger.get("schema_version") != 2 or cursor >= next_seq:
        raise FeasibilityBlocked("ledger cursor/schema invalid")
    events = ledger.get("events")
    if not isinstance(events, list):
        raise FeasibilityBlocked("ledger events invalid")
    by_seq = {}
    for event in events:
        if not isinstance(event, dict):
            raise FeasibilityBlocked("invalid event")
        seq = integer(event.get("event_seq"), 1)
        if seq in by_seq:
            raise FeasibilityBlocked("duplicate event sequence")
        by_seq[seq] = event
    if any(seq not in by_seq for seq in range(START_SEQ, next_seq)):
        raise FeasibilityBlocked("missing event sequence")
    hit = non_hit = unknown = 0
    days = set()
    missing_fields = {}
    completed_from_quantity = set()
    for captured in snapshots.values():
        if captured.get("build") != manifest["build"] or captured.get("cohort_id") != COHORT or captured.get("rule_hash") != RULE_HASH:
            raise FeasibilityBlocked("snapshot identity/build mismatch")
    for pid, entry in entries.items():
        if entry.get("cohort_id") != COHORT or entry.get("rule_hash") != RULE_HASH:
            raise FeasibilityBlocked("entry cohort/rule mismatch")
        seq = integer(entry.get("buy_event_seq"), START_SEQ)
        buy = by_seq.get(seq, {})
        if seq > cursor or buy.get("type") != "BUY" or buy.get("position_id") != pid or entry.get("buy_event_id") != buy.get("event_id"):
            raise FeasibilityBlocked("entry BUY pairing mismatch")
        signal = timestamp(entry.get("signal_timestamp"))
        if entry.get("signal_timestamp") != buy.get("signal_detected_at") or signal < timestamp(manifest["start_utc"]):
            raise FeasibilityBlocked("entry fresh boundary mismatch")
        snap = entry.get("snapshot")
        if not isinstance(snap, dict):
            raise FeasibilityBlocked("entry snapshot missing")
        value = entry.get("would_skip")
        if value is not None and type(value) is not bool:
            raise FeasibilityBlocked("classification must be bool or null")
        if value != classification(snap.get("raw_inputs", {}), manifest["contract"]["rule"]) or value != snap.get("would_skip"):
            raise FeasibilityBlocked("classification mismatch")
        if pid in snapshots:
            captured = snapshots[pid]
            if captured != snap or captured.get("build") != manifest["build"] or captured.get("cohort_id") != COHORT or captured.get("rule_hash") != RULE_HASH:
                raise FeasibilityBlocked("snapshot identity/build mismatch")
        elif value is not None:
            raise FeasibilityBlocked("valid entry snapshot missing")
        if value is not None:
            analysis = timestamp(snap.get("analysis_end_timestamp"))
            preflight = timestamp(snap.get("preflight_end_timestamp"))
            decision = timestamp(snap.get("entry_decision_timestamp"))
            if (not signal <= analysis <= preflight <= decision <= timestamp(buy.get("at"))
                    or snap.get("signal_timestamp") != entry.get("signal_timestamp")
                    or snap.get("analysis_end_timestamp") != buy.get("analysis_completed_at")
                    or snap.get("preflight_end_timestamp") != buy.get("entry_quote_at")
                    or snap["raw_inputs"] != {"analysis_duration_sec": analysis.timestamp() - signal.timestamp(),
                                               "quote_preflight_duration_sec": preflight.timestamp() - analysis.timestamp()}):
                raise FeasibilityBlocked("snapshot timing reconstruction mismatch")
        fields = snap.get("missing_fields")
        if not isinstance(fields, list) or any(not isinstance(field, str) for field in fields):
            raise FeasibilityBlocked("missing-field recording invalid")
        for field in fields:
            missing_fields[field] = missing_fields.get(field, 0) + 1
        legs = sorted((s for s in sells.values() if s.get("position_id") == pid), key=lambda s: s["event_seq"])
        amount = integer(entry.get("entry_token_amount_raw"), 1)
        if amount != integer(buy.get("token_amount_raw"), 1):
            raise FeasibilityBlocked("BUY quantity pairing mismatch")
        sold = 0
        for leg in legs:
            sell_seq = integer(leg.get("event_seq"), seq + 1)
            event = by_seq.get(sell_seq, {})
            if sell_seq > cursor or event.get("type") != "SELL" or any(event.get(k) != leg.get(k) for k in ("position_id", "event_id", "at", "token_amount_raw")):
                raise FeasibilityBlocked("SELL pairing mismatch")
            sold += integer(leg.get("token_amount_raw"), 1)
        if sold > amount:
            raise FeasibilityBlocked("SELL quantity exceeds entry")
        if sold == amount:
            completed_from_quantity.add(pid)
        if pid in completed:
            row = completed[pid]
            keys = ("cohort_id", "rule_hash", "position_id", "buy_event_id", "buy_event_seq", "signal_timestamp", "entry_token_amount_raw", "would_skip", "snapshot")
            if any(row.get(key) != entry.get(key) for key in keys) or sold != amount or row.get("closed_at") != legs[-1].get("at") or row.get("sell_event_count") != len(legs):
                raise FeasibilityBlocked("completed pairing mismatch")
            days.add(signal.astimezone(timezone(timedelta(hours=9))).date().isoformat())
            hit += value is True
            non_hit += value is False
            unknown += value is None
    if set(completed) != completed_from_quantity or any(s.get("position_id") not in entries for s in sells.values()):
        raise FeasibilityBlocked("completed/SELL population mismatch")
    if any(s.get("buy_event_seq") is None for s in completed.values()):
        raise FeasibilityBlocked("completed missing entry marker")
    unknown_entries = sum(e.get("would_skip") is None for e in entries.values())
    expected = {"admitted_count": len(entries), "completed_count": len(completed), "open_count": len(entries) - len(completed),
                "sell_record_count": len(sells), "missing_snapshot_count": unknown_entries, "missing_field_counts": missing_fields}
    if any(before.get(k) != v for k, v in expected.items()):
        raise FeasibilityBlocked("observer counts disagree with records")
    # Cursor 안의 fresh BUY/SELL은 모두 sidecar에 대응해야 한다.
    for seq, event in by_seq.items():
        if START_SEQ <= seq <= cursor:
            if event.get("type") == "BUY" and timestamp(event.get("signal_detected_at")) >= timestamp(manifest["start_utc"]) and event.get("position_id") not in entries:
                raise FeasibilityBlocked("fresh BUY missing from entries")
            if event.get("type") == "SELL" and event.get("position_id") in entries and str(seq) not in sells:
                raise FeasibilityBlocked("SELL missing from sidecar")
    recording_ok = before.get("recorder_health") == "OK" and unknown_entries == 0 and not missing_fields
    return {"completed": len(completed), "days": len(days), "hit": hit, "non_hit": non_hit,
            "unknown": unknown, "coverage": (hit / (hit + non_hit)) if hit + non_hit else None,
            "recorder_health": "OK" if recording_ok else "RECORDING_REVIEW_REQUIRED",
            "backlog": next_seq - cursor - 1, "cursor": cursor}


def _external(runtime, output):
    root, path = Path(runtime).resolve(), Path(output).resolve()
    if path == root or root in path.parents:
        raise FeasibilityBlocked("output must be outside frozen runtime")
    return path


def _write_new(path, document):
    from src.state_store import atomic_write_json, exclusive_file_lock
    path.parent.mkdir(parents=True, exist_ok=True)
    with exclusive_file_lock(path, operation="n3_external_feasibility"):
        if path.exists():
            existing = read_sealed(path)
            if existing != document:
                raise FeasibilityBlocked("conflicting immutable external artifact")
            return existing
        atomic_write_json(path, {**document, "content_hash": digest(document)})
    return document


def create_amendment(runtime, output, adopted_at=None):
    """명시적 등록 시 현재 관찰 count와 원래 manifest를 외부에서 봉인한다."""
    path = _external(runtime, output)
    manifest = source_guard(runtime)
    counts = _counts(runtime, manifest)
    adopted = adopted_at or datetime.now(timezone.utc).isoformat()
    if timestamp(adopted) < timestamp(manifest["start_utc"]):
        raise FeasibilityBlocked("amendment adoption predates cohort")
    document = {"schema_version": 1, "artifact_type": "N3_FEASIBILITY_AMENDMENT",
                "cohort_id": COHORT, "frozen_build": BUILD, "rule_hash": RULE_HASH,
                "original_manifest_hash": MANIFEST_HASH, "adopted_at": adopted,
                "disclosure": DISCLOSURE, "already_observed_completed_at_request": 17,
                "observed_at_adoption": counts, "checkpoint_gate": GATES, "decision_policy": POLICY,
                "project_status": "PAUSE_STRATEGY_REVIEW", "live_trading": "DISABLED"}
    return _write_new(path, document)


def _amendment(value):
    document = read_sealed(value) if isinstance(value, (str, Path)) else dict(value)
    if (document.get("artifact_type") != "N3_FEASIBILITY_AMENDMENT" or document.get("cohort_id") != COHORT
            or document.get("frozen_build") != BUILD or document.get("rule_hash") != RULE_HASH
            or document.get("original_manifest_hash") != MANIFEST_HASH or document.get("checkpoint_gate") != GATES
            or document.get("decision_policy") != POLICY or document.get("disclosure") != DISCLOSURE
            or document.get("already_observed_completed_at_request") != 17):
        raise FeasibilityBlocked("amendment identity/policy mismatch")
    timestamp(document.get("adopted_at"))
    return document


def checkpoint_status(runtime, amendment):
    """현재 gate와 recording/coverage만 반환한다. 운영 파일 쓰기는 없다."""
    try:
        amendment = _amendment(amendment)
        counts = _counts(runtime, source_guard(runtime))
        if counts["completed"] < amendment["observed_at_adoption"]["completed"] or counts["cursor"] < amendment["observed_at_adoption"]["cursor"]:
            raise FeasibilityBlocked("record population/cursor regressed after adoption")
        reached = counts["completed"] >= GATES["completed_control"] or counts["days"] >= GATES["active_kst_days"]
        decision = None
        outcome = None
        if reached and counts["recorder_health"] == "OK":
            decision = "N3_LOW_FRESH_COVERAGE" if counts["hit"] <= 1 else "N3_CONTINUE_PROSPECTIVE_VALIDATION"
            outcome = "LOW_COVERAGE_IN_FRESH_REGIME" if counts["hit"] <= 1 else "CONTINUE_PROSPECTIVE_VALIDATION"
        return {**counts, "cohort_id": COHORT, "original_manifest_hash": MANIFEST_HASH,
                "amendment_hash": digest(amendment), "checkpoint_status": "CHECKPOINT_REACHED" if reached else "WAITING_FOR_30_OR_7D",
                "event": "N3_FEASIBILITY_CHECKPOINT_REACHED" if reached else None,
                "decision": decision, "coverage_outcome": outcome,
                "phase": "FEASIBILITY_ONLY", "project_status": "PAUSE_STRATEGY_REVIEW"}
    except (FeasibilityBlocked, OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError) as exc:
        return {"checkpoint_status": "BLOCKED", "recorder_health": "UNVERIFIED", "phase": "FEASIBILITY_ONLY", "reason": str(exc)}


def record_checkpoint(runtime, amendment, output):
    """최초 도달 기록을 봉인하고 이후 count/판정 변화로 재작성하지 않는다."""
    path = _external(runtime, output)
    registered = _amendment(amendment)
    if path.exists():
        existing = read_sealed(path)
        if existing.get("artifact_type") != "N3_FEASIBILITY_CHECKPOINT" or existing.get("amendment_hash") != digest(registered):
            raise FeasibilityBlocked("checkpoint artifact mismatch")
        return existing
    report = checkpoint_status(runtime, registered)
    if report["checkpoint_status"] != "CHECKPOINT_REACHED":
        return None
    document = {**report, "artifact_type": "N3_FEASIBILITY_CHECKPOINT",
                "recorded_at": datetime.now(timezone.utc).isoformat()}
    from src.state_store import atomic_write_json, exclusive_file_lock
    path.parent.mkdir(parents=True, exist_ok=True)
    with exclusive_file_lock(path, operation="n3_first_feasibility_checkpoint"):
        if path.exists():
            existing = read_sealed(path)
            if existing.get("amendment_hash") != digest(registered):
                raise FeasibilityBlocked("checkpoint artifact mismatch")
            return existing
        atomic_write_json(path, {**document, "content_hash": digest(document)})
    return document
