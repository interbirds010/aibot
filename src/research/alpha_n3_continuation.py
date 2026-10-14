"""새 telemetry epoch의 N3 관찰을 원래 frozen 증거와 분리해 보존한다."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from pathlib import Path

from src.research import accelerated_alpha as core
from src.research import entry_telemetry_epoch as epoch
from src.research import entry_telemetry_join as offline
from src.research import n3_shadow as n3
from src.state_store import exclusive_file_lock

MAX_SIGNALS = 2000


def registration_sources(root: Path, manifest_path: Path):
    """명시적 cutover 경계에 봉인된 원래 N3 archive만 등록 자료로 읽는다."""
    from src.research.alpha_transition import read_boundary, supervisor_maintenance
    from src.research.n3_feasibility import source_guard
    from scripts.entry_telemetry_cutover import stopped_proof
    boundary = read_boundary(root)
    frozen = Path(boundary["frozen_runtime"])
    expected_path = frozen / "data/n3_shadow/manifest.json"
    if manifest_path.resolve() != expected_path.resolve():
        raise ValueError("continuation original manifest must reference frozen boundary archive")
    stopped_proof(frozen)
    supervisor_maintenance(frozen)
    source_guard(frozen)
    actual = {name: checksum for name, checksum in epoch.state_hashes(frozen).items()
              if Path(name).parts[0] == "n3_shadow"}
    if actual != boundary["frozen_n3_archive_hashes"]:
        raise ValueError("continuation original baseline archive hash mismatch")
    manifest = n3.read_immutable(expected_path)
    if n3.digest(manifest) != boundary["original_manifest_hash"]:
        raise ValueError("continuation original manifest boundary hash mismatch")
    return manifest, n3.read_records(expected_path.parent, "entries", 408), n3.read_records(expected_path.parent, "completed", 408)


def register_continuation(output: Path, manifest: dict, marker: dict, *, baseline_entries=(), baseline_completed=()) -> dict:
    """새 Control 시작 전에 동일 rule의 별도 관찰 segment를 봉인한다."""
    output = core._output(output)
    if manifest.get("contract") != n3.CONTRACT or manifest.get("contract_hash") != n3.digest(n3.CONTRACT):
        raise ValueError("N3 original contract mismatch")
    boundary = marker.get("n3_closure", {})
    if boundary.get("decision") != "N3_CONTINUE_PROSPECTIVE_VALIDATION":
        raise ValueError("continuation requires CASE B feasibility boundary")
    if boundary.get("cohort_id") != manifest.get("cohort_id"):
        raise ValueError("continuation cohort mismatch")
    if boundary.get("original_manifest_hash") != n3.digest(manifest):
        raise ValueError("continuation original manifest boundary hash mismatch")
    start = marker.get("start_event_seq")
    if not core._positive_int(start) or core.utc(marker.get("start_utc")) < core.utc(manifest["start_utc"]):
        raise ValueError("continuation boundary invalid")
    if not marker.get("telemetry_epoch_id") or not marker.get("build_sha") or not marker.get("config_fingerprint"):
        raise ValueError("continuation provenance missing")
    baseline = []
    seen = set()
    for entry in baseline_entries:
        if len(baseline) >= 408:
            raise ValueError("original N3 baseline entry bound exceeded")
        if entry.get("cohort_id") != manifest["cohort_id"] or entry.get("rule_hash") != n3.RULE_HASH or \
                not core._positive_int(entry.get("entry_token_amount_raw")) or entry.get("position_id") in seen \
                or not isinstance(entry.get("position_id"), str) or not entry["position_id"] \
                or not core._positive_int(entry.get("buy_event_seq")) or entry["buy_event_seq"] >= start \
                or (entry.get("would_skip") is not None and type(entry["would_skip"]) is not bool):
            raise ValueError("original N3 baseline entry invalid")
        if core.utc(entry["signal_timestamp"]) < core.utc(manifest["start_utc"]):
            raise ValueError("original N3 baseline signal before cohort")
        seen.add(entry["position_id"])
        baseline.append({k: entry[k] for k in ("position_id", "signal_timestamp", "buy_event_seq", "entry_token_amount_raw", "would_skip")})
        baseline[-1]["source_entry_hash"] = n3.digest(entry)
    completed_ids = sorted(entry["position_id"] for entry in baseline_completed)
    if len(set(completed_ids)) != len(completed_ids) or set(completed_ids) - seen or len(baseline) > 408:
        raise ValueError("original N3 baseline population invalid")
    if (len(completed_ids) >= n3.CONTRACT["hard_cap"]["completed_control"]
            or len({n3.kst_day(entry["signal_timestamp"]) for entry in baseline}) >= n3.CONTRACT["hard_cap"]["active_kst_days"]):
        raise ValueError("original N3 hard cap already reached; continuation admissions prohibited")
    document = {"schema_version": 1, "artifact_type": "N3_TELEMETRY_CONTINUATION_SEGMENT",
        "cohort_id": manifest["cohort_id"], "original_manifest_hash": n3.digest(manifest),
        "original_build": manifest["build"], "rule": n3.RULE, "rule_hash": n3.RULE_HASH,
        "contract_hash": manifest["contract_hash"], "telemetry_epoch_id": marker["telemetry_epoch_id"],
        "build_sha": marker["build_sha"], "config_fingerprint": marker["config_fingerprint"],
        "start_event_seq": start, "start_utc": marker["start_utc"],
        "boundary_hash": n3.digest(boundary),
        "baseline_entries": sorted(baseline, key=lambda e: e["buy_event_seq"]),
        "baseline_completed_ids": completed_ids, "hard_cap": dict(n3.CONTRACT["hard_cap"]),
        "semantics": "same N3 rule; separate build/latency segment; never relabel as frozen evidence",
        "control_changes": False, "automatic_economic_evaluation": False}
    n3.immutable(output / "continuity.json", document)
    return document


def _time(predictor: dict, key: str):
    value = predictor["predictors"]["timestamps"].get(key)
    return n3.epoch(value.get("wall_utc")) if isinstance(value, dict) else None


def _pair_buy(event, receipt):
    identity = receipt["identity"]
    if (event.get("type") != "BUY" or event.get("event_seq") != identity["event_seq"]
            or event.get("position_id") != identity["trade_id"]
            or event.get("mint") != identity["mint"] or event.get("route_type") != identity["route_type"]
            or core.utc(event.get("signal_detected_at")) != core.utc(identity["signal_detected_at"])
            or not core._positive_int(event.get("token_amount_raw"))):
        raise ValueError("continuation ledger BUY identity mismatch")


def _scan_ledger(segment, receipts, ledger, closure):
    """완전한 원장과 수량을 검증한 뒤 저장할 경계/SELL 목록만 만든다."""
    if (ledger.get("schema_version") != 2 or not isinstance(ledger.get("events"), list)
            or len(ledger["events"]) > 100000 or not core._positive_int(ledger.get("next_event_seq"))
            or ledger["next_event_seq"] < segment["start_event_seq"]):
        raise ValueError("continuation ledger schema/scan bound invalid")
    by_seq, buys = {}, {}
    for event in ledger["events"]:
        if not isinstance(event, dict) or not core._positive_int(event.get("event_seq")):
            raise ValueError("continuation ledger event invalid")
        seq = event["event_seq"]
        if seq in by_seq or seq >= ledger["next_event_seq"]:
            raise ValueError("continuation ledger duplicate/order")
        by_seq[seq] = event
        if event.get("type") == "BUY":
            buys[seq] = event
    start = segment["start_event_seq"]
    open_baseline = [entry for entry in segment["baseline_entries"]
                     if entry["position_id"] not in segment["baseline_completed_ids"]]
    required_start = min([start, *(entry["buy_event_seq"] for entry in open_baseline)])
    if ledger["next_event_seq"] - required_start > 100000 or any(
            seq not in by_seq for seq in range(required_start, ledger["next_event_seq"])):
        raise ValueError("continuation ledger missing event sequence")
    for entry in open_baseline:
        original_buy = by_seq[entry["buy_event_seq"]]
        if (original_buy.get("type") != "BUY" or original_buy.get("position_id") != entry["position_id"]
                or original_buy.get("token_amount_raw") != entry["entry_token_amount_raw"]):
            raise ValueError("carried original BUY identity/quantity mismatch")
    admissions = {entry["position_id"]: entry for entry in segment["baseline_entries"]}
    completed = set(segment["baseline_completed_ids"])
    days = {n3.kst_day(entry["signal_timestamp"]) for entry in admissions.values()}
    baseline_ids = set(admissions)
    sold, completion_seq, carried = {}, {}, []
    buy_rows = {}
    for receipt in receipts:
        seq = receipt["identity"].get("event_seq")
        if not core._positive_int(seq) or seq in buy_rows:
            raise ValueError("continuation duplicate/invalid receipt event_seq")
        buy_rows[seq] = receipt
        event = buys.get(seq)
        if event is None:
            raise ValueError("continuation receipt has no Control BUY")
        _pair_buy(event, receipt)
    limit = closure["end_event_seq"] if closure else ledger["next_event_seq"] - 1
    if not core._positive_int(limit) or limit >= ledger["next_event_seq"]:
        raise ValueError("continuation closure boundary invalid")
    pending_closure = None
    for seq in sorted(by_seq):
        if seq > limit:
            break
        event = by_seq[seq]
        pid = event.get("position_id")
        if seq >= start and event.get("type") == "BUY":
            receipt = buy_rows.get(seq)
            if receipt is None:
                raise ValueError("continuation BUY receipt pending; retry after flush")
            if pid in admissions:
                raise ValueError("continuation duplicate BUY position_id")
            signal_stamp = receipt["identity"]["signal_detected_at"]
            if core.utc(signal_stamp) >= core.utc(segment["start_utc"]):
                admissions[pid] = {"position_id": pid, "entry_token_amount_raw": event["token_amount_raw"]}
                days.add(n3.kst_day(signal_stamp))
        if event.get("type") == "SELL" and pid in admissions:
            amount = event.get("token_amount_raw")
            if not core._positive_int(amount):
                raise ValueError("continuation SELL quantity invalid")
            sold[pid] = sold.get(pid, 0) + amount
            if sold[pid] > admissions[pid]["entry_token_amount_raw"]:
                raise ValueError("continuation SELL exceeds entry")
            if sold[pid] == admissions[pid]["entry_token_amount_raw"]:
                completed.add(pid)
                completion_seq[pid] = seq
            if seq >= start and pid in baseline_ids:
                carried.append((str(seq) + ".json", {"cohort_id": segment["cohort_id"], "source_event": event,
                    "semantics": "original admission; new epoch SELL; original classification unchanged"}))
        if seq >= start and not closure and (
                len(days) >= segment["hard_cap"]["active_kst_days"]
                or len(completed) >= segment["hard_cap"]["completed_control"]):
            core.utc(event.get("at"))
            pending_closure = {"artifact_type": "N3_SEGMENTED_HARD_CAP", "cohort_id": segment["cohort_id"],
                "end_event_seq": seq, "end_utc": event["at"], "reason": "ORIGINAL_HARD_CAP",
                "active_kst_days": len(days), "completed_control": len(completed),
                "censored_position_ids": sorted(set(admissions) - completed)}
            closure = pending_closure
            break
    return buys, closure, pending_closure, carried, completion_seq


def observe_rows(output: Path, predictors: list[dict], receipts: list[dict], outcomes: list[dict], *, ledger=None) -> dict:
    """모든 검증을 마친 뒤에만 관찰 기록을 봉인한다. Control은 변경하지 않는다."""
    output = core._output(output)
    segment = n3.read_immutable(output / "continuity.json")
    if not segment or segment.get("rule_hash") != n3.RULE_HASH or segment.get("rule") != n3.RULE:
        raise ValueError("continuation segment missing or rule mismatch")
    if any(len(rows) > MAX_SIGNALS for rows in (predictors, receipts, outcomes)):
        raise ValueError("partition continuation input to at most 2000 rows per stream")
    joined = offline.join_streams(predictors, receipts, outcomes)
    from scripts.entry_telemetry_acceptance import validate_predictor
    # orphan도 epoch/build/config 오염으로 제외되지 않도록 전체 물리 행을 확인한다.
    for stream in (predictors, receipts, outcomes):
        for row in stream:
            if (any(row.get(key) != segment[key] for key in ("telemetry_epoch_id", "build_sha"))
                    or row["provenance"].get("config_fingerprint") != segment["config_fingerprint"]):
                raise ValueError("N3 continuation epoch/build/config mismatch")
    for predictor in predictors:
        validate_predictor(predictor)
    now = datetime.now(timezone.utc).isoformat()
    appended = 0
    with exclusive_file_lock(output / "observe_transaction", operation="n3_continuation"):
        closure = n3.read_immutable(output / "segment_close.json")
        buys, pending_closure, carried, completion_seq = {}, None, [], {}
        if ledger is not None:
            buys, closure, pending_closure, carried, completion_seq = _scan_ledger(segment, receipts, ledger, closure)
        elif segment["baseline_entries"]:
            raise ValueError("carried positions require Control ledger for original hard-cap continuity")
        pending = []
        for joined_row in joined["rows"]:
            predictor, receipt = joined_row["predictor"], joined_row["receipt"]
            signal = core.utc(predictor["identity"]["signal_detected_at"])
            if signal < core.utc(segment["start_utc"]) or predictor["decision_outcome"] != "BUY" or receipt is None:
                continue
            buy_seq = receipt["identity"].get("event_seq")
            if not core._positive_int(buy_seq) or buy_seq < segment["start_event_seq"]:
                raise ValueError("N3 continuation BUY before boundary")
            if closure and buy_seq > closure["end_event_seq"]:
                continue
            buy = buys.get(buy_seq)
            decision = _time(predictor, "entry_decision_at")
            analysis = n3.epoch(buy.get("analysis_completed_at")) if buy else None
            preflight = n3.epoch(buy.get("entry_quote_at")) if buy else None
            clock_source = "ORIGINAL_CONTROL_BUY_ENTRY_TIMESTAMPS" if buy else "UNVERIFIED_NO_CONTROL_BUY_CLOCK"
            if buy is not None:
                _pair_buy(buy, receipt)
                buy_at = n3.epoch(buy.get("at"))
                if decision is not None and (buy_at is None or decision > buy_at):
                    raise ValueError("N3 decision after Control BUY")
                for native, name in ((analysis, "analysis_completed_at"), (preflight, "preflight_completed_at")):
                    physical = _time(predictor, name)
                    if native is not None and physical is not None and native > physical:
                        raise ValueError("N3 original clock occurs after physical telemetry mark")
            valid = None not in (analysis, preflight, decision) and signal.timestamp() <= analysis <= preflight <= decision
            inputs = {"analysis_duration_sec": analysis - signal.timestamp() if valid else None,
                      "quote_preflight_duration_sec": preflight - analysis if valid else None}
            record = {"schema_version": 1, "cohort_id": segment["cohort_id"],
                "telemetry_epoch_id": segment["telemetry_epoch_id"], "build_sha": segment["build_sha"],
                "rule_hash": n3.RULE_HASH, "signal_id": predictor["identity"]["signal_id"],
                "position_id": receipt["identity"]["trade_id"], "buy_event_seq": buy_seq,
                "signal_timestamp": predictor["identity"]["signal_detected_at"], "raw_inputs": inputs,
                "would_skip": n3.classify(inputs), "timestamp_semantics": clock_source,
                "missing_reason": None if valid else "invalid_or_missing_decision_timestamp",
                "predictor_hash": predictor["content_hash"], "receipt_hash": receipt["content_hash"]}
            identity = hashlib.sha256(record["signal_id"].encode()).hexdigest()
            path = output / "entries" / (identity + ".json")
            existing = n3.read_immutable(path)
            if existing and {key: value for key, value in existing.items() if key != "recorded_at"} != record:
                raise ValueError("conflicting N3 continuation entry")
            stored = existing or {**record, "recorded_at": now}
            pending.append((path, stored))
            outcome = joined_row["outcome"]
            if outcome is not None and (not closure or outcome["identity"]["event_seq"] <= closure["end_event_seq"]):
                if ledger is not None:
                    sell = next((event for event in ledger["events"]
                                 if event["event_seq"] == outcome["identity"]["event_seq"]), None)
                    if (sell is None or sell.get("type") != "SELL" or sell.get("position_id") != record["position_id"]
                            or outcome["identity"]["event_seq"] <= buy_seq
                            or completion_seq.get(record["position_id"]) != outcome["identity"]["event_seq"]):
                        raise ValueError("continuation completed outcome ledger identity mismatch")
                pending.append((output / "outcomes" / (identity + ".json"), {
                    "schema_version": 1, "cohort_id": segment["cohort_id"],
                    "telemetry_epoch_id": segment["telemetry_epoch_id"], "signal_id": record["signal_id"],
                    "position_id": record["position_id"], "entry_hash": n3.digest(stored),
                    "source_outcome_hash": outcome["content_hash"], "source_outcome": outcome}))
        pending.extend((output / "carried_sells" / name, record) for name, record in carried)
        if pending_closure is not None:
            pending.append((output / "segment_close.json", pending_closure))
        # 충돌 역시 어느 새 파일도 저장하기 전에 전체를 확인한다.
        for path, document in pending:
            existing = n3.read_immutable(path)
            if existing and existing != document:
                raise ValueError("conflicting immutable N3 continuation record")
        for path, document in pending:
            created = n3.immutable(path, document)
            if path.parent.name == "entries":
                appended += created
    return {"status": "N3_CONTINUATION_OBSERVED", "appended_entries": appended,
        "pending_buy_receipts": sum(row["join_status"] == "MISSING_RECEIPT" for row in joined["rows"]),
        "orphan_receipts": len(joined["orphan_receipts"]), "orphan_outcomes": len(joined["orphan_outcomes"]),
        "control_changes": False, "economic_evaluation": False}


def observe(root: Path, output: Path, *, config: dict) -> dict:
    """검증된 활성 epoch의 물리 stream을 외부 관찰기에 공급한다."""
    marker = epoch.require_epoch(root, config=config)
    output = core._output(output)
    if Path(root).resolve() == output or Path(root).resolve() in output.parents:
        raise ValueError("continuation output must be outside Control runtime")
    segment = n3.read_immutable(output / "continuity.json")
    if segment.get("telemetry_epoch_id") != marker["telemetry_epoch_id"]:
        raise ValueError("continuation active epoch mismatch")
    streams = []
    for name in offline.SCHEMAS:
        rows = []
        for row in offline.iter_stream(root, marker["telemetry_epoch_id"], name):
            if name == "predictors" and row.get("decision_outcome") != "BUY":
                continue
            if len(rows) >= MAX_SIGNALS:
                raise ValueError("continuation scan exceeds 2000 rows; partition offline input")
            rows.append(row)
        streams.append(rows)
    from src.state_store import read_json
    return observe_rows(output, *streams, ledger=read_json(Path(root) / "data/paper_trades.json", {}))
