"""Control과 격리된 N3 진입 snapshot 및 append-only 연구 관찰기."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import platform
import queue
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from src.state_store import atomic_write_json, exclusive_file_lock, read_json, update_json

ROOT = Path(__file__).resolve().parents[2]
SIDECAR = ROOT / "data" / "n3_shadow"
EXPERIMENT_VERSION = "n3_prospective_shadow_v1"
RULE_VERSION = "historical_n3_20261002_v1"
RULE = {"op": "AND", "parts": [
    {"feature": "quote_preflight_duration_sec", "op": "outside",
     "low": 4.165494775772094, "high": 4.808962464332581},
    {"feature": "analysis_duration_sec", "op": "gt", "threshold": 1.497460412979126},
]}
RULE_HASH = "b219661c31c132e1b2f11901664981482a9e42dc847ab300f590072eb3271165"
CONTRACT = {
    "experiment_version": EXPERIMENT_VERSION, "rule_version": RULE_VERSION,
    "rule": RULE, "rule_hash": RULE_HASH,
    "canonical_serialization": "UTF-8; sorted keys; compact JSON; no newline; allow_nan=false",
    "outside": "value <= low OR value > high", "gt": "value > threshold",
    "predictors": {"analysis_duration_sec": "analysis_completed_at_epoch - signal_timestamp_epoch",
                   "quote_preflight_duration_sec": "preflight_end_epoch - analysis_completed_at_epoch"},
    "missing_policy": "UNKNOWN; would_skip=null; Control unchanged; separate missing stratum",
    "inclusion": "signal_timestamp >= start_utc AND BUY event_seq >= start_event_seq; fresh Control only",
    "primary_unit": "paired delta per completed Control trade; pnl/original_entry_cost; equal 1 SOL bookkeeping",
    "secondary_unit": "actual recorded lamports; skipped delta=-Control realized PnL; nonhit delta=0",
    "sample_gates": {"completed_control": 200, "valid_hits": 40, "valid_non_hits": 40,
                     "control_winners": 20, "active_kst_days": 14},
    "route_sample_gates": {"completed_control": 200, "valid_hits": 40, "valid_non_hits": 40,
                           "control_winners": 20, "active_kst_days": 14},
    "hard_cap": {"active_kst_days": 28, "completed_control": 400},
    "stop_policy": "close admissions only at first hard-cap event; never stop at minimum gates or intermediate performance",
    "active_day": "minimum gate: KST signal/entry days represented by completed Control trades; hard cap: all eligible BUY days including open entries",
    "open_at_close": "censor positions still open at hard cap; freeze evaluation at end_event_seq; no forced sale or extension",
    "bootstrap": {"method": "paired whole KST entry-day cluster resampling", "seed": 20261003,
                  "repetitions": 5000, "confidence": 0.95},
    "cost_scenarios": [
        {"name": "recorded_paper", "extra_round_trip_bps": 0, "fixed_round_trip_lamports": 0},
        {"name": "additional_50bps", "extra_round_trip_bps": 50, "fixed_round_trip_lamports": 0},
        {"name": "additional_100bps_plus_10000lamports", "extra_round_trip_bps": 100,
         "fixed_round_trip_lamports": 10000}],
    "cost_limitations": "sensitivity only; not live fill/price-impact/capital recycling simulation; unrecorded network/Jito costs unmodeled",
    "verdict": {
        "insufficient": "any minimum sample gate missing",
        "failed": "normalized net<=0 OR avoided<=missed OR either temporal half net<0 OR top2 positive normalized deltas removed net<=0",
        "robust": "normalized day-cluster CI lower>0 AND actual net>=0 AND both halves positive AND top3 removal positive AND all cost scenario normalized effects positive AND no unknown classifications",
        "promising": "positive point estimate without robust evidence; never implies profitable Alpha"},
    "hard_stops": ["missing event sequence", "malformed/conflicting immutable sidecar",
                   "contract/rule/build mismatch", "invalid SELL aggregation", "more than 408 admitted positions"],
    "challengers": ["N3"], "project_status": "PAUSE_STRATEGY_REVIEW", "phase": "PAPER_MVP_RUNNING",
}
logger = logging.getLogger("n3-shadow")
SESSION = str(uuid4())
_queue: queue.Queue = queue.Queue(maxsize=16)
_worker: threading.Thread | None = None
_worker_lock = threading.Lock()
_capture_failures = 0


def canonical(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def epoch(value: object) -> float | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        number = parsed.timestamp() if parsed.tzinfo else float("nan")
        return number if math.isfinite(number) else None
    except (ValueError, OverflowError, OSError):
        return None


def kst_day(value: str) -> str:
    stamp = epoch(value)
    if stamp is None:
        raise ValueError("invalid timestamp")
    return datetime.fromtimestamp(stamp, timezone(timedelta(hours=9))).date().isoformat()


def classify(inputs: dict) -> bool | None:
    values = [inputs.get("quote_preflight_duration_sec"), inputs.get("analysis_duration_sec")]
    if any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) or x < 0
           for x in values):
        return None
    preflight, analysis = values
    return (preflight <= RULE["parts"][0]["low"] or preflight > RULE["parts"][0]["high"]) and analysis > RULE["parts"][1]["threshold"]


def prepare_entry_snapshot(*, signal_timestamp: str, analysis_start: str | None,
                           analysis_end: str, preflight_start: str | None,
                           preflight_end: str, entry_decision: str,
                           quote_timestamp: str | None) -> dict:
    """사후값 없이 historical과 동일한 epoch 차이를 진입 전에 고정한다."""
    signal, analysis, preflight, decision = map(epoch, (signal_timestamp, analysis_end, preflight_end, entry_decision))
    valid = None not in (signal, analysis, preflight, decision) and signal <= analysis <= preflight <= decision
    inputs = {"analysis_duration_sec": analysis - signal if valid else None,
              "quote_preflight_duration_sec": preflight - analysis if valid else None}
    quote = epoch(quote_timestamp)
    quote_age = (decision - quote) * 1000 if valid and quote is not None and quote <= decision else None
    timestamps = {"signal_timestamp": signal_timestamp, "analysis_start_timestamp": analysis_start,
                  "analysis_end_timestamp": analysis_end, "preflight_start_timestamp": preflight_start,
                  "preflight_end_timestamp": preflight_end, "entry_decision_timestamp": entry_decision,
                  "quote_timestamp": quote_timestamp, "quote_age_ms": quote_age}
    missing = [key for key, value in timestamps.items() if value is None]
    if not valid:
        missing.extend(inputs)
    return {**timestamps, "entry_decision_semantics": "post-preflight shadow evaluation instant before observation decision persistence and BUY",
            "quote_timestamp_semantics": "local receipt after jupiter_quote returns; not provider generation time",
            "raw_inputs": inputs, "would_skip": classify(inputs),
            "missing_fields": missing, "missing_reason": "invalid_or_missing_timestamp" if not valid else
            ("not_available" if missing else None), "control_process_id": os.getpid(),
            "control_session_id": SESSION, "platform": sys.platform,
            "clock_semantics": "UTC wall-clock epoch subtraction; legacy elapsed intervals include scheduling/state/rate-limit waits"}


def build_identity(root: Path) -> dict:
    """비밀 파일을 제외한 실행 소스의 SHA를 백그라운드에서 기록한다."""
    result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, check=True,
                            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    paths = sorted(list((root / "src").rglob("*.py")) + list((root / "scripts").glob("*.py")) +
                   [root / "requirements.txt", root / "ecosystem.config.js"])
    source = {str(path.relative_to(root)).replace("\\", "/"): hashlib.sha256(path.read_bytes()).hexdigest()
              for path in paths if path.is_file()}
    return {"git_sha": result.stdout.strip(), "source_digest": digest(source), "source_files": source}


def read_immutable(path: Path) -> dict:
    """JSON 문법이 맞는 손상도 내용 해시로 검출한다."""
    if not path.exists():
        return {}
    document = read_json(path, {})
    content_hash = document.pop("content_hash", None)
    if not isinstance(content_hash, str) or content_hash != digest(document):
        raise RuntimeError("N3 immutable content hash mismatch")
    return document


def immutable(path: Path, document: dict) -> bool:
    """기존 연구 기록은 재작성하지 않고 동일 재시도만 허용한다."""
    with exclusive_file_lock(path, operation="n3_immutable_record"):
        if path.exists():
            if read_immutable(path) != document:
                raise RuntimeError("conflicting immutable N3 record")
            return False
        canonical(document)
        atomic_write_json(path, {**document, "content_hash": digest(document)})
        return True


def record_path(sidecar: Path, kind: str, identity: str) -> Path:
    return sidecar / kind / (hashlib.sha256(identity.encode("utf-8")).hexdigest() + ".json")


def load_manifest(sidecar: Path) -> dict:
    manifest = read_immutable(sidecar / "manifest.json")
    if not manifest:
        raise RuntimeError("N3_NOT_REGISTERED")
    if (digest(RULE) != RULE_HASH or manifest.get("contract") != CONTRACT or
            manifest.get("contract_hash") != digest(CONTRACT)):
        raise RuntimeError("N3 contract mismatch")
    if epoch(manifest.get("start_utc")) is None or not isinstance(manifest.get("start_event_seq"), int):
        raise RuntimeError("malformed N3 start marker")
    return manifest


def register(root: Path, sidecar: Path, locked_path: Path) -> dict:
    """명시적 명령에서만 원장 락 안의 미래 경계를 한 번 생성한다."""
    locked = read_json(locked_path, {})
    candidates = [item for item in locked.get("selected_candidates", []) if item.get("candidate_id") == "N3"]
    if len(candidates) != 1 or candidates[0].get("rule") != RULE or candidates[0].get("exclude_condition") is not True:
        raise RuntimeError("locked historical N3 differs")
    if digest(RULE) != RULE_HASH:
        raise RuntimeError("canonical N3 hash mismatch")
    build = build_identity(root)
    with exclusive_file_lock(sidecar / "manifest.json", operation="n3_register"):
        if (sidecar / "manifest.json").exists():
            existing = load_manifest(sidecar)
            if existing["build"] != build:
                raise RuntimeError("N3 registered build mismatch")
            return existing
        ledger_path = root / "data" / "paper_trades.json"
        with exclusive_file_lock(ledger_path, operation="n3_start_boundary"):
            ledger = read_json(ledger_path, {})
            if ledger.get("schema_version") != 2 or not isinstance(ledger.get("next_event_seq"), int):
                raise RuntimeError("invalid Control ledger")
            manifest = {"schema_version": 1, "cohort_id": str(uuid4()), "start_utc": now_iso(),
                        "start_event_seq": ledger["next_event_seq"], "contract": CONTRACT,
                        "contract_hash": digest(CONTRACT), "build": build,
                        "source_locked_file_sha256": hashlib.sha256(locked_path.read_bytes()).hexdigest()}
            atomic_write_json(sidecar / "manifest.json", {**manifest, "content_hash": digest(manifest)})
    return manifest


def eligible(manifest: dict, buy: dict) -> bool:
    stamp = epoch(buy.get("signal_detected_at"))
    return stamp is not None and stamp >= epoch(manifest["start_utc"]) and buy["event_seq"] >= manifest["start_event_seq"]


def persist_capture(sidecar: Path, position_id: str, snapshot: dict, build: dict) -> None:
    if not (sidecar / "manifest.json").exists() or (sidecar / "closure.json").exists():
        return
    manifest = load_manifest(sidecar)
    if build != manifest["build"]:
        raise RuntimeError("N3 build mismatch")
    signal = epoch(snapshot.get("signal_timestamp"))
    if signal is None or signal < epoch(manifest["start_utc"]):
        return
    document = {"schema_version": 1, "cohort_id": manifest["cohort_id"], "experiment_version": EXPERIMENT_VERSION,
                "rule_version": RULE_VERSION, "rule_hash": RULE_HASH, "build": build,
                "position_id": position_id, **snapshot, "os_version": platform.platform()}
    immutable(record_path(sidecar, "snapshots", position_id), document)


def _capture_loop() -> None:
    global _capture_failures
    build = None
    while True:
        position_id, snapshot = _queue.get()
        try:
            if build is None:
                build = build_identity(ROOT)
            persist_capture(SIDECAR, position_id, snapshot, build)
        except Exception as exc:
            _capture_failures += 1
            logger.warning("N3 recorder failure; Control 유지 category=%s", type(exc).__name__)
            try:
                update_json(SIDECAR / "capture_health.json", {}, lambda doc: doc.update(
                    {"failure_count": _capture_failures, "last_failure_at": now_iso(),
                     "category": type(exc).__name__, "session_id": SESSION}), operation="n3_capture_health")
            except Exception:
                pass
        finally:
            _queue.task_done()


def start_capture_worker() -> None:
    """서비스 시작 단계에서만 백그라운드 writer를 준비한다."""
    global _worker
    with _worker_lock:
        if _worker is None:
            _worker = threading.Thread(target=_capture_loop, name="n3-recorder", daemon=True)
            _worker.start()


def submit_control_entry(position_id: str, snapshot: dict | None) -> bool:
    """작은 bounded queue만 사용하며 저장 실패는 이미 실행한 Control과 격리한다."""
    global _capture_failures
    try:
        if snapshot is None or _worker is None or not _worker.is_alive():
            return False
        _queue.put_nowait((position_id, snapshot))
        return True
    except Exception as exc:
        _capture_failures += 1
        logger.warning("N3 recorder queue failure; Control 유지 category=%s", type(exc).__name__)
        return False


def read_records(sidecar: Path, kind: str, limit: int) -> list[dict]:
    records = []
    for path in sorted((sidecar / kind).glob("*.json")):
        if len(records) >= limit:
            raise RuntimeError("N3 record limit exceeded")
        record = read_immutable(path)
        identity = str(record.get("event_seq")) if kind == "sells" else record.get("position_id")
        if not isinstance(identity, str) or record_path(sidecar, kind, identity) != path:
            raise RuntimeError("N3 record filename/identity mismatch")
        records.append(record)
    return records


def completed_trades(sidecar: Path, *, end_event_seq: int | None = None) -> list[dict]:
    entries = read_records(sidecar, "entries", 408)
    if end_event_seq is not None:
        entries = [entry for entry in entries if entry["buy_event_seq"] <= end_event_seq]
    sells: dict[str, list] = {}
    for event in read_records(sidecar, "sells", 1632):
        if end_event_seq is not None and event["event_seq"] > end_event_seq:
            continue
        sells.setdefault(event["position_id"], []).append(event)
    completed = []
    for entry in entries:
        legs = sorted(sells.get(entry["position_id"], []), key=lambda item: item["event_seq"])
        amount = sum(item["token_amount_raw"] for item in legs)
        if amount > entry["entry_token_amount_raw"]:
            raise RuntimeError("N3 SELL exceeds entry amount")
        if amount != entry["entry_token_amount_raw"]:
            continue
        pnl = sum(item["realized_pnl_lamports"] for item in legs)
        proceeds = sum(item["proceeds_lamports"] for item in legs)
        if proceeds - entry["entry_cost_lamports"] != pnl:
            raise RuntimeError("N3 realized cost invariant failed")
        completed.append({**entry, "realized_pnl_lamports": pnl,
                          "normalized_return": pnl / entry["entry_cost_lamports"],
                          "shadow_delta_actual_lamports": -pnl if entry["would_skip"] is True else
                          (0 if entry["would_skip"] is False else None),
                          "normalized_delta": -pnl / entry["entry_cost_lamports"] if entry["would_skip"] is True else
                          (0 if entry["would_skip"] is False else None),
                          "closed_at": legs[-1]["at"], "sell_event_count": len(legs)})
    return completed


def _entry_record(manifest: dict, buy: dict, snapshot: dict) -> dict:
    if buy.get("cost_lamports", 0) <= 0 or buy.get("token_amount_raw", 0) <= 0:
        raise RuntimeError("invalid N3 BUY amounts")
    return {"schema_version": 1, "cohort_id": manifest["cohort_id"], "experiment_version": EXPERIMENT_VERSION,
            "rule_version": RULE_VERSION, "rule_hash": RULE_HASH, "position_id": buy["position_id"],
            "buy_event_id": buy["event_id"], "buy_event_seq": buy["event_seq"], "mint": buy["mint"],
            "family": {"A": "SMART_MONEY", "B": "MOMENTUM"}.get(buy.get("route_type"), "UNKNOWN"),
            "strategy_version": buy.get("strategy_version"), "signal_timestamp": buy["signal_detected_at"],
            "entry_cost_lamports": buy["cost_lamports"], "entry_token_amount_raw": buy["token_amount_raw"],
            "entry_price_lamports_per_raw_token": buy["cost_lamports"] / buy["token_amount_raw"],
            "snapshot": snapshot, "would_skip": snapshot.get("would_skip")}


def observe_once(root: Path, sidecar: Path, *, snapshot_grace_seconds: float = 30) -> dict:
    """단일 observer만 연구 cursor를 변경하며 Control 원장은 읽기만 한다."""
    manifest = load_manifest(sidecar)
    if build_identity(root) != manifest["build"]:
        raise RuntimeError("N3 observer build mismatch")
    with exclusive_file_lock(sidecar / "observer_state.json", operation="n3_observer"):
        state = read_json(sidecar / "observer_state.json", {"schema_version": 1, "cohort_id": manifest["cohort_id"],
                          "cursor": manifest["start_event_seq"] - 1})
        if state.get("cohort_id") != manifest["cohort_id"]:
            raise RuntimeError("N3 observer identity mismatch")
        ledger = read_json(root / "data" / "paper_trades.json", {})
        events = sorted([x for x in ledger["events"] if x["event_seq"] > state["cursor"]], key=lambda x: x["event_seq"])
        if ledger["next_event_seq"] - 1 > state["cursor"] and (not events or events[0]["event_seq"] != state["cursor"] + 1):
            raise RuntimeError("N3 missing Control event sequence")
        closure = read_immutable(sidecar / "closure.json")
        entries = {x["position_id"]: x for x in read_records(sidecar, "entries", 408)}
        prior_entries = sum(x["buy_event_seq"] <= state["cursor"] for x in entries.values())
        prior_sells = sum(x["event_seq"] <= state["cursor"] for x in read_records(sidecar, "sells", 1632))
        prior_completed = len(completed_trades(sidecar, end_event_seq=state["cursor"]))
        if (prior_entries != state.get("admitted_count", 0) or prior_sells != state.get("sell_record_count", 0)
            or prior_completed != state.get("completed_count", 0)):
            raise RuntimeError("N3 committed sidecar records missing or inconsistent")
        for event in events:
            if closure and event["event_seq"] > closure["end_event_seq"]:
                break
            if event["event_seq"] != state["cursor"] + 1:
                raise RuntimeError("N3 noncontiguous Control event sequence")
            pid = event.get("position_id")
            if event["type"] == "BUY" and not closure and eligible(manifest, event):
                path = record_path(sidecar, "snapshots", pid)
                # 최초 admission 이후 늦게 도착한 snapshot으로 분류를 바꾸지 않는다.
                snapshot = entries[pid]["snapshot"] if pid in entries else read_immutable(path)
                captured = pid not in entries and bool(snapshot)
                if not snapshot and time.time() - (epoch(event.get("at")) or time.time()) < snapshot_grace_seconds:
                    break
                if not snapshot:
                    snapshot = {"would_skip": None, "raw_inputs": {"analysis_duration_sec": None,
                                "quote_preflight_duration_sec": None}, "missing_fields": ["decision_snapshot"],
                                "missing_reason": "recorder_snapshot_unavailable"}
                elif captured and (snapshot.get("cohort_id") != manifest["cohort_id"] or snapshot.get("rule_hash") != RULE_HASH
                      or snapshot.get("build") != manifest["build"] or snapshot.get("position_id") != pid
                      or snapshot.get("signal_timestamp") != event.get("signal_detected_at")
                      or snapshot.get("would_skip") != classify(snapshot.get("raw_inputs", {}))):
                    raise RuntimeError("N3 snapshot identity/classification mismatch")
                if snapshot.get("would_skip") is not None:
                    reconstructed = prepare_entry_snapshot(
                        signal_timestamp=snapshot["signal_timestamp"],
                        analysis_start=snapshot.get("analysis_start_timestamp"),
                        analysis_end=snapshot["analysis_end_timestamp"],
                        preflight_start=snapshot.get("preflight_start_timestamp"),
                        preflight_end=snapshot["preflight_end_timestamp"],
                        entry_decision=snapshot["entry_decision_timestamp"],
                        quote_timestamp=snapshot.get("quote_timestamp"))
                    if (reconstructed["raw_inputs"] != snapshot["raw_inputs"]
                        or reconstructed["would_skip"] != snapshot["would_skip"]
                        or snapshot["analysis_end_timestamp"] != event.get("analysis_completed_at")
                        or snapshot["preflight_end_timestamp"] != event.get("entry_quote_at")
                        or epoch(snapshot["entry_decision_timestamp"]) > epoch(event["at"])):
                        raise RuntimeError("N3 decision-time provenance mismatch")
                entry = _entry_record(manifest, event, snapshot)
                immutable(record_path(sidecar, "entries", pid), entry)
                entries[pid] = entry
                if len(entries) > 408:
                    raise RuntimeError("N3 admission limit exceeded")
            if event["type"] == "SELL" and pid in entries:
                sell = {key: event[key] for key in ("event_id", "event_seq", "position_id", "at", "token_amount_raw",
                                                  "proceeds_lamports", "realized_pnl_lamports")}
                immutable(record_path(sidecar, "sells", str(event["event_seq"])), sell)
            state["cursor"] = event["event_seq"]
            days = {kst_day(x["signal_timestamp"]) for x in entries.values() if x["buy_event_seq"] <= state["cursor"]}
            completed = completed_trades(sidecar, end_event_seq=state["cursor"]) if event["type"] == "SELL" and pid in entries else []
            if not closure and (len(days) >= CONTRACT["hard_cap"]["active_kst_days"] or
                                len(completed) >= CONTRACT["hard_cap"]["completed_control"]):
                closure = {"schema_version": 1, "cohort_id": manifest["cohort_id"], "end_event_seq": state["cursor"],
                           "end_utc": event["at"], "reason": "HARD_CAP", "active_kst_days": len(days)}
                immutable(sidecar / "closure.json", closure)
            if closure and state["cursor"] >= closure["end_event_seq"]:
                break
        trades = completed_trades(sidecar, end_event_seq=closure.get("end_event_seq", state["cursor"]))
        for trade in trades:
            immutable(record_path(sidecar, "completed", trade["position_id"]), trade)
        pending = len(entries) - len(trades)
        state.update({"version": int(state.get("version", 0)) + 1, "last_observed_at": now_iso(),
                      "observer_pid": os.getpid(), "observer_session_id": SESSION,
                      "status": "CLOSED" if closure else "RUNNING",
                      "admitted_count": len(entries), "completed_count": len(trades), "open_count": pending,
                      "sell_record_count": sum(x["event_seq"] <= state["cursor"] for x in read_records(sidecar, "sells", 1632)),
                      "missing_snapshot_count": sum(x["would_skip"] is None for x in entries.values()),
                      "missing_field_counts": {field: sum(field in x["snapshot"].get("missing_fields", []) for x in entries.values())
                                               for field in sorted({name for x in entries.values()
                                                                    for name in x["snapshot"].get("missing_fields", [])})},
                      "recorder_health": "MISSING_SNAPSHOTS" if any(x["would_skip"] is None for x in entries.values()) else "OK"})
        atomic_write_json(sidecar / "observer_state.json", state)
        if closure:
            from src.research.n3_shadow_evaluation import evaluate_closed_cohort
            report = evaluate_closed_cohort(trades, CONTRACT)
            completed_ids = {item["position_id"] for item in trades}
            immutable(sidecar / "final_report.json", {"cohort_id": manifest["cohort_id"], "closure": closure,
                       "censored_position_ids": sorted(set(entries) - completed_ids), **report})
        return state


def main() -> None:
    parser = argparse.ArgumentParser(description="N3 research sidecar; Control 거래 변경 없음")
    parser.add_argument("command", choices=("register", "status", "observe", "watch"))
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--sidecar", type=Path)
    parser.add_argument("--locked-rule", type=Path)
    args = parser.parse_args()
    sidecar = args.sidecar or args.root / "data" / "n3_shadow"
    if args.command == "register":
        if args.locked_rule is None:
            parser.error("register requires --locked-rule")
        manifest = register(args.root, sidecar, args.locked_rule)
        print(canonical({key: manifest[key] for key in ("cohort_id", "start_utc", "start_event_seq", "contract_hash")}))
    elif args.command == "status":
        manifest = load_manifest(sidecar)
        print(canonical({"cohort_id": manifest["cohort_id"], "start_utc": manifest["start_utc"],
                         "start_event_seq": manifest["start_event_seq"], "rule_hash": RULE_HASH,
                         "build_sha": manifest["build"]["git_sha"], "state": read_json(sidecar / "observer_state.json", {})}))
    else:
        with exclusive_file_lock(sidecar / "watch_owner", timeout_seconds=0, operation="n3_watch_owner"):
            while True:
                try:
                    state = observe_once(args.root, sidecar)
                except Exception as exc:
                    update_json(sidecar / "health.json", {}, lambda doc: doc.update(
                        {"status": "BLOCKED", "category": type(exc).__name__, "at": now_iso(),
                         "observer_pid": os.getpid()}), operation="n3_observer_failure")
                    raise
                if args.command == "observe" or state["status"] == "CLOSED":
                    print(canonical(state))
                    break
                time.sleep(5)


if __name__ == "__main__":
    main()
