"""진입 당시 근거만 담는 bounded 연구 sidecar. 실패는 거래와 격리한다."""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from contextvars import ContextVar
from datetime import datetime, timezone
import hashlib
from itertools import islice
import json
import math
import os
from pathlib import Path
import platform
import queue
import subprocess
import sys
import threading
import time
from uuid import uuid4

from src.state_store import atomic_write_json, exclusive_file_lock, read_json
from src.research import entry_predictor_schema as predictor_schema
from src.research import entry_telemetry_config as config_contract

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_VERSION = 2
SCHEMAS = {"predictor": 2, "receipt": 1, "outcome": 1}
MAX_ROW_BYTES = 64 * 1024
MAX_ITEMS = 128
MAX_STRING = 256
MAX_DEPTH = 5
MAX_CAPTURE_NODES = 256
MAX_CAPTURE_CHARACTERS = 8192
TIMESTAMPS = frozenset({
    "signal_enqueued_at", "analysis_started_at", "analysis_completed_at",
    "risk_check_started_at", "risk_check_completed_at", "preflight_started_at",
    "quote_request_started_at", "quote_received_at", "entry_decision_at",
    "quote_buy_request_started_at", "quote_buy_received_at",
    "quote_exit_preflight_request_started_at", "quote_exit_preflight_received_at",
    "analysis_semaphore_acquired_at", "preflight_completed_at",
})
QUOTE_FIELDS = frozenset({"quote_timestamp", "quote_age_at_decision_sec", "quote_age_ms",
    "input_amount", "expected_output", "price_impact_pct", "route_count", "route_hash",
    "selected_route_hash", "dex_identifiers", "route_identifiers_truncated", "same_notional_quote_delta", "last_error_type", "missing_reason"})
SECTION_FIELDS = {
    "signal": frozenset({"source", "strategy_family", "signal_timestamp", "sol_amount",
        "detected_at", "source_wallet_hash", "source_signature_hash", "dex_id", "pair_id_hash",
        "source_token_amount_raw", "source_token_decimals", "source_paid_lamports", "discovery_source", "prefilter_reasons"}),
    "scores": frozenset({"safety", "momentum", "raw_components", "uncapped_total", "capped_total",
        "threshold_result", "missing_reason"}),
    "wallets": frozenset({"wallet_hashes", "wallet_ids", "unique_wallet_count", "uncapped_wallet_count",
        "uncapped_whale_count", "capped_whale_count", "repeated_wallet_count", "contributions",
        "top_wallet_contribution", "concentration_ratio", "simultaneous_wallet_arrival_count", "missing_reason",
        "observed_whale_count", "observed_unique_wallet_count", "participating_wallet_ids", "paid_lamports_by_wallet", "count_is_lower_bound"}),
    "short_flow": frozenset({"windows", "cadence_sec", "missing_reason", "window_seconds", "buy_count",
        "sell_count", "combined_volume_usd", "short_windows_missing_reason"}),
    "trajectory": frozenset({"price_now", "liquidity_now", "volume_now", "history", "missing_reason",
        "schema_version", "collector_version", "prospective_collection_start", "snapshot_count", "pre_signal_snapshots"}),
    "ages": frozenset({"mint_first_seen_at", "pool_first_seen_at", "pair_created_at", "pair_age_sec",
        "observation_age_sec", "signal_age_sec", "pair_age_seconds", "missing_reason"}),
    "pressure": frozenset({"open_positions_count", "concurrent_candidate_count", "concurrent_analysis_count",
        "queue_depth", "pending_quote_count", "recent_signal_count", "recent_signal_window_sec", "missing_reason",
        "pending_signal_tasks", "pending_shadow_tasks", "analysis_semaphore_available", "permitted_analysis_concurrency", "risk_check_scope"}),
    "wallet_performance": frozenset({"entry_time_snapshot", "missing_reason"}),
    "rpc": frozenset({"last_error_type", "missing_reason", "scope", "confirmation_missing_reason"}),
    "rpc_errors": frozenset({"last_error_type"}),
    "quote_buy_errors": frozenset({"last_error_type"}),
    "quote_exit_preflight_errors": frozenset({"last_error_type"}),
    "analyzer_source": frozenset({"cache_hit", "shared_flight_owner", "raw_components_missing_reason", "rpc_attempt_attribution"}),
    "safety_components": frozenset({"actualrawinputs", "actualallocated", "raw_inputs", "allocated_components",
        "uncapped_total", "capped_total", "cap_applied", "max_possible_total", "thresholds", "threshold_result",
        "mint_authority_renounced", "developer_supply_raw", "developer_supply_percent_raw", "lp_locked_percent_raw",
        "liquidity_usd_raw", "components", "maximum_possible_total"}),
    "quote_buy": QUOTE_FIELDS,
    "quote_exit_preflight": QUOTE_FIELDS,
    "decision": frozenset({"outcome", "reason", "reasons", "missing_reason"}),
}
COUNTERS = frozenset({"rpc_attempt_count", "rpc_retry_count", "quote_attempt_count", "quote_retry_count",
    "limiter_wait_count", "transient_error_count", "quote_buy_attempt_count", "quote_buy_retry_count",
    "quote_exit_preflight_attempt_count", "quote_exit_preflight_retry_count"})
COUNTERS = COUNTERS | frozenset({"quote_buy_transient_error_count", "quote_exit_preflight_transient_error_count",
    "quote_limiter_wait_count", "rpc_transient_error_count", "rpc_limiter_wait_count",
    "rpc_failover_attempt_count", "rpc_local_retry_count"})
DURATIONS = frozenset({"scheduler_wait_sec", "queue_wait_sec", "limiter_wait_sec", "retry_sleep_sec",
    "rpc_request_sec", "quote_request_sec", "quote_buy_request_sec", "quote_exit_preflight_request_sec"})
DURATIONS = DURATIONS | frozenset({"quote_buy_request_duration_sec", "quote_buy_reservation_duration_sec",
    "quote_buy_retry_sleep_sec", "quote_exit_preflight_request_duration_sec", "quote_exit_preflight_reservation_duration_sec",
    "quote_exit_preflight_retry_sleep_sec", "quote_limiter_wait_duration_sec", "rpc_request_duration_sec",
    "rpc_retry_sleep_sec", "rpc_limiter_wait_duration_sec", "rpc_reservation_duration_sec", "rpc_reservation_queue_wait_sec"})
CONFIG_FIELDS = config_contract.CONFIG_KEYS
OUTCOMES = frozenset({"BUY", "REJECT_ANALYZER", "REJECT_RISK", "QUOTE_FAILED", "RPC_SKIPPED", "OTHER"})
_current: ContextVar = ContextVar("entry_telemetry", default=None)
_queue: queue.Queue = queue.Queue(maxsize=16)
_receipt_queue: queue.Queue = queue.Queue(maxsize=16)
_outcome_queue: queue.Queue = queue.Queue(maxsize=16)
_worker = None
_worker_lock = threading.Lock()
_health_lock = threading.Lock()
_health = {"status": "TELEMETRY_READY", "dropped_row_count": 0, "write_error_count": 0,
    "duplicate_count": 0, "conflict_count": 0, "last_error": None}
_session = str(uuid4())
_process_start = datetime.now(timezone.utc).isoformat()
_root = ROOT
_provenance = {}
_epoch = None
_role = "monitor"
SECTION_SHAPES = predictor_schema.section_shapes(SECTION_FIELDS)


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value):
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _now():
    return datetime.now(timezone.utc).isoformat()


def _forbidden(key):
    return predictor_schema.forbidden(key)


def _safe(value, depth=0, budget=None):
    """메모리/행 크기를 제한하고 secret 및 미래 label 키를 재귀 제외한다."""
    if budget is None:
        budget = [MAX_CAPTURE_NODES, MAX_CAPTURE_CHARACTERS]
    if budget[0] <= 0:
        return None
    budget[0] -= 1
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        # URL과 인증 토큰은 raw feature가 아니다.
        if "://" in value or value.lower().startswith("bearer "):
            return None
        text = value[:min(MAX_STRING, max(0, budget[1]))]
        budget[1] -= len(text)
        return text
    if depth >= MAX_DEPTH:
        return None
    if isinstance(value, dict):
        return {str(key)[:80]: _safe(item, depth + 1, budget) for key, item in islice(value.items(), MAX_ITEMS)
                if isinstance(key, str) and not _forbidden(key)}
    if isinstance(value, (list, tuple)):
        return [_safe(item, depth + 1, budget) for item in value[:MAX_ITEMS]]
    return None


def _degrade(reason, *, write=False, conflict=False, dropped=False, stream=None):
    with _health_lock:
        _health["status"] = "TELEMETRY_DEGRADED"
        _health["last_error"] = str(reason)[:80]
        _health["degraded_event_count"] = _health.get("degraded_event_count", 0) + 1
        if write:
            _health["write_error_count"] += 1
        if dropped:
            _health["dropped_row_count"] += 1
        if conflict:
            _health["conflict_count"] += 1
        if stream:
            counters = _health.setdefault("streams", {}).setdefault(stream, {
                "written_count": 0, "dropped_row_count": 0, "write_error_count": 0,
                "duplicate_count": 0, "conflict_count": 0})
            for flag, key in ((write, "write_error_count"), (dropped, "dropped_row_count"), (conflict, "conflict_count")):
                if flag:
                    counters[key] += 1


class Capture:
    def __init__(self, identity):
        self.identity = identity
        self.timestamps = {key: None for key in TIMESTAMPS}
        self.timestamps["signal_detected_at"] = {"wall_utc": identity["signal_detected_at"],
            "monotonic_ns": None, "semantics": "upstream UTC; historical monotonic unavailable"}
        self.sections = {name: {key: None for key in fields} for name, fields in SECTION_FIELDS.items()}
        for section in self.sections.values():
            if "missing_reason" in section:
                section["missing_reason"] = "not_available_in_existing_flow"
        self.counters = {key: None for key in COUNTERS}
        self.durations = {key: None for key in DURATIONS}
        self.receipt = {}
        self.frozen = False
        self.finished = False
        self.safe_budget = [MAX_CAPTURE_NODES, MAX_CAPTURE_CHARACTERS]


def begin_signal(*, mint, route_type, signal_detected_at, source_wallet=None, source_signature=None, signal_id=None):
    """I/O 없이 후보 identity를 만든다. 불완전한 입력은 계측만 생략한다."""
    try:
        if not isinstance(mint, str) or not mint or len(mint) > MAX_STRING or route_type not in ("A", "B"):
            return None
        for value in (source_wallet, source_signature, signal_id):
            if value is not None and (not isinstance(value, str) or len(value) > MAX_STRING):
                _degrade("identity_input_too_large_or_invalid", dropped=True)
                return None
        if not isinstance(signal_detected_at, str) or len(signal_detected_at) > 80:
            _degrade("timestamp_input_too_large_or_invalid", dropped=True)
            return None
        parsed = datetime.fromisoformat(signal_detected_at.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        stamp = parsed.astimezone(timezone.utc).isoformat()
        if _epoch and parsed < datetime.fromisoformat(_epoch["start_utc"].replace("Z", "+00:00")):
            _degrade("signal_before_epoch_start", dropped=True, stream="predictors")
            return None
        wallet = hashlib.sha256(source_wallet.encode()).hexdigest() if source_wallet else None
        signature = hashlib.sha256(source_signature.encode()).hexdigest() if source_signature else None
        identity = {"mint": mint, "route_type": route_type, "strategy_family": "whale_route_a" if route_type == "A" else "dex_momentum_b",
            "signal_detected_at": stamp, "source_wallet_hash": wallet, "source_signature_hash": signature}
        identity["signal_id"] = _digest({"upstream_signal_id": signal_id, **identity})
        return Capture(identity)
    except Exception:
        _degrade("capture_input_error", dropped=True)
        return None


@contextmanager
def bind(capture):
    token = _current.set(capture if isinstance(capture, Capture) else None)
    try:
        yield capture
    finally:
        _current.reset(token)


def current_capture():
    return _current.get()


def mark(name, **safe_values):
    try:
        capture = current_capture()
        if capture is None or capture.finished:
            return
        stamp = {"wall_utc": _now(), "monotonic_ns": time.monotonic_ns()}
        if "wall_clock" in safe_values and "monotonic" in safe_values:
            parsed = datetime.fromisoformat(safe_values["wall_clock"].replace("Z", "+00:00"))
            mono = safe_values["monotonic"]
            if parsed.tzinfo and isinstance(mono, (int, float)) and not isinstance(mono, bool) and math.isfinite(mono) and mono >= 0:
                stamp = {"wall_utc": parsed.astimezone(timezone.utc).isoformat(),
                    "monotonic_ns": int(mono * 1_000_000_000), "semantics": "actual upstream capture; not reconstructed"}
        if name == "paper_buy_created_at":
            capture.frozen = True
            capture.receipt.setdefault(name, stamp)
            for key in ("trade_id", "event_seq"):
                if safe_values.get(key) is not None:
                    capture.receipt[key] = _safe(safe_values[key])
            if capture.receipt.get("trade_id") is not None:
                capture.sections["decision"]["outcome"] = "BUY"
        elif name in TIMESTAMPS and not capture.frozen:
            if capture.timestamps.get(name) is None:
                capture.timestamps[name] = stamp
            if name == "quote_buy_request_started_at" and capture.timestamps["quote_request_started_at"] is None:
                capture.timestamps["quote_request_started_at"] = {**stamp, "semantics": "first BUY quote request alias"}
            if name == "quote_buy_received_at":
                if capture.timestamps["quote_received_at"] is None:
                    capture.timestamps["quote_received_at"] = {**stamp, "semantics": "first BUY quote local receipt alias"}
                capture.sections["quote_buy"]["quote_timestamp"] = stamp["wall_utc"]
            if name == "entry_decision_at":
                received = capture.timestamps.get("quote_buy_received_at")
                if received:
                    elapsed = (stamp["monotonic_ns"] - received["monotonic_ns"]) / 1_000_000_000
                    if elapsed >= 0:
                        capture.sections["quote_buy"]["quote_age_at_decision_sec"] = elapsed
                        capture.sections["quote_buy"]["quote_age_ms"] = elapsed * 1000
                capture.frozen = True
        elif name == "signal_detected_at" and not capture.frozen and "wall_clock" in safe_values:
            if stamp["wall_utc"] == capture.identity["signal_detected_at"]:
                capture.timestamps[name] = stamp
    except Exception:
        _degrade("timestamp_capture_error")


def set_section(name, mapping):
    try:
        capture = current_capture()
        if capture is None or capture.finished or name not in SECTION_FIELDS or not isinstance(mapping, dict):
            return
        if capture.frozen and name != "decision":
            return
        if name == "wallet_performance":
            # 안전한 entry 시점 원본이 없다. 현재값/미래값 backfill은 금지한다.
            capture.sections[name] = {"entry_time_snapshot": None, "missing_reason": "NO_VERIFIED_ENTRY_TIME_SNAPSHOT_SOURCE"}
            return
        for key, value in islice(mapping.items(), MAX_ITEMS):
            if key in SECTION_FIELDS[name] and (not _forbidden(key) or name == "decision" and key == "outcome"):
                if name == "decision" and key == "outcome" and capture.receipt.get("trade_id") is not None:
                    continue
                capture.sections[name][key] = predictor_schema.project(value, SECTION_SHAPES[name][key],
                    _safe, budget=capture.safe_budget)
    except Exception:
        _degrade("section_capture_error")


def add_counter(name, amount=1):
    try:
        capture = current_capture()
        if capture and not capture.frozen and not capture.finished and name in COUNTERS:
            if isinstance(amount, (int, float)) and not isinstance(amount, bool) and math.isfinite(amount) and amount >= 0:
                capture.counters[name] = (capture.counters[name] or 0) + amount
    except Exception:
        _degrade("counter_capture_error")


def duration(name, seconds):
    try:
        capture = current_capture()
        if capture and not capture.frozen and not capture.finished and name in DURATIONS:
            if isinstance(seconds, (int, float)) and not isinstance(seconds, bool) and math.isfinite(seconds) and seconds >= 0:
                capture.durations[name] = (capture.durations[name] or 0) + seconds
    except Exception:
        _degrade("duration_capture_error")


def safe_hook(name, *args, **kwargs):
    try:
        if name in ("mark", "set_section", "add_counter", "duration", "begin_signal", "finish", "start_worker", "current_capture", "submit_outcome"):
            return globals()[name](*args, **kwargs)
    except Exception:
        _degrade("hook_error")
    return None


def finish(capture, outcome=None, trade_id=None, event_seq=None):
    """작은 큐로 ownership을 넘긴다. JSON 직렬화와 디스크는 worker만 수행한다."""
    try:
        if not isinstance(capture, Capture) or capture.finished:
            return
        capture.finished = capture.frozen = True
        selected = "BUY" if trade_id is not None or capture.receipt.get("trade_id") is not None else (
            outcome or capture.sections["decision"].get("outcome") or "OTHER")
        capture.sections["decision"]["outcome"] = selected if selected in OUTCOMES else "OTHER"
        capture.receipt.update({"trade_id": _safe(trade_id) if trade_id is not None else capture.receipt.get("trade_id"),
            "event_seq": _safe(event_seq) if event_seq is not None else capture.receipt.get("event_seq"),
            "semantics": "post-decision execution identity; never entry predictor"})
        if not is_enabled():
            return
        # 서로 다른 큐를 사용한다. predictor overflow가 BUY receipt를 버리지 않는다.
        for stream, target, value in (("predictors", _queue, capture), ("receipts", _receipt_queue, capture)):
            if stream == "receipts" and not capture.receipt.get("trade_id"):
                continue
            try:
                target.put_nowait(value)
            except queue.Full:
                _degrade("queue_full", dropped=True, stream=stream)
    except queue.Full:
        _degrade("queue_full", dropped=True)
    except Exception:
        _degrade("finish_error", dropped=True)


def _envelope(stream):
    provenance_fields = ("git_sha", "source_digest", "source_scope", "config_fingerprint", "config_scope", "platform", "os",
        "python_version", "runtime", "process_id", "session_id", "process_start_timestamp",
        "process_start_semantics", "telemetry_schema_version", "missing_reason")
    provenance = {key: predictor_schema.project(_provenance.get(key), predictor_schema.SCALAR, _safe)
        for key in provenance_fields}
    provenance["safe_config"] = config_contract.project_config(_provenance.get("safe_config", {}))
    return {"schema_version": SCHEMAS[stream], f"{stream}_schema_version": SCHEMAS[stream],
        "telemetry_epoch_id": (_epoch or {}).get("telemetry_epoch_id"),
        "build_sha": _provenance.get("git_sha"), "session_id": _provenance.get("session_id"),
        "provenance": provenance}


IDENTITY_FIELDS = ("signal_id", "mint", "route_type", "strategy_family", "signal_detected_at",
    "source_wallet_hash", "source_signature_hash")


def _identity(capture):
    values = {key: _safe(capture.identity.get(key)) if not isinstance(capture.identity.get(key), (dict, list, tuple)) else None
        for key in IDENTITY_FIELDS}
    if not isinstance(values["signal_id"], str) or not isinstance(values["mint"], str) or values["route_type"] not in ("A", "B"):
        raise ValueError("invalid_capture_identity")
    return values


def _timestamp(value):
    if not isinstance(value, dict):
        return None
    return {key: _safe(value.get(key)) if not isinstance(value.get(key), (dict, list, tuple)) else None
        for key in ("wall_utc", "monotonic_ns", "semantics")}


def _row(capture):
    sections = {name: predictor_schema.project(capture.sections.get(name), SECTION_SHAPES[name], _safe)
                for name in SECTION_FIELDS if name != "decision"}
    sections["wallet_performance"] = {"entry_time_snapshot": None,
        "missing_reason": "NO_VERIFIED_ENTRY_TIME_SNAPSHOT_SOURCE"}
    # Serializer도 검증한다. capture 내부에 직접 섞은 미래 history가 출력되지 않는다.
    for key in ("history", "pre_signal_snapshots"):
        values = sections["trajectory"].get(key)
        if isinstance(values, list):
            boundary = datetime.fromisoformat(capture.identity["signal_detected_at"]).timestamp()
            sections["trajectory"][key] = [value for value in values if isinstance(value, dict)
                and isinstance(value.get("snapshot_at_epoch"), (int, float))
                and value["snapshot_at_epoch"] <= boundary]
    decision = predictor_schema.project(capture.sections["decision"], SECTION_SHAPES["decision"], _safe)
    decision["outcome"] = decision.get("outcome") if decision.get("outcome") in OUTCOMES else "OTHER"
    return {**_envelope("predictor"), "identity": {**_identity(capture), "trade_id": None, "event_seq": None},
        "decision": decision, "decision_outcome": decision["outcome"],
        "predictors": {"timestamps": {key: _timestamp(capture.timestamps.get(key)) for key in TIMESTAMPS | {"signal_detected_at"}},
            "timestamp_missing_reason": "null means not reached or unavailable in existing flow",
            "sections": sections,
            "counters": {key: predictor_schema.project(capture.counters.get(key), predictor_schema.SCALAR, _safe) for key in COUNTERS},
            "durations": {key: predictor_schema.project(capture.durations.get(key), predictor_schema.SCALAR, _safe) for key in DURATIONS}},
        "collection_limits": {"mapping_or_list_items": MAX_ITEMS, "string_characters": MAX_STRING, "nested_depth": MAX_DEPTH,
            "capture_nodes": MAX_CAPTURE_NODES, "capture_string_characters": MAX_CAPTURE_CHARACTERS,
            "capture_budget_exhausted": capture.safe_budget[0] <= 0 or capture.safe_budget[1] <= 0,
            "semantics": "bounded prefix only; raw lists may be truncated; count fields preserve upstream counts"}}


def _receipt_row(capture):
    return {**_envelope("receipt"), "identity": {**_identity(capture),
        "trade_id": capture.receipt.get("trade_id"), "event_seq": capture.receipt.get("event_seq")},
        "execution_receipt": {"trade_id": capture.receipt.get("trade_id"), "event_seq": capture.receipt.get("event_seq"),
            "paper_buy_created_at": _timestamp(capture.receipt.get("paper_buy_created_at")),
            "semantics": "post-decision execution identity; never entry predictor"}}


OUTCOME_IDENTITY_FIELDS = ("trade_id", "mint", "route_type", "strategy_family", "signal_detected_at",
    "buy_event_seq", "buy_event_id", "event_seq")
OUTCOME_VALUE_FIELDS = ("completed_at", "exit_reason", "entry_cost_lamports",
    "cumulative_proceeds_lamports", "realized_pnl_lamports", "sell_legs_truncated", "sell_legs_complete")
SELL_FIELDS = ("event_seq", "event_id", "at", "reason", "token_amount_raw", "proceeds_lamports", "realized_pnl_lamports")


def is_enabled():
    return _epoch is not None


def submit_outcome(mapping):
    """완료 원장에서 추출한 scalar만 작은 독립 큐로 넘긴다."""
    try:
        if not is_enabled() or not isinstance(mapping, dict):
            return
        row = {key: _safe(mapping.get(key)) if not isinstance(mapping.get(key), (dict, list, tuple)) else None
               for key in OUTCOME_IDENTITY_FIELDS + OUTCOME_VALUE_FIELDS}
        if not isinstance(row["trade_id"], str) or not row["trade_id"] or not isinstance(row["mint"], str):
            _degrade("outcome_identity_invalid", dropped=True, stream="outcomes")
            return
        stamp = datetime.fromisoformat(row["signal_detected_at"].replace("Z", "+00:00"))
        if (stamp.tzinfo is None or stamp < datetime.fromisoformat(_epoch["start_utc"].replace("Z", "+00:00"))
                or not isinstance(row["buy_event_seq"], int) or row["buy_event_seq"] < _epoch["start_event_seq"]):
            _degrade("outcome_outside_epoch", dropped=True, stream="outcomes")
            return
        legs = mapping.get("sell_legs")
        row["sell_legs"] = [{key: _safe(leg.get(key)) if not isinstance(leg.get(key), (dict, list, tuple)) else None
            for key in SELL_FIELDS} for leg in legs[:MAX_ITEMS] if isinstance(leg, dict)] if isinstance(legs, list) else []
        row["sell_legs_truncated"] = bool(row["sell_legs_truncated"] or isinstance(legs, list) and len(legs) > MAX_ITEMS)
        _outcome_queue.put_nowait(row)
    except queue.Full:
        _degrade("queue_full", dropped=True, stream="outcomes")
    except Exception:
        _degrade("outcome_capture_error", dropped=True, stream="outcomes")


def _outcome_row(mapping):
    return {**_envelope("outcome"), "identity": {key: mapping.get(key) for key in OUTCOME_IDENTITY_FIELDS} | {"signal_id": None},
        "outcome": {key: mapping.get(key) for key in OUTCOME_VALUE_FIELDS + ("sell_legs",)}}


def _directory():
    if not _epoch:
        raise ValueError("telemetry_epoch_required")
    return _root / "data" / "research" / "entry_telemetry" / "epochs" / _epoch["telemetry_epoch_id"]


def _record_path(stream, document):
    identity = document["identity"]
    key = identity["trade_id"] if stream == "outcomes" else identity["signal_id"]
    # signal ids와 ledger UUID만 파일명으로 쓴다. 경로 이동 문자열은 거부한다.
    if not isinstance(key, str) or not key or len(key) > 128 or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in key):
        raise ValueError("unsafe_record_identity")
    from datetime import timedelta
    stamp = datetime.fromisoformat(identity["signal_detected_at"].replace("Z", "+00:00"))
    day = stamp.astimezone(timezone(timedelta(hours=9))).date().isoformat()
    return _directory() / stream / day / (key + ".json")


def _increment(stream, key):
    with _health_lock:
        counters = _health.setdefault("streams", {}).setdefault(stream, {
            "written_count": 0, "dropped_row_count": 0, "write_error_count": 0,
            "duplicate_count": 0, "conflict_count": 0})
        counters[key] += 1
        if key in ("duplicate_count", "conflict_count"):
            _health[key] += 1


def _read_sealed(path, max_bytes=MAX_ROW_BYTES):
    if path.stat().st_size > max_bytes:
        raise ValueError("immutable_content_too_large")
    value = read_json(path, {})
    prior_hash = value.pop("content_hash", None)
    if prior_hash != _digest(value):
        raise ValueError("immutable_content_corrupt")
    return value


def _identity_index_path(stream, document):
    identity = document["identity"]
    key = identity["trade_id"] if stream == "outcomes" else identity["signal_id"]
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return _directory() / stream / "_identity" / digest[:2] / (digest + ".json")


def _persist_stream(stream, document):
    encoded = _canonical(document).encode("utf-8")
    content_hash = hashlib.sha256(encoded).hexdigest()
    sealed = {**document, "content_hash": content_hash}
    # state_store는 끝 newline을 쓰며 Windows text 모드는 CRLF로 저장한다.
    byte_count = len(_canonical(sealed).encode("utf-8")) + (2 if os.name == "nt" else 1)
    if byte_count > MAX_ROW_BYTES:
        _degrade("row_too_large", dropped=True, stream=stream)
        return
    directory = _directory() / stream
    path = _record_path(stream, document)
    index_path = _identity_index_path(stream, document)
    reservation = {"index_schema_version": 1, "telemetry_epoch_id": document.get("telemetry_epoch_id"),
        "stream": stream, "identity": document["identity"],
        "record_relative_path": path.relative_to(directory).as_posix(), "record_content_hash": content_hash}
    # 고정 크기 identity 조회로 날짜 간 중복을 막는다. 이력 스캔/용량 예약은 없다.
    with exclusive_file_lock(directory / "recorder", operation="entry_telemetry_append"):
        if index_path.exists():
            try:
                index = _read_sealed(index_path, 4096)
            except Exception:
                _degrade("immutable_index_corrupt", conflict=True, dropped=True, stream=stream)
                return
            if any(index.get(key) != reservation[key] for key in
                    ("index_schema_version", "telemetry_epoch_id", "stream", "identity", "record_relative_path")):
                _degrade("immutable_identity_conflict", conflict=True, dropped=True, stream=stream)
                return
            # 예약 후 crash 복구는 최초 payload만 허용한다.
            if not path.exists() and index.get("record_content_hash") != content_hash:
                _degrade("immutable_reservation_conflict", conflict=True, dropped=True, stream=stream)
                return
        if path.exists():
            try:
                old = _read_sealed(path)
            except Exception:
                _degrade("immutable_content_corrupt", conflict=True, dropped=True, stream=stream)
                return
            if index_path.exists() and index.get("record_content_hash") != _digest(old):
                _degrade("immutable_index_record_conflict", conflict=True, dropped=True, stream=stream)
                return
            if (old.get("identity") == document["identity"] and
                    all(old.get("execution_receipt", {}).get(key) == document.get("execution_receipt", {}).get(key)
                        for key in ("trade_id", "event_seq")) and
                    old.get("outcome", {}) == document.get("outcome", {}) and
                    old.get("decision", {}).get("outcome") == document.get("decision", {}).get("outcome")):
                if not index_path.exists():
                    reservation["record_content_hash"] = _digest(old)
                    atomic_write_json(index_path, {**reservation, "content_hash": _digest(reservation)})
                _increment(stream, "duplicate_count")
                return
            _degrade("immutable_identity_conflict", conflict=True, dropped=True, stream=stream)
            return
        if not index_path.exists():
            atomic_write_json(index_path, {**reservation, "content_hash": _digest(reservation)})
        atomic_write_json(path, sealed)
        _increment(stream, "written_count")


def _write_stream(stream, value):
    try:
        builder = {"predictors": _row, "receipts": _receipt_row, "outcomes": _outcome_row}[stream]
        document = builder(value)
        stamp = datetime.fromisoformat(document["identity"]["signal_detected_at"].replace("Z", "+00:00"))
        if not _epoch or stamp < datetime.fromisoformat(_epoch["start_utc"].replace("Z", "+00:00")):
            _degrade("record_outside_epoch", dropped=True, stream=stream)
            return
        seq = document["identity"].get("buy_event_seq" if stream == "outcomes" else "event_seq")
        if seq is not None and (not isinstance(seq, int) or isinstance(seq, bool) or seq < _epoch["start_event_seq"]):
            _degrade("record_outside_epoch", dropped=True, stream=stream)
            return
        _persist_stream(stream, document)
    except Exception:
        _degrade("record_write_error", write=True, dropped=True, stream=stream)


def _persist(capture):
    """Offline 검증용 convenience. 실제 worker는 각 큐를 독립 소비한다."""
    _write_stream("predictors", capture)
    if capture.receipt.get("trade_id"):
        _write_stream("receipts", capture)


def _publish_health():
    directory = _directory()
    with _health_lock:
        doc = {**deepcopy(_health), "session_id": _session, "process_id": os.getpid(), "heartbeat_utc": _now(),
            "queue_depth": _queue.qsize(), "queue_capacity": _queue.maxsize,
            "telemetry_epoch_id": _epoch["telemetry_epoch_id"],
            "stream_queues": {name: {"depth": target.qsize(), "capacity": target.maxsize} for name, target in
                (("predictors", _queue), ("receipts", _receipt_queue), ("outcomes", _outcome_queue))},
            "storage_limits": {"rows": None, "bytes": None, "row_bytes": MAX_ROW_BYTES},
            "storage_policy": "immutable individual JSON; signal KST day partitions; sharded identity index; no automatic deletion",
            "counter_semantics": "this recorder process/session only; written_count counts successful row commits; persistent totals require offline enumeration"}
    with exclusive_file_lock(directory / "health", operation="entry_telemetry_health"):
        health_path = directory / "health" / (_role + ".json")
        prior = read_json(health_path, {"version": 0})
        atomic_write_json(health_path, {**doc, "version": prior["version"] + 1,
            "publisher_semantics": "latest recorder process; per-session provenance remains in immutable rows"})


def _build_provenance(config):
    safe = config_contract.project_config(config)
    result = {"git_sha": None, "source_digest": None, "config_fingerprint": _digest(safe), "safe_config": safe,
        "config_scope": "effective non-secret settings contract v2; credentials and endpoint values excluded",
        "platform": sys.platform, "os": platform.system(), "python_version": platform.python_version(),
        "runtime": platform.python_implementation(), "process_id": os.getpid(), "session_id": _session,
        "process_start_timestamp": _process_start, "process_start_semantics": "module initialization UTC",
        "telemetry_schema_version": SCHEMA_VERSION}
    try:
        git = subprocess.run(["git", "rev-parse", "HEAD"], cwd=_root, capture_output=True, text=True,
            timeout=5, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        if git.returncode == 0:
            result["git_sha"] = git.stdout.strip()
        source = {}
        for path in sorted((_root / "src").rglob("*.py")):
            source[str(path.relative_to(_root)).replace("\\", "/")] = hashlib.sha256(path.read_bytes()).hexdigest()
        result["source_digest"] = _digest(source)
    except Exception:
        result["missing_reason"] = "build_identity_unavailable"
    return result


def _load_epoch(config):
    from src.research import entry_telemetry_epoch as epoch
    marker = epoch.require_epoch(_root, provenance=_provenance, config=config, schemas=SCHEMAS)
    epoch.register_session(_root, marker, session_id=_session, process_id=os.getpid(), provenance=_provenance)
    return marker


def _initialize(config):
    global _provenance, _epoch
    try:
        _provenance = _build_provenance(config)
        _epoch = _load_epoch(config)
        _provenance["source_digest"] = _epoch["build"]["source_digest"]
        _provenance["source_scope"] = "all src/**/*.py, scripts/*.py, requirements.txt and ecosystem.config.js"
    except Exception:
        _epoch = None
        with _health_lock:
            _health["status"] = "TELEMETRY_DISABLED"
            _health["last_error"] = "validated_epoch_required"
        return False
    return True


def _run(config, initialized=False):
    if not initialized and not _initialize(config):
        return
    # 재시작 시 전체 이력을 읽지 않는다. 각 identity 예약만 write 시 검증한다.
    last_health = 0.0
    while True:
        handled = False
        for stream, target in (("predictors", _queue), ("receipts", _receipt_queue), ("outcomes", _outcome_queue)):
            value = None
            try:
                value = target.get(timeout=0.25 if stream == "predictors" else 0)
                handled = True
                _write_stream(stream, value)
            except queue.Empty:
                pass
            except Exception:
                _degrade("worker_queue_error", write=True, dropped=value is not None, stream=stream)
            finally:
                if value is not None:
                    target.task_done()
        if handled or time.monotonic() - last_health >= 30:
            try:
                _publish_health()
                last_health = time.monotonic()
            except Exception:
                _degrade("health_write_error", write=True)


def flush(timeout=5.0):
    """Clean stop에서만 큐가 비워질 때까지 bounded 대기한다."""
    deadline = time.monotonic() + max(0.0, min(float(timeout), 30.0))
    while any(target.unfinished_tasks for target in (_queue, _receipt_queue, _outcome_queue)):
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.01)
    return True


def start_worker(root=ROOT, safe_config=None, *, role="monitor"):
    """monitor 시작 때만 daemon을 만든다. 거래 경로에서는 호출하지 않는다."""
    global _worker, _root, _role
    try:
        with _worker_lock:
            if _worker and _worker.is_alive():
                return
            if role not in ("monitor", "risk-manager"):
                return
            _root = Path(root)
            _role = role
            # startup에서 marker/build/config를 먼저 검증해 첫 신호 수집 race를 없앤다.
            # 거래 hot path에는 파일 I/O와 기다림을 추가하지 않는다.
            if not _initialize(safe_config):
                return
            _worker = threading.Thread(target=_run, args=(safe_config, True), daemon=True, name="entry-telemetry")
            _worker.start()
    except Exception:
        _degrade("worker_launch_error", write=True)
