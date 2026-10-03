"""진입 당시 근거만 담는 bounded 연구 sidecar. 실패는 거래와 격리한다."""
from __future__ import annotations

from contextlib import contextmanager
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

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_VERSION = 1
MAX_ROW_BYTES = 64 * 1024
MAX_ROWS = 4096
MAX_STORAGE_BYTES = 128 * 1024 * 1024
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
    "wallet_performance": frozenset({"wallet_hash", "snapshot_at", "known_trades", "wins", "losses",
        "realized_pnl", "roi", "recent_activity_count", "status", "missing_reason"}),
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
CONFIG_FIELDS = frozenset({"TRADING_MODE", "OBSERVATION_MODE", "APPROVED_SIGNAL_PAPER_MODE",
    "MAX_OPEN_POSITIONS", "PAPER_INITIAL_SOL", "JUPITER_MAX_CONCURRENCY", "JUPITER_MIN_REQUEST_INTERVAL_SEC"})
CONFIG_FIELDS = CONFIG_FIELDS | frozenset({"paper_buy_basis_points", "single_strength_lamports",
    "momentum_min_volume_m5_usd", "momentum_min_net_buys_m5", "momentum_min_buy_sell_ratio",
    "momentum_min_liquidity_usd", "momentum_min_pair_age_seconds", "route_b_min_safety_score", "unknown_whale_min_count"})
OUTCOMES = frozenset({"BUY", "REJECT_ANALYZER", "REJECT_RISK", "QUOTE_FAILED", "RPC_SKIPPED", "OTHER"})
_current: ContextVar = ContextVar("entry_telemetry", default=None)
_queue: queue.Queue = queue.Queue(maxsize=16)
_worker = None
_worker_lock = threading.Lock()
_health_lock = threading.Lock()
_health = {"status": "TELEMETRY_READY", "dropped_row_count": 0, "write_error_count": 0,
    "duplicate_count": 0, "conflict_count": 0, "last_error": None}
_session = str(uuid4())
_process_start = datetime.now(timezone.utc).isoformat()
_root = ROOT
_provenance = {}


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value):
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _now():
    return datetime.now(timezone.utc).isoformat()


def _forbidden(key):
    normalized = "".join(char for char in key.lower() if char.isalnum())
    return any(word in normalized for word in ("secret", "apikey", "privatekey", "password", "cookie",
        "rpcurl", "authorization", "finalpnl", "winner", "loser", "exitreason", "future", "mfe", "mae",
        "horizonreturn", "finalresearch", "postentry"))


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


def _degrade(reason, *, write=False, conflict=False, dropped=False):
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
            capture.receipt[name] = stamp
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
        for key, value in islice(mapping.items(), MAX_ITEMS):
            if key in SECTION_FIELDS[name] and not _forbidden(key):
                if name == "decision" and key == "outcome" and capture.receipt.get("trade_id") is not None:
                    continue
                capture.sections[name][key] = _safe(value, budget=capture.safe_budget)
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
        if name in ("mark", "set_section", "add_counter", "duration", "begin_signal", "finish", "start_worker", "current_capture"):
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
        _queue.put_nowait(capture)
    except queue.Full:
        _degrade("queue_full", dropped=True)
    except Exception:
        _degrade("finish_error", dropped=True)


def _row(capture):
    return {"schema_version": SCHEMA_VERSION, "identity": capture.identity,
        "decision": capture.sections["decision"],
        "predictors": {"timestamps": capture.timestamps, "timestamp_missing_reason": "null means not reached or unavailable in existing flow",
            "sections": {key: value for key, value in capture.sections.items() if key != "decision"},
            "counters": capture.counters, "durations": capture.durations},
        "execution_receipt": capture.receipt, "provenance": _provenance,
        "collection_limits": {"mapping_or_list_items": MAX_ITEMS, "string_characters": MAX_STRING, "nested_depth": MAX_DEPTH,
            "capture_nodes": MAX_CAPTURE_NODES, "capture_string_characters": MAX_CAPTURE_CHARACTERS,
            "capture_budget_exhausted": capture.safe_budget[0] <= 0 or capture.safe_budget[1] <= 0,
            "semantics": "bounded prefix only; raw lists may be truncated; count fields preserve upstream counts"}}


def _persist(capture):
    document = _row(capture)
    encoded = _canonical(document).encode("utf-8")
    content_hash = hashlib.sha256(encoded).hexdigest()
    sealed = {**document, "content_hash": content_hash}
    # state_store는 끝 newline을 쓰며 Windows text 모드는 CRLF로 저장한다.
    byte_count = len(_canonical(sealed).encode("utf-8")) + (2 if os.name == "nt" else 1)
    if byte_count > MAX_ROW_BYTES:
        _degrade("row_too_large", dropped=True)
        return
    directory = _root / "data" / "research" / "entry_telemetry"
    path = directory / "rows" / (capture.identity["signal_id"] + ".json")
    with exclusive_file_lock(directory / "recorder", operation="entry_telemetry_append"):
        if path.exists():
            old = read_json(path, {})
            prior_hash = old.pop("content_hash", None)
            if prior_hash != _digest(old):
                _degrade("immutable_content_corrupt", conflict=True, dropped=True)
                return
            if (old.get("identity") == document["identity"] and
                    old.get("execution_receipt", {}).get("trade_id") == document["execution_receipt"].get("trade_id") and
                    old.get("execution_receipt", {}).get("event_seq") == document["execution_receipt"].get("event_seq") and
                    old.get("decision", {}).get("outcome") == document["decision"]["outcome"]):
                with _health_lock:
                    _health["duplicate_count"] += 1
                return
            _degrade("immutable_identity_conflict", conflict=True, dropped=True)
            return
        budget_path = directory / "storage.json"
        budget = read_json(budget_path, {"version": 0, "row_count": 0, "bytes": 0})
        if budget["row_count"] >= MAX_ROWS or budget["bytes"] + byte_count > MAX_STORAGE_BYTES:
            _degrade("storage_budget_reached", dropped=True)
            return
        # 예약을 먼저 저장한다. 행 쓰기 실패 시 재시작까지 보수적으로 용량을 차감한다.
        atomic_write_json(budget_path, {"version": budget["version"] + 1, "row_count": budget["row_count"] + 1,
            "bytes": budget["bytes"] + byte_count})
        atomic_write_json(path, sealed)


def _publish_health():
    directory = _root / "data" / "research" / "entry_telemetry"
    with _health_lock:
        doc = {**_health, "session_id": _session, "process_id": os.getpid(), "heartbeat_utc": _now(),
            "queue_depth": _queue.qsize(), "queue_capacity": _queue.maxsize,
            "storage_limits": {"rows": MAX_ROWS, "bytes": MAX_STORAGE_BYTES, "row_bytes": MAX_ROW_BYTES},
            "counter_semantics": "this recorder process/session; storage.json counts persistent rows"}
    with exclusive_file_lock(directory / "health", operation="entry_telemetry_health"):
        health_path = directory / "health.json"
        prior = read_json(health_path, {"version": 0})
        atomic_write_json(health_path, {**doc, "version": prior["version"] + 1,
            "publisher_semantics": "latest recorder process; per-session provenance remains in immutable rows"})


def _build_provenance(config):
    safe = {key: _safe(value) for key, value in (config or {}).items() if key in CONFIG_FIELDS}
    result = {"git_sha": None, "source_digest": None, "config_fingerprint": _digest(safe), "safe_config": safe,
        "config_scope": "explicit non-secret allowlist only; not full environment",
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


def _run(config):
    global _provenance
    try:
        _provenance = _build_provenance(config)
        # crash 직후 budget 저장 누락도 실제 immutable 파일 크기로 복구한다.
        directory = _root / "data" / "research" / "entry_telemetry"
        with exclusive_file_lock(directory / "recorder", operation="entry_telemetry_budget"):
            count = size = 0
            for path in (directory / "rows").glob("*.json"):
                count += 1
                size += path.stat().st_size
                if count > MAX_ROWS or size > MAX_STORAGE_BYTES:
                    break
            old = read_json(directory / "storage.json", {"version": 0})
            atomic_write_json(directory / "storage.json", {"version": old["version"] + 1,
                "row_count": count, "bytes": size})
            if count >= MAX_ROWS or size >= MAX_STORAGE_BYTES:
                _degrade("storage_budget_reached")
    except Exception:
        _degrade("worker_start_error", write=True)
    while True:
        capture = None
        try:
            capture = _queue.get(timeout=30)
            _persist(capture)
        except queue.Empty:
            pass
        except Exception:
            _degrade("record_write_error", write=True, dropped=capture is not None)
        finally:
            if capture is not None:
                _queue.task_done()
        try:
            _publish_health()
        except Exception:
            _degrade("health_write_error", write=True)


def start_worker(root=ROOT, safe_config=None):
    """monitor 시작 때만 daemon을 만든다. 거래 경로에서는 호출하지 않는다."""
    global _worker, _root
    try:
        with _worker_lock:
            if _worker and _worker.is_alive():
                return
            _root = Path(root)
            _worker = threading.Thread(target=_run, args=(safe_config,), daemon=True, name="entry-telemetry")
            _worker.start()
    except Exception:
        _degrade("worker_launch_error", write=True)
