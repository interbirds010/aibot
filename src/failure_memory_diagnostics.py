"""고RSS 직전의 스칼라 증거를 별도 bounded 원장에 기록한다."""

from __future__ import annotations

import math
import contextvars
import json
import logging
import os
import re
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any

PHASES = frozenset({"candidate_fetch", "whale_confirmation", "smart_get_transaction", "shadow_ledger_write"})
STAGES = frozenset({"enter", "materialized", "compact_projected", "raw_released", "serialize", "serialized", "write", "flushed", "replaced", "buffer_released", "exit", "failed", "rpc_decode_begin", "http_body_decoded", "raw_materialized", "sorted", "signatures_materialized", "signatures_projected", "transaction_fetch", "transaction_materialized", "transaction_projected", "transaction_released", "ledger_loaded", "ledger_mutated", "ledger_released", "tmp_write_start", "file_buffer_closed", "rename_start"})
EVENTS = frozenset({"trim_attempt", "trim_success", "trim_failure", "trim_skipped_cooldown"})
THRESHOLDS = (220 * 1024 * 1024, 240 * 1024 * 1024, 250 * 1024 * 1024)
MAX_PROCESSES = 8
MAX_EVENTS = 48
MAX_BASELINES = 8
MAX_ACTIVE_PHASES = 16
MAX_PENDING = 64
MAX_PHASE_SCALARS = 8
MAX_DOCUMENT_BYTES = 1024 * 1024
HWM_INCREMENT_BYTES = 5 * 1024 * 1024
SNAPSHOT_PATH = Path(__file__).resolve().parents[1] / "data" / "failure_memory_snapshots.json"
logger = logging.getLogger("failure-memory-diagnostics")


def _scalars(values: dict[str, Any]) -> dict[str, int | float | bool | None]:
    result: dict[str, int | float | bool | None] = {}
    for key, value in values.items():
        if len(result) >= 32:
            break
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", key):
            continue
        if any(word in key for word in ("secret", "password", "api_key", "cookie", "private", "url")):
            continue
        if not key.endswith(("count", "bytes", "size", "seconds", "eligible", "in_progress", "live", "known", "code")):
            continue
        if value is None or type(value) is bool or (type(value) is int and abs(value) <= 10**15 and (value >= 0 or key.endswith("bytes"))):
            result[key] = value
        elif type(value) is float and math.isfinite(value) and 0 <= value <= 10**15:
            result[key] = value
    return result


def _nonnegative_number(value: Any) -> bool:
    try:
        return type(value) in (int, float) and value >= 0 and math.isfinite(value)
    except (OverflowError, TypeError, ValueError):
        return False


def _snapshot_key(event: Any) -> tuple[int | float, int, int | float, str] | None:
    """동일 시각은 표본 순번으로 정렬하고, 기존 표본은 내용으로 결정한다."""
    if not isinstance(event, dict) or not _nonnegative_number(event.get("timestamp")):
        return None
    sequence = event.get("sample_sequence", 0)
    sequence = sequence if type(sequence) is int and sequence >= 0 else 0
    elapsed = event.get("process_elapsed_seconds", 0)
    elapsed = elapsed if _nonnegative_number(elapsed) else 0
    try:
        tie_break = json.dumps(event, sort_keys=True, ensure_ascii=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, OverflowError):
        return None
    return event["timestamp"], sequence, elapsed, tie_break


class _Recorder:
    def __init__(self, path: Path = SNAPSHOT_PATH) -> None:
        self.path = path
        self.lock = threading.Lock()
        self.active: dict[int, dict[str, Any]] = {}
        self.overflow_active = 0
        self.sequence = 0
        self.sample_sequence = 0
        self.counts: dict[str, Any] = {}
        self.counts_at: float | None = None
        self.pending: deque[dict[str, Any]] = deque(maxlen=MAX_PENDING)
        self.pending_critical: dict[str, Any] | None = None
        self.pending_hwm: dict[str, Any] | None = None
        self.dropped_events = 0
        self.persistence_failures = 0
        self.last_level = 0
        self.last_high = float("-inf")
        self.last_baseline = float("-inf")
        self.last_cooldown = float("-inf")
        self.started_monotonic = time.monotonic()
        self.last_sampler_tick: float | None = None
        self.sampler_gap_seconds: float | None = None
        self.last_hwm: int | None = None

    def counts_update(self, values: dict[str, Any]) -> None:
        values = _scalars(values)
        with self.lock:
            # 한 번에 갱신한 monitor snapshot만 유지한다.
            self.counts = values
            self.counts_at = time.monotonic()

    def phase_enter(self, name: str, values: dict[str, Any]) -> int | None:
        if name not in PHASES:
            return None
        from src import runtime_memory
        rss = runtime_memory.current_rss_bytes()
        with self.lock:
            self.sequence += 1
            identity = self.sequence
            if len(self.active) >= MAX_ACTIVE_PHASES:
                self.overflow_active += 1
                return -identity
            self.active[identity] = {
                "name": name, "started_at": time.time(),
                "started_monotonic": time.monotonic(), "stage": "enter",
                "rss_before_bytes": rss, "rss_stage_bytes": rss,
                "counts": dict(list(_scalars(values).items())[:MAX_PHASE_SCALARS]),
            }
        try:
            self.capture("phase_boundary")
        except Exception:
            pass
        return identity

    def phase_mark(self, identity: int | None, stage: str, values: dict[str, Any]) -> None:
        if identity is None or identity < 0 or stage not in STAGES:
            return
        from src import runtime_memory
        rss = runtime_memory.current_rss_bytes()
        with self.lock:
            phase = self.active.get(identity)
            if phase is None:
                return
            phase["stage"] = stage
            phase["rss_stage_bytes"] = rss
            updated = _scalars(values)
            for key in updated:
                phase["counts"].pop(key, None)
            phase["counts"].update(updated)
            phase["counts"] = dict(list(phase["counts"].items())[-MAX_PHASE_SCALARS:])
        self.capture("phase_boundary")

    def phase_exit(self, identity: int | None, failed: bool) -> None:
        try:
            self.phase_mark(identity, "failed" if failed else "exit", {})
        finally:
            with self.lock:
                if identity is not None and identity < 0:
                    self.overflow_active = max(0, self.overflow_active - 1)
                else:
                    self.active.pop(identity, None)

    def capture(self, kind: str = "sample", values: dict[str, Any] | None = None) -> None:
        from src import runtime_memory
        memory = runtime_memory.process_memory_snapshot()
        now = time.monotonic()
        rss = memory.get("rss_bytes")
        hwm = memory.get("hwm_bytes")
        level = sum(rss is not None and rss >= threshold for threshold in THRESHOLDS)
        trim = runtime_memory.trim_cooldown_snapshot(rss_bytes=rss)
        existing_phase_counts = None
        try:
            from src.phase_memory_telemetry import active_phase_counts_snapshot
            existing_phase_counts = active_phase_counts_snapshot()
        except Exception:
            pass
        with self.lock:
            if kind == "sample":
                self.sampler_gap_seconds = round(max(0, now - self.last_sampler_tick), 4) if self.last_sampler_tick is not None else None
                self.last_sampler_tick = now
            crossing = level > self.last_level
            crossed = list(THRESHOLDS[self.last_level:level]) if crossing else []
            self.last_level = level
            previous_hwm = self.last_hwm
            hwm_crossed = [threshold for threshold in THRESHOLDS if hwm is not None and hwm >= threshold and (previous_hwm is None or previous_hwm < threshold)]
            hwm_delta = hwm - previous_hwm if hwm is not None and previous_hwm is not None else None
            hwm_peak = bool(hwm_crossed or (hwm is not None and hwm >= THRESHOLDS[0] and hwm_delta is not None and hwm_delta >= HWM_INCREMENT_BYTES))
            if hwm is not None and (previous_hwm is None or hwm_peak or hwm < THRESHOLDS[0]):
                self.last_hwm = hwm
            if kind == "sample":
                if crossing:
                    kind = "threshold_crossing"
                elif hwm_peak:
                    kind = "hwm_peak"
                elif level and now - self.last_high >= 10:
                    kind = "high_rss_repeat"
                elif not level and now - self.last_baseline >= 60:
                    kind = "baseline"
                else:
                    return
            elif kind == "phase_boundary" and not level:
                if hwm_peak:
                    kind = "hwm_peak"
                elif now - self.last_high > 10:
                    return
            elif kind == "trim_skipped_cooldown":
                if now - self.last_cooldown < 10:
                    if crossing:
                        kind = "threshold_crossing"
                    elif hwm_peak:
                        kind = "hwm_peak"
                    else:
                        return
                else:
                    self.last_cooldown = now
            if level:
                self.last_high = now
            if kind == "baseline":
                self.last_baseline = now
            phases = []
            active_counts = dict.fromkeys(sorted(PHASES), 0)
            for identity, phase in self.active.items():
                active_counts[phase["name"]] += 1
                phases.append({
                    "phase_id": identity, "name": phase["name"],
                    "started_at": phase["started_at"],
                    "elapsed_seconds": round(max(0, now - phase["started_monotonic"]), 4),
                    "stage": phase["stage"],
                    "rss_before_bytes": phase["rss_before_bytes"],
                    "rss_stage_bytes": phase["rss_stage_bytes"],
                    "counts": dict(phase["counts"]),
                })
            self.sample_sequence += 1
            event = {
                "timestamp": time.time(), "pid": os.getpid(),
                "sample_sequence": self.sample_sequence,
                "process_start_id": runtime_memory._PROCESS_START_ID,
                "process_elapsed_seconds": round(max(0, now - self.started_monotonic), 4),
                "sampler_gap_seconds": self.sampler_gap_seconds,
                "sampler_tick_age_seconds": round(max(0, now - self.last_sampler_tick), 4) if self.last_sampler_tick is not None else None,
                "kind": kind, "rss_bytes": rss,
                "rss_measurement_available": rss is not None,
                "vmhwm_bytes": hwm,
                "hwm_prior_bytes": previous_hwm,
                "hwm_delta_bytes": hwm_delta,
                "hwm_first_observation": previous_hwm is None,
                "hwm_threshold_crossings_bytes": hwm_crossed,
                "hwm_new_peak": hwm_peak,
                "thresholds_exceeded_bytes": list(THRESHOLDS[:level]),
                "threshold_crossings_bytes": crossed,
                "active_phases": phases, "active_counts": active_counts,
                "existing_active_phase_counts": existing_phase_counts,
                "existing_phase_counts_available": existing_phase_counts is not None,
                "active_overflow_count": self.overflow_active,
                "runtime_counts": dict(self.counts),
                "runtime_counts_age_seconds": round(max(0, now - self.counts_at), 4) if self.counts_at is not None else None,
                "trim": trim, "event_counts": _scalars(values or {}),
                "dropped_event_count": self.dropped_events,
                "persistence_failure_count": self.persistence_failures,
            }
            if len(self.pending) == MAX_PENDING:
                self.dropped_events += 1
                event["dropped_event_count"] = self.dropped_events
            self.pending.append(event)
            if level == len(THRESHOLDS):
                self.pending_critical = event
            if hwm_peak:
                self.pending_hwm = event

    def flush(self) -> None:
        from src import state_store
        with self.lock:
            batch = list(self.pending)
            self.pending.clear()
            critical = self.pending_critical
            self.pending_critical = None
            if critical is not None and not any(event is critical for event in batch):
                batch.append(critical)
            hwm_event = self.pending_hwm
            self.pending_hwm = None
            if hwm_event is not None and not any(event is hwm_event for event in batch):
                batch.append(hwm_event)
        if not batch:
            return

        def mutate(document: dict[str, Any]) -> None:
            if document.get("schema_version") != 1 or not isinstance(document.get("processes"), list):
                raise ValueError("unsupported failure memory diagnostics schema")
            processes = document["processes"]
            for event in batch:
                if (_snapshot_key(event) is None
                        or not isinstance(event.get("process_start_id"), str)
                        or not event["process_start_id"]
                        or not isinstance(event.get("kind"), str)
                        or type(event.get("pid")) is not int):
                    continue
                identity = event["process_start_id"]
                process = next((row for row in processes if row.get("process_start_id") == identity), None)
                if process is None:
                    process = {"process_start_id": identity, "pid": event["pid"], "events": [], "baselines": [], "latest_critical_snapshot": None, "latest_hwm_snapshot": None}
                    processes.append(process)
                key = "baselines" if event["kind"] == "baseline" else "events"
                process[key].append(event)
            ranked_processes = []
            for process in processes:
                candidates = []
                for key in ("events", "baselines"):
                    ranked = [(freshness, event) for event in process[key]
                              if (freshness := _snapshot_key(event)) is not None]
                    ranked.sort(key=lambda item: item[0])
                    process[key] = [event for _, event in ranked]
                    candidates.extend(ranked)
                # 보존 슬롯도 비교 후보에 넣어 배열 축소나 재시도가 latest를 되돌리지 않게 한다.
                for field in ("latest_rss_snapshot", "latest_critical_snapshot", "latest_hwm_snapshot"):
                    previous = process.get(field)
                    freshness = _snapshot_key(previous)
                    if freshness is not None:
                        candidates.append((freshness, previous))
                latest_keys = {}
                for field in ("latest_rss_snapshot", "latest_critical_snapshot", "latest_hwm_snapshot"):
                    process[field] = None
                for freshness, event in candidates:
                    rss = event.get("rss_bytes")
                    eligible = {
                        "latest_rss_snapshot": _nonnegative_number(rss),
                        "latest_critical_snapshot": _nonnegative_number(rss) and rss >= THRESHOLDS[-1],
                        "latest_hwm_snapshot": event.get("hwm_new_peak") is True and _nonnegative_number(event.get("vmhwm_bytes")),
                    }
                    for field, allowed in eligible.items():
                        if allowed and (field not in latest_keys or freshness > latest_keys[field]):
                            latest_keys[field] = freshness
                            process[field] = event
                process["events"] = process["events"][-MAX_EVENTS:]
                if len(process["baselines"]) > MAX_BASELINES:
                    process["baselines"] = process["baselines"][:1] + process["baselines"][-(MAX_BASELINES - 1):]
                if candidates:
                    ranked_processes.append((max(freshness for freshness, _ in candidates), process))
            ranked_processes.sort(key=lambda item: (item[0], item[1]["process_start_id"]))
            document["processes"] = [process for _, process in ranked_processes[-MAX_PROCESSES:]]
            # 진단 문서만 직렬화하여 크기를 제한한다. 원장/거래 payload는 읽지 않는다.
            while len(json.dumps(document, ensure_ascii=True, separators=(",", ":"))) > MAX_DOCUMENT_BYTES - 128:
                removable = [row for row in document["processes"] if len(row["events"]) > 1]
                if removable:
                    for row in removable:
                        row["events"] = row["events"][max(1, len(row["events"]) // 2):]
                    continue
                removable = next((row for row in document["processes"] if len(row["baselines"]) > 1), None)
                if removable is not None:
                    del removable["baselines"][1]
                    continue
                if len(document["processes"]) > 1:
                    del document["processes"][0]
                    continue
                raise ValueError("failure memory diagnostics snapshot too large")

        try:
            state_store.update_json(self.path, {"schema_version": 1, "processes": []}, mutate, operation="failure_memory_diagnostics")
        except Exception:
            # 손상 파일을 덮지 않고, 실패 시에도 거래 경로에 예외를 전파하지 않는다.
            with self.lock:
                self.persistence_failures += 1
                if critical is not None and self.pending_critical is None:
                    self.pending_critical = critical
                if hwm_event is not None and self.pending_hwm is None:
                    self.pending_hwm = hwm_event
                for index, event in enumerate(reversed(batch)):
                    if len(self.pending) == MAX_PENDING:
                        self.dropped_events += len(batch) - index
                        break
                    self.pending.appendleft(event)
                first_failure = self.persistence_failures == 1
            if first_failure:
                logger.warning("memory_failure_snapshot_persistence_unavailable failure_count=1")


_recorder = _Recorder()
_current_phase: contextvars.ContextVar[Any] = contextvars.ContextVar("failure_memory_phase", default=None)


class _Phase:
    def __init__(self, name: str, values: dict[str, Any]) -> None:
        self.name = name
        self.values = _scalars(values)
        self.identity: int | None = None
        self.token: Any = None

    def __enter__(self) -> "_Phase":
        try:
            self.identity = _recorder.phase_enter(self.name, self.values)
        except Exception:
            pass
        self.values = {}
        self.token = _current_phase.set(self)
        return self

    def mark(self, stage: str, **scalar_counts: Any) -> None:
        try:
            _recorder.phase_mark(self.identity, stage, scalar_counts)
        except Exception:
            pass

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        try:
            _recorder.phase_exit(self.identity, exc_type is not None)
        except Exception:
            pass
        finally:
            if self.token is not None:
                try:
                    _current_phase.reset(self.token)
                except Exception:
                    pass
        return False


def diagnostic_phase(name: str, **scalar_counts: Any) -> _Phase:
    return _Phase(name, scalar_counts)


def update_runtime_counts(**scalar_counts: Any) -> None:
    try:
        _recorder.counts_update(scalar_counts)
    except Exception:
        pass


def mark_current_phase(stage: str, *, phase_name: str | None = None, **scalar_counts: Any) -> None:
    try:
        current = _current_phase.get()
        if current is not None and (phase_name is None or current.name == phase_name):
            current.mark(stage, **scalar_counts)
    except Exception:
        pass


def record_event(kind: str, **scalar_counts: Any) -> None:
    if kind not in EVENTS or _sampler is None:
        return
    try:
        _recorder.capture(kind, scalar_counts)
    except Exception:
        pass


class _Sampler:
    def __init__(self) -> None:
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self.run, name="failure-memory-sampler", daemon=True)
        self.thread.start()

    def run(self) -> None:
        while not self.stopped.is_set():
            try:
                _recorder.capture()
                _recorder.flush()
            except Exception:
                pass
            self.stopped.wait(1.0)
        try:
            _recorder.flush()
        except Exception:
            pass

    def stop(self) -> None:
        # 이벤트 루프에서는 join이나 파일 쓰기를 하지 않는다.
        self.stopped.set()


_sampler: _Sampler | None = None
_sampler_lock = threading.Lock()


def start_failure_sampler() -> _Sampler | None:
    global _sampler
    with _sampler_lock:
        if _sampler is None or not _sampler.thread.is_alive():
            try:
                _sampler = _Sampler()
            except Exception:
                return None
        return _sampler
