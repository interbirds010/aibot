"""Hot-path phase memory telemetry with bounded, identifier-free retention."""

from __future__ import annotations

import asyncio
import gc
import math
import os
import sys
import threading
import time
from contextlib import AbstractContextManager
from contextvars import ContextVar, Token as ContextToken
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping

from src import state_store


MEMORY_PHASE_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "monitor_memory_phases.json"
)
SCHEMA_VERSION = 1
DETAIL_EVENT_LIMIT = 200
DETAIL_RSS_DELTA_BYTES = 20 * 1024 * 1024
DETAIL_RSS_BYTES = 200 * 1024 * 1024
DETAIL_FLUSH_MIN_INTERVAL_SECONDS = 10.0
SAMPLER_EVENT_LIMIT = 96
SAMPLER_INTERVAL_SECONDS = 2.0
SAMPLER_RSS_BYTES = 230 * 1024 * 1024
SAMPLER_HWM_DELTA_BYTES = 5 * 1024 * 1024
SAMPLER_ACTIVE_CONTEXT_LIMIT = 8
ACTIVE_CONTEXT_REGISTRY_LIMIT = 64
CONTEXT_STACK_LIMIT = 8
MAX_SCALAR_COUNT = 10**15

_SAMPLER_CONTEXT_METADATA_KEYS = (
    "workload",
    "operation",
    "kind",
    "stage",
    "payload_bytes",
    "response_bytes",
    "file_bytes",
    "serialized_bytes",
    "row_count",
    "signature_count",
    "transaction_count",
    "candidate_count",
    "retained_count",
    "projected_count",
    "task_count",
    "queue_depth",
    "response_count",
    "missing_length_count",
)

# Phase names are deliberately finite so a mint, signature, URL, or other
# identifier cannot accidentally become a persisted dimension.
ALLOWED_PHASES = frozenset({
    "candidate_fetch",
    "whale_confirmation",
    "whale_signature_retrieval",
    "whale_signature_projection",
    "whale_transaction_fetch",
    "whale_transaction_parse",
    "whale_transaction_matching",
    "whale_confirmation_aggregation",
    "whale_result_projection",
    "analyzer",
    "observation_due_scan",
    "coverage_telemetry_flush",
    "hourly_rollup",
    "archive_write",
    "archive_record_preparation",
    "archive_serialization_write",
    "archive_metric_write",
    "archive_retention_projection",
    "wallet_performance_refresh",
    "wallet_reload",
    "ws_refresh_reconnect",
})

_ENUM_METADATA: dict[str, frozenset[str]] = {
    "workload": frozenset({
        "analyzer",
        "momentum",
        "observation",
        "coverage",
        "shadow_trade",
        "wallet_performance",
        "websocket",
        "transaction",
        "global_metrics",
    }),
    "operation": frozenset({
        "analyze",
        "fetch",
        "confirm",
        "scan",
        "read",
        "parse",
        "update",
        "serialize",
        "flush",
        "rebuild",
        "archive",
        "write",
        "connect",
    }),
    "result": frozenset({"success", "failure", "cancelled", "unknown"}),
    "mode": frozenset({"enhanced", "standard", "unknown"}),
    "kind": frozenset({"startup", "steady", "hour_boundary", "manual", "unknown"}),
    "stage": frozenset({
        "serialize",
        "serialized",
        "flushed",
        "replaced",
    }),
}
_INTEGER_METADATA = frozenset({
    "row_count",
    "bucket_count",
    "hour_count",
    "task_count",
    "queue_depth",
    "file_bytes",
    "payload_bytes",
    "retry_count",
    "dimension_count",
    "batch_size",
    "candidate_count",
    "response_count",
    "response_bytes",
    "missing_length_count",
    "request_count",
    "approved_count",
    "shadow_count",
    "signature_count",
    "transaction_count",
    "wallet_count",
    "pending_count",
    "due_count",
    "source_bucket_count",
    "archive_count",
    "duplicate_count",
    "failure_count",
    "success_count",
    "replacement_count",
    "subscription_count",
    "cancelled_task_count",
    "active_series_count",
    "snapshot_count",
    "projected_count",
    "retained_count",
    "serialized_bytes",
})
_BOOLEAN_METADATA = frozenset({
    "cold_start",
    "truncated",
    "rollup_needed",
    "revalidation",
    "early_exit",
    "content_length_known",
})

_active_lock = threading.RLock()
_active_phase_counts: dict[str, int] = {}
_active_contexts: dict[int, dict[str, Any]] = {}
_next_phase_instance_id = 0
_next_context_id = 0
_active_context_overflow_count = 0
_pending_batch: dict[str, Any] | None = None
_flush_lock = threading.Lock()
_detail_flush_lock = threading.Lock()
_detail_flush_requested = False
_detail_flush_running = False
_last_detail_flush_monotonic = 0.0
_sampler_thread: threading.Thread | None = None
_sampler_stop_event: threading.Event | None = None
_sampler_last_hwm_bytes: int | None = None
_sampler_previous_contexts: list[dict[str, Any]] = []
_sampler_previous_active_phases: list[dict[str, int | str]] = []

MemoryReader = Callable[[], Mapping[str, int | None]]
Clock = Callable[[], float]


def _proc_kib_values(text: str) -> dict[str, int]:
    values: dict[str, int] = {}
    for line in str(text).splitlines():
        name, separator, raw = line.partition(":")
        if not separator or name not in {"VmRSS", "VmHWM"}:
            continue
        fields = raw.strip().split()
        if not fields:
            continue
        try:
            value = int(fields[0])
        except (TypeError, ValueError, OverflowError):
            continue
        if value < 0:
            continue
        if len(fields) > 1 and fields[1].lower() == "kb":
            value *= 1024
        values[name] = value
    return values


def process_memory_measurement(
    *, status_text: str | None = None, status_path: Path = Path("/proc/self/status")
) -> dict[str, int | None]:
    """Read current RSS and process high-water RSS; unsupported systems stay open."""
    try:
        text = (
            status_path.read_text(encoding="utf-8")
            if status_text is None
            else str(status_text)
        )
        values = _proc_kib_values(text)
    except (OSError, TypeError, ValueError):
        values = {}
    return {
        "rss_bytes": values.get("VmRSS"),
        "hwm_bytes": values.get("VmHWM"),
    }


def normalize_workload_metadata(
    metadata: Mapping[str, Any] | None,
) -> dict[str, bool | int | str]:
    """Accept only bounded scalar dimensions with finite predefined values."""
    if metadata is None:
        return {}
    if not isinstance(metadata, Mapping):
        raise ValueError("phase metadata must be a mapping")
    normalized: dict[str, bool | int | str] = {}
    for raw_key, value in metadata.items():
        key = str(raw_key).strip().lower()
        if key in _ENUM_METADATA:
            if not isinstance(value, str):
                raise ValueError(f"phase metadata {key} must be a string enum")
            enum_value = value.strip().lower()
            if enum_value not in _ENUM_METADATA[key]:
                raise ValueError(f"unsupported phase metadata value for {key}")
            normalized[key] = enum_value
        elif key in _INTEGER_METADATA:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"phase metadata {key} must be numeric")
            try:
                number = float(value)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(
                    f"phase metadata {key} must be finite and non-negative"
                ) from exc
            if not math.isfinite(number) or number < 0:
                raise ValueError(f"phase metadata {key} must be finite and non-negative")
            normalized[key] = min(MAX_SCALAR_COUNT, int(number))
        elif key in _BOOLEAN_METADATA:
            if not isinstance(value, bool):
                raise ValueError(f"phase metadata {key} must be boolean")
            normalized[key] = value
        else:
            raise ValueError(f"unsupported phase metadata key: {key or 'empty'}")
    return normalized


def _normalize_phase(name: str) -> str:
    phase = str(name).strip().lower()
    if phase not in ALLOWED_PHASES:
        raise ValueError("unsupported memory phase")
    return phase


def _bounded_count(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return min(MAX_SCALAR_COUNT, max(0, number))


def _safe_measure(reader: MemoryReader) -> dict[str, int | None]:
    try:
        values = reader()
    except Exception:
        values = {}
    if not isinstance(values, Mapping):
        values = {}
    return {
        "rss_bytes": _bounded_count(values.get("rss_bytes")),
        "hwm_bytes": _bounded_count(values.get("hwm_bytes")),
    }


def _safe_clock(clock: Clock, fallback: float) -> float:
    try:
        value = float(clock())
    except Exception:
        return fallback
    return value if math.isfinite(value) else fallback


def _optional_runtime_counts(
    *, include_gc_counts: bool, include_object_count: bool
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if include_gc_counts:
        try:
            result["gc_counts"] = [
                min(MAX_SCALAR_COUNT, max(0, int(value)))
                for value in gc.get_count()[:3]
            ]
        except Exception:
            result["gc_counts"] = None
    if include_object_count:
        try:
            # This is a bounded scalar proxy and avoids materializing gc.get_objects().
            result["allocated_block_count"] = _bounded_count(
                sys.getallocatedblocks()
            )
        except Exception:
            result["allocated_block_count"] = None
    return result


def _active_snapshot() -> list[dict[str, int | str]]:
    return [
        {"phase": phase, "count": count}
        for phase, count in sorted(_active_phase_counts.items())
        if count > 0
    ]


def _next_identity(*, context: bool = False) -> int:
    global _next_context_id
    global _next_phase_instance_id
    with _active_lock:
        if context:
            _next_context_id = (_next_context_id % MAX_SCALAR_COUNT) + 1
            return _next_context_id
        _next_phase_instance_id = (_next_phase_instance_id % MAX_SCALAR_COUNT) + 1
        return _next_phase_instance_id


def _task_kind() -> str:
    try:
        return "async_task" if asyncio.current_task() is not None else "sync_thread"
    except RuntimeError:
        return "sync_thread"


def _active_context_snapshot() -> tuple[list[dict[str, Any]], bool, int]:
    contexts = sorted(
        _active_contexts.values(),
        key=lambda item: (int(item["depth"]), int(item["span_id"])),
        reverse=True,
    )
    truncated = (
        len(contexts) > SAMPLER_ACTIVE_CONTEXT_LIMIT
        or sum(_active_phase_counts.values()) > len(contexts)
    )
    return [
        {
            "span_id": int(item["span_id"]),
            "context_id": int(item["context_id"]),
            "task_kind": str(item["task_kind"]),
            "stack": list(item["stack"]),
            "stack_truncated": bool(item.get("stack_truncated", False)),
            "metadata": {
                key: item["metadata"][key]
                for key in _SAMPLER_CONTEXT_METADATA_KEYS
                if key in item["metadata"]
            },
        }
        for item in contexts[:SAMPLER_ACTIVE_CONTEXT_LIMIT]
    ], truncated, _active_context_overflow_count


def _activate(
    phase: str,
    *,
    instance_id: int,
    context_id: int,
    context_stack: tuple[str, ...],
    task_kind: str,
    metadata: Mapping[str, Any],
) -> list[dict[str, int | str]]:
    global _active_context_overflow_count
    with _active_lock:
        _active_phase_counts[phase] = _active_phase_counts.get(phase, 0) + 1
        if len(_active_contexts) < ACTIVE_CONTEXT_REGISTRY_LIMIT:
            _active_contexts[instance_id] = {
                "span_id": instance_id,
                "context_id": context_id,
                "depth": len(context_stack),
                "task_kind": task_kind,
                "stack": context_stack[-CONTEXT_STACK_LIMIT:],
                "stack_truncated": len(context_stack) > CONTEXT_STACK_LIMIT,
                "metadata": dict(metadata),
            }
        else:
            _active_context_overflow_count = _saturated_sum(
                _active_context_overflow_count, 1
            )
        return _active_snapshot()


def _update_active_context(instance_id: int, metadata: Mapping[str, Any]) -> None:
    with _active_lock:
        active = _active_contexts.get(instance_id)
        if active is not None:
            active["metadata"] = dict(metadata)


def _deactivate(phase: str, instance_id: int | None = None) -> list[dict[str, int | str]]:
    with _active_lock:
        if instance_id is not None:
            _active_contexts.pop(instance_id, None)
        count = _active_phase_counts.get(phase, 0)
        if count <= 1:
            _active_phase_counts.pop(phase, None)
        else:
            _active_phase_counts[phase] = count - 1
        return _active_snapshot()


def _empty_document() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "version": 0,
        "detail_event_limit": DETAIL_EVENT_LIMIT,
        "detail_rss_delta_bytes": DETAIL_RSS_DELTA_BYTES,
        "detail_rss_bytes": DETAIL_RSS_BYTES,
        "total_phase_count": 0,
        "detail_retained_count": 0,
        "measurement_failure_count": 0,
        "persistence_failure_count": 0,
        "maxima": {},
        "phases": {},
        "events": [],
        "sampler_event_limit": SAMPLER_EVENT_LIMIT,
        "sampler_interval_seconds": SAMPLER_INTERVAL_SECONDS,
        "sampler_rss_bytes": SAMPLER_RSS_BYTES,
        "sampler_hwm_delta_bytes": SAMPLER_HWM_DELTA_BYTES,
        "sampler_sample_count": 0,
        "sampler_event_retained_count": 0,
        "sampler_event_evicted_count": 0,
        "sampler_measurement_failure_count": 0,
        "sampler_events": [],
        "last_process_id": None,
        "updated_at_epoch": None,
    }


def _empty_batch() -> dict[str, Any]:
    return {
        "total_phase_count": 0,
        "detail_retained_count": 0,
        "detail_evicted_before_flush_count": 0,
        "measurement_failure_count": 0,
        "persistence_failure_count": 0,
        "maxima": {},
        "phases": {},
        "events": [],
        "sampler_sample_count": 0,
        "sampler_event_retained_count": 0,
        "sampler_event_evicted_before_flush_count": 0,
        "sampler_measurement_failure_count": 0,
        "sampler_events": [],
        "last_process_id": None,
        "updated_at_epoch": None,
    }


def _maximum(target: dict[str, Any], key: str, value: Any) -> None:
    if value is None:
        return
    if isinstance(value, float):
        target[key] = max(float(target.get(key, 0.0) or 0.0), value)
    else:
        target[key] = max(int(target.get(key, 0) or 0), int(value))


def _saturated_sum(left: Any, right: Any) -> int:
    return min(
        MAX_SCALAR_COUNT,
        max(0, int(left or 0)) + max(0, int(right or 0)),
    )


def _update_peak(
    target: dict[str, Any], key: str, value: Any, record: dict[str, Any]
) -> None:
    if value is None:
        return
    previous = target.get(key)
    if previous is not None and value <= previous:
        return
    target[key] = round(value, 3) if isinstance(value, float) else int(value)
    detail = record["detail"]
    target[f"{key}_context"] = {
        "phase": record["phase"],
        "started_at_epoch": detail["started_at_epoch"],
        "ended_at_epoch": detail["ended_at_epoch"],
        "metadata": dict(record.get("metadata", {})),
        "active_at_start": list(detail["active_at_start"]),
        "active_at_end": list(detail["active_at_end"]),
        "context_id": detail.get("context_id"),
        "span_id": detail.get("span_id"),
        "task_kind": detail.get("task_kind"),
        "context_stack_at_start": list(
            detail.get("context_stack_at_start", [])
        ),
        "context_stack_at_end": list(
            detail.get("context_stack_at_end", [])
        ),
    }


def _merge_aggregate(target: dict[str, Any], record: dict[str, Any]) -> None:
    target["count"] = _saturated_sum(target.get("count"), 1)
    if record["measurement_failed"]:
        target["measurement_failure_count"] = (
            _saturated_sum(target.get("measurement_failure_count"), 1)
        )
    if record["body_failed"]:
        target["body_failure_count"] = _saturated_sum(
            target.get("body_failure_count"), 1
        )
    if record["detail_retained"]:
        target["detail_retained_count"] = _saturated_sum(
            target.get("detail_retained_count"), 1
        )
    target["elapsed_ms_total"] = round(
        float(target.get("elapsed_ms_total", 0.0) or 0.0)
        + float(record["elapsed_ms"]),
        3,
    )
    _update_peak(target, "max_elapsed_ms", record["elapsed_ms"], record)
    _update_peak(target, "max_rss_bytes", record.get("max_rss_bytes"), record)
    _update_peak(target, "max_hwm_bytes", record.get("max_hwm_bytes"), record)
    _update_peak(
        target,
        "max_rss_delta_bytes",
        max(0, record.get("rss_delta_bytes") or 0),
        record,
    )
    _update_peak(
        target,
        "max_hwm_delta_bytes",
        max(0, record.get("hwm_delta_bytes") or 0),
        record,
    )
    _maximum(target, "max_active_phase_count", record["max_active_phase_count"])
    metadata_maxima = target.setdefault("metadata_maxima", {})
    metadata_totals = target.setdefault("metadata_totals", {})
    metadata = record.get("metadata", {})
    for key, value in metadata.items():
        if key in _INTEGER_METADATA:
            _maximum(metadata_maxima, key, value)
            metadata_totals[key] = _saturated_sum(metadata_totals.get(key), value)
    target["last_metadata"] = dict(metadata)
    target["last_process_id"] = record["process_id"]
    target["last_ended_at_epoch"] = record["ended_at_epoch"]


def _merge_aggregate_values(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key in (
        "count",
        "measurement_failure_count",
        "body_failure_count",
        "detail_retained_count",
    ):
        target[key] = _saturated_sum(target.get(key), source.get(key))
    target["elapsed_ms_total"] = round(
        float(target.get("elapsed_ms_total", 0.0) or 0.0)
        + float(source.get("elapsed_ms_total", 0.0) or 0.0),
        3,
    )
    for key in (
        "max_elapsed_ms",
        "max_rss_bytes",
        "max_hwm_bytes",
        "max_rss_delta_bytes",
        "max_hwm_delta_bytes",
    ):
        source_value = source.get(key)
        target_value = target.get(key)
        if source_value is None:
            continue
        if target_value is None or source_value > target_value:
            target[key] = source_value
            context = source.get(f"{key}_context")
            if isinstance(context, dict):
                target[f"{key}_context"] = dict(context)
    _maximum(target, "max_active_phase_count", source.get("max_active_phase_count"))
    target_metadata_maxima = target.setdefault("metadata_maxima", {})
    for key, value in source.get("metadata_maxima", {}).items():
        _maximum(target_metadata_maxima, key, value)
    target_metadata_totals = target.setdefault("metadata_totals", {})
    for key, value in source.get("metadata_totals", {}).items():
        target_metadata_totals[key] = _saturated_sum(
            target_metadata_totals.get(key), value
        )
    if isinstance(source.get("last_metadata"), dict):
        target["last_metadata"] = dict(source["last_metadata"])
    if source.get("last_process_id") is not None:
        target["last_process_id"] = int(source["last_process_id"])
    source_last = source.get("last_ended_at_epoch")
    if source_last is not None:
        target["last_ended_at_epoch"] = max(
            float(target.get("last_ended_at_epoch", 0.0) or 0.0),
            float(source_last),
        )


def _add_pending_record(record: dict[str, Any]) -> None:
    global _pending_batch
    with _active_lock:
        if _pending_batch is None:
            _pending_batch = _empty_batch()
        batch = _pending_batch
        aggregate = batch["phases"].setdefault(record["phase"], {})
        _merge_aggregate(aggregate, record)
        _merge_aggregate(batch["maxima"], record)
        batch["total_phase_count"] = _saturated_sum(
            batch["total_phase_count"], 1
        )
        if record["measurement_failed"]:
            batch["measurement_failure_count"] = _saturated_sum(
                batch["measurement_failure_count"], 1
            )
        if record["detail_retained"]:
            events = batch["events"]
            if len(events) >= DETAIL_EVENT_LIMIT:
                events.pop(0)
                batch["detail_evicted_before_flush_count"] = _saturated_sum(
                    batch["detail_evicted_before_flush_count"], 1
                )
            events.append(record["detail"])
            batch["detail_retained_count"] = _saturated_sum(
                batch["detail_retained_count"], 1
            )
        batch["updated_at_epoch"] = record["ended_at_epoch"]
        batch["last_process_id"] = record["process_id"]


def record_memory_attribution_sample(
    *,
    memory_reader: MemoryReader = process_memory_measurement,
    epoch_clock: Clock = time.time,
) -> dict[str, Any]:
    """Sample process memory and retain only bounded high-water evidence."""
    global _pending_batch
    global _sampler_last_hwm_bytes
    global _sampler_previous_active_phases
    global _sampler_previous_contexts
    memory = _safe_measure(memory_reader)
    sampled_at = _safe_clock(epoch_clock, time.time())
    rss_bytes = memory["rss_bytes"]
    hwm_bytes = memory["hwm_bytes"]
    with _active_lock:
        previous_hwm = _sampler_last_hwm_bytes
        if hwm_bytes is not None:
            _sampler_last_hwm_bytes = hwm_bytes
        contexts, contexts_truncated, context_overflow_count = (
            _active_context_snapshot()
        )
        active_phases = _active_snapshot()
        previous_contexts = list(_sampler_previous_contexts)
        previous_active_phases = list(_sampler_previous_active_phases)
        if _pending_batch is None:
            _pending_batch = _empty_batch()
        batch = _pending_batch
        batch["sampler_sample_count"] = _saturated_sum(
            batch["sampler_sample_count"], 1
        )
        measurement_failed = rss_bytes is None or hwm_bytes is None
        if measurement_failed:
            batch["sampler_measurement_failure_count"] = _saturated_sum(
                batch["sampler_measurement_failure_count"], 1
            )
        hwm_delta = _delta(hwm_bytes, previous_hwm)
        trigger_reasons: list[str] = []
        if rss_bytes is not None and rss_bytes >= SAMPLER_RSS_BYTES:
            trigger_reasons.append("RSS_HIGH")
        if hwm_delta is not None and hwm_delta >= SAMPLER_HWM_DELTA_BYTES:
            trigger_reasons.append("HWM_INCREASE")
        event = {
            "sampled_at_epoch": round(sampled_at, 3),
            "process_id": os.getpid(),
            "rss_bytes": rss_bytes,
            "hwm_bytes": hwm_bytes,
            "hwm_delta_bytes": hwm_delta,
            "trigger_reasons": trigger_reasons,
            "previous_active_phases": previous_active_phases,
            "previous_active_contexts": previous_contexts,
            "active_phases": active_phases,
            "active_contexts": contexts,
            "active_contexts_truncated": contexts_truncated,
            "active_context_overflow_count": context_overflow_count,
        }
        _sampler_previous_contexts = contexts
        _sampler_previous_active_phases = active_phases
        if trigger_reasons:
            events = batch["sampler_events"]
            if len(events) >= SAMPLER_EVENT_LIMIT:
                events.pop(0)
                batch["sampler_event_evicted_before_flush_count"] = _saturated_sum(
                    batch["sampler_event_evicted_before_flush_count"], 1
                )
            events.append(event)
            batch["sampler_event_retained_count"] = _saturated_sum(
                batch["sampler_event_retained_count"], 1
            )
        batch["updated_at_epoch"] = round(sampled_at, 3)
        batch["last_process_id"] = os.getpid()
    if trigger_reasons:
        _request_background_flush()
    return event


def _memory_attribution_sampler_loop(
    stop_event: threading.Event,
    interval_seconds: float,
) -> None:
    while True:
        try:
            record_memory_attribution_sample()
        except Exception:
            pass
        if stop_event.wait(interval_seconds):
            return


@dataclass
class MemoryAttributionSampler:
    """Handle for the process-wide bounded memory sampler."""

    stop_event: threading.Event
    thread: threading.Thread

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread is not threading.current_thread():
            self.thread.join(timeout=SAMPLER_INTERVAL_SECONDS + 1.0)


def start_memory_attribution_sampler(
    *, interval_seconds: float = SAMPLER_INTERVAL_SECONDS,
) -> MemoryAttributionSampler | None:
    """Start one fail-open daemon sampler for this process."""
    global _sampler_stop_event
    global _sampler_thread
    try:
        interval = float(interval_seconds)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(interval) or interval < 1.0 or interval > 5.0:
        return None
    with _active_lock:
        if _sampler_thread is not None and _sampler_thread.is_alive():
            if _sampler_stop_event is None:
                return None
            return MemoryAttributionSampler(_sampler_stop_event, _sampler_thread)
        stop_event = threading.Event()
        thread = threading.Thread(
            target=_memory_attribution_sampler_loop,
            args=(stop_event, interval),
            name="memory-attribution-sampler",
            daemon=True,
        )
        _sampler_stop_event = stop_event
        _sampler_thread = thread
        try:
            thread.start()
        except Exception:
            _sampler_stop_event = None
            _sampler_thread = None
            return None
    return MemoryAttributionSampler(stop_event, thread)


def _merge_batch_back(batch: dict[str, Any], *, persistence_failed: bool) -> None:
    global _pending_batch
    with _active_lock:
        current = _pending_batch or _empty_batch()
        current["total_phase_count"] = _saturated_sum(
            current["total_phase_count"], batch["total_phase_count"]
        )
        current["detail_retained_count"] = _saturated_sum(
            current["detail_retained_count"], batch["detail_retained_count"]
        )
        current["detail_evicted_before_flush_count"] = _saturated_sum(
            current["detail_evicted_before_flush_count"],
            batch["detail_evicted_before_flush_count"],
        )
        current["measurement_failure_count"] = _saturated_sum(
            current["measurement_failure_count"], batch["measurement_failure_count"]
        )
        current["persistence_failure_count"] = _saturated_sum(
            current["persistence_failure_count"],
            _saturated_sum(batch["persistence_failure_count"], persistence_failed),
        )
        for key in (
            "sampler_sample_count",
            "sampler_event_retained_count",
            "sampler_measurement_failure_count",
        ):
            current[key] = _saturated_sum(current[key], batch[key])
        current["sampler_event_evicted_before_flush_count"] = _saturated_sum(
            current["sampler_event_evicted_before_flush_count"],
            batch["sampler_event_evicted_before_flush_count"],
        )
        _merge_aggregate_values(current["maxima"], batch["maxima"])
        for phase, source in batch["phases"].items():
            _merge_aggregate_values(current["phases"].setdefault(phase, {}), source)
        combined_events = list(batch["events"]) + list(current["events"])
        if len(combined_events) > DETAIL_EVENT_LIMIT:
            current["detail_evicted_before_flush_count"] = _saturated_sum(
                current["detail_evicted_before_flush_count"],
                len(combined_events) - DETAIL_EVENT_LIMIT,
            )
        current["events"] = combined_events[-DETAIL_EVENT_LIMIT:]
        combined_samples = (
            list(batch["sampler_events"]) + list(current["sampler_events"])
        )
        if len(combined_samples) > SAMPLER_EVENT_LIMIT:
            current["sampler_event_evicted_before_flush_count"] = _saturated_sum(
                current["sampler_event_evicted_before_flush_count"],
                len(combined_samples) - SAMPLER_EVENT_LIMIT,
            )
        current["sampler_events"] = combined_samples[-SAMPLER_EVENT_LIMIT:]
        timestamps = [
            value for value in (
                batch.get("updated_at_epoch"), current.get("updated_at_epoch")
            ) if value is not None
        ]
        current["updated_at_epoch"] = max(timestamps) if timestamps else None
        if current.get("last_process_id") is None:
            current["last_process_id"] = batch.get("last_process_id")
        _pending_batch = current


def _persist_batch(batch: dict[str, Any], state_path: Path) -> bool:

    def mutate(document: dict[str, Any]) -> None:
        schema_version = int(document.get("schema_version", 1) or 1)
        if schema_version > SCHEMA_VERSION:
            raise ValueError("unsupported phase memory telemetry schema")
        events = document.get("events")
        phases = document.get("phases")
        maxima = document.get("maxima")
        if not isinstance(events, list):
            events = []
        if not isinstance(phases, dict):
            phases = {}
        if not isinstance(maxima, dict):
            maxima = {}
        sampler_events = document.get("sampler_events")
        if not isinstance(sampler_events, list):
            sampler_events = []

        for phase, source in batch["phases"].items():
            aggregate = phases.setdefault(phase, {})
            if not isinstance(aggregate, dict):
                aggregate = {}
                phases[phase] = aggregate
            _merge_aggregate_values(aggregate, source)
        _merge_aggregate_values(maxima, batch["maxima"])
        document["total_phase_count"] = _saturated_sum(
            document.get("total_phase_count"), batch["total_phase_count"]
        )
        document["measurement_failure_count"] = _saturated_sum(
            document.get("measurement_failure_count"),
            batch["measurement_failure_count"],
        )
        document["detail_retained_count"] = _saturated_sum(
            document.get("detail_retained_count"), batch["detail_retained_count"]
        )
        document["detail_evicted_count"] = _saturated_sum(
            document.get("detail_evicted_count"),
            batch["detail_evicted_before_flush_count"],
        )
        events.extend(batch["events"])
        if len(events) > DETAIL_EVENT_LIMIT:
            document["detail_evicted_count"] = _saturated_sum(
                document["detail_evicted_count"], len(events) - DETAIL_EVENT_LIMIT
            )
        events = events[-DETAIL_EVENT_LIMIT:]
        sampler_events.extend(batch["sampler_events"])
        sampler_evicted = _saturated_sum(
            document.get("sampler_event_evicted_count"),
            batch["sampler_event_evicted_before_flush_count"],
        )
        if len(sampler_events) > SAMPLER_EVENT_LIMIT:
            sampler_evicted = _saturated_sum(
                sampler_evicted, len(sampler_events) - SAMPLER_EVENT_LIMIT
            )
        sampler_events = sampler_events[-SAMPLER_EVENT_LIMIT:]
        document.update({
            "schema_version": SCHEMA_VERSION,
            "detail_event_limit": DETAIL_EVENT_LIMIT,
            "detail_rss_delta_bytes": DETAIL_RSS_DELTA_BYTES,
            "detail_rss_bytes": DETAIL_RSS_BYTES,
            "sampler_event_limit": SAMPLER_EVENT_LIMIT,
            "sampler_interval_seconds": SAMPLER_INTERVAL_SECONDS,
            "sampler_rss_bytes": SAMPLER_RSS_BYTES,
            "sampler_hwm_delta_bytes": SAMPLER_HWM_DELTA_BYTES,
            "sampler_sample_count": _saturated_sum(
                document.get("sampler_sample_count"),
                batch["sampler_sample_count"],
            ),
            "sampler_event_retained_count": _saturated_sum(
                document.get("sampler_event_retained_count"),
                batch["sampler_event_retained_count"],
            ),
            "sampler_event_evicted_count": sampler_evicted,
            "sampler_measurement_failure_count": _saturated_sum(
                document.get("sampler_measurement_failure_count"),
                batch["sampler_measurement_failure_count"],
            ),
            "sampler_events": sampler_events,
            "persistence_failure_count": _saturated_sum(
                document.get("persistence_failure_count"),
                batch["persistence_failure_count"],
            ),
            "maxima": maxima,
            "phases": phases,
            "events": events,
            "last_process_id": batch["last_process_id"],
            "updated_at_epoch": batch["updated_at_epoch"],
        })

    try:
        state_store.update_json(state_path, _empty_document(), mutate)
    except Exception:
        return False
    return True


def flush_phase_memory_telemetry(*, state_path: Path | None = None) -> bool:
    """Atomically persist pending aggregates; failures remain dirty and fail open."""
    global _pending_batch
    target = Path(state_path) if state_path is not None else MEMORY_PHASE_PATH
    with _flush_lock:
        with _active_lock:
            if _pending_batch is None or not (
                _pending_batch["total_phase_count"]
                or _pending_batch["sampler_sample_count"]
            ):
                return True
            batch = _pending_batch
            _pending_batch = None
        persisted = _persist_batch(batch, target)
        if not persisted:
            _merge_batch_back(batch, persistence_failed=True)
        return persisted


def _detail_flush_worker() -> None:
    """Coalesce detailed events into one bounded background writer."""
    global _detail_flush_requested
    global _detail_flush_running
    global _last_detail_flush_monotonic
    while True:
        with _detail_flush_lock:
            delay = max(
                0.0,
                _last_detail_flush_monotonic
                + DETAIL_FLUSH_MIN_INTERVAL_SECONDS
                - time.monotonic(),
            )
        if delay:
            time.sleep(delay)
        with _detail_flush_lock:
            _detail_flush_requested = False
        try:
            flush_phase_memory_telemetry()
        except Exception:
            pass
        with _detail_flush_lock:
            _last_detail_flush_monotonic = time.monotonic()
            if _detail_flush_requested:
                continue
            _detail_flush_running = False
            return


def _request_background_flush() -> bool:
    """Signal at most one daemon writer; thread failures remain fail-open."""
    global _detail_flush_requested
    global _detail_flush_running
    with _detail_flush_lock:
        _detail_flush_requested = True
        if _detail_flush_running:
            return True
        _detail_flush_running = True
    try:
        threading.Thread(
            target=_detail_flush_worker,
            name="phase-memory-flush",
            daemon=True,
        ).start()
    except Exception:
        with _detail_flush_lock:
            _detail_flush_running = False
        return False
    return True


@dataclass
class PhaseToken:
    """Private phase state with identifier-free sequential correlation values."""

    phase: str
    metadata: dict[str, bool | int | str]
    memory_reader: MemoryReader
    epoch_clock: Clock
    monotonic_clock: Clock
    include_gc_counts: bool
    include_object_count: bool
    started_at_epoch: float
    started_monotonic: float
    start_memory: dict[str, int | None]
    start_runtime_counts: dict[str, Any]
    active_at_start: list[dict[str, int | str]]
    process_id: int
    instance_id: int
    context_id: int
    context_stack: tuple[str, ...]
    task_kind: str
    parent: "PhaseToken | None" = field(default=None, repr=False)
    metadata_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    active: bool = True
    finished: bool = False
    result: dict[str, Any] | None = field(default=None, repr=False)

    def set_metadata(self, metadata: Mapping[str, Any]) -> bool:
        """Replace supplied dimensions while the phase is active."""
        normalized = normalize_workload_metadata(metadata)
        with self.metadata_lock:
            if self.finished:
                return False
            self.metadata.update(normalized)
            _update_active_context(self.instance_id, self.metadata)
        return True

    def add_metadata(self, **scalars: Any) -> bool:
        """Accumulate numeric dimensions and set enum/boolean dimensions."""
        normalized = normalize_workload_metadata(scalars)
        with self.metadata_lock:
            if self.finished:
                return False
            for key, value in normalized.items():
                if key in _INTEGER_METADATA:
                    self.metadata[key] = min(
                        MAX_SCALAR_COUNT,
                        int(self.metadata.get(key, 0) or 0) + int(value),
                    )
                else:
                    self.metadata[key] = value
            _update_active_context(self.instance_id, self.metadata)
        return True


_current_phase = ContextVar("phase_memory_current", default=None)


def start_phase(
    name: str,
    *,
    metadata: Mapping[str, Any] | None = None,
    memory_reader: MemoryReader = process_memory_measurement,
    epoch_clock: Clock = time.time,
    monotonic_clock: Clock = time.monotonic,
    include_gc_counts: bool = False,
    include_object_count: bool = False,
) -> PhaseToken:
    """Start a phase after validating all dimensions at the API boundary."""
    phase = _normalize_phase(name)
    normalized_metadata = normalize_workload_metadata(metadata)
    started_at_epoch = _safe_clock(epoch_clock, time.time())
    started_monotonic = _safe_clock(monotonic_clock, time.monotonic())
    start_memory = _safe_measure(memory_reader)
    start_runtime_counts = _optional_runtime_counts(
        include_gc_counts=include_gc_counts,
        include_object_count=include_object_count,
    )
    parent = _current_phase.get()
    if parent is not None and parent.finished:
        parent = None
    context_id = parent.context_id if parent is not None else _next_identity(context=True)
    context_stack = (
        parent.context_stack + (phase,) if parent is not None else (phase,)
    )
    instance_id = _next_identity()
    task_kind = _task_kind()
    active_at_start = _activate(
        phase,
        instance_id=instance_id,
        context_id=context_id,
        context_stack=context_stack,
        task_kind=task_kind,
        metadata=normalized_metadata,
    )
    return PhaseToken(
        phase=phase,
        metadata=normalized_metadata,
        memory_reader=memory_reader,
        epoch_clock=epoch_clock,
        monotonic_clock=monotonic_clock,
        include_gc_counts=bool(include_gc_counts),
        include_object_count=bool(include_object_count),
        started_at_epoch=started_at_epoch,
        started_monotonic=started_monotonic,
        start_memory=start_memory,
        start_runtime_counts=start_runtime_counts,
        active_at_start=active_at_start,
        process_id=os.getpid(),
        instance_id=instance_id,
        context_id=context_id,
        context_stack=context_stack,
        task_kind=task_kind,
        parent=parent,
    )


def _delta(after: int | None, before: int | None) -> int | None:
    if after is None or before is None:
        return None
    return after - before


def finish_phase(token: PhaseToken, *, body_failed: bool = False) -> dict[str, Any]:
    """Finish and enqueue a phase sample without synchronous persistence."""
    if token.finished:
        return token.result or {"persisted": False, "detail_retained": False}
    token.finished = True
    ended_at_epoch = _safe_clock(token.epoch_clock, token.started_at_epoch)
    ended_monotonic = _safe_clock(token.monotonic_clock, token.started_monotonic)
    end_memory = _safe_measure(token.memory_reader)
    end_runtime_counts = _optional_runtime_counts(
        include_gc_counts=token.include_gc_counts,
        include_object_count=token.include_object_count,
    )
    active_at_end = _deactivate(token.phase, token.instance_id)
    token.active = False

    rss_delta = _delta(end_memory["rss_bytes"], token.start_memory["rss_bytes"])
    hwm_delta = _delta(end_memory["hwm_bytes"], token.start_memory["hwm_bytes"])
    rss_values = [
        value for value in (
            token.start_memory["rss_bytes"], end_memory["rss_bytes"]
        ) if value is not None
    ]
    hwm_values = [
        value for value in (
            token.start_memory["hwm_bytes"], end_memory["hwm_bytes"]
        ) if value is not None
    ]
    max_rss = max(rss_values) if rss_values else None
    max_hwm = max(hwm_values) if hwm_values else None
    trigger_reasons: list[str] = []
    if rss_delta is not None and rss_delta >= DETAIL_RSS_DELTA_BYTES:
        trigger_reasons.append("RSS_DELTA")
    if max_rss is not None and max_rss >= DETAIL_RSS_BYTES:
        trigger_reasons.append("RSS_HIGH")
    if hwm_delta is not None and hwm_delta > 0:
        trigger_reasons.append("HWM_INCREASE")
    measurement_failed = any(
        value is None
        for value in (
            token.start_memory["rss_bytes"],
            token.start_memory["hwm_bytes"],
            end_memory["rss_bytes"],
            end_memory["hwm_bytes"],
        )
    )
    elapsed_ms = round(
        max(0.0, ended_monotonic - token.started_monotonic) * 1000,
        3,
    )
    max_active = max(
        sum(int(item["count"]) for item in token.active_at_start),
        sum(int(item["count"]) for item in active_at_end),
    )
    with token.metadata_lock:
        detail_metadata = dict(token.metadata)
    detail: dict[str, Any] = {
        "phase": token.phase,
        "process_id": token.process_id,
        "started_at_epoch": round(token.started_at_epoch, 3),
        "ended_at_epoch": round(ended_at_epoch, 3),
        "elapsed_ms": elapsed_ms,
        "rss_start_bytes": token.start_memory["rss_bytes"],
        "rss_end_bytes": end_memory["rss_bytes"],
        "rss_delta_bytes": rss_delta,
        "hwm_start_bytes": token.start_memory["hwm_bytes"],
        "hwm_end_bytes": end_memory["hwm_bytes"],
        "hwm_delta_bytes": hwm_delta,
        "active_at_start": token.active_at_start,
        "active_at_end": active_at_end,
        "context_id": token.context_id,
        "span_id": token.instance_id,
        "task_kind": token.task_kind,
        "context_stack_at_start": list(token.context_stack[-CONTEXT_STACK_LIMIT:]),
        "context_stack_at_end": list(
            token.context_stack[:-1][-CONTEXT_STACK_LIMIT:]
        ),
        "context_stack_truncated": len(token.context_stack) > CONTEXT_STACK_LIMIT,
        "metadata": detail_metadata,
        "body_failed": bool(body_failed),
        "trigger_reasons": trigger_reasons,
    }
    if token.include_gc_counts:
        detail["gc_start"] = token.start_runtime_counts.get("gc_counts")
        detail["gc_end"] = end_runtime_counts.get("gc_counts")
    if token.include_object_count:
        detail["object_count_start"] = token.start_runtime_counts.get(
            "allocated_block_count"
        )
        detail["object_count_end"] = end_runtime_counts.get(
            "allocated_block_count"
        )
    record = {
        "phase": token.phase,
        "process_id": token.process_id,
        "ended_at_epoch": detail["ended_at_epoch"],
        "elapsed_ms": elapsed_ms,
        "rss_delta_bytes": rss_delta,
        "hwm_delta_bytes": hwm_delta,
        "max_rss_bytes": max_rss,
        "max_hwm_bytes": max_hwm,
        "max_active_phase_count": max_active,
        "measurement_failed": measurement_failed,
        "body_failed": bool(body_failed),
        "detail_retained": bool(trigger_reasons),
        "metadata": detail_metadata,
        "detail": detail,
    }
    _add_pending_record(record)
    flush_scheduled = (
        _request_background_flush() if record["detail_retained"] else False
    )
    token.result = {
        "persisted": False,
        "persistence_status": "QUEUED" if flush_scheduled else "DEFERRED",
        "detail_retained": record["detail_retained"],
        "measurement_failed": measurement_failed,
        "trigger_reasons": list(trigger_reasons),
        "rss_delta_bytes": rss_delta,
        "hwm_delta_bytes": hwm_delta,
    }
    return token.result


class PhaseMemoryScope(AbstractContextManager["PhaseMemoryScope"]):
    """Synchronous context usable around both synchronous and awaited work."""

    def __init__(self, name: str, **options: Any) -> None:
        self._name = name
        self._options = options
        self.token: PhaseToken | None = None
        self.result: dict[str, Any] | None = None
        self._context_token: ContextToken[PhaseToken | None] | None = None
        self._borrowed_token = False

    def __enter__(self) -> "PhaseMemoryScope":
        try:
            current = _current_phase.get()
            if current is not None and current.phase == str(self._name).strip().lower():
                self.token = current
                self._borrowed_token = True
                return self
            self.token = start_phase(self._name, **self._options)
            self._context_token = _current_phase.set(self.token)
        except Exception:
            self.token = None
            self._context_token = None
        return self

    def set_metadata(self, metadata: Mapping[str, Any]) -> bool:
        if self.token is None:
            return False
        try:
            return self.token.set_metadata(metadata)
        except Exception:
            return False

    def add_metadata(self, **scalars: Any) -> bool:
        if self.token is None:
            return False
        try:
            return self.token.add_metadata(**scalars)
        except Exception:
            return False

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        del exc, traceback
        try:
            if self.token is not None and not self._borrowed_token:
                try:
                    self.result = finish_phase(
                        self.token, body_failed=exc_type is not None
                    )
                except Exception:
                    if self.token.active:
                        try:
                            _deactivate(
                                self.token.phase, self.token.instance_id
                            )
                        except Exception:
                            pass
                        self.token.active = False
                    self.result = {
                        "persisted": False,
                        "persistence_status": "FAILED",
                        "detail_retained": False,
                        "measurement_failed": True,
                    }
        finally:
            if self._context_token is not None:
                try:
                    _current_phase.reset(self._context_token)
                except Exception:
                    pass
        return False


def phase_memory(name: str, **options: Any) -> PhaseMemoryScope:
    """Return an identifier-free bounded phase telemetry context manager."""
    return PhaseMemoryScope(name, **options)


def add_current_phase_metadata(**scalars: Any) -> bool:
    """Add safe scalar counters to the phase inherited by async child tasks."""
    token = _current_phase.get()
    if token is None:
        return False
    try:
        return token.add_metadata(**scalars)
    except Exception:
        return False


def current_phase_context_contains(name: str) -> bool:
    """Return whether the current task-local phase stack contains a fixed phase."""
    try:
        phase = _normalize_phase(name)
        token = _current_phase.get()
        return bool(
            token is not None
            and not token.finished
            and phase in token.context_stack
        )
    except Exception:
        return False


def add_ancestor_phase_metadata(name: str, **scalars: Any) -> bool:
    """Add safe scalars to the nearest matching task-local parent."""
    try:
        phase = _normalize_phase(name)
        current = _current_phase.get()
        token = current.parent if current is not None else None
        while token is not None:
            if not token.finished and token.phase == phase:
                return token.add_metadata(**scalars)
            token = token.parent
    except Exception:
        return False
    return False
