"""Monitor 프로세스의 저비용·내용 비노출 메모리 진단 지표."""

from __future__ import annotations

import ctypes
import logging
import math
import os
import sys
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any


MEMORY_PHASES = frozenset({
    "analyzer",
    "momentum_candidate_fetch",
    "momentum_whale_confirmation",
    "observation_analysis",
    "observation_due_scan",
    "smart_get_transaction",
})
PAYLOAD_SIZE_WINDOW = 128
PAYLOAD_ESTIMATE_MAX_NODES = 20_000
# Production's normal p95 is about 188.7 MiB.  This threshold avoids normal
# churn while preserving 60 MiB of headroom below PM2's 260 MiB ceiling.
ALLOCATOR_TRIM_RSS_THRESHOLD_BYTES = 200 * 1024 * 1024
# PM2 samples RSS every 30 seconds; one attempt per minute is soon enough to be
# visible by the next samples without putting malloc_trim on every ledger call.
ALLOCATOR_TRIM_MIN_INTERVAL_SECONDS = 60.0

logger = logging.getLogger("runtime-memory")
_PROCESS_START_ID = f"{os.getpid()}-{int(time.time())}"

_phase_stats: dict[str, dict[str, int]] = {}
_transaction_payload_sizes: deque[int] = deque(maxlen=PAYLOAD_SIZE_WINDOW)
_transaction_payload_count = 0
_transaction_payload_truncated_count = 0
_MALLOC_TRIM_UNINITIALIZED = object()
_malloc_trim_function: Any = _MALLOC_TRIM_UNINITIALIZED
_allocator_libc_handle: Any = None
_allocator_trim_lock = threading.Lock()
_allocator_trim_last_attempt_monotonic: float | None = None
_allocator_trim_in_progress = False
_allocator_trim_stats: dict[str, int | float | None] = {
    "attempt_count": 0,
    "success_count": 0,
    "failure_count": 0,
    "last_at_epoch_seconds": None,
    "last_rss_before_bytes": None,
    "last_rss_after_bytes": None,
    "last_latency_ms": None,
}


def _proc_values(text: str) -> dict[str, int]:
    values: dict[str, int] = {}
    for line in str(text).splitlines():
        name, separator, raw = line.partition(":")
        if not separator:
            continue
        fields = raw.strip().split()
        if not fields:
            continue
        try:
            value = int(fields[0])
        except ValueError:
            continue
        if len(fields) > 1 and fields[1].lower() == "kb":
            value *= 1024
        values[name] = value
    return values


def process_memory_snapshot(
    *,
    status_text: str | None = None,
    meminfo_text: str | None = None,
) -> dict[str, int | None]:
    """Linux procfs에서 현재값만 읽고, 미지원 환경은 None으로 둔다."""
    try:
        status = (
            Path("/proc/self/status").read_text(encoding="utf-8")
            if status_text is None else status_text
        )
        process = _proc_values(status)
    except OSError:
        process = {}
    try:
        meminfo = (
            Path("/proc/meminfo").read_text(encoding="utf-8")
            if meminfo_text is None else meminfo_text
        )
        system = _proc_values(meminfo)
    except OSError:
        system = {}
    return {
        "rss_bytes": process.get("VmRSS"),
        "hwm_bytes": process.get("VmHWM"),
        "vms_bytes": process.get("VmSize"),
        "thread_count": process.get("Threads"),
        "system_available_memory_bytes": system.get("MemAvailable"),
    }


def current_rss_bytes() -> int | None:
    return process_memory_snapshot()["rss_bytes"]


def trim_cooldown_snapshot(*, rss_bytes: int | None = None) -> dict[str, Any]:
    """Sampler가 allocator lock을 기다리지 않고 스칼라 현황을 읽는다."""
    last = _allocator_trim_last_attempt_monotonic
    age = max(0.0, time.monotonic() - last) if last is not None else None
    remaining = max(0.0, ALLOCATOR_TRIM_MIN_INTERVAL_SECONDS - age) if age is not None else 0.0
    in_progress = _allocator_trim_in_progress
    above_threshold = rss_bytes is not None and rss_bytes >= ALLOCATOR_TRIM_RSS_THRESHOLD_BYTES
    return {
        "last_trim_timestamp": _allocator_trim_stats["last_at_epoch_seconds"],
        "seconds_since_last_trim": round(age, 4) if age is not None else None,
        "cooldown_remaining_seconds": round(remaining, 4),
        "trim_in_progress": in_progress,
        "rss_threshold_bytes": ALLOCATOR_TRIM_RSS_THRESHOLD_BYTES,
        "rss_threshold_exceeded": above_threshold,
        "trim_eligible": bool(sys.platform.startswith("linux") and above_threshold and not remaining and not in_progress),
    }


def _failure_diagnostic_event(kind: str, *, phase: str, reason: str, **scalar_counts: Any) -> None:
    try:
        from src.failure_memory_diagnostics import record_event
        scalar_counts["phase_code"] = {
            "unspecified": 0, "momentum_candidate_fetch": 1,
            "momentum_whale_confirmation": 2, "smart_get_transaction": 3,
            "shadow_ledger_write": 4,
        }.get(phase, 0)
        scalar_counts["reason_code"] = {
            "unspecified": 0, "raw_candidate_payload_released": 1,
            "raw_confirmation_payload_released": 2,
            "restored_transaction_consumed": 3,
        }.get(reason, 0)
        record_event(kind, **scalar_counts)
    except Exception:
        pass


def _load_malloc_trim() -> Any:
    """Resolve glibc malloc_trim lazily, caching unsupported environments."""
    global _allocator_libc_handle, _malloc_trim_function
    if _malloc_trim_function is not _MALLOC_TRIM_UNINITIALIZED:
        return _malloc_trim_function
    try:
        libc = ctypes.CDLL("libc.so.6", use_errno=True)
        glibc_version = libc.gnu_get_libc_version
        glibc_version.argtypes = []
        glibc_version.restype = ctypes.c_char_p
        if not glibc_version():
            raise RuntimeError("glibc version is unavailable")
        malloc_trim = libc.malloc_trim
        malloc_trim.argtypes = [ctypes.c_size_t]
        malloc_trim.restype = ctypes.c_int
    except Exception:
        _malloc_trim_function = None
        return None
    _allocator_libc_handle = libc
    _malloc_trim_function = malloc_trim
    return malloc_trim


def _trim_label(value: str) -> str:
    normalized = "".join(
        character
        for character in str(value)[:80]
        if character.isalnum() or character in {"_", "-", "."}
    )
    return normalized or "unknown"


def maybe_trim_allocator(
    *,
    rss_threshold_bytes: int = ALLOCATOR_TRIM_RSS_THRESHOLD_BYTES,
    minimum_interval_seconds: float = ALLOCATOR_TRIM_MIN_INTERVAL_SECONDS,
    reason: str = "unspecified",
    phase: str = "unspecified",
) -> bool:
    """Rate-limited, fail-open glibc heap release after large objects die.

    Callers must invoke this only after releasing the final large-object
    reference.  Every actual attempt is logged so evidence survives PM2 process
    replacement even though in-process counters reset.
    """
    global _allocator_trim_last_attempt_monotonic, _allocator_trim_in_progress
    owns_attempt = False
    normalized_reason = _trim_label(reason)
    normalized_phase = _trim_label(phase)
    try:
        if not sys.platform.startswith("linux"):
            return False
        rss_before = current_rss_bytes()
        threshold = max(0, int(rss_threshold_bytes))
        if rss_before is None or rss_before < threshold:
            return False
        now = time.monotonic()
        interval = max(0.0, float(minimum_interval_seconds))
        with _allocator_trim_lock:
            if (
                _allocator_trim_last_attempt_monotonic is not None
                and now - _allocator_trim_last_attempt_monotonic < interval
            ):
                _failure_diagnostic_event("trim_skipped_cooldown", phase=normalized_phase, reason=normalized_reason, rss_before_bytes=rss_before)
                return False
            _allocator_trim_last_attempt_monotonic = now
            _allocator_trim_stats["attempt_count"] = int(
                _allocator_trim_stats["attempt_count"] or 0
            ) + 1
            attempt_count = int(_allocator_trim_stats["attempt_count"] or 0)
            _allocator_trim_stats["last_at_epoch_seconds"] = time.time()
            _allocator_trim_stats["last_rss_before_bytes"] = rss_before
            _allocator_trim_in_progress = True
            owns_attempt = True
            _failure_diagnostic_event("trim_attempt", phase=normalized_phase, reason=normalized_reason, rss_before_bytes=rss_before, attempt_count=attempt_count)
            logger.info(
                "memory_trim_attempt process_start_id=%s phase=%s reason=%s "
                "attempt_count=%d rss_before_bytes=%d threshold_bytes=%d",
                _PROCESS_START_ID,
                normalized_phase,
                normalized_reason,
                attempt_count,
                rss_before,
                threshold,
            )
            started = time.perf_counter()
            malloc_trim = _load_malloc_trim()
            if malloc_trim is None:
                latency_ms = round((time.perf_counter() - started) * 1000, 4)
                _allocator_trim_stats["failure_count"] = int(
                    _allocator_trim_stats["failure_count"] or 0
                ) + 1
                _allocator_trim_stats["last_latency_ms"] = latency_ms
                logger.warning(
                    "memory_trim_failure process_start_id=%s phase=%s reason=%s "
                    "attempt_count=%d rss_before_bytes=%d rss_after_bytes=unknown "
                    "delta_bytes=unknown duration_ms=%.4f failure=unsupported",
                    _PROCESS_START_ID,
                    normalized_phase,
                    normalized_reason,
                    attempt_count,
                    rss_before,
                    latency_ms,
                )
                _failure_diagnostic_event("trim_failure", phase=normalized_phase, reason=normalized_reason, rss_before_bytes=rss_before, attempt_count=attempt_count)
                return False
            try:
                succeeded = bool(malloc_trim(0))
            except Exception:
                succeeded = False
            latency_ms = round((time.perf_counter() - started) * 1000, 4)
            _allocator_trim_stats["last_latency_ms"] = latency_ms
            try:
                rss_after = current_rss_bytes()
            except Exception:
                rss_after = None
            _allocator_trim_stats["last_rss_after_bytes"] = rss_after
            counter = "success_count" if succeeded else "failure_count"
            _allocator_trim_stats[counter] = int(
                _allocator_trim_stats[counter] or 0
            ) + 1
            delta = (
                rss_after - rss_before
                if rss_after is not None
                else None
            )
            event = "memory_trim_success" if succeeded else "memory_trim_failure"
            log = logger.info if succeeded else logger.warning
            log(
                "%s process_start_id=%s phase=%s reason=%s attempt_count=%d "
                "rss_before_bytes=%d rss_after_bytes=%s delta_bytes=%s "
                "duration_ms=%.4f",
                event,
                _PROCESS_START_ID,
                normalized_phase,
                normalized_reason,
                attempt_count,
                rss_before,
                rss_after if rss_after is not None else "unknown",
                delta if delta is not None else "unknown",
                latency_ms,
            )
            _failure_diagnostic_event("trim_success" if succeeded else "trim_failure", phase=normalized_phase, reason=normalized_reason, rss_before_bytes=rss_before, rss_after_bytes=rss_after, attempt_count=attempt_count)
            return succeeded
    except Exception:
        return False
    finally:
        if owns_attempt:
            _allocator_trim_in_progress = False


def record_memory_phase(name: str, before_rss_bytes: int | None) -> None:
    """고정된 phase의 종료 RSS와 증가량만 bounded counter로 보존한다."""
    if name not in MEMORY_PHASES:
        raise ValueError("unsupported memory phase")
    after = current_rss_bytes()
    values = _phase_stats.setdefault(name, {
        "count": 0,
        "max_after_rss_bytes": 0,
        "max_rss_increase_bytes": 0,
    })
    values["count"] += 1
    if after is not None:
        values["max_after_rss_bytes"] = max(
            values["max_after_rss_bytes"], after
        )
    if before_rss_bytes is not None and after is not None:
        values["max_rss_increase_bytes"] = max(
            values["max_rss_increase_bytes"], after - before_rss_bytes
        )


def estimate_object_size_bytes(
    value: Any,
    *,
    maximum_nodes: int = PAYLOAD_ESTIMATE_MAX_NODES,
) -> tuple[int, bool]:
    """JSON-like payload를 복사하지 않고 bounded하게 Python 크기를 추정한다."""
    limit = max(1, int(maximum_nodes))
    total = 0
    visited: set[int] = set()
    pending = [value]
    nodes = 0
    while pending and nodes < limit:
        current = pending.pop()
        identity = id(current)
        if identity in visited:
            continue
        visited.add(identity)
        nodes += 1
        total += sys.getsizeof(current)
        if isinstance(current, dict):
            pending.extend(current.keys())
            pending.extend(current.values())
        elif isinstance(current, (list, tuple, set, frozenset, deque)):
            pending.extend(current)
    return total, bool(pending)


def record_transaction_payload(value: Any) -> int:
    global _transaction_payload_count, _transaction_payload_truncated_count
    size, truncated = estimate_object_size_bytes(value)
    _transaction_payload_count += 1
    if truncated:
        _transaction_payload_truncated_count += 1
    _transaction_payload_sizes.append(size)
    return size


def _percentile(values: list[int], percentile: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = (len(ordered) - 1) * percentile
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    return round(
        ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)
    )


def runtime_memory_metrics(*, rss_ceiling_bytes: int) -> dict[str, Any]:
    snapshot = process_memory_snapshot()
    rss = snapshot["rss_bytes"]
    ceiling = max(1, int(rss_ceiling_bytes))
    payloads = list(_transaction_payload_sizes)
    with _allocator_trim_lock:
        allocator_trim_stats = dict(_allocator_trim_stats)
    return {
        "monitor_memory_rss_bytes": rss,
        "monitor_memory_vms_bytes": snapshot["vms_bytes"],
        "monitor_memory_system_available_bytes": snapshot[
            "system_available_memory_bytes"
        ],
        "monitor_memory_thread_count": snapshot["thread_count"],
        "monitor_memory_rss_ceiling_bytes": ceiling,
        "monitor_memory_rss_headroom_bytes": (
            ceiling - rss if rss is not None else None
        ),
        "monitor_memory_rss_ceiling_percent": (
            round(rss * 100 / ceiling, 4) if rss is not None else None
        ),
        "monitor_memory_phase_stats": {
            name: dict(values) for name, values in sorted(_phase_stats.items())
        },
        "monitor_transaction_payload_count": _transaction_payload_count,
        "monitor_transaction_payload_window_count": len(payloads),
        "monitor_transaction_payload_p50_bytes": _percentile(payloads, 0.5),
        "monitor_transaction_payload_p95_bytes": _percentile(payloads, 0.95),
        "monitor_transaction_payload_max_bytes": max(payloads) if payloads else None,
        "monitor_transaction_payload_estimate_truncated_count": (
            _transaction_payload_truncated_count
        ),
        "monitor_allocator_trim_attempt_count": allocator_trim_stats[
            "attempt_count"
        ],
        "monitor_allocator_trim_success_count": allocator_trim_stats[
            "success_count"
        ],
        "monitor_allocator_trim_failure_count": allocator_trim_stats[
            "failure_count"
        ],
        "monitor_allocator_trim_last_at_epoch_seconds": allocator_trim_stats[
            "last_at_epoch_seconds"
        ],
        "monitor_allocator_trim_last_rss_before_bytes": allocator_trim_stats[
            "last_rss_before_bytes"
        ],
        "monitor_allocator_trim_last_rss_after_bytes": allocator_trim_stats[
            "last_rss_after_bytes"
        ],
        "monitor_allocator_trim_last_latency_ms": allocator_trim_stats[
            "last_latency_ms"
        ],
    }
