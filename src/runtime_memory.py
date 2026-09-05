"""Monitor 프로세스의 저비용·내용 비노출 메모리 진단 지표."""

from __future__ import annotations

import math
import sys
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

_phase_stats: dict[str, dict[str, int]] = {}
_transaction_payload_sizes: deque[int] = deque(maxlen=PAYLOAD_SIZE_WINDOW)
_transaction_payload_count = 0
_transaction_payload_truncated_count = 0


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
        "vms_bytes": process.get("VmSize"),
        "thread_count": process.get("Threads"),
        "system_available_memory_bytes": system.get("MemAvailable"),
    }


def current_rss_bytes() -> int | None:
    return process_memory_snapshot()["rss_bytes"]


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
    }
