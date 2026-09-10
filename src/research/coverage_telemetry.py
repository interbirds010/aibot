"""Research funnel 누락을 민감정보 없이 bounded aggregate로 계측한다."""

from __future__ import annotations

import hashlib
import math
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src import state_store
from src.phase_memory_telemetry import (
    add_current_phase_metadata,
    phase_memory,
)


TELEMETRY_PATH = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "research_coverage_telemetry.json"
)
HOURLY_TELEMETRY_PATH = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "research_coverage_hourly.json"
)
TELEMETRY_SCHEMA_VERSION = 2
BUCKET_SECONDS = 900
MAX_BUCKETS = 24
MAX_RPC_DIMENSIONS_PER_BUCKET = 64
HOUR_SECONDS = 3_600
EXPECTED_BUCKETS_PER_HOUR = HOUR_SECONDS // BUCKET_SECONDS
MAX_HOURLY_ROLLUPS = 72
MAX_RPC_METHOD_DIMENSIONS_PER_BUCKET = 64
UNIQUE_BITMAP_BITS = 512
UNIQUE_BITMAP_HEX_LENGTH = UNIQUE_BITMAP_BITS // 4
ROLLUP_SOURCE_COMMIT_SHA = "1654e9e530af47617e9655159f2d1c32c908b1f9"
LATENCY_BUCKET_KEYS = (
    "le_250_ms",
    "le_1000_ms",
    "le_5000_ms",
    "gt_5000_ms",
)

FUNNEL_STAGES = frozenset({
    "poll_candidate_observed",
    "candidate_considered",
    "rpc_confirmation_started",
    "rpc_confirmation_succeeded",
    "rpc_confirmation_failed",
    "observation_created",
    "analyzer_started",
    "analyzer_completed",
    "analyzer_failed",
    "quote_preflight_started",
    "quote_preflight_failed",
    "prospective_eligible",
    "horizon_60m_due",
    "horizon_60m_successful",
    "horizon_60m_missed",
    "horizon_60m_unavailable",
})
FAMILIES = frozenset({"SMART_MONEY", "MOMENTUM", "UNKNOWN"})
RPC_RESULTS = frozenset({
    "success",
    "rate_limit",
    "timeout",
    "connection",
    "exhausted",
    "other",
})
RPC_METHODS = frozenset({
    "confirmation_bundle",
    "getAccountInfo",
    "getTokenSupply",
    "getBalance",
    "getTokenAccountsByOwner",
    "getTransaction",
    "getSignaturesForAddress",
    "getHealth",
    "unknown",
})
RPC_PROVIDERS = frozenset({
    "alchemy",
    "chainstack",
    "ankr",
    "helius",
    "solana_public",
    "router",
    "unknown",
})

_pending_lock = threading.Lock()
_pending_buckets: dict[int, dict[str, Any]] = {}
_last_raw_heartbeat_bucket_start: int | None = None
_last_hourly_refresh_bucket_start: int | None = None


def _empty_document() -> dict[str, Any]:
    return {
        "schema_version": TELEMETRY_SCHEMA_VERSION,
        "version": 0,
        "bucket_seconds": BUCKET_SECONDS,
        "retention_buckets": MAX_BUCKETS,
        "unique_bitmap_bits": UNIQUE_BITMAP_BITS,
        "rpc_dimension_limit_per_bucket": MAX_RPC_DIMENSIONS_PER_BUCKET,
        "rpc_method_dimension_limit_per_bucket": (
            MAX_RPC_METHOD_DIMENSIONS_PER_BUCKET
        ),
        "buckets": [],
        "updated_at": None,
    }


def _empty_hourly_document() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "version": 0,
        "source_bucket_seconds": BUCKET_SECONDS,
        "hourly_bucket_seconds": HOUR_SECONDS,
        "retention_hours": MAX_HOURLY_ROLLUPS,
        "unique_bitmap_bits": UNIQUE_BITMAP_BITS,
        "rpc_dimension_limit_per_hour": MAX_RPC_DIMENSIONS_PER_BUCKET,
        "rpc_method_dimension_limit_per_hour": (
            MAX_RPC_METHOD_DIMENSIONS_PER_BUCKET
        ),
        "rollup_source_commit_sha": ROLLUP_SOURCE_COMMIT_SHA,
        "hours": [],
        "updated_at": None,
    }


def _empty_bucket(bucket_start_epoch: int) -> dict[str, Any]:
    return {
        "bucket_start_epoch": int(bucket_start_epoch),
        "families": {},
        "rpc_confirmation": {},
        "rpc_dimension_overflow_event_count": 0,
        "rpc_methods": {},
        "rpc_method_dimension_overflow_event_count": 0,
        "heartbeat_seen": False,
    }


def _empty_hour(hour_start_epoch: int) -> dict[str, Any]:
    expected = [
        int(hour_start_epoch) + index * BUCKET_SECONDS
        for index in range(EXPECTED_BUCKETS_PER_HOUR)
    ]
    return {
        "hour_start_epoch": int(hour_start_epoch),
        "hour_start_utc": (
            datetime(1970, 1, 1, tzinfo=timezone.utc)
            + timedelta(seconds=int(hour_start_epoch))
        ).isoformat(),
        "source_bucket_starts": [],
        "source_bucket_count": 0,
        "expected_bucket_count": EXPECTED_BUCKETS_PER_HOUR,
        "missing_source_bucket_starts": expected,
        "status": "MISSING",
        "complete": False,
        "overflow": False,
        "saturation": False,
        "families": {},
        "rpc_confirmation": {},
        "rpc_dimension_overflow_event_count": 0,
        "rpc_methods": {},
        "rpc_method_dimension_overflow_event_count": 0,
    }


def _family(value: str) -> str:
    normalized = str(value).upper()
    if normalized == "A":
        normalized = "SMART_MONEY"
    elif normalized == "B":
        normalized = "MOMENTUM"
    return normalized if normalized in FAMILIES else "UNKNOWN"


def _safe_epoch(value: float | None) -> float:
    if value is None:
        return time.time()
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return time.time()
    return parsed if math.isfinite(parsed) and parsed >= 0 else time.time()


def _bucket_start(timestamp: float | None) -> int:
    epoch = _safe_epoch(timestamp)
    return int(epoch // BUCKET_SECONDS) * BUCKET_SECONDS


def _mint_bit(mint: str) -> int:
    digest = hashlib.sha256(str(mint).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % UNIQUE_BITMAP_BITS


def _bitmap_value(value: Any) -> int:
    try:
        parsed = int(str(value or "0"), 16)
    except (TypeError, ValueError, OverflowError):
        return 0
    return parsed & ((1 << UNIQUE_BITMAP_BITS) - 1)


def _bitmap_hex(value: int) -> str:
    return format(
        int(value) & ((1 << UNIQUE_BITMAP_BITS) - 1),
        f"0{UNIQUE_BITMAP_HEX_LENGTH}x",
    )


def _record_unique(metric: dict[str, Any], bit: int) -> None:
    bitmap = _bitmap_value(metric.get("unique_bitmap_hex"))
    metric["unique_bitmap_hex"] = _bitmap_hex(bitmap | (1 << int(bit)))


def _unique_estimate(bitmap: int) -> tuple[int | None, int, bool]:
    occupied = int(bitmap).bit_count()
    if occupied >= UNIQUE_BITMAP_BITS:
        return None, occupied, True
    estimate = -UNIQUE_BITMAP_BITS * math.log1p(
        -occupied / UNIQUE_BITMAP_BITS
    )
    return int(round(estimate)), occupied, False


def _stage_metric() -> dict[str, Any]:
    return {
        "event_count": 0,
        "unique_bitmap_hex": _bitmap_hex(0),
    }


def _pending_bucket(start: int) -> dict[str, Any]:
    bucket = _pending_buckets.setdefault(start, _empty_bucket(start))
    while len(_pending_buckets) > MAX_BUCKETS:
        _pending_buckets.pop(min(_pending_buckets), None)
    return bucket


def _add_stage(
    bucket: dict[str, Any], family: str, stage: str, mint_bit: int,
) -> None:
    families = bucket.setdefault("families", {})
    stages = families.setdefault(family, {})
    metric = stages.setdefault(stage, _stage_metric())
    metric["event_count"] = int(metric.get("event_count", 0) or 0) + 1
    _record_unique(metric, mint_bit)


def record_funnel_stage(
    stage: str,
    *,
    mint: str,
    family: str,
    timestamp: float | None = None,
) -> None:
    """고빈도 경로에서 파일 I/O 없이 stage counter만 갱신한다."""
    normalized_stage = str(stage)
    if normalized_stage not in FUNNEL_STAGES:
        raise ValueError("unsupported research funnel stage")
    bit = _mint_bit(mint)
    start = _bucket_start(timestamp)
    with _pending_lock:
        bucket = _pending_bucket(start)
        _add_stage(bucket, _family(family), normalized_stage, bit)


def record_confirmation_result(
    *,
    mint: str,
    family: str,
    method: str,
    provider: str,
    result: str,
    timestamp: float | None = None,
) -> None:
    """Pre-observation confirmation 결과를 bounded dimension으로 집계한다."""
    normalized_result = str(result).lower()
    normalized_method = (
        str(method) if str(method) in RPC_METHODS else "unknown"
    )
    normalized_provider = (
        str(provider).lower()
        if str(provider).lower() in RPC_PROVIDERS
        else "unknown"
    )
    normalized_family = _family(family)
    if normalized_result not in RPC_RESULTS:
        normalized_result = "other"
    start = _bucket_start(timestamp)
    bit = _mint_bit(mint)
    key = "|".join((
        normalized_family,
        normalized_method,
        normalized_provider,
        normalized_result,
    ))
    with _pending_lock:
        bucket = _pending_bucket(start)
        dimensions = bucket.setdefault("rpc_confirmation", {})
        metric = dimensions.get(key)
        if metric is None:
            if len(dimensions) >= MAX_RPC_DIMENSIONS_PER_BUCKET:
                bucket["rpc_dimension_overflow_event_count"] = (
                    int(bucket.get("rpc_dimension_overflow_event_count", 0) or 0)
                    + 1
                )
                return
            metric = _stage_metric()
            dimensions[key] = metric
        metric["event_count"] = int(metric.get("event_count", 0) or 0) + 1
        _record_unique(metric, bit)


def _rpc_method_metric() -> dict[str, Any]:
    return {
        "request_count": 0,
        "success_count": 0,
        "failure_count": 0,
        "rate_limit_count": 0,
        "exhaustion_count": 0,
        "retry_count": 0,
        "failover_count": 0,
        "latency_count": 0,
        "latency_sum_ms": 0.0,
        "latency_max_ms": 0.0,
        "latency_buckets": {key: 0 for key in LATENCY_BUCKET_KEYS},
    }


def record_rpc_method_metric(
    *,
    provider: str,
    method: str,
    request_count: int = 0,
    success_count: int = 0,
    failure_count: int = 0,
    rate_limit_count: int = 0,
    exhaustion_count: int = 0,
    retry_count: int = 0,
    failover_count: int = 0,
    latency_ms: float | None = None,
    timestamp: float | None = None,
) -> None:
    """RPC 동작을 바꾸지 않고 provider/method별 메모리 counter만 갱신한다."""
    normalized_provider = (
        str(provider).lower()
        if str(provider).lower() in RPC_PROVIDERS
        else "unknown"
    )
    normalized_method = str(method) if str(method) in RPC_METHODS else "unknown"
    increments = {
        "request_count": request_count,
        "success_count": success_count,
        "failure_count": failure_count,
        "rate_limit_count": rate_limit_count,
        "exhaustion_count": exhaustion_count,
        "retry_count": retry_count,
        "failover_count": failover_count,
    }
    normalized: dict[str, int] = {}
    for key, value in increments.items():
        try:
            normalized[key] = max(0, int(value))
        except (TypeError, ValueError, OverflowError):
            normalized[key] = 0
    latency: float | None
    try:
        parsed_latency = float(latency_ms) if latency_ms is not None else None
    except (TypeError, ValueError, OverflowError):
        parsed_latency = None
    latency = (
        max(0.0, parsed_latency)
        if parsed_latency is not None and math.isfinite(parsed_latency)
        else None
    )
    if not any(normalized.values()) and latency is None:
        return
    start = _bucket_start(timestamp)
    key = "|".join((normalized_provider, normalized_method))
    with _pending_lock:
        bucket = _pending_bucket(start)
        dimensions = bucket.setdefault("rpc_methods", {})
        metric = dimensions.get(key)
        if metric is None:
            if len(dimensions) >= MAX_RPC_METHOD_DIMENSIONS_PER_BUCKET:
                bucket["rpc_method_dimension_overflow_event_count"] = (
                    int(bucket.get(
                        "rpc_method_dimension_overflow_event_count", 0
                    ) or 0)
                    + max(1, sum(normalized.values()))
                )
                return
            metric = _rpc_method_metric()
            dimensions[key] = metric
        for name, value in normalized.items():
            metric[name] = int(metric.get(name, 0) or 0) + value
        if latency is not None:
            metric["latency_count"] = int(
                metric.get("latency_count", 0) or 0
            ) + 1
            metric["latency_sum_ms"] = round(
                float(metric.get("latency_sum_ms", 0.0) or 0.0) + latency,
                3,
            )
            metric["latency_max_ms"] = round(max(
                float(metric.get("latency_max_ms", 0.0) or 0.0), latency
            ), 3)
            if latency <= 250:
                latency_bucket = "le_250_ms"
            elif latency <= 1_000:
                latency_bucket = "le_1000_ms"
            elif latency <= 5_000:
                latency_bucket = "le_5000_ms"
            else:
                latency_bucket = "gt_5000_ms"
            buckets = metric.setdefault("latency_buckets", {})
            buckets[latency_bucket] = int(
                buckets.get(latency_bucket, 0) or 0
            ) + 1


def _merge_metric(target: dict[str, Any], source: dict[str, Any]) -> None:
    target["event_count"] = (
        int(target.get("event_count", 0) or 0)
        + int(source.get("event_count", 0) or 0)
    )
    target["unique_bitmap_hex"] = _bitmap_hex(
        _bitmap_value(target.get("unique_bitmap_hex"))
        | _bitmap_value(source.get("unique_bitmap_hex"))
    )


def _merge_rpc_method_metric(
    target: dict[str, Any], source: dict[str, Any]
) -> None:
    for name in (
        "request_count",
        "success_count",
        "failure_count",
        "rate_limit_count",
        "exhaustion_count",
        "retry_count",
        "failover_count",
        "latency_count",
    ):
        target[name] = (
            int(target.get(name, 0) or 0)
            + int(source.get(name, 0) or 0)
        )
    target["latency_sum_ms"] = round(
        float(target.get("latency_sum_ms", 0.0) or 0.0)
        + float(source.get("latency_sum_ms", 0.0) or 0.0),
        3,
    )
    target["latency_max_ms"] = round(max(
        float(target.get("latency_max_ms", 0.0) or 0.0),
        float(source.get("latency_max_ms", 0.0) or 0.0),
    ), 3)
    target_buckets = target.setdefault("latency_buckets", {})
    source_buckets = source.get("latency_buckets", {})
    source_buckets = source_buckets if isinstance(source_buckets, dict) else {}
    for name in LATENCY_BUCKET_KEYS:
        target_buckets[name] = (
            int(target_buckets.get(name, 0) or 0)
            + int(source_buckets.get(name, 0) or 0)
        )


def _merge_bucket(target: dict[str, Any], source: dict[str, Any]) -> None:
    target["heartbeat_seen"] = bool(
        target.get("heartbeat_seen") or source.get("heartbeat_seen")
    )
    for family, stages in source.get("families", {}).items():
        target_stages = target.setdefault("families", {}).setdefault(family, {})
        for stage, metric in stages.items():
            _merge_metric(target_stages.setdefault(stage, _stage_metric()), metric)
    target_dimensions = target.setdefault("rpc_confirmation", {})
    for key in sorted(source.get("rpc_confirmation", {})):
        metric = source.get("rpc_confirmation", {}).get(key)
        if not isinstance(metric, dict):
            continue
        if key not in target_dimensions:
            if len(target_dimensions) >= MAX_RPC_DIMENSIONS_PER_BUCKET:
                target["rpc_dimension_overflow_event_count"] = (
                    int(target.get("rpc_dimension_overflow_event_count", 0) or 0)
                    + int(metric.get("event_count", 0) or 0)
                )
                continue
            target_dimensions[key] = _stage_metric()
        _merge_metric(target_dimensions[key], metric)
    target["rpc_dimension_overflow_event_count"] = (
        int(target.get("rpc_dimension_overflow_event_count", 0) or 0)
        + int(source.get("rpc_dimension_overflow_event_count", 0) or 0)
    )
    target_methods = target.setdefault("rpc_methods", {})
    for key in sorted(source.get("rpc_methods", {})):
        metric = source.get("rpc_methods", {}).get(key)
        if not isinstance(metric, dict):
            continue
        if key not in target_methods:
            if len(target_methods) >= MAX_RPC_METHOD_DIMENSIONS_PER_BUCKET:
                target["rpc_method_dimension_overflow_event_count"] = (
                    int(target.get(
                        "rpc_method_dimension_overflow_event_count", 0
                    ) or 0)
                    + max(1, int(metric.get("request_count", 0) or 0))
                )
                continue
            target_methods[key] = _rpc_method_metric()
        _merge_rpc_method_metric(target_methods[key], metric)
    target["rpc_method_dimension_overflow_event_count"] = (
        int(target.get("rpc_method_dimension_overflow_event_count", 0) or 0)
        + int(source.get(
            "rpc_method_dimension_overflow_event_count", 0
        ) or 0)
    )


def _merge_pending_back(pending: dict[int, dict[str, Any]]) -> None:
    with _pending_lock:
        for start, source in pending.items():
            target = _pending_bucket(start)
            _merge_bucket(target, source)


def _hour_start(epoch: float | int) -> int:
    return int(float(epoch) // HOUR_SECONDS) * HOUR_SECONDS


def _epoch_iso(epoch: float | int) -> str:
    return (
        datetime(1970, 1, 1, tzinfo=timezone.utc)
        + timedelta(seconds=float(epoch))
    ).isoformat()


def _hour_is_saturated(hour: dict[str, Any]) -> bool:
    for stages in hour.get("families", {}).values():
        if not isinstance(stages, dict):
            continue
        for metric in stages.values():
            if (
                isinstance(metric, dict)
                and _unique_estimate(_bitmap_value(
                    metric.get("unique_bitmap_hex")
                ))[2]
            ):
                return True
    for metric in hour.get("rpc_confirmation", {}).values():
        if (
            isinstance(metric, dict)
            and _unique_estimate(_bitmap_value(
                metric.get("unique_bitmap_hex")
            ))[2]
        ):
            return True
    return False


def _build_hourly_rollup(
    hour_start_epoch: int,
    raw_index: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    hour = _empty_hour(hour_start_epoch)
    expected = [
        hour_start_epoch + index * BUCKET_SECONDS
        for index in range(EXPECTED_BUCKETS_PER_HOUR)
    ]
    available = [start for start in expected if start in raw_index]
    for start in available:
        _merge_bucket(hour, raw_index[start])
    missing = [start for start in expected if start not in raw_index]
    hour.update({
        "source_bucket_starts": available,
        "source_bucket_count": len(available),
        "expected_bucket_count": EXPECTED_BUCKETS_PER_HOUR,
        "missing_source_bucket_starts": missing,
        "status": (
            "COMPLETE" if len(available) == EXPECTED_BUCKETS_PER_HOUR
            else "PARTIAL" if available
            else "MISSING"
        ),
        "complete": len(available) == EXPECTED_BUCKETS_PER_HOUR,
    })
    hour["overflow"] = bool(
        int(hour.get("rpc_dimension_overflow_event_count", 0) or 0)
        or int(hour.get(
            "rpc_method_dimension_overflow_event_count", 0
        ) or 0)
    )
    hour["saturation"] = _hour_is_saturated(hour)
    return hour


def _refresh_hourly_rollups_body(
    raw_document: dict[str, Any], *, now_epoch: float
) -> dict[str, Any]:
    current_hour = _hour_start(now_epoch)
    raw_index = {
        int(bucket.get("bucket_start_epoch", 0) or 0): bucket
        for bucket in raw_document.get("buckets", [])
        if isinstance(bucket, dict)
        and int(bucket.get("bucket_start_epoch", 0) or 0) < current_hour
    }
    first_hour = current_hour - MAX_HOURLY_ROLLUPS * HOUR_SECONDS
    expected_hours = list(range(
        first_hour,
        current_hour,
        HOUR_SECONDS,
    ))

    def mutate(document: dict[str, Any]) -> None:
        schema_version = int(document.get("schema_version", 1) or 1)
        if schema_version > 1:
            raise ValueError("unsupported research hourly telemetry schema")
        existing = {
            int(hour.get("hour_start_epoch", 0) or 0): hour
            for hour in document.get("hours", [])
            if isinstance(hour, dict)
        }
        hours: list[dict[str, Any]] = []
        for start in expected_hours:
            rebuilt = _build_hourly_rollup(start, raw_index)
            prior = existing.get(start)
            prior_count = (
                int(prior.get("source_bucket_count", 0) or 0)
                if isinstance(prior, dict) else -1
            )
            if rebuilt["source_bucket_count"] >= prior_count:
                selected = rebuilt
            elif isinstance(prior, dict):
                selected = prior
            else:
                selected = rebuilt
            hours.append(selected)
        document.update({
            "schema_version": 1,
            "source_bucket_seconds": BUCKET_SECONDS,
            "hourly_bucket_seconds": HOUR_SECONDS,
            "retention_hours": MAX_HOURLY_ROLLUPS,
            "unique_bitmap_bits": UNIQUE_BITMAP_BITS,
            "rpc_dimension_limit_per_hour": MAX_RPC_DIMENSIONS_PER_BUCKET,
            "rpc_method_dimension_limit_per_hour": (
                MAX_RPC_METHOD_DIMENSIONS_PER_BUCKET
            ),
            "rollup_source_commit_sha": ROLLUP_SOURCE_COMMIT_SHA,
            "hours": hours[-MAX_HOURLY_ROLLUPS:],
            "updated_at": datetime.fromtimestamp(
                now_epoch, timezone.utc
            ).isoformat(),
        })

    _, saved = state_store.update_json(
        HOURLY_TELEMETRY_PATH,
        _empty_hourly_document(),
        mutate,
    )
    return saved


def _refresh_hourly_rollups(
    raw_document: dict[str, Any], *, now_epoch: float
) -> None:
    """72-hour rebuild/write의 RSS·HWM과 compact workload만 기록한다."""
    with phase_memory(
        "hourly_rollup",
        metadata={
            "workload": "coverage",
            "operation": "rebuild",
            "source_bucket_count": len(raw_document.get("buckets", [])),
            "rollup_needed": True,
        },
        include_gc_counts=True,
        include_object_count=True,
    ) as scope:
        saved = _refresh_hourly_rollups_body(
            raw_document, now_epoch=now_epoch
        )
        hours = saved.get("hours", [])
        try:
            state_bytes = HOURLY_TELEMETRY_PATH.stat().st_size
        except OSError:
            state_bytes = 0
        scope.add_metadata(
            hour_count=len(hours) if isinstance(hours, list) else 0,
            file_bytes=state_bytes,
        )


def _flush_coverage_telemetry_body(*, now_epoch: float | None = None) -> bool:
    """메모리 집계를 단일 원자적 write로 병합하고 최근 6시간만 유지한다."""
    global _last_hourly_refresh_bucket_start
    global _last_raw_heartbeat_bucket_start
    now = _safe_epoch(now_epoch)
    current_bucket = _bucket_start(now)
    current_hour = _hour_start(now)
    with _pending_lock:
        pending = dict(_pending_buckets)
        _pending_buckets.clear()
    add_current_phase_metadata(pending_count=len(pending))
    had_pending = bool(pending)
    heartbeat_needed = _last_raw_heartbeat_bucket_start != current_bucket
    rollup_needed = (
        _last_hourly_refresh_bucket_start != current_hour
        or any(_hour_start(start) < current_hour for start in pending)
    )
    add_current_phase_metadata(rollup_needed=rollup_needed)
    if not had_pending and not heartbeat_needed and not rollup_needed:
        return False

    def mutate(document: dict[str, Any]) -> None:
        schema_version = int(document.get("schema_version", 1) or 1)
        if schema_version > TELEMETRY_SCHEMA_VERSION:
            raise ValueError("unsupported research coverage telemetry schema")
        document.setdefault("buckets", [])
        indexed: dict[int, dict[str, Any]] = {}
        for raw in document.get("buckets", []):
            if not isinstance(raw, dict):
                continue
            start = int(raw.get("bucket_start_epoch", 0) or 0)
            if start >= 0:
                indexed[start] = raw
        for start, source in pending.items():
            target = indexed.setdefault(start, _empty_bucket(start))
            _merge_bucket(target, source)
        indexed.setdefault(current_bucket, _empty_bucket(current_bucket))[
            "heartbeat_seen"
        ] = True
        cutoff = _bucket_start(now) - (MAX_BUCKETS - 1) * BUCKET_SECONDS
        document.update({
            "schema_version": TELEMETRY_SCHEMA_VERSION,
            "bucket_seconds": BUCKET_SECONDS,
            "retention_buckets": MAX_BUCKETS,
            "unique_bitmap_bits": UNIQUE_BITMAP_BITS,
            "rpc_dimension_limit_per_bucket": MAX_RPC_DIMENSIONS_PER_BUCKET,
            "rpc_method_dimension_limit_per_bucket": (
                MAX_RPC_METHOD_DIMENSIONS_PER_BUCKET
            ),
            "buckets": [
                indexed[start]
                for start in sorted(indexed)
                if start >= cutoff
            ][-MAX_BUCKETS:],
            "updated_at": datetime.fromtimestamp(
                now, timezone.utc
            ).isoformat(),
        })

    try:
        _, raw_document = state_store.update_json(
            TELEMETRY_PATH, _empty_document(), mutate
        )
    except Exception:
        _merge_pending_back(pending)
        raise
    _last_raw_heartbeat_bucket_start = current_bucket
    try:
        state_bytes = TELEMETRY_PATH.stat().st_size
    except OSError:
        state_bytes = 0
    add_current_phase_metadata(
        bucket_count=len(raw_document.get("buckets", [])),
        file_bytes=state_bytes,
    )
    if rollup_needed:
        _refresh_hourly_rollups(raw_document, now_epoch=now)
        _last_hourly_refresh_bucket_start = current_hour
    return had_pending


def flush_coverage_telemetry(*, now_epoch: float | None = None) -> bool:
    """Raw merge와 선택적 hourly rebuild overlap을 함께 계측한다."""
    with phase_memory(
        "coverage_telemetry_flush",
        metadata={"workload": "coverage", "operation": "flush"},
        include_gc_counts=True,
        include_object_count=True,
    ):
        return _flush_coverage_telemetry_body(now_epoch=now_epoch)


def _ratio(
    numerator: int | None, denominator: int | None,
) -> float | None:
    if numerator is None or denominator in {None, 0}:
        return None
    return round(numerator / denominator * 100, 4)


def coverage_report(*, now_epoch: float | None = None) -> dict[str, Any]:
    """보존 bucket 전체의 event/unique estimate와 명시적 ratio를 반환한다."""
    flush_coverage_telemetry(now_epoch=now_epoch)
    with state_store.exclusive_file_lock(TELEMETRY_PATH):
        document = state_store.read_json(TELEMETRY_PATH, _empty_document())
    cutoff = _bucket_start(now_epoch) - (MAX_BUCKETS - 1) * BUCKET_SECONDS
    buckets = [
        bucket
        for bucket in document.get("buckets", [])
        if isinstance(bucket, dict)
        and int(bucket.get("bucket_start_epoch", 0) or 0) >= cutoff
    ][-MAX_BUCKETS:]
    families: dict[str, Any] = {}
    rpc_dimensions: dict[str, dict[str, Any]] = {}
    rpc_methods: dict[str, dict[str, Any]] = {}
    rpc_dimension_overflow_event_count = 0
    rpc_method_dimension_overflow_event_count = 0
    for bucket in buckets:
        rpc_dimension_overflow_event_count += int(
            bucket.get("rpc_dimension_overflow_event_count", 0) or 0
        )
        for key, metric in bucket.get("rpc_confirmation", {}).items():
            if not isinstance(metric, dict):
                continue
            aggregate = rpc_dimensions.setdefault(key, {
                "event_count": 0,
                "unique_bitmap": 0,
            })
            aggregate["event_count"] += int(metric.get("event_count", 0) or 0)
            aggregate["unique_bitmap"] |= _bitmap_value(
                metric.get("unique_bitmap_hex")
            )
        rpc_method_dimension_overflow_event_count += int(
            bucket.get("rpc_method_dimension_overflow_event_count", 0) or 0
        )
        for key, metric in bucket.get("rpc_methods", {}).items():
            if not isinstance(metric, dict):
                continue
            aggregate = rpc_methods.setdefault(key, _rpc_method_metric())
            _merge_rpc_method_metric(aggregate, metric)
    for family in sorted(FAMILIES):
        stage_events = {stage: 0 for stage in FUNNEL_STAGES}
        stage_bitmaps = {stage: 0 for stage in FUNNEL_STAGES}
        for bucket in buckets:
            stages = bucket.get("families", {}).get(family, {})
            for stage, metric in stages.items():
                if stage not in FUNNEL_STAGES or not isinstance(metric, dict):
                    continue
                stage_events[stage] += int(metric.get("event_count", 0) or 0)
                stage_bitmaps[stage] |= _bitmap_value(
                    metric.get("unique_bitmap_hex")
                )
        stage_unique = {}
        stages_report = {}
        for stage in sorted(FUNNEL_STAGES):
            estimate, occupied, saturated = _unique_estimate(
                stage_bitmaps[stage]
            )
            stage_unique[stage] = estimate
            stages_report[stage] = {
                "event_count": stage_events[stage],
                "unique_mint_count_estimate": estimate,
                "unique_bitmap_occupied_bits": occupied,
                "unique_estimate_saturated": saturated,
            }
        families[family] = {
            "stages": stages_report,
            "unique_estimate_ratios_percent": {
                "confirmation_coverage": _ratio(
                    stage_unique["rpc_confirmation_succeeded"],
                    stage_unique["rpc_confirmation_started"],
                ),
                "observation_creation_coverage": _ratio(
                    stage_unique["observation_created"],
                    stage_unique["candidate_considered"],
                ),
                "analyzer_completion_coverage": _ratio(
                    stage_unique["analyzer_completed"],
                    stage_unique["analyzer_started"],
                ),
                "prospective_eligibility_coverage": _ratio(
                    stage_unique["prospective_eligible"],
                    stage_unique["observation_created"],
                ),
                "horizon_60m_completion_coverage": _ratio(
                    stage_unique["horizon_60m_successful"],
                    stage_unique["horizon_60m_due"],
                ),
                "horizon_60m_disposition_coverage": _ratio(
                    _unique_estimate(
                        stage_bitmaps["horizon_60m_successful"]
                        | stage_bitmaps["horizon_60m_missed"]
                        | stage_bitmaps["horizon_60m_unavailable"]
                    )[0],
                    stage_unique["horizon_60m_due"],
                ),
            },
        }
    return {
        "schema_version": TELEMETRY_SCHEMA_VERSION,
        "bucket_count": len(buckets),
        "families": families,
        "rpc_confirmation_by_dimension": [
            {
                "family": key.split("|", 3)[0],
                "method": key.split("|", 3)[1],
                "provider": key.split("|", 3)[2],
                "result": key.split("|", 3)[3],
                "event_count": metric["event_count"],
                "unique_mint_count_estimate": _unique_estimate(
                    metric["unique_bitmap"]
                )[0],
                "unique_bitmap_occupied_bits": _unique_estimate(
                    metric["unique_bitmap"]
                )[1],
                "unique_estimate_saturated": _unique_estimate(
                    metric["unique_bitmap"]
                )[2],
            }
            for key, metric in sorted(rpc_dimensions.items())
            if len(key.split("|", 3)) == 4
        ],
        "rpc_dimension_overflow_event_count": (
            rpc_dimension_overflow_event_count
        ),
        "rpc_methods": [
            {
                "provider": key.split("|", 1)[0],
                "method": key.split("|", 1)[1],
                **metric,
                "latency_average_ms": (
                    round(
                        float(metric.get("latency_sum_ms", 0.0) or 0.0)
                        / int(metric.get("latency_count", 0) or 0),
                        3,
                    )
                    if int(metric.get("latency_count", 0) or 0)
                    else None
                ),
            }
            for key, metric in sorted(rpc_methods.items())
            if len(key.split("|", 1)) == 2
        ],
        "rpc_method_dimension_overflow_event_count": (
            rpc_method_dimension_overflow_event_count
        ),
        "ratio_denominators": {
            "confirmation_coverage": (
                "unique rpc_confirmation_started mint bitmap estimate"
            ),
            "observation_creation_coverage": (
                "unique candidate_considered mint bitmap estimate"
            ),
            "analyzer_completion_coverage": (
                "unique analyzer_started mint bitmap estimate"
            ),
            "prospective_eligibility_coverage": (
                "unique observation_created mint bitmap estimate"
            ),
            "horizon_60m_completion_coverage": (
                "unique horizon_60m_due mint bitmap estimate"
            ),
            "horizon_60m_disposition_coverage": (
                "unique horizon_60m_due mint bitmap estimate"
            ),
        },
    }


def coverage_review_window_status(
    *, required_hours: int = 24, now_epoch: float | None = None,
) -> dict[str, Any]:
    """최근 연속 종료 UTC hour가 review 가능한지 파일 변경 없이 확인한다."""
    required = int(required_hours)
    if required <= 0 or required > MAX_HOURLY_ROLLUPS:
        raise ValueError("required_hours is outside hourly retention")
    now = _safe_epoch(now_epoch)
    end_exclusive = _hour_start(now)
    starts = list(range(
        end_exclusive - required * HOUR_SECONDS,
        end_exclusive,
        HOUR_SECONDS,
    ))
    with state_store.exclusive_file_lock(HOURLY_TELEMETRY_PATH):
        document = state_store.read_json(
            HOURLY_TELEMETRY_PATH, _empty_hourly_document()
        )
    indexed = {
        int(hour.get("hour_start_epoch", 0) or 0): hour
        for hour in document.get("hours", [])
        if isinstance(hour, dict)
    }
    complete = [
        start for start in starts
        if str(indexed.get(start, {}).get("status")) == "COMPLETE"
        and bool(indexed.get(start, {}).get("complete"))
    ]
    partial = [
        start for start in starts
        if str(indexed.get(start, {}).get("status")) == "PARTIAL"
    ]
    missing = [
        start for start in starts
        if start not in indexed
        or str(indexed.get(start, {}).get("status")) == "MISSING"
    ]
    eligible = len(complete) == required
    reason = "READY" if eligible else (
        "MISSING_HOURS" if missing else "PARTIAL_HOURS"
    )
    return {
        "eligible": eligible,
        "eligibility_status": (
            "READY" if eligible else "INCOMPLETE_WINDOW"
        ),
        "completed_hours": len(complete),
        "required_hours": required,
        "partial_hours": len(partial),
        "missing_hours": len(missing),
        "partial_hour_starts": partial,
        "missing_hour_starts": missing,
        "window_start_epoch": starts[0],
        "window_start_utc": _epoch_iso(starts[0]),
        "window_end_epoch_exclusive": end_exclusive,
        "window_end_utc_exclusive": _epoch_iso(end_exclusive),
        "eligibility_reason": reason,
    }


def _review_summary(hours: list[dict[str, Any]]) -> dict[str, Any]:
    combined = _empty_bucket(0)
    for hour in hours:
        _merge_bucket(combined, hour)
    families: dict[str, Any] = {}
    ratio_pairs = {
        "confirmation_coverage": (
            "rpc_confirmation_succeeded", "rpc_confirmation_started"
        ),
        "observation_creation_coverage": (
            "observation_created", "candidate_considered"
        ),
        "analyzer_completion_coverage": (
            "analyzer_completed", "analyzer_started"
        ),
        "prospective_eligibility_coverage": (
            "prospective_eligible", "observation_created"
        ),
        "horizon_60m_completion_coverage": (
            "horizon_60m_successful", "horizon_60m_due"
        ),
    }
    for family in sorted(FAMILIES):
        stages = combined.get("families", {}).get(family, {})
        stage_report: dict[str, Any] = {}
        for stage in sorted(FUNNEL_STAGES):
            metric = stages.get(stage, {})
            estimate, occupied, saturated = _unique_estimate(
                _bitmap_value(metric.get("unique_bitmap_hex"))
            )
            stage_report[stage] = {
                "event_count": int(metric.get("event_count", 0) or 0),
                "unique_mint_count_estimate": estimate,
                "unique_bitmap_occupied_bits": occupied,
                "unique_estimate_saturated": saturated,
            }
        event_ratios = {}
        unique_ratios = {}
        for name, (numerator, denominator) in ratio_pairs.items():
            event_ratios[name] = _ratio(
                stage_report[numerator]["event_count"],
                stage_report[denominator]["event_count"],
            )
            unique_ratios[name] = _ratio(
                stage_report[numerator]["unique_mint_count_estimate"],
                stage_report[denominator]["unique_mint_count_estimate"],
            )
        disposition_stages = (
            "horizon_60m_successful",
            "horizon_60m_missed",
            "horizon_60m_unavailable",
        )
        event_ratios["horizon_60m_disposition_coverage"] = _ratio(
            sum(stage_report[name]["event_count"] for name in disposition_stages),
            stage_report["horizon_60m_due"]["event_count"],
        )
        disposition_bitmap = 0
        for name in disposition_stages:
            disposition_bitmap |= _bitmap_value(
                stages.get(name, {}).get("unique_bitmap_hex")
            )
        unique_ratios["horizon_60m_disposition_coverage"] = _ratio(
            _unique_estimate(disposition_bitmap)[0],
            stage_report["horizon_60m_due"]["unique_mint_count_estimate"],
        )
        families[family] = {
            "stages": stage_report,
            "event_ratios_percent": event_ratios,
            "unique_estimate_ratios_percent": unique_ratios,
        }
    rpc_confirmation = []
    for key, metric in sorted(combined.get("rpc_confirmation", {}).items()):
        parts = key.split("|", 3)
        if len(parts) != 4:
            continue
        estimate, occupied, saturated = _unique_estimate(
            _bitmap_value(metric.get("unique_bitmap_hex"))
        )
        rpc_confirmation.append({
            "family": parts[0],
            "method": parts[1],
            "provider": parts[2],
            "result": parts[3],
            "event_count": int(metric.get("event_count", 0) or 0),
            "unique_mint_count_estimate": estimate,
            "unique_bitmap_occupied_bits": occupied,
            "unique_estimate_saturated": saturated,
        })
    rpc_methods = []
    for key, metric in sorted(combined.get("rpc_methods", {}).items()):
        parts = key.split("|", 1)
        if len(parts) != 2:
            continue
        item = {"provider": parts[0], "method": parts[1]}
        item.update({
            name: metric.get(name, 0)
            for name in _rpc_method_metric()
        })
        latency_count = int(metric.get("latency_count", 0) or 0)
        item["latency_average_ms"] = (
            round(float(metric.get("latency_sum_ms", 0.0) or 0.0)
                  / latency_count, 3)
            if latency_count else None
        )
        rpc_methods.append(item)
    worst_hours = []
    for hour in hours:
        for family in ("SMART_MONEY", "MOMENTUM"):
            stages = hour.get("families", {}).get(family, {})
            worst_hours.append({
                "hour_start_epoch": hour.get("hour_start_epoch"),
                "hour_start_utc": hour.get("hour_start_utc"),
                "family": family,
                "rpc_confirmation_failed": int(stages.get(
                    "rpc_confirmation_failed", {}
                ).get("event_count", 0) or 0),
                "analyzer_failed": int(stages.get(
                    "analyzer_failed", {}
                ).get("event_count", 0) or 0),
                "quote_preflight_failed": int(stages.get(
                    "quote_preflight_failed", {}
                ).get("event_count", 0) or 0),
            })
    return {
        "families": families,
        "rpc_failure_taxonomy": rpc_confirmation,
        "rpc_methods": rpc_methods,
        "worst_hours": sorted(
            worst_hours,
            key=lambda item: (
                -item["rpc_confirmation_failed"],
                -item["analyzer_failed"],
                -item["quote_preflight_failed"],
            ),
        )[:5],
        "overflow": bool(
            combined.get("rpc_dimension_overflow_event_count")
            or combined.get("rpc_method_dimension_overflow_event_count")
        ),
    }


def coverage_review_report(
    *, required_hours: int = 24, now_epoch: float | None = None,
) -> dict[str, Any]:
    """완료된 연속 hour만 24시간 Research coverage로 요약한다."""
    status = coverage_review_window_status(
        required_hours=required_hours,
        now_epoch=now_epoch,
    )
    if not status["eligible"]:
        return {"window_status": status, "summary": None}
    with state_store.exclusive_file_lock(HOURLY_TELEMETRY_PATH):
        document = state_store.read_json(
            HOURLY_TELEMETRY_PATH, _empty_hourly_document()
        )
    start = int(status["window_start_epoch"])
    end = int(status["window_end_epoch_exclusive"])
    hours = [
        hour for hour in document.get("hours", [])
        if isinstance(hour, dict)
        and start <= int(hour.get("hour_start_epoch", 0) or 0) < end
        and str(hour.get("status")) == "COMPLETE"
    ]
    return {"window_status": status, "summary": _review_summary(hours)}


def reset_pending_telemetry() -> None:
    """테스트에서 process-local pending aggregate만 초기화한다."""
    global _last_hourly_refresh_bucket_start
    global _last_raw_heartbeat_bucket_start
    with _pending_lock:
        _pending_buckets.clear()
    _last_hourly_refresh_bucket_start = None
    _last_raw_heartbeat_bucket_start = None
