"""Research funnel 누락을 민감정보 없이 bounded aggregate로 계측한다."""

from __future__ import annotations

import hashlib
import math
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src import state_store


TELEMETRY_PATH = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "research_coverage_telemetry.json"
)
TELEMETRY_SCHEMA_VERSION = 1
BUCKET_SECONDS = 900
MAX_BUCKETS = 24
MAX_RPC_DIMENSIONS_PER_BUCKET = 64
UNIQUE_BITMAP_BITS = 512
UNIQUE_BITMAP_HEX_LENGTH = UNIQUE_BITMAP_BITS // 4

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


def _empty_document() -> dict[str, Any]:
    return {
        "schema_version": TELEMETRY_SCHEMA_VERSION,
        "version": 0,
        "bucket_seconds": BUCKET_SECONDS,
        "retention_buckets": MAX_BUCKETS,
        "unique_bitmap_bits": UNIQUE_BITMAP_BITS,
        "rpc_dimension_limit_per_bucket": MAX_RPC_DIMENSIONS_PER_BUCKET,
        "buckets": [],
        "updated_at": None,
    }


def _empty_bucket(bucket_start_epoch: int) -> dict[str, Any]:
    return {
        "bucket_start_epoch": int(bucket_start_epoch),
        "families": {},
        "rpc_confirmation": {},
        "rpc_dimension_overflow_event_count": 0,
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


def _merge_metric(target: dict[str, Any], source: dict[str, Any]) -> None:
    target["event_count"] = (
        int(target.get("event_count", 0) or 0)
        + int(source.get("event_count", 0) or 0)
    )
    target["unique_bitmap_hex"] = _bitmap_hex(
        _bitmap_value(target.get("unique_bitmap_hex"))
        | _bitmap_value(source.get("unique_bitmap_hex"))
    )


def _merge_bucket(target: dict[str, Any], source: dict[str, Any]) -> None:
    for family, stages in source.get("families", {}).items():
        target_stages = target.setdefault("families", {}).setdefault(family, {})
        for stage, metric in stages.items():
            _merge_metric(target_stages.setdefault(stage, _stage_metric()), metric)
    target_dimensions = target.setdefault("rpc_confirmation", {})
    for key, metric in source.get("rpc_confirmation", {}).items():
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


def _merge_pending_back(pending: dict[int, dict[str, Any]]) -> None:
    with _pending_lock:
        for start, source in pending.items():
            target = _pending_bucket(start)
            _merge_bucket(target, source)


def flush_coverage_telemetry(*, now_epoch: float | None = None) -> bool:
    """메모리 집계를 단일 원자적 write로 병합하고 최근 6시간만 유지한다."""
    with _pending_lock:
        if not _pending_buckets:
            return False
        pending = dict(_pending_buckets)
        _pending_buckets.clear()
    now = _safe_epoch(now_epoch)

    def mutate(document: dict[str, Any]) -> None:
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
        cutoff = _bucket_start(now) - (MAX_BUCKETS - 1) * BUCKET_SECONDS
        document.update({
            "schema_version": TELEMETRY_SCHEMA_VERSION,
            "bucket_seconds": BUCKET_SECONDS,
            "retention_buckets": MAX_BUCKETS,
            "unique_bitmap_bits": UNIQUE_BITMAP_BITS,
            "rpc_dimension_limit_per_bucket": MAX_RPC_DIMENSIONS_PER_BUCKET,
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
        state_store.update_json(TELEMETRY_PATH, _empty_document(), mutate)
    except Exception:
        _merge_pending_back(pending)
        raise
    return True


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
    rpc_dimension_overflow_event_count = 0
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


def reset_pending_telemetry() -> None:
    """테스트에서 process-local pending aggregate만 초기화한다."""
    with _pending_lock:
        _pending_buckets.clear()
