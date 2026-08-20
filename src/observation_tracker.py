"""Approved-signal observation ledger without reserving paper cash."""

from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
from dotenv import load_dotenv

from src.logging_utils import redact_sensitive_text
from src.state_store import migrate_json, read_json, update_json

logger = logging.getLogger("signal-observer")

OBSERVATION_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "signal_observations.json"
)
OBSERVATION_INTERVALS = (("1m", 60), ("5m", 300), ("15m", 900))
MAX_OBSERVATIONS = 1_000
MAX_ACTIVE_OBSERVATIONS = 200
OBSERVATION_SAMPLE_BATCH_SIZE = 20
MAX_SAMPLE_ATTEMPTS = 3
OBSERVATION_SCHEMA_VERSION = 4
TERMINAL_OBSERVATION_STATUSES = {"COMPLETE", "EXPIRED_UNSAMPLED"}
CANDIDATE_V2_MIN_SCORE = 90.0
CANDIDATE_V2_MAX_SCORE = 100.0
CANDIDATE_V2_MINT_COOLDOWN_SECONDS = 86_400.0
CANDIDATE_V2_EARLY_FAILURE_PERCENT = -10.0


@dataclass(frozen=True, slots=True)
class ObservationDecision:
    created: bool
    observation_id: str
    candidate_v2_eligible: bool
    strategy_variants: tuple[str, ...]


def empty_observations() -> dict[str, Any]:
    return {
        "schema_version": OBSERVATION_SCHEMA_VERSION,
        "observations": [],
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "version": 0,
    }


def migrate_observation_document(document: dict[str, Any]) -> bool:
    """기존 관찰 표본을 보존하며 현재 스키마의 분석 필드를 보완한다."""
    schema_version = int(document.get("schema_version", 1) or 1)
    if schema_version > OBSERVATION_SCHEMA_VERSION:
        raise RuntimeError("signal observation schema is newer than this service")
    rows = document.setdefault("observations", [])
    if not isinstance(rows, list):
        raise RuntimeError("signal observation ledger is malformed")
    changed = schema_version != OBSERVATION_SCHEMA_VERSION
    for row in rows:
        if not isinstance(row, dict):
            raise RuntimeError("signal observation row is malformed")
        defaults = {
            "strategy_version": "baseline_v1",
            "strategy_variants": ["baseline_v1"],
            "safety_metrics": {},
            "paper_experiment_status": "LEGACY_OBSERVATION",
            "paper_experiment_position_id": None,
            "candidate_v2_paper_status": "LEGACY_OBSERVATION",
            "candidate_v2_position_id": None,
            "sample_attempts": {},
            "samples": [],
            "decision_status": "APPROVED",
            "decision_reasons": [],
            "quote_status": "EXECUTABLE",
            "discovery_metadata": {},
        }
        for key, value in defaults.items():
            if key not in row:
                row[key] = value
                changed = True
        if not isinstance(row.get("samples"), list):
            raise RuntimeError("signal observation samples are malformed")
        if not isinstance(row.get("sample_attempts"), dict):
            raise RuntimeError("signal observation sample attempts are malformed")
        if not isinstance(row.get("safety_metrics"), dict):
            raise RuntimeError("signal observation safety metrics are malformed")
        if not isinstance(row.get("decision_reasons"), list):
            raise RuntimeError("signal observation decision reasons are malformed")
        if not isinstance(row.get("discovery_metadata"), dict):
            raise RuntimeError("signal observation discovery metadata is malformed")
    if changed:
        document["schema_version"] = OBSERVATION_SCHEMA_VERSION
        document["updated_at"] = datetime.now(timezone.utc).isoformat()
    return changed


def ensure_observations_migrated() -> dict[str, Any]:
    return migrate_json(
        OBSERVATION_PATH,
        empty_observations(),
        migrate_observation_document,
    )


def normalized_safety_metrics(metrics: dict[str, Any] | None) -> dict[str, Any]:
    """분석용 안전 게이트 값을 유한하고 제한된 스냅샷으로 정규화한다."""
    source = metrics if isinstance(metrics, dict) else {}

    def finite_number(
        name: str,
        *,
        minimum: float | None = None,
        maximum: float | None = None,
    ) -> float | None:
        value = source.get(name)
        if value is None or value == "":
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(number):
            return None
        if minimum is not None and number < minimum:
            return None
        if maximum is not None and number > maximum:
            return None
        return number

    def strict_bool(name: str) -> bool:
        value = source.get(name)
        return value if isinstance(value, bool) else False

    def limited_strings(name: str) -> list[str]:
        values = source.get(name)
        if not isinstance(values, (list, tuple)):
            return []
        return [str(value)[:200] for value in values[:20]]

    return {
        "developer_supply_percent": finite_number(
            "developer_supply_percent", minimum=0.0, maximum=100.0
        ),
        "developer_below_ten_percent": strict_bool(
            "developer_below_ten_percent"
        ),
        "mint_authority_renounced": strict_bool("mint_authority_renounced"),
        "lp_locked": strict_bool("lp_locked"),
        "lp_locked_percent": finite_number(
            "lp_locked_percent", minimum=0.0, maximum=100.0
        ),
        "liquidity_usd": finite_number("liquidity_usd", minimum=0.0),
        "liquidity_above_minimum": strict_bool("liquidity_above_minimum"),
        "reasons": limited_strings("reasons"),
        "sources": limited_strings("sources"),
    }


def retained_observations(rows: list[Any]) -> list[Any]:
    """진행 중 표본은 보존하고 완료 표본만 오래된 순서로 제한한다."""
    active = [
        row for row in rows
        if isinstance(row, dict)
        and (
            row.get("status") not in TERMINAL_OBSERVATION_STATUSES
            or row.get("paper_experiment_status") == "OPENED"
        )
    ]
    completed = [
        row for row in rows
        if isinstance(row, dict)
        and row.get("status") in TERMINAL_OBSERVATION_STATUSES
        and row.get("paper_experiment_status") != "OPENED"
    ]
    completed_budget = max(0, MAX_OBSERVATIONS - len(active))
    kept_completed = completed[-completed_budget:] if completed_budget else []
    kept_ids = {id(row) for row in active + kept_completed}
    return [row for row in rows if id(row) in kept_ids]


def expire_observation_backlog(rows: list[Any]) -> None:
    """장애 중 쌓인 미실행 표본을 명시적으로 만료해 원장을 제한한다."""
    expirable = [
        row for row in rows
        if isinstance(row, dict)
        and row.get("status") not in TERMINAL_OBSERVATION_STATUSES
        and row.get("paper_experiment_status") != "OPENED"
    ]
    overflow = max(0, len(expirable) - MAX_ACTIVE_OBSERVATIONS)
    for row in expirable[:overflow]:
        row["status"] = "EXPIRED_UNSAMPLED"
        row["expiration_reason"] = "ACTIVE_OBSERVATION_LIMIT"
        row["completed_at"] = datetime.now(timezone.utc).isoformat()


def observation_mode_enabled() -> bool:
    raw = os.getenv("OBSERVATION_MODE", "false").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError("OBSERVATION_MODE must be true or false")


def approved_signal_paper_mode_enabled() -> bool:
    """모든 안전 승인 신호를 가상매매 검증에도 보낼지 반환한다."""
    raw = os.getenv("APPROVED_SIGNAL_PAPER_MODE", "false").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError("APPROVED_SIGNAL_PAPER_MODE must be true or false")


def approved_signal_max_open_positions() -> int:
    """Jupiter 순차 청산 점검이 밀리지 않도록 실험 포지션 수를 제한한다."""
    raw = os.getenv("APPROVED_SIGNAL_MAX_OPEN_POSITIONS", "8").strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(
            "APPROVED_SIGNAL_MAX_OPEN_POSITIONS must be an integer"
        ) from exc
    if not 1 <= value <= 20:
        raise RuntimeError(
            "APPROVED_SIGNAL_MAX_OPEN_POSITIONS must be between 1 and 20"
        )
    return value


def strategy_variants(route_type: str, score: float) -> tuple[str, ...]:
    """한 승인 신호를 동시에 비교할 고정 전략 집단으로 분류한다."""
    route = str(route_type).upper()
    variants = ["baseline_v1", f"route_{route.lower()}_baseline"]
    if route != "B":
        return tuple(variants)
    if score < CANDIDATE_V2_MIN_SCORE:
        variants.append("route_b_score_below_90")
    elif score < CANDIDATE_V2_MAX_SCORE:
        variants.append("candidate_v2_score_90_to_below_100")
    else:
        variants.append("route_b_score_100_or_more")
    return tuple(variants)


def _bounded_metadata(values: dict[str, Any] | None) -> dict[str, Any]:
    source = values if isinstance(values, dict) else {}
    bounded: dict[str, Any] = {}
    for key, value in list(source.items())[:30]:
        name = str(key)[:80]
        if isinstance(value, bool) or value is None:
            bounded[name] = value
        elif isinstance(value, (int, float)):
            number = float(value)
            bounded[name] = number if math.isfinite(number) else None
        else:
            bounded[name] = str(value)[:200]
    return bounded


async def record_candidate_discovery(
    *,
    mint: str,
    route_type: str,
    source_wallet: str,
    source_signature: str,
    token_amount_raw: int,
    token_decimals: int,
    signal_detected_at: str,
    dex_momentum_score: float = 0.0,
    momentum_metrics: dict[str, int | float] | None = None,
    discovery_metadata: dict[str, Any] | None = None,
) -> ObservationDecision:
    """거래 게이트 전에 후보를 원자적으로 기록해 탈락 표본도 보존한다."""
    observation_id = f"{source_signature}:{source_wallet}:{mint}"
    started_at = time.time()
    score = _float_or_zero(dex_momentum_score)
    variants = strategy_variants(route_type, score)

    def mutate(document: dict[str, Any]) -> ObservationDecision:
        migrate_observation_document(document)
        rows = document.setdefault("observations", [])
        existing = next((
            row for row in rows
            if isinstance(row, dict) and row.get("observation_id") == observation_id
        ), None)
        if isinstance(existing, dict):
            return ObservationDecision(
                False,
                observation_id,
                existing.get("candidate_v2_eligible") is True,
                tuple(existing.get("strategy_variants") or variants),
            )
        rows.append({
            "observation_id": observation_id,
            "mint": mint,
            "route_type": str(route_type).upper(),
            "source_wallet": source_wallet,
            "source_signature": source_signature,
            "safety_score": None,
            "entry_cost_lamports": 0,
            "token_amount_raw": max(0, int(token_amount_raw)),
            "token_decimals": max(0, int(token_decimals)),
            "entry_price_impact_pct": None,
            "exit_price_impact_pct": None,
            "expected_slippage_bps": None,
            "dex_momentum_score": score,
            "strategy_version": "broad_observation_v1",
            "strategy_variants": list(variants),
            "safety_metrics": {},
            "momentum_metrics": _normalized_momentum_metrics(momentum_metrics),
            "candidate_v2_eligible": False,
            "candidate_v2_filter_reasons": ["DECISION_PENDING"],
            "candidate_v2_early_failure": None,
            "candidate_v2_paper_status": "NOT_EVALUATED",
            "candidate_v2_position_id": None,
            "paper_experiment_status": "NOT_EVALUATED",
            "paper_experiment_position_id": None,
            "decision_status": "DISCOVERED",
            "decision_reasons": [],
            "quote_status": "NOT_REQUESTED",
            "discovery_metadata": _bounded_metadata(discovery_metadata),
            "signal_detected_at": signal_detected_at,
            "analysis_completed_at": None,
            "entry_quote_at": None,
            "entry_latency_ms": None,
            "started_at_epoch": started_at,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "samples": [],
            "sample_attempts": {},
            "status": "DISCOVERED",
        })
        expire_observation_backlog(rows)
        document["observations"] = retained_observations(rows)
        document["schema_version"] = OBSERVATION_SCHEMA_VERSION
        document["updated_at"] = datetime.now(timezone.utc).isoformat()
        return ObservationDecision(True, observation_id, False, variants)

    decision, _ = await asyncio.to_thread(
        update_json, OBSERVATION_PATH, empty_observations(), mutate
    )
    return decision


def _float_or_zero(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def _normalized_momentum_metrics(
    metrics: dict[str, int | float] | None,
) -> dict[str, int | float] | None:
    if not isinstance(metrics, dict):
        return None
    return {
        "volume_m5_usd": _float_or_zero(metrics.get("volume_m5_usd", 0)),
        "buys_m5": int(_float_or_zero(metrics.get("buys_m5", 0))),
        "sells_m5": int(_float_or_zero(metrics.get("sells_m5", 0))),
        "net_buys_m5": int(_float_or_zero(metrics.get("net_buys_m5", 0))),
        "buy_sell_ratio_m5": _float_or_zero(metrics.get("buy_sell_ratio_m5", 0)),
        "liquidity_usd": _float_or_zero(metrics.get("liquidity_usd", 0)),
        "pair_age_seconds": _float_or_zero(metrics.get("pair_age_seconds", 0)),
        "unknown_whale_count": int(_float_or_zero(metrics.get("unknown_whale_count", 0))),
    }


def finalize_candidate_without_quote(
    observation_id: str,
    *,
    decision_status: str,
    decision_reasons: list[str] | tuple[str, ...],
    quote_status: str,
    safety_score: int | None = None,
    safety_metrics: dict[str, Any] | None = None,
    analysis_completed_at: str | None = None,
) -> bool:
    """실행 가격을 얻지 못한 후보도 유실 없이 terminal 상태로 마감한다."""
    normalized_decision = str(decision_status).upper()
    if normalized_decision not in {"REJECTED", "UNAVAILABLE", "FAILED"}:
        raise ValueError("unsupported candidate decision status")

    def mutate(document: dict[str, Any]) -> bool:
        migrate_observation_document(document)
        target = next((
            row for row in document.get("observations", [])
            if isinstance(row, dict) and row.get("observation_id") == observation_id
        ), None)
        if not isinstance(target, dict):
            return False
        target["decision_status"] = normalized_decision
        target["decision_reasons"] = [str(reason)[:200] for reason in decision_reasons[:20]]
        target["quote_status"] = str(quote_status).upper()[:80]
        target["paper_experiment_status"] = "NOT_ELIGIBLE"
        target["candidate_v2_paper_status"] = "NOT_ELIGIBLE"
        target["safety_score"] = safety_score
        target["safety_metrics"] = normalized_safety_metrics(safety_metrics)
        target["analysis_completed_at"] = analysis_completed_at
        target["status"] = "COMPLETE"
        target["completed_at"] = datetime.now(timezone.utc).isoformat()
        document["observations"] = retained_observations(
            document.get("observations", [])
        )
        document["updated_at"] = datetime.now(timezone.utc).isoformat()
        return True

    changed, _ = update_json(OBSERVATION_PATH, empty_observations(), mutate)
    return bool(changed)


async def record_observation_decision(
    *,
    mint: str,
    route_type: str,
    source_wallet: str,
    source_signature: str,
    safety_score: int,
    entry_cost_lamports: int,
    token_amount_raw: int,
    token_decimals: int,
    entry_price_impact_pct: float,
    exit_price_impact_pct: float | None,
    expected_slippage_bps: int,
    dex_momentum_score: float,
    signal_detected_at: str,
    analysis_completed_at: str,
    entry_quote_at: str,
    entry_latency_ms: int,
    momentum_metrics: dict[str, int | float] | None = None,
    safety_metrics: dict[str, Any] | None = None,
    decision_status: str = "APPROVED",
    decision_reasons: list[str] | tuple[str, ...] = (),
    quote_status: str = "EXECUTABLE",
) -> ObservationDecision:
    """승인 신호를 기록하고 원자적으로 확정된 실험 판정을 반환한다."""
    if entry_cost_lamports <= 0 or token_amount_raw <= 0:
        raise ValueError("observation quote amounts must be positive")
    observation_id = f"{source_signature}:{source_wallet}:{mint}"
    started_at = time.time()

    def mutate(document: dict[str, Any]) -> ObservationDecision:
        migrate_observation_document(document)
        rows = document.setdefault("observations", [])
        if not isinstance(rows, list):
            raise RuntimeError("signal observation ledger is malformed")
        existing = next((
            row for row in rows
            if isinstance(row, dict) and row.get("observation_id") == observation_id
        ), None)
        if isinstance(existing, dict) and existing.get("quote_status") == "EXECUTABLE":
            return ObservationDecision(
                False,
                observation_id,
                existing.get("candidate_v2_eligible") is True,
                tuple(existing.get("strategy_variants") or ("baseline_v1",)),
            )
        score = _float_or_zero(dex_momentum_score)
        normalized_decision = str(decision_status).upper()
        if normalized_decision not in {"APPROVED", "REJECTED"}:
            raise ValueError("unsupported observation decision status")
        candidate_reasons: list[str] = []
        candidate_eligible = route_type == "B" and normalized_decision == "APPROVED"
        if normalized_decision != "APPROVED":
            candidate_reasons.append("ENTRY_DECISION_REJECTED")
        if route_type != "B":
            candidate_reasons.append("ROUTE_NOT_B")
        if not CANDIDATE_V2_MIN_SCORE <= score < CANDIDATE_V2_MAX_SCORE:
            candidate_eligible = False
            candidate_reasons.append("MOMENTUM_OUTSIDE_90_TO_BELOW_100")
        if candidate_eligible and any(
            isinstance(row, dict)
            and row.get("mint") == mint
            and row.get("candidate_v2_eligible") is True
            and started_at - float(row.get("started_at_epoch", 0) or 0)
            < CANDIDATE_V2_MINT_COOLDOWN_SECONDS
            for row in rows
        ):
            candidate_eligible = False
            candidate_reasons.append("MINT_SEEN_WITHIN_24H")
        variants = strategy_variants(route_type, score)
        metrics = momentum_metrics if route_type == "B" else None
        payload = {
            "observation_id": observation_id,
            "mint": mint,
            "route_type": route_type,
            "source_wallet": source_wallet,
            "source_signature": source_signature,
            "safety_score": int(safety_score),
            "entry_cost_lamports": int(entry_cost_lamports),
            "token_amount_raw": int(token_amount_raw),
            "token_decimals": int(token_decimals),
            "entry_price_impact_pct": float(entry_price_impact_pct),
            "exit_price_impact_pct": (
                float(exit_price_impact_pct)
                if exit_price_impact_pct is not None
                else None
            ),
            "expected_slippage_bps": int(expected_slippage_bps),
            "dex_momentum_score": score,
            "strategy_version": "baseline_v1+candidate_v2",
            "strategy_variants": list(variants),
            "safety_metrics": normalized_safety_metrics(safety_metrics),
            "momentum_metrics": _normalized_momentum_metrics(metrics),
            "candidate_v2_eligible": candidate_eligible,
            "candidate_v2_filter_reasons": candidate_reasons,
            "candidate_v2_early_failure": None,
            "candidate_v2_paper_status": (
                "ELIGIBLE" if candidate_eligible else "NOT_ELIGIBLE"
            ),
            "candidate_v2_position_id": None,
            "paper_experiment_status": (
                "ELIGIBLE" if normalized_decision == "APPROVED" else "NOT_ELIGIBLE"
            ),
            "paper_experiment_position_id": None,
            "signal_detected_at": signal_detected_at,
            "analysis_completed_at": analysis_completed_at,
            "entry_quote_at": entry_quote_at,
            "entry_latency_ms": int(entry_latency_ms),
            "started_at_epoch": started_at,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "samples": [],
            "sample_attempts": {},
            "status": "PENDING",
            "decision_status": normalized_decision,
            "decision_reasons": [str(reason)[:200] for reason in decision_reasons[:20]],
            "quote_status": str(quote_status).upper()[:80],
            "discovery_metadata": (
                existing.get("discovery_metadata", {})
                if isinstance(existing, dict)
                else {}
            ),
        }
        if isinstance(existing, dict):
            payload["started_at_epoch"] = existing.get("started_at_epoch", started_at)
            payload["started_at"] = existing.get("started_at", payload["started_at"])
            existing.update(payload)
        else:
            rows.append(payload)
        expire_observation_backlog(rows)
        document["observations"] = retained_observations(rows)
        document["schema_version"] = OBSERVATION_SCHEMA_VERSION
        document["updated_at"] = datetime.now(timezone.utc).isoformat()
        return ObservationDecision(True, observation_id, candidate_eligible, variants)

    decision, _ = await asyncio.to_thread(
        update_json, OBSERVATION_PATH, empty_observations(), mutate
    )
    return decision


async def record_observation(
    *,
    mint: str,
    route_type: str,
    source_wallet: str,
    source_signature: str,
    safety_score: int,
    entry_cost_lamports: int,
    token_amount_raw: int,
    token_decimals: int,
    entry_price_impact_pct: float,
    exit_price_impact_pct: float,
    expected_slippage_bps: int,
    dex_momentum_score: float,
    signal_detected_at: str,
    analysis_completed_at: str,
    entry_quote_at: str,
    entry_latency_ms: int,
    momentum_metrics: dict[str, int | float] | None = None,
    safety_metrics: dict[str, Any] | None = None,
) -> bool:
    """Record one approved hypothetical entry without changing trading state."""
    decision = await record_observation_decision(
        mint=mint,
        route_type=route_type,
        source_wallet=source_wallet,
        source_signature=source_signature,
        safety_score=safety_score,
        entry_cost_lamports=entry_cost_lamports,
        token_amount_raw=token_amount_raw,
        token_decimals=token_decimals,
        entry_price_impact_pct=entry_price_impact_pct,
        exit_price_impact_pct=exit_price_impact_pct,
        expected_slippage_bps=expected_slippage_bps,
        dex_momentum_score=dex_momentum_score,
        momentum_metrics=momentum_metrics,
        safety_metrics=safety_metrics,
        signal_detected_at=signal_detected_at,
        analysis_completed_at=analysis_completed_at,
        entry_quote_at=entry_quote_at,
        entry_latency_ms=entry_latency_ms,
    )
    return decision.created


def mark_paper_experiment_status(
    observation_id: str,
    status: str,
    *,
    position_id: str | None = None,
) -> bool:
    """관찰과 광범위 페이퍼 실험 포지션의 연결 상태를 갱신한다."""
    allowed = {"ELIGIBLE", "OPENED", "CLOSED", "SKIPPED_CAPACITY", "FAILED"}
    normalized = str(status).upper()
    if normalized not in allowed:
        raise ValueError("unsupported paper experiment status")

    def mutate(document: dict[str, Any]) -> bool:
        migrate_observation_document(document)
        target = next((
            row for row in document.get("observations", [])
            if isinstance(row, dict)
            and row.get("observation_id") == observation_id
        ), None)
        if not isinstance(target, dict):
            return False
        target["paper_experiment_status"] = normalized
        target["paper_experiment_position_id"] = position_id
        if target.get("candidate_v2_eligible") is True:
            target["candidate_v2_paper_status"] = normalized
            target["candidate_v2_position_id"] = position_id
        document["observations"] = retained_observations(
            document.get("observations", [])
        )
        document["updated_at"] = datetime.now(timezone.utc).isoformat()
        return True

    changed, _ = update_json(
        OBSERVATION_PATH, empty_observations(), mutate
    )
    return bool(changed)


def due_observation_samples(now: float) -> list[tuple[str, str, str, int, int]]:
    """Return at most one due interval per observation to bound quote traffic."""
    document = ensure_observations_migrated()
    due: list[tuple[str, str, str, int, int]] = []
    for row in document.get("observations", []):
        if (
            not isinstance(row, dict)
            or row.get("status") != "PENDING"
        ):
            continue
        completed = {
            str(sample.get("interval"))
            for sample in row.get("samples", [])
            if isinstance(sample, dict)
        }
        started_at = float(row.get("started_at_epoch", 0) or 0)
        for label, delay in OBSERVATION_INTERVALS:
            if label not in completed and now >= started_at + delay:
                due.append((
                    str(row.get("observation_id", "")),
                    label,
                    str(row.get("mint", "")),
                    int(row.get("token_amount_raw", 0) or 0),
                    int(row.get("entry_cost_lamports", 0) or 0),
                ))
                if len(due) >= OBSERVATION_SAMPLE_BATCH_SIZE:
                    return due
                break
    return due


def record_sample_attempt(
    observation_id: str,
    interval: str,
    *,
    error: str,
) -> int:
    """실패 횟수를 원자적으로 기록해 영구적인 샘플 선점을 방지한다."""
    allowed = {label for label, _ in OBSERVATION_INTERVALS}
    if interval not in allowed:
        raise ValueError("unsupported observation interval")

    def mutate(document: dict[str, Any]) -> int:
        migrate_observation_document(document)
        target = next((
            row for row in document.get("observations", [])
            if isinstance(row, dict)
            and row.get("observation_id") == observation_id
        ), None)
        if not isinstance(target, dict):
            return 0
        if any(
            isinstance(sample, dict) and sample.get("interval") == interval
            for sample in target.get("samples", [])
        ):
            return 0
        attempts = target.setdefault("sample_attempts", {})
        count = int(attempts.get(interval, 0) or 0) + 1
        attempts[interval] = count
        target["sample_last_error"] = error[:500]
        target["sample_last_attempt_at"] = datetime.now(timezone.utc).isoformat()
        document["updated_at"] = datetime.now(timezone.utc).isoformat()
        return count

    count, _ = update_json(
        OBSERVATION_PATH,
        empty_observations(),
        mutate,
    )
    return int(count)


def record_sample(
    observation_id: str,
    interval: str,
    *,
    proceeds_lamports: int | None,
    error: str | None = None,
) -> bool:
    """Append one idempotent interval result and complete after 15 minutes."""
    allowed = {label for label, _ in OBSERVATION_INTERVALS}
    if interval not in allowed:
        raise ValueError("unsupported observation interval")

    def mutate(document: dict[str, Any]) -> bool:
        migrate_observation_document(document)
        rows = document.get("observations", [])
        target = next((
            row for row in rows
            if isinstance(row, dict)
            and row.get("observation_id") == observation_id
        ), None)
        if not isinstance(target, dict):
            return False
        samples = target.setdefault("samples", [])
        if any(
            isinstance(sample, dict) and sample.get("interval") == interval
            for sample in samples
        ):
            return False
        entry_cost = int(target.get("entry_cost_lamports", 0) or 0)
        return_percent = (
            round((int(proceeds_lamports) / entry_cost - 1) * 100, 4)
            if proceeds_lamports is not None and entry_cost > 0
            else None
        )
        samples.append({
            "interval": interval,
            "proceeds_lamports": proceeds_lamports,
            "return_percent": return_percent,
            "error": error[:500] if error else None,
            "sampled_at": datetime.now(timezone.utc).isoformat(),
        })
        if interval == "1m" and target.get("candidate_v2_eligible") is True:
            target["candidate_v2_early_failure"] = (
                return_percent <= CANDIDATE_V2_EARLY_FAILURE_PERCENT
                if return_percent is not None
                else None
            )
        if interval == "15m":
            target["status"] = "COMPLETE"
            valid_returns = [
                float(sample["return_percent"])
                for sample in samples
                if isinstance(sample, dict)
                and sample.get("return_percent") is not None
            ]
            target["max_return_percent"] = (
                max(valid_returns) if valid_returns else None
            )
            target["min_return_percent"] = (
                min(valid_returns) if valid_returns else None
            )
        expire_observation_backlog(rows)
        document["observations"] = retained_observations(rows)
        document["updated_at"] = datetime.now(timezone.utc).isoformat()
        return True

    recorded, _ = update_json(
        OBSERVATION_PATH, empty_observations(), mutate
    )
    return bool(recorded)


async def observation_loop(interval_seconds: float = 15.0) -> None:
    """Sample executable Jupiter exit values at 1, 5, and 15 minutes."""
    load_dotenv()
    api_key = os.getenv("JUPITER_API_KEY", "").strip()
    if not api_key:
        logger.warning("signal observer disabled: JUPITER_API_KEY is missing")
        return
    from src.executor import JupiterNoRouteError, WSOL_MINT, jupiter_quote

    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            await asyncio.sleep(interval_seconds)
            due = await asyncio.to_thread(due_observation_samples, time.time())
            analysis_dirty = False
            for observation_id, label, mint, amount, _ in due:
                try:
                    quote = await jupiter_quote(
                        session,
                        api_key,
                        mint,
                        WSOL_MINT,
                        amount,
                        fail_fast_bad_request=True,
                    )
                    await asyncio.to_thread(
                        record_sample,
                        observation_id,
                        label,
                        proceeds_lamports=int(quote["outAmount"]),
                    )
                    analysis_dirty = True
                except JupiterNoRouteError as exc:
                    await asyncio.to_thread(
                        record_sample,
                        observation_id,
                        label,
                        proceeds_lamports=None,
                        error=redact_sensitive_text(exc),
                    )
                    analysis_dirty = True
                except Exception as exc:
                    error = redact_sensitive_text(exc)
                    attempts = await asyncio.to_thread(
                        record_sample_attempt,
                        observation_id,
                        label,
                        error=error,
                    )
                    if attempts >= MAX_SAMPLE_ATTEMPTS:
                        await asyncio.to_thread(
                            record_sample,
                            observation_id,
                            label,
                            proceeds_lamports=None,
                            error=error,
                        )
                        analysis_dirty = True
                        logger.warning(
                            "observation sample failed permanently: mint=%s "
                            "interval=%s attempts=%s error=%s",
                            mint,
                            label,
                            attempts,
                            error,
                        )
                    else:
                        logger.warning(
                            "observation sample retry scheduled: mint=%s "
                            "interval=%s attempt=%s/%s error=%s",
                            mint,
                            label,
                            attempts,
                            MAX_SAMPLE_ATTEMPTS,
                            error,
                        )
            if analysis_dirty:
                try:
                    from src.observation_analysis import refresh_observation_analysis

                    await asyncio.to_thread(refresh_observation_analysis)
                except Exception:
                    logger.exception("observation condition analysis refresh failed")
