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
from typing import Any, Awaitable, Callable

import aiohttp
from dotenv import load_dotenv

from src.logging_utils import redact_sensitive_text
from src.runtime_memory import current_rss_bytes, record_memory_phase
from src.state_store import migrate_json, read_json, set_global_metrics, update_json

logger = logging.getLogger("signal-observer")

OBSERVATION_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "signal_observations.json"
)
OBSERVATION_INTERVALS = (
    ("1m", 60),
    ("3m", 180),
    ("5m", 300),
    ("15m", 900),
    ("30m", 1_800),
    ("60m", 3_600),
)
MAX_OBSERVATIONS = 1_000
MAX_ACTIVE_OBSERVATIONS = 200
OBSERVATION_SAMPLE_BATCH_SIZE = 20
OBSERVATION_SAMPLE_CONCURRENCY = 4
MAX_SAMPLE_ATTEMPTS = 3
MAX_HORIZON_SAMPLE_LAG_SECONDS = 60.0
OBSERVER_HEALTH_INTERVAL_SECONDS = 60.0
OBSERVER_RESTART_DELAY_SECONDS = 30.0
DISCOVERY_RECONCILIATION_GRACE_SECONDS = 3_600.0
DISCOVERY_PROCESSING_INTERRUPTED = "DISCOVERY_PROCESSING_INTERRUPTED"
OBSERVATION_SCHEMA_VERSION = 5
TERMINAL_OBSERVATION_STATUSES = {"COMPLETE", "EXPIRED_UNSAMPLED"}
CANDIDATE_V2_MIN_SCORE = 90.0
CANDIDATE_V2_MAX_SCORE = 100.0
CANDIDATE_V2_MINT_COOLDOWN_SECONDS = 86_400.0
CANDIDATE_V2_EARLY_FAILURE_PERCENT = -10.0


def required_observation_intervals(row: dict[str, Any]) -> set[str]:
    """추적 profile별 완료에 필요한 horizon 집합을 반환한다."""
    if row.get("tracking_profile") == "legacy_15m":
        return {"1m", "5m", "15m"}
    return {label for label, _ in OBSERVATION_INTERVALS}


@dataclass(frozen=True, slots=True)
class ObservationDecision:
    created: bool
    observation_id: str
    candidate_v2_eligible: bool
    strategy_variants: tuple[str, ...]


def signal_type_for_route(route_type: Any) -> str:
    """현재 신호 생성 경로를 안정적인 연구 분류명으로 변환한다."""
    route = str(route_type).upper()
    if route == "A":
        return "SMART_MONEY"
    if route == "B":
        return "MOMENTUM"
    return "UNKNOWN"


def canonical_research_decision(row: dict[str, Any]) -> str:
    """기존 세부 상태를 배타적인 연구 판정으로 정규화한다."""
    paper_status = str(row.get("paper_experiment_status", "")).upper()
    if paper_status in {"OPENED", "CLOSED"}:
        return "ENTERED"
    if str(row.get("decision_status", "")).upper() in {
        "REJECTED", "UNAVAILABLE", "FAILED",
    }:
        return "REJECTED"
    return "SHADOW"


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
    legacy_schema = schema_version < OBSERVATION_SCHEMA_VERSION
    if schema_version > OBSERVATION_SCHEMA_VERSION:
        raise RuntimeError("signal observation schema is newer than this service")
    rows = document.setdefault("observations", [])
    if not isinstance(rows, list):
        raise RuntimeError("signal observation ledger is malformed")
    changed = schema_version != OBSERVATION_SCHEMA_VERSION
    for row in rows:
        if not isinstance(row, dict):
            raise RuntimeError("signal observation row is malformed")
        legacy_mfe = _finite_or_none(row.get("max_return_percent"))
        legacy_mae = _finite_or_none(row.get("min_return_percent"))
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
            "signal_type": signal_type_for_route(row.get("route_type")),
            "research_decision": canonical_research_decision(row),
            "mfe_percent": max(0.0, legacy_mfe) if legacy_mfe is not None else None,
            "mae_percent": min(0.0, legacy_mae) if legacy_mae is not None else None,
            "excursion_basis": "scheduled_jupiter_executable_quotes",
            "tracking_profile": (
                "legacy_15m"
                if legacy_schema and str(row.get("status", "")).upper() == "COMPLETE"
                else "research_v1_60m"
            ),
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
        completed_intervals = {
            str(sample.get("interval"))
            for sample in row["samples"]
            if isinstance(sample, dict)
        }
        if (
            str(row.get("status", "")).upper() == "PENDING"
            and required_observation_intervals(row) <= completed_intervals
        ):
            row["status"] = "COMPLETE"
            sampled_at_values = [
                str(sample.get("sampled_at"))
                for sample in row["samples"]
                if isinstance(sample, dict) and sample.get("sampled_at")
            ]
            if not row.get("completed_at"):
                row["completed_at"] = (
                    max(sampled_at_values)
                    if sampled_at_values
                    else datetime.now(timezone.utc).isoformat()
                )
            changed = True
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


def reconcile_interrupted_discoveries(
    *,
    now_epoch: float | None = None,
    grace_seconds: float = DISCOVERY_RECONCILIATION_GRACE_SECONDS,
) -> int:
    """충분히 오래된 미분석 discovery만 terminal 운영 실패로 마감한다."""
    now = time.time() if now_epoch is None else float(now_epoch)
    grace = max(60.0, float(grace_seconds))
    reconciled = 0

    def migrate(document: dict[str, Any]) -> bool:
        nonlocal reconciled
        changed = migrate_observation_document(document)
        rows = document.get("observations", [])
        for row in rows:
            if not isinstance(row, dict):
                continue
            started_at = _finite_or_none(row.get("started_at_epoch"))
            if (
                str(row.get("status") or "").upper() != "DISCOVERED"
                or str(row.get("decision_status") or "").upper() != "DISCOVERED"
                or str(row.get("quote_status") or "").upper() != "NOT_REQUESTED"
                or row.get("tracking_profile") != "research_v1_60m"
                or row.get("analysis_completed_at") is not None
                or row.get("entry_quote_at") is not None
                or row.get("decision_reasons") != []
                or row.get("samples") != []
                or row.get("sample_attempts") not in ({}, None)
                or row.get("paper_experiment_position_id") is not None
                or row.get("candidate_v2_position_id") is not None
                or str(row.get("paper_experiment_status") or "NOT_EVALUATED").upper()
                != "NOT_EVALUATED"
                or started_at is None
                or now - started_at < grace
            ):
                continue
            completed_at = datetime.fromtimestamp(now, timezone.utc).isoformat()
            row["status"] = "COMPLETE"
            row["decision_status"] = "INTERRUPTED"
            row["decision_reasons"] = [DISCOVERY_PROCESSING_INTERRUPTED]
            row["quote_status"] = "PROCESSING_FAILED"
            row["research_decision"] = "SHADOW"
            row["paper_experiment_status"] = "NOT_ELIGIBLE"
            row["candidate_v2_paper_status"] = "NOT_ELIGIBLE"
            row["reconciled_at"] = completed_at
            row["reconciliation_reason"] = DISCOVERY_PROCESSING_INTERRUPTED
            row["completed_at"] = completed_at
            reconciled += 1
            changed = True
        if reconciled:
            document["updated_at"] = datetime.now(timezone.utc).isoformat()
        return changed

    migrate_json(OBSERVATION_PATH, empty_observations(), migrate)
    return reconciled


def _sampled_at_epoch(sample: dict[str, Any]) -> float | None:
    epoch = _finite_or_none(sample.get("sampled_at_epoch"))
    if epoch is not None and epoch >= 0:
        return float(epoch)
    raw = sample.get("sampled_at")
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        parsed = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    value = parsed.timestamp()
    return value if math.isfinite(value) and value >= 0 else None


def observation_runtime_metrics(
    document: dict[str, Any] | None = None,
    *,
    now_epoch: float | None = None,
) -> dict[str, int | float | None]:
    """원장을 변경하지 않고 observer 운영 지표를 계산한다."""
    source = (
        document
        if document is not None
        else read_json(OBSERVATION_PATH, empty_observations())
    )
    rows = source.get("observations")
    if not isinstance(rows, list):
        raise RuntimeError("signal observation ledger is malformed")
    pending = 0
    missed_count = 0
    last_success: float | None = None
    last_missed: float | None = None
    for row in rows:
        if not isinstance(row, dict):
            raise RuntimeError("signal observation row is malformed")
        if str(row.get("status") or "").upper() not in TERMINAL_OBSERVATION_STATUSES:
            pending += 1
        samples = row.get("samples")
        if not isinstance(samples, list):
            raise RuntimeError("signal observation samples are malformed")
        for sample in samples:
            if not isinstance(sample, dict):
                continue
            sampled_at = _sampled_at_epoch(sample)
            outcome = _finite_or_none(sample.get("return_percent"))
            if outcome is not None and sampled_at is not None:
                last_success = max(last_success or sampled_at, sampled_at)
            if "HORIZON_MISSED" in str(sample.get("error") or "").upper():
                missed_count += 1
                if sampled_at is not None:
                    last_missed = max(last_missed or sampled_at, sampled_at)
    now = time.time() if now_epoch is None else float(now_epoch)
    due = _due_sample_candidates(rows, now)
    return {
        "pending_research_observations": pending,
        "last_successful_horizon_sample_at": last_success,
        "horizon_missed_count": missed_count,
        "last_horizon_missed_at": last_missed,
        "observer_due_backlog_depth": len(due),
        "observer_oldest_due_lag_seconds": (
            round(max(0.0, now - due[0][4]), 4) if due else None
        ),
    }


async def _publish_observer_metrics(values: dict[str, Any]) -> None:
    """관측 지표 저장 실패가 monitor 생명주기에 영향을 주지 않게 한다."""
    try:
        await asyncio.to_thread(set_global_metrics, values)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("observer health metric update failed")


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


def archive_and_retain_observations(rows: list[Any]) -> list[Any]:
    """terminal row를 먼저 보존하고 operational ledger를 제한한다."""
    from src.research_archive import archive_terminal_rows

    archive_path = OBSERVATION_PATH.parent / "research_archive"
    metrics_path = OBSERVATION_PATH.parent / "research_archive_metrics.json"
    archive_terminal_rows(
        (
            row for row in rows
            if isinstance(row, dict)
            and not (
                row.get("archive_schema_version") == 1
                and row.get("archived_at")
            )
        ),
        archive_path=archive_path,
        metrics_path=metrics_path,
    )
    unarchived_ids = {
        id(row) for row in rows
        if isinstance(row, dict)
        and str(row.get("status") or "").upper() in TERMINAL_OBSERVATION_STATUSES
        and not (
            row.get("archive_schema_version") == 1
            and row.get("archived_at")
        )
    }
    retained = retained_observations(rows)
    kept_ids = {id(row) for row in retained}
    if unarchived_ids <= kept_ids:
        return retained
    # Archive 장애 중에는 손실보다 일시적인 operational cap 초과를 택한다.
    return [
        row for row in rows
        if id(row) in kept_ids or id(row) in unarchived_ids
    ]


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
            "signal_type": signal_type_for_route(route_type),
            "research_decision": "SHADOW",
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
            "mfe_percent": None,
            "mae_percent": None,
            "excursion_basis": "scheduled_jupiter_executable_quotes",
            "tracking_profile": "research_v1_60m",
            "status": "DISCOVERED",
        })
        expire_observation_backlog(rows)
        document["observations"] = archive_and_retain_observations(rows)
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


def _finite_or_none(value: Any, *, integer: bool = False) -> int | float | None:
    if isinstance(value, bool) or value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number):
        return None
    return int(number) if integer else number


def _normalized_momentum_metrics(
    metrics: dict[str, int | float] | None,
) -> dict[str, int | float | None] | None:
    if not isinstance(metrics, dict):
        return None
    return {
        "volume_m5_usd": _finite_or_none(metrics.get("volume_m5_usd")),
        "buys_m5": _finite_or_none(metrics.get("buys_m5"), integer=True),
        "sells_m5": _finite_or_none(metrics.get("sells_m5"), integer=True),
        "net_buys_m5": _finite_or_none(metrics.get("net_buys_m5"), integer=True),
        "buy_sell_ratio_m5": _finite_or_none(metrics.get("buy_sell_ratio_m5")),
        "liquidity_usd": _finite_or_none(metrics.get("liquidity_usd")),
        "pair_age_seconds": _finite_or_none(metrics.get("pair_age_seconds")),
        "unknown_whale_count": _finite_or_none(
            metrics.get("unknown_whale_count"), integer=True
        ),
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
        target["research_decision"] = "REJECTED"
        target["decision_reasons"] = [str(reason)[:200] for reason in decision_reasons[:20]]
        target["quote_status"] = str(quote_status).upper()[:80]
        target["paper_experiment_status"] = "NOT_ELIGIBLE"
        target["candidate_v2_paper_status"] = "NOT_ELIGIBLE"
        target["safety_score"] = safety_score
        target["safety_metrics"] = normalized_safety_metrics(safety_metrics)
        target["analysis_completed_at"] = analysis_completed_at
        target["status"] = "COMPLETE"
        target["completed_at"] = datetime.now(timezone.utc).isoformat()
        document["observations"] = archive_and_retain_observations(
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
            "signal_type": signal_type_for_route(route_type),
            "research_decision": (
                "SHADOW" if normalized_decision == "APPROVED" else "REJECTED"
            ),
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
            "mfe_percent": None,
            "mae_percent": None,
            "excursion_basis": "scheduled_jupiter_executable_quotes",
            "tracking_profile": "research_v1_60m",
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
        document["observations"] = archive_and_retain_observations(rows)
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
        target["research_decision"] = (
            "ENTERED" if normalized in {"OPENED", "CLOSED"}
            else canonical_research_decision(target)
        )
        document["observations"] = archive_and_retain_observations(
            document.get("observations", [])
        )
        document["updated_at"] = datetime.now(timezone.utc).isoformat()
        return True

    changed, _ = update_json(
        OBSERVATION_PATH, empty_observations(), mutate
    )
    return bool(changed)


def _due_sample_candidates(
    rows: list[Any], now: float,
) -> list[tuple[str, str, str, int, float]]:
    """모든 due horizon을 deadline 우선순위로 반환한다."""
    due: list[tuple[str, str, str, int, float]] = []
    for row in rows:
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
                    started_at + delay,
                ))
    interval_order = {
        label: index for index, (label, _) in enumerate(OBSERVATION_INTERVALS)
    }
    due.sort(key=lambda item: (item[4], interval_order[item[1]], item[0]))
    return due


def due_observation_samples(now: float) -> list[tuple[str, str, str, int, float]]:
    """가장 오래된 deadline부터 bounded batch를 반환한다."""
    document = ensure_observations_migrated()
    due = _due_sample_candidates(document.get("observations", []), now)
    return due[:OBSERVATION_SAMPLE_BATCH_SIZE]


def horizon_sample_is_missed(now_epoch: float, target_at_epoch: float) -> bool:
    """60초 경계는 포함하고 그 이후 표본만 missed로 판정한다."""
    return now_epoch - target_at_epoch > MAX_HORIZON_SAMPLE_LAG_SECONDS


async def run_due_sample_batch(
    candidates: list[tuple[str, str, str, int, float]],
    worker: Callable[
        [tuple[str, str, str, int, float]], Awaitable[bool]
    ],
    *,
    concurrency: int = OBSERVATION_SAMPLE_CONCURRENCY,
) -> list[bool]:
    """느린 quote 하나가 전체 batch를 막지 않도록 병행 수를 제한한다."""
    semaphore = asyncio.Semaphore(max(1, int(concurrency)))

    async def limited(
        candidate: tuple[str, str, str, int, float],
    ) -> bool:
        async with semaphore:
            return await worker(candidate)

    return list(await asyncio.gather(*(limited(item) for item in candidates)))


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
    sampled_at_epoch: float | None = None,
    quote_latency_ms: float | None = None,
) -> bool:
    """한 horizon 결과를 멱등 저장하고 필수 horizon이 모이면 완료한다."""
    allowed = {label for label, _ in OBSERVATION_INTERVALS}
    if interval not in allowed:
        raise ValueError("unsupported observation interval")

    def mutate(document: dict[str, Any]) -> dict[str, Any] | None:
        migrate_observation_document(document)
        rows = document.get("observations", [])
        target = next((
            row for row in rows
            if isinstance(row, dict)
            and row.get("observation_id") == observation_id
        ), None)
        if not isinstance(target, dict):
            return None
        samples = target.setdefault("samples", [])
        if any(
            isinstance(sample, dict) and sample.get("interval") == interval
            for sample in samples
        ):
            return None
        entry_cost = int(target.get("entry_cost_lamports", 0) or 0)
        return_percent = (
            round((int(proceeds_lamports) / entry_cost - 1) * 100, 4)
            if proceeds_lamports is not None and entry_cost > 0
            else None
        )
        sample_epoch = (
            time.time() if sampled_at_epoch is None else float(sampled_at_epoch)
        )
        latency_ms = _finite_or_none(quote_latency_ms)
        if latency_ms is not None and latency_ms < 0:
            latency_ms = None
        delay_seconds = dict(OBSERVATION_INTERVALS)[interval]
        target_at_epoch = float(target.get("started_at_epoch", 0) or 0) + delay_seconds
        samples.append({
            "interval": interval,
            "proceeds_lamports": proceeds_lamports,
            "return_percent": return_percent,
            "error": error[:500] if error else None,
            "sampled_at": datetime.fromtimestamp(
                sample_epoch, timezone.utc
            ).isoformat(),
            "target_at_epoch": target_at_epoch,
            "sampled_at_epoch": sample_epoch,
            "sample_lag_seconds": round(
                max(0.0, sample_epoch - target_at_epoch), 4
            ),
            "quote_latency_ms": (
                round(latency_ms, 4) if latency_ms is not None else None
            ),
        })
        if interval == "1m" and target.get("candidate_v2_eligible") is True:
            target["candidate_v2_early_failure"] = (
                return_percent <= CANDIDATE_V2_EARLY_FAILURE_PERCENT
                if return_percent is not None
                else None
            )
        observed_returns = [
            float(sample["return_percent"])
            for sample in samples
            if isinstance(sample, dict)
            and sample.get("return_percent") is not None
        ]
        excursion_returns = [0.0, *observed_returns]
        target["mfe_percent"] = (
            max(excursion_returns) if observed_returns else None
        )
        target["mae_percent"] = (
            min(excursion_returns) if observed_returns else None
        )
        # 기존 필드도 호환성을 위해 같은 표본 극값으로 유지한다.
        target["max_return_percent"] = target["mfe_percent"]
        target["min_return_percent"] = target["mae_percent"]
        completed_intervals = {
            str(sample.get("interval"))
            for sample in samples
            if isinstance(sample, dict)
        }
        required_intervals = required_observation_intervals(target)
        if required_intervals <= completed_intervals:
            target["status"] = "COMPLETE"
            if not target.get("completed_at"):
                target["completed_at"] = datetime.now(timezone.utc).isoformat()
        expire_observation_backlog(rows)
        document["observations"] = archive_and_retain_observations(rows)
        document["updated_at"] = datetime.now(timezone.utc).isoformat()
        return dict(target)

    snapshot, _ = update_json(
        OBSERVATION_PATH, empty_observations(), mutate
    )
    if (
        snapshot is not None
        and str(snapshot.get("status", "")).upper() == "COMPLETE"
    ):
        from src.shadow_trade_ledger import record_completed_shadow_trade

        record_completed_shadow_trade(snapshot)
    return snapshot is not None


async def observation_loop(interval_seconds: float = 15.0) -> None:
    """기존 bounded Jupiter 경로로 1/3/5/15/30/60분 값을 표본화한다."""
    started_at = time.time()
    await _publish_observer_metrics({
        "observer_state": "STARTING",
        "observer_started_at": started_at,
        "observer_heartbeat_at": started_at,
    })
    observation_document = await asyncio.to_thread(ensure_observations_migrated)
    reconciled = await asyncio.to_thread(reconcile_interrupted_discoveries)
    if reconciled:
        observation_document = await asyncio.to_thread(ensure_observations_migrated)
        logger.warning(
            "interrupted discovery observations reconciled: count=%s reason=%s",
            reconciled,
            DISCOVERY_PROCESSING_INTERRUPTED,
        )
    await _publish_observer_metrics({
        "discovery_reconciliation_last_count": reconciled,
        "discovery_reconciliation_last_at": time.time(),
        "discovery_reconciliation_reason": DISCOVERY_PROCESSING_INTERRUPTED,
    })
    from src.shadow_trade_ledger import (
        backfill_completed_shadow_trades,
        ensure_shadow_trades_migrated,
    )
    from src.research_archive import (
        archive_integrity_metrics,
        backfill_research_archive,
    )

    await asyncio.to_thread(ensure_shadow_trades_migrated)
    archive_backfill = await asyncio.to_thread(
        backfill_research_archive,
        observation_document.get("observations", []),
        archive_path=OBSERVATION_PATH.parent / "research_archive",
        metrics_path=OBSERVATION_PATH.parent / "research_archive_metrics.json",
    )
    logger.info("research archive reconciliation: %s", archive_backfill)
    archive_metrics = await asyncio.to_thread(
        archive_integrity_metrics,
        operational_rows=observation_document.get("observations", []),
        archive_path=OBSERVATION_PATH.parent / "research_archive",
        metrics_path=OBSERVATION_PATH.parent / "research_archive_metrics.json",
    )
    backfilled = await asyncio.to_thread(
        backfill_completed_shadow_trades,
        observation_document.get("observations", []),
    )
    if backfilled:
        logger.info("completed shadow trades backfilled: count=%s", backfilled)

    load_dotenv()
    api_key = os.getenv("JUPITER_API_KEY", "").strip()
    if not api_key:
        now = time.time()
        await _publish_observer_metrics({
            "observer_state": "DISABLED_MISSING_API_KEY",
            "observer_heartbeat_at": now,
            "observer_state_changed_at": now,
            **{f"research_{key}": value for key, value in archive_metrics.items()},
        })
        logger.warning("signal observer disabled: JUPITER_API_KEY is missing")
        return
    from src.executor import JupiterNoRouteError, WSOL_MINT, jupiter_quote

    now = time.time()
    await _publish_observer_metrics({
        "observer_state": "RUNNING",
        "observer_heartbeat_at": now,
        "observer_state_changed_at": now,
        **{f"research_{key}": value for key, value in archive_metrics.items()},
        **observation_runtime_metrics(observation_document),
    })
    last_health_refresh = time.monotonic()

    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async def sample_due(
            candidate: tuple[str, str, str, int, float],
        ) -> bool:
            observation_id, label, mint, amount, target_at_epoch = candidate
            attempted_at = time.time()
            if horizon_sample_is_missed(attempted_at, target_at_epoch):
                await asyncio.to_thread(
                    record_sample,
                    observation_id,
                    label,
                    proceeds_lamports=None,
                    error="HORIZON_MISSED",
                    sampled_at_epoch=attempted_at,
                )
                logger.warning(
                    "observation horizon missed: mint=%s interval=%s lag=%.3f",
                    mint,
                    label,
                    max(0.0, attempted_at - target_at_epoch),
                )
                return True

            quote_started = time.monotonic()
            try:
                quote = await jupiter_quote(
                    session,
                    api_key,
                    mint,
                    WSOL_MINT,
                    amount,
                    fail_fast_bad_request=True,
                )
                sampled_at = time.time()
                latency_ms = (time.monotonic() - quote_started) * 1_000
                if horizon_sample_is_missed(sampled_at, target_at_epoch):
                    await asyncio.to_thread(
                        record_sample,
                        observation_id,
                        label,
                        proceeds_lamports=None,
                        error="HORIZON_MISSED",
                        sampled_at_epoch=sampled_at,
                        quote_latency_ms=latency_ms,
                    )
                    logger.warning(
                        "observation quote exceeded horizon deadline: "
                        "mint=%s interval=%s lag=%.3f quote_latency_ms=%.1f",
                        mint,
                        label,
                        max(0.0, sampled_at - target_at_epoch),
                        latency_ms,
                    )
                    return True
                await asyncio.to_thread(
                    record_sample,
                    observation_id,
                    label,
                    proceeds_lamports=int(quote["outAmount"]),
                    sampled_at_epoch=sampled_at,
                    quote_latency_ms=latency_ms,
                )
                return True
            except JupiterNoRouteError as exc:
                sampled_at = time.time()
                latency_ms = (time.monotonic() - quote_started) * 1_000
                error = (
                    "HORIZON_MISSED"
                    if horizon_sample_is_missed(sampled_at, target_at_epoch)
                    else redact_sensitive_text(exc)
                )
                await asyncio.to_thread(
                    record_sample,
                    observation_id,
                    label,
                    proceeds_lamports=None,
                    error=error,
                    sampled_at_epoch=sampled_at,
                    quote_latency_ms=latency_ms,
                )
                return True
            except Exception as exc:
                sampled_at = time.time()
                latency_ms = (time.monotonic() - quote_started) * 1_000
                error = redact_sensitive_text(exc)
                attempts = await asyncio.to_thread(
                    record_sample_attempt,
                    observation_id,
                    label,
                    error=error,
                )
                if attempts >= MAX_SAMPLE_ATTEMPTS:
                    final_error = (
                        "HORIZON_MISSED"
                        if horizon_sample_is_missed(
                            sampled_at, target_at_epoch
                        )
                        else error
                    )
                    await asyncio.to_thread(
                        record_sample,
                        observation_id,
                        label,
                        proceeds_lamports=None,
                        error=final_error,
                        sampled_at_epoch=sampled_at,
                        quote_latency_ms=latency_ms,
                    )
                    logger.warning(
                        "observation sample failed permanently: mint=%s "
                        "interval=%s attempts=%s error=%s",
                        mint,
                        label,
                        attempts,
                        final_error,
                    )
                    return True
                logger.warning(
                    "observation sample retry scheduled: mint=%s "
                    "interval=%s attempt=%s/%s error=%s",
                    mint,
                    label,
                    attempts,
                    MAX_SAMPLE_ATTEMPTS,
                    error,
                )
                return False

        while True:
            tick_started = time.monotonic()
            due_scan_memory_start = current_rss_bytes()
            try:
                due = await asyncio.to_thread(
                    due_observation_samples, time.time()
                )
            finally:
                record_memory_phase(
                    "observation_due_scan", due_scan_memory_start
                )
            results = await run_due_sample_batch(due, sample_due)
            analysis_dirty = any(results)
            if (
                analysis_dirty
                or time.monotonic() - last_health_refresh
                >= OBSERVER_HEALTH_INTERVAL_SECONDS
            ):
                now = time.time()
                await _publish_observer_metrics({
                    "observer_state": "RUNNING",
                    "observer_heartbeat_at": now,
                    **observation_runtime_metrics(),
                })
                last_health_refresh = time.monotonic()
            await asyncio.sleep(max(
                0.0,
                interval_seconds - (time.monotonic() - tick_started),
            ))


async def observation_supervisor(
    restart_delay_seconds: float = OBSERVER_RESTART_DELAY_SECONDS,
) -> None:
    """observer 장애를 monitor의 거래·감시 루프와 격리한다."""
    while True:
        try:
            await observation_loop()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            now = time.time()
            await _publish_observer_metrics({
                "observer_state": "RESTARTING",
                "observer_heartbeat_at": now,
                "observer_state_changed_at": now,
                "observer_last_error_at": now,
                "observer_last_error_type": type(exc).__name__,
            })
            logger.exception(
                "signal observer failed; restarting in %.1f seconds",
                restart_delay_seconds,
            )
        await asyncio.sleep(max(0.0, restart_delay_seconds))
