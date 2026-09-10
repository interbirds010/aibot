"""실행 가능한 모든 관찰 후보의 완결형 shadow 거래 원장."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.state_store import migrate_json, update_json


SHADOW_TRADE_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "shadow_trades.json"
)
SHADOW_TRADE_SCHEMA_VERSION = 2
MAX_COMPLETED_SHADOW_TRADES = 10_000
SHADOW_HORIZONS = ("1m", "3m", "5m", "15m", "30m", "60m")


def empty_shadow_trades() -> dict[str, Any]:
    return {
        "schema_version": SHADOW_TRADE_SCHEMA_VERSION,
        "trades": [],
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "version": 0,
    }


def migrate_shadow_trade_document(document: dict[str, Any]) -> bool:
    schema_version = int(document.get("schema_version", 1) or 1)
    if schema_version > SHADOW_TRADE_SCHEMA_VERSION:
        raise RuntimeError("shadow trade schema is newer than this service")
    trades = document.setdefault("trades", [])
    if not isinstance(trades, list) or any(
        not isinstance(trade, dict) for trade in trades
    ):
        raise RuntimeError("shadow trade ledger is malformed")
    changed = schema_version != SHADOW_TRADE_SCHEMA_VERSION
    for trade in trades:
        legacy_returns = [
            value
            for sample in trade.get("samples", [])
            if isinstance(sample, dict)
            if (value := _finite_number(sample.get("return_percent"))) is not None
        ] if isinstance(trade.get("samples"), list) else []
        stored_mfe = _finite_number(trade.get("max_return_percent"))
        stored_mae = _finite_number(trade.get("min_return_percent"))
        defaults = {
            "signal_type": (
                "MOMENTUM"
                if str(trade.get("route_type", "A")).upper() == "B"
                else "SMART_MONEY"
            ),
            "research_decision": (
                "ENTERED"
                if str(trade.get("paper_experiment_status", "")).upper()
                in {"OPENED", "CLOSED"}
                else "REJECTED"
                if str(trade.get("decision_status", "")).upper()
                in {"REJECTED", "UNAVAILABLE", "FAILED"}
                else "SHADOW"
            ),
            "mfe_percent": (
                max(0.0, stored_mfe) if stored_mfe is not None
                else max([0.0, *legacy_returns]) if legacy_returns else None
            ),
            "mae_percent": (
                min(0.0, stored_mae) if stored_mae is not None
                else min([0.0, *legacy_returns]) if legacy_returns else None
            ),
            "excursion_basis": "scheduled_jupiter_executable_quotes",
            "tracking_profile": "legacy_15m",
        }
        for key, value in defaults.items():
            if key not in trade:
                trade[key] = value
                changed = True
    if changed:
        document["schema_version"] = SHADOW_TRADE_SCHEMA_VERSION
        document["updated_at"] = datetime.now(timezone.utc).isoformat()
    return changed


def ensure_shadow_trades_migrated() -> dict[str, Any]:
    return migrate_json(
        SHADOW_TRADE_PATH,
        empty_shadow_trades(),
        migrate_shadow_trade_document,
    )


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _samples(row: dict[str, Any]) -> list[dict[str, Any]]:
    samples = row.get("samples")
    if not isinstance(samples, list):
        return []
    return [dict(sample) for sample in samples if isinstance(sample, dict)]


def _strategy_results(row: dict[str, Any]) -> dict[str, Any]:
    """동일 진입가를 고정 보유와 route 임계값으로 동시에 평가한다."""
    samples = _samples(row)
    by_interval = {
        str(sample.get("interval")): _finite_number(sample.get("return_percent"))
        for sample in samples
    }
    fixed = {
        f"fixed_hold_{interval}": {
            "status": "CLOSED" if by_interval.get(interval) is not None else "NO_ROUTE",
            "exit_interval": interval,
            "return_percent": by_interval.get(interval),
        }
        for interval in SHADOW_HORIZONS
    }
    route = str(row.get("route_type", "A")).upper()
    stop_percent = -10.0 if route == "B" else -15.0
    take_profit_percent = 30.0 if route == "B" else 50.0
    sampled_exit: dict[str, Any] = {
        "status": "NO_ROUTE",
        "exit_interval": None,
        "exit_reason": "NO_VALID_SAMPLE",
        "return_percent": None,
    }
    last_valid: tuple[str, float] | None = None
    for interval in SHADOW_HORIZONS:
        value = by_interval.get(interval)
        if value is None:
            continue
        last_valid = (interval, value)
        if value <= stop_percent:
            sampled_exit = {
                "status": "CLOSED",
                "exit_interval": interval,
                "exit_reason": "SAMPLED_STOP_LOSS",
                "return_percent": value,
            }
            break
        if value >= take_profit_percent:
            sampled_exit = {
                "status": "CLOSED",
                "exit_interval": interval,
                "exit_reason": "SAMPLED_TAKE_PROFIT",
                "return_percent": value,
            }
            break
    else:
        if last_valid is not None:
            sampled_exit = {
                "status": "CLOSED",
                "exit_interval": last_valid[0],
                "exit_reason": "SAMPLED_TIME_EXIT",
                "return_percent": last_valid[1],
            }
    return {**fixed, "sampled_route_exit": sampled_exit}


def completed_shadow_trade(row: dict[str, Any]) -> dict[str, Any] | None:
    """완료된 실행 가능 관찰을 장기 분석용 shadow 거래로 정규화한다."""
    if (
        str(row.get("status", "")).upper() != "COMPLETE"
        or str(row.get("quote_status", "")).upper() != "EXECUTABLE"
    ):
        return None
    samples = _samples(row)
    required_horizon = (
        "15m" if row.get("tracking_profile") == "legacy_15m" else SHADOW_HORIZONS[-1]
    )
    required_intervals = (
        {"1m", "5m", "15m"}
        if required_horizon == "15m"
        else set(SHADOW_HORIZONS)
    )
    completed_intervals = {str(sample.get("interval")) for sample in samples}
    if not required_intervals <= completed_intervals:
        return None
    observation_id = str(row.get("observation_id", "")).strip()
    if not observation_id:
        return None
    copied_keys = (
        "observation_id", "mint", "route_type", "source_wallet",
        "source_signature", "safety_score", "entry_cost_lamports",
        "token_amount_raw", "token_decimals", "entry_price_impact_pct",
        "exit_price_impact_pct", "expected_slippage_bps",
        "dex_momentum_score", "strategy_version", "strategy_variants",
        "safety_metrics", "momentum_metrics", "decision_status",
        "decision_reasons", "quote_status", "discovery_metadata",
        "signal_detected_at", "analysis_completed_at", "entry_quote_at",
        "entry_latency_ms", "started_at_epoch", "started_at",
        "candidate_v2_eligible", "candidate_v2_filter_reasons",
        "paper_experiment_status", "paper_experiment_position_id",
        "signal_type", "research_decision", "mfe_percent", "mae_percent",
        "excursion_basis",
        "tracking_profile",
    )
    trade = {key: row.get(key) for key in copied_keys}
    trade.update({
        "shadow_trade_id": observation_id,
        "entry_event": "SHADOW_BUY",
        "exit_event": "SHADOW_SELL",
        "trade_status": "CLOSED",
        "samples": samples,
        "strategy_results": _strategy_results(row),
        "closed_at": datetime.now(timezone.utc).isoformat(),
    })
    return trade


def record_completed_shadow_trade(row: dict[str, Any]) -> bool:
    trade = completed_shadow_trade(row)
    if trade is None:
        return False

    def mutate(document: dict[str, Any]) -> bool:
        migrate_shadow_trade_document(document)
        trades = document.setdefault("trades", [])
        if any(
            item.get("shadow_trade_id") == trade["shadow_trade_id"]
            for item in trades
            if isinstance(item, dict)
        ):
            return False
        trades.append(trade)
        document["trades"] = trades[-MAX_COMPLETED_SHADOW_TRADES:]
        document["updated_at"] = datetime.now(timezone.utc).isoformat()
        return True

    recorded, _ = update_json(
        SHADOW_TRADE_PATH,
        empty_shadow_trades(),
        mutate,
    )
    return bool(recorded)


def backfill_completed_shadow_trades(rows: list[Any]) -> int:
    """현재 관찰 원장의 완료 표본을 한 번의 잠금으로 shadow 원장에 백필한다."""
    candidates = [
        trade
        for row in rows
        if isinstance(row, dict)
        if (trade := completed_shadow_trade(row)) is not None
    ]
    if not candidates:
        return 0
    current = ensure_shadow_trades_migrated()
    current_ids = {
        str(item.get("shadow_trade_id", ""))
        for item in current.get("trades", [])
        if isinstance(item, dict)
    }
    candidates = [
        trade for trade in candidates
        if trade["shadow_trade_id"] not in current_ids
    ]
    if not candidates:
        return 0

    # 잠금 안의 최종 중복 검증 전에 큰 사전 조회 문서를 해제한다.
    del current, current_ids

    def mutate(document: dict[str, Any]) -> int:
        migrate_shadow_trade_document(document)
        trades = document.setdefault("trades", [])
        existing = {
            str(item.get("shadow_trade_id", ""))
            for item in trades
            if isinstance(item, dict)
        }
        added = [
            trade for trade in candidates
            if trade["shadow_trade_id"] not in existing
        ]
        if added:
            trades.extend(added)
            document["trades"] = trades[-MAX_COMPLETED_SHADOW_TRADES:]
            document["updated_at"] = datetime.now(timezone.utc).isoformat()
        return len(added)

    added, _ = update_json(
        SHADOW_TRADE_PATH,
        empty_shadow_trades(),
        mutate,
    )
    return int(added)
