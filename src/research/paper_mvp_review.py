"""Paper MVP event-sequence cohort를 읽기 전용으로 검토한다."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from src.state_store import exclusive_file_lock, read_json


PAPER_MVP_START_AT = "2026-09-16T09:48:28.973888+00:00"
PAPER_MVP_FIRST_REVIEW_AT = "2026-09-23T09:48:28.973888+00:00"
PAPER_MVP_BUY_EVENT_SEQ_CUTOFF = 9_474
PAPER_MVP_BASELINE_REALIZED_PNL_LAMPORTS = -57_168_670
SUPPORTED_LEDGER_SCHEMA_VERSION = 2
LEDGER_PATH = Path(__file__).resolve().parents[2] / "data" / "paper_trades.json"


def _empty_ledger() -> dict[str, Any]:
    return {
        "schema_version": SUPPORTED_LEDGER_SCHEMA_VERSION,
        "version": 0,
        "next_event_seq": 1,
        "positions": {},
        "events": [],
    }


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _integer(value: Any, *, name: str, minimum: int | None = None) -> int:
    if isinstance(value, bool):
        raise RuntimeError(f"{name} is malformed")
    if isinstance(value, float) and not value.is_integer():
        raise RuntimeError(f"{name} is malformed")
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RuntimeError(f"{name} is malformed") from exc
    if minimum is not None and result < minimum:
        raise RuntimeError(f"{name} is malformed")
    return result


def _timestamp(value: Any, *, name: str) -> float:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{name} is missing")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise RuntimeError(f"{name} is malformed") from exc
    if parsed.tzinfo is None:
        raise RuntimeError(f"{name} must include a timezone")
    result = parsed.timestamp()
    if not math.isfinite(result):
        raise RuntimeError(f"{name} is malformed")
    return result


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def _rounded(value: float | None, digits: int = 4) -> float | None:
    return round(value, digits) if value is not None and math.isfinite(value) else None


def _mean(values: list[float]) -> float | None:
    return math.fsum(values) / len(values) if values else None


def _quantile(values: Iterable[float], fraction: float) -> float | None:
    ordered = sorted(values)
    if not ordered:
        return None
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (
        position - lower
    )


def _numeric_summary(values: Iterable[Any]) -> dict[str, Any]:
    finite = [number for value in values if (number := _number(value)) is not None]
    return {
        "count": len(finite),
        "mean": _rounded(_mean(finite)),
        "median": _rounded(_quantile(finite, 0.5)),
        "p95": _rounded(_quantile(finite, 0.95)),
        "minimum": _rounded(min(finite) if finite else None),
        "maximum": _rounded(max(finite) if finite else None),
    }


def _route_family(route: Any) -> str:
    return {"A": "SMART_MONEY", "B": "MOMENTUM"}.get(
        str(route or "").upper(), "UNKNOWN"
    )


def _performance(records: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(records, key=lambda item: (item["closed_at_epoch"], item["position_id"]))
    pnls = [int(item["realized_pnl_lamports"]) for item in ordered]
    rois = [float(item["realized_return_percent"]) for item in ordered]
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    cumulative = 0
    peak = 0
    maximum_drawdown = 0
    for value in pnls:
        cumulative += value
        peak = max(peak, cumulative)
        maximum_drawdown = max(maximum_drawdown, peak - cumulative)
    winner_removed = list(pnls)
    if winner_removed:
        winner_removed.remove(max(winner_removed))
    return {
        "completed_position_count": len(records),
        "win_count": len(wins),
        "loss_count": len(losses),
        "zero_count": sum(value == 0 for value in pnls),
        "win_rate_percent": _rounded(len(wins) * 100 / len(pnls) if pnls else None),
        "realized_pnl_lamports": sum(pnls),
        "average_win_lamports": _rounded(_mean([float(value) for value in wins])),
        "average_loss_lamports": _rounded(_mean([float(value) for value in losses])),
        "expectancy_lamports": _rounded(_mean([float(value) for value in pnls])),
        "profit_factor": _rounded(gross_profit / gross_loss if gross_loss else None),
        "profit_factor_above_one": (
            gross_profit > gross_loss if gross_loss else bool(wins and not losses)
        ),
        "median_return_percent": _rounded(statistics.median(rois) if rois else None),
        "max_realized_drawdown_lamports": maximum_drawdown if pnls else None,
        "max_loss_lamports": min(losses) if losses else None,
        "largest_winner_lamports": max(wins) if wins else None,
        "largest_winner_contribution_percent": _rounded(
            max(wins) * 100 / gross_profit if gross_profit else None
        ),
        "largest_winner_removed_expectancy_lamports": _rounded(
            _mean([float(value) for value in winner_removed])
        ),
        "entry_cost_lamports": sum(int(item["entry_cost_lamports"]) for item in records),
        "holding_seconds": _numeric_summary(item["holding_seconds"] for item in records),
    }


def _period_performance(records: list[dict[str, Any]], period: str) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        instant = datetime.fromtimestamp(record["closed_at_epoch"], timezone.utc)
        key = (
            instant.date().isoformat()
            if period == "day"
            else f"{instant.isocalendar().year}-W{instant.isocalendar().week:02d}"
        )
        groups[key].append(record)
    rows = []
    for key, values in sorted(groups.items()):
        metrics = _performance(values)
        rows.append({
            "period": key,
            "completed_position_count": metrics["completed_position_count"],
            "realized_pnl_lamports": metrics["realized_pnl_lamports"],
            "expectancy_lamports": metrics["expectancy_lamports"],
            "profit_factor": metrics["profit_factor"],
            "median_return_percent": metrics["median_return_percent"],
            "win_rate_percent": metrics["win_rate_percent"],
        })
    return rows


def _cohort_record(
    buy: dict[str, Any],
    sells: list[dict[str, Any]],
) -> dict[str, Any]:
    position_id = str(buy["position_id"])
    entry_cost = _integer(buy.get("cost_lamports"), name="BUY cost_lamports", minimum=1)
    entry_amount = _integer(
        buy.get("token_amount_raw"), name="BUY token_amount_raw", minimum=1
    )
    pnl = sum(
        _integer(item.get("realized_pnl_lamports"), name="SELL realized_pnl_lamports")
        for item in sells
    )
    closed_at = max(
        (_timestamp(item.get("at"), name="SELL at") for item in sells),
        default=None,
    )
    if closed_at is None:
        raise RuntimeError(f"completed position has no SELL: {position_id}")
    opened_at = _timestamp(buy.get("at"), name="BUY at")
    return {
        "position_id": position_id,
        "mint": str(buy.get("mint") or ""),
        "route_type": str(buy.get("route_type") or "").upper(),
        "family": _route_family(buy.get("route_type")),
        "entry_cost_lamports": entry_cost,
        "entry_token_amount_raw": entry_amount,
        "realized_pnl_lamports": pnl,
        "realized_return_percent": pnl / entry_cost * 100,
        "opened_at_epoch": opened_at,
        "closed_at_epoch": closed_at,
        "holding_seconds": max(0.0, closed_at - opened_at),
        "sell_event_count": len(sells),
    }


def _evidence_direction(metrics: dict[str, Any]) -> str:
    expectancy = metrics.get("expectancy_lamports")
    median = metrics.get("median_return_percent")
    profit_factor_above_one = metrics.get("profit_factor_above_one") is True
    removed = metrics.get("largest_winner_removed_expectancy_lamports")
    if (
        expectancy is not None
        and expectancy > 0
        and median is not None
        and median >= 0
        and profit_factor_above_one
        and removed is not None
        and removed > 0
    ):
        return "POSITIVE_DIRECTION"
    if (
        expectancy is not None
        and expectancy < 0
        and median is not None
        and median < 0
        and not profit_factor_above_one
        and (removed is None or removed <= 0)
    ):
        return "NEGATIVE_DIRECTION"
    return "MIXED_OR_THIN"


def build_paper_mvp_review(
    ledger: dict[str, Any],
    *,
    generated_at: str,
    cutoff_event_seq: int = PAPER_MVP_BUY_EVENT_SEQ_CUTOFF,
    review_at: str = PAPER_MVP_FIRST_REVIEW_AT,
    minimum_completed_positions: int | None = None,
    baseline_nav_lamports: int | None = None,
) -> dict[str, Any]:
    """원장을 바꾸지 않고 고정 event sequence 이후 Paper cohort를 계산한다."""
    if not isinstance(ledger, dict):
        raise TypeError("paper ledger must be a JSON object")
    if ledger.get("schema_version") != SUPPORTED_LEDGER_SCHEMA_VERSION:
        raise RuntimeError("paper ledger schema is unsupported")
    events = ledger.get("events")
    positions = ledger.get("positions")
    if not isinstance(events, list) or any(not isinstance(item, dict) for item in events):
        raise RuntimeError("paper ledger events are malformed")
    if not isinstance(positions, dict) or any(
        not isinstance(item, dict) for item in positions.values()
    ):
        raise RuntimeError("paper ledger positions are malformed")
    cutoff = _integer(cutoff_event_seq, name="cutoff_event_seq", minimum=1)
    sequences = [
        _integer(item.get("event_seq"), name="event_seq", minimum=1)
        for item in events
    ]
    if sequences != sorted(sequences) or len(sequences) != len(set(sequences)):
        raise RuntimeError("paper event sequence is duplicated or out of order")
    next_sequence = _integer(
        ledger.get("next_event_seq"), name="next_event_seq", minimum=1
    )
    if sequences and next_sequence <= sequences[-1]:
        raise RuntimeError("paper next_event_seq does not follow retained events")
    if next_sequence < cutoff + 1:
        raise RuntimeError("paper ledger has not reached the configured cutoff")
    post_events = [
        item for item, sequence in zip(events, sequences) if sequence > cutoff
    ]
    post_sequences = [sequence for sequence in sequences if sequence > cutoff]
    if post_sequences != list(range(cutoff + 1, next_sequence)):
        raise RuntimeError(
            "Paper MVP cohort is incomplete because retained event sequence has a gap"
        )

    new_buys = [item for item in post_events if item.get("type") == "BUY"]
    buy_by_position: dict[str, dict[str, Any]] = {}
    for buy in new_buys:
        position_id = str(buy.get("position_id") or "").strip()
        if not position_id or position_id in buy_by_position:
            raise RuntimeError("Paper MVP BUY position identity is missing or duplicated")
        buy_by_position[position_id] = buy
    post_sells = [item for item in post_events if item.get("type") == "SELL"]
    sells_by_position: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for sell in post_sells:
        position_id = str(sell.get("position_id") or "").strip()
        if not position_id:
            raise RuntimeError("post-cutoff SELL is missing position_id")
        sells_by_position[position_id].append(sell)
    open_by_position: dict[str, dict[str, Any]] = {}
    for position in positions.values():
        position_id = str(position.get("position_id") or "").strip()
        if not position_id or position_id in open_by_position:
            raise RuntimeError("open position identity is missing or duplicated")
        open_by_position[position_id] = position

    completed: list[dict[str, Any]] = []
    open_cohort: list[dict[str, Any]] = []
    for position_id, buy in buy_by_position.items():
        entry_amount = _integer(
            buy.get("token_amount_raw"), name="BUY token_amount_raw", minimum=1
        )
        sells = sells_by_position.get(position_id, [])
        sold = sum(
            _integer(item.get("token_amount_raw"), name="SELL token_amount_raw", minimum=1)
            for item in sells
        )
        if sold > entry_amount:
            raise RuntimeError(f"SELL amount exceeds BUY amount: {position_id}")
        current = open_by_position.get(position_id)
        if current is not None:
            remaining = _integer(
                current.get("token_amount_raw"),
                name="position token_amount_raw",
                minimum=1,
            )
            if sold + remaining != entry_amount:
                raise RuntimeError(f"open position token linkage is incomplete: {position_id}")
            open_cohort.append({"buy": buy, "position": current, "sells": sells})
        elif sold == entry_amount:
            completed.append(_cohort_record(buy, sells))
        else:
            raise RuntimeError(f"closed position SELL linkage is incomplete: {position_id}")

    completed_ids = {item["position_id"] for item in completed}
    open_ids = {str(item["buy"]["position_id"]) for item in open_cohort}
    if completed_ids & open_ids:
        raise RuntimeError("Paper MVP position is both open and completed")

    carry_in_sells = [
        item for item in post_sells
        if str(item.get("position_id") or "") not in buy_by_position
    ]
    new_cohort_sells = [
        item for item in post_sells
        if str(item.get("position_id") or "") in buy_by_position
    ]
    cohort_realized = sum(
        _integer(item.get("realized_pnl_lamports"), name="SELL realized_pnl_lamports")
        for item in new_cohort_sells
    )
    carry_in_realized = sum(
        _integer(item.get("realized_pnl_lamports"), name="SELL realized_pnl_lamports")
        for item in carry_in_sells
    )
    authoritative_realized_delta = cohort_realized + carry_in_realized

    missing_open_values: list[str] = []
    open_values: list[int] = []
    open_costs: list[int] = []
    open_rows: list[dict[str, Any]] = []
    for item in open_cohort:
        position = item["position"]
        position_id = str(item["buy"]["position_id"])
        cost = _integer(
            position.get("remaining_cost_lamports"),
            name="remaining_cost_lamports",
            minimum=0,
        )
        current_value_raw = position.get("current_value_lamports")
        current_value = (
            _integer(current_value_raw, name="current_value_lamports", minimum=0)
            if current_value_raw is not None else None
        )
        if current_value is None:
            missing_open_values.append(position_id)
        else:
            open_values.append(current_value)
            open_costs.append(cost)
        open_rows.append({
            "position_id": position_id,
            "family": _route_family(item["buy"].get("route_type")),
            "remaining_cost_lamports": cost,
            "current_value_lamports": current_value,
            "unrealized_pnl_lamports": (
                current_value - cost if current_value is not None else None
            ),
            "price_updated_at": position.get("price_updated_at"),
            "risk_state": str(position.get("risk_state") or "UNKNOWN"),
        })

    overall = _performance(completed)
    by_family = {
        family: _performance([item for item in completed if item["family"] == family])
        for family in ("SMART_MONEY", "MOMENTUM")
    }
    daily = _period_performance(completed, "day")
    weekly = _period_performance(completed, "week")
    chronological_completed = sorted(
        completed,
        key=lambda item: (item["closed_at_epoch"], item["position_id"]),
    )
    early_count = len(chronological_completed) // 2
    early_performance = _performance(chronological_completed[:early_count])
    late_performance = _performance(chronological_completed[early_count:])

    cash_raw = ledger.get("cash_lamports")
    current_cash = (
        _integer(cash_raw, name="cash_lamports", minimum=0)
        if cash_raw is not None else None
    )
    all_open_values: list[int] = []
    missing_nav_position_ids: list[str] = []
    for position_id, position in open_by_position.items():
        current_value_raw = position.get("current_value_lamports")
        if current_value_raw is None:
            missing_nav_position_ids.append(position_id)
        else:
            all_open_values.append(
                _integer(
                    current_value_raw,
                    name="current_value_lamports",
                    minimum=0,
                )
            )
    current_nav_available = current_cash is not None and not missing_nav_position_ids
    current_open_value = sum(all_open_values) if not missing_nav_position_ids else None
    current_nav = (
        current_cash + current_open_value
        if current_nav_available and current_cash is not None and current_open_value is not None
        else None
    )
    explicit_baseline_nav = (
        _integer(
            baseline_nav_lamports,
            name="baseline_nav_lamports",
            minimum=0,
        )
        if baseline_nav_lamports is not None else None
    )
    evidence_direction = _evidence_direction(overall)
    generated_epoch = _timestamp(generated_at, name="generated_at")
    review_epoch = _timestamp(review_at, name="review_at")
    minimum_sample = (
        _integer(
            minimum_completed_positions,
            name="minimum_completed_positions",
            minimum=1,
        )
        if minimum_completed_positions is not None else None
    )
    if generated_epoch < review_epoch:
        verdict = "NOT_DUE"
        verdict_reason = "FIRST_REVIEW_TIME_NOT_REACHED"
    elif minimum_sample is None:
        verdict = "INSUFFICIENT_MVP_SAMPLE"
        verdict_reason = "NUMERIC_SAMPLE_THRESHOLD_NOT_DEFINED_IN_PAPER_MVP_CONTRACT"
    elif overall["completed_position_count"] < minimum_sample:
        verdict = "INSUFFICIENT_MVP_SAMPLE"
        verdict_reason = "COMPLETED_POSITION_COUNT_BELOW_EXPLICIT_THRESHOLD"
    elif evidence_direction == "POSITIVE_DIRECTION":
        verdict = "POSITIVE_MVP_SIGNAL"
        verdict_reason = "POSITIVE_DIRECTION_AFTER_EXPLICIT_SAMPLE_GATE"
    elif evidence_direction == "NEGATIVE_DIRECTION":
        verdict = "NEGATIVE_MVP_SIGNAL"
        verdict_reason = "NEGATIVE_DIRECTION_AFTER_EXPLICIT_SAMPLE_GATE"
    else:
        verdict = "INSUFFICIENT_MVP_SAMPLE"
        verdict_reason = "MIXED_OR_THIN_EVIDENCE"

    retained_sell_total = sum(
        _integer(item.get("realized_pnl_lamports"), name="SELL realized_pnl_lamports")
        for item in events if item.get("type") == "SELL"
    )
    entry_events = list(buy_by_position.values())
    report = {
        "schema_version": 1,
        "generated_at": generated_at,
        "review_at": review_at,
        "review_due": generated_epoch >= review_epoch,
        "paper_mvp_start_at": PAPER_MVP_START_AT,
        "cohort_definition": f"BUY event_seq > {cutoff}",
        "cutoff_event_seq": cutoff,
        "ledger_snapshot": {
            "schema_version": ledger.get("schema_version"),
            "version": ledger.get("version"),
            "next_event_seq": next_sequence,
            "retained_event_count": len(events),
            "retained_min_event_seq": min(sequences) if sequences else None,
            "retained_max_event_seq": max(sequences) if sequences else None,
            "post_cutoff_event_count": len(post_events),
            "cohort_sequence_complete": True,
        },
        "cohort_counts": {
            "new_buy_count": len(new_buys),
            "post_cutoff_sell_event_count": len(post_sells),
            "new_entry_cohort_sell_event_count": len(new_cohort_sells),
            "completed_position_count": len(completed),
            "open_position_count": len(open_cohort),
            "total_current_open_position_count": len(positions),
            "carry_in_sell_event_count": len(carry_in_sells),
        },
        "realized_pnl": {
            "new_entry_cohort_lamports": cohort_realized,
            "carry_in_lamports": carry_in_realized,
            "mvp_delta_lamports": authoritative_realized_delta,
            "basis": "sum_of_retained_SELL_events_after_cutoff",
            "historical_baseline_lamports": PAPER_MVP_BASELINE_REALIZED_PNL_LAMPORTS,
            "retained_ledger_sell_total_lamports": retained_sell_total,
            "retained_total_minus_baseline_lamports": (
                retained_sell_total - PAPER_MVP_BASELINE_REALIZED_PNL_LAMPORTS
            ),
            "retained_total_reconciles_with_authoritative_delta": (
                retained_sell_total - PAPER_MVP_BASELINE_REALIZED_PNL_LAMPORTS
                == authoritative_realized_delta
            ),
        },
        "unrealized_pnl": {
            "available": not missing_open_values,
            "cohort_open_position_count": len(open_cohort),
            "valued_position_count": len(open_values),
            "missing_position_ids": missing_open_values,
            "remaining_cost_lamports": sum(open_costs) if not missing_open_values else None,
            "current_value_lamports": sum(open_values) if not missing_open_values else None,
            "unrealized_pnl_lamports": (
                sum(open_values) - sum(open_costs) if not missing_open_values else None
            ),
            "positions": open_rows,
        },
        "completed_performance": overall,
        "family_performance": by_family,
        "chronological_performance": {
            "utc_daily": daily,
            "utc_weekly": weekly,
            "early_half": early_performance,
            "late_half": late_performance,
            "split_rule": "completed positions by close time; odd remainder belongs to late_half",
            "completed_active_day_count": len(daily),
            "negative_completed_day_count": sum(
                item["realized_pnl_lamports"] < 0 for item in daily
            ),
            "positive_completed_day_count": sum(
                item["realized_pnl_lamports"] > 0 for item in daily
            ),
        },
        "exposure": {
            "new_entry_cost_lamports": sum(
                _integer(item.get("cost_lamports"), name="BUY cost_lamports", minimum=1)
                for item in entry_events
            ),
            "completed_entry_cost_lamports": overall["entry_cost_lamports"],
            "open_remaining_cost_lamports": sum(
                int(item["position"].get("remaining_cost_lamports", 0) or 0)
                for item in open_cohort
            ),
        },
        "nav": {
            "current_available": current_nav_available,
            "cash_lamports": current_cash,
            "current_open_position_count": len(open_by_position),
            "valued_current_open_position_count": len(all_open_values),
            "missing_current_value_position_ids": missing_nav_position_ids,
            "current_open_value_lamports": current_open_value,
            "current_nav_lamports": current_nav,
            "baseline_nav_lamports": explicit_baseline_nav,
            "baseline_nav_source": (
                "EXPLICIT_ARGUMENT" if explicit_baseline_nav is not None else None
            ),
            "nav_delta_lamports": (
                current_nav - explicit_baseline_nav
                if current_nav is not None and explicit_baseline_nav is not None
                else None
            ),
            "nav_delta_unavailable_reason": (
                None
                if current_nav is not None and explicit_baseline_nav is not None
                else (
                    "CURRENT_NAV_UNAVAILABLE"
                    if current_nav is None
                    else "PRE_MVP_BASELINE_NAV_NOT_FROZEN_IN_PAPER_MVP_CONTRACT"
                )
            ),
        },
        "execution_diagnostics": {
            "entry_price_impact_pct": _numeric_summary(
                item.get("entry_price_impact_pct") for item in entry_events
            ),
            "preflight_exit_price_impact_pct": _numeric_summary(
                item.get("exit_price_impact_pct") for item in entry_events
            ),
            "expected_slippage_bps": _numeric_summary(
                item.get("expected_slippage_bps") for item in entry_events
            ),
            "entry_latency_ms": _numeric_summary(
                item.get("entry_latency_ms") for item in entry_events
            ),
            "trigger_price_impact_pct": _numeric_summary(
                item.get("trigger_price_impact_pct") for item in new_cohort_sells
            ),
            "exit_trigger_latency_ms": _numeric_summary(
                item.get("exit_trigger_latency_ms") for item in new_cohort_sells
            ),
            "exit_quote_age_ms": _numeric_summary(
                item.get("quote_age_ms") for item in new_cohort_sells
            ),
        },
        "fee_accounting": {
            "jupiter_route_fee": "embedded_in_stored_quote_proceeds_not_separately_persisted",
            "network_fee_lamports": None,
            "jito_tip_lamports": None,
            "paper_ledger_explicit_fee_deduction": False,
        },
        "evidence_direction": evidence_direction,
        "minimum_completed_positions": minimum_sample,
        "verdict": verdict,
        "verdict_reason": verdict_reason,
        "official_due_verdicts": [
            "POSITIVE_MVP_SIGNAL",
            "NEGATIVE_MVP_SIGNAL",
            "INSUFFICIENT_MVP_SAMPLE",
        ],
        "automatic_trading_changes": False,
        "production_configuration_changes": False,
    }
    return report


def read_paper_ledger_snapshot(path: Path | None = None) -> dict[str, Any]:
    """기본 production 원장은 잠금 안에서 읽고 explicit copy는 그대로 읽는다."""
    source = path or LEDGER_PATH
    if path is not None and str(path) == "-":
        try:
            document = json.loads(sys.stdin.read())
        except json.JSONDecodeError as exc:
            raise RuntimeError("stdin paper ledger JSON is malformed") from exc
        if not isinstance(document, dict):
            raise RuntimeError("stdin paper ledger must be a JSON object")
        return document
    if path is not None:
        return read_json(source, _empty_ledger())
    with exclusive_file_lock(source):
        return read_json(source, _empty_ledger())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only Paper MVP review")
    parser.add_argument("--input", type=Path)
    parser.add_argument("--cutoff-event-seq", type=int, default=PAPER_MVP_BUY_EVENT_SEQ_CUTOFF)
    parser.add_argument("--review-at", default=PAPER_MVP_FIRST_REVIEW_AT)
    parser.add_argument("--generated-at")
    parser.add_argument("--minimum-completed-positions", type=int)
    parser.add_argument("--baseline-nav-lamports", type=int)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args(argv)
    if args.minimum_completed_positions is not None and args.minimum_completed_positions < 1:
        parser.error("--minimum-completed-positions must be positive")
    if args.baseline_nav_lamports is not None and args.baseline_nav_lamports < 0:
        parser.error("--baseline-nav-lamports must not be negative")
    generated_at = args.generated_at or datetime.now(timezone.utc).isoformat()
    ledger = read_paper_ledger_snapshot(args.input)
    report = build_paper_mvp_review(
        ledger,
        generated_at=generated_at,
        cutoff_event_seq=args.cutoff_event_seq,
        review_at=args.review_at,
        minimum_completed_positions=args.minimum_completed_positions,
        baseline_nav_lamports=args.baseline_nav_lamports,
    )
    print(json.dumps(
        report,
        ensure_ascii=False,
        indent=2 if args.pretty else None,
        sort_keys=True,
    ))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
