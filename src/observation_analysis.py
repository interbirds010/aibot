"""관찰 원장만으로 조건별 성과와 시간순 holdout 통계를 계산한다."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.solana_rpc import RPC_FAILURE_REASONS
from src.state_store import read_json, update_json


ABSOLUTE_MAX_ANALYSIS_ROWS = 10_000
DEFAULT_MAX_ANALYSIS_ROWS = 5_000
DEFAULT_MIN_SAMPLES = 20
DEFAULT_HOLDOUT_FRACTION = 0.20
DEFAULT_OUTCOME_INTERVAL = "15m"
DEFAULT_MIN_OUTCOME_COVERAGE = 0.80
SHADOW_STRATEGY_NAMES = (
    "fixed_hold_1m",
    "fixed_hold_3m",
    "fixed_hold_5m",
    "fixed_hold_15m",
    "fixed_hold_30m",
    "fixed_hold_60m",
    "sampled_route_exit",
)
RESEARCH_HORIZONS = ("1m", "3m", "5m", "15m", "30m", "60m")
RESEARCH_PRIMARY_HORIZON = "60m"
PIPELINE_INTERRUPTION_REASONS = frozenset({"DISCOVERY_PROCESSING_INTERRUPTED"})
UNTRACKABLE_QUOTE_STATUS_REASONS = {
    "NO_ROUTE": "ENTRY_NO_ROUTE",
    "NOT_REQUESTED": "ENTRY_NOT_REQUESTED",
    "SIZE_UNUSABLE": "ENTRY_SIZE_UNUSABLE",
    "PROCESSING_FAILED": "PROCESSING_FAILED",
}
UNTRACKABLE_QUOTE_STATUSES = set(UNTRACKABLE_QUOTE_STATUS_REASONS)
ANALYSIS_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "observation_analysis.json"
)


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _started_at_epoch(row: dict[str, Any]) -> float | None:
    epoch = _finite_number(row.get("started_at_epoch"))
    if epoch is not None and epoch >= 0:
        return epoch
    raw = row.get("started_at")
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        parsed = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    epoch = parsed.timestamp()
    return epoch if math.isfinite(epoch) and epoch >= 0 else None


def _interval_return(row: dict[str, Any], interval: str) -> float | None:
    sample = _interval_sample(row, interval)
    return _finite_number(sample.get("return_percent")) if sample else None


def _interval_sample(
    row: dict[str, Any], interval: str
) -> dict[str, Any] | None:
    samples = row.get("samples")
    if not isinstance(samples, list):
        return None
    for sample in samples:
        if not isinstance(sample, dict) or str(sample.get("interval")) != interval:
            continue
        return sample
    return None


def _outcome_trackable(row: dict[str, Any]) -> bool:
    """진입 견적 부재로 outcome을 만들 수 없는 신호를 구분한다."""
    quote_status = str(row.get("quote_status") or "").strip().upper()
    return quote_status not in UNTRACKABLE_QUOTE_STATUSES


def _missing_outcome_reason(
    row: dict[str, Any], sample: dict[str, Any] | None
) -> str:
    quote_status = str(row.get("quote_status") or "").strip().upper()
    if quote_status == "PROCESSING_FAILED":
        decision_reasons = row.get("decision_reasons")
        if isinstance(decision_reasons, list):
            for raw_reason in decision_reasons:
                reason = str(raw_reason).strip().upper()
                if reason in PIPELINE_INTERRUPTION_REASONS:
                    return reason
                if reason in RPC_FAILURE_REASONS:
                    return reason
    entry_reason = UNTRACKABLE_QUOTE_STATUS_REASONS.get(quote_status)
    if entry_reason is not None:
        return entry_reason
    if sample is None:
        return "NOT_SAMPLED"
    error = str(sample.get("error") or "").strip().upper()
    if "HORIZON_MISSED" in error:
        return "HORIZON_MISSED"
    if "NO_ROUTE" in error or ("NO " in error and "ROUTE" in error):
        return "EXIT_NO_ROUTE"
    if error:
        return "API_FAILURE"
    return "UNKNOWN"


def _lag_metrics(samples: list[dict[str, Any]]) -> dict[str, Any]:
    lags = sorted(
        lag
        for sample in samples
        if (lag := _finite_number(sample.get("sample_lag_seconds"))) is not None
        and lag >= 0
    )
    return {
        "lag_sample_count": len(lags),
        "mean_sample_lag_seconds": _rounded(_stable_mean(lags)),
        "median_sample_lag_seconds": _rounded(_quantile(lags, 0.50)),
        "p90_sample_lag_seconds": _rounded(_quantile(lags, 0.90)),
        "p95_sample_lag_seconds": _rounded(_quantile(lags, 0.95)),
        "max_sample_lag_seconds": _rounded(max(lags) if lags else None),
    }


def _stable_mean(values: list[float]) -> float | None:
    if not values:
        return None
    scale = max(abs(value) for value in values)
    if scale == 0:
        return 0.0
    result = scale * math.fsum(value / scale for value in values) / len(values)
    return result if math.isfinite(result) else None


def _quantile(sorted_values: list[float], fraction: float) -> float | None:
    if not sorted_values:
        return None
    position = (len(sorted_values) - 1) * fraction
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    lower = sorted_values[lower_index]
    upper = sorted_values[upper_index]
    if lower_index == upper_index:
        return lower
    weight = position - lower_index
    if lower == 0 or upper == 0 or (lower > 0) == (upper > 0):
        value = lower + (upper - lower) * weight
    else:
        value = lower * (1 - weight) + upper * weight
    return value if math.isfinite(value) else None


def performance_metrics(
    returns: list[Any], *, minimum_samples: int
) -> dict[str, Any]:
    """유한 ROI 목록의 bounded 요약 통계를 반환한다."""
    finite = [
        number
        for value in returns
        if (number := _finite_number(value)) is not None
    ]
    ordered = sorted(finite)
    count = len(ordered)
    wins = sum(value > 0 for value in ordered)
    winning_returns = [value for value in ordered if value > 0]
    losing_returns = [value for value in ordered if value < 0]
    gross_profit = math.fsum(winning_returns)
    gross_loss = abs(math.fsum(losing_returns))
    median = _quantile(ordered, 0.50)
    downside_tail = _quantile(ordered, 0.10)
    worst_count = max(1, math.ceil(count * 0.10)) if count else 0
    worst_decile = _stable_mean(ordered[:worst_count]) if worst_count else None
    return {
        "sample_count": count,
        "minimum_samples": minimum_samples,
        "sufficient_samples": count >= minimum_samples,
        "win_count": wins,
        "win_rate_percent": round(wins / count * 100, 4) if count else None,
        "mean_roi_percent": _rounded(_stable_mean(ordered)),
        "median_roi_percent": _rounded(median),
        "average_win_percent": _rounded(_stable_mean(winning_returns)),
        "average_loss_percent": _rounded(_stable_mean(losing_returns)),
        "expectancy_percent": _rounded(_stable_mean(ordered)),
        "profit_factor": (
            _rounded(gross_profit / gross_loss) if gross_loss > 0 else None
        ),
        "downside_p10_roi_percent": _rounded(downside_tail),
        "worst_decile_mean_roi_percent": _rounded(worst_decile),
    }


def _research_decision(row: dict[str, Any]) -> str:
    decision = str(row.get("research_decision") or "").strip().upper()
    if decision in {"ENTERED", "REJECTED", "SHADOW"}:
        return decision
    paper_status = str(row.get("paper_experiment_status") or "").strip().upper()
    if paper_status in {"OPENED", "CLOSED"}:
        return "ENTERED"
    entry_status = str(row.get("decision_status") or "").strip().upper()
    if entry_status in {"REJECTED", "UNAVAILABLE", "FAILED"}:
        return "REJECTED"
    if entry_status == "APPROVED":
        return "SHADOW"
    return "UNKNOWN"


def _research_group_value(value: Any, *, uppercase: bool = False) -> str:
    text = str(value or "").strip()
    if not text:
        return "UNKNOWN"
    return text.upper() if uppercase else text[:120]


def _sampled_excursions(row: dict[str, Any]) -> tuple[float | None, float | None]:
    mfe = _finite_number(row.get("mfe_percent"))
    mae = _finite_number(row.get("mae_percent"))
    if mfe is not None and mae is not None:
        return mfe, mae
    returns = [
        value
        for interval in RESEARCH_HORIZONS
        if (value := _interval_return(row, interval)) is not None
    ]
    return (
        max(0.0, mfe) if mfe is not None
        else (max([0.0, *returns]) if returns else None),
        min(0.0, mae) if mae is not None
        else (min([0.0, *returns]) if returns else None),
    )


def _research_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    decisions = [_research_decision(row) for row in rows]
    horizons: dict[str, Any] = {}
    for horizon in RESEARCH_HORIZONS:
        samples = [_interval_sample(row, horizon) for row in rows]
        returns = [
            _finite_number(sample.get("return_percent")) if sample else None
            for sample in samples
        ]
        metrics = performance_metrics(
            returns,
            minimum_samples=1,
        )
        signal_count = len(rows)
        sampled_count = metrics["sample_count"]
        trackable_count = sum(_outcome_trackable(row) for row in rows)
        missing_reasons: dict[str, int] = {}
        for row, sample, outcome in zip(rows, samples, returns):
            if outcome is not None:
                continue
            reason = _missing_outcome_reason(row, sample)
            missing_reasons[reason] = missing_reasons.get(reason, 0) + 1
        metrics.update({
            "signal_count": signal_count,
            "sampled_count": sampled_count,
            "missing_count": signal_count - sampled_count,
            "coverage_rate_percent": (
                round(sampled_count / signal_count * 100, 4)
                if signal_count else None
            ),
            "outcome_trackable_count": trackable_count,
            "outcome_untrackable_count": signal_count - trackable_count,
            "trackable_coverage_rate_percent": (
                round(sampled_count / trackable_count * 100, 4)
                if trackable_count else None
            ),
            "missing_reasons": dict(sorted(missing_reasons.items())),
            **_lag_metrics([
                sample for sample in samples if isinstance(sample, dict)
            ]),
        })
        metrics["completed_outcome_count"] = metrics["sample_count"]
        metrics["average_return_percent"] = metrics["mean_roi_percent"]
        horizons[horizon] = metrics

    excursions = [_sampled_excursions(row) for row in rows]
    mfe_values = [mfe for mfe, _ in excursions if mfe is not None]
    mae_values = [mae for _, mae in excursions if mae is not None]
    trackable_count = sum(_outcome_trackable(row) for row in rows)
    return {
        "signal_count": len(rows),
        "outcome_trackable_count": trackable_count,
        "outcome_untrackable_count": len(rows) - trackable_count,
        "entered_count": decisions.count("ENTERED"),
        "rejected_count": decisions.count("REJECTED"),
        "shadow_count": decisions.count("SHADOW"),
        "unknown_decision_count": decisions.count("UNKNOWN"),
        "expired_count": sum(
            str(row.get("status") or "").upper() == "EXPIRED_UNSAMPLED"
            for row in rows
        ),
        "horizons": horizons,
        "sampled_mfe_count": len(mfe_values),
        "sampled_mae_count": len(mae_values),
        "average_sampled_mfe_percent": _rounded(_stable_mean(mfe_values)),
        "average_sampled_mae_percent": _rounded(_stable_mean(mae_values)),
    }


def build_research_metrics(rows: list[Any]) -> dict[str, Any]:
    """원본 signal event를 삭제하지 않고 Research V1 집계를 만든다."""
    if not isinstance(rows, list):
        raise TypeError("research rows must be a list")
    valid_rows = [row for row in rows if isinstance(row, dict)]
    summary = _research_summary(valid_rows)
    summary["invalid_row_count"] = len(rows) - len(valid_rows)
    summary["excursion_basis"] = "scheduled_jupiter_executable_quotes"

    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in valid_rows:
        key = (
            _research_group_value(row.get("strategy_version")),
            _research_group_value(row.get("route_type"), uppercase=True),
            _research_group_value(row.get("signal_type"), uppercase=True),
        )
        grouped.setdefault(key, []).append(row)
    summary["groups"] = [
        {
            "strategy_version": key[0],
            "route_type": key[1],
            "signal_type": key[2],
            **_research_summary(group_rows),
        }
        for key, group_rows in sorted(grouped.items())
    ]

    rejected_by_reason: dict[str, list[dict[str, Any]]] = {}
    for row in valid_rows:
        if _research_decision(row) != "REJECTED":
            continue
        raw_reasons = row.get("decision_reasons")
        reasons = (
            {
                str(reason).strip().upper()[:200]
                for reason in raw_reasons
                if str(reason).strip()
            }
            if isinstance(raw_reasons, list)
            else set()
        )
        for reason in reasons or {"UNKNOWN"}:
            rejected_by_reason.setdefault(reason, []).append(row)
    rejection_rows = []
    for reason, reason_rows in sorted(rejected_by_reason.items()):
        returns = [
            _interval_return(row, RESEARCH_PRIMARY_HORIZON)
            for row in reason_rows
        ]
        finite = [value for value in returns if value is not None]
        sampled_count = len(finite)
        signal_count = len(reason_rows)
        trackable_count = sum(
            _outcome_trackable(row) for row in reason_rows
        )
        rejection_rows.append({
            "reason": reason,
            "outcome_interval": RESEARCH_PRIMARY_HORIZON,
            "signal_count": signal_count,
            "outcome_sample_count": sampled_count,
            "outcome_missing_count": signal_count - sampled_count,
            "coverage_rate_percent": (
                round(sampled_count / signal_count * 100, 4)
                if signal_count else None
            ),
            "outcome_trackable_count": trackable_count,
            "outcome_untrackable_count": signal_count - trackable_count,
            "trackable_coverage_rate_percent": (
                round(sampled_count / trackable_count * 100, 4)
                if trackable_count else None
            ),
            "completed_outcome_count": sampled_count,
            "average_return_percent": _rounded(_stable_mean(finite)),
            "positive_rate_percent": (
                round(sum(value > 0 for value in finite) / len(finite) * 100, 4)
                if finite else None
            ),
        })
    summary["rejection_reasons"] = rejection_rows
    return summary


def _rounded(value: float | None) -> float | None:
    return round(value, 4) if value is not None and math.isfinite(value) else None


def _bucket(value: float | None, cuts: tuple[float, ...]) -> str | None:
    if value is None:
        return None
    lower: float | None = None
    for upper in cuts:
        if value < upper:
            return (
                f"below_{upper:g}"
                if lower is None
                else f"{lower:g}_to_below_{upper:g}"
            )
        lower = upper
    return f"{cuts[-1]:g}_or_more"


def _normalized_choice(value: Any, allowed: set[str]) -> str:
    normalized = str(value or "").strip().upper()
    return normalized if normalized in allowed else "UNKNOWN"


def _condition_keys(row: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    safety = row.get("safety_metrics")
    safety = safety if isinstance(safety, dict) else {}
    momentum = row.get("momentum_metrics")
    momentum = momentum if isinstance(momentum, dict) else {}
    discovery = row.get("discovery_metadata")
    discovery = discovery if isinstance(discovery, dict) else {}
    eligible = row.get("candidate_v2_eligible")
    candidate_decision = (
        "ELIGIBLE" if eligible is True else "NOT_ELIGIBLE" if eligible is False else "UNKNOWN"
    )
    conditions: list[tuple[str, str]] = [
        ("route", _normalized_choice(row.get("route_type"), {"A", "B"})),
        (
            "entry_decision",
            _normalized_choice(
                row.get("decision_status"),
                {"APPROVED", "REJECTED", "UNAVAILABLE", "FAILED"},
            ),
        ),
        ("candidate_decision", candidate_decision),
        (
            "paper_decision",
            _normalized_choice(
                row.get("paper_experiment_status"),
                {
                    "ELIGIBLE", "OPENED", "SKIPPED_CAPACITY", "FAILED",
                    "CLOSED", "OBSERVATION_ONLY", "NOT_ELIGIBLE",
                },
            ),
        ),
    ]
    reasons = row.get("decision_reasons")
    if isinstance(reasons, list):
        conditions.extend(
            ("decision_reason", str(reason).strip().upper()[:120])
            for reason in reasons[:20]
            if str(reason).strip()
        )
    whale_paid_lamports = _finite_number(discovery.get("whale_paid_lamports"))
    numeric_specs = (
        ("safety_score", _finite_number(row.get("safety_score")), (55.0, 70.0, 90.0)),
        ("developer_supply_percent", _finite_number(safety.get("developer_supply_percent")), (5.0, 10.0)),
        ("lp_locked_percent", _finite_number(safety.get("lp_locked_percent")), (40.0, 80.0)),
        ("liquidity_usd", _finite_number(safety.get("liquidity_usd")), (7_500.0, 10_000.0)),
        ("dex_momentum_score", _finite_number(row.get("dex_momentum_score")), (90.0, 100.0)),
        (
            "volume_m5_usd",
            _finite_number(momentum.get("volume_m5_usd")),
            (10_000.0, 15_000.0),
        ),
        (
            "net_buys_m5",
            _finite_number(momentum.get("net_buys_m5")),
            (10.0, 15.0),
        ),
        (
            "buy_sell_ratio_m5",
            _finite_number(momentum.get("buy_sell_ratio_m5")),
            (1.5, 1.8),
        ),
        (
            "pair_age_seconds",
            _finite_number(momentum.get("pair_age_seconds")),
            (900.0,),
        ),
        ("unknown_whale_count", _finite_number(momentum.get("unknown_whale_count")), (3.0,)),
        (
            "source_buy_sol",
            whale_paid_lamports / 1_000_000_000
            if whale_paid_lamports is not None
            else None,
            (1.0, 1.5, 5.0),
        ),
        (
            "entry_price_impact_pct",
            _finite_number(row.get("entry_price_impact_pct")),
            (1.0, 3.5),
        ),
        (
            "exit_price_impact_pct",
            _finite_number(row.get("exit_price_impact_pct")),
            (1.0, 3.5),
        ),
    )
    for dimension, value, cuts in numeric_specs:
        bucket = _bucket(value, cuts)
        if bucket is not None:
            conditions.append((dimension, bucket))
    return tuple(conditions)


def _validation_status(
    train: dict[str, Any], holdout: dict[str, Any]
) -> str:
    if not train["sufficient_samples"] or not holdout["sufficient_samples"]:
        return "INSUFFICIENT_SAMPLES"
    return "SUFFICIENT_SAMPLES"


def _outcome_coverage(values: list[Any]) -> dict[str, Any]:
    valid_count = sum(_finite_number(value) is not None for value in values)
    total_count = len(values)
    return {
        "cohort_count": total_count,
        "valid_outcome_count": valid_count,
        "missing_outcome_count": total_count - valid_count,
        "outcome_coverage_percent": (
            round(valid_count / total_count * 100, 4) if total_count else None
        ),
    }


def _comparison(
    train_returns: list[Any],
    holdout_returns: list[Any],
    *,
    minimum_samples: int,
    minimum_outcome_coverage: float,
) -> dict[str, Any]:
    train = performance_metrics(train_returns, minimum_samples=minimum_samples)
    holdout = performance_metrics(holdout_returns, minimum_samples=minimum_samples)
    train_coverage = _outcome_coverage(train_returns)
    holdout_coverage = _outcome_coverage(holdout_returns)
    train_coverage_ok = (
        train_coverage["cohort_count"] > 0
        and train_coverage["valid_outcome_count"]
        / train_coverage["cohort_count"] >= minimum_outcome_coverage
    )
    holdout_coverage_ok = (
        holdout_coverage["cohort_count"] > 0
        and holdout_coverage["valid_outcome_count"]
        / holdout_coverage["cohort_count"] >= minimum_outcome_coverage
    )
    train["outcome_coverage"] = train_coverage
    train["coverage_sufficient"] = train_coverage_ok
    holdout["outcome_coverage"] = holdout_coverage
    holdout["coverage_sufficient"] = holdout_coverage_ok
    train_mean = train["mean_roi_percent"]
    holdout_mean = holdout["mean_roi_percent"]
    return {
        "validation_status": (
            _validation_status(train, holdout)
            if train_coverage_ok and holdout_coverage_ok
            else "INSUFFICIENT_OUTCOME_COVERAGE"
        ),
        "train": train,
        "holdout": holdout,
        "mean_roi_delta_percent": (
            _rounded(holdout_mean - train_mean)
            if train_mean is not None and holdout_mean is not None
            else None
        ),
        "mean_direction_consistent": (
            (train_mean >= 0) == (holdout_mean >= 0)
            if train_mean is not None and holdout_mean is not None
            else None
        ),
    }


def build_observation_analysis(
    rows: list[Any],
    *,
    outcome_interval: str = DEFAULT_OUTCOME_INTERVAL,
    minimum_samples: int = DEFAULT_MIN_SAMPLES,
    holdout_fraction: float = DEFAULT_HOLDOUT_FRACTION,
    max_rows: int = DEFAULT_MAX_ANALYSIS_ROWS,
    minimum_outcome_coverage: float = DEFAULT_MIN_OUTCOME_COVERAGE,
) -> dict[str, Any]:
    """최신 bounded 관찰을 시간순 train/holdout으로 나눠 조건별 성과를 계산한다."""
    if not isinstance(rows, list):
        raise TypeError("observation rows must be a list")
    if outcome_interval not in set(RESEARCH_HORIZONS):
        raise ValueError(
            "outcome_interval must be 1m, 3m, 5m, 15m, 30m, or 60m"
        )
    fraction = _finite_number(holdout_fraction)
    if fraction is None or not 0 < fraction < 1:
        raise ValueError("holdout_fraction must be finite and between 0 and 1")
    coverage = _finite_number(minimum_outcome_coverage)
    if coverage is None or not 0 < coverage <= 1:
        raise ValueError("minimum_outcome_coverage must be finite and in (0, 1]")
    try:
        requested_limit = int(max_rows)
        minimum = int(minimum_samples)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("max_rows and minimum_samples must be integers") from exc
    limit = min(ABSOLUTE_MAX_ANALYSIS_ROWS, requested_limit)
    if limit < 1:
        raise ValueError("max_rows must be positive")
    if minimum < 1 or minimum > limit:
        raise ValueError("minimum_samples must be between 1 and max_rows")

    bounded = rows[-limit:]
    cohort: list[tuple[float, int, dict[str, Any], float | None]] = []
    invalid_rows = 0
    missing_outcomes = 0
    missing_timestamps = 0
    for index, item in enumerate(bounded):
        if not isinstance(item, dict):
            invalid_rows += 1
            continue
        started_at = _started_at_epoch(item)
        if started_at is None:
            missing_timestamps += 1
            continue
        outcome = _interval_return(item, outcome_interval)
        if outcome is None:
            missing_outcomes += 1
        cohort.append((started_at, index, item, outcome))
    cohort.sort(key=lambda value: (value[0], value[1]))

    eligible: list[tuple[float, int, dict[str, Any], float | None]] = []
    seen_mints: set[str] = set()
    duplicate_mint_rows = 0
    for item in cohort:
        row = item[2]
        group = str(row.get("mint") or row.get("observation_id") or "").strip()
        if not group:
            invalid_rows += 1
            continue
        if group in seen_mints:
            duplicate_mint_rows += 1
            continue
        seen_mints.add(group)
        eligible.append(item)

    total = len(eligible)
    holdout_count = min(total - 1, max(1, math.ceil(total * fraction))) if total >= 2 else 0
    split_index = total - holdout_count
    train_rows = eligible[:split_index]
    holdout_rows = eligible[split_index:]
    train_returns: list[Any] = [item[3] for item in train_rows]
    holdout_returns: list[Any] = [item[3] for item in holdout_rows]

    train_conditions: dict[tuple[str, str], list[Any]] = {}
    holdout_conditions: dict[tuple[str, str], list[Any]] = {}
    for target, split_rows in (
        (train_conditions, train_rows),
        (holdout_conditions, holdout_rows),
    ):
        for _, _, row, outcome in split_rows:
            for key in _condition_keys(row):
                target.setdefault(key, []).append(outcome)

    condition_rows: list[dict[str, Any]] = []
    all_keys = sorted(set(train_conditions) | set(holdout_conditions))
    for dimension, condition in all_keys:
        condition_rows.append({
            "dimension": dimension,
            "condition": condition,
            **_comparison(
                train_conditions.get((dimension, condition), []),
                holdout_conditions.get((dimension, condition), []),
                minimum_samples=minimum,
                minimum_outcome_coverage=coverage,
            ),
        })

    review_candidates = []
    for row in condition_rows:
        train = row["train"]
        holdout = row["holdout"]
        if not (
            train["sufficient_samples"]
            and train["coverage_sufficient"]
            and (train["mean_roi_percent"] or 0) > 0
            and (train["median_roi_percent"] or 0) > 0
        ):
            continue
        holdout_confirmed = (
            holdout["sufficient_samples"]
            and holdout["coverage_sufficient"]
            and (holdout["mean_roi_percent"] or 0) > 0
            and (holdout["median_roi_percent"] or 0) > 0
        )
        review_candidates.append({
            "dimension": row["dimension"],
            "condition": row["condition"],
            "status": "TRAIN_SELECTED_REVIEW_ONLY",
            "holdout_evaluation": (
                "CONFIRMED" if holdout_confirmed else "NOT_CONFIRMED"
            ),
            "train": train,
            "holdout": holdout,
        })
    review_candidates.sort(
        key=lambda row: (
            row["train"]["mean_roi_percent"],
            row["train"]["sample_count"],
        ),
        reverse=True,
    )

    return {
        "schema_version": 1,
        "outcome_interval": outcome_interval,
        "input_row_count": len(rows),
        "bounded_row_count": len(bounded),
        "max_rows": limit,
        "independent_mint_cohort_count": total,
        "eligible_outcome_count": sum(item[3] is not None for item in eligible),
        "excluded": {
            "outside_latest_window": max(0, len(rows) - len(bounded)),
            "invalid_row": invalid_rows,
            "missing_or_nonfinite_outcome": missing_outcomes,
            "missing_or_invalid_timestamp": missing_timestamps,
            "duplicate_mint_observation": duplicate_mint_rows,
        },
        "split": {
            "method": "chronological",
            "holdout_fraction": fraction,
            "train_count": len(train_rows),
            "holdout_count": len(holdout_rows),
        },
        "overall": _comparison(
            train_returns,
            holdout_returns,
            minimum_samples=minimum,
            minimum_outcome_coverage=coverage,
        ),
        "conditions": condition_rows,
        "condition_candidates": review_candidates,
        "automatic_config_changes": False,
    }


def build_shadow_strategy_comparisons(
    rows: list[Any],
    *,
    minimum_samples: int = DEFAULT_MIN_SAMPLES,
) -> dict[str, Any]:
    """한 shadow 진입의 병렬 청산 전략을 동일 조건 집단으로 비교한다."""
    comparisons: dict[str, Any] = {}
    for strategy_name in SHADOW_STRATEGY_NAMES:
        adapted: list[Any] = []
        for row in rows:
            if not isinstance(row, dict):
                adapted.append(row)
                continue
            strategy_results = row.get("strategy_results")
            result = (
                strategy_results.get(strategy_name)
                if isinstance(strategy_results, dict)
                else None
            )
            return_percent = (
                result.get("return_percent")
                if isinstance(result, dict)
                else None
            )
            copied = dict(row)
            copied["samples"] = [{
                "interval": "15m",
                "return_percent": return_percent,
                "error": (
                    None if return_percent is not None
                    else str((result or {}).get("status", "MISSING_OUTCOME"))
                ),
            }]
            adapted.append(copied)
        analysis = build_observation_analysis(
            adapted,
            outcome_interval="15m",
            minimum_samples=minimum_samples,
        )
        comparisons[strategy_name] = {
            "input_row_count": analysis["input_row_count"],
            "bounded_row_count": analysis["bounded_row_count"],
            "independent_mint_cohort_count": analysis[
                "independent_mint_cohort_count"
            ],
            "eligible_outcome_count": analysis["eligible_outcome_count"],
            "excluded": analysis["excluded"],
            "split": analysis["split"],
            "overall": analysis["overall"],
            "conditions": analysis["conditions"],
            "condition_candidates": analysis["condition_candidates"],
        }
    return comparisons


def refresh_observation_analysis(
    *,
    observation_path: Path | None = None,
    analysis_path: Path | None = None,
    minimum_samples: int = DEFAULT_MIN_SAMPLES,
) -> dict[str, Any]:
    """현재 관찰 원장을 읽어 검토 전용 조건 후보 보고서를 원자 저장한다."""
    from src.observation_tracker import (
        OBSERVATION_PATH,
        empty_observations,
        ensure_observations_migrated,
    )

    source_path = observation_path or OBSERVATION_PATH
    target_path = analysis_path or ANALYSIS_PATH
    document = (
        ensure_observations_migrated()
        if observation_path is None
        else read_json(source_path, empty_observations())
    )
    if not isinstance(document, dict) or not isinstance(
        document.get("observations"), list
    ):
        raise RuntimeError("signal observation ledger is malformed")
    rows = document["observations"]
    research_rows = rows
    input_source = "signal_observations"
    if observation_path is None:
        from src.shadow_trade_ledger import (
            backfill_completed_shadow_trades,
            ensure_shadow_trades_migrated,
        )

        backfill_completed_shadow_trades(rows)
        shadow_document = ensure_shadow_trades_migrated()
        shadow_rows = shadow_document.get("trades")
        if not isinstance(shadow_rows, list):
            raise RuntimeError("shadow trade ledger is malformed")
        if shadow_rows:
            rows = shadow_rows
            input_source = "shadow_trades"
    report = build_observation_analysis(
        rows if isinstance(rows, list) else [],
        minimum_samples=minimum_samples,
    )
    report["input_source"] = input_source
    report["research_metrics"] = build_research_metrics(research_rows)
    if input_source == "shadow_trades":
        report["long_term_outcome_metrics"] = build_research_metrics(rows)
        report["strategy_comparisons"] = build_shadow_strategy_comparisons(
            rows,
            minimum_samples=minimum_samples,
        )
    report["generated_at"] = datetime.now(timezone.utc).isoformat()

    def mutate(current: dict[str, Any]) -> None:
        current.clear()
        current.update(report)

    _, saved = update_json(
        target_path,
        {"schema_version": 1, "version": 0},
        mutate,
    )
    return saved
