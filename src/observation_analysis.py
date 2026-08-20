"""관찰 원장만으로 조건별 성과와 시간순 holdout 통계를 계산한다."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.state_store import read_json, update_json


ABSOLUTE_MAX_ANALYSIS_ROWS = 5_000
DEFAULT_MAX_ANALYSIS_ROWS = 1_000
DEFAULT_MIN_SAMPLES = 20
DEFAULT_HOLDOUT_FRACTION = 0.20
DEFAULT_OUTCOME_INTERVAL = "15m"
DEFAULT_MIN_OUTCOME_COVERAGE = 0.80
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
    samples = row.get("samples")
    if not isinstance(samples, list):
        return None
    for sample in samples:
        if not isinstance(sample, dict) or str(sample.get("interval")) != interval:
            continue
        return _finite_number(sample.get("return_percent"))
    return None


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
        "downside_p10_roi_percent": _rounded(downside_tail),
        "worst_decile_mean_roi_percent": _rounded(worst_decile),
    }


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
    if outcome_interval not in {"1m", "5m", "15m"}:
        raise ValueError("outcome_interval must be 1m, 5m, or 15m")
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
    report = build_observation_analysis(
        rows if isinstance(rows, list) else [],
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
