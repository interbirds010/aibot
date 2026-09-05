"""사전 정의한 signal feature bucket의 시간순 성과를 분석한다."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.observation_analysis import (
    canonical_missing_outcome_reason,
    performance_metrics,
)
from src.research_archive import (
    ARCHIVE_SCHEMA_VERSION,
    DEFAULT_COHORT,
    RESEARCH_ARCHIVE_PATH,
    load_research_archive,
)
from src.state_store import read_json, update_json


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OBSERVATION_PATH = ROOT / "data" / "signal_observations.json"
DEFAULT_ARCHIVE_PATH = RESEARCH_ARCHIVE_PATH
DEFAULT_OUTPUT_PATH = ROOT / "data" / "alpha_discovery.json"

SCHEMA_VERSION = 2
MIN_SUPPORTED_OBSERVATION_SCHEMA_VERSION = 1
MAX_SUPPORTED_OBSERVATION_SCHEMA_VERSION = 5
BUCKET_VERSION = "alpha_v2_fixed_1"
ANALYSIS_HORIZONS = ("5m", "15m", "30m", "60m")
PRIMARY_HORIZON = "60m"
HORIZON_SECONDS = {"1m": 60, "3m": 180, "5m": 300, "15m": 900,
                   "30m": 1_800, "60m": 3_600}
DEFAULT_HOLDOUT_FRACTION = 0.20
MAX_ANALYSIS_ROWS = 10_000
MAX_RANKED_CANDIDATES = 100
MIN_TOTAL_SAMPLED = 50
MIN_HOLDOUT_SAMPLED = 15
MIN_TRACKABLE_COVERAGE_PERCENT = 80.0
MIN_EXPECTANCY_RETENTION_RATIO = 0.25
UNTRACKABLE_QUOTE_STATUSES = frozenset({
    "NO_ROUTE", "NOT_REQUESTED", "SIZE_UNUSABLE", "PROCESSING_FAILED",
})


@dataclass(frozen=True, slots=True)
class BucketSpec:
    cuts: tuple[float, ...]
    labels: tuple[str, ...]

    def __post_init__(self) -> None:
        if len(self.labels) != len(self.cuts) + 1:
            raise ValueError("bucket labels must contain one more item than cuts")
        if tuple(sorted(self.cuts)) != self.cuts:
            raise ValueError("bucket cuts must be sorted")


@dataclass(frozen=True, slots=True)
class FeatureSpec:
    source_path: tuple[str, ...]
    buckets: BucketSpec
    scale: float = 1.0
    minimum: float | None = 0.0
    maximum: float | None = None


@dataclass(frozen=True, slots=True)
class PreparedRow:
    raw: dict[str, Any]
    family: str
    mint: str
    timestamp: float
    stable_id: str
    digest: str

    @property
    def sort_key(self) -> tuple[float, str, str]:
        return (self.timestamp, self.stable_id, self.digest)


def _buckets(cuts: tuple[float, ...], labels: tuple[str, ...]) -> BucketSpec:
    return BucketSpec(cuts=cuts, labels=labels)


WHALE_PAID_BUCKETS = _buckets(
    (1.0, 1.5, 2.0, 3.0, 5.0),
    ("below_1", "1_to_below_1.5", "1.5_to_below_2", "2_to_below_3",
     "3_to_below_5", "5_or_more"),
)
SAFETY_SCORE_BUCKETS = _buckets(
    (55.0, 70.0, 85.0, 95.0),
    ("below_55", "55_to_below_70", "70_to_below_85",
     "85_to_below_95", "95_or_more"),
)
LIQUIDITY_BUCKETS = _buckets(
    (7_500.0, 10_000.0, 20_000.0, 50_000.0, 100_000.0),
    ("below_7500", "7500_to_below_10000", "10000_to_below_20000",
     "20000_to_below_50000", "50000_to_below_100000", "100000_or_more"),
)
LATENCY_BUCKETS = _buckets(
    (1_000.0, 3_000.0, 10_000.0, 30_000.0),
    ("below_1000", "1000_to_below_3000", "3000_to_below_10000",
     "10000_to_below_30000", "30000_or_more"),
)
PRICE_IMPACT_BUCKETS = _buckets(
    (0.25, 0.5, 1.0, 1.5),
    ("below_0.25", "0.25_to_below_0.5", "0.5_to_below_1",
     "1_to_below_1.5", "1.5_or_more"),
)
DEVELOPER_SUPPLY_BUCKETS = _buckets(
    (2.0, 5.0, 10.0, 20.0),
    ("below_2", "2_to_below_5", "5_to_below_10",
     "10_to_below_20", "20_or_more"),
)
LP_LOCKED_BUCKETS = _buckets(
    (40.0, 80.0, 90.0),
    ("below_40", "40_to_below_80", "80_to_below_90", "90_or_more"),
)
COPY_GAP_BUCKETS = _buckets(
    (-5.0, -2.0, -1.0, 0.0, 1.0, 2.0, 5.0),
    ("below_-5", "-5_to_below_-2", "-2_to_below_-1", "-1_to_below_0",
     "0_to_below_1", "1_to_below_2", "2_to_below_5", "5_or_more"),
)
MOMENTUM_SCORE_BUCKETS = _buckets(
    (55.0, 70.0, 85.0, 90.0, 95.0, 100.0),
    ("below_55", "55_to_below_70", "70_to_below_85", "85_to_below_90",
     "90_to_below_95", "95_to_below_100", "100_or_more"),
)
VOLUME_BUCKETS = _buckets(
    (10_000.0, 15_000.0, 25_000.0, 50_000.0, 100_000.0),
    ("below_10000", "10000_to_below_15000", "15000_to_below_25000",
     "25000_to_below_50000", "50000_to_below_100000", "100000_or_more"),
)
COUNT_BUCKETS = _buckets(
    (5.0, 10.0, 15.0, 25.0, 50.0),
    ("below_5", "5_to_below_10", "10_to_below_15", "15_to_below_25",
     "25_to_below_50", "50_or_more"),
)
RATIO_BUCKETS = _buckets(
    (1.2, 1.5, 1.8, 2.5, 4.0),
    ("below_1.2", "1.2_to_below_1.5", "1.5_to_below_1.8",
     "1.8_to_below_2.5", "2.5_to_below_4", "4_or_more"),
)
PAIR_AGE_BUCKETS = _buckets(
    (300.0, 900.0, 1_800.0, 3_600.0, 10_800.0),
    ("below_300", "300_to_below_900", "900_to_below_1800",
     "1800_to_below_3600", "3600_to_below_10800", "10800_or_more"),
)
UNKNOWN_WHALE_BUCKETS = _buckets(
    (1.0, 2.0, 3.0, 4.0),
    ("0", "1", "2", "3", "4_or_more"),
)


SMART_MONEY_FEATURES: dict[str, FeatureSpec] = {
    "whale_paid_sol": FeatureSpec(
        ("discovery_metadata", "whale_paid_lamports"), WHALE_PAID_BUCKETS,
        scale=1 / 1_000_000_000,
    ),
    "safety_score": FeatureSpec(("safety_score",), SAFETY_SCORE_BUCKETS),
    "developer_supply_percent": FeatureSpec(
        ("safety_metrics", "developer_supply_percent"),
        DEVELOPER_SUPPLY_BUCKETS,
    ),
    "lp_locked_percent": FeatureSpec(
        ("safety_metrics", "lp_locked_percent"), LP_LOCKED_BUCKETS,
    ),
    "liquidity_usd": FeatureSpec(
        ("safety_metrics", "liquidity_usd"), LIQUIDITY_BUCKETS,
    ),
    "entry_price_impact_pct": FeatureSpec(
        ("entry_price_impact_pct",), PRICE_IMPACT_BUCKETS,
    ),
    "exit_price_impact_pct": FeatureSpec(
        ("exit_price_impact_pct",), PRICE_IMPACT_BUCKETS,
    ),
    "entry_latency_ms": FeatureSpec(("entry_latency_ms",), LATENCY_BUCKETS),
    "copy_price_gap_pct": FeatureSpec(
        ("copy_price_gap_pct",), COPY_GAP_BUCKETS, minimum=None,
    ),
}

MOMENTUM_FEATURES: dict[str, FeatureSpec] = {
    "dex_momentum_score": FeatureSpec(
        ("dex_momentum_score",), MOMENTUM_SCORE_BUCKETS,
    ),
    "volume_m5_usd": FeatureSpec(
        ("momentum_metrics", "volume_m5_usd"), VOLUME_BUCKETS,
    ),
    "buys_m5": FeatureSpec(("momentum_metrics", "buys_m5"), COUNT_BUCKETS),
    "sells_m5": FeatureSpec(("momentum_metrics", "sells_m5"), COUNT_BUCKETS),
    "net_buys_m5": FeatureSpec(
        ("momentum_metrics", "net_buys_m5"), COUNT_BUCKETS, minimum=None,
    ),
    "buy_sell_ratio_m5": FeatureSpec(
        ("momentum_metrics", "buy_sell_ratio_m5"), RATIO_BUCKETS,
    ),
    "liquidity_usd": FeatureSpec(
        ("momentum_metrics", "liquidity_usd"), LIQUIDITY_BUCKETS,
    ),
    "pair_age_seconds": FeatureSpec(
        ("momentum_metrics", "pair_age_seconds"), PAIR_AGE_BUCKETS,
    ),
    "unknown_whale_count": FeatureSpec(
        ("momentum_metrics", "unknown_whale_count"), UNKNOWN_WHALE_BUCKETS,
    ),
    "safety_score": FeatureSpec(("safety_score",), SAFETY_SCORE_BUCKETS),
    "lp_locked_percent": FeatureSpec(
        ("safety_metrics", "lp_locked_percent"), LP_LOCKED_BUCKETS,
    ),
    "developer_supply_percent": FeatureSpec(
        ("safety_metrics", "developer_supply_percent"),
        DEVELOPER_SUPPLY_BUCKETS,
    ),
    "entry_price_impact_pct": FeatureSpec(
        ("entry_price_impact_pct",), PRICE_IMPACT_BUCKETS,
    ),
    "exit_price_impact_pct": FeatureSpec(
        ("exit_price_impact_pct",), PRICE_IMPACT_BUCKETS,
    ),
    "entry_latency_ms": FeatureSpec(("entry_latency_ms",), LATENCY_BUCKETS),
}

FEATURES_BY_FAMILY = {
    "SMART_MONEY": SMART_MONEY_FEATURES,
    "MOMENTUM": MOMENTUM_FEATURES,
}
INTERACTIONS_BY_FAMILY = {
    "SMART_MONEY": (
        ("whale_paid_sol", "liquidity_usd"),
        ("whale_paid_sol", "entry_latency_ms"),
        ("liquidity_usd", "safety_score"),
    ),
    "MOMENTUM": (
        ("volume_m5_usd", "buy_sell_ratio_m5"),
        ("volume_m5_usd", "pair_age_seconds"),
        ("buy_sell_ratio_m5", "unknown_whale_count"),
        ("liquidity_usd", "pair_age_seconds"),
    ),
}


def _finite_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _timestamp_value(value: Any) -> float | None:
    number = _finite_number(value)
    if number is not None:
        return number if number >= 0 else None
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    result = parsed.timestamp()
    return result if math.isfinite(result) and result >= 0 else None


def _signal_timestamp(row: dict[str, Any]) -> float | None:
    for value in (
        row.get("signal_detected_at"), row.get("started_at_epoch"),
        row.get("started_at"),
    ):
        timestamp = _timestamp_value(value)
        if timestamp is not None:
            return timestamp
    return None


def _signal_family(row: dict[str, Any]) -> str | None:
    signal_type = str(row.get("signal_type") or "").strip().upper()
    if signal_type in FEATURES_BY_FAMILY:
        return signal_type
    route_type = str(row.get("route_type") or "").strip().upper()
    return {"A": "SMART_MONEY", "B": "MOMENTUM"}.get(route_type)


def _signal_identity_digest(row: dict[str, Any]) -> str:
    identity = {
        key: row.get(key)
        for key in (
            "observation_id",
            "mint",
            "source_signature",
            "source_wallet",
            "signal_type",
            "route_type",
            "signal_detected_at",
            "started_at_epoch",
            "started_at",
        )
    }
    serialized = json.dumps(
        identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _prepare_rows(
    rows: list[Any], *, maximum_rows: int, cohort: str,
) -> tuple[list[PreparedRow], dict[str, int]]:
    prepared: list[PreparedRow] = []
    excluded = {
        "invalid_row": 0,
        "unknown_signal_family": 0,
        "missing_or_invalid_timestamp": 0,
        "missing_mint": 0,
        "outside_latest_window": 0,
        "outside_cohort": 0,
    }
    for item in rows:
        if not isinstance(item, dict):
            excluded["invalid_row"] += 1
            continue
        family = _signal_family(item)
        if family is None:
            excluded["unknown_signal_family"] += 1
            continue
        timestamp = _signal_timestamp(item)
        if timestamp is None:
            excluded["missing_or_invalid_timestamp"] += 1
            continue
        mint = str(item.get("mint") or "").strip()
        if not mint:
            excluded["missing_mint"] += 1
            continue
        if item.get("tracking_profile") != cohort:
            excluded["outside_cohort"] += 1
            continue
        digest = _signal_identity_digest(item)
        stable_id = str(item.get("observation_id") or "").strip() or digest
        prepared.append(PreparedRow(
            raw=item,
            family=family,
            mint=mint,
            timestamp=timestamp,
            stable_id=stable_id,
            digest=digest,
        ))
    prepared.sort(key=lambda row: row.sort_key)
    if len(prepared) > maximum_rows:
        excluded["outside_latest_window"] = len(prepared) - maximum_rows
        prepared = prepared[-maximum_rows:]
    return prepared, excluded


def _nested_value(row: dict[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = row
    for part in path:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _feature_value(row: PreparedRow, feature: str) -> float | None:
    spec = FEATURES_BY_FAMILY[row.family][feature]
    number = _finite_number(_nested_value(row.raw, spec.source_path))
    if number is None:
        return None
    number *= spec.scale
    if spec.minimum is not None and number < spec.minimum:
        return None
    if spec.maximum is not None and number > spec.maximum:
        return None
    return number


def assign_bucket(family: str, feature: str, value: Any) -> str | None:
    """정규화된 feature 값을 고정된 lower-inclusive bucket에 배정한다."""
    normalized_family = str(family).strip().upper()
    specs = FEATURES_BY_FAMILY.get(normalized_family)
    if specs is None or feature not in specs:
        raise ValueError("unsupported alpha discovery family or feature")
    number = _finite_number(value)
    spec = specs[feature]
    if number is None:
        return None
    if spec.minimum is not None and number < spec.minimum:
        return None
    if spec.maximum is not None and number > spec.maximum:
        return None
    for index, upper in enumerate(spec.buckets.cuts):
        if number < upper:
            return spec.buckets.labels[index]
    return spec.buckets.labels[-1]


def _bucket_for_row(row: PreparedRow, feature: str) -> str | None:
    value = _feature_value(row, feature)
    return assign_bucket(row.family, feature, value) if value is not None else None


def _interval_return(row: PreparedRow, horizon: str) -> float | None:
    samples = row.raw.get("samples")
    if not isinstance(samples, list):
        return None
    for sample in samples:
        if not isinstance(sample, dict) or str(sample.get("interval")) != horizon:
            continue
        return _finite_number(sample.get("return_percent"))
    return None


def _outcome_trackable(row: PreparedRow) -> bool:
    quote_status = str(row.raw.get("quote_status") or "").strip().upper()
    return quote_status not in UNTRACKABLE_QUOTE_STATUSES


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    scale = max(abs(value) for value in values)
    if scale == 0:
        return 0.0
    result = scale * math.fsum(value / scale for value in values) / len(values)
    return result if math.isfinite(result) else None


def _rounded(value: float | None) -> float | None:
    return round(value, 4) if value is not None and math.isfinite(value) else None


def _sampled_excursion(
    row: PreparedRow, horizon: str,
) -> tuple[float | None, float | None]:
    limit = HORIZON_SECONDS[horizon]
    returns = [
        value
        for label, seconds in HORIZON_SECONDS.items()
        if seconds <= limit
        and (value := _interval_return(row, label)) is not None
    ]
    if not returns:
        return None, None
    return max([0.0, *returns]), min([0.0, *returns])


def _metrics(rows: list[PreparedRow], horizon: str) -> dict[str, Any]:
    raw_returns = [_interval_return(row, horizon) for row in rows]
    trackable_flags = [_outcome_trackable(row) for row in rows]
    returns = [
        outcome if trackable else None
        for outcome, trackable in zip(raw_returns, trackable_flags)
    ]
    performance = performance_metrics(returns, minimum_samples=1)
    signal_count = len(rows)
    sampled_count = performance["sample_count"]
    trackable_count = sum(trackable_flags)
    inconsistent_sample_count = sum(
        outcome is not None and not trackable
        for outcome, trackable in zip(raw_returns, trackable_flags)
    )
    missing_reasons: dict[str, int] = {}
    for row, outcome in zip(rows, returns):
        if outcome is not None:
            continue
        samples = row.raw.get("samples")
        sample = next((
            item for item in samples
            if isinstance(item, dict) and str(item.get("interval")) == horizon
        ), None) if isinstance(samples, list) else None
        reason = canonical_missing_outcome_reason(row.raw, sample)
        missing_reasons[reason] = missing_reasons.get(reason, 0) + 1
    excursions = [
        _sampled_excursion(row, horizon)
        for row, trackable in zip(rows, trackable_flags)
        if trackable
    ]
    mfe_values = [value for value, _ in excursions if value is not None]
    mae_values = [value for _, value in excursions if value is not None]
    return {
        "signal_count": signal_count,
        "trackable_count": trackable_count,
        "sampled_count": sampled_count,
        "inconsistent_untrackable_sample_count": inconsistent_sample_count,
        "missing_count": signal_count - sampled_count,
        "missing_reasons": dict(sorted(missing_reasons.items())),
        "raw_coverage_rate_percent": (
            round(sampled_count / signal_count * 100, 4)
            if signal_count else None
        ),
        "trackable_coverage_rate_percent": (
            round(sampled_count / trackable_count * 100, 4)
            if trackable_count else None
        ),
        "win_rate_percent": performance["win_rate_percent"],
        "mean_return_percent": performance["mean_roi_percent"],
        "median_return_percent": performance["median_roi_percent"],
        "average_win_percent": performance["average_win_percent"],
        "average_loss_percent": performance["average_loss_percent"],
        "expectancy_percent": performance["expectancy_percent"],
        "profit_factor": performance["profit_factor"],
        "profit_factor_above_one": (
            performance["profit_factor"] > 1
            if performance["profit_factor"] is not None
            else (
                sampled_count > 0
                and performance["average_win_percent"] is not None
                and performance["average_loss_percent"] is None
            )
        ),
        "profit_factor_interpretation": (
            "POSITIVE_WITH_NO_LOSSES"
            if performance["profit_factor"] is None
            and sampled_count > 0
            and performance["average_win_percent"] is not None
            and performance["average_loss_percent"] is None
            else "FINITE" if performance["profit_factor"] is not None
            else "UNDEFINED"
        ),
        "sampled_mfe_count": len(mfe_values),
        "sampled_mfe_percent": _rounded(_mean(mfe_values)),
        "sampled_mae_count": len(mae_values),
        "sampled_mae_percent": _rounded(_mean(mae_values)),
    }


def _chronological_split(
    rows: list[PreparedRow], holdout_fraction: float,
) -> tuple[list[PreparedRow], list[PreparedRow]]:
    ordered = sorted(rows, key=lambda row: row.sort_key)
    if len(ordered) < 2:
        return ordered, []
    holdout_count = max(1, math.ceil(len(ordered) * holdout_fraction))
    holdout_count = min(len(ordered) - 1, holdout_count)
    split_at = len(ordered) - holdout_count
    while (
        split_at > 0
        and ordered[split_at - 1].sort_key == ordered[split_at].sort_key
    ):
        split_at -= 1
    return ordered[:split_at], ordered[split_at:]


def _first_signal_per_mint(
    rows: list[PreparedRow],
) -> tuple[list[PreparedRow], int]:
    by_mint: dict[str, list[PreparedRow]] = {}
    for row in rows:
        by_mint.setdefault(row.mint, []).append(row)
    first: list[PreparedRow] = []
    ambiguous_count = 0
    for mint in sorted(by_mint):
        ordered = sorted(by_mint[mint], key=lambda item: item.sort_key)
        first_key = ordered[0].sort_key
        tied = [row for row in ordered if row.sort_key == first_key]
        if len(tied) > 1 and any(row.raw != tied[0].raw for row in tied[1:]):
            ambiguous_count += 1
            continue
        first.append(tied[0])
    first.sort(key=lambda row: row.sort_key)
    return first, ambiguous_count


def _analyze_view(
    total: list[PreparedRow],
    train: list[PreparedRow],
    holdout: list[PreparedRow],
    *,
    primary_horizon: str,
) -> dict[str, Any]:
    horizons = {
        horizon: {
            "overall": _metrics(total, horizon),
            "train": _metrics(train, horizon),
            "holdout": _metrics(holdout, horizon),
        }
        for horizon in ANALYSIS_HORIZONS
    }
    primary = horizons[primary_horizon]
    train_mints = {row.mint for row in train}
    holdout_mints = {row.mint for row in holdout}
    return {
        "split": {
            "method": "chronological_family_cohort",
            "train_signal_count": len(train),
            "holdout_signal_count": len(holdout),
            "cross_split_mint_count": len(train_mints & holdout_mints),
        },
        "horizons": horizons,
        "train_sample_count": primary["train"]["sampled_count"],
        "holdout_sample_count": primary["holdout"]["sampled_count"],
        "train_expectancy_percent": primary["train"]["expectancy_percent"],
        "holdout_expectancy_percent": primary["holdout"]["expectancy_percent"],
        "train_profit_factor": primary["train"]["profit_factor"],
        "holdout_profit_factor": primary["holdout"]["profit_factor"],
    }


def _profit_factor_above_one(metrics: dict[str, Any]) -> bool:
    return metrics["profit_factor_above_one"] is True


def _candidate_status(
    unique_view: dict[str, Any], *, primary_horizon: str,
) -> tuple[str, list[str], float | None]:
    primary = unique_view["horizons"][primary_horizon]
    overall = primary["overall"]
    train = primary["train"]
    holdout = primary["holdout"]
    insufficient: list[str] = []
    if overall["sampled_count"] < MIN_TOTAL_SAMPLED:
        insufficient.append("TOTAL_SAMPLED_BELOW_MINIMUM")
    if holdout["sampled_count"] < MIN_HOLDOUT_SAMPLED:
        insufficient.append("HOLDOUT_SAMPLED_BELOW_MINIMUM")
    coverage = overall["trackable_coverage_rate_percent"]
    if coverage is None:
        insufficient.append("NO_TRACKABLE_OUTCOMES")
    elif coverage < MIN_TRACKABLE_COVERAGE_PERCENT:
        insufficient.append("TRACKABLE_COVERAGE_BELOW_MINIMUM")
    if insufficient:
        return "INSUFFICIENT_DATA", insufficient, None

    unstable: list[str] = []
    train_expectancy = train["expectancy_percent"]
    holdout_expectancy = holdout["expectancy_percent"]
    if train_expectancy is None or train_expectancy <= 0:
        unstable.append("TRAIN_EXPECTANCY_NOT_POSITIVE")
    if holdout_expectancy is None or holdout_expectancy <= 0:
        unstable.append("HOLDOUT_EXPECTANCY_NOT_POSITIVE")
    if not _profit_factor_above_one(train):
        unstable.append("TRAIN_PROFIT_FACTOR_NOT_ABOVE_ONE")
    if not _profit_factor_above_one(holdout):
        unstable.append("HOLDOUT_PROFIT_FACTOR_NOT_ABOVE_ONE")
    retention: float | None = None
    if (
        train_expectancy is not None and train_expectancy > 0
        and holdout_expectancy is not None
    ):
        retention = holdout_expectancy / train_expectancy
        if retention < MIN_EXPECTANCY_RETENTION_RATIO:
            unstable.append("HOLDOUT_EXPECTANCY_COLLAPSE")
    if unstable:
        return "UNSTABLE", unstable, _rounded(retention)
    return "PROMISING", [], _rounded(retention)


def _filter_bucket(
    rows: list[PreparedRow], feature: str, label: str,
) -> list[PreparedRow]:
    return [row for row in rows if _bucket_for_row(row, feature) == label]


def _candidate_record(
    *,
    candidate_id: str,
    labels: dict[str, str],
    event_rows: tuple[list[PreparedRow], list[PreparedRow], list[PreparedRow]],
    unique_rows: tuple[list[PreparedRow], list[PreparedRow], list[PreparedRow]],
    primary_horizon: str,
) -> dict[str, Any]:
    event_view = _analyze_view(
        *event_rows, primary_horizon=primary_horizon,
    )
    unique_view = _analyze_view(
        *unique_rows, primary_horizon=primary_horizon,
    )
    status, reasons, retention = _candidate_status(
        unique_view, primary_horizon=primary_horizon,
    )
    return {
        "candidate_id": candidate_id,
        "labels": labels,
        "status": status,
        "status_reasons": reasons,
        "holdout_expectancy_retention_ratio": retention,
        "event_level": event_view,
        "first_signal_per_mint": unique_view,
    }


def _single_feature_analysis(
    family: str,
    feature: str,
    event_split: tuple[list[PreparedRow], list[PreparedRow], list[PreparedRow]],
    unique_split: tuple[list[PreparedRow], list[PreparedRow], list[PreparedRow]],
    *,
    primary_horizon: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    spec = FEATURES_BY_FAMILY[family][feature]
    event_present = sum(_feature_value(row, feature) is not None for row in event_split[0])
    unique_present = sum(_feature_value(row, feature) is not None for row in unique_split[0])
    buckets: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    for order, label in enumerate(spec.buckets.labels):
        record = _candidate_record(
            candidate_id=f"{family}:single:{feature}:{label}",
            labels={feature: label},
            event_rows=tuple(
                _filter_bucket(rows, feature, label) for rows in event_split
            ),
            unique_rows=tuple(
                _filter_bucket(rows, feature, label) for rows in unique_split
            ),
            primary_horizon=primary_horizon,
        )
        record["bucket_order"] = order
        buckets.append(record)
        candidates.append(record)
    return {
        "feature": feature,
        "source_path": ".".join(spec.source_path),
        "event_feature_present_count": event_present,
        "event_feature_missing_count": len(event_split[0]) - event_present,
        "unique_mint_feature_present_count": unique_present,
        "unique_mint_feature_missing_count": len(unique_split[0]) - unique_present,
        "buckets": buckets,
    }, candidates


def _interaction_key(
    row: PreparedRow, first: str, second: str,
) -> tuple[str, str] | None:
    first_label = _bucket_for_row(row, first)
    second_label = _bucket_for_row(row, second)
    if first_label is None or second_label is None:
        return None
    return first_label, second_label


def _filter_interaction(
    rows: list[PreparedRow], first: str, second: str, key: tuple[str, str],
) -> list[PreparedRow]:
    return [row for row in rows if _interaction_key(row, first, second) == key]


def _interaction_analysis(
    family: str,
    first: str,
    second: str,
    event_split: tuple[list[PreparedRow], list[PreparedRow], list[PreparedRow]],
    unique_split: tuple[list[PreparedRow], list[PreparedRow], list[PreparedRow]],
    *,
    primary_horizon: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    event_keys = {
        key for row in event_split[0]
        if (key := _interaction_key(row, first, second)) is not None
    }
    unique_keys = {
        key for row in unique_split[0]
        if (key := _interaction_key(row, first, second)) is not None
    }
    first_labels = FEATURES_BY_FAMILY[family][first].buckets.labels
    second_labels = FEATURES_BY_FAMILY[family][second].buckets.labels
    order = {
        (left, right): (left_index, right_index)
        for left_index, left in enumerate(first_labels)
        for right_index, right in enumerate(second_labels)
    }
    cells: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    for key in sorted(event_keys | unique_keys, key=lambda item: order[item]):
        record = _candidate_record(
            candidate_id=f"{family}:interaction:{first}:{key[0]}:{second}:{key[1]}",
            labels={first: key[0], second: key[1]},
            event_rows=tuple(
                _filter_interaction(rows, first, second, key)
                for rows in event_split
            ),
            unique_rows=tuple(
                _filter_interaction(rows, first, second, key)
                for rows in unique_split
            ),
            primary_horizon=primary_horizon,
        )
        cells.append(record)
        candidates.append(record)
    return {
        "features": [first, second],
        "possible_cell_count": len(first_labels) * len(second_labels),
        "observed_cell_count": len(cells),
        "cells": cells,
    }, candidates


def _family_analysis(
    family: str,
    rows: list[PreparedRow],
    *,
    primary_horizon: str,
    holdout_fraction: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    event_rows = sorted(rows, key=lambda row: row.sort_key)
    event_train, event_holdout = _chronological_split(
        event_rows, holdout_fraction,
    )
    unique_rows, ambiguous_first_mints = _first_signal_per_mint(event_rows)
    unique_train, unique_holdout = _chronological_split(
        unique_rows, holdout_fraction,
    )
    event_split = (event_rows, event_train, event_holdout)
    unique_split = (unique_rows, unique_train, unique_holdout)

    single_features: list[dict[str, Any]] = []
    interactions: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    for feature in sorted(FEATURES_BY_FAMILY[family]):
        result, feature_candidates = _single_feature_analysis(
            family, feature, event_split, unique_split,
            primary_horizon=primary_horizon,
        )
        single_features.append(result)
        candidates.extend(feature_candidates)
    for first, second in INTERACTIONS_BY_FAMILY[family]:
        result, interaction_candidates = _interaction_analysis(
            family, first, second, event_split, unique_split,
            primary_horizon=primary_horizon,
        )
        interactions.append(result)
        candidates.extend(interaction_candidates)

    event_primary = _metrics(event_rows, primary_horizon)
    unique_primary = _metrics(unique_rows, primary_horizon)
    return {
        "signal_type": family,
        "summary": {
            "event_signal_count": len(event_rows),
            "unique_mint_count": len(unique_rows),
            "ambiguous_first_signal_mint_count": ambiguous_first_mints,
            "repeated_mint_event_count": len(event_rows) - len(unique_rows),
            "event_primary_outcome": event_primary,
            "unique_mint_primary_outcome": unique_primary,
            "event_split": {
                "train_count": len(event_train),
                "holdout_count": len(event_holdout),
            },
            "unique_mint_split": {
                "train_count": len(unique_train),
                "holdout_count": len(unique_holdout),
            },
        },
        "single_features": single_features,
        "interactions": interactions,
    }, candidates


def _bucket_configuration() -> dict[str, Any]:
    return {
        family: {
            feature: {
                "source_path": ".".join(spec.source_path),
                "scale": spec.scale,
                "cuts": list(spec.buckets.cuts),
                "labels": list(spec.buckets.labels),
                "boundary_rule": "lower_inclusive_upper_exclusive",
            }
            for feature, spec in sorted(features.items())
        }
        for family, features in FEATURES_BY_FAMILY.items()
    }


def _ranked_candidate(
    family: str, kind: str, record: dict[str, Any], primary_horizon: str,
) -> dict[str, Any]:
    primary = record["first_signal_per_mint"]["horizons"][primary_horizon]
    train = primary["train"]
    holdout = primary["holdout"]
    overall = primary["overall"]
    return {
        "candidate_id": record["candidate_id"],
        "family": family,
        "analysis_type": kind,
        "labels": record["labels"],
        "status": record["status"],
        "sampled_count": overall["sampled_count"],
        "trackable_coverage_rate_percent": overall[
            "trackable_coverage_rate_percent"
        ],
        "train_expectancy_percent": train["expectancy_percent"],
        "holdout_expectancy_percent": holdout["expectancy_percent"],
        "train_profit_factor": train["profit_factor"],
        "holdout_profit_factor": holdout["profit_factor"],
        "train_profit_factor_above_one": train["profit_factor_above_one"],
        "holdout_profit_factor_above_one": holdout["profit_factor_above_one"],
        "holdout_expectancy_retention_ratio": record[
            "holdout_expectancy_retention_ratio"
        ],
    }


def _ranking_key(candidate: dict[str, Any]) -> tuple[Any, ...]:
    holdout_expectancy = candidate["holdout_expectancy_percent"]
    holdout_factor = candidate["holdout_profit_factor"]
    factor_score = (
        float(holdout_factor) if holdout_factor is not None else float("inf")
    )
    coverage = candidate["trackable_coverage_rate_percent"] or 0.0
    retention = candidate["holdout_expectancy_retention_ratio"] or 0.0
    return (
        -(1 if holdout_expectancy is not None and holdout_expectancy > 0 else 0),
        -factor_score,
        -candidate["sampled_count"],
        -coverage,
        -retention,
        candidate["candidate_id"],
    )


def build_alpha_discovery(
    rows: list[Any],
    *,
    primary_horizon: str = PRIMARY_HORIZON,
    holdout_fraction: float = DEFAULT_HOLDOUT_FRACTION,
    maximum_rows: int = MAX_ANALYSIS_ROWS,
    generated_at: str | None = None,
    cohort: str = DEFAULT_COHORT,
) -> dict[str, Any]:
    """외부 상태를 변경하지 않고 deterministic Alpha Discovery를 계산한다."""
    if not isinstance(rows, list):
        raise TypeError("alpha discovery rows must be a list")
    if primary_horizon not in ANALYSIS_HORIZONS:
        raise ValueError("primary_horizon must be 5m, 15m, 30m, or 60m")
    fraction = _finite_number(holdout_fraction)
    if fraction is None or not 0 < fraction < 1:
        raise ValueError("holdout_fraction must be finite and between 0 and 1")
    try:
        limit = int(maximum_rows)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("maximum_rows must be an integer") from exc
    if limit < 1 or limit > MAX_ANALYSIS_ROWS:
        raise ValueError(f"maximum_rows must be between 1 and {MAX_ANALYSIS_ROWS}")

    cohort_name = str(cohort).strip()
    if not cohort_name:
        raise ValueError("alpha discovery cohort must not be empty")
    prepared, excluded = _prepare_rows(
        rows, maximum_rows=limit, cohort=cohort_name,
    )
    cohort_row_count = sum(
        isinstance(row, dict) and row.get("tracking_profile") == cohort_name
        for row in rows
    )
    families: dict[str, Any] = {}
    candidate_records: list[tuple[str, str, dict[str, Any]]] = []
    for family in ("SMART_MONEY", "MOMENTUM"):
        family_rows = [row for row in prepared if row.family == family]
        analysis, candidates = _family_analysis(
            family,
            family_rows,
            primary_horizon=primary_horizon,
            holdout_fraction=fraction,
        )
        families[family] = analysis
        single_ids = {
            bucket["candidate_id"]
            for feature in analysis["single_features"]
            for bucket in feature["buckets"]
        }
        candidate_records.extend(
            (family, "single_feature" if candidate["candidate_id"] in single_ids
             else "interaction", candidate)
            for candidate in candidates
        )

    counts = {"INSUFFICIENT_DATA": 0, "UNSTABLE": 0, "PROMISING": 0}
    promising: list[dict[str, Any]] = []
    for family, kind, record in candidate_records:
        counts[record["status"]] += 1
        if record["status"] == "PROMISING":
            promising.append(
                _ranked_candidate(family, kind, record, primary_horizon)
            )
    promising.sort(key=_ranking_key)
    top_candidates = promising[:MAX_RANKED_CANDIDATES]
    for rank, candidate in enumerate(top_candidates, start=1):
        candidate["rank"] = rank

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "primary_horizon": primary_horizon,
        "analyzed_horizons": list(ANALYSIS_HORIZONS),
        "configuration": {
            "bucket_version": BUCKET_VERSION,
            "holdout_fraction": fraction,
            "maximum_analysis_rows": limit,
            "maximum_ranked_candidates": MAX_RANKED_CANDIDATES,
            "split_method": "chronological_by_signal_timestamp",
            "same_timestamp_tie_break": (
                "observation_id_then_signal_identity_digest"
            ),
            "candidate_status_basis": "first_signal_per_mint_primary_horizon",
            "minimum_total_sampled": MIN_TOTAL_SAMPLED,
            "minimum_holdout_sampled": MIN_HOLDOUT_SAMPLED,
            "minimum_trackable_coverage_percent": (
                MIN_TRACKABLE_COVERAGE_PERCENT
            ),
            "minimum_expectancy_retention_ratio": (
                MIN_EXPECTANCY_RETENTION_RATIO
            ),
            "outcome_basis": "jupiter_executable_reverse_quote",
            "excursion_basis": "scheduled_horizon_samples_up_to_outcome",
            "unique_mint_rule": "first_signal_per_family_and_mint",
            "unique_mint_split_order": "first_signal_then_chronological_split",
            "unique_mint_cross_split_leakage_allowed": False,
            "hypothesis_policy": "predefined_buckets_and_interactions_only",
            "ranking_priority": [
                "positive_holdout_expectancy",
                "holdout_profit_factor",
                "sampled_count",
                "trackable_coverage",
                "train_holdout_expectancy_retention",
            ],
            "multiple_testing_warning": True,
            "automatic_trading_changes": False,
            "cohort": cohort_name,
            "bucket_definitions": _bucket_configuration(),
            "interactions": {
                family: [list(pair) for pair in pairs]
                for family, pairs in INTERACTIONS_BY_FAMILY.items()
            },
        },
        "input_summary": {
            "input_row_count": len(rows),
            "cohort": cohort_name,
            "cohort_row_count": cohort_row_count,
            "analyzed_row_count": len(prepared),
            "maximum_rows": limit,
            "excluded": excluded,
        },
        "families": families,
        "candidate_counts": counts,
        "ranked_promising_candidate_count": len(top_candidates),
        "unranked_promising_candidate_count": max(
            0, len(promising) - len(top_candidates)
        ),
        "top_candidates": top_candidates,
        "automatic_trading_changes": False,
    }


def refresh_alpha_discovery(
    *,
    observation_path: Path | None = None,
    output_path: Path | None = None,
) -> dict[str, Any]:
    """관찰 원장은 읽기만 하고 Alpha Discovery 결과만 원자 저장한다."""
    source = observation_path or DEFAULT_ARCHIVE_PATH
    target = output_path or DEFAULT_OUTPUT_PATH
    if source.resolve() == target.resolve():
        raise ValueError("alpha discovery output must differ from observation input")
    if source.exists() and target.exists() and source.samefile(target):
        raise ValueError("alpha discovery output must differ from observation input")
    archive_metadata: dict[str, Any] | None = None
    if (
        source.is_dir()
        or source.suffix.lower() != ".json"
        or source.resolve() == DEFAULT_ARCHIVE_PATH.resolve()
    ):
        rows, archive_metadata = load_research_archive(
            archive_path=source,
            tracking_profile=DEFAULT_COHORT,
            maximum_rows=MAX_ANALYSIS_ROWS,
        )
        document = {
            "schema_version": ARCHIVE_SCHEMA_VERSION,
            "version": None,
            "observations": rows,
        }
        source_type = "research_archive"
    else:
        document = read_json(source, {"schema_version": 0, "version": 0,
                                      "observations": []})
        source_type = "explicit_observation_ledger"
    if source.exists() and source_type == "explicit_observation_ledger":
        schema_version = document.get("schema_version")
        version = document.get("version")
        if isinstance(schema_version, bool) or not isinstance(schema_version, int):
            raise RuntimeError("signal observation schema_version is malformed")
        if not (
            MIN_SUPPORTED_OBSERVATION_SCHEMA_VERSION
            <= schema_version
            <= MAX_SUPPORTED_OBSERVATION_SCHEMA_VERSION
        ):
            raise RuntimeError("signal observation schema is unsupported")
        if isinstance(version, bool) or not isinstance(version, int) or version < 0:
            raise RuntimeError("signal observation version is malformed")
    rows = document.get("observations")
    if not isinstance(rows, list):
        raise RuntimeError("signal observation ledger is malformed")
    report = build_alpha_discovery(
        rows, generated_at=datetime.now(timezone.utc).isoformat(),
    )
    report["input_summary"]["source_schema_version"] = document.get(
        "schema_version"
    )
    report["input_summary"]["source_version"] = document.get("version")
    try:
        source_path = str(source.resolve().relative_to(ROOT.resolve())).replace("\\", "/")
    except ValueError:
        source_path = source.name
    report["input_summary"].update({
        "source_type": source_type,
        "source_path": source_path,
        "source_row_count": (
            archive_metadata["archive_total_rows"]
            if archive_metadata is not None else len(rows)
        ),
    })
    if archive_metadata is not None:
        report["input_summary"].update(archive_metadata)
        report["input_summary"]["excluded"]["outside_latest_window"] = max(
            0,
            int(archive_metadata["cohort_row_count"])
            - int(archive_metadata["loaded_cohort_row_count"]),
        )

    def mutate(current: dict[str, Any]) -> None:
        current.clear()
        current.update(report)

    _, saved = update_json(
        target, {"schema_version": SCHEMA_VERSION, "version": 0}, mutate,
    )
    return saved


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Alpha Discovery V2")
    parser.add_argument("--input", type=Path, default=DEFAULT_ARCHIVE_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args(argv)
    report = refresh_alpha_discovery(
        observation_path=args.input, output_path=args.output,
    )
    families = report["families"]
    signals = sum(
        item["summary"]["event_signal_count"] for item in families.values()
    )
    trackable = sum(
        item["summary"]["event_primary_outcome"]["trackable_count"]
        for item in families.values()
    )
    unique_mints = sum(
        item["summary"]["unique_mint_count"] for item in families.values()
    )
    counts = report["candidate_counts"]
    print(f"signals analyzed: {signals}")
    print(f"trackable outcomes ({report['primary_horizon']}): {trackable}")
    print(f"unique mints by family: {unique_mints}")
    print(f"promising candidates: {counts['PROMISING']}")
    print(f"insufficient candidates: {counts['INSUFFICIENT_DATA']}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
