"""기존 Alpha 후보의 시간 안정성을 read-only로 진단한다."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from src.research.alpha_discovery import (
    DEFAULT_OUTPUT_PATH,
    _bucket_for_row,
    _chronological_split,
    _first_signal_per_mint,
    _interval_return,
    _prepare_rows,
)
from src.research.alpha_review import build_alpha_review_summary
from src.research_archive import DEFAULT_COHORT, RESEARCH_ARCHIVE_PATH, load_research_archive
from src.state_store import read_json


PRIMARY_HORIZON = "60m"
BOOTSTRAP_ITERATIONS = 1_000
ROLLING_WINDOWS = (30, 50, 100)
MAX_LATEST_WINDOWS = 5
MAX_TIME_BLOCKS = 24
MAX_OUTPUT_LINE_CHARS = 8_000
HISTOGRAM_CUTS = (-100, -50, -20, -10, 0, 10, 25, 50, 100, 200, 500)


@dataclass(frozen=True, slots=True)
class CandidateSpec:
    key: str
    feature: str
    label: str


CANDIDATES = (
    CandidateSpec("pair_age_below_300", "pair_age_seconds", "below_300"),
    CandidateSpec("volume_15000_25000", "volume_m5_usd", "15000_to_below_25000"),
    CandidateSpec("net_buys_50_or_more", "net_buys_m5", "50_or_more"),
    CandidateSpec("liquidity_10000_20000", "liquidity_usd", "10000_to_below_20000"),
    CandidateSpec("latency_3000_10000", "entry_latency_ms", "3000_to_below_10000"),
    CandidateSpec("safety_95_or_more", "safety_score", "95_or_more"),
    CandidateSpec("developer_supply_below_2", "developer_supply_percent", "below_2"),
)


def _rounded(value: float | None) -> float | None:
    return round(value, 4) if value is not None and math.isfinite(value) else None


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = (len(ordered) - 1) * fraction
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


def return_metrics(values: Iterable[float]) -> dict[str, Any]:
    rows = []
    for value in values:
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(number):
            rows.append(number)
    wins = [value for value in rows if value > 0]
    losses = [value for value in rows if value < 0]
    gross_win = math.fsum(wins)
    gross_loss = abs(math.fsum(losses))
    profit_factor = gross_win / gross_loss if gross_loss else None
    return {
        "count": len(rows),
        "mean_return_percent": _rounded(math.fsum(rows) / len(rows)) if rows else None,
        "expectancy_percent": _rounded(math.fsum(rows) / len(rows)) if rows else None,
        "median_return_percent": _rounded(_percentile(rows, 0.5)),
        "profit_factor": _rounded(profit_factor),
        "win_rate_percent": _rounded(len(wins) * 100 / len(rows)) if rows else None,
        "min_return_percent": _rounded(min(rows)) if rows else None,
        "max_return_percent": _rounded(max(rows)) if rows else None,
    }


def _profit_factor_above_one(metrics: dict[str, Any]) -> bool:
    profit_factor = metrics.get("profit_factor")
    if profit_factor is not None:
        return profit_factor > 1
    return (
        int(metrics.get("count", 0) or 0) > 0
        and (metrics.get("max_return_percent") or 0) > 0
        and (metrics.get("min_return_percent") or 0) >= 0
    )


def _returns(rows: Iterable[Any]) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = _interval_return(row, PRIMARY_HORIZON)
        if value is not None:
            values.append(float(value))
    return values


def _iso_timestamp(value: float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def _family_inventory(events: list[Any], unique: list[Any]) -> dict[str, Any]:
    sampled_events = [
        row for row in events
        if _interval_return(row, PRIMARY_HORIZON) is not None
    ]
    sampled_unique = [
        row for row in unique
        if _interval_return(row, PRIMARY_HORIZON) is not None
    ]
    return {
        "total_eligible_rows": len(events),
        "unique_mint_count": len(unique),
        "start_timestamp": _iso_timestamp(events[0].timestamp) if events else None,
        "end_timestamp": _iso_timestamp(events[-1].timestamp) if events else None,
        "successful_60m_event_count": len(sampled_events),
        "successful_60m_unique_mint_count": len(sampled_unique),
    }


def _equal_periods(rows: list[Any], parts: int) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for index in range(parts):
        start = math.floor(len(rows) * index / parts)
        end = math.floor(len(rows) * (index + 1) / parts)
        segment = rows[start:end]
        result.append({
            "period": index + 1,
            "start_epoch": _rounded(segment[0].timestamp) if segment else None,
            "end_epoch": _rounded(segment[-1].timestamp) if segment else None,
            **return_metrics(_returns(segment)),
        })
    return result


def rolling_performance(
    values: list[float], window: int, *, timestamps: list[float] | None = None,
) -> dict[str, Any] | None:
    if len(values) < window:
        return None
    windows = []
    for index in range(len(values) - window + 1):
        metrics = return_metrics(values[index:index + window])
        if timestamps is not None and len(timestamps) == len(values):
            metrics["ending_epoch"] = _rounded(timestamps[index + window - 1])
        windows.append(metrics)
    positive_flags = [
        (item["expectancy_percent"] or 0) > 0 for item in windows
    ]
    pf_flags = [
        _profit_factor_above_one(item)
        for item in windows
    ]
    both_flags = [
        positive and pf
        for positive, pf in zip(positive_flags, pf_flags)
    ]

    def longest_run(flags: list[bool]) -> int:
        longest = current = 0
        for flag in flags:
            current = current + 1 if flag else 0
            longest = max(longest, current)
        return longest

    return {
        "window": window,
        "window_count": len(windows),
        "positive_expectancy_window_count": sum(positive_flags),
        "positive_expectancy_window_rate_percent": _rounded(
            sum(positive_flags) * 100 / len(windows)
        ),
        "profit_factor_above_one_window_count": sum(pf_flags),
        "profit_factor_above_one_window_rate_percent": _rounded(
            sum(pf_flags) * 100 / len(windows)
        ),
        "positive_expectancy_and_pf_window_count": sum(both_flags),
        "positive_expectancy_and_pf_rate_percent": _rounded(
            sum(both_flags) * 100 / len(windows)
        ),
        "maximum_consecutive_positive_expectancy_windows": longest_run(
            positive_flags
        ),
        "maximum_consecutive_positive_expectancy_and_pf_windows": longest_run(
            both_flags
        ),
        "minimum_expectancy_percent": min(item["mean_return_percent"] for item in windows),
        "maximum_expectancy_percent": max(item["mean_return_percent"] for item in windows),
        "latest": windows[-MAX_LATEST_WINDOWS:],
    }


def expanding_performance(
    values: list[float], *, timestamps: list[float] | None = None,
) -> list[dict[str, Any]]:
    endpoints = list(range(50, len(values) + 1, 50))
    if values and (not endpoints or endpoints[-1] != len(values)):
        endpoints.append(len(values))
    result = []
    for end in endpoints:
        metrics = {"through": end, **return_metrics(values[:end])}
        if timestamps is not None and len(timestamps) == len(values):
            metrics["ending_epoch"] = _rounded(timestamps[end - 1])
        result.append(metrics)
    return result


def extreme_sensitivity(values: list[float]) -> dict[str, Any]:
    ordered = sorted(values)
    winners = [value for value in ordered if value > 0]
    top_five_target = max(1, math.ceil(len(ordered) * 0.05)) if ordered else 0

    def without_top_winners(count: int) -> list[float]:
        trimmed = list(ordered)
        removed = 0
        for winner in reversed(winners):
            if removed >= count:
                break
            trimmed.remove(winner)
            removed += 1
        return trimmed

    top_five_count = min(top_five_target, len(winners))
    trim_count = math.floor(len(ordered) * 0.05)
    if ordered and trim_count:
        lower = ordered[trim_count]
        upper = ordered[-trim_count - 1]
        winsorized = [min(upper, max(lower, value)) for value in ordered]
    else:
        winsorized = ordered
    return {
        **return_metrics(ordered),
        "top_1_removed_expectancy_percent": return_metrics(
            without_top_winners(1)
        )["mean_return_percent"],
        "top_3_removed_expectancy_percent": return_metrics(
            without_top_winners(3)
        )["mean_return_percent"],
        "top_5_percent_removed_count": top_five_count,
        "top_5_percent_removed_expectancy_percent": return_metrics(
            without_top_winners(top_five_count)
        )["mean_return_percent"],
        "bottom_1_removed_expectancy_percent": return_metrics(ordered[1:])["mean_return_percent"],
        "winsorized_mean_percent": return_metrics(winsorized)["mean_return_percent"],
    }


def distribution(values: list[float]) -> dict[str, Any]:
    near_zero = 1e-9
    labels = [f"below_{cut}" for cut in HISTOGRAM_CUTS] + [f"{HISTOGRAM_CUTS[-1]}_or_more"]
    histogram = Counter({label: 0 for label in labels})
    for value in values:
        label = labels[-1]
        for index, cut in enumerate(HISTOGRAM_CUTS):
            if value < cut:
                label = labels[index]
                break
        histogram[label] += 1
    return {
        "positive_count": sum(value > near_zero for value in values),
        "negative_count": sum(value < -near_zero for value in values),
        "zero_or_near_zero_count": sum(abs(value) <= near_zero for value in values),
        **{
            f"p{percent}": _rounded(_percentile(values, percent / 100))
            for percent in (10, 25, 50, 75, 90, 95)
        },
        "histogram": {key: value for key, value in histogram.items() if value},
    }


def drawdown_and_streak(values: list[float]) -> dict[str, Any]:
    cumulative = 0.0
    peak = 0.0
    maximum_drawdown = 0.0
    loss_streak = win_streak = current_losses = current_wins = 0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        maximum_drawdown = max(maximum_drawdown, peak - cumulative)
        current_losses = current_losses + 1 if value < 0 else 0
        current_wins = current_wins + 1 if value > 0 else 0
        loss_streak = max(loss_streak, current_losses)
        win_streak = max(win_streak, current_wins)
    return {
        "maximum_additive_drawdown_points": _rounded(maximum_drawdown),
        "maximum_consecutive_losses": loss_streak,
        "maximum_consecutive_wins": win_streak,
        "worst_5_returns": sorted(values)[:5],
        "best_5_returns": sorted(values, reverse=True)[:5],
    }


def _split_metrics(rows: list[Any], holdout_fraction: float) -> dict[str, Any]:
    train, holdout = _chronological_split(rows, holdout_fraction)
    return {
        "train": return_metrics(_returns(train)),
        "holdout": return_metrics(_returns(holdout)),
    }


def holdout_concentration(rows: list[Any]) -> dict[str, Any]:
    _, holdout = _chronological_split(rows, 0.20)
    values = _returns(holdout)
    total = math.fsum(values)
    winners = sorted((value for value in values if value > 0), reverse=True)
    gross_positive = math.fsum(winners)

    def net_contribution(count: int) -> float | None:
        return _rounded(math.fsum(winners[:count]) * 100 / total) if total > 0 else None

    def gross_contribution(count: int) -> float | None:
        return (
            _rounded(math.fsum(winners[:count]) * 100 / gross_positive)
            if gross_positive > 0 else None
        )

    top_ten_count = max(1, math.ceil(len(values) * 0.10)) if values else 0
    return {
        "holdout_sampled_count": len(values),
        "holdout_total_return_points": _rounded(total),
        "holdout_gross_positive_return_points": _rounded(gross_positive),
        "net_result_contribution_is_interpretable": total > 0,
        "top_1_winner_contribution_percent": net_contribution(1),
        "top_3_winner_contribution_percent": net_contribution(3),
        "top_10_percent_trade_count": top_ten_count,
        "top_10_percent_winner_contribution_percent": net_contribution(
            top_ten_count
        ),
        "top_1_gross_positive_contribution_percent": gross_contribution(1),
        "top_3_gross_positive_contribution_percent": gross_contribution(3),
        "top_10_percent_gross_positive_contribution_percent": (
            gross_contribution(top_ten_count)
        ),
    }


def bootstrap_diagnostic(rows: list[Any], *, candidate_key: str) -> dict[str, Any]:
    _, holdout = _chronological_split(rows, 0.20)
    all_values = _returns(rows)
    holdout_values = _returns(holdout)
    if not holdout_values or not all_values:
        return {"iterations": BOOTSTRAP_ITERATIONS, "available": False}
    seed = int(hashlib.sha256(candidate_key.encode()).hexdigest()[:16], 16)
    rng = random.Random(seed)
    means = sorted(
        math.fsum(rng.choice(holdout_values) for _ in holdout_values) / len(holdout_values)
        for _ in range(BOOTSTRAP_ITERATIONS)
    )
    observed = math.fsum(holdout_values) / len(holdout_values)
    null_means = []
    for _ in range(BOOTSTRAP_ITERATIONS):
        sample = rng.sample(all_values, min(len(holdout_values), len(all_values)))
        null_means.append(math.fsum(sample) / len(sample))
    return {
        "iterations": BOOTSTRAP_ITERATIONS,
        "available": True,
        "holdout_mean_percent": _rounded(observed),
        "bootstrap_mean_ci95": [
            _rounded(_percentile(means, 0.025)),
            _rounded(_percentile(means, 0.975)),
        ],
        "permutation_null_probability_ge_observed": _rounded(
            sum(value >= observed for value in null_means) / len(null_means)
        ),
    }


def _candidate_rows(rows: list[Any], spec: CandidateSpec) -> list[Any]:
    return [row for row in rows if _bucket_for_row(row, spec.feature) == spec.label]


def _candidate_verdict(report: dict[str, Any]) -> str:
    sensitivity = report["extreme_sensitivity"]
    concentration = report["holdout_concentration"]
    rolling = [value for value in report["rolling"].values() if value]
    latest_stable = sum(
        item["latest"]
        and all(
            (window["mean_return_percent"] or 0) > 0
            and _profit_factor_above_one(window)
            for window in item["latest"]
        )
        for item in rolling
    )
    multi = list(report["pseudo_holdouts"].values())
    cross_period = bool(multi) and all(
        (item["train"]["mean_return_percent"] or 0) > 0
        and (item["holdout"]["mean_return_percent"] or 0) > 0
        and _profit_factor_above_one(item["train"])
        and _profit_factor_above_one(item["holdout"])
        for item in multi
    )
    robust_extremes = (
        (sensitivity["top_1_removed_expectancy_percent"] or 0) > 0
        and (sensitivity["top_5_percent_removed_expectancy_percent"] or 0) > 0
    )
    concentrated = (
        concentration["top_3_winner_contribution_percent"] is not None
        and concentration["top_3_winner_contribution_percent"] >= 70
    )
    adjusted_pass = report["multiple_testing_diagnostic"].get(
        "bonferroni_adjusted_pass"
    )
    if cross_period and robust_extremes and not concentrated and adjusted_pass:
        return "CROSS_PERIOD_STABLE_SIGNAL"
    if robust_extremes and not concentrated and latest_stable >= 2 and adjusted_pass:
        return "RECENT_REGIME_SIGNAL"
    return "NO_EVIDENCE_OF_ALPHA"


def pairwise_overlap(candidate_mints: dict[str, set[str]]) -> list[dict[str, Any]]:
    keys = sorted(candidate_mints)
    result = []
    for left_index, left in enumerate(keys):
        for right in keys[left_index + 1:]:
            intersection = candidate_mints[left] & candidate_mints[right]
            union = candidate_mints[left] | candidate_mints[right]
            result.append({
                "left": left,
                "right": right,
                "left_mint_count": len(candidate_mints[left]),
                "right_mint_count": len(candidate_mints[right]),
                "overlap_count": len(intersection),
                "jaccard": _rounded(len(intersection) / len(union)) if union else None,
            })
    return result


def _utc_time_blocks(rows: list[Any]) -> dict[str, Any]:
    """UTC 2-day blocks summarize regime drift without unbounded output."""
    by_block: dict[str, list[float]] = {}
    for row in rows:
        value = _interval_return(row, PRIMARY_HORIZON)
        if value is None:
            continue
        block_start = datetime.fromtimestamp(
            math.floor(row.timestamp / 172_800) * 172_800, timezone.utc
        ).date()
        by_block.setdefault(block_start.isoformat(), []).append(float(value))
    blocks = [
        {"utc_two_day_block_start": day, **return_metrics(values)}
        for day, values in sorted(by_block.items())
    ]
    positive = sum(
        (block["mean_return_percent"] or 0) > 0
        and _profit_factor_above_one(block)
        for block in blocks
    )
    return {
        "total_block_count": len(blocks),
        "positive_block_count": positive,
        "omitted_older_block_count": max(0, len(blocks) - MAX_TIME_BLOCKS),
        "latest_blocks": blocks[-MAX_TIME_BLOCKS:],
    }


def build_alpha_regime_review(raw_rows: list[Any], alpha_report: dict[str, Any]) -> dict[str, Any]:
    counts = alpha_report.get("candidate_counts")
    counts = counts if isinstance(counts, dict) else {}
    hypothesis_count = sum(
        int(counts.get(key, 0) or 0)
        for key in ("PROMISING", "UNSTABLE", "INSUFFICIENT_DATA")
    )
    prepared, excluded = _prepare_rows(raw_rows, maximum_rows=10_000, cohort=DEFAULT_COHORT)
    momentum_events = [row for row in prepared if row.family == "MOMENTUM"]
    smart_events = [row for row in prepared if row.family == "SMART_MONEY"]
    momentum_unique, momentum_ambiguous = _first_signal_per_mint(momentum_events)
    smart_unique, smart_ambiguous = _first_signal_per_mint(smart_events)
    momentum_sampled = [row for row in momentum_unique if _interval_return(row, PRIMARY_HORIZON) is not None]
    smart_sampled = [row for row in smart_unique if _interval_return(row, PRIMARY_HORIZON) is not None]

    candidates: dict[str, Any] = {}
    sampled_mints: dict[str, set[str]] = {}
    for spec in CANDIDATES:
        rows = _candidate_rows(momentum_unique, spec)
        sampled = [row for row in rows if _interval_return(row, PRIMARY_HORIZON) is not None]
        values = _returns(sampled)
        timestamps = [row.timestamp for row in sampled]
        _, sampled_holdout = _chronological_split(sampled, 0.20)
        multiple_testing = bootstrap_diagnostic(sampled, candidate_key=spec.key)
        raw_probability = multiple_testing.get(
            "permutation_null_probability_ge_observed"
        )
        adjusted_probability = (
            min(1.0, float(raw_probability) * hypothesis_count)
            if raw_probability is not None and hypothesis_count else None
        )
        multiple_testing.update({
            "hypothesis_count": hypothesis_count,
            "bonferroni_adjusted_probability": _rounded(adjusted_probability),
            "bonferroni_adjusted_pass": (
                adjusted_probability <= 0.05
                if adjusted_probability is not None else False
            ),
            "selected_after_production_screening": True,
        })
        report = {
            "feature": spec.feature,
            "label": spec.label,
            "candidate_mint_count": len({row.mint for row in rows}),
            "sampled_mint_count": len({row.mint for row in sampled}),
            "overall": return_metrics(values),
            "rolling": {
                str(window): rolling_performance(
                    values, window, timestamps=timestamps
                )
                for window in ROLLING_WINDOWS
            },
            "expanding": expanding_performance(
                values, timestamps=timestamps
            ),
            "extreme_sensitivity": extreme_sensitivity(values),
            "holdout_extreme_sensitivity": extreme_sensitivity(
                _returns(sampled_holdout)
            ),
            "distribution": distribution(values),
            "drawdown_and_streak": drawdown_and_streak(values),
            "time_quartiles": _equal_periods(sampled, 4),
            "utc_two_day_blocks": _utc_time_blocks(sampled),
            "holdout_concentration": holdout_concentration(sampled),
            "pseudo_holdouts": {
                "60_40": _split_metrics(sampled, 0.40),
                "70_30": _split_metrics(sampled, 0.30),
                "80_20": _split_metrics(sampled, 0.20),
            },
            "multiple_testing_diagnostic": multiple_testing,
        }
        report["diagnostic_verdict"] = _candidate_verdict(report)
        candidates[spec.key] = report
        sampled_mints[spec.key] = {row.mint for row in sampled}

    candidate_verdicts = Counter(
        report["diagnostic_verdict"] for report in candidates.values()
    )
    latest_quartile = _equal_periods(momentum_sampled, 4)[-1]
    if candidate_verdicts["CROSS_PERIOD_STABLE_SIGNAL"]:
        momentum_verdict = "CROSS_PERIOD_STABLE_SIGNAL"
    elif (
        candidate_verdicts["RECENT_REGIME_SIGNAL"] >= 2
        and (latest_quartile["mean_return_percent"] or 0) > 0
        and _profit_factor_above_one(latest_quartile)
    ):
        momentum_verdict = "RECENT_REGIME_SIGNAL"
    else:
        momentum_verdict = "NO_EVIDENCE_OF_ALPHA"

    production_review = build_alpha_review_summary(alpha_report)
    production_families = production_review.get("families")
    production_families = (
        production_families if isinstance(production_families, dict) else {}
    )
    smart_candidate_review = production_families.get("SMART_MONEY")
    smart_candidate_review = (
        smart_candidate_review if isinstance(smart_candidate_review, dict) else {}
    )
    smart_verdict = (
        "NO_EVIDENCE_OF_ALPHA"
        if not int(smart_candidate_review.get("PROMISING", 0) or 0)
        and not int(smart_candidate_review.get("UNSTABLE", 0) or 0)
        else "REVIEW_REQUIRED"
    )
    return {
        "schema_version": 1,
        "basis": {
            "cohort": DEFAULT_COHORT,
            "horizon": PRIMARY_HORIZON,
            "unique_mint_rule": "first_signal_per_family_and_mint",
            "input_row_count": len(raw_rows),
            "prepared_row_count": len(prepared),
            "excluded": excluded,
            "production_alpha_candidate_counts": {
                key: int(counts.get(key, 0) or 0)
                for key in ("PROMISING", "UNSTABLE", "INSUFFICIENT_DATA")
            },
            "automatic_trading_changes": False,
        },
        "smart_money": {
            "inventory": _family_inventory(smart_events, smart_unique),
            "unique_mint_count": len(smart_unique),
            "unique_sampled_count": len(smart_sampled),
            "ambiguous_first_signal_mint_count": smart_ambiguous,
            "overall": return_metrics(_returns(smart_sampled)),
            "chronological_80_20": _split_metrics(smart_sampled, 0.20),
            "production_candidate_review": smart_candidate_review,
            "verdict": smart_verdict,
        },
        "momentum": {
            "inventory": _family_inventory(momentum_events, momentum_unique),
            "unique_mint_count": len(momentum_unique),
            "unique_sampled_count": len(momentum_sampled),
            "ambiguous_first_signal_mint_count": momentum_ambiguous,
            "overall": return_metrics(_returns(momentum_sampled)),
            "quartiles": _equal_periods(momentum_sampled, 4),
            "quintiles": _equal_periods(momentum_sampled, 5),
            "early_middle_recent": _equal_periods(momentum_sampled, 3),
            "utc_two_day_blocks": _utc_time_blocks(momentum_sampled),
            "candidates": candidates,
            "candidate_overlap": pairwise_overlap(sampled_mints),
            "candidate_verdict_counts": dict(sorted(candidate_verdicts.items())),
            "verdict": momentum_verdict,
        },
        "multiple_testing": {
            "evaluated_hypothesis_count": hypothesis_count,
            "production_unstable_count": int(counts.get("UNSTABLE", 0) or 0),
            "reviewed_existing_candidate_count": len(CANDIDATES),
            "new_candidates_added": 0,
            "correction_applied": False,
            "diagnostic": "deterministic_bootstrap_ci_and_exchangeability_resampling",
        },
        "read_only": True,
    }


def _emit_record(prefix: str, payload: Any) -> None:
    """GitHub/SSH 로그의 단일 행 제한보다 작게 JSON record를 출력한다."""
    encoded = json.dumps(
        payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )
    available = max(1, MAX_OUTPUT_LINE_CHARS - len(prefix) - 40)
    if len(encoded) <= available:
        print(f"{prefix} {encoded}")
        return
    chunks = [encoded[index:index + available] for index in range(0, len(encoded), available)]
    for index, chunk in enumerate(chunks, 1):
        print(f"{prefix}_CHUNK part={index}/{len(chunks)} data={chunk}")


def _recent_positive_block_run(blocks: list[dict[str, Any]]) -> int:
    run = 0
    for block in reversed(blocks):
        if (
            (block.get("expectancy_percent") or 0) > 0
            and _profit_factor_above_one(block)
        ):
            run += 1
        else:
            break
    return run


def emit_alpha_regime_report(result: dict[str, Any]) -> None:
    """계산 결과를 A-K bounded section으로만 전달한다."""
    smart = result["smart_money"]
    momentum = result["momentum"]
    candidates = momentum["candidates"]

    _emit_record("ALPHA_REGIME_SECTION_A_DATASET", {
        "basis": result["basis"],
        "SMART_MONEY": smart["inventory"],
        "MOMENTUM": momentum["inventory"],
    })

    smart_review = smart["production_candidate_review"]
    _emit_record("ALPHA_REGIME_SECTION_B_SMART_MONEY", {
        "verdict": smart["verdict"],
        "overall": smart["overall"],
        "chronological_80_20": smart["chronological_80_20"],
        "candidate_counts": {
            key: smart_review.get(key)
            for key in ("evaluated", "PROMISING", "UNSTABLE", "INSUFFICIENT_DATA")
        },
        "gate_failure_distribution": smart_review.get(
            "gate_failure_distribution", {}
        ),
    })
    for index, evidence in enumerate(smart_review.get("near_misses", []), 1):
        _emit_record(
            "ALPHA_REGIME_SECTION_B_SMART_EVIDENCE",
            {"rank": index, **evidence},
        )

    _emit_record("ALPHA_REGIME_SECTION_C_MOMENTUM_OVERALL", momentum["overall"])
    for period in momentum["quartiles"]:
        _emit_record("ALPHA_REGIME_SECTION_C_MOMENTUM_QUARTILE", period)
    for period in momentum["quintiles"]:
        _emit_record("ALPHA_REGIME_SECTION_C_MOMENTUM_QUINTILE", period)

    for key, candidate in candidates.items():
        identity = {"candidate": key, "feature": candidate["feature"], "label": candidate["label"]}
        for window, summary in candidate["rolling"].items():
            if summary is None:
                _emit_record(
                    "ALPHA_REGIME_SECTION_D_ROLLING",
                    {**identity, "window": int(window), "available": False},
                )
                continue
            _emit_record("ALPHA_REGIME_SECTION_D_ROLLING", {
                **identity,
                "available": True,
                "window": int(window),
                "window_count": summary["window_count"],
                "latest": summary["latest"][-1],
                "positive_expectancy_window_rate_percent": summary[
                    "positive_expectancy_window_rate_percent"
                ],
                "profit_factor_above_one_window_rate_percent": summary[
                    "profit_factor_above_one_window_rate_percent"
                ],
                "maximum_consecutive_positive_expectancy_windows": summary[
                    "maximum_consecutive_positive_expectancy_windows"
                ],
                "maximum_consecutive_positive_expectancy_and_pf_windows": summary[
                    "maximum_consecutive_positive_expectancy_and_pf_windows"
                ],
                "minimum_expectancy_percent": summary["minimum_expectancy_percent"],
                "maximum_expectancy_percent": summary["maximum_expectancy_percent"],
                "overlapping_windows": True,
            })
        _emit_record("ALPHA_REGIME_SECTION_E_EXPANDING", {
            **identity,
            "checkpoints": candidate["expanding"],
        })
        _emit_record("ALPHA_REGIME_SECTION_F_EXTREME_SENSITIVITY", {
            **identity,
            "overall": candidate["extreme_sensitivity"],
            "production_80_20_holdout": candidate[
                "holdout_extreme_sensitivity"
            ],
        })
        _emit_record("ALPHA_REGIME_SECTION_G_DRAWDOWN_STREAK", {
            **identity,
            **candidate["drawdown_and_streak"],
        })
        _emit_record("ALPHA_REGIME_SECTION_I_HOLDOUT_CONCENTRATION", {
            **identity,
            **candidate["holdout_concentration"],
        })
        _emit_record("ALPHA_REGIME_SECTION_J_MULTI_SPLIT", {
            **identity,
            "splits": candidate["pseudo_holdouts"],
        })
        time_blocks = candidate["utc_two_day_blocks"]
        latest_blocks = time_blocks["latest_blocks"]
        _emit_record("ALPHA_REGIME_SECTION_K_MULTIPLE_TESTING_TIME_BLOCK", {
            **identity,
            "multiple_testing": candidate["multiple_testing_diagnostic"],
            "diagnostic_verdict": candidate["diagnostic_verdict"],
            "time_blocks": {
                "total_block_count": time_blocks["total_block_count"],
                "positive_block_count": time_blocks["positive_block_count"],
                "omitted_older_block_count": time_blocks[
                    "omitted_older_block_count"
                ],
                "latest_positive_block_run": _recent_positive_block_run(
                    latest_blocks
                ),
                "latest_five_blocks": latest_blocks[-5:],
            },
        })

    for overlap in momentum["candidate_overlap"]:
        _emit_record("ALPHA_REGIME_SECTION_H_OVERLAP", overlap)

    _emit_record("ALPHA_REGIME_SECTION_K_MULTIPLE_TESTING_SUMMARY", {
        **result["multiple_testing"],
        "momentum_time_blocks": momentum["utc_two_day_blocks"],
        "candidate_verdict_counts": momentum["candidate_verdict_counts"],
    })
    _emit_record("ALPHA_REGIME_FINAL_VERDICT", {
        "SMART_MONEY": smart["verdict"],
        "MOMENTUM": momentum["verdict"],
        "read_only": result["read_only"],
    })


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run read-only Alpha regime review")
    parser.add_argument("--archive", type=Path, default=RESEARCH_ARCHIVE_PATH)
    parser.add_argument("--alpha-report", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args(argv)
    rows, _ = load_research_archive(
        archive_path=args.archive,
        tracking_profile=DEFAULT_COHORT,
        maximum_rows=10_000,
    )
    alpha_report = read_json(args.alpha_report, {})
    result = build_alpha_regime_review(rows, alpha_report)
    emit_alpha_regime_report(result)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
