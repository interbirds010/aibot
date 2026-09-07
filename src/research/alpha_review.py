"""Alpha Discovery 결과를 원문 노출 없이 bounded 요약한다."""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from src.research.alpha_discovery import DEFAULT_OUTPUT_PATH
from src.state_store import read_json


FAMILIES = ("SMART_MONEY", "MOMENTUM")
STATUSES = ("PROMISING", "UNSTABLE", "INSUFFICIENT_DATA")
MAX_NEAR_MISSES = 10


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _number(value: Any) -> int | float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(result):
        return None
    return int(result) if result.is_integer() else result


def _candidate_rows(family: dict[str, Any]) -> Iterable[tuple[str, dict[str, Any]]]:
    for feature in _list(family.get("single_features")):
        for candidate in _list(_dict(feature).get("buckets")):
            if isinstance(candidate, dict):
                yield "single_feature", candidate
    for interaction in _list(family.get("interactions")):
        for candidate in _list(_dict(interaction).get("cells")):
            if isinstance(candidate, dict):
                yield "interaction", candidate


def _candidate_summary(
    analysis_type: str,
    candidate: dict[str, Any],
    *,
    primary_horizon: str,
    minimum_total: int,
    minimum_holdout: int,
    minimum_coverage: float,
) -> dict[str, Any]:
    unique = _dict(candidate.get("first_signal_per_mint"))
    horizon = _dict(_dict(unique.get("horizons")).get(primary_horizon))
    overall = _dict(horizon.get("overall"))
    train = _dict(horizon.get("train"))
    holdout = _dict(horizon.get("holdout"))
    labels = {
        str(key)[:100]: str(value)[:100]
        for key, value in sorted(_dict(candidate.get("labels")).items())
    }
    reasons = sorted({
        str(reason)[:100]
        for reason in _list(candidate.get("status_reasons"))
        if str(reason).strip()
    })
    sampled = int(_number(overall.get("sampled_count")) or 0)
    train_sampled = int(_number(train.get("sampled_count")) or 0)
    holdout_sampled = int(_number(holdout.get("sampled_count")) or 0)
    coverage = _number(overall.get("trackable_coverage_rate_percent"))
    sample_gate = sampled >= minimum_total and holdout_sampled >= minimum_holdout
    coverage_gate = coverage is not None and coverage >= minimum_coverage
    return {
        "analysis_type": analysis_type,
        "features": sorted(labels),
        "labels": labels,
        "status": (
            str(candidate.get("status"))
            if str(candidate.get("status")) in STATUSES else "UNKNOWN"
        ),
        "sampled_count": sampled,
        "train_sampled_count": train_sampled,
        "holdout_sampled_count": holdout_sampled,
        "trackable_coverage_rate_percent": coverage,
        "train_expectancy_percent": _number(train.get("expectancy_percent")),
        "holdout_expectancy_percent": _number(
            holdout.get("expectancy_percent")
        ),
        "train_profit_factor": _number(train.get("profit_factor")),
        "holdout_profit_factor": _number(holdout.get("profit_factor")),
        "train_profit_factor_above_one": (
            train.get("profit_factor_above_one")
            if isinstance(train.get("profit_factor_above_one"), bool) else None
        ),
        "holdout_profit_factor_above_one": (
            holdout.get("profit_factor_above_one")
            if isinstance(holdout.get("profit_factor_above_one"), bool) else None
        ),
        "holdout_expectancy_retention_ratio": _number(
            candidate.get("holdout_expectancy_retention_ratio")
        ),
        "sample_gate_met": sample_gate,
        "coverage_gate_met": coverage_gate,
        "gate_failures": reasons,
        "_stable_id": str(candidate.get("candidate_id") or "")[:300],
    }


def _descending(value: Any) -> float:
    number = _number(value)
    return -float(number) if number is not None else math.inf


def _near_miss_key(candidate: dict[str, Any]) -> tuple[Any, ...]:
    holdout_pf = candidate.get("holdout_profit_factor")
    if (
        holdout_pf is None
        and candidate.get("holdout_profit_factor_above_one") is True
    ):
        pf_key = -math.inf
    else:
        pf_key = _descending(holdout_pf)
    return (
        not candidate["sample_gate_met"],
        not candidate["coverage_gate_met"],
        len(candidate["gate_failures"]),
        _descending(candidate.get("holdout_expectancy_percent")),
        pf_key,
        candidate["_stable_id"],
    )


def build_alpha_review_summary(
    report: dict[str, Any], *, maximum_candidates: int = MAX_NEAR_MISSES,
) -> dict[str, Any]:
    """기존 status를 재판정하지 않고 후보와 실패 gate만 제한해 반환한다."""
    limit = max(0, min(MAX_NEAR_MISSES, int(maximum_candidates)))
    configuration = _dict(report.get("configuration"))
    primary_horizon = str(report.get("primary_horizon") or "60m")[:10]
    minimum_total = int(_number(configuration.get("minimum_total_sampled")) or 0)
    minimum_holdout = int(
        _number(configuration.get("minimum_holdout_sampled")) or 0
    )
    minimum_coverage = float(
        _number(configuration.get("minimum_trackable_coverage_percent")) or 0
    )
    family_summaries: dict[str, Any] = {}
    all_counts = Counter({status: 0 for status in STATUSES})
    all_failures: Counter[str] = Counter()
    for family_name in FAMILIES:
        family = _dict(_dict(report.get("families")).get(family_name))
        candidates = [
            _candidate_summary(
                kind,
                candidate,
                primary_horizon=primary_horizon,
                minimum_total=minimum_total,
                minimum_holdout=minimum_holdout,
                minimum_coverage=minimum_coverage,
            )
            for kind, candidate in _candidate_rows(family)
        ]
        counts = Counter(candidate["status"] for candidate in candidates)
        failures = Counter(
            reason for candidate in candidates for reason in candidate["gate_failures"]
        )
        near_misses = sorted(
            (
                candidate for candidate in candidates
                if candidate["status"] != "PROMISING"
            ),
            key=_near_miss_key,
        )[:limit]
        for candidate in near_misses:
            candidate.pop("_stable_id", None)
        family_summaries[family_name] = {
            "evaluated": len(candidates),
            **{status: counts[status] for status in STATUSES},
            "gate_failure_distribution": dict(sorted(failures.items())),
            "near_misses": near_misses,
        }
        all_counts.update({status: counts[status] for status in STATUSES})
        all_failures.update(failures)
    evaluated = sum(item["evaluated"] for item in family_summaries.values())
    return {
        "schema_version": 1,
        "primary_horizon": primary_horizon,
        "evaluated_hypothesis_count": evaluated,
        **{status: all_counts[status] for status in STATUSES},
        "ranked_count": int(
            _number(report.get("ranked_promising_candidate_count")) or 0
        ),
        "gate_failure_distribution": dict(sorted(all_failures.items())),
        "families": family_summaries,
        "near_miss_limit_per_family": limit,
        "multiple_testing_warning": True,
        "automatic_trading_changes": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Print bounded Alpha review")
    parser.add_argument("--input", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args(argv)
    report = read_json(args.input, {})
    summary = build_alpha_review_summary(report)
    print(
        "ALPHA_REVIEW_SUMMARY "
        + json.dumps(summary, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
