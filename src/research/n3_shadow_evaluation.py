"""종료된 N3 대조 코호트의 필터 효과를 읽기 전용으로 평가한다."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import math
import random
from typing import Any


LAMPORTS_PER_SOL = 1_000_000_000
MAX_COHORT_TRADES = 408
KST = timezone(timedelta(hours=9))
DEFAULT_SAMPLE_GATES = {
    "completed_control": 200,
    "valid_hits": 40,
    "valid_non_hits": 40,
    "control_winners": 20,
    "active_kst_days": 14,
}


def _utc_timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("trade timestamp must be an ISO UTC string")
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("trade timestamp must be an ISO UTC string") from exc
    if timestamp.utcoffset() != timedelta(0):
        raise ValueError("trade timestamp must have a UTC offset")
    return timestamp


def _rows(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """형식과 크기를 검증하고 입력 원장을 변경하지 않는다."""
    if not isinstance(trades, list) or len(trades) > MAX_COHORT_TRADES:
        raise ValueError("closed cohort must be a list of at most 408 trades")
    rows = []
    identities = set()
    for trade in trades:
        if not isinstance(trade, dict):
            raise ValueError("trade must be a dictionary")
        identity = trade.get("position_id")
        if not isinstance(identity, str) or not identity or identity in identities:
            raise ValueError("position_id must be unique and nonempty")
        identities.add(identity)
        for key in ("buy_event_seq", "entry_cost_lamports", "realized_pnl_lamports"):
            if type(trade.get(key)) is not int:
                raise ValueError(f"{key} must be an integer")
        if trade["entry_cost_lamports"] <= 0:
            raise ValueError("entry_cost_lamports must be positive")
        if trade.get("family") not in {"MOMENTUM", "SMART_MONEY"}:
            raise ValueError("unsupported trade family")
        if trade.get("would_skip") is not None and type(trade["would_skip"]) is not bool:
            raise ValueError("would_skip must be boolean or None")
        signal = _utc_timestamp(trade.get("signal_timestamp"))
        closed = _utc_timestamp(trade.get("closed_at"))
        if closed < signal:
            raise ValueError("closed_at precedes signal_timestamp")
        cost = trade["entry_cost_lamports"]
        pnl = trade["realized_pnl_lamports"]
        normalized = pnl / cost
        if not math.isfinite(normalized):
            raise ValueError("normalized trade return must be finite")
        rows.append({
            **trade,
            "_signal": signal,
            "_closed": closed,
            "_day": signal.astimezone(KST).date().isoformat(),
            "_actual": pnl,
            "_normalized": normalized,
        })
    return rows


def _counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "completed_control": len(rows),
        "valid_hits": sum(row.get("would_skip") is True for row in rows),
        "valid_non_hits": sum(row.get("would_skip") is False for row in rows),
        "control_winners": sum(row["realized_pnl_lamports"] > 0 for row in rows),
        "active_kst_days": len({row["_day"] for row in rows}),
    }


def sample_counts(trades: list[dict[str, Any]]) -> dict[str, int]:
    """모든 종료 Control을 포함한 사전 고정 표본 조건을 계산한다."""
    return _counts(_rows(trades))


def _filter_counts(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """승패별 적중과 모든 Control을 분모로 한 제외율을 공개한다."""
    hits = [row for row in rows if row.get("would_skip") is True]
    losers = sum(row["realized_pnl_lamports"] < 0 for row in rows)
    winners = sum(row["realized_pnl_lamports"] > 0 for row in rows)
    hit_losers = sum(row["realized_pnl_lamports"] < 0 for row in hits)
    hit_winners = sum(row["realized_pnl_lamports"] > 0 for row in hits)
    return {
        "hit_count": len(hits),
        "hit_losers": hit_losers,
        "hit_winners": hit_winners,
        "total_losers": losers,
        "total_winners": winners,
        "hit_coverage": len(hits) / len(rows) if rows else None,
        "valid_label_coverage": sum(row.get("would_skip") is not None for row in rows) / len(rows) if rows else None,
        "loser_exclusion_rate": hit_losers / losers if losers else None,
        "winner_exclusion_rate": hit_winners / winners if winners else None,
        "rate_denominator": "all_completed_control",
    }


def _metrics(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: (row["_closed"], row["buy_event_seq"], row["position_id"]))
    values = [row[key] for row in ordered]
    wins = [value for value in values if value > 0]
    losses = [-value for value in values if value < 0]
    gross_profit = math.fsum(wins)
    gross_loss = math.fsum(losses)
    total = math.fsum(values)
    equity = peak = drawdown = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    result = {
        "trade_count": len(values),
        "winner_count": len(wins),
        "win_rate": len(wins) / len(values) if values else None,
        "expectancy": total / len(values) if values else None,
        "total_pnl": total,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "profit_factor": gross_profit / gross_loss if gross_loss else None,
        "profit_factor_unbounded": gross_profit > 0 and gross_loss == 0,
        "profit_factor_above_one": gross_profit > gross_loss,
        "max_drawdown": drawdown,
        "drawdown_order": "closed_at_then_buy_event_seq",
    }
    if key == "_actual":
        result["total_pnl_sol"] = total / LAMPORTS_PER_SOL
        result["expectancy_sol"] = result["expectancy"] / LAMPORTS_PER_SOL if values else None
        result["max_drawdown_sol"] = drawdown / LAMPORTS_PER_SOL
    return result


def _effects(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    hits = [row for row in rows if row.get("would_skip") is True]
    valid_count = sum(row.get("would_skip") is not None for row in rows)
    avoided = math.fsum(-row[key] for row in hits if row[key] < 0)
    missed = math.fsum(row[key] for row in hits if row[key] > 0)
    net = avoided - missed
    result = {
        "avoided_losses": avoided,
        "missed_winners": missed,
        "net_effect": net,
        "mean_delta": net / valid_count if valid_count else None,
        "valid_contrast_count": valid_count,
    }
    if key == "_actual":
        for name in ("avoided_losses", "missed_winners", "net_effect"):
            result[f"{name}_sol"] = result[name] / LAMPORTS_PER_SOL
    return result


def _percentile(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * probability
    lower = math.floor(index)
    upper = math.ceil(index)
    fraction = index - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def _bootstrap(rows: list[dict[str, Any]], specification: dict[str, Any]) -> dict[str, Any]:
    """KST 신호일을 통째로 복원 추출해 paired 효과와 잔존 기대값을 계산한다."""
    seed = specification.get("seed", 20261003)
    repetitions = specification.get("repetitions", 5000)
    confidence = specification.get("confidence", 0.95)
    if type(seed) is not int or type(repetitions) is not int or not 1 <= repetitions <= 100_000:
        raise ValueError("invalid bootstrap seed or repetitions")
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not 0 < confidence < 1:
        raise ValueError("invalid bootstrap confidence")
    days: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        days[row["_day"]].append(row)
    clusters = []
    for day in sorted(days):
        members = days[day]
        valid = [row for row in members if row.get("would_skip") is not None]
        remaining = [row for row in members if row.get("would_skip") is not True]
        clusters.append((
            math.fsum(-row["_normalized"] for row in valid if row["would_skip"]),
            len(valid),
            math.fsum(row["_normalized"] for row in remaining),
            len(remaining),
        ))
    effects = []
    expectancy = []
    rng = random.Random(seed)
    if clusters:
        for _ in range(repetitions):
            sampled = [clusters[rng.randrange(len(clusters))] for _ in clusters]
            valid_count = sum(cluster[1] for cluster in sampled)
            remaining_count = sum(cluster[3] for cluster in sampled)
            if valid_count:
                effects.append(math.fsum(cluster[0] for cluster in sampled) / valid_count)
            if remaining_count:
                expectancy.append(math.fsum(cluster[2] for cluster in sampled) / remaining_count)
    tail = (1 - confidence) / 2
    return {
        "method": "paired_kst_signal_day_cluster_percentile",
        "seed": seed,
        "repetitions": repetitions,
        "confidence": confidence,
        "cluster_count": len(clusters),
        "mean_delta_ci": [_percentile(effects, tail), _percentile(effects, 1 - tail)],
        "remaining_expectancy_ci": [_percentile(expectancy, tail), _percentile(expectancy, 1 - tail)],
        "valid_effect_repetitions": len(effects),
        "valid_remaining_repetitions": len(expectancy),
    }


def _tails(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    positives = sorted(
        (-row[key] for row in rows if row.get("would_skip") is True and row[key] < 0),
        reverse=True,
    )
    total = _effects(rows, key)["net_effect"]
    return {
        str(count): {
            "removed_count": min(count, len(positives)),
            "removed_positive_delta": math.fsum(positives[:count]),
            "net_effect": total - math.fsum(positives[:count]),
        }
        for count in (1, 2, 3)
    }


def _gates(contract: dict[str, Any], *, route: bool = False) -> dict[str, int]:
    value = contract.get("route_sample_gates" if route else "sample_gates", DEFAULT_SAMPLE_GATES)
    if not isinstance(value, dict) or set(value) != set(DEFAULT_SAMPLE_GATES):
        raise ValueError("sample gates must define the five fixed sample counts")
    if any(type(threshold) is not int or threshold < 1 for threshold in value.values()):
        raise ValueError("sample gate thresholds must be positive integers")
    return dict(value)


def _costs(rows: list[dict[str, Any]], contract: dict[str, Any]) -> list[dict[str, Any]]:
    scenarios = contract.get("cost_scenarios", [])
    if not isinstance(scenarios, list) or len(scenarios) > 20:
        raise ValueError("cost scenarios must be a bounded list")
    results = []
    names = set()
    for scenario in scenarios:
        if not isinstance(scenario, dict):
            raise ValueError("cost scenario must be a dictionary")
        name = scenario.get("name")
        bps = scenario.get("extra_round_trip_bps")
        fixed = scenario.get("fixed_round_trip_lamports")
        if not isinstance(name, str) or not name or name in names:
            raise ValueError("cost scenario names must be unique")
        names.add(name)
        if isinstance(bps, bool) or not isinstance(bps, (int, float)) or not math.isfinite(bps) or bps < 0:
            raise ValueError("cost scenario bps must be finite and nonnegative")
        if type(fixed) is not int or fixed < 0:
            raise ValueError("fixed cost must be a nonnegative integer")
        adjusted = []
        for row in rows:
            cost = row["entry_cost_lamports"]
            net = row["realized_pnl_lamports"] - cost * bps / 10_000 - fixed
            adjusted.append({**row, "_actual": net, "_normalized": net / cost})
        remaining = [row for row in adjusted if row.get("would_skip") is not True]
        results.append({
            "name": name,
            "extra_round_trip_bps": bps,
            "fixed_round_trip_lamports": fixed,
            "actual": _effects(adjusted, "_actual"),
            "normalized": _effects(adjusted, "_normalized"),
            "remaining": {"actual": _metrics(remaining, "_actual"), "normalized": _metrics(remaining, "_normalized")},
        })
    return results


def _evaluate(rows: list[dict[str, Any]], contract: dict[str, Any], *, route: bool = False) -> dict[str, Any]:
    counts = _counts(rows)
    gates = _gates(contract, route=route)
    gates_passed = all(counts[key] >= gates[key] for key in gates)
    valid = [row for row in rows if row.get("would_skip") is not None]
    unknown = [row for row in rows if row.get("would_skip") is None]
    remaining = [row for row in rows if row.get("would_skip") is not True]
    actual = _effects(rows, "_actual")
    normalized = _effects(rows, "_normalized")
    bootstrap = _bootstrap(rows, contract.get("bootstrap", {}))
    normalized_tails = _tails(rows, "_normalized")
    actual_tails = _tails(rows, "_actual")
    ordered = sorted(rows, key=lambda row: (row["_signal"], row["buy_event_seq"], row["position_id"]))
    cut = len(ordered) // 2
    halves = [
        {"trade_count": len(half), **_filter_counts(half), "actual": _effects(half, "_actual"), "normalized": _effects(half, "_normalized")}
        for half in (ordered[:cut], ordered[cut:])
    ]
    scenarios = _costs(rows, contract)
    remaining_normalized = _metrics(remaining, "_normalized")
    lower_ci = bootstrap["mean_delta_ci"][0]
    remaining_lower_ci = bootstrap["remaining_expectancy_ci"][0]
    alpha_positive = bool(
        remaining_lower_ci is not None and remaining_lower_ci > 0
        and remaining_normalized["profit_factor_above_one"]
        and remaining_normalized["total_pnl"] > 0
    )
    alpha_verdict = (
        "INSUFFICIENT_SAMPLE"
        if not gates_passed or len(remaining) < gates["valid_non_hits"]
        else "POSITIVE_EXPECTANCY_EVIDENCE" if alpha_positive
        else "NO_POSITIVE_EXPECTANCY_EVIDENCE"
    )
    if not gates_passed:
        verdict = "INSUFFICIENT_ROUTE_SAMPLE" if route else "INSUFFICIENT_FRESH_SAMPLE"
    elif (
        normalized["net_effect"] <= 0
        or normalized["avoided_losses"] <= normalized["missed_winners"]
        or any(half["normalized"]["net_effect"] < 0 for half in halves)
        or normalized_tails["2"]["net_effect"] <= 0
    ):
        verdict = "NEGATIVE_FILTER_FAILED"
    elif (
        lower_ci is not None and lower_ci > 0
        and actual["net_effect"] >= 0
        and all(half["normalized"]["net_effect"] > 0 for half in halves)
        and normalized_tails["3"]["net_effect"] > 0
        and all(scenario["normalized"]["net_effect"] > 0 for scenario in scenarios)
        and not unknown
    ):
        verdict = "ROBUST_NEGATIVE_FILTER"
    else:
        verdict = "PROMISING_NEGATIVE_FILTER"
    return {
        "sample_counts": counts,
        "sample_gates": gates,
        "sample_gates_passed": gates_passed,
        "valid_contrast_count": len(valid),
        "missing_would_skip_count": len(unknown),
        "filter_counts": _filter_counts(rows),
        "unknown_stratum": {"actual": _metrics(unknown, "_actual"), "normalized": _metrics(unknown, "_normalized")},
        "control": {"actual": _metrics(rows, "_actual"), "normalized": _metrics(rows, "_normalized")},
        "primary_effect": {"actual": actual, "normalized": normalized, "mean_normalized_delta": normalized["mean_delta"], "bootstrap_ci": bootstrap["mean_delta_ci"]},
        "bootstrap": bootstrap,
        "remaining": {
            "actual": _metrics(remaining, "_actual"),
            "normalized": remaining_normalized,
            "strategy_alpha": {
                "verdict": alpha_verdict,
                "normalized_expectancy_ci": bootstrap["remaining_expectancy_ci"],
            },
        },
        "temporal_halves": halves,
        "tail_removal": {"normalized": normalized_tails, "actual": actual_tails},
        "cost_scenarios": scenarios,
        "cost_sensitivity_note": "추가 비용은 매수 생략 효과를 단조 증가시키므로 비용별 효과 개선만으로 강건성을 주장하지 않는다. 잔존 전략 손익도 함께 확인한다.",
        "unknown_policy": "UNKNOWN은 Control 거래를 유지하고 paired 효과 추정에서만 제외한다.",
        "normalization": {"entry_lamports": LAMPORTS_PER_SOL, "unit": "equal_1_SOL_pnl_SOL", "formula": "realized_pnl_lamports / entry_cost_lamports"},
        "verdict": verdict,
    }


def evaluate_closed_cohort(trades: list[dict[str, Any]], contract: dict[str, Any]) -> dict[str, Any]:
    """종료 코호트만 받아 사전 고정 조건으로 전체와 경로별 최종 판정을 만든다."""
    if not isinstance(contract, dict):
        raise ValueError("evaluation contract must be a dictionary")
    rows = _rows(trades)
    result = _evaluate(rows, contract)
    result["routes"] = {
        family: _evaluate([row for row in rows if row["family"] == family], contract, route=True)
        for family in ("MOMENTUM", "SMART_MONEY")
    }
    return result
