"""종료 배치의 승패·기회비용과 미래 challenger 검증을 읽기 전용으로 계산한다."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
import math
from statistics import median
from typing import Any

SOL = 1_000_000_000
MAX_ROWS = 2000
MAX_FEATURES = 128
MAX_TRIALS = 200
MAX_PROPOSALS = 10
OUTCOME_TOKENS = frozenset({
    "pnl", "mae", "mfe", "holding", "trajectory", "winner", "loser",
    "realized", "closed", "exit", "outcome", "forensic", "label",
})
DOMAIN_TELEMETRY = {
    "entry_timing": ("entry_latency_ms", "signal_age_ms"),
    "liquidity": ("liquidity_usd",),
    "flow": ("buy_sell_ratio_m5", "net_buys_m5", "flow_acceleration"),
    "wallet": ("wallet_combination", "whale_paid_sol"),
    "quote": ("entry_price_impact_pct", "quote_age_ms"),
    "retries": ("quote_retry_count", "rpc_retry_count"),
    "concurrency": ("concurrent_signals", "open_positions"),
    "token_pool_age": ("pair_age_seconds", "token_age_seconds"),
    "market_activity": ("market_activity_count",),
}
LABELS = ("rapid_sl", "immediate_fall", "reversal", "near_tp_failure",
          "sideways_then_sl", "long_hold_sl", "false_momentum")
WINNER_LABELS = ("fast_tp", "gradual_continuation", "large_winner", "long_hold_winner",
                 "low_liquidity_winner", "high_flow_winner")


def _time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp must be a UTC ISO string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("timestamp must be a UTC ISO string") from exc
    if parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be UTC")
    return parsed


def _number(value: Any) -> bool:
    return type(value) in (int, float) and math.isfinite(value)


def _outcome_feature(name: str) -> bool:
    tokens = name.lower().replace(".", "_").replace("-", "_").split("_")
    return bool(OUTCOME_TOKENS.intersection(tokens)) or name.lower() in {"return", "return_percent", "false_momentum"}


def _prepare(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """입력을 복사하고 사후 필드나 시각이 불명확한 predictor를 제외한다."""
    if not isinstance(rows, list) or len(rows) > MAX_ROWS:
        raise ValueError("rows must be a bounded list")
    result = []
    identities: set[str] = set()
    sequences: set[int] = set()
    feature_names: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("row must be a dictionary")
        identity = row.get("position_id")
        if not isinstance(identity, str) or not identity or identity in identities:
            raise ValueError("position_id must be unique and nonempty")
        identities.add(identity)
        for key in ("buy_event_seq", "entry_cost_lamports", "realized_pnl_lamports"):
            if type(row.get(key)) is not int:
                raise ValueError(f"{key} must be an integer")
        if row["buy_event_seq"] <= 0 or row["buy_event_seq"] in sequences:
            raise ValueError("buy_event_seq must be unique and positive")
        sequences.add(row["buy_event_seq"])
        if row["entry_cost_lamports"] <= 0:
            raise ValueError("entry cost must be positive")
        signal = _time(row.get("signal_timestamp"))
        entry = _time(row.get("entry_decision_timestamp"))
        closed = _time(row.get("closed_at"))
        if not signal <= entry <= closed:
            raise ValueError("signal/entry/close timestamps are inconsistent")
        features = row.get("features")
        if not isinstance(features, dict) or len(features) > MAX_FEATURES:
            raise ValueError("features must be a bounded dictionary")
        valid, invalid = {}, {}
        for name, feature in features.items():
            if not isinstance(name, str) or not name:
                raise ValueError("feature name must be nonempty")
            feature_names.add(name)
            reason = None
            if _outcome_feature(name):
                reason = "OUTCOME_ONLY_FEATURE"
            elif not isinstance(feature, dict):
                reason = "INVALID_FEATURE_RECORD"
            elif not isinstance(feature.get("source"), str) or not feature["source"].strip():
                reason = "MISSING_SOURCE"
            else:
                try:
                    observed = _time(feature.get("observed_at"))
                    if observed > entry:
                        reason = "AFTER_ENTRY_DECISION"
                except ValueError:
                    reason = "MISSING_OR_INVALID_OBSERVATION_TIME"
                value = feature.get("value")
                if reason is None and not (
                    type(value) is bool or isinstance(value, str) or _number(value)
                ):
                    reason = "MISSING_OR_INVALID_VALUE"
            if reason:
                invalid[name] = reason
            else:
                valid[name] = dict(feature)
        if len(feature_names) > MAX_FEATURES:
            raise ValueError("batch has too many feature names")
        normalized = row["realized_pnl_lamports"] / row["entry_cost_lamports"]
        if not math.isfinite(normalized):
            raise ValueError("trade return must be finite")
        if row.get("forensic") is not None and not isinstance(row["forensic"], dict):
            raise ValueError("forensic must be an outcome-only dictionary")
        result.append({**row, "_signal": signal, "_entry": entry, "_closed": closed,
                       "_features": valid, "_invalid": invalid,
                       "_normalized": normalized})
    return sorted(result, key=lambda row: (row["_entry"], row["buy_event_seq"], row["position_id"]))


def _metrics(rows: list[dict[str, Any]], normalized: bool = False) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: (row["_closed"], row["buy_event_seq"], row["position_id"]))
    values = [row["_normalized"] if normalized else row["realized_pnl_lamports"] / SOL for row in ordered]
    profit = math.fsum(value for value in values if value > 0)
    loss = math.fsum(-value for value in values if value < 0)
    total = math.fsum(values)
    equity = peak = drawdown = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    pf_state = "FINITE" if loss else "POSITIVE_INFINITY" if profit else "UNDEFINED"
    return {
        "count": len(values), "winners": sum(value > 0 for value in values),
        "losers": sum(value < 0 for value in values), "breakeven": sum(value == 0 for value in values),
        "total_pnl": total, "expectancy": total / len(values) if values else None,
        "profit_factor": profit / loss if loss else None, "profit_factor_state": pf_state,
        "gross_profit": profit, "gross_loss": loss,
        "win_rate": sum(value > 0 for value in values) / len(values) if values else None,
        "max_drawdown": drawdown, "drawdown_order": "closed_at_then_buy_event_seq",
        "unit": "SOL_at_equal_one_SOL_notional" if normalized else "actual_SOL",
        "mean_return_percent": math.fsum(row["_normalized"] * 100 for row in rows) / len(rows) if rows else None,
        "capital_weighted_return_percent": sum(row["realized_pnl_lamports"] for row in rows) / sum(row["entry_cost_lamports"] for row in rows) * 100 if rows else None,
    }


def economic_report(rows: list[dict[str, Any]], decisions: dict[str, bool | None]) -> dict[str, Any]:
    """포함 여부가 알려진 거래만 짝 비교하고 미확인 거래는 별도 공개한다."""
    prepared = _prepare(rows)
    if not isinstance(decisions, dict) or set(decisions) - {row["position_id"] for row in prepared}:
        raise ValueError("decision identities must belong to rows")
    if any(value is not None and type(value) is not bool for value in decisions.values()):
        raise ValueError("decisions must be include booleans or None")
    included = [row for row in prepared if decisions.get(row["position_id"]) is True]
    excluded = [row for row in prepared if decisions.get(row["position_id"]) is False]
    unknown = [row for row in prepared if decisions.get(row["position_id"]) is None]
    valid = included + excluded
    effects = {}
    for normalized, name in ((False, "actual_sol"), (True, "equal_notional")):
        values = [row["_normalized"] if normalized else row["realized_pnl_lamports"] / SOL for row in excluded]
        avoided = math.fsum(-value for value in values if value < 0)
        missed = math.fsum(value for value in values if value > 0)
        effects[name] = {"avoided_loss": avoided, "missed_profit": missed,
                         "net_effect": avoided - missed,
                         "mean_effect_per_valid_control": (avoided - missed) / len(valid) if valid else None}
    positive_effects = sorted((-row["_normalized"] for row in excluded if row["_normalized"] < 0), reverse=True)
    positive_returns = sorted((row["_normalized"] for row in included if row["_normalized"] > 0), reverse=True)
    total_effect = effects["equal_notional"]["net_effect"]
    remaining_return = math.fsum(row["_normalized"] for row in included)
    return {
        "control": {"actual": _metrics(prepared), "normalized": _metrics(prepared, True)},
        "known_comparison_control": {"actual": _metrics(valid), "normalized": _metrics(valid, True)},
        "included": {"actual": _metrics(included), "normalized": _metrics(included, True)},
        "excluded": {"actual": _metrics(excluded), "normalized": _metrics(excluded, True)},
        "unknown": {"actual": _metrics(unknown), "normalized": _metrics(unknown, True)},
        "included_trades": len(included), "excluded_trades": len(excluded),
        "unknown_trades": len(unknown),
        "winners_retained": sum(row["_normalized"] > 0 for row in included),
        "winners_excluded": sum(row["_normalized"] > 0 for row in excluded),
        "losers_retained": sum(row["_normalized"] < 0 for row in included),
        "losers_excluded": sum(row["_normalized"] < 0 for row in excluded),
        "effects": effects,
        "decision_coverage": len(valid) / len(prepared) if prepared else None,
        "exclusion_coverage": len(excluded) / len(prepared) if prepared else None,
        "inclusion_coverage": len(included) / len(prepared) if prepared else None,
        "missing_predictor_rate": len(unknown) / len(prepared) if prepared else None,
        "concentration": {
            "effect_after_removing_top_two_avoided_losses": total_effect - math.fsum(positive_effects[:2]),
            "included_pnl_after_removing_top_two_winners": remaining_return - math.fsum(positive_returns[:2]),
            "positive_effect_top_two_share": math.fsum(positive_effects[:2]) / math.fsum(positive_effects) if positive_effects else None,
            "positive_return_top_two_share": math.fsum(positive_returns[:2]) / math.fsum(positive_returns) if positive_returns else None,
            "actual_normalized_effect_sign_disagreement": effects["actual_sol"]["net_effect"] * total_effect < 0,
        },
        "normalization": {"equal_notional_lamports": SOL, "recorded_costs_already_in_pnl": True},
        "unknown_policy": "NO_COUNTERFACTUAL_ENTRY_OR_SKIP_ASSUMPTION",
    }


def _decisions(rows: list[dict[str, Any]], rule: dict[str, Any], kind: str) -> dict[str, bool | None]:
    from src.research.accelerated_alpha import evaluate_rule

    result = {}
    for row in rows:
        matched = evaluate_rule(rule, {**row, "features": row["_features"]})
        result[row["position_id"]] = None if matched is None else not matched if kind == "negative_filter" else matched
    return result


def _distribution(rows: list[dict[str, Any]], name: str) -> dict[str, Any]:
    values = [row["_features"][name]["value"] for row in rows if name in row["_features"]]
    numeric = [value for value in values if _number(value)]
    reasons = Counter(row["_invalid"].get(name, "NOT_RECORDED") for row in rows if name not in row["_features"])
    summary: dict[str, Any] = {"population": len(rows), "available": len(values),
                              "coverage": len(values) / len(rows) if rows else None,
                              "missing_reasons": dict(sorted(reasons.items()))}
    if values and len(numeric) == len(values):
        summary.update({"type": "numeric", "minimum": min(numeric), "median": median(numeric),
                        "maximum": max(numeric), "mean": math.fsum(numeric) / len(numeric)})
    else:
        categories = Counter(f"{type(value).__name__}:{value}" for value in values)
        summary.update({"type": "categorical_or_mixed", "categories": dict(categories.most_common(20)),
                        "categories_truncated": len(categories) > 20})
    return summary


def _trajectory(row: dict[str, Any]) -> tuple[list[tuple[datetime, float]], list[str]]:
    raw = row.get("forensic", {}).get("price_trajectory")
    if raw is None:
        return [], ["PRICE_TRAJECTORY_NOT_RECORDED"]
    if not isinstance(raw, list) or len(raw) > 512:
        return [], ["PRICE_TRAJECTORY_INVALID_OR_UNBOUNDED"]
    samples = []
    for item in raw:
        try:
            observed = _time(item.get("observed_at"))
            value = item.get("return_percent")
            if not _number(value) or not row["_entry"] <= observed <= row["_closed"]:
                raise ValueError("invalid trajectory sample")
            samples.append((observed, float(value)))
        except (ValueError, AttributeError):
            return [], ["PRICE_TRAJECTORY_INVALID_SAMPLE"]
    if len({time for time, _ in samples}) != len(samples):
        return [], ["PRICE_TRAJECTORY_DUPLICATE_TIME"]
    return sorted(samples), [] if samples else ["PRICE_TRAJECTORY_EMPTY"]


def _forensic(row: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any]:
    samples, missing = _trajectory(row)
    values = [value for _, value in samples]
    duration = (row["_closed"] - row["_entry"]).total_seconds()
    labels = {name: "UNKNOWN" for name in LABELS}
    loser = row["realized_pnl_lamports"] < 0
    exit_reason = row.get("forensic", {}).get("exit_reason")
    sl = exit_reason in {"SL", "STOP_LOSS"}
    exit_known = isinstance(exit_reason, str) and bool(exit_reason)
    # 임계값을 등록하지 않은 분류는 추정하지 않는다.
    if samples:
        if "rapid_sl_seconds" in contract and exit_known:
            labels["rapid_sl"] = bool(sl and duration <= contract["rapid_sl_seconds"])
        if {"immediate_fall_seconds", "immediate_fall_percent"} <= contract.keys():
            early = [value for time, value in samples if (time - row["_entry"]).total_seconds() <= contract["immediate_fall_seconds"]]
            labels["immediate_fall"] = any(value <= -contract["immediate_fall_percent"] for value in early) if early else "UNKNOWN"
        if {"reversal_peak_percent", "reversal_drop_percent"} <= contract.keys():
            peak = -math.inf
            reversed_price = False
            for value in values:
                peak = max(peak, value)
                reversed_price |= peak >= contract["reversal_peak_percent"] and peak - value >= contract["reversal_drop_percent"]
            labels["reversal"] = reversed_price
        if {"tp_percent", "near_tp_distance_percent"} <= contract.keys():
            labels["near_tp_failure"] = bool(loser and contract["tp_percent"] - contract["near_tp_distance_percent"] <= max(values) < contract["tp_percent"])
        if {"long_hold_seconds", "sideways_band_percent"} <= contract.keys() and exit_known:
            preceding = values[:-1]
            labels["sideways_then_sl"] = bool(sl and duration >= contract["long_hold_seconds"] and preceding and all(abs(value) <= contract["sideways_band_percent"] for value in preceding))
        if "long_hold_seconds" in contract and exit_known:
            labels["long_hold_sl"] = bool(sl and duration >= contract["long_hold_seconds"])
        if row.get("forensic", {}).get("momentum_signal_at_entry") is True:
            conditions = [labels[name] for name in ("rapid_sl", "immediate_fall", "reversal")]
            labels["false_momentum"] = bool(loser) if True in conditions else False if "UNKNOWN" not in conditions else "UNKNOWN"
    tp_times = [time for time, value in samples if value >= contract["tp_percent"]] if "tp_percent" in contract else []
    magnitude = "UNKNOWN"
    if loser and {"small_loss_max_percent", "medium_loss_max_percent"} <= contract.keys():
        loss_percent = -row["_normalized"] * 100
        magnitude = "SMALL" if loss_percent <= contract["small_loss_max_percent"] else "MEDIUM" if loss_percent <= contract["medium_loss_max_percent"] else "LARGE"
    winner_labels = {name: "UNKNOWN" for name in WINNER_LABELS}
    winner = row["_normalized"] > 0
    if samples and {"tp_percent", "fast_tp_seconds"} <= contract.keys():
        winner_labels["fast_tp"] = bool(winner and tp_times and (tp_times[0] - row["_entry"]).total_seconds() <= contract["fast_tp_seconds"])
    if samples and {"continuation_min_samples", "continuation_max_pullback_percent"} <= contract.keys():
        if len(samples) >= contract["continuation_min_samples"]:
            peak, pullback = values[0], 0.0
            for value in values:
                peak = max(peak, value)
                pullback = max(pullback, peak - value)
            winner_labels["gradual_continuation"] = bool(winner and values[-1] > values[0] and pullback <= contract["continuation_max_pullback_percent"])
    if "large_winner_percent" in contract:
        winner_labels["large_winner"] = bool(winner and row["_normalized"] * 100 >= contract["large_winner_percent"])
    if "long_hold_seconds" in contract:
        winner_labels["long_hold_winner"] = bool(winner and duration >= contract["long_hold_seconds"])
    for label, feature, threshold, below in (
        ("low_liquidity_winner", "liquidity_usd", "low_liquidity_max_usd", True),
        ("high_flow_winner", "buy_sell_ratio_m5", "high_flow_min_ratio", False),
    ):
        value = row["_features"].get(feature, {}).get("value")
        if threshold in contract and _number(value):
            winner_labels[label] = bool(winner and (value <= contract[threshold] if below else value >= contract[threshold]))
    return {
        "position_id": row["position_id"], "buy_event_seq": row["buy_event_seq"],
        "outcome": "WINNER" if row["_normalized"] > 0 else "LOSER" if loser else "BREAKEVEN",
        "actual_pnl_sol": row["realized_pnl_lamports"] / SOL, "return_percent": row["_normalized"] * 100,
        "holding_seconds": duration, "loss_size": magnitude,
        "sampled_mae_percent": min([0.0] + values) if samples else None,
        "sampled_mfe_percent": max([0.0] + values) if samples else None,
        "trajectory_sample_count": len(samples), "trajectory_semantics": "SAMPLED_ONLY_NO_INTERPOLATION",
        "seconds_to_sampled_tp": (tp_times[0] - row["_entry"]).total_seconds() if tp_times else None,
        "labels": labels, "winner_labels": winner_labels, "missing_telemetry": missing,
        "classification_contract": dict(contract),
        "pre_entry_features": {name: dict(value) for name, value in row["_features"].items()},
        "unusable_predictors": dict(row["_invalid"]),
        "winner_trajectory_questions": {name: "UNKNOWN" for name in ("acceleration", "liquidity_change", "flow_change")},
    }


def _screens(rows: list[dict[str, Any]], decisions: dict[str, bool | None]) -> dict[str, Any]:
    midpoint = len(rows) // 2
    subsets = (rows[:midpoint], rows[midpoint:])
    splits = [economic_report(part, {row["position_id"]: decisions[row["position_id"]] for row in part}) for part in subsets]
    full = economic_report(rows, decisions)
    return {
        "temporal_split": {"method": "entry_time_first_half_second_half",
                           "half_counts": [len(part) for part in subsets],
                           "normalized_effects": [part["effects"]["equal_notional"]["net_effect"] for part in splits],
                           "both_positive": all(part["effects"]["equal_notional"]["net_effect"] > 0 for part in splits)},
        "concentration": full["concentration"],
        "decision_coverage": full["decision_coverage"], "missing_predictor_rate": full["missing_predictor_rate"],
        "exclusion_coverage": full["exclusion_coverage"], "inclusion_coverage": full["inclusion_coverage"],
        "screen_semantics": "DESCRIPTIVE_ONLY_NOT_FRESH_VALIDATION",
    }


def discovery_screen(rows: list[dict[str, Any]], rule: dict[str, Any], kind: str,
                     policy: dict[str, Any] | None = None) -> dict[str, Any]:
    """명시한 연구 정책으로 과적합을 걸러내며 관측되지 않은 축은 통과로 보지 않는다."""
    prepared = _prepare(rows)
    decisions = _decisions(prepared, rule, kind)
    economy = economic_report(prepared, decisions)
    avoided = [row for row in prepared if decisions[row["position_id"]] is False and row["_normalized"] < 0]
    total = math.fsum(-row["_normalized"] for row in avoided)
    def concentration(key):
        groups = {}
        missing = 0
        for row in avoided:
            value = key(row)
            if value is None:
                missing += 1
            else:
                groups[str(value)] = groups.get(str(value), 0.0) - row["_normalized"]
        return {"groups": groups, "missing_affected": missing,
                "maximum_share": min(1.0, max(groups.values(), default=0) / total) if total else None}
    dependence = {
        "single_trade": concentration(lambda row: row["position_id"]),
        "single_day": concentration(lambda row: (row["_signal"] + timedelta(hours=9)).date().isoformat()),
        "route": concentration(lambda row: row["_features"].get("route_type", {}).get("value")),
        "position_size": concentration(lambda row: row["entry_cost_lamports"]),
    }
    threshold_tails = []
    def threshold_check(node):
        if node["op"] in {"AND", "OR"}:
            for part in node["parts"]:
                threshold_check(part)
        elif node["op"] in {"gt", "gte", "lt", "lte"}:
            values = [row["_features"].get(node["feature"], {}).get("value") for row in prepared]
            values = [value for value in values if _number(value)]
            lower = sum(value <= node["threshold"] for value in values)
            threshold_tails.append({"feature": node["feature"], "threshold": node["threshold"],
                                    "smaller_tail_fraction": min(lower, len(values) - lower) / len(values) if values else None})
    threshold_check(rule)
    gross_winner = economy["known_comparison_control"]["normalized"]["gross_profit"]
    winner_destruction = economy["effects"]["equal_notional"]["missed_profit"] / gross_winner if gross_winner else None
    result = {"sample": len(prepared), "affected": economy["excluded_trades"], "dependence": dependence,
              "threshold_tails": threshold_tails, "winner_profit_destroyed_share": winner_destruction,
              "economics": economy, "policy": policy, "status": "REJECT_DISCOVERY", "reasons": []}
    counts = {"minimum_sample", "minimum_affected"}
    fractions = {"maximum_single_trade_share", "maximum_single_day_share", "maximum_route_share",
                 "maximum_position_size_share", "maximum_missing_predictor_rate", "maximum_winner_profit_destroyed_share",
                 "minimum_threshold_tail_fraction"}
    if not isinstance(policy, dict) or not (counts | fractions) <= policy.keys():
        result["reasons"].append("EXPLICIT_DISCOVERY_SCREEN_POLICY_REQUIRED")
        return result
    if any(type(policy[key]) is not int or policy[key] < 1 for key in counts) or any(not _number(policy[key]) or not 0 <= policy[key] <= 1 for key in fractions):
        raise ValueError("discovery screen policy requires positive counts and fraction limits")
    if len(prepared) < policy["minimum_sample"] or result["affected"] < policy["minimum_affected"]:
        result["reasons"].append("MINIMUM_DISCOVERY_SAMPLE_OR_AFFECTED_FAILED")
    if economy["effects"]["equal_notional"]["net_effect"] <= 0:
        result["reasons"].append("NONPOSITIVE_DISCOVERY_NET_EFFECT")
    for axis, summary in dependence.items():
        if summary["missing_affected"] or summary["maximum_share"] is None:
            result["reasons"].append(axis.upper() + "_DEPENDENCE_UNMEASURED")
        elif summary["maximum_share"] > policy["maximum_" + axis + "_share"]:
            result["reasons"].append(axis.upper() + "_DEPENDENCE_FAILED")
    if (economy["missing_predictor_rate"] or 0) > policy["maximum_missing_predictor_rate"]:
        result["reasons"].append("FEATURE_MISSINGNESS_FAILED")
    if any(item["smaller_tail_fraction"] is None or item["smaller_tail_fraction"] < policy["minimum_threshold_tail_fraction"] for item in threshold_tails):
        result["reasons"].append("EXTREME_THRESHOLD_FAILED")
    if winner_destruction is None or winner_destruction > policy["maximum_winner_profit_destroyed_share"]:
        result["reasons"].append("WINNER_DESTRUCTION_FAILED_OR_UNMEASURED")
    if economy["concentration"]["actual_normalized_effect_sign_disagreement"]:
        result["reasons"].append("POSITION_SIZE_EFFECT_SIGN_CONFLICT")
    if not result["reasons"]:
        result["status"] = "PASS_DISCOVERY"
    return result


def _candidate_proposals(rows: list[dict[str, Any]], batch_id: str, screen_policy=None) -> dict[str, Any]:
    rules = []
    names = sorted({name for row in rows for name in row["_features"]})
    for name in names:
        values = [row["_features"][name]["value"] for row in rows if name in row["_features"]]
        if values and all(_number(value) for value in values):
            unique = sorted(set(values))
            thresholds = sorted({unique[int((len(unique) - 1) * quantile)] for quantile in (.25, .5, .75)})
            if len(unique) > 1:
                for threshold in thresholds:
                    for op in ("lte", "gt"):
                        rules.append({"feature": name, "op": op, "threshold": threshold})
        else:
            unique = sorted({(type(value).__name__, str(value), value) for value in values})
            for _, _, value in unique[:4]:
                rules.append({"feature": name, "op": "eq", "threshold": value})
        if len(rules) >= 60:
            rules = rules[:60]
            break
    proposals = []
    trials = 0
    seen_decisions = set()

    def trial(rule: dict[str, Any], kind: str) -> None:
        nonlocal trials
        if trials >= MAX_TRIALS:
            return
        trials += 1
        decisions = _decisions(rows, rule, kind)
        key = tuple(decisions[row["position_id"]] for row in rows)
        if key in seen_decisions:
            return
        seen_decisions.add(key)
        economy = economic_report(rows, decisions)
        if not economy["included_trades"] or not economy["excluded_trades"]:
            return
        effect = economy["effects"]["equal_notional"]["net_effect"]
        if effect <= 0:
            return
        proposals.append({"kind": kind, "rule": rule, "creation_batch": batch_id,
                          "status": "DISCOVERY_ONLY", "validated": False,
                          "discovery_evidence": {"batch_id": batch_id, "position_ids": [row["position_id"] for row in rows],
                                                 "economic_report": economy},
                          "ranking_score": effect})

    for rule in rules:
        for kind in ("negative_filter", "profitable_subset"):
            trial(rule, kind)
    numeric_rules = [rule for rule in rules if rule["op"] in {"lte", "gt"}][:10]
    for index, left in enumerate(numeric_rules):
        for right in numeric_rules[index + 1:]:
            if left["feature"] != right["feature"]:
                for kind in ("negative_filter", "profitable_subset"):
                    trial({"op": "AND", "parts": [left, right]}, kind)
    proposals.sort(key=lambda proposal: (-proposal["ranking_score"], str(proposal["rule"]), proposal["kind"]))
    selected = proposals[:MAX_PROPOSALS]
    for index, proposal in enumerate(selected, 1):
        proposal["proposal_id"] = f"{batch_id}-PROPOSAL-{index:02d}"
        from src.research.accelerated_alpha import digest
        proposal.update(version=1, rule_hash=digest(proposal["rule"]),
                        rationale="배치의 진입 전 관측값으로 손실 회피와 승자 기회비용을 함께 탐색했다.",
                        expected_mechanism="조건부 거래 제외의 순효과; 인과 기전은 미래 검증 전 미확인")
        proposal["discovery_screen"] = discovery_screen(rows, proposal["rule"], proposal["kind"], screen_policy)
        proposal["eligible_for_registration"] = proposal["discovery_screen"]["status"] == "PASS_DISCOVERY"
        proposal["discovery_evidence"]["discovery_screen_policy"] = screen_policy
        proposal["discovery_evidence"]["screens"] = _screens(
            rows, _decisions(rows, proposal["rule"], proposal["kind"])
        )
        proposal["discovery_evidence"]["cost_stress"] = cost_stress_report(
            rows, _decisions(rows, proposal["rule"], proposal["kind"])
        )
        proposal["future_validation"] = {"discovery_batch_excluded": batch_id,
                                         "start_event_seq": "ASSIGN_AT_REGISTRATION",
                                         "start_timestamp": "ASSIGN_AT_REGISTRATION",
                                         "stage1_completed": 20, "stage2_additional_completed": 30,
                                         "stage3": "PREREGISTERED_LONGER_VALIDATION_AND_HUMAN_REVIEW"}
    return {"proposals": selected, "searched_rule_kind_trials": trials, "maximum_trials": MAX_TRIALS,
            "maximum_proposals": MAX_PROPOSALS, "positive_distinct_subsets_found": len(proposals),
            "search_semantics": "BOUNDED_EXPLORATORY_MULTIPLE_TESTING_NO_ALPHA_CLAIM",
            "feature_search_order": "LEXICOGRAPHIC_BOUNDED_NO_FAMILY_LOCK"}


def build_forensic_report(rows: list[dict[str, Any]], batch_id: str, *, label_contract: dict[str, Any] | None = None,
                          screen_policy: dict[str, Any] | None = None) -> dict[str, Any]:
    """배치의 모든 승패를 공개하고 같은 배치에서 성공 판정을 만들지 않는다."""
    if not isinstance(batch_id, str) or not batch_id:
        raise ValueError("batch_id must be nonempty")
    contract = dict(label_contract or {})
    allowed = {"rapid_sl_seconds", "immediate_fall_seconds", "immediate_fall_percent",
               "reversal_peak_percent", "reversal_drop_percent", "tp_percent", "near_tp_distance_percent",
               "long_hold_seconds", "sideways_band_percent", "small_loss_max_percent", "medium_loss_max_percent",
               "fast_tp_seconds", "continuation_min_samples", "continuation_max_pullback_percent",
               "large_winner_percent", "low_liquidity_max_usd", "high_flow_min_ratio"}
    if contract.keys() - allowed or any(not _number(value) or value <= 0 for value in contract.values()):
        raise ValueError("label contract must contain explicit positive finite thresholds")
    if {"small_loss_max_percent", "medium_loss_max_percent"} <= contract.keys() and contract["small_loss_max_percent"] >= contract["medium_loss_max_percent"]:
        raise ValueError("loss size thresholds must be ordered")
    if "continuation_min_samples" in contract and (type(contract["continuation_min_samples"]) is not int or contract["continuation_min_samples"] < 2):
        raise ValueError("continuation sample count must be an integer >= 2")
    prepared = _prepare(rows)
    names = sorted({name for row in prepared for name in row["features"]})
    winners = [row for row in prepared if row["_normalized"] > 0]
    losers = [row for row in prepared if row["_normalized"] < 0]
    return {
        "schema_version": 1, "batch_id": batch_id, "status": "DISCOVERY_ONLY",
        "population": {"completed": len(prepared), "winners": len(winners), "losers": len(losers),
                       "breakeven": len(prepared) - len(winners) - len(losers)},
        "trades": [_forensic(row, contract) for row in prepared],
        "feature_comparison": {name: {"all": _distribution(prepared, name), "winners": _distribution(winners, name),
                                     "losers": _distribution(losers, name)} for name in names},
        "telemetry_contract": {domain: {"candidate_fields": list(fields),
                                        "any_available_trades": sum(any(name in row["_features"] for name in fields) for row in prepared),
                                        "population": len(prepared)} for domain, fields in DOMAIN_TELEMETRY.items()},
        "predictor_contract": "OBSERVED_AT_OR_BEFORE_DECISION_BEFORE_BUY_SOURCE_REQUIRED_OUTCOME_FIELDS_EXCLUDED",
        "unmeasured_outcome_questions": ["continuous_MAE_MFE", "true_fill_cost", "winner_post_entry_liquidity_and_flow_change"],
        "candidate_generation": _candidate_proposals(prepared, batch_id, screen_policy),
        "stage_contract": {"stage1": "NEXT_20_COMPLETED_FRESH_CONTROL", "stage2": "NEXT_ADDITIONAL_30_COMPLETED_FRESH_CONTROL",
                           "stage3": "HUMAN_REVIEW_AND_SEPARATELY_PREREGISTERED_LONGER_VALIDATION", "automatic_control_promotion": False},
    }


def cost_stress_report(rows: list[dict[str, Any]], decisions: dict[str, bool | None], *, scenarios: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """원장 비용은 재차 차감하지 않고 추가 비용 가정의 민감도만 계산한다."""
    from src.research.n3_shadow import CONTRACT

    prepared = _prepare(rows)
    selected = CONTRACT["cost_scenarios"] if scenarios is None else scenarios
    if not isinstance(selected, list) or len(selected) > 20:
        raise ValueError("cost scenarios must be bounded")
    reports = []
    names = set()
    for scenario in selected:
        if not isinstance(scenario, dict) or not isinstance(scenario.get("name"), str) or not scenario["name"] or scenario["name"] in names:
            raise ValueError("cost scenario names must be unique and nonempty")
        names.add(scenario["name"])
        bps = scenario.get("extra_round_trip_bps")
        fixed = scenario.get("fixed_round_trip_lamports")
        if not _number(bps) or bps < 0 or type(fixed) is not int or fixed < 0:
            raise ValueError("cost additions must be nonnegative finite values")
        # 1 lamport 미만 추가 비용은 올림해 정수 원가 계약을 유지한다.
        stressed = [{**row, "realized_pnl_lamports": row["realized_pnl_lamports"] - math.ceil(row["entry_cost_lamports"] * bps / 10_000) - fixed} for row in prepared]
        reports.append({"scenario": dict(scenario), "economic_report": economic_report(stressed, decisions)})
    return {"scenarios": reports, "semantics": "SENSITIVITY_NOT_REAL_FILL", "recorded_costs_double_counted": False,
            "unmeasured_components": {"network_cost": "NOT_SEPARATELY_MEASURED_FIXED_ADDITION_PROXY",
                                      "tip": "NOT_SEPARATELY_MEASURED_FIXED_ADDITION_PROXY",
                                      "slippage": "INCREMENTAL_BPS_PROXY", "adverse_execution": "INCREMENTAL_BPS_PROXY"},
            "additional_proxies_require_explicit_scenario_amounts": True}


def evaluate_stage(rows: list[dict[str, Any]], definition: dict[str, Any], stage: int) -> dict[str, Any]:
    """생성 배치를 제외한 고정 미래 구간만 사용하며 stage3는 자동 승격하지 않는다."""
    if stage not in (1, 2, 3):
        raise ValueError("stage must be 1, 2 or 3")
    from src.research.accelerated_alpha import digest, validate_definition, validate_rows

    definition = validate_definition(definition)
    prepared = _prepare(validate_rows(rows))
    start = _time(definition.get("start_timestamp"))
    seq = definition.get("start_event_seq")
    if type(seq) is not int or seq < 0:
        raise ValueError("start_event_seq must be nonnegative")
    kind = definition.get("kind")
    if kind not in {"negative_filter", "profitable_subset", "classification"}:
        raise ValueError("unsupported challenger kind")
    evidence = definition.get("discovery_evidence", {})
    discovery_ids = set(evidence.get("position_ids", []))
    eligible = [row for row in prepared if row["buy_event_seq"] >= seq and row["_signal"] >= start
                and row.get("batch_id") != definition.get("creation_batch") and row["position_id"] not in discovery_ids]
    eligible.sort(key=lambda row: (row["_closed"], row["buy_event_seq"], row["position_id"]))
    selected = eligible[:20] if stage == 1 else eligible[20:50] if stage == 2 else eligible[50:]
    result: dict[str, Any] = {"candidate_id": definition.get("candidate_id"), "stage": stage,
                              "future_only": True, "eligible_fresh_completed": len(eligible),
                              "selected_position_ids": [row["position_id"] for row in selected],
                              "selected_rows_hash": digest([{key: value for key, value in row.items() if not key.startswith("_")} for row in selected]),
                              "excluded_nonfresh_or_discovery": len(prepared) - len(eligible),
                              "automatic_control_promotion": False}
    if stage == 3:
        return {**result, "status": "HUMAN_REVIEW_REQUIRED", "reason": "SEPARATELY_PREREGISTER_STAGE3_CONTRACT"}
    decisions = _decisions(selected, definition["rule"], kind)
    if kind == "classification":
        return {**result, "status": "DISCOVERY_ONLY", "reason": "CLASSIFICATION_HAS_NO_ENTRY_ECONOMIC_COUNTERFACTUAL",
                "classification_matches": decisions}
    economy = economic_report(selected, decisions)
    result.update({"economic_report": economy, "cost_stress": cost_stress_report(selected, decisions),
                   "screens": _screens(selected, decisions)})
    screen_policy = evidence.get("discovery_screen_policy")
    if screen_policy is not None:
        result["confound_screen"] = discovery_screen(selected, definition["rule"], kind, screen_policy)
    required = 20 if stage == 1 else 30
    reasons = []
    status = "INSUFFICIENT_COVERAGE"
    if len(selected) < required:
        reasons.append("INSUFFICIENT_FRESH_COMPLETED")
    elif not economy["included_trades"] or not economy["excluded_trades"]:
        reasons.append("NO_COMPARABLE_FRESH_COVERAGE")
    elif economy["effects"]["equal_notional"]["net_effect"] <= 0:
        status = "REJECT_EARLY"
        reasons.append("NONPOSITIVE_NORMALIZED_EFFECT_OR_OPPORTUNITY_COST")
    elif economy["concentration"]["effect_after_removing_top_two_avoided_losses"] <= 0:
        status = "REJECT_EARLY"
        reasons.append("EFFECT_DEPENDS_ON_AT_MOST_TWO_TRADES")
    elif economy["concentration"]["actual_normalized_effect_sign_disagreement"]:
        status = "REJECT_EARLY"
        reasons.append("POSITION_SIZE_EFFECT_SIGN_CONFLICT")
    elif any(reason in {"SINGLE_TRADE_DEPENDENCE_FAILED", "SINGLE_DAY_DEPENDENCE_FAILED",
                        "ROUTE_DEPENDENCE_FAILED", "POSITION_SIZE_DEPENDENCE_FAILED"}
             for reason in result.get("confound_screen", {}).get("reasons", [])):
        status = "REJECT_EARLY"
        reasons.extend(result["confound_screen"]["reasons"])
    elif any(reason.endswith("_DEPENDENCE_UNMEASURED") for reason in result.get("confound_screen", {}).get("reasons", [])):
        reasons.append("FRESH_DEPENDENCE_TELEMETRY_UNMEASURED")
    else:
        policy = evidence.get("validation_policy")
        required_keys = {"minimum_decision_coverage", "minimum_exclusion_coverage", "maximum_missing_predictor_rate"}
        if not isinstance(policy, dict) or not required_keys <= policy.keys():
            reasons.append("NUMERIC_COVERAGE_POLICY_NOT_PREREGISTERED")
        else:
            if any(not _number(policy[key]) or not 0 <= policy[key] <= 1 for key in required_keys):
                raise ValueError("validation coverage policy must contain fractions")
            if economy["decision_coverage"] < policy["minimum_decision_coverage"] or economy["missing_predictor_rate"] > policy["maximum_missing_predictor_rate"] or economy["exclusion_coverage"] < policy["minimum_exclusion_coverage"]:
                reasons.append("PREREGISTERED_COVERAGE_OR_MISSING_GATE_FAILED")
            elif stage == 1:
                status = "FRESH_DIRECTION_CONFIRMED"
            else:
                first = evaluate_stage(rows, definition, 1)
                if first["status"] != "FRESH_DIRECTION_CONFIRMED":
                    status = "REJECT_EARLY"
                    reasons.append("STAGE1_NOT_CONFIRMED")
                elif not result["screens"]["temporal_split"]["both_positive"]:
                    status = "FRESH_DIRECTION_CONFIRMED"
                    reasons.append("STAGE2_TEMPORAL_INSTABILITY")
                elif economy["included"]["normalized"]["expectancy"] <= 0:
                    status = "ONLY_NEGATIVE_FILTER"
                    reasons.append("RETAINED_STRATEGY_NONPOSITIVE_EXPECTANCY")
                elif any(
                    scenario["economic_report"]["effects"]["equal_notional"]["net_effect"] <= 0
                    or (scenario["economic_report"]["included"]["normalized"]["expectancy"] or 0) <= 0
                    for scenario in result["cost_stress"]["scenarios"]
                ):
                    status = "FRESH_DIRECTION_CONFIRMED"
                    reasons.append("STAGE2_COST_STRESS_FRAGILE")
                else:
                    status = "PROMISING_CHALLENGER"
    result.update({"status": status, "reasons": reasons, "success_claim": "POINT_ESTIMATE_DIRECTION_ONLY_NO_ROBUST_ALPHA_CLAIM"})
    return result
