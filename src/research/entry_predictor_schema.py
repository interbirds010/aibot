"""Predictor의 재귀 허용 구조. 임의 result/position 객체는 보존하지 않는다."""
from __future__ import annotations

import math
import re
from itertools import islice

SCALAR = "scalar"
RAW = {key: SCALAR for key in (
    "volume_operand_before_cap", "volume_points", "positive_net_buys",
    "imbalance_operand_before_cap", "imbalance_points", "volume_m5_usd",
    "volume_operand", "net_buy_count", "net_buy_points", "net_buy_operand",
    "volume", "developer_supply_percent_raw", "lp_locked_percent_raw", "liquidity_usd_raw")}
SCORE = {"raw_components": RAW, **{key: SCALAR for key in (
    "capped_total", "uncapped_total", "uncapped_total_missing_reason", "threshold_result", "missing_reason")}}
SNAPSHOT = {key: SCALAR for key in (
    "snapshot_at_epoch", "volume_m5_usd", "buys_m5", "sells_m5", "liquidity_usd", "price_usd")}
COMPONENTS = {key: SCALAR for key in ("mint_authority", "developer_holding", "lp_lock")}
THRESHOLDS = {key: SCALAR for key in ("minimum_safety_score", "maximum_developer_percent",
    "minimum_lp_locked_percent", "minimum_liquidity_usd", "route_b_minimum_liquidity_usd")}
DENIED = frozenset({"realizedpnl", "pnl", "profit", "loss", "exit", "sell", "closed",
    "outcome", "winner", "loser", "mae", "mfe", "horizon", "future", "finalstatus",
    "exitreason", "finalreturn", "postentry", "currentwalletperformance", "futurewalletperformance"})
SAFE_COLLISIONS = frozenset({"sell_count", "sells_m5", "quote_exit_preflight"})


def forbidden(key):
    """완전한 의미 토큰을 검사한다. entry-time sell count는 허용한다."""
    if key in SAFE_COLLISIONS:
        return False
    words = re.findall(r"[a-z0-9]+", re.sub(r"([a-z])([A-Z])", r"\1_\2", key).lower())
    normalized = "".join(words)
    return normalized in DENIED or bool(set(words) & DENIED) or any(
        word in normalized for word in ("secret", "apikey", "privatekey", "password", "cookie", "rpcurl", "authorization"))


def section_shapes(fields):
    """모든 section/key의 중첩 타입을 고정한다. scalar에 dict를 넣을 수 없다."""
    shapes = {name: {key: SCALAR for key in keys} for name, keys in fields.items()}
    shapes["scores"].update({"safety": SCORE, "momentum": SCORE, "raw_components": RAW})
    shapes["signal"]["prefilter_reasons"] = [SCALAR]
    for key in ("wallet_hashes", "wallet_ids", "participating_wallet_ids"):
        shapes["wallets"][key] = [SCALAR]
    shapes["wallets"]["paid_lamports_by_wallet"] = "numeric_wallet_map"
    shapes["wallets"]["contributions"] = [{"wallet_id": SCALAR, "paid_lamports": SCALAR}]
    shapes["short_flow"]["windows"] = [{key: SCALAR for key in ("window_seconds", "buy_count", "sell_count", "combined_volume_usd")}]
    shapes["trajectory"].update({"history": [SNAPSHOT], "pre_signal_snapshots": [SNAPSHOT]})
    for key in ("actualrawinputs", "raw_inputs"):
        shapes["safety_components"][key] = RAW
    for key in ("actualallocated", "allocated_components", "components"):
        shapes["safety_components"][key] = COMPONENTS
    shapes["safety_components"]["thresholds"] = THRESHOLDS
    for name in ("quote_buy", "quote_exit_preflight"):
        shapes[name]["dex_identifiers"] = [(SCALAR, {"ammKey": SCALAR, "label": SCALAR})]
    shapes["decision"]["reasons"] = [SCALAR]
    return shapes


def project(value, shape, scalar, *, depth=0, max_items=128, budget=None):
    if value is None:
        return None
    if shape == SCALAR:
        return scalar(value, budget=budget) if isinstance(value, (str, int, float, bool)) else None
    if depth >= 5:
        return None
    if shape == "numeric_wallet_map":
        if not isinstance(value, dict):
            return None
        return {key[:256]: scalar(item, budget=budget) for key, item in islice(value.items(), max_items)
                if isinstance(key, str) and len(key) <= 256 and not forbidden(key)
                and isinstance(item, (int, float)) and not isinstance(item, bool) and math.isfinite(item)}
    if isinstance(shape, dict):
        if not isinstance(value, dict):
            return None
        return {key: project(value[key], child, scalar, depth=depth + 1, max_items=max_items, budget=budget)
                for key, child in shape.items() if key in value and (not forbidden(key) or key == "outcome")}
    if isinstance(shape, tuple):
        return project(value, shape[1] if isinstance(value, dict) else shape[0], scalar,
                       depth=depth, max_items=max_items, budget=budget)
    if isinstance(shape, list):
        if not isinstance(value, (list, tuple)):
            return None
        return [project(item, shape[0], scalar,
                        depth=depth + 1, max_items=max_items, budget=budget) for item in value[:max_items]]
    return None
