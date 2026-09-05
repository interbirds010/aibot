"""Final pre-entry safety gate for a detected Solana token mint."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass, field, replace
from decimal import Decimal, InvalidOperation
from typing import Any

import aiohttp
from dotenv import load_dotenv
from solders.pubkey import Pubkey
from src.logging_utils import configure_safe_logging, redact_sensitive_text
from src.solana_rpc import provider_configs_from_env, solana_rpc_call

RUGCHECK_BASE = "https://api.rugcheck.xyz/v1/tokens"
ROUTE_B_MINIMUM_LIQUIDITY_USD = Decimal("10000")
ANALYZER_CACHE_TTL_SECONDS = 5.0
ANALYZER_CACHE_MAX_ENTRIES = 128
logger = logging.getLogger("analyzer")


@dataclass(frozen=True, slots=True)
class AnalyzerSettings:
    rpc_url: str
    minimum_safety_score: int = 70
    maximum_developer_percent: Decimal = Decimal("10")
    minimum_lp_locked_percent: Decimal = Decimal("80")
    minimum_liquidity_usd: Decimal = Decimal("10000")

    @classmethod
    def from_env(cls) -> "AnalyzerSettings":
        load_dotenv()
        if not provider_configs_from_env():
            raise RuntimeError("at least one Solana RPC provider must be configured")
        return cls(rpc_url="solana-rpc-router")


@dataclass(slots=True)
class SafetyReport:
    mint: str
    safety_score: int = 0
    should_enter: bool = False
    route_type: str | None = None
    developer_wallet: str | None = None
    developer_supply_percent: str | None = None
    developer_below_ten_percent: bool = False
    mint_authority_renounced: bool = False
    lp_locked: bool = False
    lp_locked_percent: str | None = None
    liquidity_usd: str | None = None
    liquidity_above_minimum: bool = False
    reasons: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)


async def rpc_call(
    session: aiohttp.ClientSession, url: str, method: str, params: list[Any]
) -> Any:
    del url  # Backward-compatible caller signature; routing owns endpoints.
    return await solana_rpc_call(
        session,
        method,
        params,
        workload="analyzer",
    )


async def rugcheck_get(
    session: aiohttp.ClientSession, mint: str, suffix: str
) -> dict[str, Any] | None:
    url = f"{RUGCHECK_BASE}/{mint}/{suffix}"
    delay = 2
    for attempt in range(4):
        async with session.get(url, headers={"accept": "application/json"}) as response:
            if response.status == 404:
                return None
            if response.status == 429 or response.status >= 500:
                if attempt == 3:
                    return None
            else:
                response.raise_for_status()
                payload = await response.json()
                return payload if isinstance(payload, dict) else None
        await asyncio.sleep(delay)
        delay *= 2
    return None


def mint_authority(result: dict[str, Any] | None) -> str | None:
    value = (result or {}).get("value")
    if not isinstance(value, dict):
        raise ValueError("mint account does not exist")
    data = value.get("data") or {}
    parsed = data.get("parsed") if isinstance(data, dict) else None
    if not isinstance(parsed, dict) or parsed.get("type") != "mint":
        raise ValueError("address is not an SPL token mint")
    info = parsed.get("info") or {}
    return info.get("mintAuthority")


def decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def developer_percentage(report: dict[str, Any] | None, supply_raw: int) -> tuple[str | None, Decimal | None]:
    """Calculate the direct creator wallet's share of total supply.

    Rugcheck's creatorBalance is preferred. If absent, sum top-holder entries
    explicitly attributed to the creator. Related/hidden wallets cannot be
    proven by this check and require a separate funding-graph analysis.
    """
    if not report or supply_raw <= 0:
        return None, None
    creator = report.get("creator")
    creator = str(creator) if creator else None
    raw_balance = decimal_or_none(report.get("creatorBalance"))
    if raw_balance is not None:
        return creator, raw_balance * 100 / Decimal(supply_raw)

    percentages: list[Decimal] = []
    for holder in report.get("topHolders") or []:
        if not isinstance(holder, dict):
            continue
        owner = holder.get("owner") or holder.get("ownerAddress")
        if creator and owner == creator:
            pct = decimal_or_none(holder.get("pct") or holder.get("percentage"))
            if pct is not None:
                percentages.append(pct)
    return creator, sum(percentages, Decimal(0)) if percentages else None


def locked_percentage(
    summary: dict[str, Any] | None, report: dict[str, Any] | None
) -> Decimal | None:
    """Use Rugcheck's aggregate LP lock percentage, then a market-level fallback."""
    for container in (summary, report):
        if not container:
            continue
        value = decimal_or_none(container.get("lpLockedPct"))
        if value is not None:
            return value

    percentages: list[Decimal] = []
    for market in (report or {}).get("markets") or []:
        if not isinstance(market, dict):
            continue
        lp = market.get("lp") or {}
        if not isinstance(lp, dict):
            continue
        value = decimal_or_none(lp.get("lpLockedPct"))
        if value is None:
            locked = decimal_or_none(lp.get("lpLocked"))
            unlocked = decimal_or_none(lp.get("lpUnlocked"))
            if locked is not None and unlocked is not None and locked + unlocked > 0:
                value = locked * 100 / (locked + unlocked)
        if value is not None:
            percentages.append(value)
    # If Rugcheck has no aggregate, every discovered market must satisfy the
    # threshold; one locked dust pool must not hide a larger unlocked pool.
    return min(percentages) if percentages else None


def liquidity_usd(report: dict[str, Any] | None) -> Decimal | None:
    """Read Rugcheck's already-fetched USD liquidity without another RPC call."""
    if not report:
        return None
    for key in ("totalMarketLiquidity", "totalLiquidity", "liquidityUsd", "liquidityUSD"):
        value = decimal_or_none(report.get(key))
        if value is not None:
            return value
    values: list[Decimal] = []
    for market in report.get("markets") or []:
        if not isinstance(market, dict):
            continue
        liquidity = market.get("liquidity")
        candidates = (
            market.get("liquidityUsd"), market.get("liquidityUSD"),
            liquidity.get("usd") if isinstance(liquidity, dict) else None,
        )
        value = next((parsed for raw in candidates if (parsed := decimal_or_none(raw)) is not None), None)
        if value is not None:
            values.append(value)
    return sum(values, Decimal(0)) if values else None


def select_route_type(
    *,
    safety_score: int,
    developer_below_ten_percent: bool,
    mint_authority_renounced: bool,
    lp_locked_percent: Decimal | None,
    liquidity: Decimal | None,
    settings: AnalyzerSettings,
) -> str | None:
    """Select one fail-closed execution route from verified safety inputs."""
    route_a = (
        safety_score >= settings.minimum_safety_score
        and developer_below_ten_percent
        and mint_authority_renounced
        and lp_locked_percent is not None
        and lp_locked_percent >= settings.minimum_lp_locked_percent
        and liquidity is not None
        and liquidity >= settings.minimum_liquidity_usd
    )
    route_b = (
        developer_below_ten_percent
        and mint_authority_renounced
        and lp_locked_percent is not None
        and liquidity is not None
        and liquidity >= ROUTE_B_MINIMUM_LIQUIDITY_USD
        and (
            lp_locked_percent < settings.minimum_lp_locked_percent
            or liquidity < settings.minimum_liquidity_usd
        )
    )
    if route_a:
        return "A"
    if route_b:
        return "B"
    return None


async def _analyze_token_uncached(
    mint: str, settings: AnalyzerSettings | None = None
) -> SafetyReport:
    """Return a detailed, fail-closed safety report for a detected mint."""
    settings = settings or AnalyzerSettings.from_env()
    try:
        Pubkey.from_string(mint)
    except ValueError as exc:
        raise ValueError(f"invalid mint address: {mint}") from exc

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        account, supply, rug_report, rug_summary = await asyncio.gather(
            rpc_call(session, settings.rpc_url, "getAccountInfo", [
                mint, {"encoding": "jsonParsed", "commitment": "finalized"}
            ]),
            rpc_call(session, settings.rpc_url, "getTokenSupply", [
                mint, {"commitment": "finalized"}
            ]),
            rugcheck_get(session, mint, "report"),
            rugcheck_get(session, mint, "report/summary"),
        )

    result = SafetyReport(mint=mint, sources=["solana_rpc_router_finalized"])
    authority = mint_authority(account)
    result.mint_authority_renounced = authority is None
    if result.mint_authority_renounced:
        result.safety_score += 35
    else:
        result.reasons.append("Mint authority has not been renounced")

    supply_raw = int((((supply or {}).get("value") or {}).get("amount")) or "0")
    creator, creator_pct = developer_percentage(rug_report, supply_raw)
    result.developer_wallet = creator
    if creator_pct is not None:
        result.developer_supply_percent = str(creator_pct.quantize(Decimal("0.01")))
        result.developer_below_ten_percent = creator_pct < settings.maximum_developer_percent
        if result.developer_below_ten_percent:
            result.safety_score += 30
        else:
            result.reasons.append(f"Developer wallet holds {creator_pct:.2f}% of supply")
    else:
        result.reasons.append("Developer wallet balance could not be verified")

    lp_pct = locked_percentage(rug_summary, rug_report)
    if lp_pct is not None:
        result.lp_locked_percent = str(lp_pct.quantize(Decimal("0.01")))
        result.lp_locked = lp_pct >= settings.minimum_lp_locked_percent
        if result.lp_locked:
            result.safety_score += 35
        else:
            result.reasons.append(f"Only {lp_pct:.2f}% of LP is locked/burned")
    else:
        result.reasons.append("LP lock status could not be verified")

    liquidity = liquidity_usd(rug_report)
    if liquidity is not None:
        result.liquidity_usd = str(liquidity.quantize(Decimal("0.01")))
        result.liquidity_above_minimum = liquidity >= settings.minimum_liquidity_usd
    if not result.liquidity_above_minimum:
        result.reasons.append(
            "Liquidity is below $10,000" if liquidity is not None else "USD liquidity could not be verified"
        )

    if rug_report is not None or rug_summary is not None:
        result.sources.append("rugcheck")
    result.route_type = select_route_type(
        safety_score=result.safety_score,
        developer_below_ten_percent=result.developer_below_ten_percent,
        mint_authority_renounced=result.mint_authority_renounced,
        lp_locked_percent=lp_pct,
        liquidity=liquidity,
        settings=settings,
    )
    result.should_enter = result.route_type is not None
    return result


_analysis_cache: dict[
    tuple[str, AnalyzerSettings], tuple[float, SafetyReport]
] = {}
_analysis_flights: dict[
    tuple[str, AnalyzerSettings], asyncio.Task[SafetyReport]
] = {}
_analysis_cache_lock = asyncio.Lock()


def analyzer_runtime_metrics() -> dict[str, int]:
    """분석기 single-flight/cache의 크기만 노출하는 운영 진단 지표다."""
    return {
        "monitor_analyzer_cache_entry_count": len(_analysis_cache),
        "monitor_analyzer_flight_task_count": len(_analysis_flights),
        "monitor_analyzer_done_flight_task_count": sum(
            task.done() for task in _analysis_flights.values()
        ),
    }


def _copy_safety_report(report: SafetyReport) -> SafetyReport:
    return replace(
        report,
        reasons=list(report.reasons),
        sources=list(report.sources),
    )


def _consume_flight_exception(task: asyncio.Task[SafetyReport]) -> None:
    """모든 대기자가 취소돼도 shared task 예외 경고를 남기지 않는다."""
    if not task.cancelled():
        task.exception()


async def _run_analysis_flight(
    key: tuple[str, AnalyzerSettings],
    mint: str,
    settings: AnalyzerSettings,
) -> SafetyReport:
    try:
        report = await _analyze_token_uncached(mint, settings)
        async with _analysis_cache_lock:
            now = time.monotonic()
            expired = [
                cache_key
                for cache_key, (expires_at, _) in _analysis_cache.items()
                if expires_at <= now
            ]
            for cache_key in expired:
                _analysis_cache.pop(cache_key, None)
            if len(_analysis_cache) >= ANALYZER_CACHE_MAX_ENTRIES:
                oldest = min(
                    _analysis_cache,
                    key=lambda cache_key: _analysis_cache[cache_key][0],
                )
                _analysis_cache.pop(oldest, None)
            _analysis_cache[key] = (
                now + ANALYZER_CACHE_TTL_SECONDS,
                _copy_safety_report(report),
            )
        return report
    finally:
        async with _analysis_cache_lock:
            current = asyncio.current_task()
            if _analysis_flights.get(key) is current:
                _analysis_flights.pop(key, None)


async def analyze_token(
    mint: str, settings: AnalyzerSettings | None = None
) -> SafetyReport:
    """동일 mint의 동시 분석은 합치고 성공 결과만 5초 동안 재사용한다."""
    resolved_settings = settings or AnalyzerSettings.from_env()
    try:
        Pubkey.from_string(mint)
    except ValueError as exc:
        raise ValueError(f"invalid mint address: {mint}") from exc
    key = (mint, resolved_settings)
    async with _analysis_cache_lock:
        now = time.monotonic()
        cached = _analysis_cache.get(key)
        if cached is not None:
            expires_at, report = cached
            if expires_at > now:
                return _copy_safety_report(report)
            _analysis_cache.pop(key, None)
        task = _analysis_flights.get(key)
        if task is None:
            task = asyncio.create_task(
                _run_analysis_flight(key, mint, resolved_settings)
            )
            task.add_done_callback(_consume_flight_exception)
            _analysis_flights[key] = task
    return _copy_safety_report(await asyncio.shield(task))


async def should_enter_token(mint: str) -> bool:
    """Return True only when all required checks produce a score of at least 70."""
    try:
        return (await analyze_token(mint)).should_enter
    except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError, ValueError):
        return False


async def async_main(mint: str) -> int:
    report = await analyze_token(mint)
    print(json.dumps(asdict(report), ensure_ascii=False, indent=2))
    return 0 if report.should_enter else 2


def main() -> None:
    configure_safe_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mint", help="SPL token Mint address (CA)")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(async_main(args.mint)))


if __name__ == "__main__":
    main()
