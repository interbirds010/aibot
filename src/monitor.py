"""Real-time smart-money DEX transaction monitor using Helius Enhanced WSS."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import subprocess
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import aiohttp
from dotenv import load_dotenv
from solders.pubkey import Pubkey
from websockets.asyncio.client import ClientConnection, connect

from src import state_store

logger = logging.getLogger("smart-money-monitor")

LAMPORTS_PER_SOL = Decimal(1_000_000_000)
WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
WALLETS_PATH = Path(__file__).resolve().parents[1] / "data" / "wallets.json"
PAPER_BUY_BASIS_POINTS = 50  # 0.5% of currently available virtual cash.
SINGLE_STRENGTH_LAMPORTS = 1_500_000_000
MIN_ACCUMULATION_TRADE_LAMPORTS = 1_000_000_000
ACCUMULATION_TARGET_LAMPORTS = 5_000_000_000
ACCUMULATION_WINDOW_SECONDS = 180.0
DEX_SCREENER_POLL_SECONDS = 5.0
DEX_SCREENER_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"
DEX_SCREENER_PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
DEX_SCREENER_BOOSTS_URL = "https://api.dexscreener.com/token-boosts/top/v1"
DEX_SCREENER_TOKENS_URL = "https://api.dexscreener.com/tokens/v1/solana"
MOMENTUM_MIN_VOLUME_M5_USD = 10_000.0
MOMENTUM_MIN_NET_BUYS_M5 = 10
MOMENTUM_MIN_LIQUIDITY_USD = 7_500.0
MOMENTUM_MAX_DISCOVERY_TOKENS = 30
MOMENTUM_MAX_CANDIDATES = 8
MOMENTUM_ENTRY_COOLDOWN_SECONDS = 2_700.0
UNKNOWN_WHALE_MIN_COUNT = 3
UNKNOWN_WHALE_SIGNATURE_LIMIT = 12
ROUTE_B_MIN_SAFETY_SCORE = 55
ROUTE_B_MIN_LIQUIDITY_USD = 7_500.0
ROUTE_B_MIN_LP_LOCKED_PERCENT = 40.0
TOKEN_TRADE_COOLDOWN_SECONDS = 2_700.0
WALLET_TARGET_COUNT = 20
WALLET_FEEDER_TRIGGER_COUNT = 17
WALLET_FEEDER_COOLDOWN_SECONDS = 7_200.0
MONITOR_MAINTENANCE_INTERVAL_SECONDS = 60.0

# On-chain feeder output is verified; fail closed if an old unverified row remains.
TEST_ALLOW_UNVERIFIED_WALLETS = False
_analysis_limit = asyncio.Semaphore(2)
_signal_tasks: set[asyncio.Task[None]] = set()
_whale_buy_history: dict[tuple[str, str], deque[tuple[float, int]]] = {}
_last_history_cleanup_at = 0.0
_market_entry_cooldowns: dict[str, float] = {}

# Canonical mainnet program IDs. Keep this list reviewed before production use.
DEX_PROGRAMS = {
    "Pump.fun": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
    "PumpSwap": "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
    "Raydium AMM v4": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "Raydium CPMM": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
    "Raydium CLMM": "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
    "Raydium Stable": "5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h",
}


@dataclass(frozen=True, slots=True)
class MonitorSettings:
    ws_url: str
    http_url: str
    wallets_path: Path = WALLETS_PATH
    wallet_reload_seconds: float = 5.0

    @classmethod
    def from_env(cls) -> "MonitorSettings":
        load_dotenv()
        api_key = os.getenv("HELIUS_API_KEY", "").strip()
        ws_url = os.getenv("HELIUS_RPC_WS_URL", "").strip()
        ws_url = ws_url.replace("${HELIUS_API_KEY}", api_key)
        http_url = os.getenv("HELIUS_RPC_HTTP_URL", "").strip()
        http_url = http_url.replace("${HELIUS_API_KEY}", api_key)
        if not api_key or not ws_url or not http_url:
            raise RuntimeError("Helius API key, WSS URL and HTTP URL must be set in .env")
        reload_seconds = max(1.0, float(os.getenv("WALLET_RELOAD_SECONDS", "5")))
        return cls(ws_url=ws_url, http_url=http_url, wallet_reload_seconds=reload_seconds)


@dataclass(frozen=True, slots=True)
class MomentumCandidate:
    mint: str
    pair_address: str
    volume_m5_usd: float
    buys_m5: int
    sells_m5: int
    liquidity_usd: float
    momentum_score: float


@dataclass(frozen=True, slots=True)
class UnknownWhaleBuy:
    wallet: str
    signature: str
    paid_lamports: int
    token_amount_raw: int
    token_decimals: int


def load_wallets(path: Path) -> tuple[str, ...]:
    """Load and validate feeder output; accepts detailed objects or plain strings."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"Wallet file not found: {path}. Run 'python -m src.wallet_feeder --once' first."
        ) from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Wallet file contains invalid JSON: {path}") from exc

    entries = document.get("wallets", []) if isinstance(document, dict) else document
    if not isinstance(entries, list):
        raise RuntimeError("wallets.json must contain a 'wallets' list")
    wallets: list[str] = []
    for entry in entries:
        if (
            isinstance(entry, dict)
            and entry.get("verified") is False
            and not TEST_ALLOW_UNVERIFIED_WALLETS
        ):
            continue
        address = entry.get("address") if isinstance(entry, dict) else entry
        if not isinstance(address, str):
            continue
        try:
            Pubkey.from_string(address)
        except ValueError:
            logger.warning("ignoring invalid wallet in %s: %s", path, address)
            continue
        wallets.append(address)
    result = tuple(dict.fromkeys(wallets))
    if not result:
        raise RuntimeError(f"No valid wallets found in {path}")
    return result


def whale_buy_amount_allowed(
    paid_lamports: int,
    *,
    wallet: str = "unknown",
    mint: str = "unknown",
    signature: str = "unknown",
    observed_at: float | None = None,
) -> bool:
    """Apply the single-strength or per-wallet/token 3-minute accumulation gate.

    ``time.monotonic`` keeps expiry independent of local timezone or wall-clock
    adjustments. This function contains no await, so deque mutation is atomic
    with respect to other tasks on the monitor's single asyncio event loop.
    """
    global _last_history_cleanup_at
    now = time.monotonic() if observed_at is None else observed_at
    if now - _last_history_cleanup_at >= 60.0:
        global_cutoff = now - ACCUMULATION_WINDOW_SECONDS
        for history_key, buffered in list(_whale_buy_history.items()):
            while buffered and buffered[0][0] < global_cutoff:
                buffered.popleft()
            if not buffered:
                _whale_buy_history.pop(history_key, None)
        _last_history_cleanup_at = now
    key = (wallet, mint)
    history = _whale_buy_history.setdefault(key, deque())
    cutoff = now - ACCUMULATION_WINDOW_SECONDS
    while history and history[0][0] < cutoff:
        history.popleft()

    # Trades at or below 1 SOL are bait/noise: clean old state, but never add
    # them to accumulation and never approve the current signal.
    if paid_lamports <= MIN_ACCUMULATION_TRADE_LAMPORTS:
        if not history:
            _whale_buy_history.pop(key, None)
        logger.info(
            "[FILTER] Low intensity whale trade. Skip. "
            "amount=%.9f SOL wallet=%s mint=%s signature=%s",
            paid_lamports / 1_000_000_000,
            wallet,
            mint,
            signature,
        )
        return False

    history.append((now, paid_lamports))
    if paid_lamports >= SINGLE_STRENGTH_LAMPORTS:
        logger.info(
            "[BUY_SIGNAL] Approved by Single Strength (%.2f SOL) "
            "wallet=%s mint=%s signature=%s",
            paid_lamports / 1_000_000_000,
            wallet,
            mint,
            signature,
        )
        return True

    accumulated_lamports = sum(amount for _, amount in history)
    if accumulated_lamports >= ACCUMULATION_TARGET_LAMPORTS:
        logger.info(
            "[BUY_SIGNAL] Approved by 3Min Accumulation (%.2f SOL Total) "
            "wallet=%s mint=%s signature=%s",
            accumulated_lamports / 1_000_000_000,
            wallet,
            mint,
            signature,
        )
        # Do not reuse the same accumulated trades to approve later signals.
        _whale_buy_history.pop(key, None)
        return True

    logger.info(
        "[FILTER] Low intensity whale trade. Skip. "
        "amount=%.9f SOL wallet=%s mint=%s signature=%s",
        paid_lamports / 1_000_000_000,
        wallet,
        mint,
        signature,
    )
    return False


class WalletListChanged(Exception):
    """Signal that subscriptions must be rebuilt from an updated wallet file."""


class EnhancedSubscriptionUnavailable(Exception):
    """Signal that the Helius plan requires standard WebSocket fallback."""


async def watch_wallet_file(
    path: Path, initial_mtime_ns: int, reload_seconds: float = 5.0
) -> None:
    while True:
        await asyncio.sleep(reload_seconds)
        try:
            current_mtime_ns = path.stat().st_mtime_ns
        except FileNotFoundError:
            continue
        if current_mtime_ns != initial_mtime_ns:
            raise WalletListChanged(f"wallet list changed: {path}")


async def monitor_heartbeat(wallet_count: int) -> None:
    while True:
        logger.info("heartbeat: actively monitoring %s wallets", wallet_count)
        await asyncio.sleep(60)


def _float_or_none(value: Any) -> float | None:
    """외부 분석값을 유한한 실수로 안전하게 정규화한다."""
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in {float("inf"), float("-inf")}:
        return None
    return number


def route_b_safety_filter(report: Any, mint: str) -> bool:
    """B 경로의 강화된 안전점수, 유동성, LP 잠금 하한을 검증한다."""
    safety_score = _float_or_none(getattr(report, "safety_score", None))
    liquidity = _float_or_none(getattr(report, "liquidity_usd", None))
    lp_locked = _float_or_none(getattr(report, "lp_locked_percent", None))
    if safety_score is None or safety_score < ROUTE_B_MIN_SAFETY_SCORE:
        logger.warning(
            "FAIL_SAFETY_SCORE_UNDER_55 mint=%s safety_score=%s threshold=%s",
            mint,
            safety_score,
            ROUTE_B_MIN_SAFETY_SCORE,
        )
        return False
    if liquidity is None or liquidity < ROUTE_B_MIN_LIQUIDITY_USD:
        logger.warning(
            "FAIL_LIQUIDITY_UNDER_7500 mint=%s liquidity_usd=%s threshold=%s",
            mint,
            liquidity,
            ROUTE_B_MIN_LIQUIDITY_USD,
        )
        return False
    if lp_locked is None or lp_locked < ROUTE_B_MIN_LP_LOCKED_PERCENT:
        logger.warning(
            "FAIL_LP_LOCKED_UNDER_40 mint=%s lp_locked_percentage=%s threshold=%s",
            mint,
            lp_locked,
            ROUTE_B_MIN_LP_LOCKED_PERCENT,
        )
        return False
    return True


def token_cooldown_is_active(mint: str, now: float | None = None) -> bool:
    """공통 거래 원장을 기준으로 동일 토큰의 45분 재진입을 차단한다."""
    current = float(now if now is not None else time.time())
    try:
        last_trade_time = float(state_store.get_last_trade_time(mint) or 0)
    except Exception:
        logger.exception("token cooldown lookup failed; fail-closed mint=%s", mint)
        return True
    elapsed = current - last_trade_time
    if last_trade_time > 0 and elapsed < TOKEN_TRADE_COOLDOWN_SECONDS:
        logger.info(
            "FAIL_TOKEN_COOLDOWN_ACTIVE mint=%s elapsed_seconds=%.3f "
            "required_seconds=%s last_trade_time=%.3f",
            mint,
            elapsed,
            TOKEN_TRADE_COOLDOWN_SECONDS,
            last_trade_time,
        )
        return True
    return False


def trigger_wallet_feeder_if_needed(now: float | None = None) -> bool:
    """감시 지갑 부족 시 두 시간에 한 번만 PM2 공급기를 비동기 기동한다."""
    current = float(now if now is not None else time.time())
    try:
        active_count = state_store.get_active_wallets_count()
        if active_count > WALLET_FEEDER_TRIGGER_COUNT:
            return False
        raw_last_run = state_store.get_global_metric(
            "last_wallet_feeder_run_time",
            0,
        )
        try:
            last_run = float(raw_last_run or 0)
        except (TypeError, ValueError):
            last_run = 0.0
        elapsed = current - last_run
        if elapsed < WALLET_FEEDER_COOLDOWN_SECONDS:
            logger.info(
                "wallet feeder trigger skipped: active_wallets=%s threshold=%s "
                "cooldown_remaining_seconds=%.3f",
                active_count,
                WALLET_FEEDER_TRIGGER_COUNT,
                WALLET_FEEDER_COOLDOWN_SECONDS - elapsed,
            )
            return False
        if not state_store.claim_global_interval(
            "last_wallet_feeder_run_time",
            current,
            WALLET_FEEDER_COOLDOWN_SECONDS,
        ):
            logger.info(
                "wallet feeder trigger already claimed by another process: "
                "active_wallets=%s",
                active_count,
            )
            return False
        subprocess.Popen(
            ["pm2", "start", "wallet_feeder"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True,
        )
        logger.warning(
            "wallet feeder started asynchronously: active_wallets=%s "
            "target_wallets=%s trigger_threshold=%s",
            active_count,
            WALLET_TARGET_COUNT,
            WALLET_FEEDER_TRIGGER_COUNT,
        )
        return True
    except FileNotFoundError:
        logger.exception("wallet feeder trigger failed: pm2 executable not found")
    except Exception:
        logger.exception("wallet feeder trigger failed")
    return False


async def monitor_maintenance_loop() -> None:
    """모니터 프로세스 안에서 쿨다운 복구와 공급 공백 복원을 수행한다."""
    from src.wallet_performance import self_recovery_cooldown_wallets

    while True:
        started = time.monotonic()
        try:
            restored = self_recovery_cooldown_wallets()
            if restored:
                logger.info(
                    "wallet cooldown self-recovery completed: restored=%s",
                    restored,
                )
            trigger_wallet_feeder_if_needed()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("monitor maintenance cycle failed")
        elapsed = time.monotonic() - started
        await asyncio.sleep(
            max(1.0, MONITOR_MAINTENANCE_INTERVAL_SECONDS - elapsed)
        )


def subscription_request(request_id: int, wallets: tuple[str, ...], program: str) -> dict[str, Any]:
    """Require one watched wallet AND the given DEX program at the RPC layer."""
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "transactionSubscribe",
        "params": [
            {
                "vote": False,
                "failed": False,
                "accountInclude": list(wallets),
                "accountRequired": [program],
            },
            {
                "commitment": "confirmed",
                "encoding": "jsonParsed",
                "transactionDetails": "full",
                "showRewards": False,
                "maxSupportedTransactionVersion": 0,
            },
        ],
    }


def account_keys(message: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    for item in message.get("accountKeys", []):
        keys.append(item.get("pubkey", "") if isinstance(item, dict) else str(item))
    return keys


def raw_token_amount(balance: dict[str, Any]) -> tuple[int, int]:
    ui = balance.get("uiTokenAmount") or {}
    return int(ui.get("amount", "0")), int(ui.get("decimals", 0))


def wallet_token_deltas(meta: dict[str, Any], wallet: str) -> dict[str, tuple[int, int]]:
    """Return exact raw SPL-token balance deltas, including newly created ATAs."""
    totals: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
    for side, sign in (("preTokenBalances", -1), ("postTokenBalances", 1)):
        for balance in meta.get(side) or []:
            if balance.get("owner") != wallet:
                continue
            mint = balance.get("mint")
            if not mint:
                continue
            amount, decimals = raw_token_amount(balance)
            totals[mint][0] += sign * amount
            totals[mint][1] = decimals
            totals[mint][2] += 1
    return {mint: (values[0], values[1]) for mint, values in totals.items() if values[0]}


def decimal_amount(raw: int, decimals: int) -> Decimal:
    return Decimal(raw) / (Decimal(10) ** decimals)


def signature_of(result: dict[str, Any]) -> str:
    transaction = result.get("transaction") or {}
    signatures = transaction.get("signatures") or []
    return str(signatures[0]) if signatures else "unknown"


def route_report_allowed(requested_route: str, analyzed_route: str | None) -> bool:
    """Keep whale Route A strict while allowing safe market candidates on B."""
    if requested_route == "A":
        return analyzed_route == "A"
    if requested_route == "B":
        return analyzed_route in {"A", "B"}
    return False


async def process_paper_signal(
    mint: str,
    whale_token_amount_raw: int,
    token_decimals: int,
    whale_paid_lamports: int,
    wallet: str,
    signature: str,
    signal_detected_at: str,
    requested_route: str = "A",
    dex_momentum_score: float = 0.0,
) -> None:
    """Run the rug gate, then record an executable Jupiter paper-buy quote."""
    async with _analysis_limit:
        try:
            if requested_route == "B" and token_cooldown_is_active(mint):
                return
            from src.analyzer import analyze_token
            from src.risk_manager import (
                paper_cash_balance,
                record_paper_buy,
                record_paper_rejection,
                record_rpc_skip,
            )

            report = await analyze_token(mint)
            analysis_completed_at = datetime.now(timezone.utc).isoformat()
            if requested_route == "B" and not route_b_safety_filter(report, mint):
                return
            route_allowed = route_report_allowed(requested_route, report.route_type)
            if not route_allowed:
                if requested_route == "A":
                    from src.wallet_performance import reject_unsafe_buy
                    await reject_unsafe_buy(wallet, mint, report.reasons, signature)
                await record_paper_rejection(
                    mint, report.safety_score, report.reasons, wallet, signature
                )
                logger.info(
                    "paper signal rejected by analyzer: mint=%s score=%s reasons=%s",
                    mint, report.safety_score, "; ".join(report.reasons),
                )
                return
            cash = await paper_cash_balance()
            base_paper_cost = cash * PAPER_BUY_BASIS_POINTS // 10_000
            from src.executor import route_sized_amount
            paper_cost = route_sized_amount(base_paper_cost, requested_route)
            if paper_cost <= 0:
                logger.warning("paper signal has unusable observed price: %s", signature)
                return
            from src.executor import jupiter_quote, validate_entry_price_impact

            timeout = aiohttp.ClientTimeout(total=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                quote = await jupiter_quote(
                    session, os.getenv("JUPITER_API_KEY", "").strip(),
                    WSOL_MINT, mint, paper_cost,
                )
            entry_price_impact = validate_entry_price_impact(quote)
            entry_quote_at = datetime.now(timezone.utc).isoformat()
            paper_tokens = int(quote["outAmount"])
            whale_reference_price = (
                whale_paid_lamports / whale_token_amount_raw
                if whale_token_amount_raw > 0 else 0.0
            )
            copy_price = paper_cost / paper_tokens if paper_tokens > 0 else 0.0
            copy_price_gap_pct = (
                (copy_price / whale_reference_price - 1) * 100
                if whale_reference_price > 0 else 0.0
            )
            detected = datetime.fromisoformat(signal_detected_at.replace("Z", "+00:00"))
            entry_latency_ms = int(
                (datetime.now(timezone.utc) - detected).total_seconds() * 1000
            )
            await record_paper_buy(
                mint,
                paper_cost,
                paper_tokens,
                token_decimals,
                source_wallet=wallet,
                source_signature=signature,
                safety_score=int(report.safety_score),
                entry_reason=(
                    "whale_route_a"
                    if requested_route == "A"
                    else "dex_momentum_unknown_whales"
                ),
                signal_detected_at=signal_detected_at,
                analysis_completed_at=analysis_completed_at,
                entry_quote_at=entry_quote_at,
                entry_price_impact_pct=entry_price_impact,
                expected_slippage_bps=int(quote.get("slippageBps", 100) or 100),
                whale_reference_price=whale_reference_price,
                copy_price_gap_pct=copy_price_gap_pct,
                entry_latency_ms=entry_latency_ms,
                route_type=requested_route,
                dex_momentum_score=dex_momentum_score,
            )
            if requested_route == "A":
                from src.wallet_performance import record_paper_buy_success
                await record_paper_buy_success(wallet, mint, signature)
            logger.info(
                "paper buy recorded: mint=%s route=%s momentum=%.2f cost=%s "
                "score=%s source_wallet=%s signature=%s",
                mint, requested_route, dex_momentum_score, paper_cost,
                report.safety_score, wallet, signature,
            )
        except RuntimeError as exc:
            reason = str(exc)
            if (
                "failed after 3 attempts" in reason
                or "getTokenSupply failed" in reason
                or "could not find account" in reason
            ):
                from src.risk_manager import record_rpc_skip
                await record_rpc_skip(mint, wallet, signature, reason)
            logger.info("paper signal skipped: mint=%s reason=%s", mint, exc)
        except Exception:
            logger.exception("paper signal processing failed: mint=%s", mint)


def schedule_paper_signal(
    mint: str, acquired_raw: int, token_decimals: int, paid_lamports: int,
    wallet: str, signature: str
) -> None:
    signal_detected_at = datetime.now(timezone.utc).isoformat()
    task = asyncio.create_task(
        process_paper_signal(
            mint, acquired_raw, token_decimals, paid_lamports, wallet, signature,
            signal_detected_at,
            "A",
            0.0,
        )
    )
    _signal_tasks.add(task)
    task.add_done_callback(_signal_tasks.discard)


def print_buys(result: dict[str, Any], dex_name: str, watched_wallets: set[str]) -> None:
    transaction = result.get("transaction") or {}
    message = transaction.get("message") or {}
    meta = result.get("meta") or {}
    keys = account_keys(message)
    signature = signature_of(result)

    for wallet in watched_wallets.intersection(keys):
        deltas = wallet_token_deltas(meta, wallet)
        acquired = [(mint, raw, decimals) for mint, (raw, decimals) in deltas.items() if raw > 0]
        if not acquired:
            continue

        paid_tokens = [
            (mint, -raw, decimals)
            for mint, (raw, decimals) in deltas.items()
            if raw < 0 and mint in {WSOL_MINT, USDC_MINT}
        ]
        payment = ", ".join(
            f"{decimal_amount(raw, decimals):f} {'WSOL' if mint == WSOL_MINT else 'USDC'}"
            for mint, raw, decimals in paid_tokens
        )
        paid_lamports = next(
            (raw for mint, raw, decimals in paid_tokens if mint == WSOL_MINT and decimals == 9),
            0,
        )

        # Native SOL net outflow, with the transaction network fee removed. This is
        # an exact wallet-level balance delta; it can include account rent in a swap.
        if wallet in keys:
            index = keys.index(wallet)
            pre = meta.get("preBalances") or []
            post = meta.get("postBalances") or []
            if index < len(pre) and index < len(post):
                fee = int(meta.get("fee", 0)) if index == 0 else 0
                lamports = int(pre[index]) - int(post[index]) - fee
                if lamports > 0 and paid_lamports <= 0:
                    paid_lamports = lamports
                    native = Decimal(lamports) / LAMPORTS_PER_SOL
                    payment = f"{native:f} SOL net outflow" + (f", {payment}" if payment else "")

        for mint, raw, decimals in acquired:
            # Do not report quote-token change as the purchased asset.
            if mint in {WSOL_MINT, USDC_MINT}:
                continue
            print(
                f"[BUY] DEX={dex_name} wallet={wallet}\n"
                f"      CA={mint}\n"
                f"      acquired={decimal_amount(raw, decimals):f} tokens\n"
                f"      paid={payment or 'unresolved (non-SOL/USDC quote)'}\n"
                f"      signature={signature}",
                flush=True,
            )
            if paid_lamports > 0:
                # Wallet performance is an independent observation pipeline.
                # Record every resolved on-chain buy before deciding whether our
                # bot should copy it.
                from src.wallet_performance import observe_buy
                observation = asyncio.create_task(
                    observe_buy(wallet, mint, raw, paid_lamports, signature)
                )
                _signal_tasks.add(observation)
                observation.add_done_callback(_signal_tasks.discard)

                if not whale_buy_amount_allowed(
                    paid_lamports,
                    wallet=wallet,
                    mint=mint,
                    signature=signature,
                ):
                    continue
                schedule_paper_signal(
                    mint, raw, decimals, paid_lamports, wallet, signature
                )


def momentum_score(volume_m5_usd: float, buys_m5: int, sells_m5: int) -> float:
    """Score bounded five-minute activity without retaining a time-series."""
    volume_points = min(60.0, max(0.0, volume_m5_usd) / 500.0)
    net_buys = max(0, buys_m5 - sells_m5)
    imbalance_points = min(40.0, net_buys * 2.0)
    return round(volume_points + imbalance_points, 4)


def momentum_candidate_from_pair(pair: dict[str, Any]) -> MomentumCandidate | None:
    if pair.get("chainId") != "solana":
        return None
    base = pair.get("baseToken") or {}
    mint = str(base.get("address") or "")
    pair_address = str(pair.get("pairAddress") or "")
    if not mint or not pair_address or mint in {WSOL_MINT, USDC_MINT}:
        return None
    txns_m5 = (pair.get("txns") or {}).get("m5") or {}
    volume_m5 = float((pair.get("volume") or {}).get("m5") or 0)
    buys_m5 = int(txns_m5.get("buys") or 0)
    sells_m5 = int(txns_m5.get("sells") or 0)
    liquidity = float((pair.get("liquidity") or {}).get("usd") or 0)
    net_buys = buys_m5 - sells_m5
    has_volume_burst = volume_m5 >= MOMENTUM_MIN_VOLUME_M5_USD
    has_buy_burst = (
        net_buys >= MOMENTUM_MIN_NET_BUYS_M5
        and buys_m5 >= max(1, sells_m5) * 1.5
    )
    if liquidity < MOMENTUM_MIN_LIQUIDITY_USD or not (
        has_volume_burst or has_buy_burst
    ):
        return None
    return MomentumCandidate(
        mint=mint,
        pair_address=pair_address,
        volume_m5_usd=volume_m5,
        buys_m5=buys_m5,
        sells_m5=sells_m5,
        liquidity_usd=liquidity,
        momentum_score=momentum_score(volume_m5, buys_m5, sells_m5),
    )


async def _dexscreener_json(
    session: aiohttp.ClientSession, url: str, **params: str
) -> Any:
    async with session.get(
        url,
        params=params or None,
        headers={"accept": "application/json"},
    ) as response:
        response.raise_for_status()
        return await response.json()


async def fetch_momentum_candidates(
    session: aiohttp.ClientSession,
) -> list[MomentumCandidate]:
    """Discover and rank a bounded Solana market snapshot via public endpoints."""
    search, profiles, boosts = await asyncio.gather(
        _dexscreener_json(session, DEX_SCREENER_SEARCH_URL, q="solana"),
        _dexscreener_json(session, DEX_SCREENER_PROFILES_URL),
        _dexscreener_json(session, DEX_SCREENER_BOOSTS_URL),
    )
    pairs: list[dict[str, Any]] = [
        pair
        for pair in (search.get("pairs") or [])
        if isinstance(pair, dict)
    ] if isinstance(search, dict) else []
    discovered_mints: list[str] = []
    for payload in (profiles, boosts):
        rows = payload if isinstance(payload, list) else [payload]
        for row in rows:
            if not isinstance(row, dict) or row.get("chainId") != "solana":
                continue
            mint = str(row.get("tokenAddress") or "")
            if mint and mint not in discovered_mints:
                discovered_mints.append(mint)
            if len(discovered_mints) >= MOMENTUM_MAX_DISCOVERY_TOKENS:
                break
    if discovered_mints:
        token_pairs = await _dexscreener_json(
            session,
            f"{DEX_SCREENER_TOKENS_URL}/{','.join(discovered_mints)}",
        )
        if isinstance(token_pairs, list):
            pairs.extend(pair for pair in token_pairs if isinstance(pair, dict))

    best_by_mint: dict[str, MomentumCandidate] = {}
    for pair in pairs:
        candidate = momentum_candidate_from_pair(pair)
        if candidate is None:
            continue
        incumbent = best_by_mint.get(candidate.mint)
        if incumbent is None or candidate.momentum_score > incumbent.momentum_score:
            best_by_mint[candidate.mint] = candidate
    return sorted(
        best_by_mint.values(),
        key=lambda item: (
            item.momentum_score,
            item.volume_m5_usd,
            item.buys_m5 - item.sells_m5,
        ),
        reverse=True,
    )[:MOMENTUM_MAX_CANDIDATES]


async def _solana_rpc(
    session: aiohttp.ClientSession,
    http_url: str,
    method: str,
    params: list[Any],
) -> Any:
    request = {"jsonrpc": "2.0", "id": method, "method": method, "params": params}
    async with session.post(http_url, json=request) as response:
        response.raise_for_status()
        payload = await response.json()
    if payload.get("error"):
        raise RuntimeError(f"{method} failed: {payload['error']}")
    return payload.get("result")


def unknown_whale_buy_from_transaction(
    transaction: dict[str, Any],
    mint: str,
    watched_wallets: set[str],
) -> list[UnknownWhaleBuy]:
    message = ((transaction.get("transaction") or {}).get("message") or {})
    meta = transaction.get("meta") or {}
    keys = account_keys(message)
    key_rows = message.get("accountKeys") or []
    signature = signature_of(transaction)
    results: list[UnknownWhaleBuy] = []
    for index, row in enumerate(key_rows):
        wallet = str(row.get("pubkey") or "") if isinstance(row, dict) else str(row)
        is_signer = bool(row.get("signer")) if isinstance(row, dict) else index == 0
        if not is_signer or not wallet or wallet in watched_wallets:
            continue
        token_deltas = wallet_token_deltas(meta, wallet)
        token_delta, decimals = token_deltas.get(mint, (0, 0))
        if token_delta <= 0:
            continue
        pre = meta.get("preBalances") or []
        post = meta.get("postBalances") or []
        if index >= len(pre) or index >= len(post):
            continue
        fee = int(meta.get("fee", 0) or 0) if index == 0 else 0
        native_paid = int(pre[index]) - int(post[index]) - fee
        wsol_delta, wsol_decimals = token_deltas.get(WSOL_MINT, (0, 9))
        wsol_paid = -wsol_delta if wsol_delta < 0 and wsol_decimals == 9 else 0
        paid_lamports = max(native_paid, wsol_paid)
        if paid_lamports < SINGLE_STRENGTH_LAMPORTS:
            continue
        results.append(
            UnknownWhaleBuy(
                wallet=wallet,
                signature=signature,
                paid_lamports=paid_lamports,
                token_amount_raw=token_delta,
                token_decimals=decimals,
            )
        )
    return results


async def confirm_unknown_whales(
    session: aiohttp.ClientSession,
    http_url: str,
    candidate: MomentumCandidate,
    watched_wallets: set[str],
) -> list[UnknownWhaleBuy]:
    signatures = await _solana_rpc(
        session,
        http_url,
        "getSignaturesForAddress",
        [
            candidate.pair_address,
            {"limit": UNKNOWN_WHALE_SIGNATURE_LIMIT, "commitment": "confirmed"},
        ],
    )
    transaction_signatures = [
        str(row.get("signature"))
        for row in (signatures or [])
        if isinstance(row, dict) and row.get("signature") and row.get("err") is None
    ]
    if not transaction_signatures:
        return []
    by_wallet: dict[str, UnknownWhaleBuy] = {}
    # Helius rejects JSON-RPC batch getTransaction on some plans. Sequential,
    # early-exit reads keep peak memory flat and stop as soon as 3 whales exist.
    for signature in transaction_signatures:
        transaction = await _solana_rpc(
            session,
            http_url,
            "getTransaction",
            [
                signature,
                {
                    "commitment": "confirmed",
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0,
                },
            ],
        )
        if not isinstance(transaction, dict):
            continue
        for buy in unknown_whale_buy_from_transaction(
            transaction, candidate.mint, watched_wallets
        ):
            incumbent = by_wallet.get(buy.wallet)
            if incumbent is None or buy.paid_lamports > incumbent.paid_lamports:
                by_wallet[buy.wallet] = buy
        if len(by_wallet) >= UNKNOWN_WHALE_MIN_COUNT:
            break
    return sorted(
        by_wallet.values(), key=lambda buy: buy.paid_lamports, reverse=True
    )


async def run_market_momentum_route(settings: MonitorSettings) -> None:
    """Lean Route B loop: one bounded snapshot and one candidate scan per tick."""
    timeout = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            started = time.monotonic()
            try:
                wallets = set(load_wallets(settings.wallets_path))
                candidates = await fetch_momentum_candidates(session)
                now = time.monotonic()
                for mint, expires_at in list(_market_entry_cooldowns.items()):
                    if expires_at <= now:
                        _market_entry_cooldowns.pop(mint, None)
                candidate = next(
                    (
                        item
                        for item in candidates
                        if item.mint not in _market_entry_cooldowns
                    ),
                    None,
                )
                if candidate is not None:
                    whales = await confirm_unknown_whales(
                        session, settings.http_url, candidate, wallets
                    )
                    if len(whales) >= UNKNOWN_WHALE_MIN_COUNT:
                        strongest = whales[0]
                        _market_entry_cooldowns[candidate.mint] = (
                            now + MOMENTUM_ENTRY_COOLDOWN_SECONDS
                        )
                        signal_signature = (
                            f"dexscreener:{candidate.pair_address}:"
                            f"{int(time.time())}"
                        )
                        logger.info(
                            "[ROUTE_B] momentum confirmed mint=%s score=%.2f "
                            "volume_m5=$%.2f unknown_whales=%d",
                            candidate.mint,
                            candidate.momentum_score,
                            candidate.volume_m5_usd,
                            len(whales),
                        )
                        task = asyncio.create_task(
                            process_paper_signal(
                                candidate.mint,
                                strongest.token_amount_raw,
                                strongest.token_decimals,
                                strongest.paid_lamports,
                                strongest.wallet,
                                signal_signature,
                                datetime.now(timezone.utc).isoformat(),
                                "B",
                                candidate.momentum_score,
                            )
                        )
                        _signal_tasks.add(task)
                        task.add_done_callback(_signal_tasks.discard)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Route B market momentum poll failed")
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.0, DEX_SCREENER_POLL_SECONDS - elapsed))


async def keepalive(socket: ClientConnection) -> None:
    """Helius recommends periodic pings to avoid its inactivity timeout."""
    while True:
        await asyncio.sleep(60)
        await socket.ping()


async def fetch_transaction(
    session: aiohttp.ClientSession, http_url: str, signature: str
) -> dict[str, Any] | None:
    request = {
        "jsonrpc": "2.0", "id": signature, "method": "getTransaction",
        "params": [signature, {"commitment": "confirmed", "encoding": "jsonParsed",
                                "maxSupportedTransactionVersion": 0}],
    }
    for _ in range(4):
        async with session.post(http_url, json=request) as response:
            response.raise_for_status()
            payload = await response.json()
        if payload.get("result"):
            return payload["result"]
        await asyncio.sleep(0.4)
    return None


async def monitor_standard_once(
    settings: MonitorSettings, wallets: tuple[str, ...]
) -> None:
    """Free-plan fallback: wallet-filtered logs, then fetch only matching DEX txs."""
    request_to_wallet: dict[int, str] = {}
    seen: set[str] = set()
    timeout = aiohttp.ClientTimeout(total=15)
    async with connect(
        settings.ws_url, ping_interval=20, ping_timeout=20, open_timeout=20, max_queue=512
    ) as socket, aiohttp.ClientSession(timeout=timeout) as session:
        for request_id, wallet in enumerate(wallets, start=1):
            request_to_wallet[request_id] = wallet
            await socket.send(json.dumps({
                "jsonrpc": "2.0", "id": request_id, "method": "logsSubscribe",
                "params": [{"mentions": [wallet]}, {"commitment": "confirmed"}],
            }))
        acknowledgements = 0
        async for raw_message in socket:
            message = json.loads(raw_message)
            if "id" in message:
                if "error" in message:
                    raise RuntimeError(f"standard subscription rejected: {message['error']}")
                acknowledgements += 1
                if acknowledgements == len(wallets):
                    logger.info("standard fallback subscribed to %s wallets", len(wallets))
                    logger.info(
                        "SUCCESS: actively monitoring %s verified whale wallets in real time",
                        len(wallets),
                    )
                continue
            value = ((message.get("params") or {}).get("result") or {}).get("value") or {}
            signature = value.get("signature")
            if not signature or signature in seen or value.get("err") is not None:
                continue
            logs = value.get("logs") or []
            dex_name = next(
                (name for name, program in DEX_PROGRAMS.items()
                 if any(program in line for line in logs)), None,
            )
            if not dex_name:
                continue
            seen.add(signature)
            if len(seen) > 10_000:
                seen.clear()
                seen.add(signature)
            transaction = await fetch_transaction(session, settings.http_url, signature)
            if transaction:
                print_buys(transaction, dex_name, set(wallets))


async def monitor_once(settings: MonitorSettings, wallets: tuple[str, ...]) -> None:
    request_to_dex = {index: name for index, name in enumerate(DEX_PROGRAMS, start=1)}
    subscription_to_dex: dict[int, str] = {}
    async with connect(
        settings.ws_url,
        ping_interval=20,
        ping_timeout=20,
        open_timeout=20,
        max_queue=512,
    ) as socket:
        for request_id, (name, program) in enumerate(DEX_PROGRAMS.items(), start=1):
            await socket.send(json.dumps(subscription_request(request_id, wallets, program)))

        ping_task = asyncio.create_task(keepalive(socket))
        try:
            async for raw_message in socket:
                message = json.loads(raw_message)
                if "id" in message:
                    if "error" in message:
                        if "not available on the free plan" in str(message["error"]):
                            raise EnhancedSubscriptionUnavailable
                        raise RuntimeError(f"subscription rejected: {message['error']}")
                    request_id = int(message["id"])
                    subscription_to_dex[int(message["result"])] = request_to_dex[request_id]
                    logger.info("subscribed: %s", request_to_dex[request_id])
                    continue

                params = message.get("params") or {}
                subscription_id = params.get("subscription")
                dex_name = subscription_to_dex.get(subscription_id)
                value = (params.get("result") or {}).get("value")
                if dex_name and isinstance(value, dict):
                    print_buys(value, dex_name, set(wallets))
        finally:
            ping_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ping_task


async def run_forever(settings: MonitorSettings) -> None:
    delay = 3
    use_standard_fallback = False
    while True:
        connected_at = time.monotonic()
        try:
            wallets = load_wallets(settings.wallets_path)
            mtime_ns = settings.wallets_path.stat().st_mtime_ns
            logger.info("loaded %s wallets from %s", len(wallets), settings.wallets_path)
            monitor_task = asyncio.create_task(
                monitor_standard_once(settings, wallets)
                if use_standard_fallback else monitor_once(settings, wallets)
            )
            watcher_task = asyncio.create_task(
                watch_wallet_file(
                    settings.wallets_path, mtime_ns, settings.wallet_reload_seconds
                )
            )
            heartbeat_task = asyncio.create_task(monitor_heartbeat(len(wallets)))
            done, pending = await asyncio.wait(
                {monitor_task, watcher_task, heartbeat_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            results = await asyncio.gather(*done, return_exceptions=True)
            for result in results:
                if isinstance(result, WalletListChanged):
                    raise result
            for result in results:
                if isinstance(result, BaseException):
                    raise result
            raise ConnectionError("WebSocket stream ended")
        except WalletListChanged:
            logger.info("wallet list updated; reloading file and rebuilding RPC subscriptions")
            delay = 3
            continue
        except EnhancedSubscriptionUnavailable:
            logger.warning(
                "enhanced transactionSubscribe unavailable; switching to wallet-filtered logsSubscribe"
            )
            use_standard_fallback = True
            delay = 3
            continue
        except asyncio.CancelledError:
            raise
        except Exception:
            if time.monotonic() - connected_at >= 60:
                delay = 3
            logger.exception("connection lost; reconnecting in %s seconds", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 300)


async def run_service() -> None:
    from src.wallet_performance import performance_loop

    settings = MonitorSettings.from_env()
    await asyncio.gather(
        run_forever(settings),
        performance_loop(),
        run_market_momentum_route(settings),
        monitor_maintenance_loop(),
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(run_service())


if __name__ == "__main__":
    main()
