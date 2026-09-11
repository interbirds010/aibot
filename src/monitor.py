"""Real-time smart-money DEX transaction monitor using Helius Enhanced WSS."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
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
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus

from src import state_store
from src.phase_memory_telemetry import (
    add_current_phase_metadata,
    flush_phase_memory_telemetry,
    phase_memory,
    start_memory_attribution_sampler,
)
from src.solana_rpc import (
    SolanaRpcExhaustedError,
    SolanaRpcRateLimitExhaustedError,
    canonical_rpc_failure_reason,
    solana_rpc_call,
)
from src.logging_utils import configure_safe_logging, redact_sensitive_text
from src.runtime_memory import (
    current_rss_bytes,
    record_memory_phase,
    record_transaction_payload,
    runtime_memory_metrics,
)
from src.research.prospective_features import MomentumSnapshotStore
from src.research.coverage_telemetry import (
    flush_coverage_telemetry,
    record_confirmation_result,
    record_funnel_stage,
)

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
MOMENTUM_MIN_VOLUME_M5_USD = 15_000.0
MOMENTUM_MIN_NET_BUYS_M5 = 15
MOMENTUM_MIN_BUY_SELL_RATIO = 1.8
MOMENTUM_MIN_LIQUIDITY_USD = 10_000.0
MOMENTUM_MIN_PAIR_AGE_SECONDS = 900.0
MOMENTUM_MAX_DISCOVERY_TOKENS = 30
MOMENTUM_MAX_CANDIDATES = 8
MOMENTUM_MAX_RAW_PAIRS = 200
MOMENTUM_MAX_SHADOW_CANDIDATES = 8
MOMENTUM_SHADOWS_PER_TICK = 1
MOMENTUM_SHADOW_CAPTURE_INTERVAL_SECONDS = 30.0
MOMENTUM_ENTRY_COOLDOWN_SECONDS = 2_700.0
UNKNOWN_WHALE_MIN_COUNT = 3
UNKNOWN_WHALE_SIGNATURE_LIMIT = 12
ROUTE_B_MIN_SAFETY_SCORE = 55
ROUTE_B_MIN_LIQUIDITY_USD = 10_000.0
ROUTE_B_MIN_LP_LOCKED_PERCENT = 40.0
TOKEN_TRADE_COOLDOWN_SECONDS = 2_700.0
STOP_LOSS_TOKEN_COOLDOWN_SECONDS = 86_400.0
STOP_LOSS_BLACKLIST_MAX_TOKENS = 50
ROUTE_A_LOSS_SIZE_REDUCTION_STREAK = 2
ROUTE_A_PAUSE_STREAK = 3
ROUTE_A_PAUSE_SECONDS = 21_600.0
WALLET_TARGET_COUNT = 20
WALLET_FEEDER_TRIGGER_COUNT = 17
WALLET_FEEDER_COOLDOWN_SECONDS = 7_200.0
MONITOR_MAINTENANCE_INTERVAL_SECONDS = 60.0
SUBSCRIPTION_REFRESH_SECONDS = 1_800.0
HEALTH_WRITE_INTERVAL_SECONDS = 60.0
ROUTE_B_CONFIRM_TIMEOUT_SECONDS = 180.0
MAX_PENDING_SHADOW_SIGNALS = 40
WALLET_WS_FAILURE_REASONS = frozenset({
    "WS_AUTH_FAILED",
    "WS_RATE_LIMITED",
    "WS_HANDSHAKE_FAILED",
    "WS_SUBSCRIPTION_FAILED",
    "WS_REMOTE_CLOSED",
    "WS_HEARTBEAT_TIMEOUT",
    "WS_TRANSPORT_ERROR",
    "WS_MALFORMED_RESPONSE",
})
SOLANA_PUBLIC_WS_URL = "wss://api.mainnet.solana.com"
HELIUS_RECOVERY_PROBE_SECONDS = 1_800.0
MAX_SEEN_WALLET_SIGNATURES = 10_000
MONITOR_RSS_CEILING_BYTES = 260 * 1024 * 1024
DISCOVERY_SOURCE_HELIUS = "helius_transaction_subscribe"
DISCOVERY_SOURCE_SOLANA = "solana_logs_subscribe"
WALLET_WS_ACTIVITY_KEYS = frozenset({
    "connection_success",
    "subscription_success",
    "subscription_failure",
    "notification",
    "dex_log_match",
    "unique_signature",
    "transaction_fetch",
    "transaction_restore_success",
    "transaction_restore_failure",
    "transaction_parsed",
    "smart_money_candidate",
    "research_discovered",
    "analyzer_reached",
    "analyzer_success",
    "analyzer_failure",
})

# On-chain feeder output is verified; fail closed if an old unverified row remains.
TEST_ALLOW_UNVERIFIED_WALLETS = False
_analysis_limit = asyncio.Semaphore(2)
_signal_tasks: set[asyncio.Task[None]] = set()
_shadow_signal_tasks: set[asyncio.Task[None]] = set()
_signal_task_created_count = 0
_signal_task_completed_count = 0
_shadow_signal_task_created_count = 0
_shadow_signal_task_completed_count = 0
_whale_buy_history: dict[tuple[str, str], deque[tuple[float, int]]] = {}
_last_history_cleanup_at = 0.0
_market_entry_cooldowns: dict[str, float] = {}
_market_shadow_cooldowns: dict[str, float] = {}
_momentum_snapshot_store = MomentumSnapshotStore()
_last_route_b_health_write_at = 0.0
_route_b_consecutive_failures = 0
_last_market_shadow_capture_at = 0.0
_wallet_ws_activity_started_at = time.time()
_wallet_ws_activity: dict[str, int] = {
    key: 0 for key in WALLET_WS_ACTIVITY_KEYS
}
_wallet_ws_activity_by_source: dict[str, dict[str, int]] = {
    key: {} for key in WALLET_WS_ACTIVITY_KEYS
}
_wallet_ws_restore_failures_by_source: dict[str, dict[str, int]] = {}
_active_signature_window_size = 0
_active_signature_window_max_size = 0

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
    standard_ws_url: str = SOLANA_PUBLIC_WS_URL

    @classmethod
    def from_env(cls) -> "MonitorSettings":
        load_dotenv()
        api_key = os.getenv("HELIUS_API_KEY", "").strip()
        ws_template = os.getenv("HELIUS_RPC_WS_URL", "").strip()
        if "${HELIUS_API_KEY}" in ws_template and not api_key:
            raise RuntimeError("Helius WSS API key must be set in .env")
        ws_url = ws_template.replace("${HELIUS_API_KEY}", api_key)
        http_url = os.getenv("HELIUS_RPC_HTTP_URL", "").strip()
        http_url = http_url.replace("${HELIUS_API_KEY}", api_key)
        standard_ws_url = (
            os.getenv("SOLANA_PUBLIC_WS_URL", "").strip()
            or SOLANA_PUBLIC_WS_URL
        )
        if not ws_url or "${" in ws_url:
            raise RuntimeError("Helius WSS URL must be set in .env")
        if not standard_ws_url.startswith(("ws://", "wss://")):
            raise RuntimeError("Solana public WSS URL must use ws:// or wss://")
        reload_seconds = max(1.0, float(os.getenv("WALLET_RELOAD_SECONDS", "5")))
        return cls(
            ws_url=ws_url,
            http_url=http_url,
            standard_ws_url=standard_ws_url,
            wallet_reload_seconds=reload_seconds,
        )


@dataclass(slots=True)
class WalletWsRouteState:
    """Helius 복구를 주기적으로 확인하는 bounded WS fallback 상태다."""

    mode: str = "HELIUS_ENHANCED"
    next_helius_probe_at: float = 0.0

    @property
    def uses_standard(self) -> bool:
        return self.mode == "SOLANA_STANDARD"

    def activate_standard(self, now: float) -> None:
        self.mode = "SOLANA_STANDARD"
        self.next_helius_probe_at = now + HELIUS_RECOVERY_PROBE_SECONDS

    def record_failure(self, now: float) -> None:
        if not self.uses_standard:
            self.activate_standard(now)
        elif now >= self.next_helius_probe_at:
            self.mode = "HELIUS_ENHANCED"

    def refresh(self, now: float) -> None:
        if self.uses_standard and now >= self.next_helius_probe_at:
            self.mode = "HELIUS_ENHANCED"


class SignatureWindow:
    """한 연결 안의 중복 signature를 bounded FIFO로 억제한다."""

    def __init__(self, maximum: int = MAX_SEEN_WALLET_SIGNATURES) -> None:
        global _active_signature_window_size
        self.maximum = max(1, int(maximum))
        self._ordered: deque[str] = deque()
        self._seen: set[str] = set()
        _active_signature_window_size = 0

    def add(self, signature: str) -> bool:
        global _active_signature_window_size, _active_signature_window_max_size
        if signature in self._seen:
            return False
        self._seen.add(signature)
        self._ordered.append(signature)
        while len(self._ordered) > self.maximum:
            self._seen.discard(self._ordered.popleft())
        _active_signature_window_size = len(self._ordered)
        _active_signature_window_max_size = max(
            _active_signature_window_max_size,
            _active_signature_window_size,
        )
        return True

    def __len__(self) -> int:
        return len(self._ordered)


def reset_wallet_ws_activity(*, now_epoch: float | None = None) -> None:
    global _wallet_ws_activity_started_at
    _wallet_ws_activity_started_at = (
        time.time() if now_epoch is None else float(now_epoch)
    )
    for key in WALLET_WS_ACTIVITY_KEYS:
        _wallet_ws_activity[key] = 0
        _wallet_ws_activity_by_source[key] = {}
    _wallet_ws_restore_failures_by_source.clear()


def record_wallet_ws_activity(name: str, source: str) -> None:
    """고빈도 경로에서는 메모리 counter만 갱신하고 heartbeat가 저장한다."""
    if name not in WALLET_WS_ACTIVITY_KEYS:
        raise ValueError("unsupported wallet WebSocket activity metric")
    provider = str(source)[:80]
    _wallet_ws_activity[name] += 1
    per_source = _wallet_ws_activity_by_source[name]
    per_source[provider] = per_source.get(provider, 0) + 1


def record_transaction_restore_failure(
    source: str, error: BaseException | None,
) -> str:
    """getTransaction 실패를 WebSocket transport와 분리해 집계한다."""
    provider = str(source)[:80]
    reason = (
        canonical_rpc_failure_reason(error)
        if error is not None else "RPC_GET_TRANSACTION_NOT_AVAILABLE"
    ) or "RPC_GET_TRANSACTION_FAILED"
    record_wallet_ws_activity("transaction_restore_failure", provider)
    reasons = _wallet_ws_restore_failures_by_source.setdefault(provider, {})
    reasons[reason] = reasons.get(reason, 0) + 1
    return reason


def wallet_ws_activity_metrics() -> dict[str, Any]:
    values: dict[str, Any] = {
        "wallet_ws_activity_started_at": _wallet_ws_activity_started_at,
    }
    for name in sorted(WALLET_WS_ACTIVITY_KEYS):
        values[f"wallet_ws_{name}_process_count"] = _wallet_ws_activity[name]
        values[f"wallet_ws_{name}_counts_by_source"] = dict(
            sorted(_wallet_ws_activity_by_source[name].items())
        )
    values["wallet_ws_transaction_restore_failure_reasons_by_source"] = {
        source: dict(sorted(reasons.items()))
        for source, reasons in sorted(
            _wallet_ws_restore_failures_by_source.items()
        )
    }
    return values


def _release_signal_task(task: asyncio.Task[None], *, shadow: bool) -> None:
    global _signal_task_completed_count, _shadow_signal_task_completed_count
    _signal_tasks.discard(task)
    _signal_task_completed_count += 1
    if shadow:
        _shadow_signal_tasks.discard(task)
        _shadow_signal_task_completed_count += 1


def track_signal_task(task: asyncio.Task[None], *, shadow: bool = False) -> None:
    """Task registry를 단일 lifecycle로 관리하고 완료 참조를 제거한다."""
    global _signal_task_created_count, _shadow_signal_task_created_count
    _signal_tasks.add(task)
    _signal_task_created_count += 1
    if shadow:
        _shadow_signal_tasks.add(task)
        _shadow_signal_task_created_count += 1
    task.add_done_callback(
        lambda completed: _release_signal_task(completed, shadow=shadow)
    )


def monitor_runtime_metrics(wallet_count: int) -> dict[str, Any]:
    """민감한 object 내용 없이 live task/collection 크기만 집계한다."""
    from src.analyzer import analyzer_runtime_metrics

    tasks = [task for task in asyncio.all_tasks() if not task.done()]
    signal_tasks = list(_signal_tasks)
    shadow_tasks = list(_shadow_signal_tasks)
    metrics = runtime_memory_metrics(
        rss_ceiling_bytes=MONITOR_RSS_CEILING_BYTES
    )
    metrics.update({
        "monitor_asyncio_live_task_count": len(tasks),
        "monitor_signal_task_count": len(signal_tasks),
        "monitor_signal_done_task_count": sum(
            task.done() for task in signal_tasks
        ),
        "monitor_signal_task_created_count": _signal_task_created_count,
        "monitor_signal_task_completed_count": _signal_task_completed_count,
        "monitor_shadow_signal_task_count": len(shadow_tasks),
        "monitor_shadow_signal_done_task_count": sum(
            task.done() for task in shadow_tasks
        ),
        "monitor_shadow_signal_task_created_count": (
            _shadow_signal_task_created_count
        ),
        "monitor_shadow_signal_task_completed_count": (
            _shadow_signal_task_completed_count
        ),
        "monitor_wallet_count": int(wallet_count),
        "monitor_signature_window_size": _active_signature_window_size,
        "monitor_signature_window_max_size": (
            _active_signature_window_max_size
        ),
        "monitor_whale_history_key_count": len(_whale_buy_history),
        "monitor_whale_history_entry_count": sum(
            len(history) for history in _whale_buy_history.values()
        ),
        "monitor_market_entry_cooldown_count": len(_market_entry_cooldowns),
        "monitor_market_shadow_cooldown_count": len(_market_shadow_cooldowns),
        "monitor_momentum_snapshot_series_count": (
            _momentum_snapshot_store.series_count
        ),
        "monitor_momentum_snapshot_count": (
            _momentum_snapshot_store.snapshot_count
        ),
        "monitor_ws_metric_source_bucket_count": sum(
            len(values) for values in _wallet_ws_activity_by_source.values()
        ),
        "monitor_ws_failure_reason_bucket_count": sum(
            len(values)
            for values in _wallet_ws_restore_failures_by_source.values()
        ),
    })
    metrics.update(analyzer_runtime_metrics())
    return metrics


@dataclass(frozen=True, slots=True)
class MomentumCandidate:
    mint: str
    pair_address: str
    volume_m5_usd: float
    buys_m5: int
    sells_m5: int
    liquidity_usd: float
    momentum_score: float
    pair_age_seconds: float = 0.0
    price_usd: float | None = None


@dataclass(frozen=True, slots=True)
class MomentumShadowCandidate:
    candidate: MomentumCandidate
    rejection_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _MomentumPairProjection:
    is_solana: bool
    mint: str
    pair_address: str
    buys_m5: float | None = None
    sells_m5: float | None = None
    volume_m5_usd: float | None = None
    liquidity_usd: float | None = None
    pair_created_at_ms: float | None = None
    price_usd: float | None = None
    structural_error: bool = False


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


class WebSocketSubscriptionRejected(RuntimeError):
    """Bounded subscription rejection without persisting provider payloads."""

    def __init__(self, canonical_reason: str) -> None:
        self.canonical_reason = canonical_reason
        super().__init__(canonical_reason)


class SubscriptionRefresh(Exception):
    """정상 연결도 주기적으로 재구독해 장기 정체를 방지한다."""


def _websocket_http_status(error: BaseException) -> int | None:
    response = getattr(error, "response", None)
    raw_status = getattr(response, "status_code", None)
    if raw_status is None:
        raw_status = getattr(error, "status_code", None)
    try:
        return int(raw_status) if raw_status is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def websocket_subscription_failure_reason(payload: Any) -> str:
    """subscription 오류 payload를 bounded 운영 category로 정규화한다."""
    error = payload if isinstance(payload, dict) else {}
    try:
        code = int(error.get("code"))
    except (TypeError, ValueError, OverflowError):
        code = 0
    message = str(error.get("message") or "").upper()
    if code in {401, 403} or "AUTH" in message or "API KEY" in message:
        return "WS_AUTH_FAILED"
    if code == 429 or "RATE LIMIT" in message or "TOO MANY" in message:
        return "WS_RATE_LIMITED"
    return "WS_SUBSCRIPTION_FAILED"


def canonical_websocket_failure_reason(error: BaseException) -> str:
    """raw 예외를 저장하지 않고 WebSocket 장애 category만 반환한다."""
    explicit = str(getattr(error, "canonical_reason", "") or "")
    if explicit in WALLET_WS_FAILURE_REASONS:
        return explicit
    status = _websocket_http_status(error)
    if status in {401, 403}:
        return "WS_AUTH_FAILED"
    if status == 429:
        return "WS_RATE_LIMITED"
    if isinstance(error, (InvalidStatus, InvalidHandshake)):
        return "WS_HANDSHAKE_FAILED"
    if isinstance(error, json.JSONDecodeError):
        return "WS_MALFORMED_RESPONSE"
    if isinstance(error, ConnectionClosed):
        description = str(error).upper()
        if "PING TIMEOUT" in description or "HEARTBEAT" in description:
            return "WS_HEARTBEAT_TIMEOUT"
        return "WS_REMOTE_CLOSED"
    if isinstance(error, (OSError, asyncio.TimeoutError, ConnectionError)):
        return "WS_TRANSPORT_ERROR"
    return "WS_TRANSPORT_ERROR"


def record_wallet_ws_failure(reason: str, *, now_epoch: float | None = None) -> None:
    """WebSocket reconnect 원인을 전역 원장에 원자적으로 누적한다."""
    category = str(reason).upper()
    if category not in WALLET_WS_FAILURE_REASONS:
        raise ValueError("unsupported wallet WebSocket failure reason")
    now = time.time() if now_epoch is None else float(now_epoch)

    def mutate(document: dict[str, Any]) -> None:
        document.setdefault("schema_version", 2)
        metrics = document.setdefault("metrics", {})
        if not isinstance(metrics, dict):
            raise RuntimeError("global metrics are malformed")
        metrics["wallet_ws_reconnect_count"] = (
            int(metrics.get("wallet_ws_reconnect_count", 0) or 0) + 1
        )
        metrics["wallet_ws_consecutive_failures"] = (
            int(metrics.get("wallet_ws_consecutive_failures", 0) or 0) + 1
        )
        metrics["wallet_ws_last_failure_at"] = now
        metrics["wallet_ws_last_failure_category"] = category
        metrics["wallet_ws_state"] = "RECONNECTING"
        metrics["wallet_ws_state_changed_at"] = now

    state_store.update_json(
        state_store.GLOBAL_METRICS_PATH,
        {"schema_version": 2, "version": 0, "metrics": {}},
        mutate,
    )


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
        metrics = wallet_ws_activity_metrics()
        metrics["monitor_process_heartbeat_at"] = time.time()
        metrics.update(monitor_runtime_metrics(wallet_count))
        await asyncio.to_thread(
            state_store.set_global_metrics,
            metrics,
        )
        try:
            await asyncio.to_thread(flush_coverage_telemetry)
        except Exception:
            logger.exception("research coverage telemetry flush failed")
        try:
            await asyncio.to_thread(flush_phase_memory_telemetry)
        except Exception:
            logger.exception("phase memory telemetry flush failed")
        rss = metrics.get("monitor_memory_rss_bytes")
        logger.info(
            "heartbeat: wallets=%s rss_mib=%s ceiling_percent=%s "
            "tasks=%s signal_tasks=%s shadow_tasks=%s signature_window=%s",
            wallet_count,
            round(rss / 1024 / 1024, 2) if isinstance(rss, int) else "UNKNOWN",
            metrics.get("monitor_memory_rss_ceiling_percent"),
            metrics.get("monitor_asyncio_live_task_count"),
            metrics.get("monitor_signal_task_count"),
            metrics.get("monitor_shadow_signal_task_count"),
            metrics.get("monitor_signature_window_size"),
        )
        await asyncio.sleep(60)


async def subscription_refresh_timer() -> None:
    await asyncio.sleep(SUBSCRIPTION_REFRESH_SECONDS)
    raise SubscriptionRefresh


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
            "FAIL_LIQUIDITY_UNDER_10000 mint=%s liquidity_usd=%s threshold=%s",
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
        last_stop_loss_time = float(
            state_store.get_recent_stop_loss_time(
                mint,
                maximum_tokens=STOP_LOSS_BLACKLIST_MAX_TOKENS,
            )
            or 0
        )
        last_trade_time = float(state_store.get_last_trade_time(mint) or 0)
    except Exception:
        logger.exception("token cooldown lookup failed; fail-closed mint=%s", mint)
        return True
    stop_elapsed = current - last_stop_loss_time
    if (
        last_stop_loss_time > 0
        and stop_elapsed < STOP_LOSS_TOKEN_COOLDOWN_SECONDS
    ):
        logger.info(
            "FAIL_STOP_LOSS_BLACKLIST_ACTIVE mint=%s elapsed_seconds=%.3f "
            "required_seconds=%s last_stop_loss_time=%.3f",
            mint,
            stop_elapsed,
            STOP_LOSS_TOKEN_COOLDOWN_SECONDS,
            last_stop_loss_time,
        )
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


def route_a_entry_multiplier(
    loss_streak: int,
    latest_loss_at: float,
    now: float | None = None,
) -> Decimal | None:
    """A 경로 연속 손절에 따른 중단 또는 진입 배수를 반환한다."""
    current = float(now if now is not None else time.time())
    if (
        loss_streak >= ROUTE_A_PAUSE_STREAK
        and latest_loss_at > 0
        and current - latest_loss_at < ROUTE_A_PAUSE_SECONDS
    ):
        return None
    if loss_streak >= ROUTE_A_LOSS_SIZE_REDUCTION_STREAK:
        return Decimal("0.5")
    return Decimal("1")


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
    *,
    momentum_metrics: dict[str, int | float] | None = None,
    prospective_feature_collection: dict[str, Any] | None = None,
    prefilter_reasons: tuple[str, ...] = (),
    discovery_source: str | None = None,
) -> None:
    """모든 형성 후보를 관찰하고 승인 후보만 페이퍼 매수한다."""
    async with _analysis_limit:
        observation_id: str | None = None
        observation_enabled = False
        decision_reasons = [str(reason) for reason in prefilter_reasons]
        safety_snapshot: dict[str, Any] = {}
        analysis_completed_at: str | None = None
        analyzer_started = False
        quote_preflight_started = False
        quote_preflight_finished = False
        try:
            from src.observation_tracker import (
                approved_signal_max_open_positions,
                approved_signal_paper_mode_enabled,
                finalize_candidate_without_quote,
                mark_paper_experiment_status,
                observation_mode_enabled,
                record_candidate_discovery,
                record_observation_decision,
            )

            observation_enabled = observation_mode_enabled()
            if observation_enabled:
                discovery_details: dict[str, Any] = {
                    "whale_paid_lamports": whale_paid_lamports,
                    "source_token_amount_raw": whale_token_amount_raw,
                    "source_token_decimals": token_decimals,
                    "prefilter_reasons": ",".join(decision_reasons),
                }
                if discovery_source:
                    discovery_details["discovery_source"] = discovery_source
                discovered = await record_candidate_discovery(
                    mint=mint,
                    route_type=requested_route,
                    source_wallet=wallet,
                    source_signature=signature,
                    token_amount_raw=whale_token_amount_raw,
                    token_decimals=token_decimals,
                    signal_detected_at=signal_detected_at,
                    dex_momentum_score=dex_momentum_score,
                    momentum_metrics=momentum_metrics,
                    discovery_metadata=discovery_details,
                    prospective_feature_collection=(
                        prospective_feature_collection
                    ),
                )
                observation_id = discovered.observation_id
                if discovered.created:
                    record_funnel_stage(
                        "observation_created",
                        mint=mint,
                        family=requested_route,
                    )
                    from src.research.prospective_features import (
                        prospective_feature_collection_eligible,
                    )
                    if prospective_feature_collection_eligible({
                        "prospective_feature_collection": (
                            prospective_feature_collection
                        ),
                        "signal_detected_at": signal_detected_at,
                    }):
                        record_funnel_stage(
                            "prospective_eligible",
                            mint=mint,
                            family=requested_route,
                        )
                if requested_route == "A" and discovered.created:
                    record_wallet_ws_activity(
                        "research_discovered",
                        discovery_source or "unknown",
                    )
            elif decision_reasons:
                return
            route_a_size_multiplier = Decimal("1")
            if requested_route == "A":
                try:
                    loss_streak, latest_loss_at = (
                        state_store.get_route_initial_stop_streak("A")
                    )
                except Exception:
                    logger.exception(
                        "route A loss streak lookup failed; fail-closed mint=%s",
                        mint,
                    )
                    decision_reasons.append("ROUTE_A_LOSS_STREAK_LOOKUP_FAILED")
                    if not observation_enabled:
                        return
                    loss_streak, latest_loss_at = 0, 0.0
                route_a_size_multiplier = route_a_entry_multiplier(
                    loss_streak,
                    latest_loss_at,
                )
                if route_a_size_multiplier is None:
                    pause_elapsed = time.time() - latest_loss_at
                    logger.warning(
                        "FAIL_ROUTE_A_LOSS_PAUSE mint=%s streak=%s "
                        "remaining_seconds=%.3f",
                        mint,
                        loss_streak,
                        ROUTE_A_PAUSE_SECONDS - pause_elapsed,
                    )
                    decision_reasons.append("ROUTE_A_LOSS_PAUSE")
                    if not observation_enabled:
                        return
                    route_a_size_multiplier = Decimal("1")
                if route_a_size_multiplier < 1:
                    logger.warning(
                        "ROUTE_A_SIZE_REDUCED mint=%s streak=%s multiplier=%s",
                        mint,
                        loss_streak,
                        route_a_size_multiplier,
                    )
            if token_cooldown_is_active(mint):
                decision_reasons.append("TOKEN_COOLDOWN_OR_LOOKUP_FAILURE")
                if not observation_enabled:
                    return
            from src.analyzer import analyze_token
            from src.risk_manager import (
                paper_cash_balance,
                record_paper_buy,
                record_paper_rejection,
                record_rpc_skip,
            )

            if requested_route == "A":
                record_wallet_ws_activity(
                    "analyzer_reached",
                    discovery_source or "unknown",
                )
            analyzer_started = True
            record_funnel_stage(
                "analyzer_started",
                mint=mint,
                family=requested_route,
            )
            analyzer_memory_start = current_rss_bytes()
            try:
                report = await analyze_token(mint)
            finally:
                record_memory_phase("analyzer", analyzer_memory_start)
            analysis_completed_at = datetime.now(timezone.utc).isoformat()
            record_funnel_stage(
                "analyzer_completed",
                mint=mint,
                family=requested_route,
            )
            if requested_route == "A":
                record_wallet_ws_activity(
                    "analyzer_success",
                    discovery_source or "unknown",
                )
            safety_snapshot = {
                "developer_supply_percent": (
                    getattr(report, "developer_supply_percent", None)
                ),
                "developer_below_ten_percent": (
                    getattr(report, "developer_below_ten_percent", False)
                ),
                "mint_authority_renounced": (
                    getattr(report, "mint_authority_renounced", False)
                ),
                "lp_locked": getattr(report, "lp_locked", False),
                "lp_locked_percent": getattr(report, "lp_locked_percent", None),
                "liquidity_usd": getattr(report, "liquidity_usd", None),
                "liquidity_above_minimum": (
                    getattr(report, "liquidity_above_minimum", False)
                ),
                "reasons": getattr(report, "reasons", []),
                "sources": getattr(report, "sources", []),
            }
            if requested_route == "B" and not route_b_safety_filter(report, mint):
                decision_reasons.append("ROUTE_B_SAFETY_FILTER_REJECTED")
            route_allowed = route_report_allowed(requested_route, report.route_type)
            if not route_allowed:
                operational_rejection = not decision_reasons
                decision_reasons.append("ANALYZER_ROUTE_REJECTED")
                if requested_route == "A" and operational_rejection:
                    from src.wallet_performance import reject_unsafe_buy
                    await reject_unsafe_buy(wallet, mint, report.reasons, signature)
                if operational_rejection:
                    await record_paper_rejection(
                        mint, report.safety_score, report.reasons, wallet, signature
                    )
                logger.info(
                    "paper signal rejected by analyzer: mint=%s score=%s reasons=%s",
                    mint, report.safety_score, "; ".join(report.reasons),
                )
            if decision_reasons and not observation_enabled:
                return
            cash = await paper_cash_balance()
            base_paper_cost = cash * PAPER_BUY_BASIS_POINTS // 10_000
            from src.executor import route_sized_amount
            paper_cost = route_sized_amount(base_paper_cost, requested_route)
            paper_cost = int(Decimal(paper_cost) * route_a_size_multiplier)
            if paper_cost <= 0:
                logger.warning("paper signal has unusable observed price: %s", signature)
                if observation_id:
                    await asyncio.to_thread(
                        finalize_candidate_without_quote,
                        observation_id,
                        decision_status="UNAVAILABLE",
                        decision_reasons=decision_reasons + ["PAPER_SIZE_UNUSABLE"],
                        quote_status="SIZE_UNUSABLE",
                        safety_score=int(report.safety_score),
                        safety_metrics=safety_snapshot,
                        analysis_completed_at=analysis_completed_at,
                    )
                return
            from src.executor import (
                MAX_EXIT_PRICE_IMPACT_PCT,
                jupiter_quote,
                validate_entry_price_impact,
                validate_exit_price_impact,
            )

            timeout = aiohttp.ClientTimeout(total=20)
            quote_preflight_started = True
            record_funnel_stage(
                "quote_preflight_started",
                mint=mint,
                family=requested_route,
            )
            async with aiohttp.ClientSession(timeout=timeout) as session:
                quote = await jupiter_quote(
                    session, os.getenv("JUPITER_API_KEY", "").strip(),
                    WSOL_MINT, mint, paper_cost,
                )
                entry_price_impact = validate_entry_price_impact(quote)
                paper_tokens = int(quote["outAmount"])
                quote_status = "EXECUTABLE"
                exit_price_impact: float | None = None
                try:
                    exit_quote = await jupiter_quote(
                        session,
                        os.getenv("JUPITER_API_KEY", "").strip(),
                        mint,
                        WSOL_MINT,
                        paper_tokens,
                    )
                    exit_price_impact = validate_exit_price_impact(exit_quote)
                except RuntimeError as exc:
                    operational_rejection = not decision_reasons
                    reason = redact_sensitive_text(exc)
                    rejection_reason = (
                        reason
                        if reason.startswith("[ENTRY_REJECTED]")
                        else f"[ENTRY_REJECTED] Exit pre-flight failed: {reason}"
                    )
                    if operational_rejection:
                        await record_paper_rejection(
                            mint,
                            int(report.safety_score),
                            [rejection_reason],
                            wallet,
                            signature,
                        )
                    logger.warning(
                        "%s mint=%s threshold=%.2f",
                        rejection_reason,
                        mint,
                        MAX_EXIT_PRICE_IMPACT_PCT,
                    )
                    decision_reasons.append("EXIT_PREFLIGHT_FAILED")
                    record_funnel_stage(
                        "quote_preflight_failed",
                        mint=mint,
                        family=requested_route,
                    )
                    quote_status = "ENTRY_ONLY"
                    if not observation_enabled:
                        return
            quote_preflight_finished = True
            entry_quote_at = datetime.now(timezone.utc).isoformat()
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
            strategy_version = "baseline_v1"
            if observation_enabled:
                decision = await record_observation_decision(
                    mint=mint,
                    route_type=requested_route,
                    source_wallet=wallet,
                    source_signature=signature,
                    safety_score=int(report.safety_score),
                    entry_cost_lamports=paper_cost,
                    token_amount_raw=paper_tokens,
                    token_decimals=token_decimals,
                    entry_price_impact_pct=entry_price_impact,
                    exit_price_impact_pct=exit_price_impact,
                    expected_slippage_bps=int(
                        quote.get("slippageBps", 100) or 100
                    ),
                    dex_momentum_score=dex_momentum_score,
                    momentum_metrics=momentum_metrics,
                    safety_metrics=safety_snapshot,
                    signal_detected_at=signal_detected_at,
                    analysis_completed_at=analysis_completed_at,
                    entry_quote_at=entry_quote_at,
                    entry_latency_ms=entry_latency_ms,
                    copy_price_gap_pct=copy_price_gap_pct,
                    prospective_feature_collection=(
                        prospective_feature_collection
                    ),
                    decision_status=(
                        "APPROVED" if not decision_reasons else "REJECTED"
                    ),
                    decision_reasons=decision_reasons,
                    quote_status=quote_status,
                )
                observation_id = decision.observation_id
                if decision_reasons:
                    logger.info(
                        "candidate observed without paper entry: mint=%s route=%s reasons=%s",
                        mint,
                        requested_route,
                        ",".join(decision_reasons),
                    )
                    return
                if not (
                    approved_signal_paper_mode_enabled()
                    and decision.created
                ):
                    logger.info(
                        "observation recorded without paper entry: mint=%s "
                        "route=%s momentum=%.2f score=%s variants=%s",
                        mint,
                        requested_route,
                        dex_momentum_score,
                        report.safety_score,
                        ",".join(decision.strategy_variants),
                    )
                    return
                strategy_version = "broad_discovery_v1"
                logger.info(
                    "approved observation promoted to paper experiment: "
                    "mint=%s route=%s momentum=%.2f score=%s",
                    mint,
                    requested_route,
                    dex_momentum_score,
                    report.safety_score,
                )
            try:
                position_id = await record_paper_buy(
                    mint,
                    paper_cost,
                    paper_tokens,
                    token_decimals,
                    source_wallet=wallet,
                    source_signature=signature,
                    safety_score=int(report.safety_score),
                    entry_reason=(
                        "broad_discovery_approved_signal"
                        if strategy_version == "broad_discovery_v1"
                        else "whale_route_a"
                        if requested_route == "A"
                        else "dex_momentum_unknown_whales"
                    ),
                    signal_detected_at=signal_detected_at,
                    analysis_completed_at=analysis_completed_at,
                    entry_quote_at=entry_quote_at,
                    entry_price_impact_pct=entry_price_impact,
                    exit_price_impact_pct=exit_price_impact,
                    expected_slippage_bps=int(
                        quote.get("slippageBps", 100) or 100
                    ),
                    whale_reference_price=whale_reference_price,
                    copy_price_gap_pct=copy_price_gap_pct,
                    entry_latency_ms=entry_latency_ms,
                    route_type=requested_route,
                    dex_momentum_score=dex_momentum_score,
                    strategy_version=strategy_version,
                    observation_id=observation_id,
                    max_strategy_open_positions=(
                        approved_signal_max_open_positions()
                        if strategy_version == "broad_discovery_v1"
                        else None
                    ),
                )
            except RuntimeError as exc:
                if strategy_version == "broad_discovery_v1" and observation_id:
                    status = (
                        "SKIPPED_CAPACITY"
                        if "capacity reached" in str(exc)
                        else "FAILED"
                    )
                    await asyncio.to_thread(
                        mark_paper_experiment_status,
                        observation_id,
                        status,
                    )
                    logger.info(
                        "paper experiment entry skipped: mint=%s status=%s reason=%s",
                        mint,
                        status,
                        redact_sensitive_text(exc),
                    )
                    return
                raise
            if strategy_version == "broad_discovery_v1" and observation_id:
                await asyncio.to_thread(
                    mark_paper_experiment_status,
                    observation_id,
                    "OPENED",
                    position_id=position_id,
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
            if analyzer_started and not analysis_completed_at:
                record_funnel_stage(
                    "analyzer_failed",
                    mint=mint,
                    family=requested_route,
                )
            if quote_preflight_started and not quote_preflight_finished:
                record_funnel_stage(
                    "quote_preflight_failed",
                    mint=mint,
                    family=requested_route,
                )
            if (
                requested_route == "A"
                and analyzer_started
                and not analysis_completed_at
            ):
                record_wallet_ws_activity(
                    "analyzer_failure",
                    discovery_source or "unknown",
                )
            reason = redact_sensitive_text(exc)
            canonical_failure = canonical_rpc_failure_reason(exc)
            if observation_enabled and observation_id:
                try:
                    from src.observation_tracker import finalize_candidate_without_quote

                    await asyncio.to_thread(
                        finalize_candidate_without_quote,
                        observation_id,
                        decision_status="UNAVAILABLE",
                        decision_reasons=decision_reasons + [
                            canonical_failure or reason
                        ],
                        quote_status="PROCESSING_FAILED",
                        safety_metrics=safety_snapshot,
                        analysis_completed_at=analysis_completed_at,
                    )
                except Exception:
                    logger.exception(
                        "candidate terminal status update failed: mint=%s", mint
                    )
            if (
                isinstance(exc, SolanaRpcExhaustedError)
                or canonical_failure is not None
                or "failed after 3 attempts" in reason
                or "getTokenSupply failed" in reason
                or "could not find account" in reason
            ):
                from src.risk_manager import record_rpc_skip
                await record_rpc_skip(mint, wallet, signature, reason)
            logger.info(
                "paper signal skipped: mint=%s reason=%s",
                mint,
                redact_sensitive_text(exc),
            )
        except Exception:
            if analyzer_started and not analysis_completed_at:
                record_funnel_stage(
                    "analyzer_failed",
                    mint=mint,
                    family=requested_route,
                )
            if quote_preflight_started and not quote_preflight_finished:
                record_funnel_stage(
                    "quote_preflight_failed",
                    mint=mint,
                    family=requested_route,
                )
            if (
                requested_route == "A"
                and analyzer_started
                and not analysis_completed_at
            ):
                record_wallet_ws_activity(
                    "analyzer_failure",
                    discovery_source or "unknown",
                )
            if observation_enabled and observation_id:
                try:
                    from src.observation_tracker import finalize_candidate_without_quote

                    await asyncio.to_thread(
                        finalize_candidate_without_quote,
                        observation_id,
                        decision_status="FAILED",
                        decision_reasons=decision_reasons + ["PROCESSING_FAILED"],
                        quote_status="PROCESSING_FAILED",
                        safety_metrics=safety_snapshot,
                        analysis_completed_at=analysis_completed_at,
                    )
                except Exception:
                    logger.exception(
                        "candidate terminal status update failed: mint=%s", mint
                    )
            logger.exception("paper signal processing failed: mint=%s", mint)


def schedule_paper_signal(
    mint: str, acquired_raw: int, token_decimals: int, paid_lamports: int,
    wallet: str, signature: str,
    *,
    prefilter_reasons: tuple[str, ...] = (),
    discovery_source: str = DISCOVERY_SOURCE_HELIUS,
) -> None:
    if prefilter_reasons and len(_shadow_signal_tasks) >= MAX_PENDING_SHADOW_SIGNALS:
        logger.warning(
            "shadow candidate backlog full: mint=%s pending=%s limit=%s",
            mint,
            len(_shadow_signal_tasks),
            MAX_PENDING_SHADOW_SIGNALS,
        )
        return
    signal_detected_at = datetime.now(timezone.utc).isoformat()
    task = asyncio.create_task(
        process_paper_signal(
            mint, acquired_raw, token_decimals, paid_lamports, wallet, signature,
            signal_detected_at,
            "A",
            0.0,
            prefilter_reasons=prefilter_reasons,
            discovery_source=discovery_source,
        )
    )
    track_signal_task(task, shadow=bool(prefilter_reasons))


def print_buys(
    result: dict[str, Any],
    dex_name: str,
    watched_wallets: set[str],
    *,
    discovery_source: str = DISCOVERY_SOURCE_HELIUS,
) -> None:
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
                track_signal_task(observation)

                amount_allowed = whale_buy_amount_allowed(
                    paid_lamports,
                    wallet=wallet,
                    mint=mint,
                    signature=signature,
                )
                record_wallet_ws_activity(
                    "smart_money_candidate",
                    discovery_source,
                )
                schedule_paper_signal(
                    mint,
                    raw,
                    decimals,
                    paid_lamports,
                    wallet,
                    signature,
                    prefilter_reasons=(
                        () if amount_allowed else ("WHALE_AMOUNT_FILTER_REJECTED",)
                    ),
                    discovery_source=discovery_source,
                )
            else:
                schedule_paper_signal(
                    mint,
                    raw,
                    decimals,
                    0,
                    wallet,
                    signature,
                    prefilter_reasons=("PAYMENT_UNRESOLVED",),
                    discovery_source=discovery_source,
                )


def momentum_score(volume_m5_usd: float, buys_m5: int, sells_m5: int) -> float:
    """Score bounded five-minute activity without retaining a time-series."""
    volume_points = min(60.0, max(0.0, volume_m5_usd) / 500.0)
    net_buys = max(0, buys_m5 - sells_m5)
    imbalance_points = min(40.0, net_buys * 2.0)
    return round(volume_points + imbalance_points, 4)


def momentum_is_still_strong(
    original: MomentumCandidate,
    refreshed: MomentumCandidate,
) -> bool:
    """고래 확인 중 약해진 B 경로 모멘텀을 추격하지 않는다."""
    return (
        refreshed.mint == original.mint
        and refreshed.volume_m5_usd >= original.volume_m5_usd
        and refreshed.buys_m5 - refreshed.sells_m5
        >= original.buys_m5 - original.sells_m5
        and refreshed.momentum_score >= original.momentum_score
    )


def momentum_candidate_from_pair(
    pair: dict[str, Any],
    *,
    now_ms: float | None = None,
) -> MomentumCandidate | None:
    evaluated = momentum_shadow_candidate_from_pair(pair, now_ms=now_ms)
    if evaluated is None or evaluated.rejection_reasons:
        return None
    return evaluated.candidate


def _momentum_pair_projection(
    pair: dict[str, Any],
) -> _MomentumPairProjection:
    """후보 판정에 필요한 scalar만 raw pair에서 즉시 분리한다."""
    is_solana = pair.get("chainId") == "solana"
    if not is_solana:
        return _MomentumPairProjection(False, "", "")
    try:
        base = pair.get("baseToken") or {}
        mint = str(base.get("address") or "")
    except AttributeError:
        return _MomentumPairProjection(True, "", "", structural_error=True)
    pair_address = str(pair.get("pairAddress") or "")
    if not mint or not pair_address or mint in {WSOL_MINT, USDC_MINT}:
        return _MomentumPairProjection(True, mint, pair_address)

    def finite_value(value: Any) -> float | None:
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return number if math.isfinite(number) else None

    try:
        txns_m5 = (pair.get("txns") or {}).get("m5") or {}
        buys_m5 = finite_value(txns_m5.get("buys"))
        sells_m5 = finite_value(txns_m5.get("sells"))
        volume_m5_usd = finite_value((pair.get("volume") or {}).get("m5"))
        liquidity_usd = finite_value(
            (pair.get("liquidity") or {}).get("usd")
        )
    except AttributeError:
        return _MomentumPairProjection(
            True, mint, pair_address, structural_error=True
        )
    return _MomentumPairProjection(
        is_solana=True,
        mint=mint,
        pair_address=pair_address,
        buys_m5=buys_m5,
        sells_m5=sells_m5,
        volume_m5_usd=volume_m5_usd,
        liquidity_usd=liquidity_usd,
        pair_created_at_ms=finite_value(pair.get("pairCreatedAt")),
        price_usd=finite_value(pair.get("priceUsd")),
    )


def _momentum_shadow_candidate_from_projection(
    projection: _MomentumPairProjection,
    *,
    now_ms: float | None = None,
) -> MomentumShadowCandidate | None:
    if not projection.is_solana:
        return None
    if projection.structural_error:
        raise AttributeError("malformed DexScreener pair structure")
    mint = projection.mint
    pair_address = projection.pair_address
    if not mint or not pair_address or mint in {WSOL_MINT, USDC_MINT}:
        return None
    volume_value = projection.volume_m5_usd
    buys_value = projection.buys_m5
    sells_value = projection.sells_m5
    liquidity_value = projection.liquidity_usd
    created_value = projection.pair_created_at_ms
    price_value = projection.price_usd
    metric_invalid = any(value is None for value in (
        volume_value, buys_value, sells_value, liquidity_value
    ))
    volume_m5 = max(0.0, volume_value or 0.0)
    buys_m5 = max(0, int(buys_value or 0))
    sells_m5 = max(0, int(sells_value or 0))
    liquidity = max(0.0, liquidity_value or 0.0)
    pair_created_at_ms = created_value or 0.0
    current_ms = float(now_ms if now_ms is not None else time.time() * 1000)
    if not math.isfinite(current_ms):
        return None
    pair_age_seconds = (current_ms - pair_created_at_ms) / 1000
    net_buys = buys_m5 - sells_m5
    reasons: list[str] = []
    if metric_invalid:
        reasons.append("MOMENTUM_METRIC_INVALID")
    if pair_created_at_ms <= 0:
        reasons.append("PAIR_CREATED_AT_INVALID")
    elif pair_created_at_ms > current_ms:
        reasons.append("PAIR_CREATED_IN_FUTURE")
    elif pair_age_seconds < MOMENTUM_MIN_PAIR_AGE_SECONDS:
        reasons.append("PAIR_TOO_YOUNG")
    if liquidity < MOMENTUM_MIN_LIQUIDITY_USD:
        reasons.append("MOMENTUM_LIQUIDITY_UNDER_MIN")
    if volume_m5 < MOMENTUM_MIN_VOLUME_M5_USD:
        reasons.append("MOMENTUM_VOLUME_UNDER_MIN")
    if net_buys < MOMENTUM_MIN_NET_BUYS_M5:
        reasons.append("MOMENTUM_NET_BUYS_UNDER_MIN")
    if buys_m5 < sells_m5 * MOMENTUM_MIN_BUY_SELL_RATIO:
        reasons.append("MOMENTUM_BUY_SELL_RATIO_UNDER_MIN")
    candidate = MomentumCandidate(
        mint=mint,
        pair_address=pair_address,
        volume_m5_usd=volume_m5,
        buys_m5=buys_m5,
        sells_m5=sells_m5,
        liquidity_usd=liquidity,
        momentum_score=momentum_score(volume_m5, buys_m5, sells_m5),
        pair_age_seconds=pair_age_seconds,
        price_usd=(
            price_value
            if price_value is not None and price_value >= 0
            else None
        ),
    )
    return MomentumShadowCandidate(candidate, tuple(reasons))


def momentum_shadow_candidate_from_pair(
    pair: dict[str, Any],
    *,
    now_ms: float | None = None,
) -> MomentumShadowCandidate | None:
    """유효 Solana 페어를 파싱하고 현행 B 필터 탈락 사유를 분리한다."""
    return _momentum_shadow_candidate_from_projection(
        _momentum_pair_projection(pair),
        now_ms=now_ms,
    )


def _search_pair_projections(payload: Any) -> list[_MomentumPairProjection]:
    projections: list[_MomentumPairProjection] = []
    if not isinstance(payload, dict):
        return projections
    for pair in payload.get("pairs") or []:
        if isinstance(pair, dict):
            projections.append(_momentum_pair_projection(pair))
        if len(projections) >= MOMENTUM_MAX_RAW_PAIRS:
            break
    return projections


def _extend_discovered_mints(payload: Any, discovered_mints: list[str]) -> None:
    rows = payload if isinstance(payload, list) else [payload]
    for row in rows:
        if not isinstance(row, dict) or row.get("chainId") != "solana":
            continue
        mint = str(row.get("tokenAddress") or "")
        if mint and mint not in discovered_mints:
            discovered_mints.append(mint)
        if len(discovered_mints) >= MOMENTUM_MAX_DISCOVERY_TOKENS:
            break


def _token_pair_projections(
    payload: Any,
    *,
    limit: int,
) -> list[_MomentumPairProjection]:
    if not isinstance(payload, list):
        return []
    return [
        _momentum_pair_projection(pair)
        for pair in payload[:max(0, int(limit))]
        if isinstance(pair, dict)
    ]


async def _dexscreener_json(
    session: aiohttp.ClientSession, url: str, **params: str
) -> Any:
    async with session.get(
        url,
        params=params or None,
        headers={"accept": "application/json"},
    ) as response:
        content_length = getattr(response, "content_length", None)
        content_length_known = (
            isinstance(content_length, int) and content_length >= 0
        )
        add_current_phase_metadata(
            response_count=1,
            response_bytes=int(content_length) if content_length_known else 0,
            missing_length_count=int(not content_length_known),
            content_length_known=content_length_known,
        )
        response.raise_for_status()
        return await response.json()


async def _fetch_momentum_candidate_cohorts(
    session: aiohttp.ClientSession,
) -> tuple[list[MomentumCandidate], list[MomentumShadowCandidate]]:
    """승인 후보와 현행 임계값 바로 아래 shadow 후보를 함께 반환한다."""
    pair_projections = _search_pair_projections(
        await _dexscreener_json(
            session, DEX_SCREENER_SEARCH_URL, q="solana"
        )
    )
    discovered_mints: list[str] = []
    for url in (DEX_SCREENER_PROFILES_URL, DEX_SCREENER_BOOSTS_URL):
        _extend_discovered_mints(
            await _dexscreener_json(session, url),
            discovered_mints,
        )
    if discovered_mints:
        remaining = max(
            0, MOMENTUM_MAX_RAW_PAIRS - len(pair_projections)
        )
        pair_projections.extend(
            _token_pair_projections(
                await _dexscreener_json(
                    session,
                    f"{DEX_SCREENER_TOKENS_URL}/{','.join(discovered_mints)}",
                ),
                limit=remaining,
            )
        )

    best_by_mint: dict[str, MomentumCandidate] = {}
    shadow_by_mint: dict[str, MomentumShadowCandidate] = {}
    snapshot_at_epoch = time.time()
    for projection in pair_projections:
        evaluated = _momentum_shadow_candidate_from_projection(
            projection, now_ms=snapshot_at_epoch * 1000
        )
        if evaluated is None:
            continue
        candidate = evaluated.candidate
        record_funnel_stage(
            "poll_candidate_observed",
            mint=candidate.mint,
            family="MOMENTUM",
            timestamp=snapshot_at_epoch,
        )
        if evaluated.rejection_reasons:
            incumbent_shadow = shadow_by_mint.get(candidate.mint)
            if (
                incumbent_shadow is None
                or (
                    len(evaluated.rejection_reasons),
                    -candidate.momentum_score,
                    -candidate.volume_m5_usd,
                )
                < (
                    len(incumbent_shadow.rejection_reasons),
                    -incumbent_shadow.candidate.momentum_score,
                    -incumbent_shadow.candidate.volume_m5_usd,
                )
            ):
                shadow_by_mint[candidate.mint] = evaluated
            continue
        incumbent = best_by_mint.get(candidate.mint)
        if incumbent is None or candidate.momentum_score > incumbent.momentum_score:
            best_by_mint[candidate.mint] = candidate
    for mint in best_by_mint:
        shadow_by_mint.pop(mint, None)
    projected_candidates = list(best_by_mint.values()) + [
        item.candidate for item in shadow_by_mint.values()
    ]
    for candidate in projected_candidates:
        _momentum_snapshot_store.record(
            mint=candidate.mint,
            pair_address=candidate.pair_address,
            snapshot_at_epoch=snapshot_at_epoch,
            volume_m5_usd=candidate.volume_m5_usd,
            buys_m5=candidate.buys_m5,
            sells_m5=candidate.sells_m5,
            liquidity_usd=candidate.liquidity_usd,
            price_usd=candidate.price_usd,
        )
    approved = sorted(
        best_by_mint.values(),
        key=lambda item: (
            item.momentum_score,
            item.volume_m5_usd,
            item.buys_m5 - item.sells_m5,
        ),
        reverse=True,
    )[:MOMENTUM_MAX_CANDIDATES]
    shadows = sorted(
        shadow_by_mint.values(),
        key=lambda item: (
            len(item.rejection_reasons),
            -item.candidate.momentum_score,
            -item.candidate.volume_m5_usd,
        ),
    )[:MOMENTUM_MAX_SHADOW_CANDIDATES]
    for candidate in approved:
        record_funnel_stage(
            "candidate_considered",
            mint=candidate.mint,
            family="MOMENTUM",
            timestamp=snapshot_at_epoch,
        )
    for shadow in shadows:
        record_funnel_stage(
            "candidate_considered",
            mint=shadow.candidate.mint,
            family="MOMENTUM",
            timestamp=snapshot_at_epoch,
        )
    add_current_phase_metadata(
        row_count=len(pair_projections),
        approved_count=len(approved),
        shadow_count=len(shadows),
        active_series_count=_momentum_snapshot_store.series_count,
        snapshot_count=_momentum_snapshot_store.snapshot_count,
    )
    return approved, shadows


async def fetch_momentum_candidate_cohorts(
    session: aiohttp.ClientSession,
    *,
    revalidation: bool = False,
) -> tuple[list[MomentumCandidate], list[MomentumShadowCandidate]]:
    """후보 fetch/parse/projection 전체의 sub-minute high-water를 기록한다."""
    with phase_memory(
        "candidate_fetch",
        metadata={
            "workload": "momentum",
            "operation": "fetch",
            "revalidation": bool(revalidation),
        },
        include_gc_counts=True,
        include_object_count=True,
    ) as scope:
        approved, shadows = await _fetch_momentum_candidate_cohorts(session)
        scope.add_metadata(candidate_count=len(approved) + len(shadows))
        return approved, shadows


async def fetch_momentum_candidates(
    session: aiohttp.ClientSession,
) -> list[MomentumCandidate]:
    """기존 거래 경로에는 현행 필터 승인 후보만 반환한다."""
    approved, _ = await fetch_momentum_candidate_cohorts(
        session, revalidation=True
    )
    return approved


async def _solana_rpc(
    session: aiohttp.ClientSession,
    http_url: str,
    method: str,
    params: list[Any],
) -> Any:
    del http_url  # Kept for call-site compatibility; the router owns endpoints.
    return await solana_rpc_call(
        session,
        method,
        params,
        workload="transaction_history",
    )


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
    with phase_memory(
        "whale_signature_retrieval",
        metadata={"workload": "momentum", "operation": "fetch"},
    ):
        signatures = await _solana_rpc(
            session,
            http_url,
            "getSignaturesForAddress",
            [
                candidate.pair_address,
                {
                    "limit": UNKNOWN_WHALE_SIGNATURE_LIMIT,
                    "commitment": "confirmed",
                },
            ],
        )
    with phase_memory(
        "whale_signature_projection",
        metadata={"workload": "momentum", "operation": "parse"},
    ) as projection_scope:
        transaction_signatures = [
            str(row.get("signature"))
            for row in (signatures or [])
            if (
                isinstance(row, dict)
                and row.get("signature")
                and row.get("err") is None
            )
        ]
        projection_scope.add_metadata(
            signature_count=len(signatures or []),
            projected_count=len(transaction_signatures),
            retained_count=len(transaction_signatures),
        )
    add_current_phase_metadata(signature_count=len(transaction_signatures))
    if not transaction_signatures:
        return []
    by_wallet: dict[str, UnknownWhaleBuy] = {}
    transaction: Any = None
    # Sequential early-exit reads keep peak memory flat and stop as soon as
    # three qualifying wallets exist.
    for signature in transaction_signatures:
        add_current_phase_metadata(transaction_count=1)
        with phase_memory(
            "whale_transaction_fetch",
            metadata={
                "workload": "transaction",
                "operation": "fetch",
                "transaction_count": 1,
                # 이전 raw transaction은 matching 직후 해제되어야 한다.
                "retained_count": int(isinstance(transaction, dict)),
            },
        ):
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
            transaction = None
            continue
        add_current_phase_metadata(retained_count=1)
        with phase_memory(
            "whale_transaction_matching",
            metadata={
                "workload": "transaction",
                "operation": "analyze",
                "transaction_count": 1,
            },
        ) as matching_scope:
            matched_buys = unknown_whale_buy_from_transaction(
                transaction, candidate.mint, watched_wallets
            )
            matching_scope.add_metadata(projected_count=len(matched_buys))
        # 이후 단계는 compact projection만 사용하므로 raw graph를 해제한다.
        transaction = None
        with phase_memory(
            "whale_confirmation_aggregation",
            metadata={"workload": "momentum", "operation": "update"},
        ) as aggregation_scope:
            for buy in matched_buys:
                incumbent = by_wallet.get(buy.wallet)
                if incumbent is None or buy.paid_lamports > incumbent.paid_lamports:
                    by_wallet[buy.wallet] = buy
            aggregation_scope.add_metadata(
                projected_count=len(matched_buys),
                retained_count=len(by_wallet),
            )
        if len(by_wallet) >= UNKNOWN_WHALE_MIN_COUNT:
            break
    add_current_phase_metadata(
        candidate_count=len(by_wallet),
        early_exit=len(by_wallet) >= UNKNOWN_WHALE_MIN_COUNT,
    )
    with phase_memory(
        "whale_result_projection",
        metadata={
            "workload": "momentum",
            "operation": "serialize",
            "retained_count": int(isinstance(transaction, dict)),
        },
    ) as result_scope:
        result = sorted(
            by_wallet.values(), key=lambda buy: buy.paid_lamports, reverse=True
        )
        result_scope.add_metadata(projected_count=len(result))
        return result


def momentum_observation_metrics(
    candidate: MomentumCandidate,
    *,
    unknown_whale_count: int,
) -> dict[str, int | float]:
    return {
        "volume_m5_usd": candidate.volume_m5_usd,
        "buys_m5": candidate.buys_m5,
        "sells_m5": candidate.sells_m5,
        "net_buys_m5": candidate.buys_m5 - candidate.sells_m5,
        "buy_sell_ratio_m5": candidate.buys_m5 / max(1, candidate.sells_m5),
        "liquidity_usd": candidate.liquidity_usd,
        "pair_age_seconds": candidate.pair_age_seconds,
        "unknown_whale_count": unknown_whale_count,
    }


def momentum_prospective_feature_collection(
    candidate: MomentumCandidate,
    *,
    signal_detected_at: str,
) -> dict[str, Any]:
    """현재 pair와 signal 시각에 맞는 과거 projection만 반환한다."""
    return _momentum_snapshot_store.collection(
        mint=candidate.mint,
        pair_address=candidate.pair_address,
        signal_timestamp=signal_detected_at,
    )


def _confirmation_failure_telemetry(
    error: BaseException,
) -> tuple[str, str, str]:
    """민감정보 없이 confirmation failure dimension을 정규화한다."""
    method = str(getattr(error, "method", "confirmation_bundle") or "unknown")
    provider = str(getattr(error, "last_provider", "router") or "router")
    if isinstance(error, SolanaRpcRateLimitExhaustedError):
        result = "rate_limit"
    elif isinstance(error, asyncio.TimeoutError) or (
        str(getattr(error, "last_category", "")).upper() == "TIMEOUT"
    ):
        result = "timeout"
    elif str(getattr(error, "last_category", "")).upper() == "CONNECTION":
        result = "connection"
    elif isinstance(error, SolanaRpcExhaustedError):
        result = "exhausted"
    else:
        result = "other"
    return method, provider, result


async def _confirm_unknown_whales_with_funnel_telemetry(
    session: aiohttp.ClientSession,
    http_url: str,
    candidate: MomentumCandidate,
    watched_wallets: set[str],
) -> list[UnknownWhaleBuy]:
    """Observation 이전 confirmation funnel도 누락 없이 집계한다."""
    record_funnel_stage(
        "rpc_confirmation_started",
        mint=candidate.mint,
        family="MOMENTUM",
    )
    try:
        whales = await asyncio.wait_for(
            confirm_unknown_whales(
                session, http_url, candidate, watched_wallets
            ),
            timeout=ROUTE_B_CONFIRM_TIMEOUT_SECONDS,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        method, provider, result = _confirmation_failure_telemetry(exc)
        record_funnel_stage(
            "rpc_confirmation_failed",
            mint=candidate.mint,
            family="MOMENTUM",
        )
        record_confirmation_result(
            mint=candidate.mint,
            family="MOMENTUM",
            method=method,
            provider=provider,
            result=result,
        )
        raise
    record_funnel_stage(
        "rpc_confirmation_succeeded",
        mint=candidate.mint,
        family="MOMENTUM",
    )
    record_confirmation_result(
        mint=candidate.mint,
        family="MOMENTUM",
        method="confirmation_bundle",
        provider="router",
        result="success",
    )
    return whales


async def _confirm_unknown_whales_with_telemetry(
    session: aiohttp.ClientSession,
    http_url: str,
    candidate: MomentumCandidate,
    watched_wallets: set[str],
) -> list[UnknownWhaleBuy]:
    """기존 funnel과 별도로 confirmation memory high-water를 기록한다."""
    with phase_memory(
        "whale_confirmation",
        metadata={"workload": "momentum", "operation": "confirm"},
        include_gc_counts=True,
        include_object_count=True,
    ) as scope:
        whales = await _confirm_unknown_whales_with_funnel_telemetry(
            session, http_url, candidate, watched_wallets
        )
        scope.add_metadata(success_count=len(whales))
        return whales


def schedule_market_shadow(
    candidate: MomentumCandidate,
    rejection_reasons: tuple[str, ...],
    *,
    now: float,
    unknown_whale_count: int = 0,
) -> bool:
    """B near-miss를 전역·mint·task 상한 안에서 관찰 전용으로 예약한다."""
    global _last_market_shadow_capture_at
    if not rejection_reasons:
        raise ValueError("market shadow requires at least one rejection reason")
    if candidate.mint in _market_shadow_cooldowns:
        return False
    if len(_shadow_signal_tasks) >= MAX_PENDING_SHADOW_SIGNALS:
        return False
    if (
        _last_market_shadow_capture_at > 0
        and now - _last_market_shadow_capture_at
        < MOMENTUM_SHADOW_CAPTURE_INTERVAL_SECONDS
    ):
        return False
    _market_shadow_cooldowns[candidate.mint] = (
        now + MOMENTUM_ENTRY_COOLDOWN_SECONDS
    )
    _last_market_shadow_capture_at = now
    shadow_signature = (
        f"dexscreener-shadow:{candidate.pair_address}:"
        f"{int(time.time() // MOMENTUM_ENTRY_COOLDOWN_SECONDS)}"
    )
    signal_detected_at = datetime.now(timezone.utc).isoformat()
    task = asyncio.create_task(
        process_paper_signal(
            candidate.mint,
            0,
            0,
            0,
            "market-near-miss",
            shadow_signature,
            signal_detected_at,
            "B",
            candidate.momentum_score,
            momentum_metrics=momentum_observation_metrics(
                candidate,
                unknown_whale_count=unknown_whale_count,
            ),
            prospective_feature_collection=(
                momentum_prospective_feature_collection(
                    candidate,
                    signal_detected_at=signal_detected_at,
                )
            ),
            prefilter_reasons=rejection_reasons,
        )
    )
    track_signal_task(task, shadow=True)
    return True


async def run_market_momentum_route(settings: MonitorSettings) -> None:
    """Lean Route B loop: one bounded snapshot and one candidate scan per tick."""
    global _last_route_b_health_write_at, _route_b_consecutive_failures
    timeout = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            started = time.monotonic()
            try:
                wallets = set(load_wallets(settings.wallets_path))
                momentum_fetch_memory_start = current_rss_bytes()
                try:
                    candidates, near_misses = (
                        await fetch_momentum_candidate_cohorts(session)
                    )
                finally:
                    record_memory_phase(
                        "momentum_candidate_fetch",
                        momentum_fetch_memory_start,
                    )
                _route_b_consecutive_failures = 0
                health_now = time.time()
                if (
                    health_now - _last_route_b_health_write_at
                    >= HEALTH_WRITE_INTERVAL_SECONDS
                ):
                    await asyncio.to_thread(
                        state_store.set_global_metrics,
                        {
                            "route_b_last_poll_success_at": health_now,
                            "route_b_consecutive_failures": 0,
                        },
                    )
                    _last_route_b_health_write_at = health_now
                now = time.monotonic()
                for mint, expires_at in list(_market_entry_cooldowns.items()):
                    if expires_at <= now:
                        _market_entry_cooldowns.pop(mint, None)
                for mint, expires_at in list(_market_shadow_cooldowns.items()):
                    if expires_at <= now:
                        _market_shadow_cooldowns.pop(mint, None)
                candidate = next(
                    (
                        item
                        for item in candidates
                        if item.mint not in _market_entry_cooldowns
                    ),
                    None,
                )
                shadow_scheduled = False
                if candidate is not None:
                    momentum_whale_memory_start = current_rss_bytes()
                    try:
                        whales = await _confirm_unknown_whales_with_telemetry(
                            session,
                            settings.http_url,
                            candidate,
                            wallets,
                        )
                    finally:
                        record_memory_phase(
                            "momentum_whale_confirmation",
                            momentum_whale_memory_start,
                        )
                    if len(whales) >= UNKNOWN_WHALE_MIN_COUNT:
                        refreshed_candidates = await fetch_momentum_candidates(session)
                        refreshed = next(
                            (
                                item
                                for item in refreshed_candidates
                                if item.mint == candidate.mint
                            ),
                            None,
                        )
                        if (
                            refreshed is None
                            or not momentum_is_still_strong(candidate, refreshed)
                        ):
                            logger.info(
                                "FAIL_ROUTE_B_MOMENTUM_WEAKENED mint=%s "
                                "initial_score=%.2f refreshed_score=%s",
                                candidate.mint,
                                candidate.momentum_score,
                                (
                                    f"{refreshed.momentum_score:.2f}"
                                    if refreshed is not None
                                    else "missing"
                                ),
                            )
                            shadow_scheduled = schedule_market_shadow(
                                candidate,
                                ((
                                    "MOMENTUM_REFRESH_MISSING"
                                    if refreshed is None
                                    else "MOMENTUM_WEAKENED"
                                ),),
                                now=now,
                                unknown_whale_count=len(whales),
                            )
                            if not shadow_scheduled:
                                for shadow in near_misses:
                                    if schedule_market_shadow(
                                        shadow.candidate,
                                        shadow.rejection_reasons,
                                        now=now,
                                    ):
                                        break
                            await asyncio.sleep(DEX_SCREENER_POLL_SECONDS)
                            continue
                        candidate = refreshed
                        strongest = whales[0]
                        _market_entry_cooldowns[candidate.mint] = (
                            now + MOMENTUM_ENTRY_COOLDOWN_SECONDS
                        )
                        signal_signature = (
                            f"dexscreener:{candidate.pair_address}:"
                            f"{int(time.time())}"
                        )
                        signal_detected_at = datetime.now(timezone.utc).isoformat()
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
                                signal_detected_at,
                                "B",
                                candidate.momentum_score,
                                momentum_metrics={
                                    "volume_m5_usd": candidate.volume_m5_usd,
                                    "buys_m5": candidate.buys_m5,
                                    "sells_m5": candidate.sells_m5,
                                    "net_buys_m5": (
                                        candidate.buys_m5 - candidate.sells_m5
                                    ),
                                    "buy_sell_ratio_m5": (
                                        candidate.buys_m5
                                        / max(1, candidate.sells_m5)
                                    ),
                                    "liquidity_usd": candidate.liquidity_usd,
                                    "pair_age_seconds": candidate.pair_age_seconds,
                                    "unknown_whale_count": len(whales),
                                },
                                prospective_feature_collection=(
                                    momentum_prospective_feature_collection(
                                        candidate,
                                        signal_detected_at=signal_detected_at,
                                    )
                                ),
                            )
                        )
                        track_signal_task(task)
                    else:
                        shadow_scheduled = schedule_market_shadow(
                            candidate,
                            ("UNKNOWN_WHALES_UNDER_MIN",),
                            now=now,
                            unknown_whale_count=len(whales),
                        )
                if not shadow_scheduled:
                    captured = 0
                    for shadow in near_misses:
                        if schedule_market_shadow(
                            shadow.candidate,
                            shadow.rejection_reasons,
                            now=now,
                        ):
                            captured += 1
                        if captured >= MOMENTUM_SHADOWS_PER_TICK:
                            break
            except asyncio.CancelledError:
                raise
            except Exception:
                _route_b_consecutive_failures += 1
                health_now = time.time()
                if (
                    _route_b_consecutive_failures == 1
                    or health_now - _last_route_b_health_write_at
                    >= HEALTH_WRITE_INTERVAL_SECONDS
                ):
                    await asyncio.to_thread(
                        state_store.set_global_metrics,
                        {
                            "route_b_last_poll_failure_at": health_now,
                            "route_b_consecutive_failures": (
                                _route_b_consecutive_failures
                            ),
                        },
                    )
                    _last_route_b_health_write_at = health_now
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
    for _ in range(4):
        transaction_memory_start = current_rss_bytes()
        try:
            result = await solana_rpc_call(
                session,
                "getTransaction",
                [
                    signature,
                    {
                        "commitment": "confirmed",
                        "encoding": "jsonParsed",
                        "maxSupportedTransactionVersion": 0,
                    },
                ],
                workload="transaction_history",
            )
        finally:
            record_memory_phase(
                "smart_get_transaction", transaction_memory_start
            )
        if isinstance(result, dict):
            record_transaction_payload(result)
            return result
        await asyncio.sleep(0.4)
    return None


@contextlib.asynccontextmanager
async def _connect_with_memory_phase(
    url: str, *, phase_mode: str, **options: Any
):
    connection = connect(url, **options)
    with phase_memory(
        "ws_refresh_reconnect",
        metadata={
            "workload": "websocket",
            "operation": "connect",
            "mode": phase_mode,
            "kind": "steady",
            "result": "unknown",
        },
        include_gc_counts=True,
        include_object_count=True,
    ) as scope:
        try:
            socket = await connection.__aenter__()
        except BaseException:
            scope.set_metadata({"result": "failure"})
            raise
        scope.set_metadata({"result": "success"})
    try:
        yield socket
    except BaseException as exc:
        suppressed = await connection.__aexit__(
            type(exc), exc, exc.__traceback__
        )
        if not suppressed:
            raise
    else:
        await connection.__aexit__(None, None, None)


async def monitor_standard_once(
    settings: MonitorSettings, wallets: tuple[str, ...]
) -> None:
    """Public fallback: wallet logs, then fetch only matching DEX transactions."""
    seen = SignatureWindow()
    timeout = aiohttp.ClientTimeout(total=15)
    async with _connect_with_memory_phase(
        settings.standard_ws_url,
        phase_mode="standard",
        ping_interval=20,
        ping_timeout=20,
        open_timeout=20,
        max_queue=512,
    ) as socket, aiohttp.ClientSession(timeout=timeout) as session:
        connected_at = time.time()
        record_wallet_ws_activity("connection_success", DISCOVERY_SOURCE_SOLANA)
        await asyncio.to_thread(
            state_store.set_global_metrics,
            {
                "wallet_ws_connected_at": connected_at,
                "wallet_ws_endpoint_kind": "SOLANA_PUBLIC_WSS",
                "wallet_ws_subscription_method": "logsSubscribe",
                "wallet_ws_active_source": DISCOVERY_SOURCE_SOLANA,
            },
        )
        for request_id, wallet in enumerate(wallets, start=1):
            await socket.send(json.dumps({
                "jsonrpc": "2.0", "id": request_id, "method": "logsSubscribe",
                "params": [{"mentions": [wallet]}, {"commitment": "confirmed"}],
            }))
        acknowledgements = 0
        subscriptions_ready = False
        async for raw_message in socket:
            memory_context = (
                phase_memory(
                    "ws_refresh_reconnect",
                    metadata={
                        "workload": "websocket",
                        "operation": "connect",
                        "mode": "standard",
                        "kind": "steady",
                        "result": "unknown",
                    },
                )
                if not subscriptions_ready else contextlib.nullcontext()
            )
            with memory_context as ws_scope:
                if ws_scope is not None:
                    ws_scope.add_metadata(payload_bytes=len(raw_message))
                message = json.loads(raw_message)
                if "id" in message:
                    if "error" in message:
                        if ws_scope is not None:
                            ws_scope.set_metadata({"result": "failure"})
                        raise WebSocketSubscriptionRejected(
                            websocket_subscription_failure_reason(message["error"])
                        )
                    acknowledgements += 1
                    if acknowledgements == len(wallets):
                        subscriptions_ready = True
                        if ws_scope is not None:
                            ws_scope.add_metadata(
                                subscription_count=acknowledgements
                            )
                            ws_scope.set_metadata({"result": "success"})
                        record_wallet_ws_activity(
                            "subscription_success", DISCOVERY_SOURCE_SOLANA
                        )
                        await asyncio.to_thread(
                            state_store.set_global_metrics,
                            {
                                "wallet_ws_state": "SUBSCRIBED",
                                "wallet_ws_subscribed_at": time.time(),
                                "wallet_ws_mode": "STANDARD",
                                "wallet_ws_active_source": DISCOVERY_SOURCE_SOLANA,
                                "wallet_ws_last_success_at": time.time(),
                                "wallet_ws_consecutive_failures": 0,
                                "wallet_ws_state_changed_at": time.time(),
                            },
                        )
                        logger.info(
                            "standard fallback subscribed to %s wallets", len(wallets)
                        )
                        logger.info(
                            "SUCCESS: actively monitoring %s verified whale wallets in real time",
                            len(wallets),
                        )
                    continue
                value = ((message.get("params") or {}).get("result") or {}).get("value") or {}
                signature = value.get("signature")
                if not signature or value.get("err") is not None:
                    continue
                record_wallet_ws_activity("notification", DISCOVERY_SOURCE_SOLANA)
                if not seen.add(str(signature)):
                    continue
                record_wallet_ws_activity(
                    "unique_signature", DISCOVERY_SOURCE_SOLANA
                )
                logs = value.get("logs") or []
                dex_name = next(
                    (name for name, program in DEX_PROGRAMS.items()
                     if any(program in line for line in logs)), None,
                )
                if not dex_name:
                    continue
                record_wallet_ws_activity("dex_log_match", DISCOVERY_SOURCE_SOLANA)
                record_wallet_ws_activity("transaction_fetch", DISCOVERY_SOURCE_SOLANA)
                try:
                    transaction = await fetch_transaction(
                        session, settings.http_url, signature
                    )
                except Exception as exc:
                    failure_reason = record_transaction_restore_failure(
                        DISCOVERY_SOURCE_SOLANA, exc
                    )
                    logger.warning(
                        "standard transaction restore failed category=%s",
                        failure_reason,
                    )
                    continue
                if transaction:
                    record_wallet_ws_activity(
                        "transaction_restore_success", DISCOVERY_SOURCE_SOLANA
                    )
                    record_wallet_ws_activity(
                        "transaction_parsed", DISCOVERY_SOURCE_SOLANA
                    )
                    print_buys(
                        transaction,
                        dex_name,
                        set(wallets),
                        discovery_source=DISCOVERY_SOURCE_SOLANA,
                    )
                else:
                    record_transaction_restore_failure(
                        DISCOVERY_SOURCE_SOLANA, None
                    )


async def monitor_once(settings: MonitorSettings, wallets: tuple[str, ...]) -> None:
    request_to_dex = {index: name for index, name in enumerate(DEX_PROGRAMS, start=1)}
    subscription_to_dex: dict[int, str] = {}
    async with _connect_with_memory_phase(
        settings.ws_url,
        phase_mode="enhanced",
        ping_interval=20,
        ping_timeout=20,
        open_timeout=20,
        max_queue=512,
    ) as socket:
        connected_at = time.time()
        record_wallet_ws_activity("connection_success", DISCOVERY_SOURCE_HELIUS)
        await asyncio.to_thread(
            state_store.set_global_metrics,
            {
                "wallet_ws_connected_at": connected_at,
                "wallet_ws_endpoint_kind": "HELIUS_WSS",
                "wallet_ws_subscription_method": "transactionSubscribe",
                "wallet_ws_active_source": DISCOVERY_SOURCE_HELIUS,
            },
        )
        for request_id, (name, program) in enumerate(DEX_PROGRAMS.items(), start=1):
            await socket.send(json.dumps(subscription_request(request_id, wallets, program)))

        ping_task = asyncio.create_task(keepalive(socket))
        subscriptions_ready = False
        try:
            async for raw_message in socket:
                memory_context = (
                    phase_memory(
                        "ws_refresh_reconnect",
                        metadata={
                            "workload": "websocket",
                            "operation": "connect",
                            "mode": "enhanced",
                            "kind": "steady",
                            "result": "unknown",
                        },
                    )
                    if not subscriptions_ready else contextlib.nullcontext()
                )
                with memory_context as ws_scope:
                    if ws_scope is not None:
                        ws_scope.add_metadata(payload_bytes=len(raw_message))
                    message = json.loads(raw_message)
                    if "id" in message:
                        if "error" in message:
                            if ws_scope is not None:
                                ws_scope.set_metadata({"result": "failure"})
                            if "not available on the free plan" in str(message["error"]):
                                raise EnhancedSubscriptionUnavailable
                            raise WebSocketSubscriptionRejected(
                                websocket_subscription_failure_reason(message["error"])
                            )
                        request_id = int(message["id"])
                        subscription_to_dex[int(message["result"])] = request_to_dex[request_id]
                        if len(subscription_to_dex) == len(DEX_PROGRAMS):
                            subscriptions_ready = True
                            if ws_scope is not None:
                                ws_scope.add_metadata(
                                    subscription_count=len(subscription_to_dex)
                                )
                                ws_scope.set_metadata({"result": "success"})
                            record_wallet_ws_activity(
                                "subscription_success", DISCOVERY_SOURCE_HELIUS
                            )
                            await asyncio.to_thread(
                                state_store.set_global_metrics,
                                {
                                    "wallet_ws_state": "SUBSCRIBED",
                                    "wallet_ws_subscribed_at": time.time(),
                                    "wallet_ws_mode": "ENHANCED",
                                    "wallet_ws_active_source": DISCOVERY_SOURCE_HELIUS,
                                    "wallet_ws_last_success_at": time.time(),
                                    "wallet_ws_consecutive_failures": 0,
                                    "wallet_ws_state_changed_at": time.time(),
                                },
                            )
                        logger.info("subscribed: %s", request_to_dex[request_id])
                        continue

                    params = message.get("params") or {}
                    subscription_id = params.get("subscription")
                    dex_name = subscription_to_dex.get(subscription_id)
                    value = (params.get("result") or {}).get("value")
                    if dex_name and isinstance(value, dict):
                        record_wallet_ws_activity(
                            "notification", DISCOVERY_SOURCE_HELIUS
                        )
                        record_wallet_ws_activity(
                            "transaction_parsed", DISCOVERY_SOURCE_HELIUS
                        )
                        print_buys(
                            value,
                            dex_name,
                            set(wallets),
                            discovery_source=DISCOVERY_SOURCE_HELIUS,
                        )
        finally:
            ping_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ping_task


async def run_forever(settings: MonitorSettings) -> None:
    global _active_signature_window_size
    delay = 3
    route = WalletWsRouteState()
    first_wallet_load = True
    while True:
        _active_signature_window_size = 0
        connected_at = time.monotonic()
        cancelled_task_count = 0
        try:
            with phase_memory(
                "wallet_reload",
                metadata={
                    "workload": "websocket",
                    "operation": "read",
                    "cold_start": first_wallet_load,
                },
                include_gc_counts=True,
                include_object_count=True,
            ) as wallet_scope:
                wallets = load_wallets(settings.wallets_path)
                wallet_stat = settings.wallets_path.stat()
                mtime_ns = wallet_stat.st_mtime_ns
                wallet_scope.add_metadata(
                    wallet_count=len(wallets),
                    file_bytes=wallet_stat.st_size,
                )
            first_wallet_load = False
            logger.info("loaded %s wallets from %s", len(wallets), settings.wallets_path)
            await asyncio.to_thread(
                state_store.set_global_metrics,
                {
                    "wallet_ws_state": "CONNECTING",
                    "wallet_ws_state_changed_at": time.time(),
                    "wallet_ws_endpoint_kind": (
                        "SOLANA_PUBLIC_WSS"
                        if route.uses_standard else "HELIUS_WSS"
                    ),
                    "wallet_ws_subscription_method": (
                        "logsSubscribe"
                        if route.uses_standard else "transactionSubscribe"
                    ),
                    "wallet_ws_active_source": (
                        DISCOVERY_SOURCE_SOLANA
                        if route.uses_standard else DISCOVERY_SOURCE_HELIUS
                    ),
                },
            )
            monitor_task = asyncio.create_task(
                monitor_standard_once(settings, wallets)
                if route.uses_standard else monitor_once(settings, wallets)
            )
            watcher_task = asyncio.create_task(
                watch_wallet_file(
                    settings.wallets_path, mtime_ns, settings.wallet_reload_seconds
                )
            )
            heartbeat_task = asyncio.create_task(monitor_heartbeat(len(wallets)))
            refresh_task = asyncio.create_task(subscription_refresh_timer())
            done, pending = await asyncio.wait(
                {monitor_task, watcher_task, heartbeat_task, refresh_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            with phase_memory(
                "ws_refresh_reconnect",
                metadata={
                    "workload": "websocket",
                    "operation": "update",
                    "mode": "standard" if route.uses_standard else "enhanced",
                    "kind": "steady",
                    "result": "unknown",
                },
                include_gc_counts=True,
                include_object_count=True,
            ) as reconnect_scope:
                for task in pending:
                    task.cancel()
                cancelled_task_count = len(pending)
                await asyncio.gather(*pending, return_exceptions=True)
                reconnect_scope.add_metadata(
                    task_count=len(done) + len(pending),
                    cancelled_task_count=cancelled_task_count,
                )
            results = await asyncio.gather(*done, return_exceptions=True)
            for result in results:
                if isinstance(result, WalletListChanged):
                    raise result
            for result in results:
                if isinstance(result, BaseException):
                    raise result
            raise ConnectionError("WebSocket stream ended")
        except WalletListChanged:
            with phase_memory(
                "ws_refresh_reconnect",
                metadata={
                    "workload": "websocket",
                    "operation": "update",
                    "mode": "standard" if route.uses_standard else "enhanced",
                    "kind": "steady",
                    "result": "success",
                    "cancelled_task_count": cancelled_task_count,
                },
            ):
                logger.info(
                    "wallet list updated; reloading file and rebuilding RPC subscriptions"
                )
            delay = 3
            continue
        except SubscriptionRefresh:
            with phase_memory(
                "ws_refresh_reconnect",
                metadata={
                    "workload": "websocket",
                    "operation": "update",
                    "mode": "standard" if route.uses_standard else "enhanced",
                    "kind": "steady",
                    "result": "success",
                    "cancelled_task_count": cancelled_task_count,
                },
            ):
                logger.info(
                    "wallet WebSocket refresh interval reached; rebuilding subscriptions"
                )
                await asyncio.to_thread(
                    state_store.set_global_metrics,
                    {
                        "wallet_ws_state": "REFRESHING",
                        "wallet_ws_state_changed_at": time.time(),
                    },
                )
                route.refresh(time.monotonic())
            delay = 3
            continue
        except EnhancedSubscriptionUnavailable:
            with phase_memory(
                "ws_refresh_reconnect",
                metadata={
                    "workload": "websocket",
                    "operation": "update",
                    "mode": "enhanced",
                    "result": "failure",
                    "cancelled_task_count": cancelled_task_count,
                },
            ):
                record_wallet_ws_activity(
                    "subscription_failure", DISCOVERY_SOURCE_HELIUS
                )
                logger.warning(
                    "enhanced transactionSubscribe unavailable; switching to wallet-filtered logsSubscribe"
                )
                route.activate_standard(time.monotonic())
                await asyncio.to_thread(
                    state_store.set_global_metrics,
                    {
                        "wallet_ws_enhanced_fallback_reason": (
                            "WS_ENHANCED_NOT_AVAILABLE"
                        ),
                        "wallet_ws_enhanced_fallback_at": time.time(),
                    },
                )
            delay = 3
            continue
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            with phase_memory(
                "ws_refresh_reconnect",
                metadata={
                    "workload": "websocket",
                    "operation": "update",
                    "mode": "standard" if route.uses_standard else "enhanced",
                    "result": "failure",
                    "cancelled_task_count": cancelled_task_count,
                },
            ):
                category = canonical_websocket_failure_reason(exc)
                failed_mode = route.mode
                if isinstance(exc, WebSocketSubscriptionRejected):
                    record_wallet_ws_activity(
                        "subscription_failure",
                        DISCOVERY_SOURCE_SOLANA
                        if route.uses_standard else DISCOVERY_SOURCE_HELIUS,
                    )
                route.record_failure(time.monotonic())
                await asyncio.to_thread(
                    record_wallet_ws_failure,
                    category,
                )
                if (
                    failed_mode == "HELIUS_ENHANCED"
                    and route.mode == "SOLANA_STANDARD"
                ):
                    await asyncio.to_thread(
                        state_store.set_global_metrics,
                        {
                            "wallet_ws_enhanced_fallback_reason": category,
                            "wallet_ws_enhanced_fallback_at": time.time(),
                        },
                    )
                if time.monotonic() - connected_at >= 60:
                    delay = 3
                logger.exception(
                    "connection lost; reconnecting in %s seconds category=%s "
                    "failed_mode=%s next_mode=%s",
                    delay,
                    category,
                    failed_mode,
                    route.mode,
                )
            await asyncio.sleep(delay)
            delay = min(delay * 2, 300)


async def run_service() -> None:
    from src.observation_tracker import (
        approved_signal_paper_mode_enabled,
        observation_mode_enabled,
        observation_supervisor,
    )
    from src.wallet_performance import performance_loop

    settings = MonitorSettings.from_env()
    observation_mode = observation_mode_enabled()
    approved_paper_mode = (
        approved_signal_paper_mode_enabled() if observation_mode else False
    )
    paper_entries_enabled = not observation_mode or approved_paper_mode
    started_at = time.time()
    reset_wallet_ws_activity(now_epoch=started_at)
    await asyncio.to_thread(
        state_store.set_global_metrics,
        {
            "monitor_started_at": started_at,
            "monitor_process_heartbeat_at": started_at,
            "wallet_ws_state": "STARTING",
            "wallet_ws_state_changed_at": started_at,
            **wallet_ws_activity_metrics(),
        },
    )
    logger.info(
        "monitor startup: observation_mode=%s approved_signal_paper_mode=%s "
        "paper_entries=%s",
        observation_mode,
        approved_paper_mode,
        paper_entries_enabled,
    )
    memory_sampler = start_memory_attribution_sampler()
    try:
        await asyncio.gather(
            run_forever(settings),
            performance_loop(),
            run_market_momentum_route(settings),
            monitor_maintenance_loop(),
            observation_supervisor() if observation_mode else asyncio.Event().wait(),
        )
    finally:
        if memory_sampler is not None:
            memory_sampler.stop()


def main() -> None:
    configure_safe_logging()
    try:
        asyncio.run(run_service())
    finally:
        flush_phase_memory_telemetry()


if __name__ == "__main__":
    main()
