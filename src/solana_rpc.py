"""무료 provider를 안전하게 분산 사용하는 표준 Solana JSON-RPC router."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import secrets
import threading
import time
import weakref
from collections import OrderedDict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import aiohttp

from src.phase_memory_telemetry import (
    add_current_phase_metadata,
    add_ancestor_phase_metadata,
    current_phase_context_contains,
    phase_memory,
)
from dotenv import load_dotenv

from src.helius_rpc import (
    HELIUS_RATE_LIMIT_FAILURE_REASONS,
    helius_backoff_seconds,
    jittered_backoff_seconds,
)
from src.research.coverage_telemetry import (
    RPC_PACING_BURST_BUCKETS,
    RPC_PACING_INTERVAL_BUCKETS,
    record_rpc_method_metric,
)
from src.state_store import atomic_write_json, exclusive_file_lock, read_json

logger = logging.getLogger("solana-rpc")

_reservation_lock_guard = threading.Lock()
_reservation_locks: weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop, dict[str, asyncio.Lock]
] = weakref.WeakKeyDictionary()

RPC_PROVIDER_STATE_DIR = (
    Path(__file__).resolve().parents[1] / "data" / "solana_rpc_providers"
)
RPC_PROVIDER_STATE_SCHEMA_VERSION = 6
SOLANA_PUBLIC_DEFAULT_URL = "https://api.mainnet.solana.com"
RPC_CIRCUIT_FAILURE_THRESHOLD = 3
RPC_CIRCUIT_COOLDOWN_SECONDS = 60.0
RPC_HALF_OPEN_LEASE_SECONDS = 20.0
RPC_PROVIDER_LOCAL_ATTEMPTS = 2
RPC_OVERALL_ATTEMPT_BUDGET = 6
RPC_MAX_INLINE_BACKOFF_SECONDS = 5.0
RPC_RETRYABLE_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})
RPC_RATE_LIMIT_ERROR_CODES = frozenset({
    429,
    -32005,
    -32067,
    -32068,
    -32069,
    -32070,
    -32077,
    -32090,
})
RPC_TRANSIENT_ERROR_CODES = frozenset({
    -32055,
    -32056,
    -32057,
    -32059,
    -32061,
    -32063,
    -32064,
    -32071,
    -32076,
})
HEAVY_RPC_METHODS = frozenset({"getTransaction", "getSignaturesForAddress"})
METHOD_SCOPED_AVAILABILITY_METHODS = HEAVY_RPC_METHODS
HEAVY_WORKLOADS = frozenset({"transaction_history", "wallet_feeder"})
LIGHT_PROVIDER_ORDER = (
    "alchemy",
    "chainstack",
    "ankr",
    "helius",
    "solana_public",
)
HEAVY_PROVIDER_ORDER = (
    "ankr",
    "chainstack",
    "alchemy",
    "helius",
    "solana_public",
)
PROVIDER_UNSUPPORTED_METHODS = {
    # Chainstack Developer의 Solana getTokenAccountsByOwner는 유료 전용이다.
    "chainstack": frozenset({"getTokenAccountsByOwner"}),
}
PROVIDER_ENVIRONMENTS = {
    "alchemy": ("ALCHEMY_SOLANA_RPC_URL", "ALCHEMY_RPC_MAX_RPS", 3.0),
    "chainstack": (
        "CHAINSTACK_SOLANA_RPC_URL",
        "CHAINSTACK_RPC_MAX_RPS",
        3.0,
    ),
    "ankr": ("ANKR_SOLANA_RPC_URL", "ANKR_RPC_MAX_RPS", 5.0),
    "helius": ("HELIUS_RPC_HTTP_URL", "HELIUS_RPC_MAX_RPS", 8.0),
    "solana_public": (
        "SOLANA_PUBLIC_RPC_URL",
        "SOLANA_PUBLIC_RPC_MAX_RPS",
        2.0,
    ),
}
METHOD_RATE_LIMIT_REASONS = {
    "getAccountInfo": "RPC_GET_ACCOUNT_INFO_RATE_LIMIT_EXHAUSTED",
    "getTokenSupply": "RPC_GET_TOKEN_SUPPLY_RATE_LIMIT_EXHAUSTED",
    "getBalance": "RPC_GET_BALANCE_RATE_LIMIT_EXHAUSTED",
    "getTokenAccountsByOwner": (
        "RPC_GET_TOKEN_ACCOUNTS_BY_OWNER_RATE_LIMIT_EXHAUSTED"
    ),
    "getTransaction": "RPC_GET_TRANSACTION_RATE_LIMIT_EXHAUSTED",
    "getSignaturesForAddress": (
        "RPC_GET_SIGNATURES_FOR_ADDRESS_RATE_LIMIT_EXHAUSTED"
    ),
    "getHealth": "RPC_RATE_LIMIT_EXHAUSTED",
}
RPC_ALL_PROVIDERS_EXHAUSTED = "RPC_ALL_PROVIDERS_EXHAUSTED"
RPC_NO_PROVIDER_CONFIGURED = "RPC_NO_PROVIDER_CONFIGURED"
RPC_FAILURE_REASONS = frozenset({
    *HELIUS_RATE_LIMIT_FAILURE_REASONS,
    *METHOD_RATE_LIMIT_REASONS.values(),
    RPC_ALL_PROVIDERS_EXHAUSTED,
    RPC_NO_PROVIDER_CONFIGURED,
})
RPC_METHOD_METRIC_NAMES = frozenset({
    *METHOD_RATE_LIMIT_REASONS,
    "getProgramAccounts",
    "getMultipleAccounts",
    "getBlock",
    "other",
})
RPC_LATENCY_BUCKET_LIMITS_MS = (250.0, 1_000.0, 5_000.0)
RPC_SEMANTIC_REPETITION_TTL_SECONDS = 15 * 60.0
RPC_SEMANTIC_REPETITION_MAX_ENTRIES = 256
RPC_PACING_TARGET_PROVIDER = "solana_public"
RPC_PACING_TARGET_METHOD = "getTransaction"
RPC_PACING_HISTORY_SECONDS = 10.0
RPC_PACING_HISTORY_LIMIT = 256
RPC_METHOD_PACING_INTERVAL_SECONDS = {
    ("solana_public", "getTransaction"): 2.0,
}

_semantic_repetition_lock = threading.Lock()
_semantic_repetition_key = secrets.token_bytes(32)
_semantic_repetition_seen: OrderedDict[bytes, float] = OrderedDict()
_pacing_attribution_lock = threading.Lock()
_pacing_request_times: deque[float] = deque(maxlen=RPC_PACING_HISTORY_LIMIT)


@dataclass(frozen=True, slots=True)
class RpcProvider:
    name: str
    url: str
    max_rps: float
    public_fallback: bool = False

    @property
    def minimum_interval_seconds(self) -> float:
        return 1.0 / self.max_rps


@dataclass(frozen=True, slots=True)
class ProviderReservation:
    half_open_probe: bool
    method_half_open_probe: bool = False
    pacing_request_epoch: float | None = None
    pacing_interval_bucket: str | None = None
    pacing_burst_buckets: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class ProviderFailure:
    provider: str
    transient: bool
    rate_limited: bool
    retry_delay_seconds: float
    retry_source: str
    category: str


class SolanaRpcError(RuntimeError):
    """표준 RPC router가 안전하게 완료하지 못한 오류다."""


class SolanaRpcConfigurationError(SolanaRpcError):
    """활성화된 표준 RPC provider가 없는 구성 오류다."""

    def __init__(self) -> None:
        self.canonical_reason = RPC_NO_PROVIDER_CONFIGURED
        super().__init__(self.canonical_reason)


class SolanaRpcExhaustedError(SolanaRpcError):
    """전체 provider/attempt budget 소진 오류다."""

    def __init__(
        self,
        method: str,
        attempts: int,
        *,
        last_provider: str | None = None,
        last_category: str | None = None,
    ) -> None:
        self.method = str(method)
        self.attempts = int(attempts)
        self.last_provider = str(last_provider or "") or None
        self.last_category = str(last_category or "") or None
        self.canonical_reason = RPC_ALL_PROVIDERS_EXHAUSTED
        super().__init__(self.canonical_reason)


class SolanaRpcRateLimitExhaustedError(SolanaRpcExhaustedError):
    """시도한 모든 provider가 rate limit으로 소진된 오류다."""

    def __init__(
        self,
        method: str,
        attempts: int,
        *,
        last_provider: str | None = None,
        last_category: str | None = None,
    ) -> None:
        self.method = str(method)
        self.attempts = int(attempts)
        self.last_provider = str(last_provider or "") or None
        self.last_category = str(last_category or "") or None
        self.canonical_reason = METHOD_RATE_LIMIT_REASONS.get(
            self.method,
            "RPC_RATE_LIMIT_EXHAUSTED",
        )
        SolanaRpcError.__init__(self, self.canonical_reason)


class _ProviderRequestError(Exception):
    def __init__(
        self,
        *,
        transient: bool,
        rate_limited: bool,
        status: int | None,
        headers: Any = None,
        category: str,
    ) -> None:
        self.transient = bool(transient)
        self.rate_limited = bool(rate_limited)
        self.status = status
        self.headers = headers or {}
        self.category = str(category)
        super().__init__(self.category)


def canonical_rpc_failure_reason(error: BaseException) -> str | None:
    """원장/분석에서 사용할 bounded RPC failure category를 반환한다."""
    reason = str(getattr(error, "canonical_reason", "") or "")
    return reason if reason in RPC_FAILURE_REASONS else None


def _positive_float(value: Any, *, setting: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RuntimeError(f"{setting} must be a positive number") from exc
    if not math.isfinite(number) or number <= 0 or number > 1_000:
        raise RuntimeError(f"{setting} must be between 0 and 1000")
    return number


def _positive_int(value: Any, *, setting: str, maximum: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RuntimeError(f"{setting} must be a positive integer") from exc
    if number <= 0 or number > maximum:
        raise RuntimeError(f"{setting} must be between 1 and {maximum}")
    return number


def _resolved_endpoint(name: str, raw_url: str, environ: Mapping[str, str]) -> str:
    url = str(raw_url or "").strip()
    if name == "helius":
        helius_key = str(environ.get("HELIUS_API_KEY", "")).strip()
        if "${HELIUS_API_KEY}" in url and not helius_key:
            return ""
        url = url.replace(
            "${HELIUS_API_KEY}",
            helius_key,
        )
    if not url or "${" in url or not url.startswith("https://"):
        return ""
    return url


def provider_configs_from_env(
    environ: Mapping[str, str] | None = None,
) -> tuple[RpcProvider, ...]:
    """endpoint가 있는 provider만 secret을 노출하지 않고 활성화한다."""
    if environ is None:
        load_dotenv()
        environ = os.environ
    providers: list[RpcProvider] = []
    for name, (url_setting, rps_setting, default_rps) in (
        PROVIDER_ENVIRONMENTS.items()
    ):
        if name == "solana_public":
            raw_url = (
                environ[url_setting]
                if url_setting in environ
                else SOLANA_PUBLIC_DEFAULT_URL
            )
        else:
            raw_url = environ.get(url_setting, "")
        url = _resolved_endpoint(name, str(raw_url or ""), environ)
        if not url:
            continue
        max_rps = _positive_float(
            environ.get(rps_setting, default_rps),
            setting=rps_setting,
        )
        providers.append(RpcProvider(
            name=name,
            url=url,
            max_rps=max_rps,
            public_fallback=name == "solana_public",
        ))
    return tuple(providers)


def ordered_providers(
    providers: Sequence[RpcProvider],
    method: str,
    workload: str,
) -> tuple[RpcProvider, ...]:
    """method/workload별 고정 우선순위를 반환하며 public은 항상 마지막이다."""
    heavy = method in HEAVY_RPC_METHODS or workload in HEAVY_WORKLOADS
    order = HEAVY_PROVIDER_ORDER if heavy else LIGHT_PROVIDER_ORDER
    indexed = {provider.name: provider for provider in providers}
    unsupported = PROVIDER_UNSUPPORTED_METHODS
    selected = [
        indexed[name]
        for name in order
        if name in indexed
        and method not in unsupported.get(name, frozenset())
    ]
    return tuple(selected)


def _state_path(provider_name: str) -> Path:
    if provider_name not in PROVIDER_ENVIRONMENTS:
        raise ValueError("unsupported RPC provider name")
    return RPC_PROVIDER_STATE_DIR / f"{provider_name}.json"


def _empty_availability_state() -> dict[str, Any]:
    return {
        "consecutive_failures": 0,
        "last_success_at_epoch": None,
        "last_failure_at_epoch": None,
        "last_failure_category": None,
        "last_failure_method": None,
        "last_rate_limit_at_epoch": None,
        "last_rate_limit_method": None,
        "last_circuit_open_at_epoch": None,
        "last_circuit_open_method": None,
        "cooldown_until_epoch": 0.0,
        "circuit_state": "CLOSED",
        "circuit_open_count": 0,
        "half_open_lease_until_epoch": 0.0,
    }


def _empty_provider_state(provider_name: str) -> dict[str, Any]:
    method_last_request_at_epoch = {
        method: 0.0
        for target_provider, method in RPC_METHOD_PACING_INTERVAL_SECONDS
        if target_provider == provider_name
    }
    return {
        "schema_version": RPC_PROVIDER_STATE_SCHEMA_VERSION,
        "version": 0,
        "provider": provider_name,
        "enabled": True,
        "request_count": 0,
        "success_count": 0,
        "failure_count": 0,
        "rate_limit_count": 0,
        "last_request_at_epoch": 0.0,
        "method_last_request_at_epoch": method_last_request_at_epoch,
        **_empty_availability_state(),
        "method_availability": {
            method: _empty_availability_state()
            for method in sorted(METHOD_SCOPED_AVAILABILITY_METHODS)
        },
        "method_metrics": {},
    }


def _method_metric() -> dict[str, Any]:
    return {
        "request_count": 0,
        "success_count": 0,
        "failure_count": 0,
        "rate_limit_count": 0,
        "exhaustion_count": 0,
        "retry_count": 0,
        "failover_count": 0,
        "latency_sample_count": 0,
        "latency_sum_ms": 0.0,
        "latency_max_ms": 0.0,
        "latency_buckets": {
            "le_250_ms": 0,
            "le_1000_ms": 0,
            "le_5000_ms": 0,
            "gt_5000_ms": 0,
        },
    }


def _metric_method(method: str) -> str:
    normalized = str(method)
    return normalized if normalized in RPC_METHOD_METRIC_NAMES else "other"


def _state_method_metric(
    state: dict[str, Any], method: str,
) -> dict[str, Any]:
    metrics = state.setdefault("method_metrics", {})
    name = _metric_method(method)
    raw = metrics.get(name)
    if not isinstance(raw, dict):
        raw = _method_metric()
        metrics[name] = raw
    defaults = _method_metric()
    for key, value in defaults.items():
        raw.setdefault(key, value)
    buckets = raw.get("latency_buckets")
    if not isinstance(buckets, dict):
        buckets = {}
        raw["latency_buckets"] = buckets
    for key, value in defaults["latency_buckets"].items():
        buckets.setdefault(key, value)
    return raw


def _record_latency(metric: dict[str, Any], latency_ms: float) -> None:
    value = max(0.0, float(latency_ms))
    metric["latency_sample_count"] = (
        int(metric.get("latency_sample_count", 0) or 0) + 1
    )
    metric["latency_sum_ms"] = round(
        float(metric.get("latency_sum_ms", 0.0) or 0.0) + value,
        3,
    )
    metric["latency_max_ms"] = round(max(
        float(metric.get("latency_max_ms", 0.0) or 0.0), value
    ), 3)
    if value <= RPC_LATENCY_BUCKET_LIMITS_MS[0]:
        bucket = "le_250_ms"
    elif value <= RPC_LATENCY_BUCKET_LIMITS_MS[1]:
        bucket = "le_1000_ms"
    elif value <= RPC_LATENCY_BUCKET_LIMITS_MS[2]:
        bucket = "le_5000_ms"
    else:
        bucket = "gt_5000_ms"
    buckets = metric.setdefault("latency_buckets", {})
    buckets[bucket] = int(buckets.get(bucket, 0) or 0) + 1


def _migrate_provider_state(
    provider_name: str,
    state: dict[str, Any],
) -> dict[str, Any]:
    """기존 provider state에 새 bounded metric 기본값을 보완한다."""
    previous_schema = int(state.get("schema_version", 0) or 0)
    defaults = _empty_provider_state(provider_name)
    for key, value in defaults.items():
        state.setdefault(key, value)
    state["provider"] = provider_name
    raw_availability = state.get("method_availability")
    if not isinstance(raw_availability, dict):
        raw_availability = {}
    bounded_availability: dict[str, dict[str, Any]] = {}
    for method in sorted(METHOD_SCOPED_AVAILABILITY_METHODS):
        raw = raw_availability.get(method)
        source = raw if isinstance(raw, dict) else {}
        scoped = {
            key: source.get(key, value)
            for key, value in _empty_availability_state().items()
        }
        bounded_availability[method] = scoped
    state["method_availability"] = bounded_availability

    raw_method_pacing = state.get("method_last_request_at_epoch")
    raw_method_pacing = (
        raw_method_pacing if isinstance(raw_method_pacing, dict) else {}
    )
    state["method_last_request_at_epoch"] = {
        method: _safe_epoch(raw_method_pacing.get(method))
        for target_provider, method in RPC_METHOD_PACING_INTERVAL_SECONDS
        if target_provider == provider_name
    }

    if previous_schema < 5:
        circuit = str(state.get("circuit_state") or "CLOSED").upper()
        trigger = (
            state.get("last_circuit_open_method")
            if circuit in {"OPEN", "HALF_OPEN"}
            else state.get("last_failure_method")
        )
        if (
            trigger in METHOD_SCOPED_AVAILABILITY_METHODS
            and state.get("last_failure_category") in {
                "RATE_LIMIT",
                "RPC_TRANSIENT",
            }
            and (
                circuit in {"OPEN", "HALF_OPEN"}
                or _safe_epoch(state.get("cooldown_until_epoch")) > 0
            )
        ):
            scoped = bounded_availability[str(trigger)]
            for key in _empty_availability_state():
                scoped[key] = state.get(key, scoped[key])
            state["consecutive_failures"] = 0
            state["cooldown_until_epoch"] = 0.0
            state["circuit_state"] = "CLOSED"
            state["half_open_lease_until_epoch"] = 0.0

    state["schema_version"] = RPC_PROVIDER_STATE_SCHEMA_VERSION
    metrics = state.get("method_metrics")
    if not isinstance(metrics, dict):
        state["method_metrics"] = {}
    else:
        for method in list(metrics):
            if method not in RPC_METHOD_METRIC_NAMES:
                metrics.pop(method, None)
                continue
            _state_method_metric(state, method)
    return state


def _method_availability_state(
    state: dict[str, Any], method: str,
) -> dict[str, Any] | None:
    if method not in METHOD_SCOPED_AVAILABILITY_METHODS:
        return None
    availability = state["method_availability"]
    return availability[method]


def _failure_uses_method_availability(
    method: str,
    failure: ProviderFailure,
) -> bool:
    return (
        method in METHOD_SCOPED_AVAILABILITY_METHODS
        and (failure.rate_limited or failure.category == "RPC_TRANSIENT")
    )


def _safe_epoch(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return number if math.isfinite(number) and number >= 0 else 0.0


def _increment_state_version(state: dict[str, Any]) -> None:
    state["version"] = int(state.get("version", 0) or 0) + 1


def _record_coverage_rpc_metric(**values: Any) -> None:
    """진단 계측 실패가 RPC 결과를 바꾸지 않도록 격리한다."""
    try:
        record_rpc_method_metric(**values)
    except Exception:
        logger.exception("research RPC method telemetry record failed")


def _semantic_request_digest(method: str, params: list[Any]) -> bytes:
    """원문을 보존하지 않는 process-local semantic request key를 만든다."""
    canonical = json.dumps(
        [str(method), params],
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hmac.new(
        _semantic_repetition_key,
        canonical,
        hashlib.sha256,
    ).digest()


def _record_semantic_repetition(
    method: str,
    params: list[Any],
    *,
    now_monotonic: float | None = None,
) -> None:
    """getSignatures 반복을 bounded process-local history로만 집계한다."""
    if str(method) != "getSignaturesForAddress":
        return
    try:
        now = time.monotonic() if now_monotonic is None else float(now_monotonic)
        digest = _semantic_request_digest(str(method), params)
        expired_or_evicted = 0
        repeated_1m = repeated_5m = repeated_15m = 0
        with _semantic_repetition_lock:
            cutoff = now - RPC_SEMANTIC_REPETITION_TTL_SECONDS
            while _semantic_repetition_seen:
                _, oldest = next(iter(_semantic_repetition_seen.items()))
                if oldest >= cutoff:
                    break
                _semantic_repetition_seen.popitem(last=False)
                expired_or_evicted += 1
            previous = _semantic_repetition_seen.pop(digest, None)
            if previous is not None and now >= previous:
                age = now - previous
                repeated_1m = int(age <= 60.0)
                repeated_5m = int(age <= 5 * 60.0)
                repeated_15m = int(age <= 15 * 60.0)
            _semantic_repetition_seen[digest] = now
            while (
                len(_semantic_repetition_seen)
                > RPC_SEMANTIC_REPETITION_MAX_ENTRIES
            ):
                _semantic_repetition_seen.popitem(last=False)
                expired_or_evicted += 1
        _record_coverage_rpc_metric(
            provider="router",
            method=str(method),
            semantic_request_count=1,
            semantic_repeated_within_1m_count=repeated_1m,
            semantic_repeated_within_5m_count=repeated_5m,
            semantic_repeated_within_15m_count=repeated_15m,
            semantic_tracker_eviction_count=expired_or_evicted,
        )
    except Exception:
        logger.exception("RPC semantic repetition telemetry failed")


def safe_rpc_runtime_config(
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """URL과 key 없이 effective RPC router 설정만 반환한다."""
    source = os.environ if environ is None else environ
    configured = provider_configs_from_env(environ)
    indexed = {provider.name: provider for provider in configured}
    local_attempts = _positive_int(
        source.get("SOLANA_RPC_PROVIDER_ATTEMPTS", RPC_PROVIDER_LOCAL_ATTEMPTS),
        setting="SOLANA_RPC_PROVIDER_ATTEMPTS",
        maximum=3,
    )
    overall_attempts = _positive_int(
        source.get("SOLANA_RPC_OVERALL_ATTEMPT_BUDGET", RPC_OVERALL_ATTEMPT_BUDGET),
        setting="SOLANA_RPC_OVERALL_ATTEMPT_BUDGET",
        maximum=20,
    )
    return {
        "providers": [
            {
                "name": name,
                "enabled": name in indexed,
                "max_rps": indexed[name].max_rps if name in indexed else None,
            }
            for name in PROVIDER_ENVIRONMENTS
        ],
        "provider_local_attempts": local_attempts,
        "overall_attempt_budget": overall_attempts,
        "heavy_provider_order": [
            provider.name for provider in ordered_providers(
                configured,
                "getSignaturesForAddress",
                "transaction_history",
            )
        ],
    }


def _record_reservation_skip(
    provider: RpcProvider,
    method: str,
    reason: str,
    state: Mapping[str, Any],
    skip_reasons: dict[str, int] | None,
) -> None:
    if skip_reasons is not None:
        skip_reasons[reason] = int(skip_reasons.get(reason, 0) or 0) + 1
    if reason in {"circuit_open_cooldown", "half_open_lease"}:
        trigger = (
            state.get("last_circuit_open_method")
            or state.get("last_failure_method")
        )
    else:
        trigger = state.get("last_failure_method")
    _record_coverage_rpc_metric(
        provider=provider.name,
        method=method,
        reservation_skip_count=1,
        reservation_skip_reason=reason,
        reservation_skip_trigger_method=str(trigger or "unknown"),
    )


def _availability_block_reason(
    availability: Mapping[str, Any], now: float,
) -> str | None:
    circuit = str(availability.get("circuit_state") or "CLOSED").upper()
    cooldown_until = _safe_epoch(availability.get("cooldown_until_epoch"))
    lease_until = _safe_epoch(
        availability.get("half_open_lease_until_epoch")
    )
    if circuit == "OPEN" and cooldown_until > now:
        return "circuit_open_cooldown"
    if circuit == "HALF_OPEN" and lease_until > now:
        return "half_open_lease"
    if circuit == "CLOSED" and cooldown_until > now:
        return "cooldown"
    return None


def _activate_half_open_probe(
    availability: dict[str, Any], now: float,
) -> bool:
    circuit = str(availability.get("circuit_state") or "CLOSED").upper()
    if circuit not in {"OPEN", "HALF_OPEN"}:
        return False
    availability["circuit_state"] = "HALF_OPEN"
    availability["half_open_lease_until_epoch"] = (
        now + RPC_HALF_OPEN_LEASE_SECONDS
    )
    return True


def _pacing_interval_bucket(interval_seconds: float | None) -> str:
    if interval_seconds is None or interval_seconds < 0:
        return "no_previous"
    interval_ms = interval_seconds * 1_000.0
    if interval_ms < 100:
        return "lt_100_ms"
    if interval_ms < 250:
        return "100_249_ms"
    if interval_ms < 500:
        return "250_499_ms"
    if interval_ms < 1_000:
        return "500_999_ms"
    if interval_ms <= 2_000:
        return "1_2_s"
    return "gt_2_s"


def _pacing_burst_bucket(request_count: int) -> str:
    count = max(1, int(request_count))
    if count == 1:
        return "1"
    if count == 2:
        return "2"
    if count <= 4:
        return "3_4"
    if count <= 8:
        return "5_8"
    if count <= 16:
        return "9_16"
    return "gt_16"


def _physical_pacing_attribution(
    provider: RpcProvider,
    method: str,
    *,
    now_epoch: float,
) -> tuple[str | None, tuple[tuple[str, str], ...]]:
    """Public getTransaction 요청 밀도를 bounded process-local 상태로 계산한다."""
    if (
        provider.name != RPC_PACING_TARGET_PROVIDER
        or str(method) != RPC_PACING_TARGET_METHOD
    ):
        return None, ()
    now = float(now_epoch)
    with _pacing_attribution_lock:
        if _pacing_request_times and now < _pacing_request_times[-1]:
            _pacing_request_times.clear()
        previous = _pacing_request_times[-1] if _pacing_request_times else None
        cutoff = now - RPC_PACING_HISTORY_SECONDS
        while _pacing_request_times and _pacing_request_times[0] < cutoff:
            _pacing_request_times.popleft()
        _pacing_request_times.append(now)
        burst_buckets = tuple(
            (
                f"{int(window)}s",
                _pacing_burst_bucket(sum(
                    1 for timestamp in _pacing_request_times
                    if timestamp >= now - window
                )),
            )
            for window in (1.0, 5.0, 10.0)
        )
    interval = None if previous is None else now - previous
    interval_bucket = _pacing_interval_bucket(interval)
    if interval_bucket not in RPC_PACING_INTERVAL_BUCKETS:
        return None, ()
    if any(bucket not in RPC_PACING_BURST_BUCKETS for _, bucket in burst_buckets):
        return None, ()
    return interval_bucket, burst_buckets


def _reset_pacing_attribution() -> None:
    with _pacing_attribution_lock:
        _pacing_request_times.clear()


def _method_pacing_interval_seconds(
    provider: RpcProvider, method: str,
) -> float | None:
    return RPC_METHOD_PACING_INTERVAL_SECONDS.get((provider.name, str(method)))


def _method_pacing_lock_path(provider: RpcProvider, method: str) -> Path:
    if _method_pacing_interval_seconds(provider, method) is None:
        raise ValueError("unsupported method pacing target")
    return RPC_PROVIDER_STATE_DIR / f"{provider.name}.{method}.pacing"


def _reserve_provider_slot_core_sync(
    provider: RpcProvider,
    *,
    method: str = "other",
    retry: bool = False,
    failover: bool = False,
    now_epoch: float | None = None,
    skip_reasons: dict[str, int] | None = None,
) -> ProviderReservation | None:
    """전역 pacing과 provider/method availability를 원자적으로 예약한다."""
    path = _state_path(provider.name)
    with exclusive_file_lock(path, timeout_seconds=180.0):
        state = _migrate_provider_state(
            provider.name,
            read_json(path, _empty_provider_state(provider.name)),
        )
        now = time.time() if now_epoch is None else float(now_epoch)
        global_block = _availability_block_reason(state, now)
        if global_block is not None:
            _record_reservation_skip(
                provider, method, global_block, state, skip_reasons
            )
            return None
        method_state = _method_availability_state(state, method)
        if method_state is not None:
            method_block = _availability_block_reason(method_state, now)
            if method_block is not None:
                _record_reservation_skip(
                    provider, method, method_block, method_state, skip_reasons
                )
                return None

        half_open_probe = _activate_half_open_probe(state, now)
        method_half_open_probe = (
            _activate_half_open_probe(method_state, now)
            if method_state is not None else False
        )

        last_request = _safe_epoch(state.get("last_request_at_epoch"))
        target = max(now, last_request + provider.minimum_interval_seconds)
        if now_epoch is None and target > now:
            time.sleep(target - now)
            now = time.time()
        else:
            now = target
        pacing_interval_bucket, pacing_burst_buckets = (
            _physical_pacing_attribution(provider, method, now_epoch=now)
        )
        state["schema_version"] = RPC_PROVIDER_STATE_SCHEMA_VERSION
        state["provider"] = provider.name
        state["enabled"] = True
        state["request_count"] = int(state.get("request_count", 0) or 0) + 1
        metric = _state_method_metric(state, method)
        metric["request_count"] = int(metric.get("request_count", 0) or 0) + 1
        if retry:
            metric["retry_count"] = int(metric.get("retry_count", 0) or 0) + 1
        if failover:
            metric["failover_count"] = (
                int(metric.get("failover_count", 0) or 0) + 1
            )
        state["last_request_at_epoch"] = now
        method_interval = _method_pacing_interval_seconds(provider, method)
        if method_interval is not None:
            state["method_last_request_at_epoch"][str(method)] = now
        _increment_state_version(state)
        atomic_write_json(path, state)
        _record_coverage_rpc_metric(
            provider=provider.name,
            method=method,
            request_count=1,
            retry_count=int(retry),
            failover_count=int(failover),
            timestamp=now,
        )
        return ProviderReservation(
            half_open_probe=half_open_probe,
            method_half_open_probe=method_half_open_probe,
            pacing_request_epoch=now if pacing_interval_bucket is not None else None,
            pacing_interval_bucket=pacing_interval_bucket,
            pacing_burst_buckets=pacing_burst_buckets,
        )


def _reserve_method_paced_provider_slot_sync(
    provider: RpcProvider,
    *,
    method: str,
    retry: bool = False,
    failover: bool = False,
    now_epoch: float | None = None,
    skip_reasons: dict[str, int] | None = None,
) -> ProviderReservation | None:
    """대상 method만 직렬화하고 긴 pacing 대기는 provider lock 밖에서 수행한다."""
    interval = _method_pacing_interval_seconds(provider, method)
    if interval is None:
        return _reserve_provider_slot_core_sync(
            provider,
            method=method,
            retry=retry,
            failover=failover,
            now_epoch=now_epoch,
            skip_reasons=skip_reasons,
        )
    with exclusive_file_lock(
        _method_pacing_lock_path(provider, method), timeout_seconds=180.0
    ):
        state_path = _state_path(provider.name)
        with exclusive_file_lock(state_path, timeout_seconds=180.0):
            state = _migrate_provider_state(
                provider.name,
                read_json(state_path, _empty_provider_state(provider.name)),
            )
            now = time.time() if now_epoch is None else float(now_epoch)
            global_block = _availability_block_reason(state, now)
            if global_block is not None:
                _record_reservation_skip(
                    provider, method, global_block, state, skip_reasons
                )
                return None
            method_state = _method_availability_state(state, method)
            if method_state is not None:
                method_block = _availability_block_reason(method_state, now)
                if method_block is not None:
                    _record_reservation_skip(
                        provider, method, method_block, method_state, skip_reasons
                    )
                    return None
            last_method_request = _safe_epoch(
                state["method_last_request_at_epoch"].get(method)
            )
            method_target = max(now, last_method_request + interval)

        if now_epoch is None:
            delay = method_target - time.time()
            if delay > 0:
                time.sleep(delay)
            reservation_now = None
        else:
            reservation_now = method_target
        return _reserve_provider_slot_core_sync(
            provider,
            method=method,
            retry=retry,
            failover=failover,
            now_epoch=reservation_now,
            skip_reasons=skip_reasons,
        )


def _reserve_provider_slot_sync(
    provider: RpcProvider,
    *,
    method: str = "other",
    retry: bool = False,
    failover: bool = False,
    now_epoch: float | None = None,
    skip_reasons: dict[str, int] | None = None,
) -> ProviderReservation | None:
    if _method_pacing_interval_seconds(provider, method) is not None:
        return _reserve_method_paced_provider_slot_sync(
            provider,
            method=method,
            retry=retry,
            failover=failover,
            now_epoch=now_epoch,
            skip_reasons=skip_reasons,
        )
    return _reserve_provider_slot_core_sync(
        provider,
        method=method,
        retry=retry,
        failover=failover,
        now_epoch=now_epoch,
        skip_reasons=skip_reasons,
    )


def _process_reservation_lock(provider_name: str) -> asyncio.Lock:
    """같은 event loop의 provider 예약 순서를 결정적으로 직렬화한다."""
    loop = asyncio.get_running_loop()
    with _reservation_lock_guard:
        locks = _reservation_locks.setdefault(loop, {})
        return locks.setdefault(provider_name, asyncio.Lock())


async def _reserve_provider_slot(
    provider: RpcProvider,
    *,
    method: str = "other",
    retry: bool = False,
    failover: bool = False,
    skip_reasons: dict[str, int] | None = None,
) -> ProviderReservation | None:
    if _method_pacing_interval_seconds(provider, method) is not None:
        return await asyncio.to_thread(
            _reserve_provider_slot_sync,
            provider,
            method=method,
            retry=retry,
            failover=failover,
            skip_reasons=skip_reasons,
        )
    async with _process_reservation_lock(provider.name):
        return await asyncio.to_thread(
            _reserve_provider_slot_sync,
            provider,
            method=method,
            retry=retry,
            failover=failover,
            skip_reasons=skip_reasons,
        )


def _record_pacing_outcome(
    provider: RpcProvider,
    method: str,
    reservation: ProviderReservation,
    outcome: str,
) -> None:
    if (
        reservation.pacing_request_epoch is None
        or reservation.pacing_interval_bucket is None
    ):
        return
    _record_coverage_rpc_metric(
        provider=provider.name,
        method=method,
        pacing_interval_bucket=reservation.pacing_interval_bucket,
        pacing_burst_buckets=dict(reservation.pacing_burst_buckets),
        pacing_outcome=outcome,
        timestamp=reservation.pacing_request_epoch,
    )


def _record_provider_success_sync(
    provider: RpcProvider,
    reservation: ProviderReservation,
    *,
    method: str = "other",
    latency_ms: float = 0.0,
) -> None:
    path = _state_path(provider.name)
    with exclusive_file_lock(path, timeout_seconds=180.0):
        state = _migrate_provider_state(
            provider.name,
            read_json(path, _empty_provider_state(provider.name)),
        )
        state["success_count"] = int(state.get("success_count", 0) or 0) + 1
        metric = _state_method_metric(state, method)
        metric["success_count"] = int(metric.get("success_count", 0) or 0) + 1
        _record_latency(metric, latency_ms)
        now = time.time()
        state["consecutive_failures"] = 0
        state["last_success_at_epoch"] = now
        state["last_failure_category"] = None
        state["cooldown_until_epoch"] = 0.0
        state["circuit_state"] = "CLOSED"
        state["half_open_lease_until_epoch"] = 0.0
        method_state = _method_availability_state(state, method)
        if method_state is not None:
            method_state["consecutive_failures"] = 0
            method_state["last_success_at_epoch"] = now
            method_state["last_failure_category"] = None
            method_state["cooldown_until_epoch"] = 0.0
            method_state["circuit_state"] = "CLOSED"
            method_state["half_open_lease_until_epoch"] = 0.0
        _increment_state_version(state)
        atomic_write_json(path, state)
        _record_coverage_rpc_metric(
            provider=provider.name,
            method=method,
            success_count=1,
            latency_ms=latency_ms,
        )
        _record_pacing_outcome(provider, method, reservation, "success")


def _record_provider_failure_sync(
    provider: RpcProvider,
    reservation: ProviderReservation,
    failure: ProviderFailure,
    *,
    method: str = "other",
    latency_ms: float = 0.0,
) -> None:
    path = _state_path(provider.name)
    with exclusive_file_lock(path, timeout_seconds=180.0):
        state = _migrate_provider_state(
            provider.name,
            read_json(path, _empty_provider_state(provider.name)),
        )
        now = time.time()
        state["failure_count"] = int(state.get("failure_count", 0) or 0) + 1
        metric = _state_method_metric(state, method)
        metric["failure_count"] = int(metric.get("failure_count", 0) or 0) + 1
        _record_latency(metric, latency_ms)
        state["last_failure_at_epoch"] = now
        state["last_failure_category"] = failure.category
        state["last_failure_method"] = _metric_method(method)
        if failure.rate_limited:
            state["rate_limit_count"] = (
                int(state.get("rate_limit_count", 0) or 0) + 1
            )
            metric["rate_limit_count"] = (
                int(metric.get("rate_limit_count", 0) or 0) + 1
            )
            state["last_rate_limit_at_epoch"] = now
            state["last_rate_limit_method"] = _metric_method(method)
        method_scoped = _failure_uses_method_availability(method, failure)
        availability = (
            _method_availability_state(state, method)
            if method_scoped else state
        )
        if availability is None:
            raise RuntimeError("missing method availability state")
        prior_circuit = str(
            availability.get("circuit_state") or "CLOSED"
        ).upper()
        availability["last_failure_at_epoch"] = now
        availability["last_failure_category"] = failure.category
        availability["last_failure_method"] = _metric_method(method)
        if failure.rate_limited:
            availability["last_rate_limit_at_epoch"] = now
            availability["last_rate_limit_method"] = _metric_method(method)
        if failure.transient:
            consecutive = int(
                availability.get("consecutive_failures", 0) or 0
            ) + 1
            availability["consecutive_failures"] = consecutive
            retry_at = now + max(0.0, failure.retry_delay_seconds)
            availability["cooldown_until_epoch"] = max(
                _safe_epoch(availability.get("cooldown_until_epoch")),
                retry_at,
            )
            if (
                (
                    reservation.method_half_open_probe
                    if method_scoped else reservation.half_open_probe
                )
                or consecutive >= RPC_CIRCUIT_FAILURE_THRESHOLD
            ):
                availability["circuit_state"] = "OPEN"
                if prior_circuit != "OPEN":
                    availability["circuit_open_count"] = (
                        int(availability.get("circuit_open_count", 0) or 0) + 1
                    )
                    availability["last_circuit_open_at_epoch"] = now
                    availability["last_circuit_open_method"] = (
                        _metric_method(method)
                    )
                    if method_scoped:
                        state["circuit_open_count"] = (
                            int(state.get("circuit_open_count", 0) or 0) + 1
                        )
                        state["last_circuit_open_at_epoch"] = now
                        state["last_circuit_open_method"] = (
                            _metric_method(method)
                        )
                availability["cooldown_until_epoch"] = max(
                    availability["cooldown_until_epoch"],
                    now + RPC_CIRCUIT_COOLDOWN_SECONDS,
                )
            else:
                availability["circuit_state"] = "CLOSED"
        else:
            availability["consecutive_failures"] = 0
            availability["circuit_state"] = "CLOSED"
            availability["cooldown_until_epoch"] = 0.0
        availability["half_open_lease_until_epoch"] = 0.0
        if method_scoped:
            state["consecutive_failures"] = 0
            state["cooldown_until_epoch"] = 0.0
            state["circuit_state"] = "CLOSED"
            state["half_open_lease_until_epoch"] = 0.0
        elif reservation.method_half_open_probe:
            method_state = _method_availability_state(state, method)
            if method_state is not None:
                method_state["half_open_lease_until_epoch"] = 0.0
        _increment_state_version(state)
        atomic_write_json(path, state)
        _record_coverage_rpc_metric(
            provider=provider.name,
            method=method,
            failure_count=1,
            rate_limit_count=int(failure.rate_limited),
            latency_ms=latency_ms,
            timestamp=now,
        )
        _record_pacing_outcome(
            provider,
            method,
            reservation,
            "rate_limit" if failure.rate_limited else "failure",
        )


def _record_provider_exhaustion_sync(
    provider: RpcProvider,
    *,
    method: str,
) -> None:
    """최종 logical-call exhaustion을 마지막 실제 provider에 귀속한다."""
    path = _state_path(provider.name)
    with exclusive_file_lock(path, timeout_seconds=180.0):
        state = _migrate_provider_state(
            provider.name,
            read_json(path, _empty_provider_state(provider.name)),
        )
        metric = _state_method_metric(state, method)
        metric["exhaustion_count"] = (
            int(metric.get("exhaustion_count", 0) or 0) + 1
        )
        _increment_state_version(state)
        atomic_write_json(path, state)
        _record_coverage_rpc_metric(
            provider=provider.name,
            method=method,
            exhaustion_count=1,
        )


def provider_state(
    provider_name: str,
    providers: Sequence[RpcProvider] | None = None,
) -> dict[str, Any]:
    """endpoint 없이 provider 운영 state만 반환한다."""
    path = _state_path(provider_name)
    with exclusive_file_lock(path):
        state = _migrate_provider_state(
            provider_name,
            read_json(path, _empty_provider_state(provider_name)),
        )
    configured = (
        tuple(providers)
        if providers is not None
        else provider_configs_from_env()
    )
    state["enabled"] = provider_name in {
        provider.name for provider in configured
    }
    completed = (
        int(state.get("success_count", 0) or 0)
        + int(state.get("failure_count", 0) or 0)
    )
    state["success_rate_percent"] = (
        round(int(state.get("success_count", 0) or 0) / completed * 100, 4)
        if completed else None
    )
    return state


def provider_states(
    providers: Sequence[RpcProvider] | None = None,
) -> dict[str, dict[str, Any]]:
    """활성/비활성 provider를 모두 포함한 endpoint-free 상태를 반환한다."""
    configured = (
        tuple(providers)
        if providers is not None
        else provider_configs_from_env()
    )
    return {
        name: provider_state(name, configured)
        for name in PROVIDER_ENVIRONMENTS
    }


def _payload_rate_limited(error: Any) -> bool:
    if not isinstance(error, dict):
        return False
    try:
        code = int(error.get("code"))
    except (TypeError, ValueError, OverflowError):
        code = 0
    message = str(error.get("message") or "").upper()
    return (
        code in RPC_RATE_LIMIT_ERROR_CODES
        or "RATE LIMIT" in message
        or "TOO MANY REQUEST" in message
    )


def _payload_transient(error: Any) -> bool:
    if not isinstance(error, dict):
        return False
    try:
        code = int(error.get("code"))
    except (TypeError, ValueError, OverflowError):
        code = 0
    message = str(error.get("message") or "").upper()
    return (
        code in RPC_TRANSIENT_ERROR_CODES
        or "TEMPORAR" in message
        or "UNAVAILABLE" in message
        or "TIMEOUT" in message
    )


async def _provider_request_once(
    session: aiohttp.ClientSession,
    provider: RpcProvider,
    method: str,
    params: list[Any],
) -> Any:
    request = {
        "jsonrpc": "2.0",
        "id": str(method),
        "method": str(method),
        "params": params,
    }
    try:
        async with session.post(provider.url, json=request) as response:
            status = int(response.status)
            headers = response.headers
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
            if current_phase_context_contains("whale_confirmation"):
                add_ancestor_phase_metadata(
                    "whale_confirmation",
                    response_count=1,
                    response_bytes=(
                        int(content_length) if content_length_known else 0
                    ),
                    missing_length_count=int(not content_length_known),
                    content_length_known=content_length_known,
                )
            if status in RPC_RETRYABLE_HTTP_STATUSES:
                raise _ProviderRequestError(
                    transient=True,
                    rate_limited=status == 429,
                    status=status,
                    headers=headers,
                    category="RATE_LIMIT" if status == 429 else "HTTP_TRANSIENT",
                )
            if status >= 400:
                raise _ProviderRequestError(
                    transient=False,
                    rate_limited=False,
                    status=status,
                    category="HTTP_TERMINAL",
                )
            if (
                method == "getTransaction"
                and current_phase_context_contains("whale_confirmation")
            ):
                with phase_memory(
                    "whale_transaction_parse",
                    metadata={
                        "workload": "transaction",
                        "operation": "parse",
                        "response_count": 1,
                        "response_bytes": (
                            int(content_length) if content_length_known else 0
                        ),
                        "missing_length_count": int(not content_length_known),
                        "content_length_known": content_length_known,
                    },
                ):
                    payload = await response.json()
            else:
                payload = await response.json()
    except _ProviderRequestError:
        raise
    except asyncio.TimeoutError as exc:
        raise _ProviderRequestError(
            transient=True,
            rate_limited=False,
            status=None,
            category="TIMEOUT",
        ) from exc
    except aiohttp.ClientConnectionError as exc:
        raise _ProviderRequestError(
            transient=True,
            rate_limited=False,
            status=None,
            category="CONNECTION",
        ) from exc
    except aiohttp.ClientError as exc:
        raise _ProviderRequestError(
            transient=True,
            rate_limited=False,
            status=None,
            category="TRANSPORT",
        ) from exc
    if not isinstance(payload, dict):
        raise _ProviderRequestError(
            transient=False,
            rate_limited=False,
            status=status,
            category="MALFORMED_RESPONSE",
        )
    error = payload.get("error")
    if _payload_rate_limited(error):
        raise _ProviderRequestError(
            transient=True,
            rate_limited=True,
            status=status,
            headers=headers,
            category="RATE_LIMIT",
        )
    if error:
        transient = _payload_transient(error)
        raise _ProviderRequestError(
            transient=transient,
            rate_limited=False,
            status=status,
            headers=headers,
            category="RPC_TRANSIENT" if transient else "RPC_TERMINAL",
        )
    return payload.get("result")


def _failure_from_error(
    provider: RpcProvider,
    error: _ProviderRequestError,
    attempt_index: int,
) -> ProviderFailure:
    if error.transient:
        base_delay, source = helius_backoff_seconds(
            error.headers,
            attempt_index,
            time.time(),
        )
        delay = jittered_backoff_seconds(base_delay, source)
    else:
        delay, source = 0.0, "terminal"
    return ProviderFailure(
        provider=provider.name,
        transient=error.transient,
        rate_limited=error.rate_limited,
        retry_delay_seconds=delay,
        retry_source=source,
        category=error.category,
    )


async def solana_rpc_call(
    session: aiohttp.ClientSession,
    method: str,
    params: list[Any],
    *,
    workload: str = "default",
    providers: Sequence[RpcProvider] | None = None,
    overall_attempt_budget: int | None = None,
    provider_local_attempts: int | None = None,
) -> Any:
    """활성 provider를 순서대로 시도하고 모두 실패하면 fail-closed한다."""
    _record_semantic_repetition(str(method), params)
    configured = tuple(providers) if providers is not None else provider_configs_from_env()
    ordered = ordered_providers(configured, str(method), str(workload))
    if not ordered:
        raise SolanaRpcConfigurationError()
    total_budget = _positive_int(
        overall_attempt_budget
        if overall_attempt_budget is not None
        else os.getenv("SOLANA_RPC_OVERALL_ATTEMPT_BUDGET", RPC_OVERALL_ATTEMPT_BUDGET),
        setting="SOLANA_RPC_OVERALL_ATTEMPT_BUDGET",
        maximum=20,
    )
    local_budget = _positive_int(
        provider_local_attempts
        if provider_local_attempts is not None
        else os.getenv("SOLANA_RPC_PROVIDER_ATTEMPTS", RPC_PROVIDER_LOCAL_ATTEMPTS),
        setting="SOLANA_RPC_PROVIDER_ATTEMPTS",
        maximum=3,
    )
    failures: list[ProviderFailure] = []
    skip_reasons: dict[str, int] = {}
    attempts_used = 0
    last_attempted_provider: RpcProvider | None = None
    for provider_index, provider in enumerate(ordered):
        for local_attempt in range(local_budget):
            if attempts_used >= total_budget:
                break
            reservation = await _reserve_provider_slot(
                provider,
                method=str(method),
                retry=local_attempt > 0,
                failover=provider_index > 0,
                skip_reasons=skip_reasons,
            )
            if reservation is None:
                break
            attempts_used += 1
            last_attempted_provider = provider
            request_started = time.monotonic()
            try:
                result = await _provider_request_once(
                    session,
                    provider,
                    str(method),
                    params,
                )
            except _ProviderRequestError as exc:
                latency_ms = (time.monotonic() - request_started) * 1_000
                failure = _failure_from_error(provider, exc, local_attempt)
                failures.append(failure)
                await asyncio.to_thread(
                    _record_provider_failure_sync,
                    provider,
                    reservation,
                    failure,
                    method=str(method),
                    latency_ms=latency_ms,
                )
                logger.warning(
                    "Solana RPC provider failure: provider=%s method=%s "
                    "category=%s attempt=%d/%d source=%s",
                    provider.name,
                    method,
                    failure.category,
                    attempts_used,
                    total_budget,
                    failure.retry_source,
                )
                if failure.rate_limited or not failure.transient:
                    break
                if local_attempt + 1 >= local_budget:
                    break
                providers_after_current = len(ordered) - provider_index - 1
                if total_budget - attempts_used <= providers_after_current:
                    break
                if failure.retry_delay_seconds > RPC_MAX_INLINE_BACKOFF_SECONDS:
                    break
                await asyncio.sleep(failure.retry_delay_seconds)
                continue
            await asyncio.to_thread(
                _record_provider_success_sync,
                provider,
                reservation,
                method=str(method),
                latency_ms=(time.monotonic() - request_started) * 1_000,
            )
            return result
        if attempts_used >= total_budget:
            break
    if last_attempted_provider is not None:
        await asyncio.to_thread(
            _record_provider_exhaustion_sync,
            last_attempted_provider,
            method=str(method),
        )
    if attempts_used == 0:
        _record_coverage_rpc_metric(
            provider="router",
            method=str(method),
            zero_attempt_exhaustion_count=1,
            zero_attempt_provider_count=len(ordered),
            zero_attempt_skip_reasons=skip_reasons,
            zero_attempt_workload=str(workload),
        )
    last_provider_name = (
        last_attempted_provider.name
        if last_attempted_provider is not None else None
    )
    last_category = failures[-1].category if failures else None
    if failures and all(failure.rate_limited for failure in failures):
        raise SolanaRpcRateLimitExhaustedError(
            str(method),
            attempts_used,
            last_provider=last_provider_name,
            last_category=last_category,
        )
    raise SolanaRpcExhaustedError(
        str(method),
        attempts_used,
        last_provider=last_provider_name,
        last_category=last_category,
    )
