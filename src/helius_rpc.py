"""PM2 프로세스가 공유하는 bounded Helius JSON-RPC 호출기."""

from __future__ import annotations

import asyncio
import logging
import math
import random
import time
from datetime import timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import aiohttp

from src.logging_utils import redact_sensitive_text
from src.state_store import atomic_write_json, exclusive_file_lock, read_json

logger = logging.getLogger("helius-rpc")

HELIUS_RATE_LIMIT_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "helius_rpc_rate_limit.json"
)
HELIUS_MIN_INTERVAL_SECONDS = 0.125
HELIUS_MAX_ATTEMPTS = 5
HELIUS_MAX_BACKOFF_SECONDS = 30.0
HELIUS_RETRYABLE_STATUSES = frozenset({408, 429, 500, 502, 503, 504})
HELIUS_RATE_LIMIT_FALLBACK = {
    "schema_version": 1,
    "last_request_at_epoch": 0.0,
    "not_before_epoch": 0.0,
    "request_count": 0,
    "rate_limit_count": 0,
    "rate_limit_counts_by_method": {},
}

_RATE_LIMIT_REASONS = {
    "getAccountInfo": "RPC_GET_ACCOUNT_INFO_RATE_LIMIT_EXHAUSTED",
    "getTokenSupply": "RPC_GET_TOKEN_SUPPLY_RATE_LIMIT_EXHAUSTED",
    "getBalance": "RPC_GET_BALANCE_RATE_LIMIT_EXHAUSTED",
    "getTransaction": "RPC_GET_TRANSACTION_RATE_LIMIT_EXHAUSTED",
    "getSignaturesForAddress": (
        "RPC_GET_SIGNATURES_FOR_ADDRESS_RATE_LIMIT_EXHAUSTED"
    ),
}
HELIUS_RATE_LIMIT_FAILURE_REASONS = frozenset({
    *_RATE_LIMIT_REASONS.values(),
    "RPC_RATE_LIMIT_EXHAUSTED",
})


class HeliusRpcError(RuntimeError):
    """재시도 이후에도 완료되지 않은 Helius RPC 오류다."""


class HeliusRpcRateLimitError(HeliusRpcError):
    """안전검사를 fail-closed하는 안정적인 429 소진 오류다."""

    def __init__(self, method: str, attempts: int) -> None:
        self.method = str(method)
        self.attempts = int(attempts)
        self.canonical_reason = _RATE_LIMIT_REASONS.get(
            self.method,
            "RPC_RATE_LIMIT_EXHAUSTED",
        )
        super().__init__(self.canonical_reason)


def canonical_rpc_failure_reason(error: BaseException) -> str | None:
    """원장에 저장 가능한 bounded rate-limit category를 반환한다."""
    reason = getattr(error, "canonical_reason", None)
    return (
        str(reason)
        if reason in HELIUS_RATE_LIMIT_FAILURE_REASONS
        else None
    )


def _finite_nonnegative(value: Any) -> float | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and number >= 0 else None


def _wait_for_helius_slot_sync(
    interval_seconds: float = HELIUS_MIN_INTERVAL_SECONDS,
) -> float:
    """전체 PM2 프로세스에서 Helius 요청 시작 시각을 직렬화한다."""
    interval = max(0.0, float(interval_seconds))
    with exclusive_file_lock(HELIUS_RATE_LIMIT_PATH, timeout_seconds=180.0):
        state = read_json(HELIUS_RATE_LIMIT_PATH, HELIUS_RATE_LIMIT_FALLBACK)
        now = time.time()
        last_request = _finite_nonnegative(state.get("last_request_at_epoch")) or 0.0
        not_before = _finite_nonnegative(state.get("not_before_epoch")) or 0.0
        target = max(now, last_request + interval, not_before)
        delay = max(0.0, target - now)
        if delay:
            time.sleep(delay)
        request_at = time.time()
        state["schema_version"] = 1
        state["last_request_at_epoch"] = request_at
        state["request_count"] = int(state.get("request_count", 0) or 0) + 1
        if not_before <= request_at:
            state["not_before_epoch"] = 0.0
        atomic_write_json(HELIUS_RATE_LIMIT_PATH, state)
        return delay


async def _wait_for_global_helius_slot() -> None:
    await asyncio.to_thread(_wait_for_helius_slot_sync)


def _defer_helius_until_sync(
    not_before_epoch: float,
    *,
    method: str,
    rate_limited: bool,
) -> None:
    """한 프로세스가 본 cooldown을 다른 PM2 프로세스와 공유한다."""
    with exclusive_file_lock(HELIUS_RATE_LIMIT_PATH, timeout_seconds=180.0):
        state = read_json(HELIUS_RATE_LIMIT_PATH, HELIUS_RATE_LIMIT_FALLBACK)
        current = _finite_nonnegative(state.get("not_before_epoch")) or 0.0
        state["schema_version"] = 1
        state["not_before_epoch"] = max(current, float(not_before_epoch))
        if rate_limited:
            state["rate_limit_count"] = (
                int(state.get("rate_limit_count", 0) or 0) + 1
            )
            counts = state.get("rate_limit_counts_by_method")
            if not isinstance(counts, dict):
                counts = {}
                state["rate_limit_counts_by_method"] = counts
            stable_method = method if method in _RATE_LIMIT_REASONS else "OTHER"
            counts[stable_method] = int(counts.get(stable_method, 0) or 0) + 1
            state["last_rate_limit_at_epoch"] = time.time()
            state["last_rate_limit_method"] = stable_method
        atomic_write_json(HELIUS_RATE_LIMIT_PATH, state)


def _header_delay_seconds(
    value: str | None,
    *,
    now_epoch: float,
    relative_only: bool,
) -> float | None:
    if not value:
        return None
    numeric = _finite_nonnegative(value)
    if numeric is not None:
        if not relative_only and numeric > now_epoch:
            return numeric - now_epoch
        return numeric
    if relative_only:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(0.0, parsed.timestamp() - now_epoch)
    return None


def helius_backoff_seconds(
    headers: Any,
    attempt_index: int,
    now_epoch: float,
) -> tuple[float, str]:
    """Retry-After/reset을 우선하고 없으면 bounded exponential을 쓴다."""
    retry_after = _header_delay_seconds(
        headers.get("Retry-After") if headers else None,
        now_epoch=now_epoch,
        relative_only=True,
    )
    if retry_after is not None:
        return min(retry_after, HELIUS_MAX_BACKOFF_SECONDS), "retry-after"
    for header_name in ("x-ratelimit-reset", "ratelimit-reset"):
        reset = _header_delay_seconds(
            headers.get(header_name) if headers else None,
            now_epoch=now_epoch,
            relative_only=False,
        )
        if reset is not None:
            return min(reset, HELIUS_MAX_BACKOFF_SECONDS), "rate-limit-reset"
    return (
        min(float(2**attempt_index), HELIUS_MAX_BACKOFF_SECONDS),
        "exponential-fallback",
    )


def jittered_backoff_seconds(
    delay_seconds: float,
    source: str,
    *,
    random_value: float | None = None,
) -> float:
    """동시 재시도를 흩뜨리되 provider가 지정한 최소 대기는 줄이지 않는다."""
    sample = random.random() if random_value is None else float(random_value)
    sample = min(1.0, max(0.0, sample))
    factor = (
        1.0 + 0.25 * sample
        if source in {"retry-after", "rate-limit-reset"}
        else 0.75 + 0.5 * sample
    )
    return min(
        HELIUS_MAX_BACKOFF_SECONDS,
        max(0.0, float(delay_seconds)) * factor,
    )


def _payload_is_rate_limited(error: Any) -> bool:
    if not isinstance(error, dict):
        return False
    if error.get("code") == -32005:
        return True
    message = str(error.get("message") or "").upper()
    return "RATE LIMIT" in message or "TOO MANY REQUEST" in message


async def helius_rpc_call(
    session: aiohttp.ClientSession,
    url: str,
    method: str,
    params: list[Any],
    *,
    max_attempts: int = HELIUS_MAX_ATTEMPTS,
) -> Any:
    """공유 slot과 Helius 권장 retry를 적용한 JSON-RPC 호출이다."""
    attempts = max(1, int(max_attempts))
    request = {
        "jsonrpc": "2.0",
        "id": str(method),
        "method": str(method),
        "params": params,
    }
    last_error: BaseException | None = None
    for attempt_index in range(attempts):
        status: int | None = None
        headers: Any = {}
        try:
            await _wait_for_global_helius_slot()
            async with session.post(url, json=request) as response:
                status = int(response.status)
                headers = response.headers
                if status in HELIUS_RETRYABLE_STATUSES:
                    payload = None
                else:
                    response.raise_for_status()
                    payload = await response.json()
            payload_error = (
                payload.get("error") if isinstance(payload, dict) else None
            )
            payload_rate_limited = _payload_is_rate_limited(payload_error)
            if status not in HELIUS_RETRYABLE_STATUSES and not payload_rate_limited:
                if payload_error:
                    raise HeliusRpcError(
                        f"{method} failed: "
                        f"{redact_sensitive_text(payload_error)[:300]}"
                    )
                return payload.get("result") if isinstance(payload, dict) else None
            rate_limited = status == 429 or payload_rate_limited
            now_epoch = time.time()
            delay, source = helius_backoff_seconds(
                headers,
                attempt_index,
                now_epoch,
            )
            delay = jittered_backoff_seconds(delay, source)
            await asyncio.to_thread(
                _defer_helius_until_sync,
                now_epoch + delay,
                method=method,
                rate_limited=rate_limited,
            )
            if attempt_index + 1 >= attempts:
                if rate_limited:
                    raise HeliusRpcRateLimitError(method, attempts)
                raise HeliusRpcError(
                    f"{method} transient failure after {attempts} attempts"
                )
            logger.warning(
                "Helius RPC transient failure: method=%s status=%s "
                "attempt=%d/%d delay=%.3fs source=%s",
                method,
                status if status is not None else "jsonrpc",
                attempt_index + 1,
                attempts,
                delay,
                source,
            )
            await asyncio.sleep(delay)
        except HeliusRpcError:
            raise
        except aiohttp.ClientResponseError as exc:
            raise HeliusRpcError(
                f"{method} HTTP {exc.status}"
            ) from exc
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            last_error = exc
            if attempt_index + 1 >= attempts:
                break
            base, source = helius_backoff_seconds({}, attempt_index, time.time())
            delay = jittered_backoff_seconds(base, source)
            await asyncio.to_thread(
                _defer_helius_until_sync,
                time.time() + delay,
                method=method,
                rate_limited=False,
            )
            logger.warning(
                "Helius RPC transport retry: method=%s attempt=%d/%d "
                "delay=%.3fs error=%s",
                method,
                attempt_index + 1,
                attempts,
                delay,
                redact_sensitive_text(exc)[:200],
            )
            await asyncio.sleep(delay)
    raise HeliusRpcError(
        f"{method} transport failure after {attempts} attempts: "
        f"{redact_sensitive_text(last_error)[:200]}"
    ) from last_error
