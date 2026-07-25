"""Safety-gated Jupiter Metis swap execution through a Jito bundle."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import math
import os
import struct
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import aiohttp
from cryptography.fernet import Fernet, InvalidToken
from dotenv import load_dotenv
from solders.keypair import Keypair
from solders.instruction import CompiledInstruction
from solders.message import Message, MessageV0
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction

from src.analyzer import analyze_token
from src.state_store import (
    atomic_write_json,
    exclusive_file_lock,
    normalized_route_metadata,
    read_json,
)

WSOL_MINT = "So11111111111111111111111111111111111111112"
LIVE_BUY_PERCENT = 1  # Live execution remains capped at 1% of current SOL.
PAPER_BUY_BASIS_POINTS = 50  # 0.5% of currently available virtual cash.
ROUTE_B_SIZE_MULTIPLIER = Decimal("0.15")
LAMPORTS_PER_SOL = 1_000_000_000
JITO_FALLBACK_TIP_LAMPORTS = 2_000_000  # 0.002 SOL
JITO_MIN_TIP_LAMPORTS = 1_000
JITO_TIP_FLOOR_URL = "https://bundles.jito.wtf/api/v1/bundles/tip_floor"
MAX_FEE_BASIS_POINTS = 200  # 2% of the order's SOL notional.
MAX_ABSOLUTE_FEE_LAMPORTS = 30_000_000  # 0.03 SOL
MAX_COMPUTE_UNIT_LIMIT = 1_400_000
COMPUTE_UNIT_MARGIN = 1.10
COMPUTE_BUDGET_PROGRAM = Pubkey.from_string(
    "ComputeBudget111111111111111111111111111111"
)
MIN_FEE_RESERVE_LAMPORTS = 2_000_000
JUPITER_BASE = "https://api.jup.ag/swap/v1"
WALLETS_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "wallets.json")
JUPITER_RATE_LIMIT_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "jupiter_rate_limit.json"
)
JUPITER_RATE_LIMIT_FALLBACK = {
    "last_request_at_epoch": 0.0,
    "not_before_epoch": 0.0,
}
_QUOTE_INTERVAL_SECONDS = 1.25
_JUPITER_MAX_QUOTE_ATTEMPTS = 3
_JUPITER_MAX_RESET_WAIT_SECONDS = 60.0
MAX_ENTRY_PRICE_IMPACT_PCT = 1.5
logger = logging.getLogger("executor")
_last_successful_tip_lamports: int | None = None


def entry_price_impact_pct(quote: dict[str, Any]) -> float:
    raw_impact = quote.get("priceImpactPct")
    if raw_impact in (None, ""):
        raise RuntimeError(
            "[FILTER] Jupiter quote is missing priceImpactPct. Abort buy."
        )
    try:
        return float(raw_impact)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Jupiter quote has an invalid priceImpactPct") from exc


def validate_entry_price_impact(quote: dict[str, Any]) -> float:
    """Fail closed before every paper or live buy when impact exceeds 1.5%."""
    impact = entry_price_impact_pct(quote)
    if impact < 0 or impact > MAX_ENTRY_PRICE_IMPACT_PCT:
        raise RuntimeError(
            f"[FILTER] Entry price impact {impact:.4f}% exceeds "
            f"{MAX_ENTRY_PRICE_IMPACT_PCT:.2f}%. Abort buy."
        )
    return impact


@dataclass(frozen=True, slots=True)
class ExecutionSettings:
    rpc_url: str
    trading_mode: str
    encrypted_private_key: str
    jupiter_api_key: str
    jito_url: str
    expected_wallet_address: str
    live_acknowledgement: str

    @classmethod
    def from_env(cls) -> "ExecutionSettings":
        load_dotenv()
        helius_key = os.getenv("HELIUS_API_KEY", "").strip()
        rpc_url = os.getenv("HELIUS_RPC_HTTP_URL", "").strip()
        rpc_url = rpc_url.replace("${HELIUS_API_KEY}", helius_key)
        mode = os.getenv("TRADING_MODE", "paper").strip().lower()
        if not helius_key or not rpc_url:
            raise RuntimeError("Helius RPC settings are missing")
        if mode not in {"paper", "live"}:
            raise RuntimeError("TRADING_MODE must be paper or live")
        return cls(
            rpc_url=rpc_url,
            trading_mode=mode,
            encrypted_private_key=os.getenv("SOLANA_PRIVATE_KEY_ENCRYPTED", "").strip(),
            jupiter_api_key=os.getenv("JUPITER_API_KEY", "").strip(),
            jito_url=os.getenv(
                "JITO_BLOCK_ENGINE_URL", "https://mainnet.block-engine.jito.wtf"
            ).rstrip("/"),
            expected_wallet_address=os.getenv("EXPECTED_LIVE_WALLET_ADDRESS", "").strip(),
            live_acknowledgement=os.getenv("LIVE_TRADING_ACK", "").strip(),
        )


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    submitted: bool
    landed: bool
    mint: str
    input_lamports: int
    bundle_id: str | None = None
    signature: str | None = None
    status: str | None = None
    compute_units_consumed: int | None = None
    compute_unit_limit: int | None = None
    jito_tip_lamports: int | None = None
    lifecycle: tuple[dict[str, Any], ...] = ()


def lifecycle_event(status: str, **details: Any) -> dict[str, Any]:
    event = {
        "status": status,
        "at": datetime.now(timezone.utc).isoformat(),
    }
    event.update(details)
    logger.info("transaction lifecycle: %s", json.dumps(event, ensure_ascii=False))
    return event


def load_keypair(ciphertext: str, expected_address: str) -> Keypair:
    """Decrypt the wallet key; decryption key must come from the OS environment."""
    fernet_key = os.environ.get("SOLANA_KEY_ENCRYPTION_KEY", "").strip()
    if not ciphertext or not fernet_key:
        raise RuntimeError(
            "SOLANA_PRIVATE_KEY_ENCRYPTED and OS variable SOLANA_KEY_ENCRYPTION_KEY are required"
        )
    try:
        plaintext = Fernet(fernet_key.encode()).decrypt(ciphertext.encode()).decode().strip()
    except (InvalidToken, ValueError) as exc:
        raise RuntimeError("encrypted Solana private key could not be decrypted") from exc

    try:
        if plaintext.startswith("["):
            secret = bytes(json.loads(plaintext))
            if len(secret) != 64:
                raise ValueError("keypair byte array must contain 64 values")
            keypair = Keypair.from_bytes(secret)
        else:
            keypair = Keypair.from_base58_string(plaintext)
        if not expected_address:
            raise RuntimeError("EXPECTED_LIVE_WALLET_ADDRESS is required")
        try:
            expected = Pubkey.from_string(expected_address)
        except ValueError as exc:
            raise RuntimeError("EXPECTED_LIVE_WALLET_ADDRESS is invalid") from exc
        if keypair.pubkey() != expected:
            raise RuntimeError("decrypted key does not match EXPECTED_LIVE_WALLET_ADDRESS")
        return keypair
    finally:
        plaintext = ""  # Reduce the lifetime of the plaintext reference.


def validate_live_settings(settings: ExecutionSettings) -> None:
    if os.getenv("SOLANA_PRIVATE_KEY", "").strip():
        raise RuntimeError("plaintext SOLANA_PRIVATE_KEY is forbidden; remove it immediately")
    if settings.live_acknowledgement != "I_UNDERSTAND_LIVE_TRADING_RISK":
        raise RuntimeError("LIVE_TRADING_ACK is missing or incorrect")
    if not settings.rpc_url.startswith("https://") or "mainnet" not in settings.rpc_url:
        raise RuntimeError("live RPC must be an HTTPS Solana mainnet endpoint")
    if not settings.jito_url.startswith("https://") or "mainnet" not in settings.jito_url:
        raise RuntimeError("live Jito URL must be an HTTPS mainnet Block Engine endpoint")
    try:
        with open(WALLETS_PATH, "r", encoding="utf-8") as file:
            wallet_document = json.load(file)
        entries = wallet_document.get("wallets", [])
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("verified wallets.json is required for live mode") from exc
    if not entries or any(not isinstance(item, dict) or item.get("verified") is not True
                          for item in entries):
        raise RuntimeError("live mode refuses empty or unverified monitoring wallets")


async def json_rpc(
    session: aiohttp.ClientSession, url: str, method: str, params: list[Any]
) -> Any:
    request = {"jsonrpc": "2.0", "id": method, "method": method, "params": params}
    async with session.post(url, json=request) as response:
        response.raise_for_status()
        payload = await response.json()
    if payload.get("error"):
        raise RuntimeError(f"{method} failed: {payload['error']}")
    return payload.get("result")


async def sol_balance(session: aiohttp.ClientSession, rpc_url: str, owner: str) -> int:
    result = await json_rpc(session, rpc_url, "getBalance", [owner, {"commitment": "confirmed"}])
    return int((result or {}).get("value", 0))


def _wait_for_jupiter_slot_sync(
    interval_seconds: float = _QUOTE_INTERVAL_SECONDS,
) -> float:
    """Reserve one organisation-wide Jupiter request slot across PM2 processes."""
    with exclusive_file_lock(JUPITER_RATE_LIMIT_PATH, timeout_seconds=180.0):
        state = read_json(JUPITER_RATE_LIMIT_PATH, JUPITER_RATE_LIMIT_FALLBACK)
        now = time.time()
        last_request = float(state.get("last_request_at_epoch", 0.0) or 0.0)
        not_before = float(state.get("not_before_epoch", 0.0) or 0.0)
        target = max(now, last_request + interval_seconds, not_before)
        delay = max(0.0, target - now)
        if delay:
            time.sleep(delay)
        request_at = time.time()
        state["last_request_at_epoch"] = request_at
        if not_before <= request_at:
            state["not_before_epoch"] = 0.0
        atomic_write_json(JUPITER_RATE_LIMIT_PATH, state)
        return delay


async def _wait_for_global_jupiter_slot() -> None:
    await asyncio.to_thread(_wait_for_jupiter_slot_sync)


def _defer_jupiter_until_sync(not_before_epoch: float) -> None:
    """Publish a Jupiter cooldown so every local PM2 process observes it."""
    with exclusive_file_lock(JUPITER_RATE_LIMIT_PATH, timeout_seconds=180.0):
        state = read_json(JUPITER_RATE_LIMIT_PATH, JUPITER_RATE_LIMIT_FALLBACK)
        current = float(state.get("not_before_epoch", 0.0) or 0.0)
        state["not_before_epoch"] = max(current, not_before_epoch)
        atomic_write_json(JUPITER_RATE_LIMIT_PATH, state)


def _jupiter_backoff_seconds(
    reset_header: str | None, attempt_index: int, now_epoch: float
) -> tuple[float, str]:
    if reset_header:
        try:
            reset_epoch = float(reset_header)
        except (TypeError, ValueError):
            pass
        else:
            delay = (
                reset_epoch - now_epoch + 0.05
                if reset_epoch > now_epoch
                else max(0.05, reset_epoch)
            )
            return min(delay, _JUPITER_MAX_RESET_WAIT_SECONDS), "rate-limit-reset"
    return min(float(2**attempt_index), 30.0), "exponential-fallback"


async def jupiter_quote(
    session: aiohttp.ClientSession,
    api_key: str,
    input_mint: str,
    output_mint: str,
    amount: int,
) -> dict[str, Any]:
    headers = {"x-api-key": api_key}
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(amount),
        "slippageBps": "100",
        "restrictIntermediateTokens": "true",
    }
    quote: dict[str, Any] | None = None
    last_error: Exception | None = None
    for attempt_index in range(_JUPITER_MAX_QUOTE_ATTEMPTS):
        try:
            await _wait_for_global_jupiter_slot()
            async with session.get(
                f"{JUPITER_BASE}/quote", params=params, headers=headers
            ) as response:
                if response.status != 429 and response.status < 500:
                    response.raise_for_status()
                    quote = await response.json()
                    break
                now_epoch = time.time()
                delay, source = _jupiter_backoff_seconds(
                    response.headers.get("x-ratelimit-reset")
                    or response.headers.get("Retry-After"),
                    attempt_index,
                    now_epoch,
                )
                await asyncio.to_thread(
                    _defer_jupiter_until_sync, now_epoch + delay
                )
                logger.warning(
                    "Jupiter transient failure status=%d; waiting %.2fs via %s "
                    "(attempt %d/%d)",
                    response.status,
                    delay,
                    source,
                    attempt_index + 1,
                    _JUPITER_MAX_QUOTE_ATTEMPTS,
                )
                if attempt_index + 1 >= _JUPITER_MAX_QUOTE_ATTEMPTS:
                    raise RuntimeError(
                        "Jupiter quote failed after 3 attempts"
                    ) from None
                await asyncio.sleep(delay)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            last_error = exc
            if attempt_index + 1 >= _JUPITER_MAX_QUOTE_ATTEMPTS:
                break
            delay = min(float(2**attempt_index), 30.0)
            logger.warning(
                "Jupiter transport retry: attempt=%d/%d delay=%.2fs error=%s",
                attempt_index + 1,
                _JUPITER_MAX_QUOTE_ATTEMPTS,
                delay,
                type(exc).__name__,
            )
            await asyncio.sleep(delay)
    if quote is None:
        raise RuntimeError(
            f"Jupiter quote failed after 3 attempts: {last_error or 'no response'}"
        ) from last_error
    if not quote.get("routePlan") or int(quote.get("outAmount", "0")) <= 0:
        raise RuntimeError("Jupiter returned no executable route")
    return quote


async def build_swap(
    session: aiohttp.ClientSession,
    api_key: str,
    quote: dict[str, Any],
    owner: str,
    jito_tip_lamports: int,
) -> bytes:
    body = {
        "quoteResponse": quote,
        "userPublicKey": owner,
        "wrapAndUnwrapSol": True,
        # Jupiter inserts a ComputeBudget SetComputeUnitLimit instruction. Its
        # provisional value is replaced after our own Helius simulation below.
        "dynamicComputeUnitLimit": True,
        "prioritizationFeeLamports": {"jitoTipLamports": jito_tip_lamports},
    }
    async with session.post(
        f"{JUPITER_BASE}/swap", json=body, headers={"x-api-key": api_key}
    ) as response:
        response.raise_for_status()
        payload = await response.json()
    encoded = payload.get("swapTransaction")
    if not encoded:
        raise RuntimeError(f"Jupiter did not return a swap transaction: {payload}")
    return base64.b64decode(encoded, validate=True)


def sign_transaction(serialized: bytes, signer: Keypair) -> tuple[str, str]:
    unsigned = VersionedTransaction.from_bytes(serialized)
    signed = VersionedTransaction(unsigned.message, [signer])
    return base64.b64encode(bytes(signed)).decode(), str(signed.signatures[0])


async def simulate_signed_transaction(
    session: aiohttp.ClientSession, rpc_url: str, signed_transaction: str
) -> int:
    result = await json_rpc(
        session, rpc_url, "simulateTransaction",
        [signed_transaction, {"encoding": "base64", "sigVerify": True,
                              "replaceRecentBlockhash": False, "commitment": "processed"}],
    )
    value = (result or {}).get("value") or {}
    if value.get("err") is not None:
        raise RuntimeError(f"swap simulation failed: {value['err']}")
    units = value.get("unitsConsumed")
    if units is None or int(units) <= 0:
        raise RuntimeError("swap simulation did not return a positive unitsConsumed")
    return int(units)


def apply_compute_unit_limit(serialized: bytes, units: int) -> bytes:
    """Replace Jupiter's SetComputeUnitLimit instruction without changing accounts."""
    if units <= 0 or units > MAX_COMPUTE_UNIT_LIMIT:
        raise ValueError(f"compute unit limit must be between 1 and {MAX_COMPUTE_UNIT_LIMIT}")
    transaction = VersionedTransaction.from_bytes(serialized)
    message = transaction.message
    keys = list(message.account_keys)
    instructions = []
    replaced = False
    for instruction in message.instructions:
        program_id = keys[instruction.program_id_index]
        data = bytes(instruction.data)
        if program_id == COMPUTE_BUDGET_PROGRAM and data[:1] == b"\x02":
            instruction = CompiledInstruction(
                instruction.program_id_index,
                struct.pack("<BI", 2, units),
                bytes(instruction.accounts),
            )
            replaced = True
        instructions.append(instruction)
    if not replaced:
        raise RuntimeError("Jupiter transaction is missing SetComputeUnitLimit")
    if isinstance(message, MessageV0):
        tuned_message = MessageV0(
            message.header,
            keys,
            message.recent_blockhash,
            instructions,
            message.address_table_lookups,
        )
    elif isinstance(message, Message):
        tuned_message = Message.new_with_compiled_instructions(
            message.header.num_required_signatures,
            message.header.num_readonly_signed_accounts,
            message.header.num_readonly_unsigned_accounts,
            keys,
            message.recent_blockhash,
            instructions,
        )
    else:
        raise RuntimeError(f"unsupported Solana message type: {type(message).__name__}")
    return bytes(VersionedTransaction.populate(tuned_message, transaction.signatures))


def _tip_percentile(row: dict[str, Any], percentile: float) -> Decimal:
    points = []
    for key, value in row.items():
        prefix = "landed_tips_"
        suffix = "th_percentile"
        if key.startswith(prefix) and key.endswith(suffix) and not key.startswith("ema_"):
            try:
                points.append((
                    Decimal(key[len(prefix):-len(suffix)]),
                    Decimal(str(value)),
                ))
            except (InvalidOperation, TypeError, ValueError):
                continue
    points.sort()
    if not points:
        raise ValueError("tip floor response contains no percentile values")
    target = Decimal(str(percentile))
    if target <= points[0][0]:
        return points[0][1]
    if target >= points[-1][0]:
        return points[-1][1]
    for (left_p, left_v), (right_p, right_v) in zip(points, points[1:]):
        if left_p <= target <= right_p:
            weight = (target - left_p) / (right_p - left_p)
            return left_v + (right_v - left_v) * weight
    raise ValueError("could not interpolate Jito tip percentile")


async def dynamic_jito_tip(
    session: aiohttp.ClientSession,
    order_notional_lamports: int,
    urgency: str = "normal",
) -> int:
    """Return a percentile-derived Jito tip protected by relative/absolute caps."""
    global _last_successful_tip_lamports
    if urgency not in {"normal", "emergency"}:
        raise ValueError("urgency must be normal or emergency")
    percentile = 62.5 if urgency == "normal" else 82.5
    cap = min(
        max(0, order_notional_lamports) * MAX_FEE_BASIS_POINTS // 10_000,
        MAX_ABSOLUTE_FEE_LAMPORTS,
    )
    if cap < JITO_MIN_TIP_LAMPORTS:
        raise RuntimeError("order is too small to fund Jito's minimum tip within fee cap")
    source = "tip_floor"
    try:
        timeout = aiohttp.ClientTimeout(total=3)
        async with session.get(JITO_TIP_FLOOR_URL, timeout=timeout) as response:
            response.raise_for_status()
            payload = await response.json()
        if not isinstance(payload, list) or not payload or not isinstance(payload[0], dict):
            raise ValueError("unexpected Jito tip floor response")
        requested = math.ceil(
            _tip_percentile(payload[0], percentile) * LAMPORTS_PER_SOL
        )
        if requested <= 0:
            raise ValueError("Jito tip floor returned a non-positive tip")
        _last_successful_tip_lamports = requested
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError) as exc:
        requested = _last_successful_tip_lamports or JITO_FALLBACK_TIP_LAMPORTS
        source = "recent_success" if _last_successful_tip_lamports else "fallback_0.002_sol"
        logger.warning("Jito tip floor unavailable; using %s: %s", source, exc)
    selected = max(JITO_MIN_TIP_LAMPORTS, min(requested, cap))
    logger.info(
        "Jito dynamic tip: urgency=%s percentile=%.1f requested=%d cap=%d selected=%d source=%s",
        urgency, percentile, requested, cap, selected, source,
    )
    return selected


async def send_bundle(
    session: aiohttp.ClientSession, jito_url: str, signed_transaction: str
) -> str:
    result = await json_rpc(
        session,
        f"{jito_url}/api/v1/bundles",
        "sendBundle",
        [[signed_transaction], {"encoding": "base64"}],
    )
    if not result:
        raise RuntimeError("Jito did not return a bundle id")
    return str(result)


async def wait_for_bundle(
    session: aiohttp.ClientSession, jito_url: str, bundle_id: str, timeout: int = 45
) -> str:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        result = await json_rpc(
            session, f"{jito_url}/api/v1/getBundleStatuses", "getBundleStatuses", [[bundle_id]]
        )
        values = (result or {}).get("value") or []
        if values:
            status = values[0]
            if status.get("err") not in (None, {"Ok": None}):
                raise RuntimeError(f"Jito bundle failed: {status['err']}")
            confirmation = status.get("confirmation_status") or status.get("confirmationStatus")
            if confirmation in {"confirmed", "finalized"}:
                return str(confirmation)
        await asyncio.sleep(2)
    raise TimeoutError(
        "bundle landing is unconfirmed; do not resubmit until signature history is checked"
    )


async def execute_live_swap(
    session: aiohttp.ClientSession,
    settings: ExecutionSettings,
    quote: dict[str, Any],
    signer: Keypair,
    mint: str,
    input_amount: int,
    order_notional_lamports: int,
    urgency: str,
    jito_tip_lamports: int | None = None,
) -> ExecutionResult:
    """Build, simulate, tune, sign, submit, and classify one live swap."""
    events: list[dict[str, Any]] = []
    tip = (
        jito_tip_lamports
        if jito_tip_lamports is not None
        else await dynamic_jito_tip(session, order_notional_lamports, urgency)
    )
    serialized = await build_swap(
        session, settings.jupiter_api_key, quote, str(signer.pubkey()), tip
    )
    events.append(lifecycle_event("BUILT", jito_tip_lamports=tip, urgency=urgency))
    provisional_signed, _ = sign_transaction(serialized, signer)
    try:
        consumed = await simulate_signed_transaction(
            session, settings.rpc_url, provisional_signed
        )
        limit = min(MAX_COMPUTE_UNIT_LIMIT, math.ceil(consumed * COMPUTE_UNIT_MARGIN))
        tuned = apply_compute_unit_limit(serialized, limit)
        signed, signature = sign_transaction(tuned, signer)
        events.append(lifecycle_event(
            "SIMULATED",
            compute_units_consumed=consumed,
            compute_unit_limit=limit,
            margin=COMPUTE_UNIT_MARGIN,
        ))
        logger.info(
            "CU simulation: consumed=%d margin=%.2f final_limit=%d",
            consumed, COMPUTE_UNIT_MARGIN, limit,
        )
    except Exception as exc:
        events.append(lifecycle_event("FAILED", stage="SIMULATION", error=str(exc)[:500]))
        return ExecutionResult(
            False, False, mint, input_amount, status="FAILED",
            jito_tip_lamports=tip, lifecycle=tuple(events),
        )
    try:
        bundle_id = await send_bundle(session, settings.jito_url, signed)
        events.append(lifecycle_event(
            "SUBMITTED", bundle_id=bundle_id, signature=signature
        ))
    except Exception as exc:
        events.append(lifecycle_event("FAILED", stage="SUBMISSION", error=str(exc)[:500]))
        return ExecutionResult(
            False, False, mint, input_amount, signature=signature, status="FAILED",
            compute_units_consumed=consumed, compute_unit_limit=limit,
            jito_tip_lamports=tip, lifecycle=tuple(events),
        )
    try:
        confirmation = await wait_for_bundle(session, settings.jito_url, bundle_id)
        events.append(lifecycle_event("LANDED", confirmation=confirmation))
        return ExecutionResult(
            True, True, mint, input_amount, bundle_id, signature, "LANDED",
            consumed, limit, tip, tuple(events),
        )
    except TimeoutError as exc:
        events.append(lifecycle_event("UNKNOWN", error=str(exc)[:500]))
        return ExecutionResult(
            True, False, mint, input_amount, bundle_id, signature, "UNKNOWN",
            consumed, limit, tip, tuple(events),
        )
    except Exception as exc:
        events.append(lifecycle_event("FAILED", stage="CONFIRMATION", error=str(exc)[:500]))
        return ExecutionResult(
            True, False, mint, input_amount, bundle_id, signature, "FAILED",
            consumed, limit, tip, tuple(events),
        )


def route_sized_amount(base_amount: int, route_type: str) -> int:
    route = str(normalized_route_metadata(route_type)["route_type"])
    if route == "B":
        return int(Decimal(base_amount) * ROUTE_B_SIZE_MULTIPLIER)
    return base_amount


async def execute_buy(mint: str, settings: ExecutionSettings | None = None) -> ExecutionResult:
    """Analyze, size, sign and submit a single safety-gated buy."""
    settings = settings or ExecutionSettings.from_env()
    try:
        Pubkey.from_string(mint)
    except ValueError as exc:
        raise ValueError(f"invalid output mint: {mint}") from exc

    report = await analyze_token(mint)
    if not report.should_enter or report.route_type not in {"A", "B"}:
        return ExecutionResult(False, False, mint, 0, status="analyzer_rejected")
    if settings.trading_mode != "live":
        if not settings.jupiter_api_key:
            raise RuntimeError("JUPITER_API_KEY is required for paper price quotes")
        from src.risk_manager import paper_cash_balance, record_paper_buy

        balance = await paper_cash_balance()
        base_amount = balance * PAPER_BUY_BASIS_POINTS // 10_000
        amount = route_sized_amount(base_amount, report.route_type)
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            quote = await jupiter_quote(
                session, settings.jupiter_api_key, WSOL_MINT, mint, amount
            )
        impact = validate_entry_price_impact(quote)
        await record_paper_buy(
            mint,
            amount,
            int(quote["outAmount"]),
            entry_price_impact_pct=impact,
            route_type=report.route_type,
        )
        return ExecutionResult(False, False, mint, amount, status="paper_buy_recorded")
    if not settings.jupiter_api_key:
        raise RuntimeError("JUPITER_API_KEY is required for live execution")

    validate_live_settings(settings)
    signer = load_keypair(settings.encrypted_private_key, settings.expected_wallet_address)
    owner = str(signer.pubkey())
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        balance = await sol_balance(session, settings.rpc_url, owner)
        base_amount = balance * LIVE_BUY_PERCENT // 100
        amount = route_sized_amount(base_amount, report.route_type)
        if amount <= 0 or balance - amount < MIN_FEE_RESERVE_LAMPORTS:
            raise RuntimeError("insufficient SOL balance after the fixed 1% buy and fee reserve")
        quote = await jupiter_quote(
            session, settings.jupiter_api_key, WSOL_MINT, mint, amount
        )
        validate_entry_price_impact(quote)
        tip = await dynamic_jito_tip(session, amount, "normal")
        if balance - amount < tip + MIN_FEE_RESERVE_LAMPORTS:
            raise RuntimeError("insufficient SOL balance for dynamic Jito tip and fee reserve")
        result = await execute_live_swap(
            session, settings, quote, signer, mint, amount, amount, "normal", tip
        )
    return result


async def token_balance(
    session: aiohttp.ClientSession, rpc_url: str, owner: str, mint: str
) -> int:
    result = await json_rpc(
        session,
        rpc_url,
        "getTokenAccountsByOwner",
        [owner, {"mint": mint}, {"encoding": "jsonParsed", "commitment": "confirmed"}],
    )
    total = 0
    for account in (result or {}).get("value") or []:
        info = (((account.get("account") or {}).get("data") or {}).get("parsed") or {}).get("info") or {}
        total += int(((info.get("tokenAmount") or {}).get("amount")) or "0")
    return total


async def execute_sell(
    mint: str,
    percent: int,
    settings: ExecutionSettings | None = None,
    urgency: str = "normal",
) -> ExecutionResult:
    """Sell 50% or 100% of the current token balance through a Jito bundle."""
    if percent not in {50, 100}:
        raise ValueError("risk exits may sell only 50% or 100%")
    settings = settings or ExecutionSettings.from_env()
    if settings.trading_mode != "live":
        return ExecutionResult(False, False, mint, 0, status=f"paper_sell_{percent}")
    if not settings.jupiter_api_key:
        raise RuntimeError("JUPITER_API_KEY is required for live execution")
    validate_live_settings(settings)
    signer = load_keypair(settings.encrypted_private_key, settings.expected_wallet_address)
    owner = str(signer.pubkey())
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        balance = await token_balance(session, settings.rpc_url, owner, mint)
        amount = balance if percent == 100 else balance // 2
        if amount <= 0:
            raise RuntimeError("no token balance available to sell")
        quote = await jupiter_quote(
            session, settings.jupiter_api_key, mint, WSOL_MINT, amount
        )
        result = await execute_live_swap(
            session,
            settings,
            quote,
            signer,
            mint,
            amount,
            int(quote["outAmount"]),
            urgency,
        )
    return result


async def async_main(mint: str) -> int:
    result = await execute_buy(mint)
    print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
    return 0 if result.landed or result.status == "paper_mode_pass" else 2


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mint", help="SPL token Mint address to buy")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(async_main(args.mint)))


if __name__ == "__main__":
    main()
