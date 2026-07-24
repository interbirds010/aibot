"""On-chain wallet performance ledger with reversible monitoring cool-downs."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
from dotenv import load_dotenv

from src.state_store import (
    atomic_write_json,
    migrate_json,
    read_json as locked_read_json,
    update_json,
)

logger = logging.getLogger("wallet-performance")
ROOT = Path(__file__).resolve().parents[1]
WALLETS_PATH = ROOT / "data" / "wallets.json"
CANDIDATE_PATH = ROOT / "data" / "wallet_candidates.json"
PERFORMANCE_PATH = ROOT / "data" / "wallet_performance.json"
EVALUATION_DELAY_SECONDS = 3600
MAX_RETURN_PERCENT = 5000.0
COOLDOWN_SECONDS = 86_400
STATUS_ACTIVE = "ACTIVE"
STATUS_COOL_DOWN = "COOL_DOWN"
_lock = asyncio.Lock()


def capped_return_percent(value: Any) -> float:
    """Cap positive pool-price outliers while preserving genuine losses."""
    try:
        return min(float(value), MAX_RETURN_PERCENT)
    except (TypeError, ValueError):
        return 0.0


def read_json(path: Path, fallback: Any) -> Any:
    return locked_read_json(path, fallback)


def atomic_json(path: Path, document: Any) -> None:
    atomic_write_json(path, document)


def entries(document: Any) -> list[dict[str, Any]]:
    rows = document.get("wallets", []) if isinstance(document, dict) else []
    return [row for row in rows if isinstance(row, dict) and row.get("address")]


def parse_utc_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def wallet_is_cooling_down(row: Any) -> bool:
    return isinstance(row, dict) and row.get("status") == STATUS_COOL_DOWN


def cooldown_and_replace(wallet: str, reason: str) -> None:
    max_wallets = max(1, int(os.getenv("WALLET_MAX_WALLETS", "20")))
    performance = read_json(PERFORMANCE_PATH, {"wallets": {}})
    performance_rows = performance.get("wallets", {}) if isinstance(performance, dict) else {}
    unavailable = {
        address for address, state in performance_rows.items()
        if wallet_is_cooling_down(state) or (
            isinstance(state, dict) and state.get("evicted") is True
        )
    }
    candidates = entries(read_json(CANDIDATE_PATH, {}))

    def mutate(document: dict[str, Any]) -> dict[str, Any] | None:
        rows = [row for row in entries(document) if row["address"] != wallet]
        rows.sort(
            key=lambda row: (
                bool(row.get("is_elite") or row.get("locked")),
                float(row.get("win_rate", 0) or 0),
                float(row.get("average_return_percent", 0) or 0),
            ),
            reverse=True,
        )
        rows = rows[:max_wallets]
        active = {row["address"] for row in rows}
        replacement = next((
            row for row in candidates
            if row["address"] not in active
            and row["address"] != wallet
            and row["address"] not in unavailable
        ), None)
        if replacement and len(rows) < max_wallets:
            rows.append(replacement)
        rows = rows[:max_wallets]
        document.update({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "count": len(rows),
            "wallets": rows,
            "last_cooldown": {"address": wallet, "reason": reason},
        })
        return replacement

    replacement, _ = update_json(WALLETS_PATH, {"wallets": []}, mutate)
    logger.warning(
        "wallet cool-down started wallet=%s duration_seconds=%s reason=%s replacement=%s",
        wallet, COOLDOWN_SECONDS, reason, (replacement or {}).get("address"),
    )


def wallet_row(state: dict[str, Any], wallet: str) -> dict[str, Any]:
    """Return a normalized, backwards-compatible wallet pipeline record."""
    row = state.setdefault("wallets", {}).setdefault(wallet, {})
    row.setdefault("wins", 0)
    row.setdefault("losses", 0)
    row.setdefault("samples", [])
    row.setdefault("pending", [])
    row.setdefault("processed_signatures", [])
    row.setdefault("paper_buy_signatures", [])
    row.setdefault("safety_block_signatures", [])
    row.setdefault("status", STATUS_ACTIVE)

    # Keep the old keys while exposing explicit pipeline counters to new readers.
    row["evaluation_completed_count"] = max(
        int(row.get("evaluation_completed_count", 0)),
        int(row["wins"]) + int(row["losses"]),
        len(row["samples"]),
    )
    row["evaluation_pending_count"] = len(row["pending"])
    row["processed_count"] = max(
        int(row.get("processed_count", 0)),
        len(row["processed_signatures"]),
    )
    row["paper_buy_count"] = int(row.get("paper_buy_count", row.get("virtual_buys", 0)))
    row["safety_block_count"] = int(
        row.get("safety_block_count", row.get("scam_rejections", 0))
    )
    row["virtual_buys"] = row["paper_buy_count"]
    row["scam_rejections"] = row["safety_block_count"]
    return row


def sync_pipeline_counts(row: dict[str, Any]) -> None:
    """Synchronize persisted counters with their canonical event collections."""
    row["evaluation_completed_count"] = max(
        int(row.get("evaluation_completed_count", 0)),
        int(row.get("wins", 0)) + int(row.get("losses", 0)),
        len(row.get("samples", [])),
    )
    row["evaluation_pending_count"] = len(row.get("pending", []))
    row["processed_count"] = max(
        int(row.get("processed_count", 0)),
        len(row.get("processed_signatures", [])),
    )
    row["paper_buy_count"] = int(row.get("paper_buy_count", row.get("virtual_buys", 0)))
    row["safety_block_count"] = int(
        row.get("safety_block_count", row.get("scam_rejections", 0))
    )
    # Compatibility for existing feeder/monitor code and historical files.
    row["virtual_buys"] = row["paper_buy_count"]
    row["scam_rejections"] = row["safety_block_count"]


def migrate_performance_document(state: dict[str, Any]) -> bool:
    if int(state.get("schema_version", 1) or 1) >= 4:
        return False
    amnestied = 0
    migrated_at = datetime.now(timezone.utc).isoformat()
    for wallet, row in state.setdefault("wallets", {}).items():
        if not isinstance(row, dict):
            continue
        for sample in row.get("pending", []):
            if not isinstance(sample, dict):
                continue
            mint = str(sample.get("mint", ""))
            signature = str(sample.get("signature", "unknown"))
            evaluate_at = float(sample.get("evaluate_at", 0) or 0)
            sample.setdefault("observation_id", f"{signature}:{mint}")
            if "acquired_raw" not in sample:
                sample["acquired_raw"] = 1_000_000
                sample["legacy_probe"] = True
            sample.setdefault("status", "PENDING")
            sample.setdefault("evaluation_attempts", 0)
            sample.setdefault("next_retry_at", evaluate_at)
            sample.setdefault("evaluated_at", None)
            sample.setdefault("evaluation_price", None)
            sample.setdefault("price_source", None)
            sample.setdefault("last_error", None)
        legacy_status = str(row.get("status", "")).upper()
        if row.get("evicted") is True or legacy_status in {
            "EVICTED", "EXILED", "BLACKLISTED", "BLOCKED"
        }:
            legacy_reason = row.get("eviction_reason")
            if legacy_reason:
                row["legacy_eviction_reason"] = legacy_reason
            row["evicted"] = False
            row.pop("eviction_reason", None)
            row["status"] = STATUS_ACTIVE
            row["safety_block_count"] = 0
            row["scam_rejections"] = 0
            row["safety_block_signatures"] = []
            row["amnestied_at"] = migrated_at
            amnestied += 1
        else:
            row.setdefault("status", STATUS_ACTIVE)
        observed_signatures = {
            str(sample.get("signature"))
            for sample in row.get("pending", [])
            if isinstance(sample, dict) and sample.get("signature")
        }
        for sample in row.get("samples", []):
            if not isinstance(sample, dict):
                continue
            observation_id = str(sample.get("observation_id", ""))
            if ":" in observation_id:
                observed_signatures.add(observation_id.split(":", 1)[0])
        existing_signatures = [
            str(signature) for signature in row.get("processed_signatures", [])
            if signature
        ]
        row["processed_signatures"] = list(
            dict.fromkeys([*existing_signatures, *sorted(observed_signatures)])
        )[-1000:]
        row["processed_count"] = max(
            int(row.get("processed_count", 0)),
            len(row["processed_signatures"]),
            int(row.get("evaluation_completed_count", 0))
            + int(row.get("evaluation_pending_count", 0)),
        )
        sync_pipeline_counts(row)
    state.pop("latest_prices", None)
    state["schema_version"] = 4
    state["legacy_evictions_amnestied_count"] = amnestied
    state["legacy_evictions_amnestied_at"] = migrated_at
    state["updated_at"] = migrated_at
    return True


def ensure_performance_migrated() -> dict[str, Any]:
    return migrate_json(
        PERFORMANCE_PATH, {"schema_version": 4, "version": 0, "wallets": {}},
        migrate_performance_document,
    )


def restore_expired_cooldowns(now: datetime | None = None) -> int:
    """Return wallets to the candidate pool after 24 elapsed UTC hours."""
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)

    def mutate(state: dict[str, Any]) -> int:
        restored = 0
        for wallet, row in state.setdefault("wallets", {}).items():
            if isinstance(row, dict) and row.get("evicted") is True:
                legacy_reason = row.get("eviction_reason")
                if legacy_reason:
                    row["legacy_eviction_reason"] = legacy_reason
                row["evicted"] = False
                row.pop("eviction_reason", None)
                row["status"] = STATUS_ACTIVE
                row["safety_block_count"] = 0
                row["scam_rejections"] = 0
                row["safety_block_signatures"] = []
                row["amnestied_at"] = current.isoformat()
                state["late_legacy_evictions_amnestied_count"] = int(
                    state.get("late_legacy_evictions_amnestied_count", 0)
                ) + 1
                restored += 1
                continue
            if not wallet_is_cooling_down(row):
                continue
            started = parse_utc_timestamp(row.get("cooldown_started_at"))
            if started is None:
                # A malformed timestamp must not release immediately or trap forever.
                row["cooldown_started_at"] = current.isoformat()
                row["cooldown_timestamp_repaired_at"] = current.isoformat()
                continue
            if (current - started).total_seconds() < COOLDOWN_SECONDS:
                continue
            row["status"] = STATUS_ACTIVE
            row["cooldown_completed_at"] = current.isoformat()
            row.pop("cooldown_started_at", None)
            row.pop("cooldown_reason", None)
            row["safety_block_count"] = 0
            row["scam_rejections"] = 0
            row["safety_block_signatures"] = []
            row["evicted"] = False
            restored += 1
        state["updated_at"] = current.isoformat()
        return restored

    restored, _ = update_json(
        PERFORMANCE_PATH,
        {"schema_version": 3, "version": 0, "wallets": {}},
        mutate,
    )
    if restored:
        logger.info("restored %s wallets after 24-hour cool-down", restored)
    return int(restored)


async def record_paper_buy_success(wallet: str, mint: str, signature: str) -> None:
    """Atomically count an analyzer-approved paper position once per transaction."""
    async with _lock:
        def mutate(state: dict[str, Any]) -> None:
            row = wallet_row(state, wallet)
            signatures = row.setdefault("paper_buy_signatures", [])
            if signature not in signatures:
                row["paper_buy_count"] = int(row.get("paper_buy_count", 0)) + 1
                signatures.append(signature)
                row["paper_buy_signatures"] = signatures[-100:]
                row["last_virtual_buy"] = {"mint": mint, "signature": signature, "at": datetime.now(timezone.utc).isoformat()}
            sync_pipeline_counts(row)
            state["updated_at"] = datetime.now(timezone.utc).isoformat()

        update_json(PERFORMANCE_PATH, {"wallets": {}}, mutate)


async def record_virtual_buy(wallet: str, mint: str, signature: str) -> None:
    """Backward-compatible alias for older monitor integrations."""
    await record_paper_buy_success(wallet, mint, signature)


async def observe_buy(wallet: str, mint: str, acquired_raw: int, paid_lamports: int, signature: str) -> None:
    if acquired_raw <= 0 or paid_lamports <= 0:
        return
    now, price = time.time(), paid_lamports / acquired_raw
    async with _lock:
        def mutate(state: dict[str, Any]) -> None:
            row = wallet_row(state, wallet)
            processed_signatures = row.setdefault("processed_signatures", [])
            if signature not in processed_signatures:
                row["processed_count"] = int(row.get("processed_count", 0)) + 1
                processed_signatures.append(signature)
                row["processed_signatures"] = processed_signatures[-1000:]
                row["last_processed_buy"] = {
                    "mint": mint,
                    "signature": signature,
                    "at": datetime.now(timezone.utc).isoformat(),
                }
            observation_id = f"{signature}:{mint}"
            known_observations = {
                str(item.get("observation_id") or f"{item.get('signature')}:{item.get('mint')}")
                for item in [*row.get("pending", []), *row.get("samples", [])]
                if isinstance(item, dict)
            }
            if observation_id not in known_observations:
                row["pending"].append({
                    "observation_id": observation_id,
                    "mint": mint,
                    "acquired_raw": acquired_raw,
                    "entry_price": price,
                    "observed_at": now,
                    "evaluate_at": now + EVALUATION_DELAY_SECONDS,
                    "signature": signature,
                    "status": "PENDING",
                    "evaluation_attempts": 0,
                    "next_retry_at": now + EVALUATION_DELAY_SECONDS,
                    "evaluated_at": None,
                    "evaluation_price": None,
                    "price_source": None,
                    "last_error": None,
                })
            sync_pipeline_counts(row)
            state.pop("latest_prices", None)
            state["updated_at"] = datetime.now(timezone.utc).isoformat()

        update_json(PERFORMANCE_PATH, {"wallets": {}}, mutate)


async def reject_unsafe_buy(
    wallet: str, mint: str, reasons: list[str], signature: str = "unknown"
) -> None:
    """Persist one analyzer rejection per source transaction."""
    async with _lock:
        def mutate(state: dict[str, Any]) -> bool:
            row = wallet_row(state, wallet)
            signatures = row.setdefault("safety_block_signatures", [])
            decision_key = signature if signature != "unknown" else f"{mint}:{len(signatures)}"
            if decision_key not in signatures:
                row["safety_block_count"] = int(row.get("safety_block_count", 0)) + 1
                signatures.append(decision_key)
                row["safety_block_signatures"] = signatures[-100:]
            row.update({"last_rejected_mint": mint, "last_rejection_reasons": reasons})
            sync_pipeline_counts(row)
            should_cool_down = (
                row["safety_block_count"] >= 2
                and row.get("status") != STATUS_COOL_DOWN
            )
            if should_cool_down:
                row.update({
                    "status": STATUS_COOL_DOWN,
                    "cooldown_started_at": datetime.now(timezone.utc).isoformat(),
                    "cooldown_reason": "repeated unsafe-token purchases",
                    "evicted": False,
                })
                row.pop("eviction_reason", None)
            state["updated_at"] = datetime.now(timezone.utc).isoformat()
            return should_cool_down

        should_cool_down, _ = update_json(PERFORMANCE_PATH, {"wallets": {}}, mutate)
    if should_cool_down:
        cooldown_and_replace(wallet, "repeated unsafe-token purchases")


def due_observations(now: float) -> list[tuple[str, dict[str, Any]]]:
    state = read_json(PERFORMANCE_PATH, {"wallets": {}})
    due: list[tuple[str, dict[str, Any]]] = []
    for wallet, row in state.get("wallets", {}).items():
        if not isinstance(row, dict):
            continue
        for sample in row.get("pending", []):
            if not isinstance(sample, dict):
                continue
            retry_at = float(sample.get("next_retry_at", sample.get("evaluate_at", 0)) or 0)
            if (
                sample.get("status", "PENDING") == "PENDING"
                and now >= float(sample.get("evaluate_at", 0) or 0)
                and now >= retry_at
            ):
                due.append((wallet, dict(sample)))
    return due


def complete_observation(
    wallet: str, sample: dict[str, Any], current_price: float
) -> str | None:
    observation_id = str(
        sample.get("observation_id") or f"{sample.get('signature')}:{sample.get('mint')}"
    )
    now = time.time()

    def mutate(state: dict[str, Any]) -> str | None:
        row = wallet_row(state, wallet)
        target = next((
            item for item in row.get("pending", [])
            if str(item.get("observation_id") or f"{item.get('signature')}:{item.get('mint')}")
            == observation_id
        ), None)
        if not isinstance(target, dict):
            return None
        entry = float(target.get("entry_price", 0) or 0)
        change = (current_price / entry - 1) * 100 if entry else -100.0
        row.setdefault("samples", []).append({
            "observation_id": observation_id,
            "mint": target["mint"],
            "return_percent": round(change, 2),
            "evaluated_at": now,
            "evaluation_price": current_price,
            "price_source": "jupiter_executable_sell_quote",
        })
        row["pending"] = [item for item in row["pending"] if item is not target]
        result_key = "wins" if change > 0 else "losses"
        row[result_key] = int(row.get(result_key, 0)) + 1
        row["samples"] = row["samples"][-100:]
        sync_pipeline_counts(row)
        total = int(row.get("wins", 0)) + int(row.get("losses", 0))
        win_rate = int(row.get("wins", 0)) / total * 100 if total else 0.0
        bounded = [
            capped_return_percent(item.get("return_percent", 0))
            for item in row["samples"] if isinstance(item, dict)
        ]
        average = sum(bounded) / len(bounded) if bounded else 0.0
        reason = None
        if change <= -50 or (total >= 3 and (win_rate < 35 or average < -20)):
            reason = f"performance win_rate={win_rate:.1f}% avg={average:.1f}% last={change:.1f}%"
            if row.get("status") != STATUS_COOL_DOWN:
                row.update({
                    "status": STATUS_COOL_DOWN,
                    "cooldown_started_at": datetime.now(timezone.utc).isoformat(),
                    "cooldown_reason": reason,
                    "evicted": False,
                })
                row.pop("eviction_reason", None)
            else:
                reason = None
        state.pop("latest_prices", None)
        state["updated_at"] = datetime.now(timezone.utc).isoformat()
        return reason

    reason, _ = update_json(PERFORMANCE_PATH, {"wallets": {}}, mutate)
    return reason


def fail_observation(wallet: str, sample: dict[str, Any], error: Exception) -> None:
    observation_id = str(
        sample.get("observation_id") or f"{sample.get('signature')}:{sample.get('mint')}"
    )

    def mutate(state: dict[str, Any]) -> None:
        row = wallet_row(state, wallet)
        target = next((
            item for item in row.get("pending", [])
            if str(item.get("observation_id") or f"{item.get('signature')}:{item.get('mint')}")
            == observation_id
        ), None)
        if not isinstance(target, dict):
            return
        attempts = int(target.get("evaluation_attempts", 0)) + 1
        target.update({
            "observation_id": observation_id,
            "status": "PENDING",
            "evaluation_attempts": attempts,
            "next_retry_at": time.time() + min(3600, 60 * (2 ** min(attempts - 1, 6))),
            "last_error": f"{type(error).__name__}: {error}"[:500],
        })
        sync_pipeline_counts(row)
        state["updated_at"] = datetime.now(timezone.utc).isoformat()

    update_json(PERFORMANCE_PATH, {"wallets": {}}, mutate)


async def legacy_probe_amount(
    session: aiohttp.ClientSession, rpc_url: str, mint: str
) -> int:
    request = {
        "jsonrpc": "2.0", "id": f"supply:{mint}", "method": "getTokenSupply",
        "params": [mint, {"commitment": "confirmed"}],
    }
    async with session.post(rpc_url, json=request) as response:
        response.raise_for_status()
        payload = await response.json()
    if payload.get("error"):
        raise RuntimeError(f"getTokenSupply failed: {payload['error']}")
    value = (payload.get("result") or {}).get("value") or {}
    decimals = max(0, int(value.get("decimals", 0) or 0))
    # A 1,000-token sell probe avoids invalid dust routes while remaining small
    # enough to approximate the wallet's observed lamports/raw entry price.
    return max(1, 1_000 * (10 ** decimals))


async def performance_loop(interval_seconds: float = 60) -> None:
    load_dotenv()
    ensure_performance_migrated()
    api_key = os.getenv("JUPITER_API_KEY", "").strip()
    helius_key = os.getenv("HELIUS_API_KEY", "").strip()
    rpc_url = os.getenv("HELIUS_RPC_HTTP_URL", "").strip().replace(
        "${HELIUS_API_KEY}", helius_key
    )
    if not api_key:
        logger.warning("wallet performance evaluator disabled: JUPITER_API_KEY is missing")
        return
    if not rpc_url:
        logger.warning("wallet performance evaluator disabled: Helius RPC URL is missing")
        return
    from src.executor import WSOL_MINT, jupiter_quote

    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            await asyncio.sleep(interval_seconds)
            cooldowns: list[tuple[str, str]] = []
            for wallet, sample in due_observations(time.time()):
                try:
                    amount = int(sample.get("acquired_raw", 0) or 0)
                    if sample.get("legacy_probe"):
                        amount = await legacy_probe_amount(
                            session, rpc_url, str(sample["mint"])
                        )
                    quote = await jupiter_quote(
                        session, api_key, str(sample["mint"]), WSOL_MINT, amount
                    )
                    current_price = int(quote["outAmount"]) / amount
                    reason = complete_observation(wallet, sample, current_price)
                    if reason:
                        cooldowns.append((wallet, reason))
                except Exception as exc:
                    fail_observation(wallet, sample, exc)
                    logger.warning(
                        "wallet observation retry scheduled: wallet=%s mint=%s error=%s",
                        wallet, sample.get("mint"), exc,
                    )
            for wallet, reason in cooldowns:
                cooldown_and_replace(wallet, reason)
