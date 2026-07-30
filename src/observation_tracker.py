"""Approved-signal observation ledger without reserving paper cash."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
from dotenv import load_dotenv

from src.logging_utils import redact_sensitive_text
from src.state_store import read_json, update_json

logger = logging.getLogger("signal-observer")

OBSERVATION_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "signal_observations.json"
)
OBSERVATION_INTERVALS = (("1m", 60), ("5m", 300), ("15m", 900))
MAX_OBSERVATIONS = 1_000


def empty_observations() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "observations": [],
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "version": 0,
    }


def observation_mode_enabled() -> bool:
    raw = os.getenv("OBSERVATION_MODE", "false").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError("OBSERVATION_MODE must be true or false")


async def record_observation(
    *,
    mint: str,
    route_type: str,
    source_wallet: str,
    source_signature: str,
    safety_score: int,
    entry_cost_lamports: int,
    token_amount_raw: int,
    token_decimals: int,
    entry_price_impact_pct: float,
    exit_price_impact_pct: float,
    expected_slippage_bps: int,
    dex_momentum_score: float,
    signal_detected_at: str,
    analysis_completed_at: str,
    entry_quote_at: str,
    entry_latency_ms: int,
) -> bool:
    """Record one approved hypothetical entry without changing trading state."""
    if entry_cost_lamports <= 0 or token_amount_raw <= 0:
        raise ValueError("observation quote amounts must be positive")
    observation_id = f"{source_signature}:{mint}"
    started_at = time.time()

    def mutate(document: dict[str, Any]) -> bool:
        rows = document.setdefault("observations", [])
        if not isinstance(rows, list):
            raise RuntimeError("signal observation ledger is malformed")
        if any(
            isinstance(row, dict) and row.get("observation_id") == observation_id
            for row in rows
        ):
            return False
        rows.append({
            "observation_id": observation_id,
            "mint": mint,
            "route_type": route_type,
            "source_wallet": source_wallet,
            "source_signature": source_signature,
            "safety_score": int(safety_score),
            "entry_cost_lamports": int(entry_cost_lamports),
            "token_amount_raw": int(token_amount_raw),
            "token_decimals": int(token_decimals),
            "entry_price_impact_pct": float(entry_price_impact_pct),
            "exit_price_impact_pct": float(exit_price_impact_pct),
            "expected_slippage_bps": int(expected_slippage_bps),
            "dex_momentum_score": float(dex_momentum_score),
            "signal_detected_at": signal_detected_at,
            "analysis_completed_at": analysis_completed_at,
            "entry_quote_at": entry_quote_at,
            "entry_latency_ms": int(entry_latency_ms),
            "started_at_epoch": started_at,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "samples": [],
            "status": "PENDING",
        })
        document["observations"] = rows[-MAX_OBSERVATIONS:]
        document["updated_at"] = datetime.now(timezone.utc).isoformat()
        return True

    created, _ = await asyncio.to_thread(
        update_json, OBSERVATION_PATH, empty_observations(), mutate
    )
    return bool(created)


def due_observation_samples(now: float) -> list[tuple[str, str, str, int, int]]:
    """Return at most one due interval per observation to bound quote traffic."""
    document = read_json(OBSERVATION_PATH, empty_observations())
    due: list[tuple[str, str, str, int, int]] = []
    for row in document.get("observations", []):
        if not isinstance(row, dict) or row.get("status") == "COMPLETE":
            continue
        completed = {
            str(sample.get("interval"))
            for sample in row.get("samples", [])
            if isinstance(sample, dict)
        }
        started_at = float(row.get("started_at_epoch", 0) or 0)
        for label, delay in OBSERVATION_INTERVALS:
            if label not in completed and now >= started_at + delay:
                due.append((
                    str(row.get("observation_id", "")),
                    label,
                    str(row.get("mint", "")),
                    int(row.get("token_amount_raw", 0) or 0),
                    int(row.get("entry_cost_lamports", 0) or 0),
                ))
                break
    return due


def record_sample(
    observation_id: str,
    interval: str,
    *,
    proceeds_lamports: int | None,
    error: str | None = None,
) -> bool:
    """Append one idempotent interval result and complete after 15 minutes."""
    allowed = {label for label, _ in OBSERVATION_INTERVALS}
    if interval not in allowed:
        raise ValueError("unsupported observation interval")

    def mutate(document: dict[str, Any]) -> bool:
        rows = document.get("observations", [])
        target = next((
            row for row in rows
            if isinstance(row, dict)
            and row.get("observation_id") == observation_id
        ), None)
        if not isinstance(target, dict):
            return False
        samples = target.setdefault("samples", [])
        if any(
            isinstance(sample, dict) and sample.get("interval") == interval
            for sample in samples
        ):
            return False
        entry_cost = int(target.get("entry_cost_lamports", 0) or 0)
        return_percent = (
            round((int(proceeds_lamports) / entry_cost - 1) * 100, 4)
            if proceeds_lamports is not None and entry_cost > 0
            else None
        )
        samples.append({
            "interval": interval,
            "proceeds_lamports": proceeds_lamports,
            "return_percent": return_percent,
            "error": error[:500] if error else None,
            "sampled_at": datetime.now(timezone.utc).isoformat(),
        })
        if interval == "15m":
            target["status"] = "COMPLETE"
            valid_returns = [
                float(sample["return_percent"])
                for sample in samples
                if isinstance(sample, dict)
                and sample.get("return_percent") is not None
            ]
            target["max_return_percent"] = (
                max(valid_returns) if valid_returns else None
            )
            target["min_return_percent"] = (
                min(valid_returns) if valid_returns else None
            )
        document["updated_at"] = datetime.now(timezone.utc).isoformat()
        return True

    recorded, _ = update_json(
        OBSERVATION_PATH, empty_observations(), mutate
    )
    return bool(recorded)


async def observation_loop(interval_seconds: float = 15.0) -> None:
    """Sample executable Jupiter exit values at 1, 5, and 15 minutes."""
    load_dotenv()
    api_key = os.getenv("JUPITER_API_KEY", "").strip()
    if not api_key:
        logger.warning("signal observer disabled: JUPITER_API_KEY is missing")
        return
    from src.executor import JupiterNoRouteError, WSOL_MINT, jupiter_quote

    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            await asyncio.sleep(interval_seconds)
            due = await asyncio.to_thread(due_observation_samples, time.time())
            for observation_id, label, mint, amount, _ in due:
                try:
                    quote = await jupiter_quote(
                        session,
                        api_key,
                        mint,
                        WSOL_MINT,
                        amount,
                        fail_fast_bad_request=True,
                    )
                    await asyncio.to_thread(
                        record_sample,
                        observation_id,
                        label,
                        proceeds_lamports=int(quote["outAmount"]),
                    )
                except JupiterNoRouteError as exc:
                    await asyncio.to_thread(
                        record_sample,
                        observation_id,
                        label,
                        proceeds_lamports=None,
                        error=redact_sensitive_text(exc),
                    )
                except Exception as exc:
                    logger.warning(
                        "observation sample retry scheduled: mint=%s interval=%s "
                        "error=%s",
                        mint,
                        label,
                        redact_sensitive_text(exc),
                    )
