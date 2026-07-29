"""Versioned paper-trading ledger and asynchronous risk loop."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import aiohttp

from src.executor import (
    MAX_ENTRY_PRICE_IMPACT_PCT,
    ExecutionSettings,
    WSOL_MINT,
    execute_sell,
    jupiter_quote,
)
from src.logging_utils import configure_safe_logging, redact_sensitive_text
from src.state_store import (
    migrate_json,
    normalized_route_metadata,
    read_json,
    update_json,
)

logger = logging.getLogger("risk-manager")

LEDGER_PATH = Path(__file__).resolve().parents[1] / "data" / "paper_trades.json"
INITIAL_PAPER_LAMPORTS = 10_000_000_000
TAKE_PROFIT_RATIO = 1.30
SECOND_TAKE_PROFIT_RATIO = 2.00
STOP_LOSS_RATIO = 0.85
ROUTE_B_TAKE_PROFIT_RATIO = 1.30
ROUTE_B_STOP_LOSS_RATIO = 0.90
BREAK_EVEN_STOP_RATIO = 1.00
TAKE_PROFIT_SELL_PERCENT = 80
PRICE_POLL_SECONDS = 1.0
QUOTE_FAILURE_WARNING_COUNT = 3
DEGRADED_QUOTE_GAP_SECONDS = 10.0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _legacy_break_even_required_value(
    position: dict[str, Any],
    token_amount_raw: int | None = None,
) -> int:
    """구버전 포지션은 최초 매수가를 기준으로 필요한 매도대금을 계산한다."""
    amount = int(
        position.get("token_amount_raw", 0)
        if token_amount_raw is None
        else token_amount_raw
    )
    entry_amount = int(
        position.get("entry_token_amount_raw", amount) or amount
    )
    entry_cost = int(
        position.get(
            "entry_cost_lamports",
            position.get("remaining_cost_lamports", 0),
        )
        or 0
    )
    if amount <= 0 or entry_amount <= 0 or entry_cost <= 0:
        return 0
    return (entry_cost * amount + entry_amount - 1) // entry_amount


def break_even_required_value(
    position: dict[str, Any],
    token_amount_raw: int | None = None,
) -> int:
    """동적 floor가 있으면 사용하고, 없으면 최초 매수가로 하위 호환한다."""
    if "break_even_required_proceeds_lamports" in position:
        return max(
            0,
            int(position.get("break_even_required_proceeds_lamports", 0) or 0),
        )
    return _legacy_break_even_required_value(position, token_amount_raw)


def empty_ledger() -> dict[str, Any]:
    return {
        "schema_version": 2,
        "version": 0,
        "next_event_seq": 1,
        "cash_lamports": INITIAL_PAPER_LAMPORTS,
        "positions": {},
        "events": [],
        "updated_at": utc_now(),
    }


def _next_event(document: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    sequence = int(document.get("next_event_seq", 1) or 1)
    document["next_event_seq"] = sequence + 1
    return {"event_id": str(uuid4()), "event_seq": sequence, **event}


def _normalize_position(
    position: dict[str, Any], mint: str, position_id: str, round_index: int
) -> None:
    opened_at = str(position.get("opened_at") or position.get("signal_detected_at") or utc_now())
    amount = int(position.get("token_amount_raw", 0) or 0)
    cost = int(position.get("remaining_cost_lamports", 0) or 0)
    position.update({
        "position_id": position_id,
        "round_index": int(position.get("round_index", round_index) or round_index),
        "mint": mint,
        "signal_detected_at": str(position.get("signal_detected_at") or opened_at),
        "entry_quote_at": str(position.get("entry_quote_at") or opened_at),
        "entry_cost_lamports": int(position.get("entry_cost_lamports", cost) or cost),
        "entry_token_amount_raw": int(position.get("entry_token_amount_raw", amount) or amount),
        "entry_price_impact_pct": float(position.get("entry_price_impact_pct", 0) or 0),
        "expected_slippage_bps": int(position.get("expected_slippage_bps", 100) or 100),
        "whale_reference_price": float(position.get("whale_reference_price", 0) or 0),
        "copy_price_gap_pct": float(position.get("copy_price_gap_pct", 0) or 0),
        "entry_latency_ms": int(position.get("entry_latency_ms", 0) or 0),
        "analysis_completed_at": str(position.get("analysis_completed_at") or opened_at),
        "peak_exit_value_lamports": int(
            position.get("peak_exit_value_lamports", position.get("current_value_lamports", 0)) or 0
        ),
        "last_quote_success_at": str(position.get("last_quote_success_at") or opened_at),
        "consecutive_quote_failures": int(position.get("consecutive_quote_failures", 0) or 0),
        "risk_state": str(position.get("risk_state") or "NORMAL"),
        "route_type": str(position.get("route_type") or "A"),
        "dex_momentum_score": float(position.get("dex_momentum_score", 0) or 0),
        "take_profit_done": bool(position.get("take_profit_done", False)),
        "second_take_profit_done": bool(position.get("second_take_profit_done", False)),
        "stop_loss_ratio": float(
            position.get(
                "stop_loss_ratio",
                BREAK_EVEN_STOP_RATIO
                if position.get("take_profit_done")
                else STOP_LOSS_RATIO,
            )
        ),
        "break_even_floor_active": bool(
            position.get("break_even_floor_active", position.get("take_profit_done", False))
        ),
        "break_even_price": float(
            position.get(
                "break_even_price",
                (
                    int(position.get("entry_cost_lamports", cost) or cost)
                    / int(position.get("entry_token_amount_raw", amount) or amount)
                    if int(position.get("entry_token_amount_raw", amount) or amount) > 0
                    else 0.0
                ),
            )
            or 0.0
        ),
        "break_even_required_proceeds_lamports": int(
            position.get(
                "break_even_required_proceeds_lamports",
                _legacy_break_even_required_value(position, amount),
            )
            or 0
        ),
        "cumulative_proceeds_lamports": int(
            position.get("cumulative_proceeds_lamports", 0) or 0
        ),
        "post_tp_peak_exit_value_lamports": int(
            position.get("post_tp_peak_exit_value_lamports", 0) or 0
        ),
        "post_tp_trailing_stop_ratio": float(
            position.get("post_tp_trailing_stop_ratio", 0.50) or 0.50
        ),
        "version": int(position.get("version", 1) or 1),
    })


def migrate_ledger_document(document: dict[str, Any]) -> bool:
    """Idempotently assign v2 event and position identities to legacy data."""
    if int(document.get("schema_version", 1) or 1) >= 2:
        changed = False
        for position in document.get("positions", {}).values():
            if not isinstance(position, dict):
                continue
            if "route_type" not in position:
                position["route_type"] = "A"
                changed = True
            if "dex_momentum_score" not in position:
                position["dex_momentum_score"] = 0.0
                changed = True
            if "break_even_price" not in position:
                entry_amount = int(
                    position.get(
                        "entry_token_amount_raw",
                        position.get("token_amount_raw", 0),
                    )
                    or 0
                )
                entry_cost = int(
                    position.get(
                        "entry_cost_lamports",
                        position.get("remaining_cost_lamports", 0),
                    )
                    or 0
                )
                position["break_even_price"] = (
                    entry_cost / entry_amount if entry_amount > 0 else 0.0
                )
                changed = True
            if "break_even_required_proceeds_lamports" not in position:
                position["break_even_required_proceeds_lamports"] = (
                    _legacy_break_even_required_value(position)
                )
                changed = True
            if "cumulative_proceeds_lamports" not in position:
                position["cumulative_proceeds_lamports"] = 0
                changed = True
            if "post_tp_peak_exit_value_lamports" not in position:
                position["post_tp_peak_exit_value_lamports"] = 0
                changed = True
            if "post_tp_trailing_stop_ratio" not in position:
                position["post_tp_trailing_stop_ratio"] = 0.50
                changed = True
        for event in document.get("events", []):
            if not isinstance(event, dict) or event.get("type") != "BUY":
                continue
            if "route_type" not in event:
                event["route_type"] = "A"
                changed = True
            if "dex_momentum_score" not in event:
                event["dex_momentum_score"] = 0.0
                changed = True
        if changed:
            document["updated_at"] = utc_now()
        return changed
    document.setdefault("cash_lamports", INITIAL_PAPER_LAMPORTS)
    document.setdefault("positions", {})
    document.setdefault("events", [])
    open_rounds: dict[str, str] = {}
    round_counts: dict[str, int] = {}
    sequence = 1
    for event in document["events"]:
        if not isinstance(event, dict):
            continue
        mint = str(event.get("mint", ""))
        event_type = str(event.get("type", ""))
        if event_type == "BUY":
            round_counts[mint] = round_counts.get(mint, 0) + 1
            open_rounds[mint] = str(uuid4())
        position_id = str(event.get("position_id") or open_rounds.get(mint) or uuid4())
        event.setdefault("position_id", position_id)
        event.setdefault("event_id", str(uuid4()))
        event.setdefault("event_seq", sequence)
        sequence = max(sequence + 1, int(event["event_seq"]) + 1)
        if event_type == "SELL":
            # Legacy state has no remaining amount per event. A later BUY naturally
            # replaces the open round; current positions below retain the final ID.
            event.setdefault("trigger_roi_percent", None)
            proceeds = int(event.get("proceeds_lamports", 0) or 0)
            pnl = int(event.get("realized_pnl_lamports", 0) or 0)
            released_cost = proceeds - pnl
            event.setdefault(
                "realized_roi_percent",
                pnl / released_cost * 100 if released_cost > 0 else 0.0,
            )
            event.setdefault("quote_age_ms", None)
    for mint, position in document["positions"].items():
        if not isinstance(position, dict):
            continue
        round_counts[mint] = max(1, round_counts.get(mint, 0))
        position_id = str(
            position.get("position_id") or open_rounds.get(mint) or uuid4()
        )
        _normalize_position(position, mint, position_id, round_counts[mint])
    document["next_event_seq"] = sequence
    document["schema_version"] = 2
    document["updated_at"] = utc_now()
    return True


def ensure_ledger_migrated() -> dict[str, Any]:
    return migrate_json(LEDGER_PATH, empty_ledger(), migrate_ledger_document)


def read_ledger() -> dict[str, Any]:
    document = read_json(LEDGER_PATH, empty_ledger())
    if int(document.get("schema_version", 1) or 1) < 2:
        document = ensure_ledger_migrated()
    document.setdefault("positions", {})
    document.setdefault("events", [])
    return document


async def paper_cash_balance() -> int:
    return int(read_ledger()["cash_lamports"])


async def record_paper_buy(
    mint: str,
    cost_lamports: int,
    token_amount_raw: int,
    token_decimals: int = 0,
    *,
    source_wallet: str | None = None,
    source_signature: str | None = None,
    safety_score: int | None = None,
    entry_reason: str = "analyzer_approved",
    signal_detected_at: str | None = None,
    analysis_completed_at: str | None = None,
    entry_quote_at: str | None = None,
    entry_price_impact_pct: float = 0.0,
    exit_price_impact_pct: float = 0.0,
    expected_slippage_bps: int = 100,
    whale_reference_price: float = 0.0,
    copy_price_gap_pct: float = 0.0,
    entry_latency_ms: int = 0,
    route_type: str = "A",
    dex_momentum_score: float = 0.0,
) -> str:
    if cost_lamports <= 0 or token_amount_raw <= 0:
        raise ValueError("paper buy amounts must be positive")
    if (
        entry_price_impact_pct < 0
        or entry_price_impact_pct > MAX_ENTRY_PRICE_IMPACT_PCT
    ):
        raise RuntimeError(
            f"[FILTER] Entry price impact {entry_price_impact_pct:.4f}% exceeds "
            f"{MAX_ENTRY_PRICE_IMPACT_PCT:.2f}%. Position not created."
        )
    ensure_ledger_migrated()
    route_metadata = normalized_route_metadata(route_type, dex_momentum_score)

    def mutate(ledger: dict[str, Any]) -> str:
        if mint in ledger.setdefault("positions", {}):
            raise RuntimeError(f"paper position already exists for {mint}")
        if source_signature and any(
            event.get("type") == "BUY"
            and event.get("source_signature") == source_signature
            and event.get("mint") == mint
            for event in ledger.setdefault("events", [])
            if isinstance(event, dict)
        ):
            raise RuntimeError(f"paper buy already recorded for {source_signature}")
        if int(ledger.get("cash_lamports", 0)) < cost_lamports:
            raise RuntimeError("paper account has insufficient virtual SOL")
        previous_rounds = [
            int(event.get("round_index", 0) or 0)
            for event in ledger["events"]
            if isinstance(event, dict)
            and event.get("type") == "BUY"
            and event.get("mint") == mint
        ]
        round_index = max(previous_rounds, default=0) + 1
        position_id = str(uuid4())
        opened_at = utc_now()
        signal_at = signal_detected_at or opened_at
        quote_at = entry_quote_at or opened_at
        metadata = {
            "position_id": position_id,
            "round_index": round_index,
            "source_wallet": source_wallet,
            "source_signature": source_signature,
            "safety_score": safety_score,
            "entry_reason": entry_reason,
            "signal_detected_at": signal_at,
            "analysis_completed_at": analysis_completed_at or quote_at,
            "entry_quote_at": quote_at,
            "entry_price_impact_pct": float(entry_price_impact_pct),
            "exit_price_impact_pct": float(exit_price_impact_pct),
            "expected_slippage_bps": int(expected_slippage_bps),
            "whale_reference_price": float(whale_reference_price),
            "copy_price_gap_pct": float(copy_price_gap_pct),
            "entry_latency_ms": int(entry_latency_ms),
            **route_metadata,
        }
        ledger["cash_lamports"] = int(ledger["cash_lamports"]) - cost_lamports
        ledger["positions"][mint] = {
            "mint": mint,
            "token_amount_raw": token_amount_raw,
            "token_decimals": max(0, token_decimals),
            "remaining_cost_lamports": cost_lamports,
            "entry_cost_lamports": cost_lamports,
            "entry_token_amount_raw": token_amount_raw,
            "opened_at": opened_at,
            "peak_exit_value_lamports": cost_lamports,
            "last_quote_success_at": quote_at,
            "consecutive_quote_failures": 0,
            "risk_state": "NORMAL",
            "take_profit_done": False,
            "second_take_profit_done": False,
            "stop_loss_ratio": (
                ROUTE_B_STOP_LOSS_RATIO
                if route_metadata["route_type"] == "B"
                else STOP_LOSS_RATIO
            ),
            "break_even_floor_active": False,
            "break_even_price": cost_lamports / token_amount_raw,
            "break_even_required_proceeds_lamports": cost_lamports,
            "cumulative_proceeds_lamports": 0,
            "post_tp_peak_exit_value_lamports": 0,
            "post_tp_trailing_stop_ratio": 0.50,
            "version": 1,
            **metadata,
        }
        ledger["events"].append(_next_event(ledger, {
            "type": "BUY", "mint": mint, "cost_lamports": cost_lamports,
            "token_amount_raw": token_amount_raw,
            "token_decimals": max(0, token_decimals), "at": opened_at, **metadata,
        }))
        ledger["updated_at"] = utc_now()
        return position_id

    position_id, _ = update_json(LEDGER_PATH, empty_ledger(), mutate)
    return position_id


async def record_paper_rejection(
    mint: str, score: int, reasons: list[str], wallet: str, signature: str
) -> None:
    ensure_ledger_migrated()

    def mutate(ledger: dict[str, Any]) -> None:
        if any(
            event.get("type") == "SIGNAL_REJECTED"
            and event.get("signature") == signature
            and event.get("mint") == mint
            for event in ledger.setdefault("events", [])
            if isinstance(event, dict)
        ):
            return
        ledger["events"].append(_next_event(ledger, {
            "type": "SIGNAL_REJECTED", "mint": mint, "safety_score": score,
            "reason": "; ".join(reasons), "source_wallet": wallet,
            "signature": signature, "at": utc_now(),
        }))
        ledger["events"] = ledger["events"][-2_000:]
        ledger["updated_at"] = utc_now()

    update_json(LEDGER_PATH, empty_ledger(), mutate)


async def record_rpc_skip(
    mint: str, wallet: str, signature: str, reason: str
) -> None:
    """Persist a fail-safe skip without reserving paper cash or a position."""
    ensure_ledger_migrated()

    def mutate(ledger: dict[str, Any]) -> None:
        if any(
            isinstance(event, dict)
            and event.get("type") == "SKIPPED_BY_RPC_ERROR"
            and event.get("signature") == signature
            and event.get("mint") == mint
            for event in ledger.setdefault("events", [])
        ):
            return
        ledger["events"].append(_next_event(ledger, {
            "type": "SKIPPED_BY_RPC_ERROR",
            "mint": mint,
            "source_wallet": wallet,
            "signature": signature,
            "reason": reason[:500],
            "at": utc_now(),
        }))
        ledger["events"] = ledger["events"][-2_000:]
        ledger["updated_at"] = utc_now()

    update_json(LEDGER_PATH, empty_ledger(), mutate)


async def claim_position_exit(
    mint: str, position_id: str, reason: str
) -> tuple[str, int] | None:
    """Claim the one allowed exit action for a position across all processes."""
    exit_id = str(uuid4())

    def mutate(ledger: dict[str, Any]) -> tuple[str, int] | None:
        position = ledger.setdefault("positions", {}).get(mint)
        if (
            not isinstance(position, dict)
            or position.get("position_id") != position_id
            or position.get("risk_state") == "EXIT_PENDING"
        ):
            return None
        position["risk_state"] = "EXIT_PENDING"
        position["pending_exit_id"] = exit_id
        position["pending_exit_reason"] = reason
        position["version"] = int(position.get("version", 0)) + 1
        ledger["updated_at"] = utc_now()
        return exit_id, int(position.get("token_amount_raw", 0))

    result, _ = update_json(LEDGER_PATH, empty_ledger(), mutate)
    return result


async def release_exit_claim(
    mint: str, position_id: str, exit_id: str, *, degraded: bool = False
) -> None:
    def mutate(ledger: dict[str, Any]) -> None:
        position = ledger.setdefault("positions", {}).get(mint)
        if (
            isinstance(position, dict)
            and position.get("position_id") == position_id
            and position.get("pending_exit_id") == exit_id
        ):
            position.pop("pending_exit_id", None)
            position.pop("pending_exit_reason", None)
            position["risk_state"] = "DEGRADED" if degraded else "NORMAL"
            position["version"] = int(position.get("version", 0)) + 1
            ledger["updated_at"] = utc_now()

    update_json(LEDGER_PATH, empty_ledger(), mutate)


async def record_paper_sell(
    mint: str,
    token_amount_raw: int,
    proceeds_lamports: int,
    reason: str,
    *,
    position_id: str | None = None,
    exit_id: str | None = None,
    trigger_roi_percent: float | None = None,
    quote_age_ms: int | None = None,
    exit_trigger_latency_ms: int | None = None,
    previous_value_lamports: int | None = None,
    previous_quote_at: str | None = None,
    trigger_peak_value_lamports: int | None = None,
    trigger_price_impact_pct: float | None = None,
) -> bool:
    ensure_ledger_migrated()

    def mutate(ledger: dict[str, Any]) -> bool:
        position = ledger.setdefault("positions", {}).get(mint)
        if not isinstance(position, dict):
            return False
        actual_position_id = str(position.get("position_id", ""))
        if position_id and actual_position_id != position_id:
            return False
        if exit_id and position.get("pending_exit_id") != exit_id:
            return False
        previous_amount = int(position.get("token_amount_raw", 0))
        sold = min(token_amount_raw, previous_amount)
        if sold <= 0:
            return False
        cost_released = int(position["remaining_cost_lamports"]) * sold // previous_amount
        realized_pnl = proceeds_lamports - cost_released
        realized_roi = realized_pnl / cost_released * 100 if cost_released > 0 else 0.0
        position["token_amount_raw"] = previous_amount - sold
        position["remaining_cost_lamports"] -= cost_released
        cumulative_proceeds = (
            int(position.get("cumulative_proceeds_lamports", 0) or 0)
            + proceeds_lamports
        )
        position["cumulative_proceeds_lamports"] = cumulative_proceeds
        position.pop("pending_exit_id", None)
        position.pop("pending_exit_reason", None)
        position["risk_state"] = "NORMAL"
        position["version"] = int(position.get("version", 0)) + 1
        ledger["cash_lamports"] = int(ledger.get("cash_lamports", 0)) + proceeds_lamports
        if reason in {
            "TAKE_PROFIT_50",
            "LIVE_TAKE_PROFIT_50",
            "ROUTE_B_TAKE_PROFIT_30",
            "TAKE_PROFIT_30_SELL_80",
        }:
            position["take_profit_done"] = True
            position["break_even_floor_active"] = False
            remaining_amount = int(position.get("token_amount_raw", 0) or 0)
            entry_cost = int(position.get("entry_cost_lamports", 0) or 0)
            required_proceeds = max(0, entry_cost - cumulative_proceeds)
            position["break_even_required_proceeds_lamports"] = required_proceeds
            position["break_even_price"] = (
                required_proceeds / remaining_amount
                if remaining_amount > 0
                else 0.0
            )
            position["post_tp_peak_exit_value_lamports"] = 0
        elif reason in {"TAKE_PROFIT_100", "LIVE_TAKE_PROFIT_100"}:
            position["second_take_profit_done"] = True
        ledger.setdefault("events", []).append(_next_event(ledger, {
            "type": "SELL", "reason": reason, "mint": mint,
            "position_id": actual_position_id,
            "round_index": int(position.get("round_index", 1)),
            "token_amount_raw": sold, "proceeds_lamports": proceeds_lamports,
            "realized_pnl_lamports": realized_pnl,
            "trigger_roi_percent": trigger_roi_percent,
            "realized_roi_percent": realized_roi,
            "quote_age_ms": quote_age_ms,
            "exit_trigger_latency_ms": exit_trigger_latency_ms,
            "previous_value_lamports": previous_value_lamports,
            "previous_quote_at": previous_quote_at,
            "trigger_peak_value_lamports": trigger_peak_value_lamports,
            "trigger_price_impact_pct": trigger_price_impact_pct,
            "at": utc_now(),
        }))
        if position["token_amount_raw"] <= 0:
            del ledger["positions"][mint]
        ledger["updated_at"] = utc_now()
        return True

    recorded, _ = update_json(LEDGER_PATH, empty_ledger(), mutate)
    return bool(recorded)


async def record_position_mark(
    mint: str, position_id: str, current_value_lamports: int
) -> None:
    def mutate(ledger: dict[str, Any]) -> None:
        position = ledger.setdefault("positions", {}).get(mint)
        if (
            not isinstance(position, dict)
            or position.get("position_id") != position_id
            or current_value_lamports < 0
        ):
            return
        amount = int(position.get("token_amount_raw", 0))
        cost = int(position.get("remaining_cost_lamports", 0))
        now = utc_now()
        position["current_value_lamports"] = current_value_lamports
        position["current_price_lamports_per_raw"] = (
            current_value_lamports / amount if amount > 0 else 0
        )
        position["unrealized_return_percent"] = (
            (current_value_lamports / cost - 1) * 100 if cost > 0 else 0
        )
        position["peak_exit_value_lamports"] = max(
            int(position.get("peak_exit_value_lamports", 0)), current_value_lamports
        )
        if position.get("take_profit_done"):
            position["post_tp_peak_exit_value_lamports"] = max(
                int(position.get("post_tp_peak_exit_value_lamports", 0) or 0),
                current_value_lamports,
            )
        position["price_updated_at"] = now
        position["last_quote_success_at"] = now
        position["consecutive_quote_failures"] = 0
        if position.get("risk_state") != "EXIT_PENDING":
            position["risk_state"] = "NORMAL"
        position["version"] = int(position.get("version", 0)) + 1
        ledger["updated_at"] = now

    update_json(LEDGER_PATH, empty_ledger(), mutate)


async def record_live_transaction_lifecycle(
    mint: str, position_id: str, lifecycle: tuple[dict[str, Any], ...]
) -> None:
    """Persist executor lifecycle states on both the position and event ledger."""
    if not lifecycle:
        return

    def mutate(ledger: dict[str, Any]) -> None:
        position = ledger.setdefault("positions", {}).get(mint)
        if not isinstance(position, dict) or position.get("position_id") != position_id:
            return
        for lifecycle_item in lifecycle:
            item = dict(lifecycle_item)
            status = str(item.get("status", "UNKNOWN"))
            position["transaction_lifecycle_status"] = status
            position["transaction_lifecycle_updated_at"] = item.get("at") or utc_now()
            ledger.setdefault("events", []).append(_next_event(ledger, {
                "type": "TRANSACTION_LIFECYCLE",
                "mint": mint,
                "position_id": position_id,
                **item,
            }))
        position["version"] = int(position.get("version", 0)) + 1
        ledger["updated_at"] = utc_now()

    update_json(LEDGER_PATH, empty_ledger(), mutate)


async def ensure_break_even_floor(mint: str, position_id: str) -> bool:
    """Persist the break-even stop for positions that cleared their first TP."""
    def mutate(ledger: dict[str, Any]) -> bool:
        position = ledger.setdefault("positions", {}).get(mint)
        if (
            not isinstance(position, dict)
            or position.get("position_id") != position_id
            or not position.get("take_profit_done")
        ):
            return False
        already_armed = (
            position.get("break_even_floor_active") is True
            and float(position.get("stop_loss_ratio", 0) or 0)
            >= BREAK_EVEN_STOP_RATIO
        )
        if already_armed:
            return False
        position["stop_loss_ratio"] = BREAK_EVEN_STOP_RATIO
        position["break_even_floor_active"] = True
        position["break_even_floor_armed_at"] = utc_now()
        position["version"] = int(position.get("version", 0)) + 1
        ledger["updated_at"] = utc_now()
        return True

    armed, _ = update_json(LEDGER_PATH, empty_ledger(), mutate)
    return bool(armed)


async def record_quote_failure(mint: str, position_id: str, error: Exception) -> None:
    def mutate(ledger: dict[str, Any]) -> tuple[int, str] | None:
        position = ledger.setdefault("positions", {}).get(mint)
        if not isinstance(position, dict) or position.get("position_id") != position_id:
            return None
        failures = int(position.get("consecutive_quote_failures", 0)) + 1
        position["consecutive_quote_failures"] = failures
        position["last_quote_error"] = redact_sensitive_text(
            f"{type(error).__name__}: {error}"
        )[:500]
        position["last_quote_failure_at"] = utc_now()
        last_success = parse_time(position.get("last_quote_success_at"))
        quote_gap = (
            (datetime.now(timezone.utc) - last_success).total_seconds()
            if last_success else DEGRADED_QUOTE_GAP_SECONDS
        )
        if failures >= QUOTE_FAILURE_WARNING_COUNT or quote_gap >= DEGRADED_QUOTE_GAP_SECONDS:
            position["risk_state"] = (
                "NO_ROUTE" if "no executable route" in str(error).lower() else "DEGRADED"
            )
        position["version"] = int(position.get("version", 0)) + 1
        ledger["updated_at"] = utc_now()
        return failures, str(position.get("risk_state", "NORMAL"))

    result, _ = update_json(LEDGER_PATH, empty_ledger(), mutate)
    if result and result[0] >= QUOTE_FAILURE_WARNING_COUNT:
        logger.warning(
            "Jupiter quote degraded: mint=%s position_id=%s failures=%s state=%s",
            mint, position_id, result[0], result[1],
        )


async def close_paper_position(mint: str) -> int:
    settings = ExecutionSettings.from_env()
    if settings.trading_mode != "paper":
        raise RuntimeError("dashboard manual close is restricted to paper mode")
    if not settings.jupiter_api_key:
        raise RuntimeError("JUPITER_API_KEY is required for manual position close")
    position = read_ledger()["positions"].get(mint)
    if not isinstance(position, dict):
        raise RuntimeError("paper position no longer exists")
    position_id = str(position["position_id"])
    claim = await claim_position_exit(mint, position_id, "MANUAL_CLOSE")
    if not claim:
        raise RuntimeError("position exit is already pending")
    exit_id, amount = claim
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        started = datetime.now(timezone.utc)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            quote = await jupiter_quote(session, settings.jupiter_api_key, mint, WSOL_MINT, amount)
        proceeds = int(quote["outAmount"])
        recorded = await record_paper_sell(
            mint, amount, proceeds, "MANUAL_CLOSE",
            position_id=position_id, exit_id=exit_id,
            quote_age_ms=int((datetime.now(timezone.utc) - started).total_seconds() * 1000),
        )
        if not recorded:
            raise RuntimeError("position changed before manual close was committed")
        logger.info("paper position manually closed: mint=%s position_id=%s", mint, position_id)
        return proceeds
    except Exception as exc:
        await release_exit_claim(mint, position_id, exit_id, degraded=True)
        await record_quote_failure(mint, position_id, exc)
        raise


async def _execute_paper_exit(
    session: aiohttp.ClientSession,
    api_key: str,
    position: dict[str, Any],
    reason: str,
    trigger_roi: float,
    *,
    detected_quote: dict[str, Any] | None = None,
    detected_amount: int | None = None,
    trigger_started: float | None = None,
) -> bool:
    mint = str(position["mint"])
    position_id = str(position["position_id"])
    claim = await claim_position_exit(mint, position_id, reason)
    if not claim:
        return False
    exit_id, current_amount = claim
    amount = (
        current_amount
        if reason in {
            "STOP_LOSS_15",
            "ROUTE_B_STOP_LOSS_10",
            "TP_BREAK_EVEN",
            "POST_TP_TRAILING_STOP_50",
        }
        else current_amount * TAKE_PROFIT_SELL_PERCENT // 100
    )
    if amount <= 0:
        await release_exit_claim(mint, position_id, exit_id)
        return False
    if detected_quote is not None and detected_amount != amount:
        await release_exit_claim(mint, position_id, exit_id)
        logger.warning(
            "paper exit quote discarded after position changed: mint=%s "
            "position_id=%s detected_amount=%s current_amount=%s",
            mint,
            position_id,
            detected_amount,
            amount,
        )
        return False
    started = datetime.now(timezone.utc)
    try:
        quote = detected_quote
        if quote is None:
            quote = await jupiter_quote(session, api_key, mint, WSOL_MINT, amount)
        latency_ms = (
            int(
                (asyncio.get_running_loop().time() - trigger_started)
                * 1000
            )
            if trigger_started is not None
            else None
        )
        recorded = await record_paper_sell(
            mint, amount, int(quote["outAmount"]), reason,
            position_id=position_id, exit_id=exit_id,
            trigger_roi_percent=trigger_roi,
            quote_age_ms=int((datetime.now(timezone.utc) - started).total_seconds() * 1000),
            exit_trigger_latency_ms=latency_ms,
            previous_value_lamports=int(
                position.get("current_value_lamports", 0) or 0
            ),
            previous_quote_at=str(position.get("price_updated_at") or ""),
            trigger_peak_value_lamports=int(
                position.get("post_tp_peak_exit_value_lamports", 0) or 0
            ),
            trigger_price_impact_pct=float(quote.get("priceImpactPct", 0) or 0),
        )
        if recorded and detected_quote is not None:
            logger.warning(
                "paper urgent exit dispatched from detection quote: mint=%s "
                "reason=%s latency_ms=%s slippage_bps=%s",
                mint,
                reason,
                latency_ms,
                quote.get("slippageBps", 100),
            )
        return recorded
    except Exception as exc:
        await release_exit_claim(mint, position_id, exit_id, degraded=True)
        await record_quote_failure(mint, position_id, exc)
        raise


async def evaluate_paper_position(
    session: aiohttp.ClientSession, api_key: str, position: dict[str, Any]
) -> None:
    mint = str(position["mint"])
    position_id = str(position["position_id"])
    amount = int(position["token_amount_raw"])
    cost = int(position["remaining_cost_lamports"])
    if amount <= 0 or cost <= 0 or position.get("risk_state") == "EXIT_PENDING":
        return
    try:
        quote = await jupiter_quote(
            session,
            api_key,
            mint,
            WSOL_MINT,
            amount,
            slippage_bps=1_000,
        )
    except Exception as exc:
        await record_quote_failure(mint, position_id, exc)
        raise
    current_value = int(quote["outAmount"])
    ratio = current_value / cost
    trigger_roi = (ratio - 1) * 100
    await record_position_mark(mint, position_id, current_value)
    route_type = str(position.get("route_type") or "A")
    take_profit_done = bool(position.get("take_profit_done"))
    stop_loss_ratio = (
        ROUTE_B_STOP_LOSS_RATIO if route_type == "B" else STOP_LOSS_RATIO
    )
    trailing_peak = int(
        position.get("post_tp_peak_exit_value_lamports", 0) or 0
    )
    stop_triggered = (
        trailing_peak > 0
        and current_value * 2 <= trailing_peak
        if take_profit_done
        else ratio <= stop_loss_ratio
    )
    if stop_triggered:
        reason = (
            "POST_TP_TRAILING_STOP_50"
            if take_profit_done
            else ("ROUTE_B_STOP_LOSS_10" if route_type == "B" else "STOP_LOSS_15")
        )
        trigger_started = asyncio.get_running_loop().time()
        if await _execute_paper_exit(
            session,
            api_key,
            position,
            reason,
            trigger_roi,
            detected_quote=quote,
            detected_amount=amount,
            trigger_started=trigger_started,
        ):
            logger.warning(
                "paper stop-loss: mint=%s reason=%s return=%.2f%% floor=%.2f%%",
                mint,
                reason,
                trigger_roi,
                (
                    -50.0
                    if take_profit_done
                    else (stop_loss_ratio - 1) * 100
                ),
            )
    elif ratio >= TAKE_PROFIT_RATIO and not take_profit_done:
        if await _execute_paper_exit(
            session, api_key, position, "TAKE_PROFIT_30_SELL_80", trigger_roi
        ):
            logger.info(
                "paper take-profit: mint=%s route=%s return=%.2f%% sold=80%%",
                mint,
                route_type,
                trigger_roi,
            )


async def run_risk_loop(paper_trading: bool = True) -> None:
    settings = ExecutionSettings.from_env()
    ensure_ledger_migrated()
    if not settings.jupiter_api_key:
        logger.warning("risk loop disabled: JUPITER_API_KEY is missing")
        return
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            started = asyncio.get_running_loop().time()
            positions = list(read_ledger()["positions"].values())
            for position in positions:
                try:
                    if paper_trading:
                        await evaluate_paper_position(session, settings.jupiter_api_key, position)
                    else:
                        # Live execution retains the same cross-process claim. A landed
                        # signature is required before the local ledger is finalized.
                        mint = str(position["mint"])
                        amount = int(position["token_amount_raw"])
                        cost = int(position["remaining_cost_lamports"])
                        quote = await jupiter_quote(
                            session, settings.jupiter_api_key, mint, WSOL_MINT, amount
                        )
                        ratio = int(quote["outAmount"]) / cost
                        await record_position_mark(
                            mint, str(position["position_id"]), int(quote["outAmount"])
                        )
                        reason = None
                        percent = 0
                        if ratio <= STOP_LOSS_RATIO:
                            reason, percent = "LIVE_STOP_LOSS_15", 100
                        elif (
                            ratio >= SECOND_TAKE_PROFIT_RATIO
                            and position.get("take_profit_done")
                            and not position.get("second_take_profit_done")
                        ):
                            reason, percent = "LIVE_TAKE_PROFIT_100", 50
                        elif ratio >= TAKE_PROFIT_RATIO and not position.get("take_profit_done"):
                            reason, percent = "LIVE_TAKE_PROFIT_50", 50
                        if reason:
                            claim = await claim_position_exit(
                                mint, str(position["position_id"]), reason
                            )
                            if claim:
                                exit_id, claimed_amount = claim
                                try:
                                    sold = (
                                        claimed_amount if percent == 100
                                        else claimed_amount // 2
                                    )
                                    exit_quote = quote
                                    if percent == 50:
                                        exit_quote = await jupiter_quote(
                                            session, settings.jupiter_api_key,
                                            mint, WSOL_MINT, sold,
                                        )
                                    urgency = (
                                        "emergency"
                                        if reason in {"LIVE_STOP_LOSS_15", "MANUAL_CLOSE"}
                                        else "normal"
                                    )
                                    result = await execute_sell(
                                        mint, percent, settings, urgency=urgency
                                    )
                                    await record_live_transaction_lifecycle(
                                        mint,
                                        str(position["position_id"]),
                                        result.lifecycle,
                                    )
                                    if result.landed:
                                        await record_paper_sell(
                                            mint, sold, int(exit_quote["outAmount"]), reason,
                                            position_id=str(position["position_id"]),
                                            exit_id=exit_id,
                                            trigger_roi_percent=(ratio - 1) * 100,
                                        )
                                    else:
                                        await release_exit_claim(
                                            mint, str(position["position_id"]),
                                            exit_id, degraded=True,
                                        )
                                except Exception:
                                    await release_exit_claim(
                                        mint, str(position["position_id"]),
                                        exit_id, degraded=True,
                                    )
                                    raise
                except Exception:
                    logger.exception("risk evaluation failed for %s", position.get("mint"))
            elapsed = asyncio.get_running_loop().time() - started
            await asyncio.sleep(max(0.0, PRICE_POLL_SECONDS - elapsed))


def main() -> None:
    """Run the risk manager as a standalone PM2 service."""
    configure_safe_logging()
    asyncio.run(run_risk_loop(paper_trading=True))


if __name__ == "__main__":
    main()
