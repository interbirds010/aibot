"""Discover and validate smart-money wallets using Solana JSON-RPC only."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
from dotenv import load_dotenv
from src.logging_utils import configure_safe_logging, redact_sensitive_text
from solders.pubkey import Pubkey
from src.wallet_performance import (
    capped_return_percent,
    ensure_performance_migrated,
    restore_expired_cooldowns,
    wallet_is_cooling_down,
)

logger = logging.getLogger("wallet-feeder")
ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = ROOT / "data" / "wallets.json"
CANDIDATE_PATH = ROOT / "data" / "wallet_candidates.json"
PERFORMANCE_PATH = ROOT / "data" / "wallet_performance.json"

# High-volume mainnet DEX programs provide a bounded, recent signer sample.
DEX_PROGRAMS = (
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",  # Pump.fun
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",  # PumpSwap
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # Raydium AMM v4
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",  # Raydium CPMM
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",  # Raydium CLMM
)


@dataclass(frozen=True, slots=True)
class FeederSettings:
    rpc_url: str
    refresh_seconds: int = 3600
    max_wallets: int = 20
    min_sol_balance: float = 0.1
    max_daily_transactions: int = 300
    signatures_per_program: int = 50
    max_candidates: int = 250
    http_concurrency: int = 3
    rpc_min_interval_seconds: float = 0.3
    elite_reserved_slots: int = 7

    @classmethod
    def from_env(cls) -> "FeederSettings":
        load_dotenv()
        key = os.getenv("HELIUS_API_KEY", "").strip()
        url = os.getenv("HELIUS_RPC_HTTP_URL", "").strip().replace("${HELIUS_API_KEY}", key)
        if not key or not url:
            raise RuntimeError("HELIUS_API_KEY and HELIUS_RPC_HTTP_URL must be set")
        max_wallets = max(1, int(os.getenv("WALLET_MAX_WALLETS", "20")))
        return cls(
            rpc_url=url,
            refresh_seconds=max(1, int(os.getenv("WALLET_REFRESH_HOURS", "1"))) * 3600,
            max_wallets=max_wallets,
            min_sol_balance=max(0.0, float(os.getenv("WALLET_MIN_SOL_BALANCE", "0.1"))),
            max_daily_transactions=max(1, int(os.getenv("WALLET_MAX_DAILY_TX", "300"))),
            signatures_per_program=max(1, min(1000, int(os.getenv("WALLET_SIGNATURES_PER_PROGRAM", "50")))),
            max_candidates=max(1, int(os.getenv("WALLET_MAX_CANDIDATES", "250"))),
            rpc_min_interval_seconds=max(0.0, float(os.getenv("WALLET_RPC_MIN_INTERVAL_SECONDS", "0.3"))),
            elite_reserved_slots=min(
                max_wallets, max(1, int(os.getenv("WALLET_ELITE_RESERVED_SLOTS", "7")))
            ),
        )


@dataclass(frozen=True, slots=True)
class WalletScore:
    address: str
    balance_sol: float
    daily_transactions: int
    wins: int = 0
    losses: int = 0
    win_rate: float = 0.0
    average_return_percent: float = 0.0
    cumulative_return_percent: float = 0.0
    processed_count: int = 0
    paper_buy_count: int = 0
    verified: bool = True
    source: str = "solana_rpc_dex_signer"
    is_elite: bool = False
    locked: bool = False


def atomic_json(path: Path, document: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as file:
            json.dump(document, file, ensure_ascii=False, indent=2)
            file.write("\n")
            temporary = file.name
        os.replace(temporary, path)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


def read_json(path: Path, fallback: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback


class RpcClient:
    def __init__(self, session: aiohttp.ClientSession, settings: FeederSettings) -> None:
        self.session = session
        self.settings = settings
        self.limit = asyncio.Semaphore(settings.http_concurrency)
        self.rate_lock = asyncio.Lock()
        self.last_request_at = 0.0
        self.request_id = 0

    async def throttle(self) -> None:
        async with self.rate_lock:
            elapsed = time.monotonic() - self.last_request_at
            await asyncio.sleep(max(0.0, self.settings.rpc_min_interval_seconds - elapsed))
            self.last_request_at = time.monotonic()

    async def call(self, method: str, params: list[Any]) -> Any:
        self.request_id += 1
        request = {"jsonrpc": "2.0", "id": self.request_id, "method": method, "params": params}
        delay = 1
        for attempt in range(5):
            await self.throttle()
            async with self.limit:
                async with self.session.post(self.settings.rpc_url, json=request) as response:
                    if response.status == 429 or response.status >= 500:
                        retry_after = response.headers.get("Retry-After")
                        wait = float(retry_after) if retry_after else delay
                    else:
                        response.raise_for_status()
                        payload = await response.json()
                        if payload.get("error"):
                            raise RuntimeError(f"{method}: {payload['error']}")
                        return payload.get("result")
            if attempt == 4:
                break
            await asyncio.sleep(wait)
            delay = min(delay * 2, 30)
        raise RuntimeError(f"RPC retries exhausted: {method}")


def transaction_signers(result: Any) -> list[str]:
    message = (((result or {}).get("transaction") or {}).get("message") or {})
    header = message.get("header") or {}
    required = int(header.get("numRequiredSignatures") or 0)
    keys = message.get("accountKeys") or []
    addresses: list[str] = []
    selected = keys[:required] if required > 0 else [
        key for key in keys if isinstance(key, dict) and key.get("signer") is True
    ]
    for key in selected:
        address = key.get("pubkey") if isinstance(key, dict) else key
        if isinstance(address, str):
            addresses.append(address)
    return addresses


def elite_performance(address: str, document: Any) -> bool:
    """Use capped samples to decide whether a wallet deserves a locked slot."""
    row = (document.get("wallets") or {}).get(address, {}) if isinstance(document, dict) else {}
    if (
        not isinstance(row, dict)
        or row.get("evicted") is True
        or wallet_is_cooling_down(row)
    ):
        return False
    wins, losses = int(row.get("wins", 0)), int(row.get("losses", 0))
    total = wins + losses
    win_rate = wins / total * 100 if total else 0.0
    returns = [
        capped_return_percent(item.get("return_percent", 0))
        for item in row.get("samples", []) if isinstance(item, dict)
    ]
    average = sum(returns) / len(returns) if returns else 0.0
    return (total >= 3 and win_rate >= 70.0) or average >= 150.0


async def candidate_wallets(rpc: RpcClient, settings: FeederSettings) -> list[str]:
    batches = await asyncio.gather(*(
        rpc.call("getSignaturesForAddress", [program, {"limit": settings.signatures_per_program}])
        for program in DEX_PROGRAMS
    ), return_exceptions=True)
    signatures: list[str] = []
    for batch in batches:
        if isinstance(batch, BaseException):
            logger.warning(
                "DEX signature scan skipped: %s",
                redact_sensitive_text(batch),
            )
            continue
        signatures.extend(str(row["signature"]) for row in batch or [] if row.get("signature"))
    signatures = list(dict.fromkeys(signatures))[: settings.max_candidates]
    transactions = await asyncio.gather(*(
        rpc.call("getTransaction", [signature, {"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}])
        for signature in signatures
    ), return_exceptions=True)
    discovered: list[str] = []
    for transaction in transactions:
        if isinstance(transaction, BaseException):
            continue
        for address in transaction_signers(transaction):
            try:
                Pubkey.from_string(address)
            except ValueError:
                continue
            if address not in DEX_PROGRAMS:
                discovered.append(address)
    # Incumbents and historical elite wallets are always placed before newly
    # discovered signers, so bounded discovery can never crowd them out.
    existing = read_json(OUTPUT_PATH, {"wallets": []})
    rows = existing.get("wallets", []) if isinstance(existing, dict) else []
    incumbents: list[str] = []
    for row in rows if isinstance(rows, list) else []:
        address = row.get("address") if isinstance(row, dict) else row
        if isinstance(address, str):
            try:
                Pubkey.from_string(address)
            except ValueError:
                continue
            incumbents.append(address)
    performance = read_json(PERFORMANCE_PATH, {"wallets": {}})
    performance_rows = performance.get("wallets", {}) if isinstance(performance, dict) else {}
    historical_elite = [
        address for address in performance_rows
        if elite_performance(address, performance)
    ]
    ordered = list(dict.fromkeys([*incumbents, *historical_elite, *discovered]))
    return ordered[:settings.max_candidates]


def performance_for(
    address: str, document: Any
) -> tuple[int, int, float, float, int, int, bool]:
    row = (document.get("wallets") or {}).get(address, {}) if isinstance(document, dict) else {}
    wins, losses = int(row.get("wins", 0)), int(row.get("losses", 0))
    samples = row.get("samples") or []
    returns = [
        capped_return_percent(item.get("return_percent", 0))
        for item in samples if isinstance(item, dict)
    ]
    average = sum(returns) / len(returns) if returns else 0.0
    cumulative = sum(returns)
    processed_count = int(row.get("processed_count", 0))
    paper_buy_count = int(row.get("paper_buy_count", row.get("virtual_buys", 0)))
    unavailable = bool(row.get("evicted", False)) or wallet_is_cooling_down(row)
    return (
        wins, losses, average, cumulative, processed_count, paper_buy_count,
        unavailable,
    )


async def evaluate(rpc: RpcClient, address: str, settings: FeederSettings, performance: Any) -> WalletScore | None:
    balance_result, signatures = await asyncio.gather(
        rpc.call("getBalance", [address, {"commitment": "confirmed"}]),
        rpc.call("getSignaturesForAddress", [address, {"limit": settings.max_daily_transactions + 1}]),
    )
    balance_sol = int((balance_result or {}).get("value") or 0) / 1_000_000_000
    cutoff = int(datetime.now(timezone.utc).timestamp()) - 86400
    daily = sum(1 for item in signatures or [] if int(item.get("blockTime") or 0) >= cutoff)
    (
        wins, losses, average, cumulative, processed_count, paper_buy_count,
        unavailable,
    ) = performance_for(address, performance)
    if unavailable or balance_sol < settings.min_sol_balance or daily > settings.max_daily_transactions:
        return None
    total = wins + losses
    win_rate = wins / total * 100 if total else 0.0
    elite = elite_performance(address, performance)
    return WalletScore(
        address, round(balance_sol, 6), daily, wins, losses, win_rate, average,
        cumulative, processed_count, paper_buy_count,
        is_elite=elite, locked=elite,
    )


def score_from_snapshot(row: dict[str, Any], performance: Any) -> WalletScore | None:
    """Rebuild an incumbent from its last good snapshot after an RPC failure."""
    address = str(row.get("address", ""))
    (
        wins, losses, average, cumulative, processed_count, paper_buy_count,
        unavailable,
    ) = performance_for(address, performance)
    if not address or unavailable:
        return None
    total = wins + losses
    elite = elite_performance(address, performance)
    return WalletScore(
        address=address,
        balance_sol=float(row.get("balance_sol", 0) or 0),
        daily_transactions=int(row.get("daily_transactions", 0) or 0),
        wins=wins,
        losses=losses,
        win_rate=wins / total * 100 if total else 0.0,
        average_return_percent=average,
        cumulative_return_percent=cumulative,
        processed_count=processed_count,
        paper_buy_count=paper_buy_count,
        verified=bool(row.get("verified", True)),
        source=str(row.get("source", "rpc_failure_snapshot")),
        is_elite=elite,
        locked=elite,
    )


def score_rank(row: WalletScore) -> tuple[float, int, float, float, int, float]:
    """Rank by proven copyability plus the wallet's raw on-chain performance."""
    evaluated = row.wins + row.losses
    confidence = min(1.0, evaluated / 5)
    win_component = row.win_rate * confidence
    roi_component = math.copysign(
        math.log1p(abs(row.cumulative_return_percent)) * 10,
        row.cumulative_return_percent,
    )
    copy_component = min(row.paper_buy_count, 50) * 5
    observation_component = min(row.processed_count, 200) * 0.1
    composite = (
        copy_component + win_component + roi_component + observation_component
    )
    return (
        composite,
        row.paper_buy_count,
        row.cumulative_return_percent,
        row.win_rate,
        row.processed_count,
        row.balance_sol,
    )


def select_active_wallets(
    scores: list[WalletScore], incumbent_addresses: set[str], settings: FeederSettings
) -> list[WalletScore]:
    """Lock elite incumbents first, then fill remaining slots by performance."""
    best_by_address: dict[str, WalletScore] = {}
    for score in scores:
        current = best_by_address.get(score.address)
        if current is None or score_rank(score) > score_rank(current):
            best_by_address[score.address] = score
    unique = list(best_by_address.values())
    incumbent_elite = sorted(
        (row for row in unique if row.is_elite and row.address in incumbent_addresses),
        key=score_rank, reverse=True,
    )
    other_elite = sorted(
        (row for row in unique if row.is_elite and row.address not in incumbent_addresses),
        key=score_rank, reverse=True,
    )
    general = sorted((row for row in unique if not row.is_elite), key=score_rank, reverse=True)
    # Every incumbent elite remains locked. New elite wallets first consume the
    # dedicated reserve; additional elite candidates compete in the general pool.
    reserve_needed = max(0, settings.elite_reserved_slots - len(incumbent_elite))
    reserved_new_elite = other_elite[:reserve_needed]
    survival_pool = sorted([*other_elite[reserve_needed:], *general], key=score_rank, reverse=True)
    ordered = [*incumbent_elite, *reserved_new_elite, *survival_pool]
    selected = ordered[:settings.max_wallets]
    locked_count = sum(row.is_elite for row in selected)
    logger.info(
        "selected %s/%s active wallets (elite=%s, reserved=%s)",
        len(selected), settings.max_wallets, locked_count, settings.elite_reserved_slots,
    )
    return selected


def write_results(
    scores: list[WalletScore], selected: list[WalletScore], settings: FeederSettings
) -> None:
    generated = datetime.now(timezone.utc).isoformat()
    criteria = {"minimum_sol_balance": settings.min_sol_balance, "maximum_daily_transactions": settings.max_daily_transactions, "source": "Solana JSON-RPC only"}
    atomic_json(CANDIDATE_PATH, {"generated_at": generated, "count": len(scores), "wallets": [asdict(score) for score in scores]})
    # Preserve a working list when a transient bounded scan finds no qualified signer.
    if not selected:
        logger.warning("no qualified candidates; preserving existing wallets.json")
        return
    atomic_json(OUTPUT_PATH, {
        "generated_at": generated,
        "criteria": {**criteria, "elite_reserved_slots": settings.elite_reserved_slots},
        "count": len(selected),
        "wallets": [asdict(score) for score in selected[:settings.max_wallets]],
    })


def replenish_from_candidate_cache(settings: FeederSettings) -> int:
    """Repair, cap, and fill the active list from the last verified pool."""
    document = read_json(OUTPUT_PATH, {"wallets": []})
    rows = document.get("wallets", []) if isinstance(document, dict) else []
    rows = [row for row in rows if isinstance(row, dict) and row.get("address")]
    performance = read_json(PERFORMANCE_PATH, {"wallets": {}})
    states = performance.get("wallets", {}) if isinstance(performance, dict) else {}
    unavailable = {
        address for address, state in states.items()
        if isinstance(state, dict) and (
            state.get("evicted") is True or wallet_is_cooling_down(state)
        )
    }
    rows = [row for row in rows if row["address"] not in unavailable]
    pool = read_json(CANDIDATE_PATH, {"wallets": []})
    candidates = pool.get("wallets", []) if isinstance(pool, dict) else []
    combined = [*rows, *(candidates if isinstance(candidates, list) else [])]
    scores: list[WalletScore] = []
    for row in combined:
        if (
            not isinstance(row, dict)
            or row.get("address") in unavailable
            or row.get("verified") is False
        ):
            continue
        score = score_from_snapshot(row, performance)
        if score:
            scores.append(score)
    incumbent_addresses = {row["address"] for row in rows}
    selected = select_active_wallets(scores, incumbent_addresses, settings)
    selected_rows = [asdict(score) for score in selected]
    if selected_rows != rows or len(rows) != int(document.get("count", 0)):
        document.update({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "count": len(selected_rows),
            "wallets": selected_rows,
        })
        atomic_json(OUTPUT_PATH, document)
    return len(selected_rows)


async def refresh(settings: FeederSettings) -> list[WalletScore]:
    ensure_performance_migrated()
    restored = restore_expired_cooldowns()
    if restored:
        logger.info("returned %s cooled-down wallets to the candidate pool", restored)
    timeout = aiohttp.ClientTimeout(total=45)
    performance = read_json(PERFORMANCE_PATH, {"wallets": {}})
    existing_document = read_json(OUTPUT_PATH, {"wallets": []})
    existing_rows = existing_document.get("wallets", []) if isinstance(existing_document, dict) else []
    existing_by_address = {
        row["address"]: row for row in existing_rows
        if isinstance(row, dict) and row.get("address")
    }
    async with aiohttp.ClientSession(timeout=timeout) as session:
        rpc = RpcClient(session, settings)
        candidates = await candidate_wallets(rpc, settings)
        logger.info("discovered %s on-chain signer candidates", len(candidates))
        results = await asyncio.gather(
            *(evaluate(rpc, address, settings, performance) for address in candidates),
            return_exceptions=True,
        )
    qualified: list[WalletScore] = []
    fallback_count = 0
    for address, result in zip(candidates, results):
        if isinstance(result, WalletScore):
            qualified.append(result)
            continue
        if isinstance(result, BaseException):
            snapshot = existing_by_address.get(address)
            fallback = score_from_snapshot(snapshot, performance) if snapshot else None
            if fallback:
                qualified.append(fallback)
                fallback_count += 1
                logger.warning("incumbent RPC validation failed; preserving snapshot wallet=%s", address)
            else:
                logger.warning(
                    "candidate validation skipped wallet=%s error=%s",
                    address,
                    redact_sensitive_text(result),
                )
    qualified.sort(key=score_rank, reverse=True)
    selected = select_active_wallets(qualified, set(existing_by_address), settings)
    write_results(qualified, selected, settings)
    logger.info(
        "qualified %s wallets using on-chain checks (fallback incumbents=%s)",
        len(qualified), fallback_count,
    )
    return selected


async def scheduler(settings: FeederSettings, once: bool) -> None:
    performance = ensure_performance_migrated()
    logger.info(
        "wallet performance schema=%s legacy_amnestied=%s",
        performance.get("schema_version"),
        performance.get("legacy_evictions_amnestied_count", 0),
    )
    restored = restore_expired_cooldowns()
    if restored:
        logger.info("returned %s cooled-down wallets to the candidate pool", restored)
    logger.info("restored %s active wallets from verified candidate cache", replenish_from_candidate_cache(settings))
    while True:
        started = asyncio.get_running_loop().time()
        try:
            await refresh(settings)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("wallet refresh failed; existing wallets.json was preserved")
        if once:
            return
        await asyncio.sleep(max(0, settings.refresh_seconds - (asyncio.get_running_loop().time() - started)))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="refresh once and exit")
    args = parser.parse_args()
    configure_safe_logging()
    asyncio.run(scheduler(FeederSettings.from_env(), args.once))


if __name__ == "__main__":
    main()
