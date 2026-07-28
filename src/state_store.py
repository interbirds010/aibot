"""Cross-process, versioned JSON state transactions for Windows and Linux.

Atomic replacement prevents torn files, while the sidecar lock prevents two
processes from reading the same revision and silently overwriting each other.
"""

from __future__ import annotations

import copy
import json
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator, TypeVar

if os.name == "nt":
    import msvcrt
else:  # pragma: no cover - exercised by the Linux deployment.
    import fcntl

T = TypeVar("T")
Mutator = Callable[[dict[str, Any]], T]
VALID_ROUTE_TYPES = frozenset({"A", "B"})
ROOT = Path(__file__).resolve().parents[1]
PAPER_TRADES_PATH = ROOT / "data" / "paper_trades.json"
WALLETS_PATH = ROOT / "data" / "wallets.json"
WALLET_PERFORMANCE_PATH = ROOT / "data" / "wallet_performance.json"
GLOBAL_METRICS_PATH = ROOT / "data" / "global_metrics.json"


class VersionConflict(RuntimeError):
    """Raised when a caller attempts to update a stale document revision."""


class StateLockTimeout(TimeoutError):
    """Raised when another process holds a state lock beyond the deadline."""


def normalized_route_metadata(
    route_type: str, dex_momentum_score: float = 0.0
) -> dict[str, str | float]:
    """Return compact, schema-safe route fields shared by position/event writes."""
    normalized_route = str(route_type).upper()
    if normalized_route not in VALID_ROUTE_TYPES:
        raise ValueError("route_type must be A or B")
    score = float(dex_momentum_score)
    if not 0.0 <= score <= 100.0:
        raise ValueError("dex_momentum_score must be between 0 and 100")
    return {
        "route_type": normalized_route,
        "dex_momentum_score": round(score, 4),
    }


def read_json(path: Path, fallback: dict[str, Any]) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return copy.deepcopy(fallback)
    if not isinstance(document, dict):
        raise RuntimeError(f"{path} must contain a JSON object")
    return document


@contextmanager
def exclusive_file_lock(
    path: Path, *, timeout_seconds: float = 15.0, poll_seconds: float = 0.05
) -> Iterator[None]:
    """Acquire an OS-visible exclusive lock using a stable sidecar file."""
    lock_path = path.with_name(f"{path.name}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(lock_path, "a+b")
    locked = False
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                handle.seek(0)
                if os.name == "nt":
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:  # pragma: no cover
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = True
                break
            except OSError as exc:
                if time.monotonic() >= deadline:
                    raise StateLockTimeout(f"timed out locking {path}") from exc
                time.sleep(poll_seconds)
        yield
    finally:
        if locked:
            try:
                handle.seek(0)
                if os.name == "nt":
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:  # pragma: no cover
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        handle.close()


def atomic_write_json(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=path.parent, delete=False
        ) as file:
            # Compact encoding shortens fsync and therefore the cross-process
            # lock hold time on the 1 GB production VPS.
            json.dump(
                document,
                file,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
            temporary = file.name
        os.replace(temporary, path)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


def update_json(
    path: Path,
    fallback: dict[str, Any],
    mutator: Mutator[T],
    *,
    expected_version: int | None = None,
) -> tuple[T, dict[str, Any]]:
    """Lock, reread, CAS-check, mutate, increment version, and atomically save."""
    with exclusive_file_lock(path):
        document = read_json(path, fallback)
        current_version = int(document.get("version", 0) or 0)
        if expected_version is not None and current_version != expected_version:
            raise VersionConflict(
                f"{path.name} version changed: expected={expected_version}, "
                f"actual={current_version}"
            )
        result = mutator(document)
        document["version"] = current_version + 1
        atomic_write_json(path, document)
        return result, document


def migrate_json(
    path: Path,
    fallback: dict[str, Any],
    migrator: Callable[[dict[str, Any]], bool],
) -> dict[str, Any]:
    """Run an idempotent migration while holding the cross-process lock."""
    with exclusive_file_lock(path):
        document = read_json(path, fallback)
        changed = migrator(document)
        if changed:
            document["version"] = int(document.get("version", 0) or 0) + 1
            atomic_write_json(path, document)
        return document


def get_last_trade_time(token_address: str) -> float:
    """특정 토큰의 마지막 매수 시각을 Unix 초 단위로 반환한다."""
    token = str(token_address).strip()
    if not token:
        return 0.0
    with exclusive_file_lock(PAPER_TRADES_PATH):
        document = read_json(PAPER_TRADES_PATH, {"events": []})
        events = document.get("events", [])
        if not isinstance(events, list):
            return 0.0
        for event in reversed(events):
            if not isinstance(event, dict):
                continue
            if event.get("type") != "BUY" or str(event.get("mint", "")) != token:
                continue
            raw_timestamp = event.get("at")
            if not raw_timestamp:
                return 0.0
            try:
                parsed = datetime_from_iso(raw_timestamp)
            except (TypeError, ValueError):
                return 0.0
            return parsed
    return 0.0


def get_recent_stop_loss_time(
    token_address: str,
    *,
    maximum_tokens: int = 50,
) -> float:
    """최근 손절 토큰 최대 50개의 원장 이력에서 해당 민트의 손절 시각을 찾는다."""
    token = str(token_address).strip()
    limit = max(1, min(50, int(maximum_tokens)))
    if not token:
        return 0.0
    stop_reasons = {
        "STOP_LOSS_15",
        "ROUTE_B_STOP_LOSS_10",
        "LIVE_STOP_LOSS_15",
    }
    with exclusive_file_lock(PAPER_TRADES_PATH):
        document = read_json(PAPER_TRADES_PATH, {"events": []})
        events = document.get("events", [])
        if not isinstance(events, list):
            raise ValueError("paper trade events must be a list")
        seen_tokens: set[str] = set()
        for event in reversed(events):
            if not isinstance(event, dict):
                continue
            if event.get("type") != "SELL" or event.get("reason") not in stop_reasons:
                continue
            event_mint = str(event.get("mint", "")).strip()
            if not event_mint or event_mint in seen_tokens:
                continue
            seen_tokens.add(event_mint)
            if event_mint == token:
                raw_timestamp = event.get("at")
                if not raw_timestamp:
                    raise ValueError("stop-loss event is missing at")
                return datetime_from_iso(raw_timestamp)
            if len(seen_tokens) >= limit:
                break
    return 0.0


def datetime_from_iso(value: Any) -> float:
    """ISO-8601 시각을 UTC Unix 초 단위로 변환한다."""
    from datetime import datetime, timezone

    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def get_active_wallets_count() -> int:
    """현재 감시 목록에 포함된 정상 지갑 수를 파일 락 안에서 계산한다."""
    with exclusive_file_lock(WALLETS_PATH):
        document = read_json(WALLETS_PATH, {"wallets": []})
        rows = document.get("wallets", [])
        if not isinstance(rows, list):
            return 0
        return sum(
            1
            for row in rows
            if (
                isinstance(row, str)
                and bool(row.strip())
            )
            or (
                isinstance(row, dict)
                and bool(str(row.get("address", "")).strip())
                and str(row.get("status", "ACTIVE")).upper() == "ACTIVE"
            )
        )


def get_global_metric(name: str, default: Any = None) -> Any:
    """공통 글로벌 메트릭을 파일 락 안에서 조회한다."""
    key = str(name).strip()
    if not key:
        raise ValueError("global metric name must not be empty")
    with exclusive_file_lock(GLOBAL_METRICS_PATH):
        document = read_json(
            GLOBAL_METRICS_PATH,
            {"schema_version": 2, "version": 0, "metrics": {}},
        )
        metrics = document.get("metrics", {})
        return metrics.get(key, default) if isinstance(metrics, dict) else default


def set_global_metric(name: str, value: Any) -> None:
    """공통 글로벌 메트릭을 파일 락과 원자적 교체를 통해 저장한다."""
    key = str(name).strip()
    if not key:
        raise ValueError("global metric name must not be empty")

    def mutate(document: dict[str, Any]) -> None:
        document.setdefault("schema_version", 2)
        metrics = document.setdefault("metrics", {})
        if not isinstance(metrics, dict):
            metrics = {}
            document["metrics"] = metrics
        metrics[key] = value

    update_json(
        GLOBAL_METRICS_PATH,
        {"schema_version": 2, "version": 0, "metrics": {}},
        mutate,
    )


def claim_global_interval(name: str, now: float, interval_seconds: float) -> bool:
    """여러 PM2 프로세스 중 하나만 주어진 주기 작업을 선점하도록 한다."""
    key = str(name).strip()
    current = float(now)
    interval = max(0.0, float(interval_seconds))
    if not key:
        raise ValueError("global metric name must not be empty")

    def mutate(document: dict[str, Any]) -> bool:
        document.setdefault("schema_version", 2)
        metrics = document.setdefault("metrics", {})
        if not isinstance(metrics, dict):
            metrics = {}
            document["metrics"] = metrics
        try:
            previous = float(metrics.get(key, 0) or 0)
        except (TypeError, ValueError):
            previous = 0.0
        if current - previous < interval:
            return False
        metrics[key] = current
        return True

    claimed, _ = update_json(
        GLOBAL_METRICS_PATH,
        {"schema_version": 2, "version": 0, "metrics": {}},
        mutate,
    )
    return bool(claimed)


def get_wallets_by_status(status: str) -> list[dict[str, Any]]:
    """성과 원장에서 지정 상태의 지갑 스냅샷을 반환한다."""
    normalized_status = str(status).upper()
    with exclusive_file_lock(WALLET_PERFORMANCE_PATH):
        document = read_json(
            WALLET_PERFORMANCE_PATH,
            {"schema_version": 4, "version": 0, "wallets": {}},
        )
        rows = document.get("wallets", {})
        if not isinstance(rows, dict):
            return []
        return [
            {"address": address, **copy.deepcopy(row)}
            for address, row in rows.items()
            if isinstance(row, dict)
            and str(row.get("status", "ACTIVE")).upper() == normalized_status
        ]


def set_wallet_status(
    address: str,
    status: str,
    *,
    reset_metrics: bool = False,
    completed_at: str | None = None,
) -> bool:
    """지갑 상태만 원자적으로 전환하고 기본적으로 누적 성과를 보존한다."""
    wallet = str(address).strip()
    normalized_status = str(status).upper()
    if not wallet:
        raise ValueError("wallet address must not be empty")
    if normalized_status not in {"ACTIVE", "COOL_DOWN"}:
        raise ValueError("wallet status must be ACTIVE or COOL_DOWN")

    def mutate(document: dict[str, Any]) -> bool:
        wallets = document.setdefault("wallets", {})
        if not isinstance(wallets, dict):
            wallets = {}
            document["wallets"] = wallets
        row = wallets.get(wallet)
        if not isinstance(row, dict):
            return False
        row["status"] = normalized_status
        if normalized_status == "ACTIVE":
            row.pop("cooldown_started_at", None)
            row.pop("cooldown_start_time", None)
            row.pop("cooldown_reason", None)
            row["evicted"] = False
            if completed_at:
                row["cooldown_completed_at"] = completed_at
        if reset_metrics:
            row["wins"] = 0
            row["losses"] = 0
            row["samples"] = []
            row["average_return_percent"] = 0.0
            row["cumulative_return_percent"] = 0.0
        if completed_at:
            document["updated_at"] = completed_at
        return True

    changed, _ = update_json(
        WALLET_PERFORMANCE_PATH,
        {"schema_version": 4, "version": 0, "wallets": {}},
        mutate,
    )
    return bool(changed)
