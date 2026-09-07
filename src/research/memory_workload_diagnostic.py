"""운영 원장을 변경하지 않고 JSON workload의 메모리 비용을 요약한다."""

from __future__ import annotations

import argparse
import json
import tracemalloc
from pathlib import Path
from typing import Any

from src import state_store
from src.observation_tracker import (
    OBSERVATION_PATH,
    _due_sample_candidates,
    empty_observations,
)
from src.research_archive import RESEARCH_ARCHIVE_PATH
from src.runtime_memory import process_memory_snapshot
from src.shadow_trade_ledger import SHADOW_TRADE_PATH, empty_shadow_trades


ROOT = Path(__file__).resolve().parents[2]
LEDGERS = (
    ("signal_observations", OBSERVATION_PATH, "observations", empty_observations()),
    ("shadow_trades", SHADOW_TRADE_PATH, "trades", empty_shadow_trades()),
    ("paper_trades", ROOT / "data" / "paper_trades.json", "events", {"events": []}),
    (
        "wallet_performance",
        ROOT / "data" / "wallet_performance.json",
        "wallets",
        {"wallets": {}},
    ),
)
MAX_ROW_SIZE_SCAN = 10_000
DEFAULT_REPETITIONS = 3


def _read_locked(path: Path, fallback: dict[str, Any]) -> dict[str, Any]:
    with state_store.exclusive_file_lock(path):
        return state_store.read_json(path, fallback)


def _rows(document: dict[str, Any], key: str) -> list[Any]:
    value = document.get(key)
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        return list(value.values())
    return []


def ledger_inventory(
    name: str, path: Path, collection_key: str, fallback: dict[str, Any],
) -> dict[str, Any]:
    if not path.exists():
        return {"name": name, "exists": False}
    document = _read_locked(path, fallback)
    rows = _rows(document, collection_key)
    scanned = rows[:MAX_ROW_SIZE_SCAN]
    sizes = [
        len(json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode())
        for row in scanned
    ]
    return {
        "name": name,
        "exists": True,
        "file_bytes": path.stat().st_size,
        "row_count": len(rows),
        "row_size_scan_count": len(scanned),
        "average_row_bytes": (
            round(sum(sizes) / len(sizes), 2) if sizes else None
        ),
        "largest_row_bytes": max(sizes) if sizes else None,
        "full_json_load": True,
    }


def archive_inventory(path: Path) -> dict[str, Any]:
    files = list((path / "records").glob("*/*.json")) if path.exists() else []
    sizes = [item.stat().st_size for item in files]
    return {
        "name": "research_archive",
        "exists": path.exists(),
        "file_count": len(files),
        "total_bytes": sum(sizes),
        "average_record_bytes": (
            round(sum(sizes) / len(sizes), 2) if sizes else None
        ),
        "largest_record_bytes": max(sizes) if sizes else None,
        "full_json_load": False,
    }


def observation_allocator_profile(
    path: Path = OBSERVATION_PATH, *, repetitions: int = DEFAULT_REPETITIONS,
) -> dict[str, Any]:
    runs = max(1, min(10, int(repetitions)))
    if not path.exists():
        return {"available": False, "repetitions": runs}
    rss_before = process_memory_snapshot().get("rss_bytes")
    tracemalloc.start()
    start_current, _ = tracemalloc.get_traced_memory()
    due_count = 0
    row_count = 0
    for _ in range(runs):
        document = _read_locked(path, empty_observations())
        rows = _rows(document, "observations")
        row_count = len(rows)
        due_count = len(_due_sample_candidates(rows, 10**12))
        del rows, document
    end_current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    rss_after = process_memory_snapshot().get("rss_bytes")
    return {
        "available": True,
        "repetitions": runs,
        "row_count": row_count,
        "last_due_count": due_count,
        "tracemalloc_current_delta_bytes": end_current - start_current,
        "tracemalloc_peak_bytes": peak,
        "rss_before_bytes": rss_before,
        "rss_after_bytes": rss_after,
        "rss_delta_bytes": (
            rss_after - rss_before
            if isinstance(rss_before, int) and isinstance(rss_after, int)
            else None
        ),
    }


def build_memory_workload_diagnostic() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "ledgers": [ledger_inventory(*spec) for spec in LEDGERS],
        "archive": archive_inventory(RESEARCH_ARCHIVE_PATH),
        "observation_allocator_profile": observation_allocator_profile(),
        "read_only": True,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Profile bounded ledger workload")
    parser.parse_args(argv)
    report = build_memory_workload_diagnostic()
    print(
        "MEMORY_WORKLOAD_DIAGNOSTIC "
        + json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
