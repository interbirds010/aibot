"""완결된 Research observation을 immutable 파일로 장기 보존한다."""

from __future__ import annotations

import argparse
import hashlib
import heapq
import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from src.phase_memory_telemetry import phase_memory
from src.state_store import (
    atomic_write_json,
    exclusive_file_lock,
    read_json,
    update_json,
)


logger = logging.getLogger("research-archive")
ROOT = Path(__file__).resolve().parents[1]
RESEARCH_ARCHIVE_PATH = ROOT / "data" / "research_archive"
RESEARCH_ARCHIVE_METRICS_PATH = ROOT / "data" / "research_archive_metrics.json"
ARCHIVE_SCHEMA_VERSION = 1
TERMINAL_STATUSES = frozenset({"COMPLETE", "EXPIRED_UNSAMPLED"})
DEFAULT_COHORT = "research_v1_60m"


def empty_archive_metrics() -> dict[str, Any]:
    return {
        "schema_version": ARCHIVE_SCHEMA_VERSION,
        "duplicate_prevented_count": 0,
        "archive_write_failure_count": 0,
        "last_archive_success": None,
        "last_archive_failure": None,
        "last_archive_failure_category": None,
        "version": 0,
    }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _observation_id(row: dict[str, Any]) -> str:
    return str(row.get("observation_id") or "").strip()


def is_terminal_observation(row: Any) -> bool:
    return (
        isinstance(row, dict)
        and str(row.get("status") or "").upper() in TERMINAL_STATUSES
    )


def archive_record_path(observation_id: str, *, archive_path: Path | None = None) -> Path:
    identity = str(observation_id).strip()
    if not identity:
        raise ValueError("archive observation_id must not be empty")
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    root = archive_path or RESEARCH_ARCHIVE_PATH
    return root / "records" / digest[:2] / f"{digest}.json"


def _metric_update(
    *,
    metrics_path: Path,
    duplicate_count: int = 0,
    failure_category: str | None = None,
    success_at: str | None = None,
) -> None:
    def mutate(document: dict[str, Any]) -> None:
        document.setdefault("schema_version", ARCHIVE_SCHEMA_VERSION)
        if duplicate_count:
            document["duplicate_prevented_count"] = int(
                document.get("duplicate_prevented_count", 0) or 0
            ) + int(duplicate_count)
        if failure_category:
            document["archive_write_failure_count"] = int(
                document.get("archive_write_failure_count", 0) or 0
            ) + 1
            document["last_archive_failure"] = _now()
            document["last_archive_failure_category"] = failure_category
        if success_at:
            document["last_archive_success"] = success_at

    with phase_memory(
        "archive_metric_write",
        metadata={"workload": "observation", "operation": "update"},
    ):
        update_json(metrics_path, empty_archive_metrics(), mutate)


def _existing_archive_record(path: Path, observation_id: str) -> dict[str, Any] | None:
    if not path.exists():
        return None
    document = read_json(path, {})
    if document.get("archive_schema_version") != ARCHIVE_SCHEMA_VERSION:
        raise RuntimeError("research archive record schema is unsupported")
    archived_id = str(document.get("observation_id") or "").strip()
    if archived_id != observation_id:
        raise RuntimeError("research archive identity hash collision")
    observation = document.get("observation")
    if not isinstance(observation, dict):
        raise RuntimeError("research archive observation is malformed")
    return document


def archive_observation(
    row: dict[str, Any],
    *,
    archive_path: Path | None = None,
    metrics_path: Path | None = None,
    source: str = "operational_ledger",
) -> tuple[bool, str]:
    """terminal row 하나를 observation_id 기준 exactly-once로 저장한다."""
    if not is_terminal_observation(row):
        return False, "NOT_TERMINAL"
    observation_id = _observation_id(row)
    if not observation_id:
        return False, "MISSING_OBSERVATION_ID"
    record_path = archive_record_path(observation_id, archive_path=archive_path)
    metric_file = metrics_path or RESEARCH_ARCHIVE_METRICS_PATH
    archived_at = _now()
    with exclusive_file_lock(record_path):
        existing = _existing_archive_record(record_path, observation_id)
        if existing is not None:
            _metric_update(metrics_path=metric_file, duplicate_count=1)
            return False, str(existing.get("archived_at") or archived_at)
        with phase_memory(
            "archive_record_preparation",
            metadata={
                "workload": "observation",
                "operation": "archive",
                "row_count": 1,
            },
        ):
            document = {
                "archive_schema_version": ARCHIVE_SCHEMA_VERSION,
                "observation_id": observation_id,
                "tracking_profile": row.get("tracking_profile"),
                "signal_type": row.get("signal_type"),
                "signal_detected_at": row.get("signal_detected_at"),
                "archived_at": archived_at,
                "archive_source": str(source)[:80],
                # Archive wrapper는 source row를 복사하지 않고 그대로 참조한다.
                "observation": row,
            }
        with phase_memory(
            "archive_serialization_write",
            metadata={
                "workload": "observation",
                "operation": "serialize",
                "row_count": 1,
            },
        ) as serialization_scope:
            def observe_write(stage: str, size_bytes: int) -> None:
                metadata: dict[str, Any] = {"stage": stage}
                if size_bytes:
                    metadata.update({
                        "serialized_bytes": size_bytes,
                        "file_bytes": size_bytes,
                    })
                serialization_scope.set_metadata(metadata)

            atomic_write_json(
                record_path,
                document,
                lifecycle_observer=observe_write,
            )
    _metric_update(metrics_path=metric_file, success_at=archived_at)
    return True, archived_at


def archive_observation_best_effort(
    row: dict[str, Any],
    *,
    archive_path: Path | None = None,
    metrics_path: Path | None = None,
    source: str = "operational_ledger",
) -> bool:
    """archive 장애를 호출자와 격리하되 canonical metric과 로그를 남긴다."""
    metric_file = metrics_path or RESEARCH_ARCHIVE_METRICS_PATH
    try:
        _, archived_at = archive_observation(
            row,
            archive_path=archive_path,
            metrics_path=metric_file,
            source=source,
        )
        if archived_at in {"NOT_TERMINAL", "MISSING_OBSERVATION_ID"}:
            return False
        row["archive_schema_version"] = ARCHIVE_SCHEMA_VERSION
        row["archived_at"] = archived_at
        return True
    except Exception:
        try:
            _metric_update(
                metrics_path=metric_file,
                failure_category="ARCHIVE_WRITE_FAILED",
            )
        except Exception:
            logger.exception("research archive failure metric update failed")
        logger.exception(
            "research archive write failed: observation_id_sha256=%s category=%s",
            hashlib.sha256(_observation_id(row).encode()).hexdigest()
            if _observation_id(row) else "missing",
            "ARCHIVE_WRITE_FAILED",
        )
        return False


def archive_terminal_rows(
    rows: Iterable[Any],
    *,
    archive_path: Path | None = None,
    metrics_path: Path | None = None,
    source: str = "operational_ledger",
    dry_run: bool = False,
) -> dict[str, int]:
    """terminal row 집합을 멱등 backfill하고 결과를 요약한다."""
    result = {"eligible": 0, "archived": 0, "duplicate": 0, "failed": 0}
    for item in rows:
        if not is_terminal_observation(item) or not _observation_id(item):
            continue
        result["eligible"] += 1
        path = archive_record_path(_observation_id(item), archive_path=archive_path)
        if path.exists():
            if dry_run:
                result["duplicate"] += 1
                continue
            try:
                existing = _existing_archive_record(path, _observation_id(item))
                item["archive_schema_version"] = ARCHIVE_SCHEMA_VERSION
                item["archived_at"] = existing.get("archived_at") if existing else None
                result["duplicate"] += 1
            except Exception:
                result["failed"] += 1
            continue
        if dry_run:
            result["archived"] += 1
            continue
        if archive_observation_best_effort(
            item, archive_path=archive_path, metrics_path=metrics_path,
            source=source,
        ):
            result["archived"] += 1
        else:
            result["failed"] += 1
    if result["duplicate"] and not dry_run:
        _metric_update(
            metrics_path=metrics_path or RESEARCH_ARCHIVE_METRICS_PATH,
            duplicate_count=result["duplicate"],
        )
    return result


def shadow_trade_observation(row: Any) -> dict[str, Any] | None:
    """Research V1 shadow row를 archive 가능한 observation snapshot으로 복원한다."""
    if (
        not isinstance(row, dict)
        or row.get("tracking_profile") != DEFAULT_COHORT
        or not str(row.get("observation_id") or row.get("shadow_trade_id") or "").strip()
    ):
        return None
    restored = dict(row)
    restored["observation_id"] = str(
        row.get("observation_id") or row.get("shadow_trade_id")
    ).strip()
    restored["status"] = "COMPLETE"
    restored.setdefault("completed_at", row.get("closed_at"))
    return restored


def backfill_research_archive(
    operational_rows: Iterable[Any],
    *,
    shadow_rows: Iterable[Any] = (),
    archive_path: Path | None = None,
    metrics_path: Path | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    operational = archive_terminal_rows(
        operational_rows,
        archive_path=archive_path,
        metrics_path=metrics_path,
        source="operational_ledger_backfill",
        dry_run=dry_run,
    )
    restored = [
        observation for row in shadow_rows
        if (observation := shadow_trade_observation(row)) is not None
    ]
    shadow = archive_terminal_rows(
        restored,
        archive_path=archive_path,
        metrics_path=metrics_path,
        source="shadow_trade_backfill",
        dry_run=dry_run,
    )
    return {
        f"operational_{key}": value for key, value in operational.items()
    } | {f"shadow_{key}": value for key, value in shadow.items()}


def _timestamp(value: Any) -> float:
    if isinstance(value, bool):
        return 0.0
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        number = math.nan
    if math.isfinite(number) and number >= 0:
        return number
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return 0.0
        if parsed.tzinfo is not None:
            return parsed.timestamp()
    return 0.0


def _record_sort_key(row: dict[str, Any]) -> tuple[float, str]:
    return (
        _timestamp(row.get("signal_detected_at"))
        or _timestamp(row.get("started_at_epoch"))
        or _timestamp(row.get("started_at")),
        _observation_id(row),
    )


def load_research_archive(
    *,
    archive_path: Path | None = None,
    tracking_profile: str = DEFAULT_COHORT,
    maximum_rows: int = 10_000,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """archive 전체를 streaming scan하고 cohort 최신 N건만 메모리에 유지한다."""
    root = archive_path or RESEARCH_ARCHIVE_PATH
    limit = max(1, int(maximum_rows))
    heap: list[tuple[tuple[float, str], dict[str, Any]]] = []
    total = 0
    cohort_count = 0
    total_bytes = 0
    for path in (root / "records").glob("*/*.json"):
        total_bytes += path.stat().st_size
        document = read_json(path, {})
        if document.get("archive_schema_version") != ARCHIVE_SCHEMA_VERSION:
            raise RuntimeError("research archive record schema is unsupported")
        row = document.get("observation")
        if not isinstance(row, dict):
            raise RuntimeError("research archive observation is malformed")
        if _observation_id(row) != str(document.get("observation_id") or ""):
            raise RuntimeError("research archive observation identity mismatch")
        total += 1
        if row.get("tracking_profile") != tracking_profile:
            continue
        cohort_count += 1
        item = (_record_sort_key(row), row)
        if len(heap) < limit:
            heapq.heappush(heap, item)
        elif item[0] > heap[0][0]:
            heapq.heapreplace(heap, item)
    rows = [item[1] for item in sorted(heap, key=lambda item: item[0])]
    return rows, {
        "archive_total_rows": total,
        "archive_total_bytes": total_bytes,
        "cohort": tracking_profile,
        "cohort_row_count": cohort_count,
        "research_v1_rows": (
            cohort_count if tracking_profile == DEFAULT_COHORT else None
        ),
        "loaded_cohort_row_count": len(rows),
    }


def archive_integrity_metrics(
    *,
    operational_rows: Iterable[Any] = (),
    archive_path: Path | None = None,
    metrics_path: Path | None = None,
    recent_seconds: float = 86_400.0,
    now_epoch: float | None = None,
) -> dict[str, Any]:
    rows, stats = load_research_archive(
        archive_path=archive_path,
        tracking_profile=DEFAULT_COHORT,
        maximum_rows=1,
    )
    del rows
    metric_file = metrics_path or RESEARCH_ARCHIVE_METRICS_PATH
    persisted = read_json(metric_file, empty_archive_metrics())
    archive_root = archive_path or RESEARCH_ARCHIVE_PATH
    now = datetime.now(timezone.utc).timestamp() if now_epoch is None else float(now_epoch)
    recent = 0
    research_recent = 0
    for path in (archive_root / "records").glob("*/*.json"):
        document = read_json(path, {})
        if now - _timestamp(document.get("archived_at")) <= recent_seconds:
            recent += 1
        row = document.get("observation")
        signal_age = (
            now - _record_sort_key(row)[0] if isinstance(row, dict) else math.inf
        )
        if (
            isinstance(row, dict)
            and row.get("tracking_profile") == DEFAULT_COHORT
            and 0 <= signal_age <= recent_seconds
        ):
            research_recent += 1
    unarchived = sum(
        1 for row in operational_rows
        if is_terminal_observation(row)
        and _observation_id(row)
        and not archive_record_path(_observation_id(row), archive_path=archive_root).exists()
    )
    return {
        **stats,
        "archived_recent_interval": recent,
        "research_v1_signals_recent_interval": research_recent,
        "recent_interval_seconds": float(recent_seconds),
        "average_archive_row_bytes": (
            round(stats["archive_total_bytes"] / stats["archive_total_rows"], 2)
            if stats["archive_total_rows"] else None
        ),
        "duplicate_prevented_count": int(
            persisted.get("duplicate_prevented_count", 0) or 0
        ),
        "archive_write_failure_count": int(
            persisted.get("archive_write_failure_count", 0) or 0
        ),
        "last_archive_success": persisted.get("last_archive_success"),
        "last_archive_failure": persisted.get("last_archive_failure"),
        "last_archive_failure_category": persisted.get(
            "last_archive_failure_category"
        ),
        "unarchived_completed_rows_count": unarchived,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill Research archive")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    from src.observation_tracker import OBSERVATION_PATH, empty_observations
    from src.shadow_trade_ledger import SHADOW_TRADE_PATH, empty_shadow_trades

    observations = read_json(OBSERVATION_PATH, empty_observations())
    shadows = read_json(SHADOW_TRADE_PATH, empty_shadow_trades())
    result = backfill_research_archive(
        observations.get("observations", []),
        shadow_rows=shadows.get("trades", []),
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
