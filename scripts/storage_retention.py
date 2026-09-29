"""검증된 Aibot backup과 중단된 legacy 임시 파일만 fail-closed로 정리한다."""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import math
import os
import re
import shutil
import stat
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Callable, Iterable


LEGACY_TEMP_PATTERN = re.compile(r"tmp[a-zA-Z0-9_]{8}")
BACKUP_PATTERN = re.compile(r"predeploy-(\d{8}T\d{6}Z)-([0-9a-f]{12})")
MARKER_PATTERN = re.compile(r"latest-([0-9a-f]{40})\.txt")
SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")

PRODUCTION_DATA_ROOT = Path("/var/www/aibot/data")
PRODUCTION_BACKUP_ROOT = Path("/home/deploy/aibot-backups")
BACKUP_FILE_NAMES = (
    "signal_observations.json",
    "shadow_trades.json",
    "paper_trades.json",
    "wallet_performance.json",
    "global_metrics.json",
    "research_archive_metrics.json",
    "hypothesis_registry.json",
    "future_validation.json",
    "future_validation_manifest.json",
)
RPC_PROVIDER_NAMES = {"alchemy", "chainstack", "ankr", "helius", "solana_public"}
RPC_PROVIDER_STATE_KEYS = {
    "request_count",
    "success_count",
    "failure_count",
    "rate_limit_count",
    "circuit_open_count",
    "circuit_state",
}
MAX_MANIFEST_BYTES = 1024 * 1024
MAX_MARKER_BYTES = 4096
MAX_ARCHIVE_BUCKETS = 65536
MAX_ARCHIVE_RECORDS = 250000
MAX_RETENTION_ROOT_ENTRIES = 100000
MAX_PROC_ENTRIES = 1000000
HASH_CHUNK_BYTES = 1024 * 1024


class Classification(str, Enum):
    SAFE_TO_DELETE = "SAFE_TO_DELETE"
    PROTECTED = "PROTECTED"
    INVALID_DO_NOT_TOUCH = "INVALID_DO_NOT_TOUCH"
    UNKNOWN_DO_NOT_TOUCH = "UNKNOWN_DO_NOT_TOUCH"


class ValidationError(RuntimeError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class FileIdentity:
    device: int
    inode: int
    mode: int
    owner: int
    size: int
    modified_ns: int

    @classmethod
    def from_stat(cls, metadata: os.stat_result) -> "FileIdentity":
        return cls(
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_mode,
            metadata.st_uid,
            metadata.st_size,
            metadata.st_mtime_ns,
        )


@dataclass(frozen=True)
class ProcScan:
    opened: frozenset[tuple[int, int]]
    complete: bool


@dataclass
class Candidate:
    path: Path
    classification: Classification
    reason: str
    logical_bytes: int = 0
    allocated_bytes: int = 0
    identity: FileIdentity | None = None
    revision: str = ""


@dataclass
class RetentionPlan:
    candidates: list[Candidate]
    removed_count: int = 0
    removed_logical_bytes: int = 0
    removed_allocated_bytes: int = 0

    @property
    def safe_names(self) -> set[str]:
        return {
            item.path.name
            for item in self.candidates
            if item.classification == Classification.SAFE_TO_DELETE
        }

    def summary(self) -> dict[str, object]:
        classifications = Counter(item.classification.value for item in self.candidates)
        reasons: dict[str, dict[str, int]] = {}
        for item in self.candidates:
            if item.classification == Classification.SAFE_TO_DELETE:
                continue
            bucket = reasons.setdefault(
                item.reason,
                {"count": 0, "logical_bytes": 0, "allocated_bytes": 0},
            )
            bucket["count"] += 1
            bucket["logical_bytes"] += item.logical_bytes
            bucket["allocated_bytes"] += item.allocated_bytes
        eligible = [
            item
            for item in self.candidates
            if item.classification == Classification.SAFE_TO_DELETE
        ]
        validated = [item for item in self.candidates if item.revision]
        return {
            "total_count": len(self.candidates),
            "total_logical_bytes": sum(item.logical_bytes for item in self.candidates),
            "total_allocated_bytes": sum(item.allocated_bytes for item in self.candidates),
            "validated_count": len(validated),
            "eligible_count": len(eligible),
            "eligible_logical_bytes": sum(item.logical_bytes for item in eligible),
            "eligible_allocated_bytes": sum(item.allocated_bytes for item in eligible),
            "classification_counts": dict(sorted(classifications.items())),
            "excluded_by_reason": dict(sorted(reasons.items())),
            "protected_latest": sum(item.reason == "protected_latest" for item in self.candidates),
            "protected_active": sum(item.reason == "protected_active" for item in self.candidates),
            "protected_successful": sum(item.reason == "protected_successful" for item in self.candidates),
            "removed_count": self.removed_count,
            "removed_logical_bytes": self.removed_logical_bytes,
            "removed_allocated_bytes": self.removed_allocated_bytes,
        }


def _allocated_bytes(metadata: os.stat_result) -> int:
    blocks = getattr(metadata, "st_blocks", None)
    return blocks * 512 if isinstance(blocks, int) else metadata.st_size


def _strict_root(path: Path) -> Path:
    if path.is_symlink():
        raise RuntimeError("retention root must not be a symlink")
    root = path.resolve(strict=True)
    if not root.is_dir():
        raise RuntimeError("retention root must be a real directory")
    return root


def _require_cli_root(path: Path, expected: Path) -> Path:
    root = _strict_root(path)
    if root != expected.resolve(strict=True):
        raise RuntimeError("retention root is not the fixed production root")
    return root


def _validate_sha(value: str, label: str) -> str:
    if not SHA_PATTERN.fullmatch(value):
        raise ValueError(f"{label} must be a lowercase 40-character SHA")
    return value


def _bounded_children(path: Path, limit: int, reason: str) -> list[Path]:
    children: list[Path] = []
    try:
        for child in path.iterdir():
            if len(children) >= limit:
                raise ValidationError(reason)
            children.append(child)
    except ValidationError:
        raise
    except OSError as exc:
        raise ValidationError("unknown") from exc
    return sorted(children, key=lambda item: item.name)


def _proc_children(path: Path, *, disappeared_ok: bool) -> tuple[list[Path], bool]:
    children: list[Path] = []
    try:
        for child in path.iterdir():
            if len(children) >= MAX_PROC_ENTRIES:
                return children, False
            children.append(child)
    except FileNotFoundError:
        return ([], True) if disappeared_ok else ([], False)
    except (PermissionError, OSError) as exc:
        if disappeared_ok and isinstance(exc, OSError) and exc.errno in {
            errno.ENOENT,
            errno.ESRCH,
        }:
            return [], True
        return [], False
    return children, True


def scan_open_regular_files(proc_root: Path = Path("/proc")) -> ProcScan:
    opened: set[tuple[int, int]] = set()
    if not proc_root.is_dir():
        return ProcScan(frozenset(), False)
    processes, complete = _proc_children(proc_root, disappeared_ok=False)
    for process in processes:
        if not process.name.isdigit():
            continue
        entries, readable = _proc_children(process / "fd", disappeared_ok=True)
        if not readable:
            complete = False
            continue
        for descriptor in entries:
            try:
                item = descriptor.stat()
            except FileNotFoundError:
                continue
            except (PermissionError, OSError) as exc:
                if isinstance(exc, OSError) and exc.errno in {errno.ENOENT, errno.ESRCH}:
                    continue
                complete = False
                continue
            if stat.S_ISREG(item.st_mode):
                opened.add((item.st_dev, item.st_ino))
    return ProcScan(frozenset(opened), complete)


def plan_legacy_temporary_files(
    data_root: Path,
    *,
    minimum_age_seconds: int,
    now: float | None = None,
    proc_scan: ProcScan | None = None,
    owner_uid: int | None = None,
) -> RetentionPlan:
    root = _strict_root(data_root)
    cutoff = (time.time() if now is None else now) - minimum_age_seconds
    scan = scan_open_regular_files() if proc_scan is None else proc_scan
    expected_owner = os.getuid() if owner_uid is None and hasattr(os, "getuid") else owner_uid
    candidates: list[Candidate] = []
    for candidate in _bounded_children(
        root, MAX_RETENTION_ROOT_ENTRIES, "too_many_root_entries"
    ):
        if not candidate.name.startswith("tmp"):
            continue
        try:
            item = candidate.lstat()
        except OSError:
            candidates.append(Candidate(candidate, Classification.UNKNOWN_DO_NOT_TOUCH, "unknown"))
            continue
        logical = item.st_size
        allocated = _allocated_bytes(item)
        if not LEGACY_TEMP_PATTERN.fullmatch(candidate.name):
            candidates.append(Candidate(candidate, Classification.INVALID_DO_NOT_TOUCH, "invalid_name", logical, allocated))
            continue
        if stat.S_ISLNK(item.st_mode):
            classification, reason = Classification.INVALID_DO_NOT_TOUCH, "symlink"
        elif not stat.S_ISREG(item.st_mode):
            classification, reason = Classification.INVALID_DO_NOT_TOUCH, "non_regular"
        elif expected_owner is not None and item.st_uid != expected_owner:
            classification, reason = Classification.PROTECTED, "owner_mismatch"
        elif item.st_mtime > cutoff:
            classification, reason = Classification.PROTECTED, "too_young"
        elif (item.st_dev, item.st_ino) in scan.opened:
            classification, reason = Classification.PROTECTED, "open"
        elif not scan.complete:
            classification, reason = Classification.UNKNOWN_DO_NOT_TOUCH, "proc_unknown"
        else:
            classification, reason = Classification.SAFE_TO_DELETE, "eligible"
        candidates.append(
            Candidate(
                candidate,
                classification,
                reason,
                logical,
                allocated,
                FileIdentity.from_stat(item),
            )
        )
    return RetentionPlan(candidates)


def _execute_temporary_plan(
    plan: RetentionPlan,
    *,
    proc_scanner: Callable[[], ProcScan],
    before_delete: Callable[[Path], None] | None = None,
) -> None:
    for candidate in plan.candidates:
        if candidate.classification != Classification.SAFE_TO_DELETE:
            continue
        if before_delete is not None:
            before_delete(candidate.path)
        try:
            current = candidate.path.lstat()
        except OSError:
            candidate.classification = Classification.UNKNOWN_DO_NOT_TOUCH
            candidate.reason = "changed_during_validation"
            continue
        if candidate.identity != FileIdentity.from_stat(current):
            candidate.classification = Classification.UNKNOWN_DO_NOT_TOUCH
            candidate.reason = "changed_during_validation"
            continue
        scan = proc_scanner()
        identity = (current.st_dev, current.st_ino)
        if not scan.complete:
            candidate.classification = Classification.UNKNOWN_DO_NOT_TOUCH
            candidate.reason = "proc_unknown"
            continue
        if identity in scan.opened:
            candidate.classification = Classification.PROTECTED
            candidate.reason = "open"
            continue
        try:
            final = candidate.path.lstat()
        except OSError:
            candidate.classification = Classification.UNKNOWN_DO_NOT_TOUCH
            candidate.reason = "changed_during_validation"
            continue
        if candidate.identity != FileIdentity.from_stat(final):
            candidate.classification = Classification.UNKNOWN_DO_NOT_TOUCH
            candidate.reason = "changed_during_validation"
            continue
        candidate.path.unlink()
        plan.removed_count += 1
        plan.removed_logical_bytes += candidate.logical_bytes
        plan.removed_allocated_bytes += candidate.allocated_bytes


def cleanup_legacy_temporary_files(
    data_root: Path,
    *,
    minimum_age_seconds: int,
    now: float | None = None,
    open_files: set[tuple[int, int]] | None = None,
    dry_run: bool = False,
    proc_scanner: Callable[[], ProcScan] | None = None,
    before_delete: Callable[[Path], None] | None = None,
) -> tuple[int, int]:
    scanner = proc_scanner or scan_open_regular_files
    initial_scan = ProcScan(frozenset(open_files), True) if open_files is not None else scanner()
    plan = plan_legacy_temporary_files(
        data_root,
        minimum_age_seconds=minimum_age_seconds,
        now=now,
        proc_scan=initial_scan,
    )
    if not dry_run:
        _execute_temporary_plan(
            plan,
            proc_scanner=(lambda: initial_scan) if open_files is not None else scanner,
            before_delete=before_delete,
        )
    return plan.removed_count, plan.removed_logical_bytes


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        while chunk := source.read(HASH_CHUNK_BYTES):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _regular_file(path: Path, reason: str) -> os.stat_result:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise ValidationError(reason) from exc
    except OSError as exc:
        raise ValidationError("unknown") from exc
    if stat.S_ISLNK(metadata.st_mode):
        raise ValidationError("symlink")
    if not stat.S_ISREG(metadata.st_mode):
        raise ValidationError(reason)
    return metadata


def _load_manifest(candidate: Path) -> tuple[dict[str, object], os.stat_result]:
    manifest_path = candidate / "manifest.json"
    metadata = _regular_file(manifest_path, "manifest_missing")
    if metadata.st_size > MAX_MANIFEST_BYTES:
        raise ValidationError("invalid_manifest")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValidationError("invalid_manifest") from exc
    if not isinstance(manifest, dict):
        raise ValidationError("invalid_manifest")
    return manifest, metadata


def _validate_manifest_metadata(manifest: dict[str, object]) -> None:
    if "created_at" in manifest:
        value = manifest["created_at"]
        if not isinstance(value, str):
            raise ValidationError("invalid_manifest")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValidationError("invalid_manifest") from exc
        if parsed.tzinfo is None:
            raise ValidationError("invalid_manifest")
    if "created_at_epoch" in manifest:
        value = manifest["created_at_epoch"]
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or value <= 0
        ):
            raise ValidationError("invalid_manifest")
    provider_states = manifest.get("rpc_provider_states")
    if provider_states is not None:
        if not isinstance(provider_states, dict) or not set(provider_states).issubset(
            RPC_PROVIDER_NAMES
        ):
            raise ValidationError("invalid_manifest")
        for state in provider_states.values():
            if not isinstance(state, dict) or not set(state).issubset(
                RPC_PROVIDER_STATE_KEYS
            ):
                raise ValidationError("invalid_manifest")


def _archive_files(root: Path) -> Iterable[Path]:
    if root.is_symlink():
        raise ValidationError("symlink")
    if not root.exists():
        return
    if not root.is_dir():
        raise ValidationError("invalid_manifest")
    buckets = _bounded_children(root, MAX_ARCHIVE_BUCKETS, "archive_too_large")
    record_count = 0
    for bucket in buckets:
        if bucket.is_symlink() or not bucket.is_dir():
            raise ValidationError("symlink" if bucket.is_symlink() else "unexpected_entry")
        records = _bounded_children(
            bucket, MAX_ARCHIVE_RECORDS - record_count, "archive_too_large"
        )
        if not records:
            raise ValidationError("unexpected_entry")
        for record in records:
            record_count += 1
            if record_count > MAX_ARCHIVE_RECORDS:
                raise ValidationError("archive_too_large")
            if record.suffix != ".json":
                raise ValidationError("unexpected_entry")
            _regular_file(record, "unexpected_entry")
            yield record


def _validate_research_archive(
    candidate: Path,
    archive_manifest: object,
) -> tuple[int, int]:
    if not isinstance(archive_manifest, dict):
        raise ValidationError("invalid_manifest")
    expected_count = archive_manifest.get("record_count")
    expected_size = archive_manifest.get("size_bytes")
    expected_digest = archive_manifest.get("tree_sha256")
    if (
        not isinstance(expected_count, int)
        or isinstance(expected_count, bool)
        or expected_count < 0
        or not isinstance(expected_size, int)
        or isinstance(expected_size, bool)
        or expected_size < 0
        or not isinstance(expected_digest, str)
        or not SHA256_PATTERN.fullmatch(expected_digest)
    ):
        raise ValidationError("invalid_manifest")
    archive_parent = candidate / "research_archive"
    archive_root = archive_parent / "records"
    if archive_parent.is_symlink():
        raise ValidationError("symlink")
    if expected_count == 0 and archive_parent.exists():
        raise ValidationError("unexpected_entry")
    if archive_parent.exists():
        if not archive_parent.is_dir():
            raise ValidationError("unexpected_entry")
        if any(
            child.name != "records"
            for child in _bounded_children(archive_parent, 2, "unexpected_entry")
        ):
            raise ValidationError("unexpected_entry")
    digest = hashlib.sha256()
    count = 0
    size = 0
    allocated = 0
    for path in _archive_files(archive_root):
        checksum, length = _hash_file(path)
        relative = path.relative_to(archive_root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(checksum.encode("ascii"))
        count += 1
        size += length
        allocated += _allocated_bytes(path.lstat())
    if count != expected_count:
        raise ValidationError("archive_count_mismatch")
    if size != expected_size:
        raise ValidationError("archive_size_mismatch")
    if digest.hexdigest() != expected_digest:
        raise ValidationError("archive_digest_mismatch")
    return size, allocated


def _validate_backup(candidate: Path, name_revision: str) -> tuple[str, int, int]:
    manifest, manifest_stat = _load_manifest(candidate)
    revision = manifest.get("deploy_sha")
    if (
        not isinstance(revision, str)
        or not SHA_PATTERN.fullmatch(revision)
        or not revision.startswith(name_revision)
    ):
        raise ValidationError("deploy_sha_mismatch")
    _validate_manifest_metadata(manifest)
    files = manifest.get("files")
    if not isinstance(files, dict) or set(files) != set(BACKUP_FILE_NAMES):
        raise ValidationError("invalid_manifest")
    logical = manifest_stat.st_size
    allocated = _allocated_bytes(manifest_stat)
    expected_top_level = {"manifest.json"}
    for name in BACKUP_FILE_NAMES:
        item = files.get(name)
        if not isinstance(item, dict) or not isinstance(item.get("exists"), bool):
            raise ValidationError("invalid_manifest")
        if "summary" in item and not isinstance(item["summary"], dict):
            raise ValidationError("invalid_manifest")
        path = candidate / name
        if not item["exists"]:
            if path.exists() or path.is_symlink():
                raise ValidationError("unexpected_entry")
            continue
        expected_top_level.add(name)
        metadata = _regular_file(path, "missing_file")
        expected_size = item.get("size_bytes")
        expected_checksum = item.get("sha256")
        if (
            not isinstance(expected_size, int)
            or isinstance(expected_size, bool)
            or expected_size < 0
            or not isinstance(expected_checksum, str)
            or not SHA256_PATTERN.fullmatch(expected_checksum)
        ):
            raise ValidationError("invalid_manifest")
        checksum, size = _hash_file(path)
        if size != expected_size:
            raise ValidationError("size_mismatch")
        if checksum != expected_checksum:
            raise ValidationError("checksum_mismatch")
        logical += size
        allocated += _allocated_bytes(metadata)
    archive_size, archive_allocated = _validate_research_archive(
        candidate, manifest.get("research_archive")
    )
    if archive_size or (candidate / "research_archive").exists():
        expected_top_level.add("research_archive")
    logical += archive_size
    allocated += archive_allocated
    try:
        actual_top_level = {item.name for item in candidate.iterdir()}
    except OSError as exc:
        raise ValidationError("unknown") from exc
    if actual_top_level != expected_top_level:
        raise ValidationError("unexpected_entry")
    return revision, logical, allocated


def _measure_regular_tree(root: Path) -> tuple[int, int]:
    root_stat = root.lstat()
    device = root_stat.st_dev
    directories = [root]
    entries_seen = 0
    logical = 0
    allocated = 0
    while directories:
        directory = directories.pop()
        for child in _bounded_children(
            directory, MAX_RETENTION_ROOT_ENTRIES, "too_many_backup_entries"
        ):
            entries_seen += 1
            if entries_seen > MAX_ARCHIVE_RECORDS + len(BACKUP_FILE_NAMES) + 4:
                raise ValidationError("too_many_backup_entries")
            try:
                metadata = child.lstat()
            except OSError as exc:
                raise ValidationError("unknown") from exc
            if metadata.st_dev != device or stat.S_ISLNK(metadata.st_mode):
                continue
            if stat.S_ISDIR(metadata.st_mode):
                directories.append(child)
            elif stat.S_ISREG(metadata.st_mode):
                logical += metadata.st_size
                allocated += _allocated_bytes(metadata)
    return logical, allocated


def _marker_target(root: Path, marker: Path, marker_sha: str) -> Path:
    metadata = _regular_file(marker, "unknown_marker_state")
    if metadata.st_size > MAX_MARKER_BYTES:
        raise ValidationError("unknown_marker_state")
    try:
        raw = marker.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError) as exc:
        raise ValidationError("unknown_marker_state") from exc
    target_path = Path(raw)
    if not target_path.is_absolute():
        raise ValidationError("unknown_marker_state")
    try:
        target = target_path.resolve(strict=True)
    except OSError as exc:
        raise ValidationError("unknown_marker_state") from exc
    if target.parent != root:
        raise ValidationError("unknown_marker_state")
    match = BACKUP_PATTERN.fullmatch(target.name)
    if match is None or match.group(2) != marker_sha[:12]:
        raise ValidationError("unknown_marker_state")
    return target


def _validate_markers(
    root: Path,
    valid_by_path: dict[Path, Candidate],
    required_shas: set[str],
) -> None:
    markers: dict[str, Path] = {}
    for entry in _bounded_children(
        root, MAX_RETENTION_ROOT_ENTRIES, "too_many_root_entries"
    ):
        if not entry.name.startswith("latest-"):
            continue
        match = MARKER_PATTERN.fullmatch(entry.name)
        if match is None:
            raise ValidationError("unknown_marker_state")
        sha = match.group(1)
        target = _marker_target(root, entry, sha)
        record = valid_by_path.get(target)
        if record is None or record.revision != sha:
            raise ValidationError("unknown_marker_state")
        markers[sha] = target
    if not required_shas.issubset(markers):
        raise ValidationError("unknown_marker_state")


def plan_validated_backups(
    backup_root: Path,
    *,
    keep: int,
    deployed_sha: str,
    active_source_sha: str,
) -> RetentionPlan:
    if keep < 1:
        raise ValueError("keep must be at least one")
    deployed = _validate_sha(deployed_sha, "deployed_sha")
    active = _validate_sha(active_source_sha, "active_source_sha")
    root = _strict_root(backup_root)
    candidates: list[Candidate] = []
    exact_named: list[Candidate] = []
    root_entries = _bounded_children(
        root, MAX_RETENTION_ROOT_ENTRIES, "too_many_root_entries"
    )
    for candidate in reversed(root_entries):
        if not candidate.name.startswith("predeploy-"):
            continue
        match = BACKUP_PATTERN.fullmatch(candidate.name)
        if match is None:
            candidates.append(Candidate(candidate, Classification.INVALID_DO_NOT_TOUCH, "invalid_name"))
            continue
        try:
            metadata = candidate.lstat()
        except OSError:
            record = Candidate(candidate, Classification.UNKNOWN_DO_NOT_TOUCH, "unknown")
        else:
            if stat.S_ISLNK(metadata.st_mode):
                record = Candidate(candidate, Classification.INVALID_DO_NOT_TOUCH, "symlink")
            elif not stat.S_ISDIR(metadata.st_mode):
                record = Candidate(candidate, Classification.INVALID_DO_NOT_TOUCH, "non_directory", metadata.st_size, _allocated_bytes(metadata))
            else:
                try:
                    revision, logical, allocated = _validate_backup(candidate, match.group(2))
                except ValidationError as exc:
                    try:
                        logical, allocated = _measure_regular_tree(candidate)
                    except (OSError, ValidationError):
                        logical, allocated = 0, 0
                    record = Candidate(
                        candidate,
                        Classification.UNKNOWN_DO_NOT_TOUCH if exc.reason == "unknown" else Classification.INVALID_DO_NOT_TOUCH,
                        exc.reason,
                        logical,
                        allocated,
                    )
                else:
                    record = Candidate(candidate, Classification.SAFE_TO_DELETE, "eligible", logical, allocated, revision=revision)
        candidates.append(record)
        exact_named.append(record)

    valid_by_path = {
        item.path.resolve(strict=False): item
        for item in exact_named
        if item.classification == Classification.SAFE_TO_DELETE
    }
    try:
        _validate_markers(root, valid_by_path, {deployed, active})
    except ValidationError:
        for item in exact_named:
            if item.classification == Classification.SAFE_TO_DELETE:
                item.classification = Classification.UNKNOWN_DO_NOT_TOUCH
                item.reason = "unknown_marker_state"
        return RetentionPlan(candidates)

    latest_paths = {item.path for item in exact_named[:keep]}
    for item in exact_named:
        if item.classification != Classification.SAFE_TO_DELETE:
            continue
        if item.revision == active:
            item.classification = Classification.PROTECTED
            item.reason = "protected_active"
        elif item.revision == deployed:
            item.classification = Classification.PROTECTED
            item.reason = "protected_successful"
        elif item.path in latest_paths:
            item.classification = Classification.PROTECTED
            item.reason = "protected_latest"
    return RetentionPlan(candidates)


def _remove_matching_marker(root: Path, candidate: Candidate) -> None:
    marker = root / f"latest-{candidate.revision}.txt"
    if not marker.exists() or marker.is_symlink() or not marker.is_file():
        return
    try:
        target = Path(marker.read_text(encoding="utf-8").strip()).resolve(strict=False)
    except (OSError, UnicodeDecodeError):
        return
    if target == candidate.path.resolve(strict=False):
        marker.unlink()


def _execute_backup_plan(
    plan: RetentionPlan,
    backup_root: Path,
    *,
    keep: int,
    deployed_sha: str,
    active_source_sha: str,
    before_delete: Callable[[Path], None] | None = None,
) -> None:
    root = _strict_root(backup_root)
    for candidate in plan.candidates:
        if candidate.classification != Classification.SAFE_TO_DELETE:
            continue
        if before_delete is not None:
            before_delete(candidate.path)
        refreshed = plan_validated_backups(
            root,
            keep=keep,
            deployed_sha=deployed_sha,
            active_source_sha=active_source_sha,
        )
        current = next((item for item in refreshed.candidates if item.path.name == candidate.path.name), None)
        if current is None or current.classification != Classification.SAFE_TO_DELETE:
            candidate.classification = Classification.UNKNOWN_DO_NOT_TOUCH
            candidate.reason = "changed_during_validation"
            continue
        shutil.rmtree(candidate.path)
        _remove_matching_marker(root, candidate)
        plan.removed_count += 1
        plan.removed_logical_bytes += candidate.logical_bytes
        plan.removed_allocated_bytes += candidate.allocated_bytes


def retain_validated_backups(
    backup_root: Path,
    *,
    keep: int,
    deployed_sha: str | None = None,
    active_source_sha: str | None = None,
    dry_run: bool = False,
    before_delete: Callable[[Path], None] | None = None,
) -> tuple[int, int]:
    if deployed_sha is None or active_source_sha is None:
        raise ValueError("backup retention requires deployed and active source SHAs")
    plan = plan_validated_backups(
        backup_root,
        keep=keep,
        deployed_sha=deployed_sha,
        active_source_sha=active_source_sha,
    )
    if not dry_run:
        _execute_backup_plan(
            plan,
            backup_root,
            keep=keep,
            deployed_sha=deployed_sha,
            active_source_sha=active_source_sha,
            before_delete=before_delete,
        )
    return plan.removed_count, plan.removed_logical_bytes


def _emit_inventory(
    *,
    dry_run: bool,
    temporary: RetentionPlan | None,
    backups: RetentionPlan | None,
) -> None:
    payload: dict[str, object] = {"dry_run": dry_run}
    if temporary is not None:
        payload["tmp"] = temporary.summary()
    if backups is not None:
        payload["backup"] = backups.summary()
    print("RETENTION_INVENTORY " + json.dumps(payload, sort_keys=True, separators=(",", ":")))


def main() -> None:
    parser = argparse.ArgumentParser(description="Aibot bounded storage retention")
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--legacy-temp-min-age-seconds", type=int, default=600)
    parser.add_argument("--backup-root", type=Path)
    parser.add_argument("--keep-backups", type=int, default=3)
    parser.add_argument("--deployed-sha")
    parser.add_argument("--active-source-sha")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.data_root is None and args.backup_root is None:
        parser.error("at least one retention root is required")

    temporary_plan = None
    backup_plan = None
    if args.data_root is not None:
        data_root = _require_cli_root(args.data_root, PRODUCTION_DATA_ROOT)
        initial_scan = scan_open_regular_files()
        temporary_plan = plan_legacy_temporary_files(
            data_root,
            minimum_age_seconds=max(0, args.legacy_temp_min_age_seconds),
            proc_scan=initial_scan,
        )
        if not args.dry_run:
            _execute_temporary_plan(temporary_plan, proc_scanner=scan_open_regular_files)
    if args.backup_root is not None:
        if args.deployed_sha is None or args.active_source_sha is None:
            parser.error("backup retention requires both protected SHA inputs")
        backup_root = _require_cli_root(args.backup_root, PRODUCTION_BACKUP_ROOT)
        backup_plan = plan_validated_backups(
            backup_root,
            keep=args.keep_backups,
            deployed_sha=args.deployed_sha,
            active_source_sha=args.active_source_sha,
        )
        if not args.dry_run:
            _execute_backup_plan(
                backup_plan,
                backup_root,
                keep=args.keep_backups,
                deployed_sha=args.deployed_sha,
                active_source_sha=args.active_source_sha,
            )
    _emit_inventory(dry_run=args.dry_run, temporary=temporary_plan, backups=backup_plan)


if __name__ == "__main__":
    main()
