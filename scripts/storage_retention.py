"""Aibot의 검증된 배포 backup과 중단된 legacy 임시 파일만 정리한다."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import stat
import time
from pathlib import Path


LEGACY_TEMP_PATTERN = re.compile(r"tmp[a-zA-Z0-9_]{8}")
BACKUP_PATTERN = re.compile(r"predeploy-(\d{8}T\d{6}Z)-([0-9a-f]{12})")


def _open_regular_files() -> set[tuple[int, int]]:
    opened: set[tuple[int, int]] = set()
    proc = Path("/proc")
    if not proc.is_dir():
        return opened
    for process in proc.iterdir():
        if not process.name.isdigit():
            continue
        descriptors = process / "fd"
        try:
            entries = list(descriptors.iterdir())
        except (FileNotFoundError, PermissionError, OSError):
            continue
        for descriptor in entries:
            try:
                item = descriptor.stat()
            except (FileNotFoundError, PermissionError, OSError):
                continue
            if stat.S_ISREG(item.st_mode):
                opened.add((item.st_dev, item.st_ino))
    return opened


def cleanup_legacy_temporary_files(
    data_root: Path,
    *,
    minimum_age_seconds: int,
    now: float | None = None,
    open_files: set[tuple[int, int]] | None = None,
) -> tuple[int, int]:
    root = data_root.resolve(strict=True)
    if root.is_symlink() or not root.is_dir():
        raise RuntimeError("data root must be a real directory")
    cutoff = (time.time() if now is None else now) - minimum_age_seconds
    opened = _open_regular_files() if open_files is None else open_files
    removed_count = 0
    removed_bytes = 0
    for candidate in root.iterdir():
        if not LEGACY_TEMP_PATTERN.fullmatch(candidate.name):
            continue
        item = candidate.lstat()
        if candidate.is_symlink() or not stat.S_ISREG(item.st_mode):
            continue
        current_uid = os.getuid() if hasattr(os, "getuid") else item.st_uid
        if item.st_uid != current_uid or item.st_mtime > cutoff:
            continue
        if (item.st_dev, item.st_ino) in opened:
            continue
        verified = candidate.resolve(strict=True)
        if verified.parent != root or verified != candidate.absolute():
            raise RuntimeError("legacy temporary file escaped data root")
        candidate.unlink()
        removed_count += 1
        removed_bytes += item.st_size
        print(f"RETENTION_TEMP name={candidate.name} bytes={item.st_size}")
    return removed_count, removed_bytes


def _validated_backups(backup_root: Path) -> list[tuple[Path, str, int]]:
    root = backup_root.resolve(strict=True)
    if root.is_symlink() or not root.is_dir():
        raise RuntimeError("backup root must be a real directory")
    result: list[tuple[Path, str, int]] = []
    for candidate in root.iterdir():
        match = BACKUP_PATTERN.fullmatch(candidate.name)
        if not match:
            continue
        if candidate.is_symlink() or not candidate.is_dir():
            raise RuntimeError(f"unsafe backup candidate: {candidate.name}")
        verified = candidate.resolve(strict=True)
        if verified.parent != root or verified != candidate.absolute():
            raise RuntimeError("backup candidate escaped backup root")
        manifest_path = candidate / "manifest.json"
        if manifest_path.is_symlink() or not manifest_path.is_file():
            raise RuntimeError(f"backup manifest missing: {candidate.name}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        revision = str(manifest.get("deploy_sha") or "")
        if not re.fullmatch(r"[0-9a-f]{40}", revision) or not revision.startswith(match.group(2)):
            raise RuntimeError(f"backup manifest revision mismatch: {candidate.name}")
        total_bytes = 0
        for parent, directories, files in os.walk(candidate, followlinks=False):
            parent_path = Path(parent)
            for directory in directories:
                if (parent_path / directory).is_symlink():
                    raise RuntimeError(f"backup contains a symlink: {candidate.name}")
            for name in files:
                child = parent_path / name
                if child.is_symlink():
                    raise RuntimeError(f"backup contains a symlink: {candidate.name}")
                total_bytes += child.stat().st_size
        result.append((candidate, revision, total_bytes))
    return sorted(result, key=lambda item: item[0].name, reverse=True)


def retain_validated_backups(backup_root: Path, *, keep: int) -> tuple[int, int]:
    if keep < 1:
        raise ValueError("keep must be at least one")
    backups = _validated_backups(backup_root)
    removed_count = 0
    removed_bytes = 0
    for candidate, revision, size_bytes in backups[keep:]:
        shutil.rmtree(candidate)
        marker = backup_root / f"latest-{revision}.txt"
        if marker.is_file() and not marker.is_symlink():
            try:
                target = Path(marker.read_text(encoding="utf-8").strip()).resolve(strict=False)
            except OSError:
                target = Path()
            if target == candidate.resolve(strict=False):
                marker.unlink()
        removed_count += 1
        removed_bytes += size_bytes
        print(f"RETENTION_BACKUP name={candidate.name} bytes={size_bytes}")
    return removed_count, removed_bytes


def main() -> None:
    parser = argparse.ArgumentParser(description="Aibot bounded storage retention")
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--legacy-temp-min-age-seconds", type=int, default=600)
    parser.add_argument("--backup-root", type=Path)
    parser.add_argument("--keep-backups", type=int, default=3)
    args = parser.parse_args()
    if args.data_root is None and args.backup_root is None:
        parser.error("at least one retention root is required")
    total_count = 0
    total_bytes = 0
    if args.data_root is not None:
        count, size_bytes = cleanup_legacy_temporary_files(
            args.data_root,
            minimum_age_seconds=max(0, args.legacy_temp_min_age_seconds),
        )
        total_count += count
        total_bytes += size_bytes
    if args.backup_root is not None:
        count, size_bytes = retain_validated_backups(
            args.backup_root,
            keep=args.keep_backups,
        )
        total_count += count
        total_bytes += size_bytes
    print(f"RETENTION_SUMMARY removed={total_count} bytes={total_bytes}")


if __name__ == "__main__":
    main()
