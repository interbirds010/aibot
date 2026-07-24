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


class VersionConflict(RuntimeError):
    """Raised when a caller attempts to update a stale document revision."""


class StateLockTimeout(TimeoutError):
    """Raised when another process holds a state lock beyond the deadline."""


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
            json.dump(document, file, ensure_ascii=False, indent=2)
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
