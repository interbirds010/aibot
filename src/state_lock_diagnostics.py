"""Bounded failure-time diagnostics for state-store file locks."""

from __future__ import annotations

import copy
import itertools
import json
import os
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO

if os.name == "nt":
    import msvcrt
else:  # pragma: no cover - exercised by Linux tests/deployment.
    import fcntl


ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_PATH = ROOT / "logs" / "state_lock_diagnostic.json"
MAX_SNAPSHOT_BYTES = 64 * 1024
MAX_TRACE_THREADS = 32
MAX_TRACE_FRAMES = 32
MAX_RECENT_EVENTS = 8
MAX_PROC_LOCK_LINES = 4096
MAX_PROC_LOCK_BYTES = 512 * 1024
MAX_PROC_LOCK_LINE_BYTES = 4096
SNAPSHOT_LOCK_TIMEOUT_SECONDS = 0.05
SNAPSHOT_LOCK_RETRY_SECONDS = 0.005
# 기본 50ms poll 20회에 해당해 15초 timeout 전에 비정상 대기를 식별한다.
SLOW_ACQUIRE_SECONDS = 1.0
# Timeout의 1/3 이상 점유만 남겨 정상 JSON write의 noise를 피한다.
SLOW_HOLD_SECONDS = 5.0

_ATTEMPT_COUNTER = itertools.count(1)
_LOCAL_HOLDER_GUARD = threading.Lock()
_LOCAL_HOLDERS: dict[tuple[int, int], dict[str, Any]] = {}
_SNAPSHOT_GUARD = threading.Lock()


def _reset_after_fork() -> None:
    global _LOCAL_HOLDER_GUARD
    global _LOCAL_HOLDERS
    global _SNAPSHOT_GUARD
    _LOCAL_HOLDERS = {}
    _LOCAL_HOLDER_GUARD = threading.Lock()
    _SNAPSHOT_GUARD = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)


@dataclass(slots=True)
class LockAttempt:
    attempt_id: str
    operation: str
    waiter_pid: int
    waiter_thread: int
    started_ns: int
    wait_ms: float = 0.0
    hold_started_ns: int | None = None
    holder_key: tuple[int, int] | None = None


def _normalized_operation(operation: str | None) -> str:
    candidate = str(operation or "unspecified").strip().lower()
    if not candidate or len(candidate) > 64:
        return "unspecified"
    allowed = "abcdefghijklmnopqrstuvwxyz0123456789_"
    if any(character not in allowed for character in candidate):
        return "unspecified"
    return candidate


def begin(operation: str | None) -> LockAttempt:
    """Create cheap process-local attempt metadata for every acquisition."""
    waiter_pid = os.getpid()
    waiter_thread = threading.get_ident()
    return LockAttempt(
        attempt_id=f"{waiter_pid}-{waiter_thread}-{next(_ATTEMPT_COUNTER)}",
        operation=_normalized_operation(operation),
        waiter_pid=waiter_pid,
        waiter_thread=waiter_thread,
        started_ns=time.monotonic_ns(),
    )


def _process_start_identity(
    pid: int, proc_root: Path = Path("/proc")
) -> tuple[str | None, str]:
    """Return boot-scoped start identity and ok/missing/unavailable status."""
    try:
        boot_id = (proc_root / "sys/kernel/random/boot_id").read_text(
            encoding="utf-8"
        ).strip()
        stat_line = (proc_root / str(pid) / "stat").read_text(
            encoding="utf-8"
        ).strip()
    except FileNotFoundError:
        return None, "missing"
    except OSError:
        return None, "unavailable"
    closing_paren = stat_line.rfind(")")
    fields = stat_line[closing_paren + 2 :].split()
    try:
        if closing_paren < 0 or len(fields) <= 19 or not boot_id:
            return None, "unavailable"
        return f"{boot_id}:{int(fields[19])}", "ok"
    except (ValueError, IndexError):
        return None, "unavailable"


def _parse_proc_lock_line(line: str) -> tuple[int, int, int, int] | None:
    """Parse one Linux FLOCK WRITE entry, including blocked-entry layouts."""
    fields = line.split()
    try:
        lock_index = fields.index("FLOCK")
        if lock_index > 0 and fields[lock_index - 1] == "->":
            return None
        if fields[lock_index + 2] != "WRITE":
            return None
        holder_pid = int(fields[lock_index + 3])
        if holder_pid <= 0:
            return None
        major_text, minor_text, inode_text = fields[lock_index + 4].split(":", 2)
        return (
            holder_pid,
            int(major_text, 16),
            int(minor_text, 16),
            int(inode_text),
        )
    except (ValueError, IndexError):
        return None


def _find_linux_lock_holder(
    device: int,
    inode: int,
    *,
    proc_root: Path = Path("/proc"),
) -> tuple[int | None, bool]:
    """Return a matching PID and whether the bounded kernel scan was usable."""
    expected_major = os.major(device)
    expected_minor = os.minor(device)
    consumed_bytes = 0
    try:
        with open(
            proc_root / "locks",
            "r",
            encoding="utf-8",
            errors="replace",
        ) as source:
            line_number = 0
            while line_number < MAX_PROC_LOCK_LINES:
                line = source.readline(MAX_PROC_LOCK_LINE_BYTES + 1)
                if not line:
                    break
                line_number += 1
                if len(line.encode("utf-8", errors="replace")) > MAX_PROC_LOCK_LINE_BYTES:
                    return None, False
                consumed_bytes += len(line.encode("utf-8", errors="replace"))
                if consumed_bytes > MAX_PROC_LOCK_BYTES:
                    return None, False
                parsed = _parse_proc_lock_line(line)
                if parsed is None:
                    continue
                holder_pid, major, minor, entry_inode = parsed
                if (
                    major == expected_major
                    and minor == expected_minor
                    and entry_inode == inode
                ):
                    return holder_pid, True
    except OSError:
        return None, False
    return None, True


def _read_linux_process_metadata(
    pid: int,
    *,
    proc_root: Path = Path("/proc"),
) -> tuple[dict[str, Any] | None, str]:
    """Read bounded holder metadata without argv, environ, or payload values."""
    start_identity, identity_status = _process_start_identity(pid, proc_root)
    if identity_status != "ok" or start_identity is None:
        return None, identity_status
    try:
        stat_line = (proc_root / str(pid) / "stat").read_text(
            encoding="utf-8"
        ).strip()
        closing_paren = stat_line.rfind(")")
        fields = stat_line[closing_paren + 2 :].split()
        if closing_paren < 0 or not fields:
            return None, "unavailable"
    except FileNotFoundError:
        return None, "missing"
    except OSError:
        return None, "unavailable"
    uid: int | None = None
    try:
        with open(
            proc_root / str(pid) / "status",
            "r",
            encoding="utf-8",
            errors="replace",
        ) as status_file:
            for line_number, line in enumerate(status_file, 1):
                if line_number > 256:
                    break
                if line.startswith("Uid:"):
                    uid = int(line.split()[1])
                    break
    except (OSError, ValueError, IndexError):
        uid = None
    try:
        wchan = (proc_root / str(pid) / "wchan").read_text(
            encoding="utf-8"
        )[:128].strip()
    except OSError:
        wchan = "unavailable"
    allowed_wchan = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-."
    if any(character not in allowed_wchan for character in wchan):
        wchan = "unavailable"
    return {
        "holder_process_start": start_identity,
        "holder_state": fields[0][:1],
        "holder_wchan": wchan or "unavailable",
        "holder_uid": uid,
    }, "ok"


def _inspect_linux_holder(
    handle: BinaryIO,
    *,
    waiter_pid: int,
    proc_root: Path = Path("/proc"),
) -> dict[str, Any]:
    """Verify a kernel holder using two scans and boot-scoped PID identity."""
    result: dict[str, Any] = {
        "holder_pid": None,
        "holder_process_start": None,
        "holder_state": None,
        "holder_wchan": None,
        "holder_uid": None,
        "holder_operation": "external_or_unknown",
        "holder_thread": None,
        "holder_hold_ms": None,
        "verified": False,
        "released_during_diagnosis": False,
        "pid_reused": False,
        "unavailable": False,
    }
    try:
        descriptor = os.fstat(handle.fileno())
        result["device"] = (
            f"{os.major(descriptor.st_dev):x}:{os.minor(descriptor.st_dev):x}"
        )
        result["inode"] = int(descriptor.st_ino)
    except (OSError, ValueError):
        result["unavailable"] = True
        return result

    holder_pid, scan_usable = _find_linux_lock_holder(
        descriptor.st_dev,
        descriptor.st_ino,
        proc_root=proc_root,
    )
    if not scan_usable or holder_pid is None:
        result["unavailable"] = True
        return result
    result["holder_pid"] = holder_pid
    first_metadata, first_status = _read_linux_process_metadata(
        holder_pid, proc_root=proc_root
    )
    if first_metadata is None:
        result[
            "released_during_diagnosis" if first_status == "missing" else "unavailable"
        ] = True
        return result
    result.update(first_metadata)

    second_pid, second_scan_usable = _find_linux_lock_holder(
        descriptor.st_dev,
        descriptor.st_ino,
        proc_root=proc_root,
    )
    second_identity, second_status = _process_start_identity(holder_pid, proc_root)
    if not second_scan_usable:
        result["unavailable"] = True
        return result
    if second_pid != holder_pid or second_status == "missing":
        result["released_during_diagnosis"] = True
        return result
    if second_status != "ok" or second_identity is None:
        result["unavailable"] = True
        return result
    if second_identity != first_metadata["holder_process_start"]:
        result["pid_reused"] = True
        return result
    result["verified"] = True

    if holder_pid == waiter_pid:
        holder_key = (int(descriptor.st_dev), int(descriptor.st_ino))
        with _LOCAL_HOLDER_GUARD:
            local_holder = dict(_LOCAL_HOLDERS.get(holder_key, {}))
        if local_holder.get("pid") == holder_pid:
            result["holder_operation"] = local_holder.get(
                "operation", "external_or_unknown"
            )
            result["holder_thread"] = local_holder.get("thread")
            started_ns = int(local_holder.get("hold_started_ns", 0) or 0)
            if started_ns > 0:
                result["holder_hold_ms"] = round(
                    max(0, time.monotonic_ns() - started_ns) / 1_000_000,
                    3,
                )
    return result


def _capture_same_process_traceback(
    preferred_thread: int | None = None,
) -> list[dict[str, Any]]:
    """Capture frame coordinates only; never locals, source lines, or values."""
    snapshots: list[dict[str, Any]] = []
    all_frames = sys._current_frames()
    ordered_thread_ids = sorted(all_frames)
    if preferred_thread in all_frames:
        ordered_thread_ids.remove(preferred_thread)
        ordered_thread_ids.insert(0, preferred_thread)
    for thread_id in ordered_thread_ids[:MAX_TRACE_THREADS]:
        initial_frame = all_frames[thread_id]
        frame = initial_frame
        frames: list[dict[str, Any]] = []
        while frame is not None and len(frames) < MAX_TRACE_FRAMES:
            frames.append({
                "file": Path(frame.f_code.co_filename).name[:128],
                "function": frame.f_code.co_name[:128],
                "line": int(frame.f_lineno),
            })
            frame = frame.f_back
        snapshots.append({"thread_id": int(thread_id), "frames": frames})
    return snapshots


def _bounded_diagnostic_bytes(payload: dict[str, Any]) -> bytes:
    """Encode valid JSON while trimming traceback frames to the fixed ceiling."""
    candidate = copy.deepcopy(payload)
    while True:
        encoded = (
            json.dumps(candidate, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        if len(encoded) <= MAX_SNAPSHOT_BYTES:
            return encoded
        recent_events = candidate.get("recent_events")
        if isinstance(recent_events, list) and recent_events:
            recent_events.pop(0)
            continue
        timeout_record = candidate.get("latest_timeout")
        stacks = (
            timeout_record.get("same_process_traceback")
            if isinstance(timeout_record, dict)
            else candidate.get("same_process_traceback")
        )
        if not isinstance(stacks, list) or not stacks:
            if isinstance(timeout_record, dict):
                timeout_record.pop("same_process_traceback", None)
            else:
                candidate.pop("same_process_traceback", None)
            minimal_timeout = timeout_record if isinstance(timeout_record, dict) else candidate
            safe_keys = (
                "attempt_id",
                "event",
                "operation",
                "waiter_pid",
                "waiter_thread",
                "waiter_process_start",
                "wait_ms",
                "hold_ms",
                "device",
                "inode",
                "holder_pid",
                "holder_process_start",
                "holder_state",
                "holder_wchan",
                "holder_uid",
                "holder_operation",
                "holder_thread",
                "holder_hold_ms",
                "verified",
                "released_during_diagnosis",
                "pid_reused",
                "unavailable",
            )
            candidate = {
                "schema_version": 1,
                "latest_timeout": {
                    key: minimal_timeout.get(key)
                    for key in safe_keys
                    if key in minimal_timeout
                },
                "recent_events": [],
            }
            encoded = (
                json.dumps(candidate, sort_keys=True, separators=(",", ":")) + "\n"
            ).encode("utf-8")
            if len(encoded) <= MAX_SNAPSHOT_BYTES:
                return encoded
            return b'{"schema_version":1,"latest_timeout":{"event":"TIMEOUT","unavailable":true},"recent_events":[]}\n'
        if len(stacks) > 1:
            stacks.pop()
            continue
        last_stack = stacks[-1]
        frames = last_stack.get("frames") if isinstance(last_stack, dict) else None
        if isinstance(frames, list) and frames:
            del frames[max(1, len(frames) // 2) :]
            if len(frames) == 1:
                stacks.pop()
        else:
            stacks.pop()


def _write_snapshot(payload: dict[str, Any]) -> bool:
    """Best-effort fixed-file replace with bounded lock waits and storage."""
    guard_locked = False
    try:
        guard_locked = _SNAPSHOT_GUARD.acquire(
            timeout=SNAPSHOT_LOCK_TIMEOUT_SECONDS
        )
        if not guard_locked:
            return False
        deadline = time.monotonic() + SNAPSHOT_LOCK_TIMEOUT_SECONDS
        try:
            SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
            lock_path = SNAPSHOT_PATH.with_name(f"{SNAPSHOT_PATH.name}.write.lock")
            temporary_path = SNAPSHOT_PATH.with_name(f"{SNAPSHOT_PATH.name}.tmp")
            try:
                initial_descriptor = os.open(
                    lock_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
            except FileExistsError:
                pass
            else:
                try:
                    os.write(initial_descriptor, b"\0")
                finally:
                    os.close(initial_descriptor)
            lock_handle = open(lock_path, "a+b")
            locked = False
            try:
                while time.monotonic() < deadline:
                    try:
                        lock_handle.seek(0)
                        if os.name == "nt":
                            msvcrt.locking(
                                lock_handle.fileno(), msvcrt.LK_NBLCK, 1
                            )
                        else:  # pragma: no cover - Linux deployment/tests.
                            fcntl.flock(
                                lock_handle.fileno(),
                                fcntl.LOCK_EX | fcntl.LOCK_NB,
                            )
                        locked = True
                        break
                    except OSError:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        time.sleep(min(SNAPSHOT_LOCK_RETRY_SECONDS, remaining))
                if not locked:
                    return False
                existing: dict[str, Any] = {}
                try:
                    with open(
                        SNAPSHOT_PATH,
                        "r",
                        encoding="utf-8",
                        errors="replace",
                    ) as current:
                        raw = current.read(MAX_SNAPSHOT_BYTES + 1)
                    parsed = json.loads(raw) if len(raw) <= MAX_SNAPSHOT_BYTES else {}
                    if isinstance(parsed, dict):
                        existing = parsed
                except (OSError, ValueError):
                    existing = {}
                existing_events = existing.get("recent_events")
                if not isinstance(existing_events, list):
                    existing_events = []
                document = {
                    "schema_version": 1,
                    "latest_timeout": existing.get("latest_timeout"),
                    "recent_events": list(existing_events)[-MAX_RECENT_EVENTS:],
                }
                if payload.get("event") == "TIMEOUT":
                    document["latest_timeout"] = payload
                else:
                    document["recent_events"].append(payload)
                    document["recent_events"] = document["recent_events"][
                        -MAX_RECENT_EVENTS:
                    ]
                encoded = _bounded_diagnostic_bytes(document)
                descriptor = os.open(
                    temporary_path,
                    os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                    0o600,
                )
                try:
                    if os.name != "nt":
                        os.fchmod(descriptor, 0o600)
                    offset = 0
                    while offset < len(encoded):
                        written = os.write(descriptor, encoded[offset:])
                        if written <= 0:
                            raise OSError("incomplete diagnostic snapshot write")
                        offset += written
                    if offset != len(encoded):
                        raise OSError("incomplete diagnostic snapshot write")
                finally:
                    os.close(descriptor)
                os.replace(temporary_path, SNAPSHOT_PATH)
                return True
            finally:
                if locked:
                    try:
                        lock_handle.seek(0)
                        if os.name == "nt":
                            msvcrt.locking(lock_handle.fileno(), msvcrt.LK_UNLCK, 1)
                        else:  # pragma: no cover
                            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
                    except OSError:
                        pass
                lock_handle.close()
        finally:
            _SNAPSHOT_GUARD.release()
    except Exception:
        # Diagnostic failure must not change acquisition or mutation semantics.
        return False


def _event(
    attempt: LockAttempt,
    event: str,
    *,
    hold_ms: float | None = None,
) -> dict[str, Any]:
    process_start, _ = _process_start_identity(attempt.waiter_pid)
    record: dict[str, Any] = {
        "attempt_id": attempt.attempt_id,
        "event": event,
        "operation": attempt.operation,
        "waiter_pid": attempt.waiter_pid,
        "waiter_thread": attempt.waiter_thread,
        "waiter_process_start": process_start,
        "wait_ms": round(max(0.0, attempt.wait_ms), 3),
        "hold_ms": round(max(0.0, hold_ms), 3) if hold_ms is not None else None,
    }
    if attempt.holder_key is not None:
        device, inode = attempt.holder_key
        record["device"] = f"{os.major(device):x}:{os.minor(device):x}"
        record["inode"] = inode
    return record


def acquired(attempt: LockAttempt, handle: BinaryIO) -> None:
    """Register a local holder without file I/O in the target critical section."""
    try:
        acquired_ns = time.monotonic_ns()
        attempt.wait_ms = max(0, acquired_ns - attempt.started_ns) / 1_000_000
        attempt.hold_started_ns = acquired_ns
        descriptor = os.fstat(handle.fileno())
        attempt.holder_key = (int(descriptor.st_dev), int(descriptor.st_ino))
        with _LOCAL_HOLDER_GUARD:
            _LOCAL_HOLDERS[attempt.holder_key] = {
                "attempt_id": attempt.attempt_id,
                "pid": attempt.waiter_pid,
                "thread": attempt.waiter_thread,
                "operation": attempt.operation,
                "hold_started_ns": acquired_ns,
            }
    except Exception:
        # Registry and timing are advisory and cannot block a successful lock.
        pass


def timeout(attempt: LockAttempt, handle: BinaryIO) -> dict[str, Any]:
    """Persist one bounded timeout snapshot and return exception metadata."""
    attempt.wait_ms = max(0, time.monotonic_ns() - attempt.started_ns) / 1_000_000
    record = _event(attempt, "TIMEOUT")
    try:
        if os.name == "nt":
            record["unavailable"] = True
        else:  # pragma: no cover - exercised by Linux tests/deployment.
            record.update(
                _inspect_linux_holder(handle, waiter_pid=attempt.waiter_pid)
            )
        snapshot = dict(record)
        if (
            record.get("verified") is True
            and record.get("holder_pid") == attempt.waiter_pid
        ):
            snapshot["same_process_traceback"] = _capture_same_process_traceback(
                record.get("holder_thread")
            )
        if not _write_snapshot(snapshot):
            record["unavailable"] = True
    except Exception:
        record["unavailable"] = True
        _write_snapshot(record)
    return record


def released(attempt: LockAttempt, released_ns: int | None = None) -> None:
    """Remove advisory holder metadata and persist only unusually long holds."""
    try:
        finished_ns = time.monotonic_ns() if released_ns is None else released_ns
        if attempt.holder_key is not None:
            with _LOCAL_HOLDER_GUARD:
                local_holder = _LOCAL_HOLDERS.get(attempt.holder_key)
                if local_holder and local_holder.get("attempt_id") == attempt.attempt_id:
                    _LOCAL_HOLDERS.pop(attempt.holder_key, None)
        if attempt.hold_started_ns is None:
            return
        hold_ms = max(0, finished_ns - attempt.hold_started_ns) / 1_000_000
        if hold_ms >= SLOW_HOLD_SECONDS * 1000:
            _write_snapshot(_event(attempt, "RELEASED", hold_ms=hold_ms))
        elif attempt.wait_ms >= SLOW_ACQUIRE_SECONDS * 1000:
            _write_snapshot(_event(attempt, "ACQUIRED", hold_ms=hold_ms))
    except Exception:
        pass
