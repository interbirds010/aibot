"""Cross-platform heartbeat checks for the monitor service."""

from __future__ import annotations

import os
import time
from pathlib import Path

DEFAULT_MAX_AGE_SECONDS = 180.0
DEFAULT_ROUTE_POLL_MAX_AGE_SECONDS = 300.0
DEFAULT_STARTUP_GRACE_SECONDS = 300.0
DEFAULT_WS_RECONNECT_GRACE_SECONDS = 180.0
DEFAULT_WS_REFRESH_MAX_AGE_SECONDS = 2_400.0


def _windows_pid_is_alive(pid: int) -> bool:
    """Windows에서 프로세스를 종료하지 않고 실행 상태를 확인한다."""
    import ctypes
    from ctypes import wintypes

    synchronize = 0x00100000
    wait_object_0 = 0
    wait_timeout = 258
    error_access_denied = 5
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = (
        wintypes.DWORD,
        wintypes.BOOL,
        wintypes.DWORD,
    )
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
    kernel32.WaitForSingleObject.restype = wintypes.DWORD
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL

    handle = kernel32.OpenProcess(
        synchronize,
        False,
        pid,
    )
    if not handle:
        return ctypes.get_last_error() == error_access_denied
    try:
        wait_result = kernel32.WaitForSingleObject(handle, 0)
        if wait_result == wait_timeout:
            return True
        if wait_result == wait_object_0:
            return False
        return False
    finally:
        kernel32.CloseHandle(handle)


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        return _windows_pid_is_alive(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _is_recent(path: Path, max_age_seconds: float, now: float) -> bool:
    try:
        return now - path.stat().st_mtime < max_age_seconds
    except OSError:
        return False


def service_is_fresh(
    legacy_log_path: Path,
    *,
    pm2_home: Path | None = None,
    max_age_seconds: float = DEFAULT_MAX_AGE_SECONDS,
    now: float | None = None,
) -> bool:
    """Return whether the monitor is alive, preferring PM2's real PID on Linux.

    PM2 logs can remain unchanged while a healthy process is idle. When PM2 has
    written monitor PID files, process liveness is therefore authoritative.
    Log timestamps are used only when PM2 PID metadata is unavailable.
    """
    resolved_pm2_home = pm2_home or Path(
        os.getenv("PM2_HOME", str(Path.home() / ".pm2"))
    )
    pid_files = list((resolved_pm2_home / "pids").glob("aibot-monitor-*.pid"))
    if pid_files:
        for pid_file in pid_files:
            try:
                if _pid_is_alive(int(pid_file.read_text(encoding="utf-8").strip())):
                    return True
            except (OSError, ValueError):
                continue
        return False

    checked_at = time.time() if now is None else now
    pm2_logs = (resolved_pm2_home / "logs").glob("aibot-monitor-*.log")
    return any(
        _is_recent(path, max_age_seconds, checked_at) for path in pm2_logs
    ) or _is_recent(legacy_log_path, max_age_seconds, checked_at)


def collection_metrics_are_fresh(
    metrics: object,
    *,
    now: float | None = None,
) -> bool:
    """PID와 별개로 핵심 수집 루프가 최근까지 진행됐는지 확인한다."""
    if not isinstance(metrics, dict):
        return False
    checked_at = time.time() if now is None else float(now)

    def number(name: str) -> float:
        try:
            return float(metrics.get(name, 0) or 0)
        except (TypeError, ValueError):
            return 0.0

    started_at = number("monitor_started_at")
    if started_at > 0 and checked_at - started_at <= DEFAULT_STARTUP_GRACE_SECONDS:
        return True
    process_at = number("monitor_process_heartbeat_at")
    route_at = number("route_b_last_poll_success_at")
    if (
        process_at <= 0
        or checked_at - process_at > DEFAULT_MAX_AGE_SECONDS
        or route_at <= 0
        or checked_at - route_at > DEFAULT_ROUTE_POLL_MAX_AGE_SECONDS
    ):
        return False
    ws_state = str(metrics.get("wallet_ws_state", "")).upper()
    ws_changed_at = number("wallet_ws_state_changed_at")
    if ws_state != "SUBSCRIBED":
        return (
            ws_changed_at > 0
            and checked_at - ws_changed_at <= DEFAULT_WS_RECONNECT_GRACE_SECONDS
        )
    subscribed_at = number("wallet_ws_subscribed_at")
    return (
        subscribed_at > 0
        and checked_at - subscribed_at <= DEFAULT_WS_REFRESH_MAX_AGE_SECONDS
    )
