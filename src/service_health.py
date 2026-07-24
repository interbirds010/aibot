"""Cross-platform heartbeat checks for the monitor service."""

from __future__ import annotations

import os
import time
from pathlib import Path

DEFAULT_MAX_AGE_SECONDS = 180.0


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
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
