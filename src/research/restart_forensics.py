"""PM2 monitor restart 원인을 Actions log에 bounded·redacted 형태로 남긴다."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any

from src.logging_utils import redact_sensitive_text


MONITOR_NAME = "aibot-monitor"
MAX_LOG_LINES = 60
MAX_LOG_CHARS = 12_000
_URL = re.compile(r"(?i)\b(?:https?|wss?)://[^\s'\"<>]+")
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(api[-_]?key|apikey|authorization|token|secret|password)"
    r"\s*[:=]\s*[^\s,'\"<>]+"
)
_OOM = re.compile(
    r"(?i)(out of memory|oom[-_ ]kill|killed process|memory cgroup out of memory)"
)
_KERNEL_ACCESS_WARNING = re.compile(
    r"(?i)(permission denied|not seeing messages|failed to open|access denied)"
)


def _integer(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _monitor(apps: list[Any]) -> dict[str, Any]:
    return next((
        app for app in apps
        if isinstance(app, dict) and app.get("name") == MONITOR_NAME
    ), {})


def pm2_snapshot(app: dict[str, Any]) -> dict[str, Any]:
    environment = app.get("pm2_env")
    environment = environment if isinstance(environment, dict) else {}
    monitor = app.get("monit")
    monitor = monitor if isinstance(monitor, dict) else {}
    uptime_ms = _integer(environment.get("pm_uptime"))
    return {
        "status": str(environment.get("status") or "UNKNOWN"),
        "pid": _integer(app.get("pid")),
        "restart_count": _integer(environment.get("restart_time")) or 0,
        "unstable_restarts": _integer(environment.get("unstable_restarts")),
        "previous_process_exit_code": _integer(environment.get("exit_code")),
        "previous_process_exit_signal": (
            str(environment.get("exit_signal"))[:40]
            if environment.get("exit_signal") is not None else None
        ),
        "restart_at_epoch": (
            round(uptime_ms / 1000, 3) if uptime_ms is not None else None
        ),
        "current_rss_bytes": _integer(monitor.get("memory")),
    }


def safe_log_tail(text: str) -> list[str]:
    """URL·credential을 제거하고 마지막 stderr 일부만 반환한다."""
    safe_lines: list[str] = []
    for raw in str(text).splitlines()[-MAX_LOG_LINES:]:
        line = redact_sensitive_text(raw)
        line = _URL.sub("<URL_REDACTED>", line)
        line = _SECRET_ASSIGNMENT.sub(r"\1=HIDDEN_MASKED", line)
        safe_lines.append(line[:1_000])
    while sum(len(line) for line in safe_lines) > MAX_LOG_CHARS:
        safe_lines.pop(0)
    return safe_lines


def build_restart_forensics(
    before_apps: list[Any],
    after_apps: list[Any],
    *,
    deployed_sha: str,
    available_memory_bytes: int | None,
    oom_evidence: str,
    oom_evidence_lines: list[str] | None = None,
    stderr_tail: str = "",
    stderr_tail_status: str = "AVAILABLE",
) -> dict[str, Any]:
    before = pm2_snapshot(_monitor(before_apps))
    after = pm2_snapshot(_monitor(after_apps))
    restart_delta = after["restart_count"] - before["restart_count"]
    sha = str(deployed_sha).strip().lower()
    if re.fullmatch(r"[0-9a-f]{40}", sha) is None:
        sha = "UNKNOWN"
    return {
        "schema_version": 1,
        "deployed_sha": sha,
        "restart_detected": restart_delta != 0,
        "restart_delta": restart_delta,
        "before": before,
        "after": after,
        "system_available_memory_bytes": available_memory_bytes,
        "oom_evidence": str(oom_evidence).upper()[:40],
        "oom_evidence_lines": safe_log_tail(
            "\n".join(oom_evidence_lines or [])
        )[-3:],
        "stderr_tail_status": str(stderr_tail_status).upper()[:40],
        "recent_safe_stderr_tail": safe_log_tail(stderr_tail),
    }


def _load_apps(path: Path) -> list[Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, list):
        raise RuntimeError("PM2 snapshot is malformed")
    return document


def _available_memory_bytes() -> int | None:
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def _kernel_oom_evidence(since_epoch: float) -> tuple[str, list[str]]:
    commands = (
        ("journalctl", "-k", "--since", f"@{int(since_epoch)}", "--no-pager", "--quiet"),
        ("dmesg", "--ctime"),
    )
    readable = False
    for command in commands:
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        if result.returncode != 0 or _KERNEL_ACCESS_WARNING.search(result.stderr):
            continue
        readable = True
        matches = [line for line in result.stdout.splitlines() if _OOM.search(line)]
        if matches:
            return "PRESENT", matches[-3:]
    return ("NONE", []) if readable else ("UNKNOWN", [])


def _safe_pm2_error_tail(after_apps: list[Any]) -> tuple[str, str]:
    environment = _monitor(after_apps).get("pm2_env")
    environment = environment if isinstance(environment, dict) else {}
    raw_path = str(environment.get("pm_err_log") or "").strip()
    if not raw_path:
        return "", "UNAVAILABLE"
    try:
        base = Path(os.getenv("PM2_HOME", "/home/deploy/.pm2")).resolve()
        path = Path(raw_path).resolve()
        path.relative_to(base)
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except (OSError, ValueError):
        return "", "UNAVAILABLE"
    return "\n".join(lines[-MAX_LOG_LINES:]), "AVAILABLE"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--deployed-sha", required=True)
    parser.add_argument("--since-epoch", type=float, required=True)
    args = parser.parse_args()

    before_apps = _load_apps(args.before)
    after_apps = _load_apps(args.after)
    oom_status, oom_lines = _kernel_oom_evidence(args.since_epoch)
    stderr_tail, stderr_status = _safe_pm2_error_tail(after_apps)
    report = build_restart_forensics(
        before_apps,
        after_apps,
        deployed_sha=args.deployed_sha,
        available_memory_bytes=_available_memory_bytes(),
        oom_evidence=oom_status,
        oom_evidence_lines=oom_lines,
        stderr_tail=stderr_tail,
        stderr_tail_status=stderr_status,
    )
    print(
        "MONITOR_RESTART_FORENSICS "
        + json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
