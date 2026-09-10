"""PM2와 실제 프로세스의 aibot singleton topology를 검증한다."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

try:
    import pwd
except ImportError:  # pragma: no cover - 운영 checker는 Linux에서만 실행한다.
    pwd = None  # type: ignore[assignment]


APP_ROOT = Path("/var/www/aibot")
VENV_BIN = APP_ROOT / "venv" / "bin"
EXPECTED_PM2_HOME = Path("/home/deploy/.pm2")
EXPECTED_USER = "deploy"
DASHBOARD_PORT = 8501


@dataclass(frozen=True)
class AppSpec:
    name: str
    script: Path
    interpreter: str
    args: tuple[str, ...] = ()
    core: bool = True


@dataclass(frozen=True)
class ProcessInfo:
    pid: int
    uid: int
    cwd: str | None
    argv: tuple[str, ...]


@dataclass(frozen=True)
class ListenerInfo:
    inode: str
    pids: tuple[int, ...]


SPECS = (
    AppSpec(
        name="aibot-monitor",
        script=APP_ROOT / "src" / "monitor.py",
        interpreter=str(VENV_BIN / "python"),
    ),
    AppSpec(
        name="aibot-risk-manager",
        script=APP_ROOT / "src" / "risk_manager.py",
        interpreter=str(VENV_BIN / "python"),
    ),
    AppSpec(
        name="aibot-dashboard",
        script=VENV_BIN / "streamlit",
        interpreter="none",
        args=(
            "run",
            "src/dashboard.py",
            "--server.port",
            "8501",
            "--server.address",
            "127.0.0.1",
            "--server.baseUrlPath",
            "ai-bot",
            "--server.headless",
            "true",
            "--browser.gatherUsageStats",
            "false",
        ),
    ),
    AppSpec(
        name="wallet_feeder",
        script=APP_ROOT / "src" / "wallet_feeder.py",
        interpreter=str(VENV_BIN / "python"),
        args=("--once",),
        core=False,
    ),
)
SPEC_BY_NAME = {spec.name: spec for spec in SPECS}


def _normalized_path(value: Any, *, cwd: Path = APP_ROOT) -> str:
    raw = str(value or "")
    if not raw:
        return ""
    path = Path(raw)
    if not path.is_absolute():
        path = cwd / path
    return os.path.normpath(str(path))


def _normalized_args(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return tuple(value.split())
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value)
    return (str(value),)


def _pm2_env(app: dict[str, Any]) -> dict[str, Any]:
    value = app.get("pm2_env")
    return value if isinstance(value, dict) else {}


def _pm2_pid(app: dict[str, Any]) -> int:
    try:
        return int(app.get("pid") or 0)
    except (TypeError, ValueError):
        return 0


def _pm2_metadata_errors(app: dict[str, Any], spec: AppSpec) -> list[str]:
    env = _pm2_env(app)
    errors: list[str] = []
    if _normalized_path(env.get("pm_cwd")) != str(APP_ROOT):
        errors.append(
            f"{spec.name}: unexpected PM2 cwd={env.get('pm_cwd')!r}"
        )
    if _normalized_path(env.get("pm_exec_path")) != str(spec.script):
        errors.append(
            f"{spec.name}: unexpected PM2 script={env.get('pm_exec_path')!r}"
        )
    interpreter = str(env.get("exec_interpreter") or "")
    if interpreter != spec.interpreter:
        errors.append(
            f"{spec.name}: unexpected PM2 interpreter={interpreter!r}"
        )
    if _normalized_args(env.get("args")) != spec.args:
        errors.append(f"{spec.name}: unexpected PM2 args")
    return errors


def _script_argument_matches(argument: str, expected: Path) -> bool:
    return _normalized_path(argument) == str(expected)


def candidate_role(process: ProcessInfo) -> str | None:
    """명령행이 canonical script를 언급하면 엄격 검증 대상으로 분류한다."""
    for spec in SPECS:
        if any(_script_argument_matches(arg, spec.script) for arg in process.argv):
            return spec.name
    dashboard = APP_ROOT / "src" / "dashboard.py"
    if any(_script_argument_matches(arg, dashboard) for arg in process.argv):
        return "aibot-dashboard"
    return None


def _python_launcher(argument: str) -> bool:
    path = Path(_normalized_path(argument))
    return path.name.startswith("python")


def process_matches_spec(process: ProcessInfo, spec: AppSpec, uid: int) -> bool:
    if process.uid != uid or process.cwd != str(APP_ROOT):
        return False
    argv = process.argv
    if spec.name == "aibot-dashboard":
        # Streamlit의 venv script가 shebang을 통해 system Python으로 보이는
        # 실행 형태도 허용하되, 뒤따르는 streamlit 경로와 인자는 그대로 검증한다.
        command = argv[1:] if argv and _python_launcher(argv[0]) else argv
        if len(command) != len(spec.args) + 1:
            return False
        if not _script_argument_matches(command[0], spec.script):
            return False
        actual_args = list(command[1:])
        expected_args = list(spec.args)
        if len(actual_args) >= 2:
            dashboard = APP_ROOT / "src" / "dashboard.py"
            if actual_args[0] == "run" and _script_argument_matches(
                actual_args[1], dashboard
            ):
                actual_args[1] = str(dashboard)
                expected_args[1] = str(dashboard)
        return tuple(actual_args) == tuple(expected_args)
    if len(argv) != len(spec.args) + 2:
        return False
    return (
        _normalized_path(argv[0]) == spec.interpreter
        and _script_argument_matches(argv[1], spec.script)
        and argv[2:] == spec.args
    )


def _group_canonical_apps(
    apps: Iterable[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    grouped: dict[str, list[dict[str, Any]]] = {
        spec.name: [] for spec in SPECS
    }
    for app in apps:
        name = app.get("name")
        if name in grouped:
            grouped[str(name)].append(app)
    errors = [
        f"{name}: duplicate PM2 entries count={len(rows)}"
        for name, rows in grouped.items()
        if len(rows) > 1
    ]
    return (
        {name: rows[0] for name, rows in grouped.items() if len(rows) == 1},
        errors,
    )


def validate_topology(
    *,
    phase: str,
    apps: Sequence[dict[str, Any]],
    processes: Sequence[ProcessInfo],
    listeners: Sequence[ListenerInfo],
    expected_uid: int,
) -> list[str]:
    """preflight 또는 poststart 시점의 singleton 불변식을 검증한다."""
    if phase not in {"preflight", "poststart"}:
        raise ValueError(f"unsupported phase: {phase}")

    canonical, errors = _group_canonical_apps(apps)
    process_by_pid = {process.pid: process for process in processes}
    managed_pids: dict[str, int] = {}

    for spec in SPECS:
        app = canonical.get(spec.name)
        if app is None:
            if phase == "poststart":
                errors.append(f"{spec.name}: PM2 entry missing after start")
            continue

        errors.extend(_pm2_metadata_errors(app, spec))
        env = _pm2_env(app)
        status = str(env.get("status") or "")
        pid = _pm2_pid(app)

        if phase == "poststart":
            allowed_statuses = {"online"} if spec.core else {"online", "stopped"}
            if status not in allowed_statuses:
                errors.append(
                    f"{spec.name}: unexpected poststart status={status!r}"
                )
        elif not spec.core and status not in {"online", "stopped"}:
            errors.append(
                f"{spec.name}: unexpected preflight feeder status={status!r}"
            )

        if status == "online":
            if pid <= 0:
                errors.append(f"{spec.name}: online PM2 entry has no PID")
                continue
            managed_pids[spec.name] = pid
            process = process_by_pid.get(pid)
            if process is None:
                errors.append(f"{spec.name}: PM2 PID {pid} is absent from /proc")
            elif not process_matches_spec(process, spec, expected_uid):
                errors.append(
                    f"{spec.name}: PM2 PID {pid} has non-canonical uid/cwd/argv"
                )
        elif pid > 0:
            errors.append(
                f"{spec.name}: non-online PM2 entry unexpectedly has PID {pid}"
            )

    candidates: dict[str, list[ProcessInfo]] = {
        spec.name: [] for spec in SPECS
    }
    for process in processes:
        role = candidate_role(process)
        if role is not None:
            candidates[role].append(process)

    for spec in SPECS:
        managed_pid = managed_pids.get(spec.name)
        for process in candidates[spec.name]:
            exact = process_matches_spec(process, spec, expected_uid)
            if process.pid != managed_pid:
                kind = "unmanaged exact" if exact else "ambiguous"
                errors.append(
                    f"{spec.name}: {kind} process pid={process.pid} "
                    f"uid={process.uid} cwd={process.cwd!r} "
                    "has non-canonical argv"
                )
            elif not exact:
                errors.append(
                    f"{spec.name}: managed process pid={process.pid} is ambiguous"
                )

        if phase == "poststart" and spec.core:
            exact_pids = {
                process.pid
                for process in candidates[spec.name]
                if process_matches_spec(process, spec, expected_uid)
            }
            expected_pid = managed_pids.get(spec.name)
            if expected_pid is not None and exact_pids != {expected_pid}:
                errors.append(
                    f"{spec.name}: OS singleton mismatch "
                    f"expected={expected_pid} exact_pids={sorted(exact_pids)}"
                )

    dashboard_pid = managed_pids.get("aibot-dashboard")
    listener_pids = {
        pid for listener in listeners for pid in listener.pids
    }
    unresolved = [listener.inode for listener in listeners if not listener.pids]
    if phase == "preflight" and not listeners:
        pass
    elif unresolved:
        errors.append(
            f"port {DASHBOARD_PORT}: listener owner unresolved inodes={unresolved}"
        )
    elif len(listeners) != 1 or dashboard_pid is None or listener_pids != {dashboard_pid}:
        errors.append(
            f"port {DASHBOARD_PORT}: listener must be the sole canonical PM2 "
            f"dashboard pid={dashboard_pid}; observed_pids={sorted(listener_pids)} "
            f"listener_count={len(listeners)}"
        )

    return errors


def collect_processes(proc_root: Path = Path("/proc")) -> list[ProcessInfo]:
    processes: list[ProcessInfo] = []
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = entry.stat()
            raw = (entry / "cmdline").read_bytes()
            if not raw:
                continue
            argv = tuple(
                part.decode("utf-8", errors="replace")
                for part in raw.rstrip(b"\0").split(b"\0")
            )
            try:
                cwd = os.readlink(entry / "cwd")
            except OSError:
                cwd = None
            processes.append(
                ProcessInfo(
                    pid=int(entry.name),
                    uid=stat.st_uid,
                    cwd=cwd,
                    argv=argv,
                )
            )
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
    return processes


def _listening_socket_inodes(
    port: int,
    proc_root: Path = Path("/proc"),
) -> set[str]:
    inodes: set[str] = set()
    readable_sources = 0
    for relative in (Path("net/tcp"), Path("net/tcp6")):
        path = proc_root / relative
        try:
            lines = path.read_text(encoding="ascii").splitlines()[1:]
        except (FileNotFoundError, PermissionError):
            continue
        readable_sources += 1
        for line in lines:
            fields = line.split()
            if len(fields) < 10 or fields[3] != "0A":
                continue
            local = fields[1]
            try:
                local_port = int(local.rsplit(":", 1)[1], 16)
            except (IndexError, ValueError):
                continue
            if local_port == port:
                inodes.add(fields[9])
    if readable_sources == 0:
        raise RuntimeError("cannot inspect /proc/net/tcp or /proc/net/tcp6")
    return inodes


def collect_listeners(
    processes: Sequence[ProcessInfo],
    *,
    port: int = DASHBOARD_PORT,
    proc_root: Path = Path("/proc"),
) -> list[ListenerInfo]:
    owners: dict[str, set[int]] = {
        inode: set() for inode in _listening_socket_inodes(port, proc_root)
    }
    if not owners:
        return []
    for process in processes:
        fd_dir = proc_root / str(process.pid) / "fd"
        try:
            entries = list(fd_dir.iterdir())
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        for entry in entries:
            try:
                target = os.readlink(entry)
            except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
                continue
            if not target.startswith("socket:[") or not target.endswith("]"):
                continue
            inode = target[8:-1]
            if inode in owners:
                owners[inode].add(process.pid)
    return [
        ListenerInfo(inode=inode, pids=tuple(sorted(pids)))
        for inode, pids in sorted(owners.items())
    ]


def load_pm2_apps() -> list[dict[str, Any]]:
    payload = subprocess.check_output(["pm2", "jlist"], text=True)
    document = json.loads(payload)
    if not isinstance(document, list) or not all(
        isinstance(item, dict) for item in document
    ):
        raise RuntimeError("pm2 jlist did not return a JSON array of objects")
    return document


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--phase",
        required=True,
        choices=("preflight", "poststart"),
    )
    args = parser.parse_args(argv)

    actual_home = Path(os.environ.get("PM2_HOME", ""))
    if actual_home != EXPECTED_PM2_HOME:
        print(
            "PM2_TOPOLOGY_ERROR "
            f"phase={args.phase} unexpected_PM2_HOME={str(actual_home)!r}",
            file=sys.stderr,
        )
        return 1
    if pwd is None or not hasattr(os, "geteuid"):
        print(
            f"PM2_TOPOLOGY_ERROR phase={args.phase} Linux /proc is required",
            file=sys.stderr,
        )
        return 1
    current_uid = os.geteuid()
    current_user = pwd.getpwuid(current_uid).pw_name
    if current_user != EXPECTED_USER:
        print(
            "PM2_TOPOLOGY_ERROR "
            f"phase={args.phase} unexpected_user={current_user!r}",
            file=sys.stderr,
        )
        return 1

    try:
        apps = load_pm2_apps()
        processes = collect_processes()
        listeners = collect_listeners(processes)
        errors = validate_topology(
            phase=args.phase,
            apps=apps,
            processes=processes,
            listeners=listeners,
            expected_uid=current_uid,
        )
    except Exception as exc:
        print(
            f"PM2_TOPOLOGY_ERROR phase={args.phase} inspection_failed={exc!r}",
            file=sys.stderr,
        )
        return 1

    if errors:
        for error in errors:
            print(
                f"PM2_TOPOLOGY_ERROR phase={args.phase} {error}",
                file=sys.stderr,
            )
        return 1
    canonical_count = sum(
        1 for app in apps if app.get("name") in SPEC_BY_NAME
    )
    print(
        f"PM2_TOPOLOGY phase={args.phase} status=OK "
        f"canonical_pm2_entries={canonical_count} "
        f"port_{DASHBOARD_PORT}_listeners={len(listeners)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
