"""Windows Paper 프로세스의 명시적 시작·중지·상태 조회."""
from __future__ import annotations

import argparse
import ctypes
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
from src.state_store import atomic_write_json, exclusive_file_lock, read_json

CORE_SERVICES = ("monitor", "risk-manager", "dashboard")
SERVICES = CORE_SERVICES + ("wallet-feeder",)


def service_command(root: Path, python: Path, service: str) -> list[str]:
    """절대 스크립트 경로로 별도 checkout의 프로세스와 구별한다."""
    if service == "dashboard":
        return [str(python), "-m", "streamlit", "run", str(root / "src/dashboard.py"),
                "--server.address", "127.0.0.1", "--server.port", "8501",
                "--server.baseUrlPath", "ai-bot", "--server.headless", "true",
                "--browser.gatherUsageStats", "false"]
    scripts = {"monitor": "monitor.py", "risk-manager": "risk_manager.py",
               "wallet-feeder": "wallet_feeder.py"}
    command = [str(python), str(root / "src" / scripts[service])]
    return command + (["--once"] if service == "wallet-feeder" else [])


def paper_environment(root: Path) -> dict[str, str]:
    """비밀값을 출력하지 않고 live 설정을 거부한 뒤 Paper만 실행한다."""
    from dotenv import dotenv_values
    configured = dotenv_values(root / ".env")
    for mode in (configured.get("TRADING_MODE"), os.environ.get("TRADING_MODE")):
        if mode is not None and str(mode).strip().lower() != "paper":
            raise RuntimeError("TRADING_MODE must be paper; live launch refused")
    environment = dict(os.environ)
    environment.update({"TRADING_MODE": "paper", "OBSERVATION_MODE": "true",
                        "APPROVED_SIGNAL_PAPER_MODE": "true",
                        "APPROVED_SIGNAL_MAX_OPEN_POSITIONS": "8",
                        "DASHBOARD_COOKIE_SECURE": "false",
                        "PYTHONPATH": str(root), "PYTHONUNBUFFERED": "1",
                        "PYTHONDONTWRITEBYTECODE": "1"})
    # .env는 기존 각 서비스의 load_dotenv(override=False)가 읽는다.
    for key in ("SOLANA_PRIVATE_KEY", "SOLANA_PRIVATE_KEY_ENCRYPTED",
                "SOLANA_KEY_ENCRYPTION_KEY", "LIVE_TRADING_ACK"):
        environment.pop(key, None)
    return environment


def _kernel32():
    from ctypes import wintypes
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.GetProcessTimes.argtypes = (wintypes.HANDLE,) + (ctypes.POINTER(wintypes.FILETIME),) * 4
    kernel.GetProcessTimes.restype = wintypes.BOOL
    kernel.TerminateProcess.argtypes = (wintypes.HANDLE, wintypes.UINT)
    kernel.TerminateProcess.restype = wintypes.BOOL
    kernel.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel.CloseHandle.restype = wintypes.BOOL
    return kernel


def _handle_creation(kernel, handle) -> str:
    from ctypes import wintypes
    fields = [wintypes.FILETIME() for _ in range(4)]
    if not kernel.GetProcessTimes(handle, *(ctypes.byref(field) for field in fields)):
        raise RuntimeError("Process creation identity could not be inspected")
    return str((fields[0].dwHighDateTime << 32) | fields[0].dwLowDateTime)


def process_creation(pid: int) -> str | None:
    kernel = _kernel32()
    handle = kernel.OpenProcess(0x1000 | 0x100000, False, pid)
    if not handle:
        if ctypes.get_last_error() == 87:  # ERROR_INVALID_PARAMETER: PID 없음
            return None
        raise RuntimeError("Process identity access failed")
    try:
        if kernel.WaitForSingleObject(handle, 0) == 0:
            return None
        return _handle_creation(kernel, handle)
    finally:
        kernel.CloseHandle(handle)


def process_snapshot(root: Path, *, include_ambiguous: bool = False,
                     registered_pids: tuple[int, ...] = ()) -> list[dict[str, Any]]:
    """Windows 대소문자 경로와 등록 PID를 조회하고 cutover는 모호한 core 실행도 거부한다."""
    literal = str(root).replace("'", "''")
    if len(registered_pids) > 32 or any(not isinstance(pid, int) or isinstance(pid, bool) or pid < 1 for pid in registered_pids):
        raise RuntimeError("Registered process inventory invalid")
    predicates = [f"($_.CommandLine -and $_.CommandLine.IndexOf('{literal}', [System.StringComparison]::OrdinalIgnoreCase) -ge 0)"]
    if include_ambiguous:
        # -m 실행은 CommandLine에 cwd가 없다. 다른 checkout일 가능성도 보수적으로 차단한다.
        predicates.append("($_.CommandLine -and $_.CommandLine -match '(?i)(?:^|\\s)-m\\s+src\\.(?:monitor|risk_manager|wallet_feeder)(?:\\s|$)')")
    if registered_pids:
        predicates.append("($_.ProcessId -in @(" + ",".join(str(pid) for pid in registered_pids) + "))")
    command = ("$ErrorActionPreference='Stop'; "
               "@(Get-CimInstance Win32_Process -Filter \"Name LIKE 'python%'\" | "
               "Where-Object {" + " -or ".join(predicates) + "} | "
               "Select-Object @{n='pid';e={$_.ProcessId}}, "
               "@{n='parent_pid';e={$_.ParentProcessId}}, "
               "@{n='executable';e={$_.ExecutablePath}}, "
               "@{n='command';e={$_.CommandLine}}) | ConvertTo-Json -Compress")
    result = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", command],
                            capture_output=True, text=True, check=False,
                            creationflags=subprocess.CREATE_NO_WINDOW)
    if result.returncode:
        raise RuntimeError("Windows process inventory failed")
    rows = json.loads(result.stdout or "[]")
    if isinstance(rows, dict):
        rows = [rows]
    output = []
    for row in rows:
        creation = process_creation(int(row["pid"]))
        if creation is not None:
            output.append({**row, "created": creation})
    return output


def process_matches(row: dict[str, Any], record: dict[str, Any]) -> bool:
    return (int(row["pid"]) == int(record["pid"]) and row["created"] == record["created"]
            and str(row["executable"]).casefold() == str(record["executable"]).casefold()
            and str(row["command"]).strip().casefold() == str(record["command"]).strip().casefold())


def terminate_record(record: dict[str, Any]) -> None:
    """같은 핸들에서 생성 시각을 확인하여 PID 재사용을 피한다."""
    kernel = _kernel32()
    handle = kernel.OpenProcess(0x1000 | 0x100000 | 1, False, int(record["pid"]))
    if not handle:
        raise RuntimeError("Process stop access failed")
    try:
        if _handle_creation(kernel, handle) != record["created"]:
            raise RuntimeError("PID identity changed; stop refused")
        if kernel.WaitForSingleObject(handle, 0) == 0:
            return
        if not kernel.TerminateProcess(handle, 0):
            if kernel.WaitForSingleObject(handle, 0) == 0:
                return
            raise RuntimeError("Process stop failed")
        if kernel.WaitForSingleObject(handle, 10000) != 0:
            raise RuntimeError("Process did not stop within ten seconds")
    finally:
        kernel.CloseHandle(handle)


def start_process(root: Path, command: list[str], service: str,
                  environment: dict[str, str]) -> dict[str, Any]:
    logs = root / "logs"
    stem = "service" if service == "monitor" else service.replace("-", "_")
    with (logs / f"{stem}.stdout.log").open("ab") as stdout, \
            (logs / f"{stem}.stderr.log").open("ab") as stderr:
        child = subprocess.Popen(command, cwd=root, env=environment,
                                 stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
                                 close_fds=True, creationflags=subprocess.CREATE_NO_WINDOW)
    try:
        creation = process_creation(child.pid)
    except Exception:
        child.terminate()
        child.wait(timeout=10)
        raise
    if creation is None:
        raise RuntimeError(f"{service} exited before PID identity registration")
    record = {"pid": child.pid, "created": creation, "executable": command[0],
              "command": subprocess.list2cmdline(command), "children": []}
    # Windows venv의 redirector와 실제 base Python은 하나의 서비스다.
    # 자신이 시작한 launcher의 직계 자식만 허용된 동일 인자로 등록한다.
    try:
        rows = process_snapshot(root)
        for row in rows:
            if row.get("parent_pid") != child.pid:
                continue
            raw = str(row["command"])
            tail = raw[raw.find('"', 1) + 1:].strip() if raw.startswith('"') else raw.partition(" ")[2]
            if tail.casefold() != subprocess.list2cmdline(command[1:]).casefold():
                raise RuntimeError("Unexpected launcher child arguments")
            record["children"].append(row)
        if len(record["children"]) > 1:
            raise RuntimeError("Unexpected multiple launcher children")
    except Exception:
        child.terminate()
        child.wait(timeout=10)
        raise
    return record


def dashboard_port_available() -> bool:
    with socket.socket() as listener:
        try:
            listener.bind(("127.0.0.1", 8501))
            return True
        except OSError:
            return False


def manage(root: Path, python: Path, action: str, names: tuple[str, ...]) -> list[dict[str, Any]]:
    if sys.platform != "win32":
        raise RuntimeError("This runner supports Windows only")
    root, python = root.resolve(), python.resolve()
    if not python.is_file() or not (root / "src/monitor.py").is_file():
        raise RuntimeError("Runtime source or Python interpreter is missing")
    logs = root / "logs"
    logs.mkdir(exist_ok=True)
    registry_path = logs / "local_runner.json"
    environment = paper_environment(root) if action == "start" else None
    with exclusive_file_lock(registry_path):
        registry = read_json(registry_path, {"version": 0, "services": {}})
        records = registry["services"]
        snapshot = process_snapshot(root)
        results = []
        for name in names:
            command = service_command(root, python, name)
            script = command[4] if name == "dashboard" else command[1]
            candidates = [row for row in snapshot if script.casefold() in str(row["command"]).casefold()]
            record = records.get(name)
            identities = [record, *record.get("children", [])] if record else []
            active = [row for row in candidates if any(process_matches(row, identity) for identity in identities)]
            if len(candidates) != len(active):
                raise RuntimeError(f"{name}: duplicate or unmanaged process; action refused")
            if action == "start" and not active:
                if name == "dashboard" and not dashboard_port_available():
                    raise RuntimeError("Dashboard port 8501 is already occupied")
                records[name] = start_process(root, command, name, environment)
                # 매 시작 직후 저장하여 일부 서비스 실패 시에도 이미 시작한 PID를 보존한다.
                registry["version"] += 1
                atomic_write_json(registry_path, registry)
                result_record = records[name]
                actual = result_record["children"][-1] if result_record.get("children") else result_record
                results.append({"service": name, "state": "started", "pid": actual["pid"],
                                "launcher_pid": result_record["pid"]})
            elif action == "stop" and active:
                # 실제 Python을 먼저 중지하고 redirector의 자연 종료를 기다린다.
                for identity in reversed(identities):
                    if process_creation(int(identity["pid"])) == identity["created"]:
                        terminate_record(identity)
                records.pop(name)
                registry["version"] += 1
                atomic_write_json(registry_path, registry)
                results.append({"service": name, "state": "stopped"})
            else:
                state = "running" if active else ("finished_or_stopped" if record else "not_started")
                children = record.get("children", []) if record else []
                actual = next((row for row in active if any(process_matches(row, child) for child in children)),
                              active[0] if active else None)
                results.append({"service": name, "state": state,
                                "pid": actual["pid"] if actual else None,
                                "launcher_pid": record["pid"] if active else None,
                                "pids": [row["pid"] for row in active]})
        return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop", "status"))
    parser.add_argument("service", nargs="?", default="all", choices=("all",) + SERVICES)
    parser.add_argument("--root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    args = parser.parse_args()
    names = SERVICES if args.action != "start" else CORE_SERVICES
    if args.service != "all":
        names = (args.service,)
    try:
        print(json.dumps(manage(args.root, args.python, args.action, names), ensure_ascii=False))
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
