"""N3 종료 후에만 수행하는 Windows telemetry cutover. 기본은 읽기 전용이다."""
from __future__ import annotations

import argparse
import ctypes
import json
import os
from pathlib import Path
import sys
import subprocess
import time
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import local_paper_runner as runner
from src.research import entry_telemetry_epoch as epoch
from src.research.n3_shadow import immutable, read_immutable


def _windows_arguments(command: str) -> list[str]:
    """Windows의 실제 argv 규칙으로 따옴표/공백을 해석한다."""
    if os.name != "nt":
        raise RuntimeError("Windows command identity parser required")
    shell = ctypes.WinDLL("shell32", use_last_error=True)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    shell.CommandLineToArgvW.argtypes = (ctypes.c_wchar_p, ctypes.POINTER(ctypes.c_int))
    shell.CommandLineToArgvW.restype = ctypes.POINTER(ctypes.c_wchar_p)
    kernel.LocalFree.argtypes = (ctypes.c_void_p,)
    kernel.LocalFree.restype = ctypes.c_void_p
    count = ctypes.c_int()
    values = shell.CommandLineToArgvW(command, ctypes.byref(count))
    if not values:
        raise RuntimeError("Cutover launcher command identity unavailable")
    try:
        if count.value > 64:
            raise RuntimeError("Cutover launcher argv bound exceeded")
        return [values[index] for index in range(count.value)]
    finally:
        kernel.LocalFree(ctypes.cast(values, ctypes.c_void_p))


def _own_launcher(row: dict) -> bool:
    """같은 CLI argv를 실행하는 실제 venv redirector 부모만 제외한다."""
    if row.get("pid") != os.getppid():
        return False
    try:
        if (not row.get("created") or runner.process_creation(row["pid"]) != row["created"]
                or str(Path(row["executable"]).resolve()).casefold() != str(Path(sys.executable).resolve()).casefold()):
            return False
        arguments = _windows_arguments(row["command"])
        if len(arguments) < 2 or not sys.argv:
            return False
        if str(Path(arguments[1]).resolve()).casefold() != str(Path(__file__).resolve()).casefold():
            return False
        # argv 전체를 비교해 action/root/python 옵션이 다른 부모를 제외하지 않는다.
        return subprocess.list2cmdline(arguments[1:]).casefold() == subprocess.list2cmdline(sys.argv).casefold()
    except (KeyError, TypeError, ValueError, OSError):
        return False


def _active_processes(root: Path) -> list[dict]:
    """명령 자신과 검증된 동일 CLI redirector만 inventory에서 제외한다."""
    registry = runner.read_json(root / "logs" / "local_runner.json", {})
    if not isinstance(registry, dict):
        raise RuntimeError("Runtime process registry malformed")
    records = registry.get("services", {})
    if not isinstance(records, dict) or any(not isinstance(record, dict) for record in records.values()):
        raise RuntimeError("Runtime process registry malformed")
    if len(records) > 8 or any(not isinstance(record.get("children", []), list)
                             or len(record.get("children", [])) > 4 for record in records.values()):
        raise RuntimeError("Runtime process registry bound invalid")
    identities = [identity for record in records.values() for identity in [record, *record.get("children", [])]]
    if any(not isinstance(identity, dict) or not isinstance(identity.get("pid"), int)
           or isinstance(identity["pid"], bool) or identity["pid"] < 1 for identity in identities):
        raise RuntimeError("Runtime process registry identity invalid")
    pids = tuple(sorted({identity["pid"] for identity in identities}))
    return [row for row in runner.process_snapshot(root, include_ambiguous=True, registered_pids=pids)
            if row["pid"] != os.getpid() and not _own_launcher(row)]


def stopped_proof(root: Path) -> dict:
    """자신의 두 Windows PID만 제외한 뒤 Control 종료를 확인한다."""
    rows = _active_processes(root)
    if rows:
        raise RuntimeError("Runtime processes still active; no snapshot/activation allowed")
    return {"runtime_root": str(Path(root).resolve()), "active_process_count": 0,
            "verified_by_pid": os.getpid()}


def _paper_config(root: Path) -> dict:
    """미래 runner/dotenv 우선순위를 재현하되 호출자 환경을 바꾸지 않는다."""
    from dotenv import dotenv_values
    from dotenv.variables import parse_variables
    from src.research.entry_telemetry_config import effective_config
    launch_environment = runner.paper_environment(root)
    configured = dotenv_values(Path(root) / ".env", interpolate=False)
    resolved = {}
    for name, value in configured.items():
        if value is not None:
            interpolation_environment = {**resolved, **launch_environment}
            resolved[name] = "".join(atom.resolve(interpolation_environment) for atom in parse_variables(value))
    return effective_config({**resolved, **launch_environment})


def _sessions(root: Path, marker: dict) -> dict:
    sessions = epoch.directory(root) / "epochs" / marker["telemetry_epoch_id"] / "sessions"
    return {binding["process_id"]: binding for path in sessions.glob("*.json")
            if (binding := read_immutable(path)) and (binding.get("process_created") is None
            or runner.process_creation(binding["process_id"]) == binding["process_created"])}


def clean_stop(root: Path, python: Path, *, timeout: float = 120.0) -> dict:
    """후속 telemetry build의 협력 정지만 요청한다. 강제 종료 fallback은 없다."""
    epoch.n3_closed(root)
    marker = epoch.require_epoch(root, config=_paper_config(root))
    bindings = _sessions(root, marker)
    registry = runner.read_json(root / "logs" / "local_runner.json", {})
    snapshot = runner.process_snapshot(root)
    requests = []
    for service in ("monitor", "risk-manager"):
        record = registry.get("services", {}).get(service)
        identities = [record, *record.get("children", [])] if record else []
        active = [row for row in snapshot if any(runner.process_matches(row, identity) for identity in identities)]
        if not active:
            continue
        row = next((row for row in reversed(active) if row["pid"] in bindings), None)
        if row is None:
            raise RuntimeError("Clean stop unsupported for unbound/frozen process; explicit operator shutdown required")
        binding = bindings[row["pid"]]
        request = {"request_id": str(uuid4()), "service": service, "process_id": row["pid"],
                   "created": row["created"], "session_id": binding["session_id"],
                   "telemetry_epoch_id": marker["telemetry_epoch_id"]}
        path = epoch.stop_request_path(root, service, row["pid"], row["created"], binding["session_id"])
        existing = read_immutable(path)
        if existing:
            request = existing
        else:
            immutable(path, request)
        requests.append(request)
    deadline = time.monotonic() + timeout
    while True:
        acknowledged = all(read_immutable(epoch.directory(root) / "stop_acknowledgements" / (request["request_id"] + ".json")).get("drained") is True for request in requests)
        exited = all(runner.process_creation(request["process_id"]) != request["created"] for request in requests)
        if acknowledged and exited:
            break
        if time.monotonic() >= deadline:
            raise RuntimeError("Clean stop timeout; processes not killed, cutover refused")
        time.sleep(0.5)
    # Dashboard는 원장을 쓰지 않는다. 소유 identity 검증을 거친 기존 runner로 종료한다.
    runner.manage(root, python, "stop", ("dashboard",))
    return stopped_proof(root)


def resume(root: Path, python: Path) -> dict:
    """중지/상태 연속성/epoch 검증을 모두 통과한 후 기존 runner로 시작한다."""
    stopped_proof(root)
    epoch.validate_snapshot(root)
    marker = epoch.require_epoch(root, config=_paper_config(root))
    results = runner.manage(root, python, "start", runner.CORE_SERVICES)
    return {"telemetry_epoch_id": marker["telemetry_epoch_id"], "services": results,
            "acceptance": "PENDING natural predictor/reject/RPC_SKIP/BUY/completed events"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "status", "clean-stop", "snapshot", "create-epoch", "validate", "resume"))
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    parser.add_argument("--execute", action="store_true", help="N3 종료 후 명시적인 변경 작업에만 필요")
    parser.add_argument("--new-activation", action="store_true", help="이전 immutable epoch를 보존하며 새 UUID 생성")
    args = parser.parse_args()
    root = args.root.resolve()
    try:
        if args.action == "plan":
            result = {"steps": ["N3 final report + closure", "owned Control shutdown; frozen build has no cooperative stop",
                                "snapshot while all runtime Python PIDs stopped", "checkout reviewed telemetry build", "compile + full tests",
                                "create-epoch", "resume", "five independent natural-event acceptance"], "activation_now": False}
        elif args.action == "status":
            result = {"runtime_process_count": len(_active_processes(root)),
                      "epoch_created": (epoch.directory(root) / "active.json").exists()}
        elif args.action == "validate":
            result = epoch.require_epoch(root, config=_paper_config(root))
        else:
            if not args.execute:
                raise RuntimeError("Mutation requires explicit --execute after N3 closure")
            if args.action == "clean-stop":
                result = clean_stop(root, args.python)
            elif args.action == "snapshot":
                result = epoch.create_snapshot(root, stopped_proof=stopped_proof(root))
            elif args.action == "create-epoch":
                result = epoch.create_epoch(root, config=_paper_config(root), stopped_proof=stopped_proof(root), new_activation=args.new_activation)
            else:
                result = resume(root, args.python)
        print(json.dumps(result, ensure_ascii=False))
    except (RuntimeError, ValueError, OSError) as exc:
        print(f"CUTOVER_BLOCKED: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
