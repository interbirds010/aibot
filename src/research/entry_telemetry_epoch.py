"""Telemetry epoch와 Windows cutover 경계. 현재 runtime에는 적용하지 않는다."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import os
from pathlib import Path
import platform
import math
import tempfile
from uuid import UUID, uuid4

from src.state_store import exclusive_file_lock, read_json, update_json
from src.research.n3_shadow import build_identity, digest, immutable, read_immutable

SCHEMAS = {"predictor": 2, "receipt": 1, "outcome": 1}
STATE_FILES = ("paper_trades.json", "wallets.json", "wallet_performance.json", "global_metrics.json", "shadow_trades.json")
_runtime_session_id = None
CONFIG_KEYS = frozenset({"paper_buy_basis_points", "single_strength_lamports", "momentum_min_volume_m5_usd", "momentum_min_net_buys_m5", "momentum_min_buy_sell_ratio", "momentum_min_liquidity_usd", "momentum_min_pair_age_seconds", "route_b_min_safety_score", "unknown_whale_min_count"})


def _safe_config(config: dict) -> dict:
    if not isinstance(config, dict) or set(config) - CONFIG_KEYS:
        raise RuntimeError("Telemetry config must use explicit non-secret keys")
    if any(isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) for value in config.values()):
        raise RuntimeError("Telemetry config must contain finite numeric scalars")
    return dict(config)


def _pointer(path: Path) -> dict:
    pointer = read_json(path, {})
    if pointer and pointer.get("content_hash") != digest({key: value for key, value in pointer.items() if key != "content_hash"}):
        raise RuntimeError("Telemetry pointer hash mismatch")
    return pointer


def _set_pointer(path: Path, values: dict) -> None:
    def mutate(document):
        next_version = int(document.get("version", 0)) + 1
        document.clear()
        document.update(values)
        # update_json increments version after the mutator returns.
        document["version"] = next_version - 1
        document["content_hash"] = digest({**values, "version": next_version})
    update_json(path, {"version": 0}, mutate, operation="telemetry_epoch_pointer")


def directory(root: Path) -> Path:
    return Path(root).resolve() / "data" / "research" / "entry_telemetry"


def safe_runtime_config() -> dict:
    """모니터의 실제 상수와 Paper 모드만 fingerprint에 포함한다."""
    from src import monitor
    names = {"paper_buy_basis_points": "PAPER_BUY_BASIS_POINTS", "single_strength_lamports": "SINGLE_STRENGTH_LAMPORTS",
             "momentum_min_volume_m5_usd": "MOMENTUM_MIN_VOLUME_M5_USD", "momentum_min_net_buys_m5": "MOMENTUM_MIN_NET_BUYS_M5",
             "momentum_min_buy_sell_ratio": "MOMENTUM_MIN_BUY_SELL_RATIO", "momentum_min_liquidity_usd": "MOMENTUM_MIN_LIQUIDITY_USD",
             "momentum_min_pair_age_seconds": "MOMENTUM_MIN_PAIR_AGE_SECONDS", "route_b_min_safety_score": "ROUTE_B_MIN_SAFETY_SCORE",
             "unknown_whale_min_count": "UNKNOWN_WHALE_MIN_COUNT"}
    return {key: getattr(monitor, name) for key, name in names.items()}


def n3_closed(root: Path) -> dict:
    """기존 N3 종료 증거를 읽으며 평가값은 반환하지 않는다."""
    side = Path(root) / "data" / "n3_shadow"
    manifest = read_immutable(side / "manifest.json")
    if not manifest:
        raise RuntimeError("N3 closure evidence missing")
    closure = read_immutable(side / "closure.json")
    report = read_immutable(side / "final_report.json")
    observer = read_json(side / "observer_state.json", {})
    cohort = manifest.get("cohort_id")
    end_seq = closure.get("end_event_seq")
    if (not isinstance(cohort, str) or not cohort or not isinstance(end_seq, int) or isinstance(end_seq, bool)
            or end_seq < 1 or not closure or not report or closure.get("cohort_id") != cohort or report.get("cohort_id") != cohort
            or report.get("closure") != closure or observer.get("status") != "CLOSED"
            or observer.get("cohort_id") != cohort or observer.get("cursor", -1) < end_seq):
        raise RuntimeError("N3 not closed with matching final report")
    return {"cohort_id": cohort, "closure_hash": digest(closure), "final_report_hash": digest(report),
            "end_event_seq": closure["end_event_seq"]}


def _file_hash(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as file:
        while block := file.read(1024 * 1024):
            checksum.update(block)
    return checksum.hexdigest()


def state_hashes(root: Path) -> dict:
    """비밀 파일을 제외한 원장/상태의 bytes hash만 읽는다."""
    hashes = {}
    total = 0
    excluded = directory(root)
    for path in (Path(root) / "data").rglob("*.json"):
        if excluded in path.parents:
            continue
        if path.is_symlink():
            raise RuntimeError("State snapshot refuses symlink")
        total += path.stat().st_size
        if len(hashes) >= 20000 or total > 512 * 1024 * 1024 or path.stat().st_size > 256 * 1024 * 1024:
            raise RuntimeError("State snapshot bounded limit exceeded")
        hashes[str(path.relative_to(Path(root) / "data"))] = _file_hash(path)
    return hashes


def create_snapshot(root: Path, *, stopped_proof: dict) -> dict:
    """Control이 모두 중지됐다는 외부 확인 후 상태를 immutable하게 기록한다."""
    if stopped_proof.get("active_process_count") != 0 or stopped_proof.get("runtime_root") != str(Path(root).resolve()):
        raise RuntimeError("Stopped process proof required")
    closure = n3_closed(root)
    path = Path(root) / "data" / "paper_trades.json"
    with exclusive_file_lock(path, operation="telemetry_cutover_snapshot"):
        ledger = read_json(path, {})
        if ledger.get("schema_version") != 2 or not isinstance(ledger.get("next_event_seq"), int) or isinstance(ledger["next_event_seq"], bool) or ledger["next_event_seq"] < 1:
            raise RuntimeError("Paper ledger schema/boundary invalid")
        marker = {"snapshot_id": str(uuid4()), "created_utc": datetime.now(timezone.utc).isoformat(),
                  "runtime_root": str(Path(root).resolve()), "next_event_seq": ledger["next_event_seq"],
                  "state_hashes": state_hashes(root), "n3_closure": closure, "stopped_proof": stopped_proof}
        destination = directory(root) / "snapshots" / marker["snapshot_id"]
        destination.mkdir(parents=True, exist_ok=True)
        for name, expected in marker["state_hashes"].items():
            source = Path(root) / "data" / name
            if _file_hash(source) != expected:
                raise RuntimeError("State changed during snapshot")
            copy = destination / "bytes" / (hashlib.sha256(name.encode()).hexdigest() + ".bin")
            copy.parent.mkdir(parents=True, exist_ok=True)
            with exclusive_file_lock(copy, operation="telemetry_snapshot_bytes"):
                with tempfile.NamedTemporaryFile("wb", dir=copy.parent, delete=False) as file:
                    temporary = Path(file.name)
                    with source.open("rb") as input_file:
                        while block := input_file.read(1024 * 1024):
                            file.write(block)
                    file.flush()
                    os.fsync(file.fileno())
                os.replace(temporary, copy)
            if _file_hash(copy) != expected:
                raise RuntimeError("Snapshot copy mismatch")
        immutable(destination / "snapshot.json", marker)
        _set_pointer(directory(root) / "cutover_snapshot.json", {"snapshot_id": marker["snapshot_id"], "marker_hash": digest(marker)})
    return marker


def validate_snapshot(root: Path) -> dict:
    pointer = _pointer(directory(root) / "cutover_snapshot.json")
    marker = read_immutable(directory(root) / "snapshots" / pointer.get("snapshot_id", "missing") / "snapshot.json")
    if pointer.get("marker_hash") != digest(marker):
        raise RuntimeError("Cutover snapshot pointer mismatch")
    if not marker or marker.get("runtime_root") != str(Path(root).resolve()) or marker.get("state_hashes") != state_hashes(root):
        raise RuntimeError("Cutover state snapshot mismatch")
    if marker.get("n3_closure") != n3_closed(root):
        raise RuntimeError("N3 closure snapshot mismatch")
    copied = directory(root) / "snapshots" / marker["snapshot_id"] / "bytes"
    for name, expected in marker["state_hashes"].items():
        path = copied / (hashlib.sha256(name.encode()).hexdigest() + ".bin")
        if not path.is_file() or _file_hash(path) != expected:
            raise RuntimeError("Cutover snapshot bytes mismatch")
    return marker


def _epoch_path(root: Path, identity: str) -> Path:
    if str(UUID(identity)) != identity:
        raise RuntimeError("Invalid epoch ID")
    return directory(root) / "epochs" / identity / "epoch.json"


def create_epoch(root: Path, *, config: dict, schemas: dict = SCHEMAS, stopped_proof: dict, new_activation: bool = False) -> dict:
    """활성화 직전 explicit 명령으로만 생성한다. 기존 active epoch는 보존한다."""
    root = Path(root).resolve()
    config = _safe_config(config)
    if next((directory(root) / "rows").glob("*.json"), None) is not None:
        raise RuntimeError("Legacy telemetry runtime rows present; migration requires separate review")
    if schemas != SCHEMAS:
        raise RuntimeError("Unsupported telemetry schemas")
    if stopped_proof.get("active_process_count") != 0 or stopped_proof.get("runtime_root") != str(root):
        raise RuntimeError("Stopped process proof required")
    snapshot = validate_snapshot(root)
    identity = build_identity(root)
    if not isinstance(identity.get("git_sha"), str) or len(identity["git_sha"]) != 40 or any(c not in "0123456789abcdef" for c in identity["git_sha"]):
        raise RuntimeError("Valid Git build SHA required")
    with exclusive_file_lock(directory(root) / "epoch_creation", operation="telemetry_epoch_create"):
        if (directory(root) / "active.json").exists() and not new_activation:
            return require_epoch(root, config=config, schemas=schemas)
        now = datetime.now(timezone.utc)
        marker = {"telemetry_epoch_id": str(uuid4()), "build_sha": identity["git_sha"], "build": identity,
                  "config_fingerprint": digest(config), "safe_config": config, "schema_versions": dict(schemas),
                  "start_event_seq": snapshot["next_event_seq"], "start_utc": now.isoformat(),
                  "start_kst": now.astimezone(timezone(timedelta(hours=9))).isoformat(),
                  "platform": platform.platform(), "os": os.name, "creator_process_id": os.getpid(),
                  "creator_session_id": str(uuid4()), "runtime_session_binding": "immutable sessions registered at worker startup",
                  "snapshot_id": snapshot["snapshot_id"], "n3_closure": snapshot["n3_closure"]}
        immutable(_epoch_path(root, marker["telemetry_epoch_id"]), marker)
        _set_pointer(directory(root) / "active.json", {"telemetry_epoch_id": marker["telemetry_epoch_id"], "marker_hash": digest(marker)})
        return marker


def require_epoch(root: Path, *, provenance: dict | None = None, config: dict | None = None, schemas: dict = SCHEMAS) -> dict:
    pointer = _pointer(directory(root) / "active.json")
    if not pointer:
        raise RuntimeError("Telemetry epoch not created")
    marker = read_immutable(_epoch_path(root, pointer["telemetry_epoch_id"]))
    if not marker or pointer.get("marker_hash") != digest(marker):
        raise RuntimeError("Telemetry epoch marker mismatch")
    if n3_closed(root) != marker.get("n3_closure"):
        raise RuntimeError("Telemetry epoch N3 closure mismatch")
    actual = build_identity(Path(root))
    if marker.get("build") != actual:
        raise RuntimeError("Telemetry epoch build mismatch")
    if marker.get("config_fingerprint") != digest(_safe_config(config if config is not None else safe_runtime_config())):
        raise RuntimeError("Telemetry epoch config mismatch")
    if marker.get("schema_versions") != schemas:
        raise RuntimeError("Telemetry epoch schema mismatch")
    if marker.get("os") != os.name or marker.get("platform") != platform.platform():
        raise RuntimeError("Telemetry epoch platform mismatch")
    if provenance is not None:
        if provenance.get("git_sha") != marker["build_sha"]:
            raise RuntimeError("Telemetry session build mismatch")
        if provenance.get("config_fingerprint") != marker["config_fingerprint"]:
            raise RuntimeError("Telemetry session config mismatch")
    return marker


def register_session(root: Path, marker: dict, session_id: str, process_id: int, provenance: dict) -> dict:
    """새 프로세스의 session을 epoch에 연결하고 marker는 수정하지 않는다."""
    if not isinstance(session_id, str) or len(session_id) > 128 or not session_id or not isinstance(process_id, int) or process_id <= 0:
        raise RuntimeError("Telemetry session identity invalid")
    global _runtime_session_id
    if read_immutable(_epoch_path(root, marker["telemetry_epoch_id"])) != marker:
        raise RuntimeError("Session requires immutable epoch marker")
    binding = {"telemetry_epoch_id": marker["telemetry_epoch_id"], "session_id": session_id,
               "process_id": process_id, "build_sha": marker["build_sha"],
               "config_fingerprint": marker["config_fingerprint"], "schema_versions": marker["schema_versions"]}
    if os.name == "nt" and process_id == os.getpid():
        from scripts.local_paper_runner import process_creation
        binding["process_created"] = process_creation(process_id)
        if binding["process_created"] is None:
            raise RuntimeError("Runtime process creation identity missing")
    if provenance.get("git_sha") != binding["build_sha"]:
        raise RuntimeError("Telemetry session build mismatch")
    if provenance.get("config_fingerprint") != binding["config_fingerprint"]:
        raise RuntimeError("Telemetry session config mismatch")
    session_path = _epoch_path(root, marker["telemetry_epoch_id"]).parent / "sessions" / (hashlib.sha256(session_id.encode()).hexdigest() + ".json")
    immutable(session_path, binding)
    _runtime_session_id = session_id
    return binding


def stop_requested(root: Path, service: str, *, session_id: str | None = None, process_id: int | None = None) -> bool:
    """일치하는 PID/생성시각/session의 정지 요청만 경계에서 확인한다."""
    if os.name != "nt":
        return False
    from scripts.local_paper_runner import process_creation
    try:
        pid = os.getpid() if process_id is None else process_id
        session = _runtime_session_id if session_id is None else session_id
        created = process_creation(pid)
        request = read_immutable(stop_request_path(root, service, pid, created, session)) if session and created else {}
        return bool(request and session and request.get("process_id") == pid
                    and request.get("created") == created
                    and request.get("session_id") == session)
    except Exception:
        return False


should_stop = stop_requested


def acknowledge_stopped(root: Path, service: str, *, session_id: str | None = None, drained: bool = True, **details) -> None:
    """거래 drain 후 acknowledgement를 별도 immutable 파일로 기록한다."""
    if not drained or not stop_requested(root, service, session_id=session_id):
        return
    from scripts.local_paper_runner import process_creation
    request = read_immutable(stop_request_path(root, service, os.getpid(), process_creation(os.getpid()), session_id or _runtime_session_id))
    immutable(directory(root) / "stop_acknowledgements" / (request["request_id"] + ".json"),
              {**request, "drained": True, "acknowledged_utc": datetime.now(timezone.utc).isoformat()})


def stop_request_path(root: Path, service: str, process_id: int, created: str, session_id: str) -> Path:
    if service not in ("monitor", "risk-manager"):
        raise RuntimeError("Unknown trading service")
    key = digest({"service": service, "process_id": process_id, "created": created, "session_id": session_id})
    return directory(root) / "stop_requests" / service / (key + ".json")
