"""Frozen N3 소스 밖에서 가용성만 복구하는 Windows supervisor."""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import importlib
import json
import logging
import math
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone, timedelta
from uuid import uuid4

BUILD = "8f0b9429352c9961b895a9f9c0e2d1f3e6b19d05"
COHORT = "cba13fbb-c702-4b18-bcf7-20b78a8e252f"
RULE_HASH = "b219661c31c132e1b2f11901664981482a9e42dc847ab300f590072eb3271165"
SOURCE_DIGEST = "f048db0dd0799fbfb8e38ecd00e69ae2b9c5813ee2ab3ac0ced56790c83fab5e"
MANIFEST_HASH = "0798898d823ea44d341130a350acd2831f80514740fade1389679814e5c7ed3b"
START_SEQ = 10901
SERVICES = ("monitor", "risk-manager", "dashboard", "observer")
WINDOW_SECONDS = 900
BACKOFF = (15, 30, 60)
VALIDATION_BACKOFF = (1, 5, 15)
LOG = logging.getLogger("n3_runtime_supervisor")


class SafetyError(RuntimeError):
    """사람이 확인하기 전에는 재시작할 수 없는 상태."""


def permission_failure(exc):
    return isinstance(exc, PermissionError) or (isinstance(exc, OSError)
                                               and getattr(exc, "winerror", None) in (5, 32, 33))


def error_diagnostics(exc, phase, action="validate"):
    """예외 원문·locals·소스 줄을 제외하고 실패 위치만 기록한다."""
    filename = getattr(exc, "supervisor_path", None) or getattr(exc, "filename", None)
    filename2 = getattr(exc, "filename2", None)
    return {"phase": phase, "action": action, "category": type(exc).__name__,
            "operation_phase": getattr(exc, "supervisor_phase", None),
            "file_action": getattr(exc, "supervisor_action", None),
            "path": str(Path(str(filename)).resolve()) if filename else None,
            "filename2": str(Path(str(filename2)).resolve()) if filename2 else None,
            "errno": getattr(exc, "errno", None),
            "winerror": getattr(exc, "winerror", None),
            "traceback": [{"file": Path(frame.filename).name, "line": frame.lineno,
                           "function": frame.name} for frame in traceback.extract_tb(exc.__traceback__)[-12:]]}


def retry_validation(operation, *, phase, sleep=time.sleep, on_retry=None):
    """파일 공유 충돌만 제한적으로 재시도하며 매번 전체 검증을 다시 수행한다."""
    for attempt in range(4):
        try:
            return operation()
        except Exception as exc:
            diagnostic = error_diagnostics(exc, phase, "write" if phase.endswith("write") else "validate")
            LOG.warning("validation failure diagnostics=%s", json.dumps(diagnostic))
            if not permission_failure(exc):
                raise
            if attempt == 3:
                error = SafetyError("validation permission failure after four attempts")
                error.diagnostics = diagnostic
                raise error from exc
            if on_retry:
                on_retry(diagnostic, attempt + 1, VALIDATION_BACKOFF[attempt])
            sleep(VALIDATION_BACKOFF[attempt])


def canonical_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode("utf-8")).hexdigest()


def read_document(path):
    def reject_nonfinite(_):
        raise SafetyError("nonfinite JSON value")
    def finite_float(value):
        result = float(value)
        if not math.isfinite(result):
            raise SafetyError("nonfinite JSON value")
        return result
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            result = json.load(handle, parse_constant=reject_nonfinite, parse_float=finite_float)
    except Exception as exc:
        exc.supervisor_path, exc.supervisor_phase, exc.supervisor_action = str(Path(path).resolve()), "json_read", "open/read"
        raise
    if not isinstance(result, dict):
        raise SafetyError(f"invalid document: {Path(path).name}")
    return result


def source_guard(root):
    """Runtime 모듈 import 전에 고정 manifest와 모든 실행 바이트를 검증한다."""
    root = Path(root).resolve()
    manifest = read_document(root / "data/n3_shadow/manifest.json")
    digest = manifest.pop("content_hash", None)
    if digest != MANIFEST_HASH or canonical_digest(manifest) != MANIFEST_HASH:
        raise SafetyError("immutable manifest mismatch")
    if manifest.get("cohort_id") != COHORT or manifest.get("start_event_seq") != START_SEQ:
        raise SafetyError("cohort/start marker mismatch")
    if manifest.get("contract", {}).get("rule_hash") != RULE_HASH:
        raise SafetyError("N3 rule hash mismatch")
    build = manifest.get("build", {})
    expected = build.get("source_files", {})
    if build.get("git_sha") != BUILD or build.get("source_digest") != SOURCE_DIGEST or len(expected) != 56:
        raise SafetyError("frozen build inventory mismatch")
    if canonical_digest(expected) != SOURCE_DIGEST:
        raise SafetyError("frozen inventory digest mismatch")
    result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True,
                            text=True, timeout=15, check=True,
                            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    if result.stdout.strip() != BUILD:
        raise SafetyError("frozen HEAD mismatch")
    paths = list((root / "src").rglob("*.py")) + list((root / "scripts").glob("*.py"))
    paths += [root / "requirements.txt", root / "ecosystem.config.js"]
    actual = {}
    for path in paths:
        try:
            if path.is_file():
                actual[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
        except Exception as exc:
            exc.supervisor_path, exc.supervisor_phase, exc.supervisor_action = str(path), "source_hash", "read_bytes"
            raise
    if actual != expected:
        raise SafetyError("unexpected source hash mismatch")
    return manifest


def windows_argv(command):
    """Windows 자체 argv parser로 인자와 경로를 비교한다."""
    from ctypes import wintypes
    shell = ctypes.WinDLL("shell32", use_last_error=True)
    shell.CommandLineToArgvW.argtypes = (wintypes.LPCWSTR, ctypes.POINTER(ctypes.c_int))
    shell.CommandLineToArgvW.restype = ctypes.POINTER(wintypes.LPWSTR)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.LocalFree.argtypes = (wintypes.HLOCAL,)
    kernel.LocalFree.restype = wintypes.HLOCAL
    count = ctypes.c_int()
    pointer = shell.CommandLineToArgvW(command, ctypes.byref(count))
    if not pointer:
        raise SafetyError("Windows argv parsing failed")
    try:
        return [pointer[i] for i in range(count.value)]
    finally:
        kernel.LocalFree(pointer)


def kst_stamp(epoch):
    return datetime.fromtimestamp(epoch, timezone(timedelta(hours=9))).isoformat()


def harness_arguments_match(argv, runtime, python, home):
    """동일 설치본의 상태/maintenance 명령을 앱 중복으로 오인하지 않는다."""
    if len(argv) < 3 or Path(argv[1]).resolve() != Path(__file__).resolve():
        return False
    action = argv[2]
    if action not in ("run", "once", "status", "clear-blocked", "maintenance"):
        return False
    options, index = {}, 3
    while index < len(argv):
        key = argv[index]
        if key == "--allow-restart" and key not in options:
            options[key] = True
            index += 1
            continue
        if key not in ("--runtime", "--python", "--home", "--invocation", "--mode") or key in options:
            return False
        if index + 1 >= len(argv):
            return False
        options[key] = argv[index + 1]
        index += 2
    for key, expected in (("--runtime", runtime), ("--python", python), ("--home", home)):
        if key not in options or Path(options[key]).resolve() != Path(expected).resolve():
            return False
    return (options.get("--invocation", "manual") in ("manual", "scheduler")
            and (options.get("--mode") in ("disable", "enable") if action == "maintenance"
                 else "--mode" not in options))


class Supervisor:
    """Backend는 검증·상태 저장·프로세스 시작을 분리하여 가짜 프로세스로 검증한다."""
    def __init__(self, backend, clock=time.time, sleep=time.sleep):
        self.backend, self.clock = backend, clock
        self.sleep = sleep
        self.session = str(uuid4())
        self.started = clock()
        self.pending_hard_block = None

    def _persist(self, operation, phase):
        return retry_validation(operation, phase=phase, sleep=self.sleep)

    def _validate(self, health=None, write_health=False):
        def operation():
            try:
                guard = self.backend.guard()
            except Exception as exc:
                exc.supervisor_phase = getattr(exc, "supervisor_phase", "guard")
                raise
            try:
                return guard, self.backend.processes()
            except Exception as exc:
                exc.supervisor_phase = getattr(exc, "supervisor_phase", "processes")
                raise
        def recovering(diagnostic, attempt, delay):
            if health is not None:
                health.update(state="RECOVERING", NEEDS_ATTENTION=True,
                              attention_reasons=["validation permission retry"],
                              error_diagnostics=diagnostic, validation_retry=attempt,
                              validation_retry_delay_seconds=delay)
                if write_health:
                    try:
                        self.backend.write_health(health)
                    except Exception as exc:
                        LOG.warning("recovering health write diagnostics=%s",
                                    json.dumps(error_diagnostics(exc, "health", "write")))
        return retry_validation(operation, phase="runtime_validation", sleep=self.sleep, on_retry=recovering)

    def clear_blocked(self):
        guard, processes = self._validate()
        if guard.get("pending_fresh_buy") and processes["observer"]["state"] == "missing":
            raise SafetyError("pending eligible fresh BUY requires human audit")
        control = self.backend.read_control()
        control["blocked_reason"] = None
        for budget in control.get("restarts", {}).values():
            budget["consecutive_start_failures"] = 0
            budget["healthy_since"] = None
        self._persist(lambda: self.backend.write_control(control), "control_write")
        self.pending_hard_block = None

    def check(self, allow_restart=False, write_health=True):
        now = self.clock()
        def control_recovering(diagnostic, attempt, delay):
            if write_health:
                document = {"state": "RECOVERING", "NEEDS_ATTENTION": True,
                            "supervisor_session": self.session, "supervisor_pid": os.getpid(),
                            "last_check_kst": kst_stamp(self.clock()), "validation_retry": attempt,
                            "validation_retry_delay_seconds": delay,
                            "attention_reasons": ["control read permission retry"],
                            "error_diagnostics": diagnostic}
                try:
                    self.backend.write_health(document)
                except Exception as exc:
                    LOG.warning("control recovery health write diagnostics=%s",
                                json.dumps(error_diagnostics(exc, "health_write", "write")))
        try:
            control = retry_validation(self.backend.read_control, phase="control_read", sleep=self.sleep,
                                       on_retry=control_recovering)
        except Exception as exc:
            self.pending_hard_block = "invalid supervisor control"
            health = {"state": "BLOCKED", "blocked_reason": "invalid supervisor control",
                      "supervisor_session": self.session, "last_check_kst": kst_stamp(now),
                      "NEEDS_ATTENTION": True,
                      "error_diagnostics": getattr(exc, "diagnostics", error_diagnostics(exc, "control_read"))}
            if write_health:
                self._persist(lambda: self.backend.write_health(health), "health_write")
            return health
        if self.pending_hard_block:
            control["blocked_reason"] = self.pending_hard_block
        health = {"supervisor_session": self.session, "supervisor_pid": os.getpid(),
                  "start_kst": kst_stamp(self.started),
                  "last_check_kst": kst_stamp(now), "frozen_build": BUILD, "cohort_id": COHORT,
                  "rule_hash": RULE_HASH, "frozen_source_digest": SOURCE_DIGEST,
                  "state": "DEGRADED", "control_states": {},
                  "observer_state": {}, "blocked_reason": control.get("blocked_reason"),
                  "restart_counts": {}, "last_restart_reason": None,
                  "last_permission_error_exit": control.get("last_permission_error_exit")}
        health.update(NEEDS_ATTENTION=False,
                      last_successful_validation=control.get("last_successful_validation"))
        try:
            guard, states = self._validate(health, write_health)
            health["last_validation_completed"] = kst_stamp(self.clock())
            control["last_successful_validation"] = health["last_validation_completed"]
            health["last_successful_validation"] = control["last_successful_validation"]
            health["control_states"] = {s: states[s] for s in SERVICES[:-1]}
            health["observer_state"] = states["observer"]
            health["cursor"] = guard["cursor"]
            health["next_event_seq"] = guard["next_event_seq"]
            if guard.get("pending_fresh_buy") and states["observer"]["state"] == "missing":
                raise SafetyError("pending eligible fresh BUY: automatic backfill refused")
            if control.get("blocked_reason"):
                health["state"] = "BLOCKED"
            elif self.backend.disabled():
                health["state"] = "DISABLED"
            else:
                health["state"] = "RUNNING_HEALTHY" if all(v["state"] == "running" for v in states.values()) else "DEGRADED"
                if guard.get("closed"):
                    health["observer_state"] = {**states["observer"], "state": "CLOSED"}
                for service in SERVICES:
                    budget = control.setdefault("restarts", {}).setdefault(service,
                        {"attempts": [], "total": 0, "missing_since": None, "last_reason": None})
                    attempts = budget["attempts"] = [t for t in budget["attempts"] if now - t < WINDOW_SECONDS]
                    health["restart_counts"][service] = {"window": len(attempts), "total": budget["total"]}
                    if states[service]["state"] != "missing" or (service == "observer" and guard.get("closed")):
                        budget["missing_since"] = None
                        if states[service]["state"] == "running":
                            healthy_since = budget.setdefault("healthy_since", now)
                            if healthy_since is None:
                                budget["healthy_since"] = now
                            elif now - healthy_since >= WINDOW_SECONDS:
                                budget["consecutive_start_failures"] = 0
                        continue
                    budget["healthy_since"] = None
                    if budget["missing_since"] is None:
                        budget["missing_since"] = now
                        LOG.warning("process disappeared service=%s", service)
                    if service == "observer":
                        health["last_permission_error_exit"] = self.backend.permission_error()
                        control["last_permission_error_exit"] = health["last_permission_error_exit"]
                    if budget.get("consecutive_start_failures", 0) >= 3:
                        raise SafetyError(f"{service}: repeated exits require human audit")
                    if len(attempts) >= 3:
                        states[service]["restart_state"] = "COOLDOWN"
                        LOG.warning("restart cooldown service=%s", service)
                        continue
                    last = attempts[-1] if attempts else budget["missing_since"]
                    if not allow_restart or now - last < BACKOFF[len(attempts)]:
                        continue
                    # 시작 직전에 모든 서비스와 상태를 다시 검사한다.
                    latest_guard, latest = self._validate(health, write_health)
                    if latest_guard.get("pending_fresh_buy") and latest["observer"]["state"] == "missing":
                        raise SafetyError("pending eligible fresh BUY: automatic backfill refused")
                    if self.backend.disabled() or latest[service]["state"] != "missing":
                        continue
                    if service == "observer" and latest_guard.get("closed"):
                        continue
                    budget["attempts"].append(now)
                    budget["total"] += 1
                    budget["consecutive_start_failures"] = budget.get("consecutive_start_failures", 0) + 1
                    if budget["consecutive_start_failures"] > 3:
                        raise SafetyError(f"{service}: repeated exits require human audit")
                    budget["last_reason"] = "process_absence"
                    control["last_restart_reason"] = f"{service}: process_absence"
                    self._persist(lambda: self.backend.write_control(control), "control_write")
                    health["last_restart_reason"] = f"{service}: process_absence"
                    LOG.warning("restart attempt service=%s count=%s", service, budget["total"])
                    health["state"] = "RECOVERING"
                    try:
                        result = self.backend.start(service)
                        LOG.info("restart success service=%s pid=%s", service, result.get("pid"))
                    except SafetyError:
                        raise
                    except Exception as exc:
                        LOG.warning("restart failed service=%s category=%s", service, type(exc).__name__)
                        if budget["consecutive_start_failures"] >= 3:
                            raise SafetyError(f"{service}: three consecutive launch failures")
                    health["restart_counts"][service] = {"window": len(budget["attempts"]), "total": budget["total"]}
        except Exception as exc:
            # 원문 오류에는 비밀값이 섞일 수 있으므로 알려진 안전 오류만 기록한다.
            reason = str(exc) if isinstance(exc, SafetyError) else f"validation failure: {type(exc).__name__}"
            control["blocked_reason"] = reason
            self.pending_hard_block = reason
            health.update(state="BLOCKED", blocked_reason=reason,
                          error_diagnostics=getattr(exc, "diagnostics", error_diagnostics(exc, "check")))
            LOG.error("supervisor blocked reason=%s", reason)
        health["NEEDS_ATTENTION"] = health["state"] in ("BLOCKED", "DEGRADED", "RECOVERING")
        health["attention_reasons"] = ([health["blocked_reason"]] if health["state"] == "BLOCKED" else
            [f"{name}: {value['state']}" for name, value in health["control_states"].items()
             if value["state"] != "running"] +
            ([f"observer: {health['observer_state'].get('state')}"]
             if health["observer_state"].get("state") not in ("running", "CLOSED") else []))
        if write_health:
            if getattr(self.backend, "last_observer", None):
                control["last_observer_identity"] = self.backend.last_observer
            health["last_restart_reason"] = control.get("last_restart_reason")
            try:
                self._persist(lambda: self.backend.write_control(control), "control_write")
                self._persist(lambda: self.backend.write_health(health), "health_write")
            except Exception as exc:
                self.pending_hard_block = "operational state write failure"
                health.update(state="BLOCKED", blocked_reason=self.pending_hard_block, NEEDS_ATTENTION=True,
                              error_diagnostics=getattr(exc, "diagnostics", error_diagnostics(exc, "state_write", "write")))
                LOG.error("operational state unavailable diagnostics=%s", json.dumps(health["error_diagnostics"]))
                # 다른 저장 대상이 열려 있으면 hard block을 남긴다. 재시작은 허용하지 않는다.
                control["blocked_reason"] = self.pending_hard_block
                failed_phase = getattr(exc, "diagnostics", {}).get("phase")
                operations = ((lambda: self.backend.write_health(health),) if failed_phase == "control_write"
                              else (lambda: self.backend.write_control(control),))
                for operation in operations:
                    try:
                        operation()
                    except Exception as store_error:
                        LOG.error("blocked state persistence unavailable diagnostics=%s",
                                  json.dumps(error_diagnostics(store_error, "blocked_state_write", "write")))
        return health


class RealBackend:
    def __init__(self, runtime, python, home):
        self.root, self.python, self.home = (Path(p).resolve() for p in (runtime, python, home))
        if self.home == self.root or self.root in self.home.parents:
            raise SafetyError("supervisor must be deployed outside frozen runtime")
        if not self.python.is_file():
            raise SafetyError("Python interpreter missing")
        self.sidecar = self.root / "data/n3_shadow"
        self.control_path = self.home / "supervisor_control.json"
        self.health_path = self.sidecar / "runtime_supervisor_health.json"
        self.marker = self.sidecar / "runtime_supervisor.disabled"
        self.last_observer = None
        source_guard(self.root)
        sys.dont_write_bytecode = True
        for name in ("src", "scripts"):
            module = sys.modules.get(name)
            if module is not None and getattr(module, "__file__", None):
                if self.root not in Path(module.__file__).resolve().parents:
                    raise SafetyError("foreign runtime module already loaded")
        sys.path.insert(0, str(self.root))
        self.store = importlib.import_module("src.state_store")
        self.n3 = importlib.import_module("src.research.n3_shadow")
        self.runner = importlib.import_module("scripts.local_paper_runner")
        self.service_health = importlib.import_module("src.service_health")
        for module in (self.store, self.n3, self.runner, self.service_health):
            if self.root not in Path(module.__file__).resolve().parents:
                raise SafetyError("runtime import identity mismatch")

    def read_control(self):
        if not self.control_path.exists():
            return {"version": 0, "blocked_reason": None, "restarts": {}}
        document = read_document(self.control_path)
        if not isinstance(document.get("restarts"), dict):
            raise SafetyError("invalid supervisor control")
        for budget in document["restarts"].values():
            if (not isinstance(budget, dict) or not isinstance(budget.get("attempts"), list)
                or len(budget["attempts"]) > 3
                or not all(type(t) in (int, float) and math.isfinite(t) for t in budget["attempts"])
                or type(budget.get("total")) is not int or budget["total"] < 0):
                raise SafetyError("invalid persistent restart budget")
        return document

    def _write(self, path, document):
        def mutate(current):
            version = current.get("version", 0)
            current.clear()
            current.update(document)
            current["version"] = version
        try:
            self.store.update_json(path, {"version": 0}, mutate, operation="n3_runtime_supervisor")
        except Exception as exc:
            exc.supervisor_path, exc.supervisor_phase, exc.supervisor_action = str(path), "state_store", "atomic update/replace"
            raise

    def write_control(self, document):
        self._write(self.control_path, document)

    def write_health(self, document):
        self._write(self.health_path, document)

    def disabled(self):
        return self.marker.exists()

    def guard(self):
        source_guard(self.root)
        manifest = self.n3.load_manifest(self.sidecar)
        with self.store.exclusive_file_lock(self.sidecar / "observer_state.json", timeout_seconds=5,
                                             operation="n3_supervisor_snapshot"):
            with self.store.exclusive_file_lock(self.root / "data/paper_trades.json", timeout_seconds=5,
                                                 operation="n3_supervisor_ledger"):
                state = read_document(self.sidecar / "observer_state.json")
                ledger = read_document(self.root / "data/paper_trades.json")
                if (ledger.get("schema_version") != 2 or type(ledger.get("next_event_seq")) is not int
                    or not isinstance(ledger.get("events"), list) or not isinstance(ledger.get("positions"), dict)
                    or type(ledger.get("cash_lamports")) is not int or ledger["cash_lamports"] < 0):
                    raise SafetyError("invalid Control ledger schema")
                events = ledger["events"]
                seqs, identities = [], set()
                for event in events:
                    if (not isinstance(event, dict) or type(event.get("event_seq")) is not int
                        or event["event_seq"] <= 0 or not isinstance(event.get("event_id"), str)
                        or not event["event_id"] or event["event_id"] in identities
                        or not isinstance(event.get("type"), str)):
                        raise SafetyError("invalid or duplicate Control event")
                    seqs.append(event["event_seq"])
                    identities.add(event["event_id"])
                if (not seqs and ledger["next_event_seq"] != 1) or (seqs and (
                    seqs != list(range(seqs[0], seqs[0] + len(seqs)))
                    or seqs[-1] != ledger["next_event_seq"] - 1)):
                    raise SafetyError("noncontiguous Control event sequence")
                for event in events:
                    if not isinstance(event.get("at"), str) or self.n3.epoch(event["at"]) is None:
                        raise SafetyError("invalid Control event timestamp")
                    if event["type"] in ("BUY", "SELL"):
                        if (not isinstance(event.get("mint"), str) or not event["mint"]
                            or not isinstance(event.get("position_id"), str) or not event["position_id"]
                            or type(event.get("token_amount_raw")) is not int or event["token_amount_raw"] <= 0):
                            raise SafetyError("invalid Control trade payload")
                        if event["type"] == "BUY" and (
                            type(event.get("cost_lamports")) is not int or event["cost_lamports"] <= 0):
                            raise SafetyError("invalid Control BUY cost")
                        if event["type"] == "SELL" and (
                            type(event.get("proceeds_lamports")) is not int or event["proceeds_lamports"] < 0
                            or type(event.get("realized_pnl_lamports")) is not int):
                            raise SafetyError("invalid Control SELL payload")
                position_ids = set()
                for mint, position in ledger["positions"].items():
                    if (not isinstance(mint, str) or not isinstance(position, dict)
                        or not isinstance(position.get("position_id"), str) or not position["position_id"]
                        or position["position_id"] in position_ids or position.get("mint") != mint
                        or any(type(position.get(k)) is not int or position[k] < 0
                               for k in ("token_amount_raw", "remaining_cost_lamports", "entry_cost_lamports", "entry_token_amount_raw"))
                        or position["remaining_cost_lamports"] > position["entry_cost_lamports"]
                        or position["token_amount_raw"] > position["entry_token_amount_raw"]):
                        raise SafetyError("invalid Control position")
                    position_ids.add(position["position_id"])
                cursor = state.get("cursor")
                if (state.get("schema_version") != 1 or state.get("cohort_id") != COHORT
                    or type(cursor) is not int or not START_SEQ - 1 <= cursor <= ledger["next_event_seq"] - 1):
                    raise SafetyError("observer cohort/cursor mismatch")
                pending = [e for e in events if e["event_seq"] > cursor]
                if ledger["next_event_seq"] - 1 > cursor and (not pending or pending[0]["event_seq"] != cursor + 1):
                    raise SafetyError("missing Control event sequence behind cursor")
                entries = self.n3.read_records(self.sidecar, "entries", 408)
                sells = self.n3.read_records(self.sidecar, "sells", 1632)
                completed = self.n3.completed_trades(self.sidecar, end_event_seq=cursor)
                committed = self.n3.read_records(self.sidecar, "completed", 408)
                snapshots = self.n3.read_records(self.sidecar, "snapshots", 408)
                if (sum(e["buy_event_seq"] <= cursor for e in entries) != state.get("admitted_count", 0)
                    or sum(e["event_seq"] <= cursor for e in sells) != state.get("sell_record_count", 0)
                    or len(completed) != state.get("completed_count", 0)
                    or state.get("open_count", 0) != state.get("admitted_count", 0) - len(completed)
                    or any(c not in committed for c in completed)):
                    raise SafetyError("committed immutable N3 count/content mismatch")
                for snapshot in snapshots:
                    if (snapshot.get("cohort_id") != COHORT or snapshot.get("rule_hash") != RULE_HASH
                        or snapshot.get("build") != manifest["build"]):
                        raise SafetyError("immutable snapshot identity mismatch")
                    if snapshot.get("would_skip") != self.n3.classify(snapshot.get("raw_inputs", {})):
                        raise SafetyError("immutable snapshot classification mismatch")
                for entry in entries:
                    if entry.get("cohort_id") != COHORT or entry.get("rule_hash") != RULE_HASH:
                        raise SafetyError("immutable entry identity mismatch")
                for name in ("final_report.json", "runtime_sources.json"):
                    if (self.sidecar / name).exists():
                        self.n3.read_immutable(self.sidecar / name)
                closure = self.n3.read_immutable(self.sidecar / "closure.json")
                if closure and (closure.get("cohort_id") != COHORT or type(closure.get("end_event_seq")) is not int
                                or closure["end_event_seq"] > cursor):
                    raise SafetyError("closure identity/cursor mismatch")
                return {"cursor": cursor, "next_event_seq": ledger["next_event_seq"],
                        "closed": bool(closure) or state.get("status") == "CLOSED",
                        "pending_fresh_buy": any(e["type"] == "BUY" and self.n3.eligible(manifest, e) for e in pending)}

    def observer_command(self):
        return [str(self.python), "-m", "src.research.n3_shadow", "watch", "--root", str(self.root)]

    def processes(self):
        rows = self.runner.process_snapshot(self.root)
        core = self.runner.manage(self.root, self.python, "status", self.runner.CORE_SERVICES)
        result = {r["service"]: {**r, "state": "running" if r["state"] == "running" else "missing"} for r in core}
        expected = self.observer_command()
        candidates = []
        config = self.python.parent.parent / "pyvenv.cfg"
        base_python = None
        if config.exists():
            for line in config.read_text(encoding="utf-8").splitlines():
                key, _, value = line.partition("=")
                if key.strip() == "home":
                    base_python = str(Path(value.strip()) / "python.exe")
        allowed = {str(self.python).casefold()}
        if base_python:
            allowed.add(base_python.casefold())
        supervisor_executables = allowed | {str(Path(exe).with_name("pythonw.exe")).casefold() for exe in allowed}
        current = next((row for row in rows if row["pid"] == os.getpid()), None)
        def own_launcher(row):
            if not current or row["pid"] != current.get("parent_pid"):
                return False
            argv = windows_argv(row["command"])
            return (len(argv) == len(sys.argv) + 1 and len(argv) > 1
                    and Path(argv[1]).resolve() == Path(__file__).resolve()
                    and argv[2:] == sys.argv[1:]
                    and str(row["executable"]).casefold() in supervisor_executables
                    and argv[0].casefold() == str(row["executable"]).casefold()
                    and int(row["created"]) <= int(current["created"]))
        rows = [row for row in rows if row["pid"] != os.getpid() and not own_launcher(row)]
        # 별도 CLI와 resident supervisor는 동일 singleton/operation 락을 사용한다.
        # 앱 PID 검사에서는 정확한 설치 경로·프로필을 가진 harness만 제외한다.
        if hasattr(self, "home"):
            rows = [row for row in rows if not (
                str(row["executable"]).casefold() in supervisor_executables
                and windows_argv(row["command"])[0].casefold() == str(row["executable"]).casefold()
                and harness_arguments_match(windows_argv(row["command"]), self.root, self.python, self.home))]
        covered = {pid for r in core for pid in r.get("pids", [])}
        for row in rows:
            argv = windows_argv(row["command"])
            if row["pid"] in covered:
                service = next(r["service"] for r in core if row["pid"] in r.get("pids", []))
                expected_core = self.runner.service_command(self.root, self.python, service)
                if (argv[1:] != expected_core[1:] or str(row["executable"]).casefold() not in allowed
                    or argv[0].casefold() != str(row["executable"]).casefold()):
                    raise SafetyError("unexpected Control command/executable")
                members = [r for r in rows if r["pid"] in next(r for r in core if r["service"] == service).get("pids", [])]
                if len(members) > 2:
                    raise SafetyError("duplicate Control processes")
                if len(members) == 2:
                    launcher = next((r for r in members if str(r["executable"]).casefold() == str(self.python).casefold()), None)
                    if not launcher:
                        raise SafetyError("invalid Control launcher")
                    child = next(r for r in members if r["pid"] != launcher["pid"])
                    if child.get("parent_pid") != launcher["pid"] or int(child["created"]) < int(launcher["created"]):
                        raise SafetyError("orphan Control child")
            if "src.research.n3_shadow" in argv:
                if (len(argv) != len(expected) or [x.casefold() for x in argv[1:]] != [x.casefold() for x in expected[1:]]
                    or str(row["executable"]).casefold() not in allowed
                    or argv[0].casefold() != str(row["executable"]).casefold()):
                    raise SafetyError("unexpected observer command/executable")
                candidates.append(row)
                covered.add(row["pid"])
            elif row["pid"] not in covered:
                raise SafetyError("unmanaged runtime Python process")
        if len(candidates) > 2:
            raise SafetyError("duplicate live observers")
        if len(candidates) == 2:
            launchers = [r for r in candidates if str(r["executable"]).casefold() == str(self.python).casefold()]
            if len(launchers) != 1:
                raise SafetyError("duplicate observer launchers")
            launcher = launchers[0]
            child = next(r for r in candidates if r["pid"] != launcher["pid"])
            if child.get("parent_pid") != launcher["pid"] or int(child["created"]) < int(launcher["created"]):
                raise SafetyError("orphan or invalid observer child")
        elif candidates and str(candidates[0]["executable"]).casefold() != str(self.python).casefold():
            raise SafetyError("unmanaged orphan observer")
        if candidates:
            actual = max(candidates, key=lambda r: int(r["created"]))
            self.last_observer = actual
            result["observer"] = {"state": "running", "pid": actual["pid"], "pids": [r["pid"] for r in candidates]}
            state = read_document(self.sidecar / "observer_state.json")
            stamp = self.n3.epoch(state.get("last_observed_at"))
            max_age = self.service_health.DEFAULT_MAX_AGE_SECONDS
            if (state.get("observer_pid") != actual["pid"] or stamp is None
                or not 0 <= time.time() - stamp <= max_age):
                result["observer"]["state"] = "degraded"
        else:
            result["observer"] = {"state": "missing", "pid": None}
        metrics_path = self.root / "data/global_metrics.json"
        if result["monitor"]["state"] == "running":
            metrics = read_document(metrics_path)
            values = metrics.get("metrics", {})
            stamp = values.get("monitor_process_heartbeat_at")
            fresh = (type(stamp) in (float, int) and math.isfinite(stamp)
                     and 0 <= time.time() - stamp <= self.service_health.DEFAULT_MAX_AGE_SECONDS
                     and str(values.get("wallet_ws_state", "")).upper() == "SUBSCRIBED"
                     and self.service_health.collection_metrics_are_fresh(values))
            result["monitor"]["collection_fresh"] = fresh
            result["monitor"]["wallet_ws_state"] = str(values.get("wallet_ws_state", ""))[:30]
            if not fresh:
                result["monitor"]["state"] = "degraded"
        return result

    def permission_error(self):
        path = self.sidecar / "health.json"
        if not self.last_observer:
            self.last_observer = self.read_control().get("last_observer_identity")
        if not path.exists() or not self.last_observer:
            return None
        health = read_document(path)
        stamp = self.n3.epoch(health.get("at"))
        created = int(self.last_observer["created"]) / 10_000_000 - 11644473600
        if (health.get("observer_pid") == self.last_observer["pid"]
            and stamp is not None and stamp >= created and 0 <= time.time() - stamp <= WINDOW_SECONDS):
            if health.get("category") != "PermissionError":
                raise SafetyError("matching observer failure requires human audit")
            return {"observer_pid": health["observer_pid"], "at": health["at"], "category": "PermissionError"}
        return None

    def start(self, service):
        if service != "observer":
            return self.runner.manage(self.root, self.python, "start", (service,))[0]
        return self.runner.start_process(self.root, self.observer_command(), "n3_observer", self.runner.paper_environment(self.root))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("run", "once", "status", "clear-blocked", "maintenance"))
    parser.add_argument("--runtime", required=True, type=Path)
    parser.add_argument("--python", required=True, type=Path)
    parser.add_argument("--home", required=True, type=Path)
    parser.add_argument("--invocation", choices=("manual", "scheduler"), default="manual")
    parser.add_argument("--allow-restart", action="store_true")
    parser.add_argument("--mode", choices=("disable", "enable"))
    args = parser.parse_args(argv)
    try:
        if args.action != "status":
            home, runtime = args.home.resolve(), args.runtime.resolve()
            if home == runtime or runtime in home.parents:
                raise SafetyError("supervisor must be deployed outside frozen runtime")
            home.mkdir(parents=True, exist_ok=True)
            handler = RotatingFileHandler(home / "n3_runtime_supervisor.log", maxBytes=1_000_000,
                                          backupCount=3, encoding="utf-8")
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            LOG.addHandler(handler)
            LOG.setLevel(logging.INFO)
        backend = retry_validation(lambda: RealBackend(args.runtime, args.python, args.home),
                                   phase="initialization")
        supervisor = Supervisor(backend)
        if args.action == "status":
            result = supervisor.check(write_health=False)
            print(json.dumps(result, ensure_ascii=False))
            return 2 if result["state"] == "BLOCKED" else 0
        if args.action == "maintenance":
            with backend.store.exclusive_file_lock(backend.home / "supervisor_operation", timeout_seconds=10,
                                                   operation="n3_supervisor_maintenance"):
                if not args.mode:
                    parser.error("maintenance requires --mode")
                if args.mode == "disable":
                    backend._write(backend.marker, {"disabled": True, "at_kst": kst_stamp(time.time())})
                else:
                    backend.guard()
                    backend.processes()
                    with backend.store.exclusive_file_lock(backend.marker, timeout_seconds=5,
                                                           operation="n3_supervisor_maintenance"):
                        backend.marker.unlink(missing_ok=True)
                print(json.dumps({"disabled": backend.disabled()}))
                return 0
        if args.action == "clear-blocked":
            with backend.store.exclusive_file_lock(backend.home / "supervisor_operation", timeout_seconds=10,
                                                   operation="n3_supervisor_clear"):
                supervisor.clear_blocked()
                print(json.dumps({"blocked_reason": None}))
                return 0
        with backend.store.exclusive_file_lock(backend.home / "supervisor_owner", timeout_seconds=1,
                                               operation="n3_supervisor_singleton"):
            LOG.info("supervisor invocation=%s session=%s", args.invocation, supervisor.session)
            while True:
                with backend.store.exclusive_file_lock(backend.home / "supervisor_operation", timeout_seconds=10,
                                                       operation="n3_supervisor_check"):
                    result = supervisor.check(allow_restart=args.action == "run" or args.allow_restart)
                if args.action == "once":
                    print(json.dumps(result, ensure_ascii=False))
                    return 2 if result["state"] == "BLOCKED" else 0
                time.sleep(30)
    except Exception as exc:
        reason = str(exc) if isinstance(exc, SafetyError) else type(exc).__name__
        LOG.error("supervisor initialization/operation blocked reason=%s", reason)
        if sys.stderr is not None:
            print(json.dumps({"state": "BLOCKED", "blocked_reason": reason,
                              "NEEDS_ATTENTION": True,
                              "error_diagnostics": getattr(exc, "diagnostics", error_diagnostics(exc, "initialization"))}),
                  file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
