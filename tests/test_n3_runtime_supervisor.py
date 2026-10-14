from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src import service_health, state_store
from src.state_store import atomic_write_json, read_json
from scripts import n3_runtime_supervisor as supervisor


class FakeBackend:
    """프로세스를 실행하지 않고 영속 예산과 원장 불변식을 검증한다."""

    def __init__(self, root: Path):
        self.root = root
        self.control_path = root / "supervisor_control.json"
        self.health_path = root / "supervisor_health.json"
        self.cursor_path = root / "observer_state.json"
        atomic_write_json(self.cursor_path, {"cohort_id": "fixed-cohort", "cursor": 10958})
        self.rows = {service: {"state": "running", "pid": 100 + index}
                     for index, service in enumerate(supervisor.SERVICES)}
        self.guard_result = {"cursor": 10958, "next_event_seq": 10959,
                             "closed": False, "pending_fresh_buy": False}
        self.guard_failure = None
        self.process_failure = None
        self.is_disabled = False
        self.starts = []
        self.start_failure = None
        self.old_permission_error = None

    def guard(self):
        if self.guard_failure:
            raise supervisor.SafetyError(self.guard_failure)
        return copy.deepcopy(self.guard_result)

    def processes(self):
        if self.process_failure:
            raise supervisor.SafetyError(self.process_failure)
        return copy.deepcopy(self.rows)

    def read_control(self):
        return read_json(self.control_path, {})

    def write_control(self, document):
        atomic_write_json(self.control_path, document)

    def write_health(self, document):
        atomic_write_json(self.health_path, document)

    def start(self, service):
        self.starts.append(service)
        if self.start_failure:
            raise RuntimeError(self.start_failure)
        self.rows[service] = {"state": "running", "pid": 200 + len(self.starts)}
        return copy.deepcopy(self.rows[service])

    def disabled(self):
        return self.is_disabled

    def permission_error(self):
        return self.old_permission_error


class ProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.manifest_path = self.root / "data/n3_shadow/manifest.json"
        sources = {}
        for index in range(54):
            path = self.root / "src" / f"module_{index}.py"
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(f"# fixture {index}\n".encode())
            sources[path.relative_to(self.root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
        for name in ("requirements.txt", "ecosystem.config.js"):
            path = self.root / name
            path.write_bytes(b"fixture\n")
            sources[name] = hashlib.sha256(path.read_bytes()).hexdigest()
        self.digest = supervisor.canonical_digest(sources)
        self.manifest = {"cohort_id": supervisor.COHORT, "start_event_seq": supervisor.START_SEQ,
                         "contract": {"rule_hash": supervisor.RULE_HASH},
                         "build": {"git_sha": supervisor.BUILD, "source_digest": self.digest,
                                   "source_files": sources}}
        self.seal()

    def seal(self):
        self.manifest_hash = supervisor.canonical_digest(self.manifest)
        atomic_write_json(self.manifest_path, {**self.manifest, "content_hash": self.manifest_hash})

    def validate(self, head=None):
        with patch.object(supervisor, "MANIFEST_HASH", self.manifest_hash), \
             patch.object(supervisor, "SOURCE_DIGEST", self.digest), \
             patch.object(supervisor.subprocess, "run", return_value=SimpleNamespace(stdout=head or supervisor.BUILD)):
            return supervisor.source_guard(self.root)

    def test_matching_manifest_and_actual_source_inventory_pass(self):
        self.assertEqual(self.validate(), self.manifest)

    def test_frozen_head_mismatch_blocks_before_import(self):
        with self.assertRaisesRegex(supervisor.SafetyError, "HEAD mismatch"):
            self.validate("latest-main-head")

    def test_build_cohort_rule_mismatches_are_checked_individually(self):
        original = copy.deepcopy(self.manifest)
        for field, reason in (("build", "build"), ("cohort", "cohort"), ("rule", "rule hash")):
            with self.subTest(field=field):
                self.manifest = copy.deepcopy(original)
                if field == "build":
                    self.manifest["build"]["git_sha"] = "latest-main-head"
                elif field == "cohort":
                    self.manifest["cohort_id"] = "another-cohort"
                else:
                    self.manifest["contract"]["rule_hash"] = "another-rule"
                self.seal()
                with self.assertRaisesRegex(supervisor.SafetyError, reason):
                    self.validate()

    def test_source_tampering_and_extra_source_are_both_blocked(self):
        original = (self.root / "src/module_0.py").read_bytes()
        (self.root / "src/module_0.py").write_bytes(b"# changed\n")
        with self.assertRaisesRegex(supervisor.SafetyError, "source hash"):
            self.validate()
        (self.root / "src/module_0.py").write_bytes(original)
        (self.root / "src/new_telemetry.py").write_bytes(b"# extra\n")
        with self.assertRaisesRegex(supervisor.SafetyError, "source hash"):
            self.validate()


class RuntimeGuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.sidecar = self.root / "data/n3_shadow"
        self.backend = supervisor.RealBackend.__new__(supervisor.RealBackend)
        self.backend.root, self.backend.sidecar = self.root, self.sidecar
        self.backend.control_path = self.root / "harness/supervisor_control.json"
        self.backend.store = state_store
        self.backend.n3 = Mock()
        self.backend.n3.load_manifest.return_value = {"build": {"git_sha": supervisor.BUILD}}
        self.backend.n3.read_records.return_value = []
        self.backend.n3.completed_trades.return_value = []
        self.backend.n3.read_immutable.return_value = {}
        self.backend.n3.eligible.return_value = True
        self.state = {"schema_version": 1, "cohort_id": supervisor.COHORT, "cursor": 10958,
                      "admitted_count": 0, "completed_count": 0, "open_count": 0,
                      "sell_record_count": 0, "status": "RUNNING"}
        self.ledger = {"schema_version": 2, "next_event_seq": 10959,
                       "events": [{"event_id": "SKIP-10958", "event_seq": 10958,
                                   "type": "SKIPPED_BY_RPC_ERROR", "at": "2026-10-04T07:00:00Z"}],
                       "positions": {}, "cash_lamports": 100000000}
        self.save()
        guard_patch = patch.object(supervisor, "source_guard", return_value={})
        guard_patch.start()
        self.addCleanup(guard_patch.stop)

    def save(self):
        atomic_write_json(self.sidecar / "observer_state.json", self.state)
        atomic_write_json(self.root / "data/paper_trades.json", self.ledger)

    def test_valid_state_is_read_without_rewriting_cursor_or_ledger(self):
        before = [(path, path.read_bytes()) for path in
                  (self.sidecar / "observer_state.json", self.root / "data/paper_trades.json")]
        result = self.backend.guard()
        self.assertEqual(result["cursor"], 10958)
        for path, data in before:
            self.assertEqual(path.read_bytes(), data)

    def test_actual_ledger_json_parse_failure_is_blocked_by_supervisor(self):
        (self.root / "data/paper_trades.json").write_bytes(b"{broken-json")
        fake = FakeBackend(self.root / "harness")
        fake.guard = self.backend.guard
        fake.rows["observer"] = {"state": "missing"}
        result = supervisor.Supervisor(fake).check(allow_restart=True)
        self.assertEqual(result["state"], "BLOCKED")
        self.assertEqual(fake.starts, [])

    def test_ledger_invalid_cost_basis_and_event_sequence_are_blocked(self):
        for changes, reason in (({"cash_lamports": -1}, "ledger schema"),
                                ({"events": [{"type": "BUY", "event_id": "B", "event_seq": 10957}]}, "sequence"),
                                ({"positions": {"mint": {"position_id": "p", "entry_cost_lamports": 10,
                                                          "remaining_cost_lamports": 11,
                                                          "token_amount_raw": 1,
                                                          "entry_token_amount_raw": 1}}}, "position")):
            with self.subTest(reason=reason):
                original = copy.deepcopy(self.ledger)
                self.ledger.update(changes)
                self.save()
                with self.assertRaisesRegex(supervisor.SafetyError, reason):
                    self.backend.guard()
                self.ledger = original

    def test_stale_pid_does_not_change_persistent_cursor_validation(self):
        self.state["observer_pid"] = 99999
        self.save()
        self.assertEqual(self.backend.guard()["cursor"], 10958)

    @unittest.skipUnless(os.name == "nt", "Windows 프로세스 argv 전용")
    def test_stale_observer_pid_is_missing_when_inventory_has_no_process(self):
        self.state["observer_pid"] = 99999
        self.save()
        self.backend.python = self.root / "venv/Scripts/python.exe"
        self.backend.runner = Mock()
        self.backend.runner.process_snapshot.return_value = []
        self.backend.runner.manage.return_value = [{"service": s, "state": "missing"}
                                                  for s in supervisor.SERVICES[:-1]]
        self.assertEqual(self.backend.processes()["observer"]["state"], "missing")

    @unittest.skipUnless(os.name == "nt", "Windows 프로세스 argv 전용")
    def test_resident_supervisor_and_maintenance_helper_are_not_application_duplicates(self):
        self.backend.python = self.root / "venv/Scripts/python.exe"
        self.backend.home = self.root / "harness"
        self.backend.runner = Mock()
        arguments = [str(self.backend.python), str(Path(supervisor.__file__).resolve()), "status",
                     "--runtime", str(self.root), "--python", str(self.backend.python),
                     "--home", str(self.backend.home)]
        row = {"pid": 900, "parent_pid": 0, "executable": str(self.backend.python),
               "command": subprocess.list2cmdline(arguments), "created": "100"}
        self.backend.runner.process_snapshot.return_value = [row]
        self.backend.runner.manage.return_value = [{"service": s, "state": "missing"}
                                                  for s in supervisor.SERVICES[:-1]]
        self.assertEqual(self.backend.processes()["observer"]["state"], "missing")
        arguments[-1] = str(self.root / "foreign-harness")
        row["command"] = subprocess.list2cmdline(arguments)
        with self.assertRaisesRegex(supervisor.SafetyError, "unmanaged runtime"):
            self.backend.processes()

    @unittest.skipUnless(os.name == "nt", "Windows 프로세스 argv 전용")
    def test_two_live_observer_launchers_are_blocked(self):
        self.backend.python = self.root / "venv/Scripts/python.exe"
        self.backend.runner = Mock()
        command = subprocess.list2cmdline(self.backend.observer_command())
        self.backend.runner.process_snapshot.return_value = [
            {"pid": pid, "parent_pid": 0, "executable": str(self.backend.python),
             "command": command, "created": str(pid)} for pid in (501, 502)]
        self.backend.runner.manage.return_value = [{"service": s, "state": "missing"}
                                                  for s in supervisor.SERVICES[:-1]]
        with self.assertRaisesRegex(supervisor.SafetyError, "duplicate observer launchers"):
            self.backend.processes()

    def test_cursor_mismatch_and_missing_backlog_sequence_block(self):
        self.state["cohort_id"] = "wrong"
        self.save()
        with self.assertRaisesRegex(supervisor.SafetyError, "cohort/cursor"):
            self.backend.guard()
        self.state["cohort_id"] = supervisor.COHORT
        self.state["cursor"] = 10957
        self.ledger["next_event_seq"] = 10960
        self.ledger["events"][0]["event_seq"] = 10959
        self.save()
        with self.assertRaisesRegex(supervisor.SafetyError, "missing Control event"):
            self.backend.guard()

    def test_actual_eligible_backlog_is_detected_without_replay(self):
        self.ledger.update(next_event_seq=10960, events=[{"event_id": "BUY-10959", "event_seq": 10959,
                                                       "type": "BUY", "position_id": "p", "mint": "mint",
                                                       "at": "2026-10-04T07:00:00Z", "token_amount_raw": 1,
                                                       "cost_lamports": 1}])
        self.save()
        before = (self.sidecar / "observer_state.json").read_bytes()
        self.assertTrue(self.backend.guard()["pending_fresh_buy"])
        self.assertEqual((self.sidecar / "observer_state.json").read_bytes(), before)

    def test_empty_ledger_with_existing_sequence_and_malformed_buy_are_blocked(self):
        self.ledger["events"] = []
        self.save()
        with self.assertRaisesRegex(supervisor.SafetyError, "event sequence"):
            self.backend.guard()
        self.ledger["events"] = [{"event_id": "B", "event_seq": 10958, "type": "BUY",
                                  "at": "2026-10-04T07:00:00Z", "mint": "mint", "position_id": "p",
                                  "token_amount_raw": 1, "cost_lamports": -1}]
        self.save()
        with self.assertRaisesRegex(supervisor.SafetyError, "BUY cost"):
            self.backend.guard()

    def test_nonfinite_persistent_budget_is_blocked_without_reset(self):
        self.backend.control_path.parent.mkdir(parents=True, exist_ok=True)
        self.backend.control_path.write_text('{"restarts":{"observer":{"attempts":[NaN],"total":1}}}',
                                             encoding="utf-8")
        before = self.backend.control_path.read_bytes()
        with self.assertRaisesRegex(supervisor.SafetyError, "nonfinite"):
            self.backend.read_control()
        self.assertEqual(self.backend.control_path.read_bytes(), before)

    def test_old_original_health_block_is_not_current_permission_failure(self):
        created = str(int((time.time() - 60 + 11644473600) * 10_000_000))
        self.backend.last_observer = {"pid": 8388, "created": created}
        self.backend.n3.epoch.return_value = time.time()
        atomic_write_json(self.sidecar / "health.json", {"status": "BLOCKED", "category": "PermissionError",
                                                        "observer_pid": 43084, "at": "2026-10-03T15:58:44Z"})
        self.assertIsNone(self.backend.permission_error())
        self.backend.last_observer = None
        self.assertIsNone(self.backend.permission_error())

    def test_matching_fresh_nonpermission_failure_blocks(self):
        created = str(int((time.time() - 60 + 11644473600) * 10_000_000))
        self.backend.last_observer = {"pid": 8388, "created": created}
        self.backend.n3.epoch.return_value = time.time()
        atomic_write_json(self.sidecar / "health.json", {"status": "BLOCKED", "category": "RuntimeError",
                                                        "observer_pid": 8388, "at": "2026-10-04T07:00:00Z"})
        with self.assertRaisesRegex(supervisor.SafetyError, "human audit"):
            self.backend.permission_error()


class WindowsArgumentsTests(unittest.TestCase):
    def test_concurrent_harness_cli_requires_exact_installation_and_profile(self):
        root, python, home = Path("C:/runtime"), Path("C:/venv/python.exe"), Path("C:/harness")
        args = [str(python), str(Path(supervisor.__file__).resolve()), "maintenance",
                "--runtime", str(root), "--python", str(python), "--home", str(home), "--mode", "disable"]
        self.assertTrue(supervisor.harness_arguments_match(args, root, python, home))
        args[args.index("--runtime") + 1] = "C:/other-runtime"
        self.assertFalse(supervisor.harness_arguments_match(args, root, python, home))
        args[args.index("--runtime") + 1] = str(root)
        args[1] = "C:/other-harness/n3_runtime_supervisor.py"
        self.assertFalse(supervisor.harness_arguments_match(args, root, python, home))

    @unittest.skipUnless(os.name == "nt", "Windows CommandLineToArgvW 전용")
    def test_paths_with_spaces_unicode_and_trailing_backslash_roundtrip(self):
        argv = [r"C:\Program Files\Python\python.exe", "-m", "src.research.n3_shadow", "watch", "--root",
                "C:\\한글 경로\\frozen runtime\\", '--literal="quoted"']
        self.assertEqual(supervisor.windows_argv(subprocess.list2cmdline(argv)), argv)


@unittest.skipUnless(os.name == "nt", "Windows 프로세스 argv 전용")
class RuntimeFreshnessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.sidecar = self.root / "data/n3_shadow"
        self.now = 10000.0
        self.backend = supervisor.RealBackend.__new__(supervisor.RealBackend)
        self.backend.root, self.backend.sidecar = self.root, self.sidecar
        self.backend.python = self.root / "venv/Scripts/python.exe"
        self.backend.store = state_store
        self.backend.service_health = service_health
        self.backend.n3 = Mock()
        self.backend.n3.epoch.side_effect = lambda stamp: (
            datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp() if isinstance(stamp, str) else None)
        self.backend.runner = Mock()
        self.monitor_command = [str(self.backend.python), str(self.root / "src/monitor.py")]
        self.backend.runner.service_command.return_value = self.monitor_command
        self.backend.runner.manage.return_value = [
            {"service": "monitor", "state": "running", "pid": 501, "pids": [501]},
            {"service": "risk-manager", "state": "missing"},
            {"service": "dashboard", "state": "missing"}]
        self.backend.runner.process_snapshot.return_value = [
            {"pid": 501, "parent_pid": 0, "created": "100", "executable": str(self.backend.python),
             "command": subprocess.list2cmdline(self.monitor_command)},
            {"pid": 502, "parent_pid": 0, "created": "101", "executable": str(self.backend.python),
             "command": subprocess.list2cmdline(self.backend.observer_command())}]
        self.state = {"observer_pid": 502, "status": "RUNNING", "last_observed_at": self.stamp(self.now - 1)}
        self.metrics = {"monitor_started_at": self.now - 1000,
                        "monitor_process_heartbeat_at": self.now - 1,
                        "route_b_last_poll_success_at": self.now - 1,
                        "wallet_ws_state": "SUBSCRIBED", "wallet_ws_state_changed_at": self.now - 1,
                        "wallet_ws_subscribed_at": self.now - 1}
        self.save()

    def stamp(self, epoch):
        return datetime.fromtimestamp(epoch, timezone.utc).isoformat()

    def save(self):
        atomic_write_json(self.sidecar / "observer_state.json", self.state)
        atomic_write_json(self.root / "data/global_metrics.json", {"metrics": self.metrics})

    def processes(self):
        with patch.object(supervisor.time, "time", return_value=self.now):
            return self.backend.processes()

    def test_fresh_live_pid_and_subscribed_metrics_are_healthy(self):
        states = self.processes()
        self.assertEqual(states["observer"]["state"], "running")
        self.assertEqual(states["monitor"]["state"], "running")

    def test_stale_running_observer_is_degraded_even_when_pid_alive(self):
        self.state["last_observed_at"] = self.stamp(self.now - service_health.DEFAULT_MAX_AGE_SECONDS - 1)
        self.save()
        self.assertEqual(self.processes()["observer"]["state"], "degraded")

    def test_stale_monitor_heartbeat_cannot_be_masked_by_subscribed_wss(self):
        self.metrics["monitor_process_heartbeat_at"] = self.now - service_health.DEFAULT_MAX_AGE_SECONDS - 1
        self.save()
        self.assertEqual(self.processes()["monitor"]["state"], "degraded")

    def test_wss_reconnecting_is_attention_even_inside_application_grace(self):
        self.metrics["wallet_ws_state"] = "RECONNECTING"
        self.save()
        self.assertEqual(self.processes()["monitor"]["state"], "degraded")

    def test_stale_subscribed_wss_and_route_poll_each_degrade_monitor(self):
        for key, limit in (("wallet_ws_subscribed_at", service_health.DEFAULT_WS_REFRESH_MAX_AGE_SECONDS),
                           ("route_b_last_poll_success_at", service_health.DEFAULT_ROUTE_POLL_MAX_AGE_SECONDS)):
            with self.subTest(metric=key):
                original = self.metrics[key]
                self.metrics[key] = self.now - limit - 1
                self.save()
                self.assertEqual(self.processes()["monitor"]["state"], "degraded")
                self.metrics[key] = original

    def test_future_observer_and_monitor_timestamps_are_not_healthy(self):
        self.state["last_observed_at"] = self.stamp(self.now + 60)
        self.metrics["monitor_process_heartbeat_at"] = self.now + 60
        self.save()
        states = self.processes()
        self.assertEqual(states["observer"]["state"], "degraded")
        self.assertEqual(states["monitor"]["state"], "degraded")


class SupervisorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.backend = FakeBackend(Path(self.temp.name))
        self.now = 1000.0
        self.runner = supervisor.Supervisor(self.backend, clock=lambda: self.now)

    def check(self, **kwargs):
        return self.runner.check(allow_restart=True, **kwargs)

    def absent(self, service="observer"):
        self.backend.rows[service] = {"state": "missing"}

    def test_each_missing_service_is_restarted_after_backoff(self):
        for service in supervisor.SERVICES:
            with self.subTest(service=service):
                self.absent(service)
                before = len(self.backend.starts)
                self.check()
                self.assertEqual(len(self.backend.starts), before)
                self.now += 14.9
                self.check()
                self.assertEqual(len(self.backend.starts), before)
                self.now += 0.1
                self.check()
                self.assertEqual(self.backend.starts[before:], [service])

    def test_duplicate_any_service_globally_blocks_missing_service(self):
        self.absent("monitor")
        self.backend.process_failure = "duplicate live dashboard processes"
        health = self.check()
        self.now += 60
        self.check()
        self.assertEqual(health["state"], "BLOCKED")
        self.assertEqual(self.backend.starts, [])

    def test_guard_errors_are_sticky_and_never_launch(self):
        for reason in ("frozen build mismatch", "cohort marker mismatch", "N3 rule hash mismatch",
                       "unexpected source hash mismatch", "ledger parse failure"):
            with self.subTest(reason=reason):
                with tempfile.TemporaryDirectory() as directory:
                    backend = FakeBackend(Path(directory))
                    backend.rows["observer"] = {"state": "missing"}
                    backend.guard_failure = reason
                    runner = supervisor.Supervisor(backend, clock=lambda: self.now)
                    self.assertEqual(runner.check(allow_restart=True)["state"], "BLOCKED")
                    backend.guard_failure = None
                    self.now += 60
                    self.assertEqual(runner.check(allow_restart=True)["state"], "BLOCKED")
                    self.assertEqual(backend.starts, [])

    def test_disabled_marker_prevents_restart_even_after_wait(self):
        self.absent()
        self.backend.is_disabled = True
        self.assertEqual(self.check()["state"], "DISABLED")
        self.now += 1000
        self.assertEqual(self.check()["state"], "DISABLED")
        self.assertEqual(self.backend.starts, [])

    def test_closed_cohort_never_restarts_observer(self):
        self.absent()
        self.backend.guard_result["closed"] = True
        self.check()
        self.now += 1000
        self.check()
        self.assertEqual(self.backend.starts, [])

    def test_unprocessed_fresh_buy_blocks_instead_of_backfill(self):
        self.absent()
        self.backend.guard_result.update(pending_fresh_buy=True, next_event_seq=10960)
        before = self.backend.cursor_path.read_bytes()
        health = self.check()
        self.now += 60
        self.check()
        self.assertEqual(health["state"], "BLOCKED")
        self.assertEqual(self.backend.starts, [])
        self.assertEqual(self.backend.cursor_path.read_bytes(), before)

    def test_restart_preserves_persistent_cursor_bytes(self):
        before = self.backend.cursor_path.read_bytes()
        self.absent()
        self.check()
        self.now += 15
        self.check()
        self.assertEqual(self.backend.starts, ["observer"])
        self.assertEqual(self.backend.cursor_path.read_bytes(), before)

    def test_restart_storm_budget_and_backoff_survive_supervisor_restart(self):
        for delay in (15, 30, 60):
            self.absent()
            self.check()
            self.now += delay - 0.1
            self.check()
            count = len(self.backend.starts)
            self.now += 0.1
            self.check()
            self.assertEqual(len(self.backend.starts), count + 1)
            self.runner = supervisor.Supervisor(self.backend, clock=lambda: self.now)
        self.assertEqual(self.backend.starts, ["observer"] * 3)
        self.absent()
        self.check()
        self.now += 120
        self.check()
        self.assertEqual(len(self.backend.starts), 3)
        self.now += 900
        self.assertEqual(self.check()["state"], "BLOCKED")
        self.assertEqual(len(self.backend.starts), 3)
        self.runner = supervisor.Supervisor(self.backend, clock=lambda: self.now)
        self.now += 1800
        self.assertEqual(self.check()["state"], "BLOCKED")
        self.assertEqual(len(self.backend.starts), 3)

    def test_failed_start_also_consumes_persistent_budget(self):
        self.backend.start_failure = "process creation denied"
        self.absent()
        self.check()
        for delay in (15, 30, 60, 120):
            self.now += delay
            self.check()
            self.runner = supervisor.Supervisor(self.backend, clock=lambda: self.now)
        self.assertEqual(len(self.backend.starts), 3)
        self.now += 1800
        self.assertEqual(self.check()["state"], "BLOCKED")
        self.assertEqual(len(self.backend.starts), 3)

    def test_default_check_is_read_only_and_never_starts(self):
        self.absent()
        self.runner.check()
        self.now += 1000
        self.runner.check()
        self.assertEqual(self.backend.starts, [])

    def test_corrupt_supervisor_control_blocks_without_overwrite(self):
        self.backend.control_path.write_bytes(b"{corrupt-budget")
        before = self.backend.control_path.read_bytes()
        health = self.check()
        self.assertEqual(health["state"], "BLOCKED")
        self.assertEqual(self.backend.starts, [])
        self.assertEqual(self.backend.control_path.read_bytes(), before)
        self.assertEqual(read_json(self.backend.health_path, {})["state"], "BLOCKED")

    def test_degraded_live_process_is_reported_without_replacement(self):
        self.backend.rows["observer"] = {"state": "degraded", "pid": 404,
                                          "reason": "heartbeat stale"}
        health = self.check()
        self.now += 1000
        self.check()
        self.assertEqual(health["state"], "DEGRADED")
        self.assertEqual(self.backend.starts, [])

    def test_old_permission_error_is_diagnostic_not_global_block(self):
        self.backend.old_permission_error = {"status": "BLOCKED", "category": "PermissionError",
                                             "observer_pid": 1, "at": "2026-10-03T15:58:44Z"}
        self.absent()
        self.check()
        self.now += 15
        health = self.check()
        self.assertNotEqual(health["state"], "BLOCKED")
        self.assertEqual(self.backend.starts, ["observer"])

    def test_health_contains_operational_states_without_performance(self):
        health = self.check()
        self.assertEqual(health["state"], "RUNNING_HEALTHY")
        raw = json.dumps(health).lower()
        for forbidden in ("pnl", "win_rate", "roi", "would_skip", "classification"):
            self.assertNotIn(forbidden, raw)
        self.assertEqual(self.backend.starts, [])

    def test_live_pids_do_not_clear_persistent_blocked_health(self):
        atomic_write_json(self.backend.control_path, {"blocked_reason": "immutable manifest mismatch", "restarts": {}})
        health = self.check()
        self.assertEqual(health["state"], "BLOCKED")
        self.assertTrue(health["NEEDS_ATTENTION"])
        self.assertEqual(health["observer_state"]["pid"], self.backend.rows["observer"]["pid"])
        self.assertEqual(self.backend.starts, [])

    def test_stale_running_pid_requires_attention_and_records_successful_validation(self):
        healthy = self.check()
        self.backend.rows["observer"].update(state="degraded", reason="stale observer heartbeat")
        self.now += 1000
        health = self.check()
        self.assertEqual(health["state"], "DEGRADED")
        self.assertTrue(health["NEEDS_ATTENTION"])
        self.assertNotEqual(health["last_successful_validation"], healthy["last_successful_validation"])
        self.assertEqual(self.backend.starts, [])


class ValidationRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.backend = FakeBackend(Path(self.temp.name))
        self.now = 1000.0
        self.delays = []
        self.health_writes = []
        original = self.backend.write_health

        def capture_health(document):
            self.health_writes.append(copy.deepcopy(document))
            original(document)

        self.backend.write_health = capture_health
        self.runner = supervisor.Supervisor(self.backend, clock=lambda: self.now, sleep=self.sleep)

    def sleep(self, delay):
        self.delays.append(delay)
        self.now += delay

    def test_one_permission_failure_recovers_without_latching_or_launching(self):
        guard_result = self.backend.guard()
        self.backend.guard = Mock(side_effect=[PermissionError("fixture sharing"), guard_result])
        result = self.runner.check(allow_restart=True)
        self.assertEqual(result["state"], "RUNNING_HEALTHY")
        self.assertEqual(self.delays, [1])
        self.assertEqual(self.backend.guard.call_count, 2)
        self.assertIsNone(self.backend.read_control().get("blocked_reason"))
        self.assertEqual(self.backend.starts, [])
        self.assertIn("RECOVERING", [health["state"] for health in self.health_writes])

    def test_four_permission_failures_stop_and_persist_block(self):
        self.backend.guard = Mock(side_effect=PermissionError("fixture sharing"))
        result = self.runner.check(allow_restart=True)
        self.assertEqual(result["state"], "BLOCKED")
        self.assertEqual(self.delays, [1, 5, 15])
        self.assertEqual(self.backend.guard.call_count, 4)
        self.assertEqual(self.backend.starts, [])
        self.assertTrue(self.backend.read_control()["blocked_reason"])
        self.backend.guard = Mock(return_value=self.backend.guard_result)
        restored = supervisor.Supervisor(self.backend, clock=lambda: self.now, sleep=self.sleep)
        self.assertEqual(restored.check(allow_restart=True)["state"], "BLOCKED")
        self.assertEqual(self.backend.starts, [])

    def test_recovering_with_missing_application_still_requires_attention(self):
        self.backend.rows["observer"] = {"state": "missing"}
        self.backend.guard = Mock(side_effect=[PermissionError("sharing"), self.backend.guard_result])
        health = self.runner.check(allow_restart=True)
        recovering = [item for item in self.health_writes if item["state"] == "RECOVERING"]
        self.assertTrue(recovering)
        self.assertTrue(all(item["NEEDS_ATTENTION"] for item in recovering))
        self.assertTrue(health["NEEDS_ATTENTION"])
        self.assertEqual(self.backend.starts, [])

    def test_final_control_and_health_writes_retry_once_without_latching(self):
        for name in ("write_control", "write_health"):
            with self.subTest(operation=name):
                with tempfile.TemporaryDirectory() as directory:
                    backend = FakeBackend(Path(directory))
                    original = getattr(backend, name)
                    calls = []

                    def flaky_write(document):
                        calls.append(copy.deepcopy(document))
                        if len(calls) == 1:
                            raise PermissionError(13, "fixture sharing", str(Path(directory) / f"{name}.json"))
                        return original(document)

                    setattr(backend, name, flaky_write)
                    sleeps = []
                    runner = supervisor.Supervisor(backend, clock=lambda: self.now, sleep=sleeps.append)
                    health = runner.check(allow_restart=True)
                    self.assertEqual(health["state"], "RUNNING_HEALTHY")
                    self.assertEqual(len(calls), 2)
                    self.assertEqual(sleeps, [1])
                    self.assertIsNone(backend.read_control().get("blocked_reason"))
                    self.assertEqual(read_json(backend.health_path, {})["state"], "RUNNING_HEALTHY")
                    self.assertEqual(backend.starts, [])

    def test_final_write_four_permission_failures_are_bounded_blocked(self):
        for name in ("write_control", "write_health"):
            with self.subTest(operation=name):
                with tempfile.TemporaryDirectory() as directory:
                    backend = FakeBackend(Path(directory))
                    original = getattr(backend, name)
                    failing = Mock(side_effect=PermissionError("fixture write sharing"))
                    setattr(backend, name, failing)
                    sleeps = []
                    runner = supervisor.Supervisor(backend, clock=lambda: self.now, sleep=sleeps.append)
                    health = runner.check(allow_restart=True)
                    self.assertEqual(health["state"], "BLOCKED")
                    self.assertTrue(health["NEEDS_ATTENTION"])
                    self.assertEqual(failing.call_count, 4)
                    self.assertEqual(sleeps, [1, 5, 15])
                    self.assertEqual(backend.starts, [])
                    setattr(backend, name, original)
                    self.assertEqual(runner.check(allow_restart=True)["state"], "BLOCKED")
                    self.assertEqual(backend.starts, [])
                    if name == "write_health":
                        restored = supervisor.Supervisor(backend, clock=lambda: self.now, sleep=sleeps.append)
                        self.assertEqual(restored.check(allow_restart=True)["state"], "BLOCKED")

    def test_retry_revalidates_all_state_after_process_inventory_sharing_failure(self):
        guard = self.backend.guard
        self.backend.guard = Mock(side_effect=guard)
        rows = self.backend.processes()
        self.backend.processes = Mock(side_effect=[PermissionError("inventory sharing"), rows])
        result = self.runner.check(allow_restart=True)
        self.assertEqual(result["state"], "RUNNING_HEALTHY")
        self.assertEqual(self.backend.guard.call_count, 2)
        self.assertEqual(self.backend.processes.call_count, 2)
        self.assertEqual(self.delays, [1])
        self.assertEqual(self.backend.starts, [])

    def test_windows_sharing_errors_32_and_33_retry_then_recover(self):
        for code in (32, 33):
            with self.subTest(winerror=code):
                error = OSError("Windows sharing fixture")
                error.winerror = code
                operation = Mock(side_effect=[error, "valid"])
                sleeps = []
                self.assertEqual(supervisor.retry_validation(operation, phase="fixture", sleep=sleeps.append), "valid")
                self.assertEqual(operation.call_count, 2)
                self.assertEqual(sleeps, [1])

    def test_control_read_permission_failure_retries_without_resetting_budget(self):
        saved = {"restarts": {"observer": {"attempts": [900], "total": 4, "missing_since": None,
                                           "last_reason": "process_absence"}}, "blocked_reason": None}
        atomic_write_json(self.backend.control_path, saved)
        original = self.backend.read_control
        self.backend.read_control = Mock(side_effect=[PermissionError("budget sharing"), saved])
        health = self.runner.check(allow_restart=True)
        self.assertEqual(health["state"], "RUNNING_HEALTHY")
        self.assertEqual(self.delays, [1])
        self.assertEqual(original()["restarts"]["observer"]["total"], 4)
        self.assertEqual(self.backend.starts, [])

    def test_readonly_recovery_does_not_write_health_or_control(self):
        before = self.backend.cursor_path.read_bytes()
        self.backend.guard = Mock(side_effect=[PermissionError("sharing"), self.backend.guard_result])
        health = self.runner.check(write_health=False)
        self.assertEqual(health["state"], "RUNNING_HEALTHY")
        self.assertEqual(self.health_writes, [])
        self.assertFalse(self.backend.control_path.exists())
        self.assertFalse(self.backend.health_path.exists())
        self.assertEqual(self.backend.cursor_path.read_bytes(), before)
        self.assertEqual(self.backend.starts, [])

    def test_manual_disable_during_validation_retry_suppresses_restart(self):
        self.backend.rows["observer"] = {"state": "missing"}
        self.backend.guard = Mock(side_effect=[PermissionError("sharing"), self.backend.guard_result])

        def disabling_sleep(delay):
            self.sleep(delay)
            self.backend.is_disabled = True

        self.runner.sleep = disabling_sleep
        health = self.runner.check(allow_restart=True)
        self.assertEqual(health["state"], "DISABLED")
        self.assertEqual(self.backend.starts, [])

    def test_retry_diagnostics_record_exact_file_without_exception_message_or_source_text(self):
        sentinel = "SECRET_FIXTURE_NOT_A_REAL_CREDENTIAL"
        filename = str(self.backend.root / "observer_state.json")
        error = PermissionError(13, sentinel, filename)
        self.backend.guard = Mock(side_effect=[error, self.backend.guard_result])
        with self.assertLogs(supervisor.LOG, level="WARNING") as logs:
            health = self.runner.check(allow_restart=True)
        self.assertEqual(health["state"], "RUNNING_HEALTHY")
        raw = json.dumps(self.health_writes) + "\n".join(logs.output)
        self.assertNotIn(sentinel, raw)
        diagnostic = self.health_writes[0]["error_diagnostics"]
        self.assertEqual(diagnostic["path"], filename)
        for frame in diagnostic["traceback"]:
            self.assertEqual(set(frame), {"file", "line", "function"})

    def test_hard_safety_and_parse_failures_block_immediately_without_retry(self):
        for error in [supervisor.SafetyError(reason) for reason in
                      ("frozen build mismatch", "unexpected source hash mismatch", "cohort marker mismatch",
                       "N3 rule hash mismatch", "immutable manifest mismatch")] + [json.JSONDecodeError("bad", "{", 0)]:
            with self.subTest(category=type(error).__name__, reason=str(error)):
                self.delays.clear()
                with tempfile.TemporaryDirectory() as directory:
                    backend = FakeBackend(Path(directory))
                    backend.guard = Mock(side_effect=error)
                    runner = supervisor.Supervisor(backend, clock=lambda: self.now, sleep=self.sleep)
                    self.assertEqual(runner.check(allow_restart=True)["state"], "BLOCKED")
                    self.assertEqual(backend.guard.call_count, 1)
                    self.assertEqual(self.delays, [])
                    self.assertEqual(backend.starts, [])


if __name__ == "__main__":
    unittest.main()
