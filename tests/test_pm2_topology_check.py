from __future__ import annotations

import unittest
from pathlib import Path

from scripts import pm2_topology_check as topology


UID = 1001


def pm2_app(
    name: str,
    *,
    status: str = "online",
    pid: int = 0,
    cwd: str = str(topology.APP_ROOT),
) -> dict:
    spec = topology.SPEC_BY_NAME[name]
    return {
        "name": name,
        "pid": pid,
        "pm2_env": {
            "status": status,
            "pm_cwd": cwd,
            "pm_exec_path": str(spec.script),
            "exec_interpreter": spec.interpreter,
            "args": list(spec.args),
        },
    }


def process(name: str, pid: int, *, uid: int = UID, cwd: str | None = None):
    spec = topology.SPEC_BY_NAME[name]
    if name == "aibot-dashboard":
        argv = (str(spec.script), *spec.args)
    else:
        argv = (spec.interpreter, str(spec.script), *spec.args)
    return topology.ProcessInfo(
        pid=pid,
        uid=uid,
        cwd=cwd if cwd is not None else str(topology.APP_ROOT),
        argv=argv,
    )


def healthy_core():
    apps = [
        pm2_app("aibot-monitor", pid=101),
        pm2_app("aibot-risk-manager", pid=102),
        pm2_app("aibot-dashboard", pid=103),
    ]
    processes = [
        process("aibot-monitor", 101),
        process("aibot-risk-manager", 102),
        process("aibot-dashboard", 103),
    ]
    listeners = [topology.ListenerInfo(inode="8501", pids=(103,))]
    return apps, processes, listeners


class TopologyValidationTests(unittest.TestCase):
    def validate(self, phase, apps, processes, listeners):
        return topology.validate_topology(
            phase=phase,
            apps=apps,
            processes=processes,
            listeners=listeners,
            expected_uid=UID,
        )

    def test_preflight_accepts_managed_existing_apps(self) -> None:
        apps, processes, listeners = healthy_core()
        apps.append(pm2_app("wallet_feeder", status="stopped"))
        self.assertEqual(
            self.validate("preflight", apps, processes, listeners),
            [],
        )

    def test_preflight_accepts_absent_apps_and_free_port(self) -> None:
        self.assertEqual(self.validate("preflight", [], [], []), [])

    def test_preflight_rejects_exact_unmanaged_orphan(self) -> None:
        errors = self.validate(
            "preflight",
            [],
            [process("aibot-risk-manager", 201)],
            [],
        )
        self.assertTrue(any("unmanaged exact process pid=201" in e for e in errors))

    def test_preflight_rejects_unrelated_port_owner(self) -> None:
        unrelated = topology.ProcessInfo(
            pid=301,
            uid=UID,
            cwd="/srv/other",
            argv=("/usr/bin/python3", "server.py"),
        )
        errors = self.validate(
            "preflight",
            [],
            [unrelated],
            [topology.ListenerInfo(inode="port", pids=(301,))],
        )
        self.assertTrue(any("listener must be the sole canonical" in e for e in errors))

    def test_preflight_rejects_wrong_cwd_or_user_as_ambiguous(self) -> None:
        wrong_cwd = process("aibot-monitor", 401, cwd="/tmp/aibot")
        wrong_user = process("aibot-risk-manager", 402, uid=0)
        errors = self.validate(
            "preflight",
            [],
            [wrong_cwd, wrong_user],
            [],
        )
        self.assertEqual(sum("ambiguous process" in e for e in errors), 2)

    def test_duplicate_pm2_name_fails(self) -> None:
        app = pm2_app("aibot-monitor", status="stopped")
        errors = self.validate("preflight", [app, app], [], [])
        self.assertTrue(any("duplicate PM2 entries count=2" in e for e in errors))

    def test_missing_pm2_cwd_fails_closed(self) -> None:
        app = pm2_app("aibot-monitor", status="stopped")
        app["pm2_env"].pop("pm_cwd")
        errors = self.validate("preflight", [app], [], [])
        self.assertTrue(any("unexpected PM2 cwd" in e for e in errors))

    def test_pm2_args_error_does_not_echo_value(self) -> None:
        app = pm2_app("wallet_feeder", status="stopped")
        app["pm2_env"]["args"] = ["--token=do-not-print"]
        errors = self.validate("preflight", [app], [], [])
        rendered = "\n".join(errors)
        self.assertIn("unexpected PM2 args", rendered)
        self.assertNotIn("do-not-print", rendered)

    def test_unrelated_pm2_app_is_ignored(self) -> None:
        unrelated = {
            "name": "another-service",
            "pid": 601,
            "pm2_env": {"status": "online"},
        }
        self.assertEqual(self.validate("preflight", [unrelated], [], []), [])

    def test_unresolved_port_owner_fails_closed(self) -> None:
        errors = self.validate(
            "preflight",
            [],
            [],
            [topology.ListenerInfo(inode="unknown", pids=())],
        )
        self.assertTrue(any("listener owner unresolved" in e for e in errors))

    def test_poststart_accepts_online_feeder(self) -> None:
        apps, processes, listeners = healthy_core()
        apps.append(pm2_app("wallet_feeder", pid=104))
        processes.append(process("wallet_feeder", 104))
        self.assertEqual(
            self.validate("poststart", apps, processes, listeners),
            [],
        )

    def test_poststart_accepts_stopped_feeder(self) -> None:
        apps, processes, listeners = healthy_core()
        apps.append(pm2_app("wallet_feeder", status="stopped"))
        self.assertEqual(
            self.validate("poststart", apps, processes, listeners),
            [],
        )

    def test_relative_script_argv_is_canonical_under_expected_cwd(self) -> None:
        monitor = process("aibot-monitor", 501)
        relative = topology.ProcessInfo(
            pid=monitor.pid,
            uid=monitor.uid,
            cwd=monitor.cwd,
            argv=(monitor.argv[0], "src/monitor.py"),
        )
        self.assertTrue(
            topology.process_matches_spec(
                relative,
                topology.SPEC_BY_NAME["aibot-monitor"],
                UID,
            )
        )

    def test_dashboard_shebang_and_absolute_page_are_canonical(self) -> None:
        spec = topology.SPEC_BY_NAME["aibot-dashboard"]
        args = list(spec.args)
        args[1] = str(topology.APP_ROOT / "src" / "dashboard.py")
        dashboard = topology.ProcessInfo(
            pid=502,
            uid=UID,
            cwd=str(topology.APP_ROOT),
            argv=(
                "/usr/bin/python3.10",
                str(spec.script),
                *args,
            ),
        )
        self.assertTrue(topology.process_matches_spec(dashboard, spec, UID))

    def test_ambiguous_process_error_does_not_echo_argv(self) -> None:
        monitor = process("aibot-monitor", 503, cwd="/tmp/aibot")
        with_secret = topology.ProcessInfo(
            pid=monitor.pid,
            uid=monitor.uid,
            cwd=monitor.cwd,
            argv=(*monitor.argv, "--token=do-not-print"),
        )
        errors = self.validate("preflight", [], [with_secret], [])
        rendered = "\n".join(errors)
        self.assertIn("ambiguous process pid=503", rendered)
        self.assertNotIn("do-not-print", rendered)


class WorkflowOrderingTests(unittest.TestCase):
    def test_checker_specs_match_ecosystem_contract(self) -> None:
        root = Path(__file__).resolve().parents[1]
        ecosystem = (root / "ecosystem.config.js").read_text(encoding="utf-8")
        for spec in topology.SPECS:
            self.assertEqual(ecosystem.count(f'name: "{spec.name}"'), 1)
            self.assertIn(f'cwd: "{topology.APP_ROOT.as_posix()}"', ecosystem)
            script = (
                spec.script.as_posix()
                if spec.name == "aibot-dashboard"
                else spec.script.relative_to(topology.APP_ROOT).as_posix()
            )
            self.assertIn(f'script: "{script}"', ecosystem)
        self.assertIn(
            'interpreter: "/var/www/aibot/venv/bin/python"', ecosystem
        )
        self.assertIn('args: "--once"', ecosystem)
        self.assertIn('autorestart: false', ecosystem)
        self.assertIn('--server.port 8501 --server.address 127.0.0.1', ecosystem)

    def test_topology_checks_and_save_marker_order(self) -> None:
        workflow = (
            Path(__file__).resolve().parents[1]
            / ".github"
            / "workflows"
            / "deploy.yml"
        ).read_text(encoding="utf-8")
        preflight = "scripts/pm2_topology_check.py --phase preflight"
        start = "pm2 startOrReload ecosystem.config.js --update-env"
        poststart = "scripts/pm2_topology_check.py --phase poststart"
        save = "pm2 save"
        marker = 'printf \'%s\\n\' "$DEPLOY_SHA" > .deployed-sha.tmp'
        self.assertLess(workflow.index(preflight), workflow.index(start))
        self.assertLess(workflow.index(start), workflow.index(poststart))
        self.assertLess(workflow.index(poststart), workflow.rindex(save))
        self.assertLess(workflow.rindex(save), workflow.index(marker))

    def test_workflow_and_checker_have_no_broad_kill(self) -> None:
        root = Path(__file__).resolve().parents[1]
        text = "\n".join(
            (
                (root / ".github" / "workflows" / "deploy.yml").read_text(
                    encoding="utf-8"
                ),
                (root / "scripts" / "pm2_topology_check.py").read_text(
                    encoding="utf-8"
                ),
            )
        )
        for forbidden in ("fuser -k", "pkill -f", "pm2 delete all", "pm2 kill"):
            self.assertNotIn(forbidden, text)
        self.assertNotIn("os.kill", text)


if __name__ == "__main__":
    unittest.main()
