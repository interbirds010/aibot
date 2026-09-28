"""Production deploy mode와 retention 의도를 엄격하게 해석한다."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DeployContract:
    mode: str
    retention_enabled: bool


@dataclass(frozen=True)
class DeploymentOutcome:
    succeeded: bool
    retention_runs: bool
    marker_advances: bool
    automatic_rollback: bool = False


@dataclass(frozen=True)
class DeployTriggerDecision:
    deploy: bool
    reason: str


NON_DEPLOY_EXACT_PATHS = frozenset(
    {
        ".github/workflows/deploy.yml",
        "scripts/deploy_contract.py",
    }
)
KNOWN_SINGLE_PATH_STATUSES = frozenset(
    {"A", "M", "D", "T", "U", "X", "B"}
)
SHA_PATTERN = re.compile(r"[0-9a-f]{40}")


def evaluate_deployment_outcome(
    contract: DeployContract, *, health_passed: bool
) -> DeploymentOutcome:
    if not health_passed:
        return DeploymentOutcome(False, False, False)
    return DeploymentOutcome(
        succeeded=True,
        retention_runs=(
            contract.mode == "normal" and contract.retention_enabled
        ),
        marker_advances=True,
    )


def write_success_marker(marker: Path, deploy_sha: str) -> None:
    temporary = marker.with_name(f"{marker.name}.tmp")
    temporary.write_text(f"{deploy_sha}\n", encoding="utf-8")
    os.replace(temporary, marker)


def is_known_non_runtime_path(path: str) -> bool:
    if path in NON_DEPLOY_EXACT_PATHS:
        return True
    if path.startswith("tests/") or path.startswith("docs/"):
        return True
    return "/" not in path and path.endswith(".md")


def classify_changed_paths(paths: tuple[str, ...]) -> DeployTriggerDecision:
    if not paths:
        return DeployTriggerDecision(False, "empty_diff")
    if all(is_known_non_runtime_path(path) for path in paths):
        return DeployTriggerDecision(False, "non_runtime_only")
    return DeployTriggerDecision(True, "runtime_or_unknown")


def parse_name_status_z(payload: bytes) -> tuple[str, ...]:
    fields = payload.split(b"\0")
    if not fields or fields[-1] != b"":
        raise ValueError("git diff output is not NUL terminated")
    fields.pop()
    paths: list[str] = []
    index = 0
    while index < len(fields):
        status = fields[index].decode("ascii")
        index += 1
        if status in KNOWN_SINGLE_PATH_STATUSES:
            path_count = 1
        elif status[:1] in {"R", "C"} and status[1:].isdigit():
            path_count = 2
        else:
            raise ValueError("unsupported git diff status")
        if index + path_count > len(fields):
            raise ValueError("git diff path record is incomplete")
        for raw_path in fields[index : index + path_count]:
            path = raw_path.decode("utf-8")
            if (
                not path
                or path.startswith("/")
                or ".." in Path(path).parts
            ):
                raise ValueError("git diff path is invalid")
            paths.append(path)
        index += path_count
    return tuple(paths)


def resolve_push_deploy_trigger(
    *, before_sha: str, after_sha: str, repository: Path | None = None
) -> DeployTriggerDecision:
    if (
        SHA_PATTERN.fullmatch(before_sha) is None
        or SHA_PATTERN.fullmatch(after_sha) is None
        or before_sha == "0" * 40
    ):
        return DeployTriggerDecision(True, "resolver_unknown")
    try:
        result = subprocess.run(
            [
                "git",
                "diff",
                "--name-status",
                "-z",
                "--find-renames",
                before_sha,
                after_sha,
                "--",
            ],
            cwd=repository,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=30,
        )
        if result.returncode != 0:
            return DeployTriggerDecision(True, "resolver_unknown")
        paths = parse_name_status_z(result.stdout)
    except (OSError, subprocess.SubprocessError, UnicodeError, ValueError):
        return DeployTriggerDecision(True, "resolver_unknown")
    return classify_changed_paths(paths)


def _trailer_value(message: str, name: str) -> str | None:
    prefix = f"{name}:"
    values = [
        line[len(prefix) :].strip()
        for line in message.splitlines()
        if line.startswith(prefix)
    ]
    if not values:
        return None
    if len(values) != 1:
        raise ValueError(f"duplicate {name} trailer")
    return values[0]


def resolve_deploy_contract(
    *,
    event_name: str,
    commit_message: str = "",
    input_mode: str = "",
    input_retention_enabled: str = "",
) -> DeployContract:
    if event_name == "workflow_dispatch":
        mode = input_mode
        retention_value = input_retention_enabled
    elif event_name == "push":
        mode = _trailer_value(commit_message, "Deploy-Mode")
        if mode is None:
            raise ValueError("push deploy requires a Deploy-Mode trailer")
        retention_value = (
            _trailer_value(commit_message, "Retention-Enabled") or "false"
        )
    else:
        raise ValueError(f"unsupported deploy event: {event_name}")

    if mode not in {"normal", "diagnostic"}:
        raise ValueError(f"invalid deploy mode: {mode}")
    if retention_value not in {"true", "false"}:
        raise ValueError(f"invalid retention flag: {retention_value}")
    retention_enabled = retention_value == "true"
    if mode == "diagnostic" and retention_enabled:
        raise ValueError("diagnostic deploy cannot enable retention")
    return DeployContract(mode=mode, retention_enabled=retention_enabled)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-name")
    parser.add_argument("--input-mode", default="")
    parser.add_argument("--input-retention-enabled", default="")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--classify-push", action="store_true")
    mode.add_argument("--success-marker")
    parser.add_argument("--deploy-sha")
    parser.add_argument("--before-sha", default="")
    parser.add_argument("--after-sha", default="")
    args = parser.parse_args()
    if args.classify_push:
        decision = resolve_push_deploy_trigger(
            before_sha=args.before_sha,
            after_sha=args.after_sha,
        )
        print(f"deploy={'true' if decision.deploy else 'false'}")
        print(f"trigger_reason={decision.reason}")
        return 0
    if args.success_marker is not None:
        if not args.deploy_sha:
            parser.error("--deploy-sha is required with --success-marker")
        write_success_marker(Path(args.success_marker), args.deploy_sha)
        return 0
    if not args.event_name:
        parser.error("--event-name is required")
    try:
        contract = resolve_deploy_contract(
            event_name=args.event_name,
            commit_message=sys.stdin.read(),
            input_mode=args.input_mode,
            input_retention_enabled=args.input_retention_enabled,
        )
    except ValueError as exc:
        parser.error(str(exc))
    print(f"deploy_mode={contract.mode}")
    print(
        "retention_enabled="
        f"{'true' if contract.retention_enabled else 'false'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
