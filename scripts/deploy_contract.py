"""Production deploy mode와 retention 의도를 엄격하게 해석한다."""

from __future__ import annotations

import argparse
import os
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
    parser.add_argument("--success-marker")
    parser.add_argument("--deploy-sha")
    args = parser.parse_args()
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
