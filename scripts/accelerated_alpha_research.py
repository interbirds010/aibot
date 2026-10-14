"""외부 immutable batch를 닫고 forensic과 shadow 등록 제안만 만든다."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.research import accelerated_alpha as core
from src.research import n3_shadow as n3


def external_output(output: Path, runtime: Path | None = None) -> Path:
    """실행 소스와 원장 안에 연구 결과를 쓰지 않는다."""
    output = output.resolve()
    forbidden = [ROOT.resolve(), core.ORIGINAL_CHECKOUT.resolve(), core.FROZEN_RUNTIME.resolve()]
    if runtime is not None:
        forbidden.append(runtime.resolve())
    if any(output == path or path in output.parents for path in forbidden):
        raise ValueError("research output must be outside source and runtime")
    return output


def n3_rows(runtime: Path) -> list[dict]:
    """기존 N3 snapshot에 실제 존재하는 entry projection만 가져온다."""
    runtime = runtime.resolve()
    sidecar = runtime / "data" / "n3_shadow"
    from src.research.n3_feasibility import source_guard
    manifest = source_guard(runtime)
    state = json.loads((sidecar / "observer_state.json").read_text(encoding="utf-8"))
    if state.get("cohort_id") != manifest["cohort_id"] or state.get("recorder_health") != "OK":
        raise ValueError("N3 identity or recording is unhealthy")
    completed = n3.read_records(sidecar, "completed", 408)
    trades = n3.completed_trades(sidecar, end_event_seq=state["cursor"])
    if len(trades) != state.get("completed_count"):
        raise ValueError("N3 completion count mismatch")
    by_id = {trade["position_id"]: trade for trade in completed}
    if len(by_id) != len(trades) or any(by_id.get(trade["position_id"]) != trade for trade in trades):
        raise ValueError("N3 completed records differ from SELL aggregation")
    rows = []
    for trade in trades:
        snapshot = trade["snapshot"]
        decision = snapshot["entry_decision_timestamp"]
        features = {}
        for name, stamp in (("analysis_duration_sec", snapshot["analysis_end_timestamp"]),
                            ("quote_preflight_duration_sec", snapshot["preflight_end_timestamp"])):
            features[name] = {"value": snapshot["raw_inputs"].get(name),
                              "observed_at": stamp, "source": "frozen_n3_entry_snapshot"}
        features["quote_age_ms"] = {"value": snapshot.get("quote_age_ms"),
                                     "observed_at": decision, "source": "frozen_n3_entry_snapshot"}
        rows.append({"position_id": trade["position_id"], "buy_event_seq": trade["buy_event_seq"],
                     "signal_timestamp": trade["signal_timestamp"], "entry_decision_timestamp": decision,
                     "closed_at": trade["closed_at"], "entry_cost_lamports": trade["entry_cost_lamports"],
                     "realized_pnl_lamports": trade["realized_pnl_lamports"], "features": features,
                     "forensic": {"source_cohort_id": manifest["cohort_id"],
                                  "trajectory_missing_reason": "not_recorded_by_frozen_n3"}})
    return core.validate_rows(rows)


def input_rows(path: Path) -> list[dict]:
    """명시적 canonical offline dataset만 읽는다."""
    if path.stat().st_size > 32 * 1024 * 1024:
        raise ValueError("offline dataset exceeds 32 MiB; partition input")
    document = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(document, dict) and "content_hash" in document:
        document = n3.read_immutable(path)
    return core.validate_rows(document.get("rows") if isinstance(document, dict) else document)


def input_definition(path: Path) -> dict:
    """등록 제안은 읽고 봉인된 후보 정의는 해시도 확인한다."""
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError("candidate definition must be an object")
    return n3.read_immutable(path) if "content_hash" in document else document


def _forensic_report(batch: dict, output: Path, screen_policy=None, label_contract=None) -> None:
    from src.research.alpha_batch_forensics import build_forensic_report
    path = output / "reports" / (batch["batch_id"] + ".json")
    policy = {"screen_policy": screen_policy, "label_contract": label_contract}
    if path.exists():
        existing = n3.read_immutable(path)
        if existing.get("batch_hash") != core.digest(batch) or existing.get("analysis_policy") != policy:
            raise ValueError("existing forensic report has conflicting batch or policy")
        return
    report = build_forensic_report(batch["rows"], batch["batch_id"], screen_policy=screen_policy, label_contract=label_contract)
    n3.immutable(path, {"schema_version": 1, "report_contract": "accelerated_forensic_v1",
                        "batch_id": batch["batch_id"], "batch_hash": core.digest(batch),
                        "report": report, "registration_mode": "PROPOSAL_ONLY", "analysis_policy": policy,
                        "discovery_endpoint": batch["discovery_endpoint"]})


def import_telemetry_batch(input_path: Path, output: Path, *, screen_policy=None, label_contract=None) -> dict:
    """동결 telemetry 표본을 보존하고 완료 Control만 forensic에 연결한다."""
    output = external_output(output)
    batch = core.import_frozen_telemetry_batch(input_path, output)
    _forensic_report(batch, output, screen_policy, label_contract)
    return {"mode": "IMPORTED", "batch_id": batch["batch_id"], "source_batch_id": batch["source_batch_id"],
            "completed_input": batch["completed_count"], "original_close_reason": batch["close_reason"],
            "source_dataset_hash": batch["source_dataset_hash"], "control_changes": False,
            "registration_mode": "PROPOSAL_ONLY", "economics_population": batch["economics_population"]}


def refresh(rows: list[dict], output: Path, as_of: str, *, execute: bool = False,
            screen_policy: dict | None = None, label_contract: dict | None = None) -> dict:
    """기본은 검토용이며 명시적 실행만 외부 batch와 report를 봉인한다."""
    output = external_output(output)
    rows = core.validate_rows(rows)
    if not execute:
        return {"mode": "PREVIEW", "completed_input": len(rows), "writes": False,
                "batch_contract": {"completed": 20, "active_kst_days": 7}}
    batches = core.freeze_batches(output, rows, as_of)
    for batch in batches:
        _forensic_report(batch, output, screen_policy, label_contract)
    used = {row["position_id"] for batch in batches for row in batch["rows"]}
    return {"mode": "EXECUTED", "completed_input": len(rows),
            "closed_batches": [batch["batch_id"] for batch in batches],
            "pending_completed": sum(row["position_id"] not in used for row in rows),
            "control_changes": False, "registration_mode": "PROPOSAL_ONLY"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    refresh_parser = sub.add_parser("refresh")
    source = refresh_parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--runtime", type=Path)
    source.add_argument("--input", type=Path)
    refresh_parser.add_argument("--output", type=Path, required=True)
    refresh_parser.add_argument("--execute", action="store_true")
    refresh_parser.add_argument("--screen-policy", type=Path)
    refresh_parser.add_argument("--label-contract", type=Path)
    refresh_parser.add_argument("--watch", action="store_true")
    refresh_parser.add_argument("--interval", type=int, choices=(60, 300), default=60)
    reg = sub.add_parser("register")
    reg.add_argument("--definition", type=Path, required=True)
    reg.add_argument("--output", type=Path, required=True)
    imported = sub.add_parser("import-telemetry-batch")
    imported.add_argument("--input", type=Path, required=True)
    imported.add_argument("--output", type=Path, required=True)
    imported.add_argument("--screen-policy", type=Path)
    imported.add_argument("--label-contract", type=Path)
    stage = sub.add_parser("stage")
    stage.add_argument("--definition", type=Path, required=True)
    stage.add_argument("--input", type=Path, required=True)
    stage.add_argument("--stage", choices=("1", "2", "3"), required=True)
    stage.add_argument("--research-root", type=Path, required=True)
    shadow = sub.add_parser("shadow")
    shadow.add_argument("--input", type=Path, required=True, help="one external Control predictor object")
    shadow.add_argument("--research-root", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.action == "import-telemetry-batch":
            result = import_telemetry_batch(args.input, args.output,
                        screen_policy=input_definition(args.screen_policy) if args.screen_policy else None,
                        label_contract=input_definition(args.label_contract) if args.label_contract else None)
        elif args.action == "register":
            definition = core.validate_definition(input_definition(args.definition))
            if core.utc(definition["start_timestamp"]) < datetime.now(timezone.utc):
                raise ValueError("new CLI registration requires a future timestamp")
            result = core.register_challenger(external_output(args.output), definition)
        elif args.action == "shadow":
            predictor = input_definition(args.input)
            result = core.record_shadow_predictor(external_output(args.research_root), predictor, datetime.now(timezone.utc).isoformat())
        elif args.action == "stage":
            definition = input_definition(args.definition)
            registered = core.registered_definition(external_output(args.research_root), definition.get("candidate_id"))
            if core.validate_definition(definition) != registered:
                raise ValueError("stage definition differs from registered immutable candidate")
            definition = registered
            result = core.persist_stage_result(external_output(args.research_root), definition["candidate_id"],
                                               input_rows(args.input), int(args.stage), datetime.now(timezone.utc).isoformat())
        else:
            external_output(args.output, args.runtime)
            if args.watch and not args.execute:
                raise ValueError("watch requires explicit --execute")
            while True:
                rows = n3_rows(args.runtime) if args.runtime else input_rows(args.input)
                result = refresh(rows, args.output, datetime.now(timezone.utc).isoformat(), execute=args.execute,
                                 screen_policy=input_definition(args.screen_policy) if args.screen_policy else None,
                                 label_contract=input_definition(args.label_contract) if args.label_contract else None)
                print(core.canonical(result), flush=True)
                if not args.watch:
                    return 0
                time.sleep(args.interval)
        print(core.canonical(result))
        return 0
    except (ValueError, RuntimeError, OSError, KeyError) as exc:
        print(core.canonical({"status": "BLOCKED", "reason": str(exc)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
