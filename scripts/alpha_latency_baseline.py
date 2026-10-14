"""Frozen Control의 실제 시간 기록과 명시적 latency 허용 정책을 외부에 봉인한다."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.research import alpha_telemetry_batch as batch
from src.research import n3_feasibility as feasibility
from src.research import n3_shadow as n3
from src.research import accelerated_alpha as core
from scripts.entry_telemetry_cutover import _paper_config

METRICS = {
    "signal_to_analysis_ms": ("signal_timestamp", "analysis_start_timestamp"),
    "analysis_to_preflight_ms": ("analysis_end_timestamp", "preflight_start_timestamp"),
    "preflight_to_paper_buy_ms": ("preflight_start_timestamp", "buy_at"),
    "total_decision_ms": ("signal_timestamp", "entry_decision_timestamp"),
}


def collect(runtime: Path) -> dict:
    """N3 분류·경제 성과를 분석하지 않고 실제 BUY와 snapshot 시각만 추출한다."""
    runtime = runtime.resolve()
    manifest = feasibility.source_guard(runtime)
    ledger = feasibility.read_document(runtime / "data/paper_trades.json")
    if ledger.get("schema_version") != 2 or not isinstance(ledger.get("events"), list) or len(ledger["events"]) > 100000:
        raise ValueError("latency baseline ledger schema/scan bound invalid")
    buys = {}
    for event in ledger["events"]:
        if event.get("type") == "BUY":
            if event["event_seq"] in buys:
                raise ValueError("duplicate BUY sequence in baseline")
            buys[event["event_seq"]] = event
    now = datetime.now(timezone.utc).isoformat()
    samples = []
    for entry in n3.read_records(runtime / "data/n3_shadow", "entries", 408):
        snapshot = entry["snapshot"]
        buy = buys.get(entry["buy_event_seq"], {})
        if entry.get("cohort_id") != manifest["cohort_id"] or entry.get("rule_hash") != n3.RULE_HASH or \
                buy.get("position_id") != entry["position_id"] or buy.get("event_id") != entry["buy_event_id"]:
            raise ValueError("latency baseline Control identity mismatch")
        stamps = {**snapshot, "buy_at": buy["at"]}
        values, missing = {}, {}
        for name, (left, right) in METRICS.items():
            start, end = n3.epoch(stamps.get(left)), n3.epoch(stamps.get(right))
            if start is None or end is None:
                missing[name] = "original frozen source lacks timestamp"
            elif end < start or core.utc(stamps[right]) > core.utc(now):
                raise ValueError("latency baseline timing order invalid")
            else:
                values[name] = (end - start) * 1000
        source = n3.record_path(runtime / "data/n3_shadow", "entries", entry["position_id"])
        samples.append({"signal_id": "N3_BUY_" + str(entry["buy_event_seq"]),
            "source_reference": str(source), "source_hash": hashlib.sha256(source.read_bytes()).hexdigest(),
            "observed_at_utc": buy["at"], "metrics_ms": values, "missing_metrics": missing})
    return {"schema_version": 1, "phase": "PRE_ACTIVATION_CONTROL", "captured_at_utc": now,
        "runtime_root": str(runtime), "build_sha": manifest["build"]["git_sha"],
        "source_reference": str(runtime / "data/n3_shadow/manifest.json"),
        "source_digest": manifest["build"]["source_digest"],
        "control_fingerprint": core.digest(_paper_config(runtime)), "clock_semantics": "UTC_WALL_CLOCK",
        "samples": samples, "unavailable_metrics": {"quote_ms": "frozen N3 has no quote request start; do not infer from preflight"},
        "metric_semantics": {k: {"from": a, "to": b} for k, (a, b) in METRICS.items()},
        "economic_evaluation": False}


def policy(baseline: dict, *, metrics: list[str], minimum_samples: int, maximum_increase_ms: float, statistic: str) -> dict:
    """운영자가 지정한 값만 고정하며 허용 수치를 도구가 정하지 않는다."""
    import math
    if type(minimum_samples) is not int or minimum_samples < 1 or not math.isfinite(maximum_increase_ms) or maximum_increase_ms < 0:
        raise ValueError("explicit positive sample minimum and finite nonnegative tolerance required")
    if not metrics or len(set(metrics)) != len(metrics) or set(metrics) - set(batch.LATENCY_METRICS) or statistic not in {"median", "p95"}:
        raise ValueError("latency metric/statistic policy invalid")
    return {"schema_version": 1, "registered_at_utc": datetime.now(timezone.utc).isoformat(),
        "control_fingerprint": baseline["control_fingerprint"], "baseline_runtime_root": baseline["runtime_root"],
        "baseline_hash": core.digest(baseline), "metrics": {name: {"statistic": statistic,
            "minimum_samples": minimum_samples, "maximum_increase_ms": maximum_increase_ms} for name in metrics}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    gather = sub.add_parser("collect")
    gather.add_argument("--runtime", type=Path, required=True)
    gather.add_argument("--output", type=Path, required=True)
    register = sub.add_parser("policy")
    register.add_argument("--baseline", type=Path, required=True)
    register.add_argument("--metrics", nargs="+", choices=tuple(batch.LATENCY_METRICS), required=True)
    register.add_argument("--minimum-samples", type=int, required=True)
    register.add_argument("--maximum-increase-ms", type=float, required=True)
    register.add_argument("--statistic", choices=("median", "p95"), required=True)
    register.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.action == "collect":
            document = collect(args.runtime)
        else:
            document = policy(batch.read_evidence(args.baseline), metrics=args.metrics,
                minimum_samples=args.minimum_samples, maximum_increase_ms=args.maximum_increase_ms, statistic=args.statistic)
        batch.write_evidence(args.output, document, runtime_root=getattr(args, "runtime", None))
        print(json.dumps({"status": "RECORDED", "path": str(args.output.resolve()),
            "samples": len(document.get("samples", [])), "content_hash": core.digest(document), "economic_evaluation": False}))
        return 0
    except (ValueError, RuntimeError, OSError, KeyError) as exc:
        print(json.dumps({"status": "BLOCKED", "reason": str(exc)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
