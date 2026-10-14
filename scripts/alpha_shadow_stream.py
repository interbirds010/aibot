"""실제 BUY predictor와 receipt identity만 외부 challenger recorder에 공급한다."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.research import accelerated_alpha as core
from src.research import alpha_telemetry_batch as batch
from src.research import entry_telemetry_epoch as epoch, entry_telemetry_join as offline
from scripts.entry_telemetry_cutover import _paper_config


def project(predictor: dict, receipt: dict) -> dict:
    """receipt의 BUY 식별자 외에는 decision 당시 봉인 predictor만 사용한다."""
    joined = offline.join_streams([predictor], [receipt], [])
    if joined["rows"][0]["join_status"] != "BUY_RECEIPT":
        raise ValueError("matching Control BUY predictor/receipt required")
    features = batch.project_features(predictor)
    return {"control_predictor_id": predictor["identity"]["signal_id"],
        "control_event_seq": receipt["identity"]["event_seq"], "control_decision": "BUY",
        "signal_timestamp": predictor["identity"]["signal_detected_at"],
        "entry_decision_timestamp": predictor["predictors"]["timestamps"]["entry_decision_at"]["wall_utc"],
        "features": features, "epoch_id": predictor["telemetry_epoch_id"], "build_sha": predictor["build_sha"],
        "strategy_fingerprint": predictor["provenance"]["config_fingerprint"]}


def consume(root: Path, output: Path, *, config: dict) -> dict:
    root, output = root.resolve(), core._output(output)
    if root == output or root in output.parents:
        raise ValueError("shadow output must be outside Control runtime")
    marker = epoch.require_epoch(root, config=config)
    streams = {}
    for name in ("predictors", "receipts"):
        values, size = [], 0
        for row in offline.iter_stream(root, marker["telemetry_epoch_id"], name):
            if name == "predictors" and row.get("decision_outcome") != "BUY":
                continue
            size += len(offline._canonical(row))
            if len(values) >= 2000 or size > batch.MAX_TOTAL_BYTES:
                raise ValueError("partition shadow input to at most 2000 rows per stream")
            if row["build_sha"] != marker["build_sha"] or row["provenance"]["config_fingerprint"] != marker["config_fingerprint"]:
                raise ValueError("shadow runtime build/config mismatch")
            values.append(row)
        streams[name] = values
    joined = offline.join_streams(streams["predictors"], streams["receipts"], [])
    now = datetime.now(timezone.utc).isoformat()
    count = 0
    for item in joined["rows"]:
        if item["join_status"] == "BUY_RECEIPT":
            core.record_shadow_predictor(output, project(item["predictor"], item["receipt"]), now)
            count += 1
    return {"status": "SHADOW_RECORDED", "buy_predictors_consumed": count,
        "pending_buy_receipts": sum(item["join_status"] == "MISSING_RECEIPT" for item in joined["rows"]),
        "orphan_receipts": len(joined["orphan_receipts"]), "outcome_stream_read": False,
        "control_changes": False, "mode": "OFFLINE_PREREGISTERED_ENTRY_PREDICTOR_CONSUMER"}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--research-root", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = consume(args.root, args.research_root, config=_paper_config(args.root)) if args.execute else {
            "mode": "PREVIEW", "writes": False, "control_changes": False}
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    except (ValueError, RuntimeError, KeyError, OSError, TypeError) as exc:
        print(json.dumps({"status": "BLOCKED", "reason": str(exc)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
