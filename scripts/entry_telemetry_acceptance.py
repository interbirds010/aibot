"""활성화 후 자연 발생한 다섯 acceptance를 읽기 전용으로 검사한다."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.research import entry_telemetry as t, entry_telemetry_epoch as epoch
from src.research import entry_telemetry_join as offline
from src.research.n3_shadow import read_immutable
from src.state_store import read_json, atomic_write_json, exclusive_file_lock


def validate_predictor(row):
    """봉인 검증과 별도로 feature 영역의 실제 허용 구조를 검사한다."""
    if "execution_receipt" in row or "outcome" in row:
        raise ValueError("predictor contains execution payload")
    allowed = {"schema_version", "predictor_schema_version", "telemetry_epoch_id", "build_sha", "session_id",
        "provenance", "identity", "decision", "decision_outcome", "predictors", "collection_limits", "content_hash"}
    if set(row) - allowed:
        raise ValueError("unknown predictor envelope field")
    identity = row["identity"]
    if set(identity) - (set(t.IDENTITY_FIELDS) | {"trade_id", "event_seq"}) or \
            any(isinstance(v, (dict, list, tuple)) for v in identity.values()):
        raise ValueError("predictor identity allowlist violation")
    if identity.get("trade_id") is not None or identity.get("event_seq") is not None:
        raise ValueError("post-decision identity in predictor")
    features = row["predictors"]
    if set(features) != {"timestamps", "timestamp_missing_reason", "sections", "counters", "durations"}:
        raise ValueError("predictor feature envelope allowlist violation")
    decision = row["decision"]
    if decision.get("outcome") not in t.OUTCOMES or row.get("decision_outcome") != decision.get("outcome") or \
            t.predictor_schema.project(decision, t.SECTION_SHAPES["decision"], t._safe) != decision:
        raise ValueError("decision classification mismatch")
    for name, value in features["sections"].items():
        if name == "wallet_performance":
            if value != {"entry_time_snapshot": None, "missing_reason": "NO_VERIFIED_ENTRY_TIME_SNAPSHOT_SOURCE"}:
                raise ValueError("unverified wallet snapshot")
        elif name not in t.SECTION_SHAPES or name == "decision" or \
                t.predictor_schema.project(value, t.SECTION_SHAPES[name], t._safe) != value:
            raise ValueError("predictor nested allowlist violation")
    if set(features["timestamps"]) - (set(t.TIMESTAMPS) | {"signal_detected_at"}):
        raise ValueError("unknown predictor timestamp")
    if any(t._timestamp(v) != v for v in features["timestamps"].values()):
        raise ValueError("timestamp nested allowlist violation")
    if set(features["counters"]) - set(t.COUNTERS) or set(features["durations"]) - set(t.DURATIONS):
        raise ValueError("unknown predictor counter/duration")
    for values in (features["counters"], features["durations"]):
        if any(v is not None and (not isinstance(v, (int, float)) or isinstance(v, bool) or v < 0) for v in values.values()):
            raise ValueError("counter/duration scalar violation")
    provenance = row["provenance"]
    if any(isinstance(v, (dict, list, tuple)) for k, v in provenance.items() if k != "safe_config") or \
            set(provenance.get("safe_config", {})) - t.CONFIG_FIELDS or \
            any(isinstance(v, (dict, list, tuple)) for v in provenance.get("safe_config", {}).values()):
        raise ValueError("provenance scalar/config allowlist violation")
    signal_at = datetime.fromisoformat(identity["signal_detected_at"]).timestamp()
    for key in ("history", "pre_signal_snapshots"):
        for snapshot in features["sections"]["trajectory"].get(key) or []:
            if not isinstance(snapshot.get("snapshot_at_epoch"), (int, float)) or snapshot["snapshot_at_epoch"] > signal_at:
                raise ValueError("future trajectory snapshot")


def inspect(root):
    marker = epoch.require_epoch(root, config=epoch.safe_runtime_config())
    epoch_id = marker["telemetry_epoch_id"]
    directory = epoch.directory(root) / "epochs" / epoch_id
    streams = {name: offline.load_stream(root, epoch_id, name) for name in offline.SCHEMAS}
    joined = offline.join_streams(*(streams[name] for name in offline.SCHEMAS))
    ledger = read_json(root / "data/paper_trades.json", {})
    events = ledger.get("events", [])
    if not isinstance(events, list) or len(events) > 100000:
        raise ValueError("acceptance ledger scan bound/type")
    ledger_events = {e.get("event_seq"): e for e in events if isinstance(e, dict)}
    for name, rows in streams.items():
        for row in rows:
            session = row.get("session_id")
            if not isinstance(session, str):
                raise ValueError("runtime session missing")
            binding = read_immutable(directory / "sessions" / (hashlib.sha256(session.encode()).hexdigest() + ".json"))
            if not binding or any(binding.get(key) != row.get(key) for key in ("telemetry_epoch_id", "build_sha", "session_id")):
                raise ValueError("runtime epoch/session association mismatch")
            if row["build_sha"] != marker["build_sha"] or row["provenance"].get("config_fingerprint") != marker["config_fingerprint"]:
                raise ValueError("runtime build/config mismatch")
            signal_at = datetime.fromisoformat(row["identity"]["signal_detected_at"])
            if signal_at.tzinfo is None or signal_at < datetime.fromisoformat(marker["start_utc"]):
                raise ValueError("runtime signal outside epoch")
            if name == "predictors":
                validate_predictor(row)
            else:
                identity = row["identity"]
                buy_seq = identity.get("event_seq" if name == "receipts" else "buy_event_seq")
                buy = ledger_events.get(buy_seq, {})
                if not isinstance(buy_seq, int) or isinstance(buy_seq, bool) or buy_seq < marker["start_event_seq"] or \
                        buy.get("type") != "BUY" or buy.get("position_id") != identity["trade_id"] or buy.get("mint") != identity["mint"]:
                    raise ValueError("receipt/outcome Control BUY identity mismatch")
                if name == "outcomes":
                    sell = ledger_events.get(identity.get("event_seq"), {})
                    trade_sells = [e for e in events if e.get("type") == "SELL" and e.get("position_id") == identity["trade_id"]]
                    actual_last = max(trade_sells, key=lambda e: e["event_seq"], default={})
                    outcome = row["outcome"]
                    if sell.get("type") != "SELL" or sell.get("position_id") != identity["trade_id"] or sell.get("mint") != identity["mint"] or \
                            actual_last.get("event_seq") != identity.get("event_seq") or \
                            outcome.get("completed_at") != sell.get("at") or outcome.get("exit_reason") != sell.get("reason") or \
                            any(p.get("position_id") == identity["trade_id"] for p in ledger.get("positions", {}).values()):
                        raise ValueError("completed outcome Control SELL identity mismatch")
                    for leg in outcome.get("sell_legs", []):
                        actual = ledger_events.get(leg.get("event_seq"), {})
                        if actual.get("type") != "SELL" or actual.get("position_id") != identity["trade_id"] or \
                                any(leg.get(k) != actual.get(k) for k in t.SELL_FIELDS):
                            raise ValueError("completed outcome SELL leg mismatch")
    def first(name, predicate=lambda row: True):
        eligible = [row for row in streams[name] if predicate(row)]
        if not eligible:
            return {"status": "PENDING", "reason": "natural event not recorded yet"}
        row = min(eligible, key=lambda item: t._record_path(name, item).stat().st_mtime_ns)
        return {"status": "PASS", "identity": row["identity"], "schema_version": row["schema_version"],
                "path": str(t._record_path(name, row))}
    # File paths are evaluated against this inspected epoch, not module default runtime.
    previous_root, previous_epoch = t._root, t._epoch
    try:
        t._root, t._epoch = root, marker
        cases = {"first_predictor": first("predictors"),
            "first_rejected_predictor": first("predictors", lambda r: r["decision_outcome"] in {"REJECT_ANALYZER", "REJECT_RISK", "QUOTE_FAILED"}),
            "first_rpc_skipped_predictor": first("predictors", lambda r: r["decision_outcome"] == "RPC_SKIPPED"),
            "first_buy_receipt": first("receipts"), "first_completed_outcome": first("outcomes")}
    finally:
        t._root, t._epoch = previous_root, previous_epoch
    health = {}
    for role in ("monitor", "risk-manager"):
        document = read_json(directory / "health" / (role + ".json"), {})
        if not document:
            raise ValueError("recorder role health missing")
        heartbeat = datetime.fromisoformat(document["heartbeat_utc"]).timestamp()
        if time.time() - heartbeat > 90 or document.get("telemetry_epoch_id") != epoch_id:
            raise ValueError("recorder role health stale/epoch mismatch")
        if document.get("status") != "TELEMETRY_READY" or any(document.get(key, 0) for key in
                ("dropped_row_count", "write_error_count", "conflict_count", "duplicate_count", "degraded_event_count")):
            raise ValueError("recorder role degraded/drop/error/duplicate")
        if any(q["depth"] > q["capacity"] or q["capacity"] != 16 for q in document["stream_queues"].values()):
            raise ValueError("recorder queue bound violated")
        health[role] = document
    if joined["orphan_receipts"] or joined["orphan_outcomes"] or any(r["join_status"] == "MISSING_RECEIPT" for r in joined["rows"]):
        # First predictor may precede receipt flush. Retry later; never fabricate a join.
        join_status = "PENDING concurrent flush or missing counterpart; inspect dropped counters"
    else:
        join_status = "PASS"
    return {"status": "PASS" if all(c["status"] == "PASS" for c in cases.values()) and join_status == "PASS" else "PENDING",
            "checked_utc": datetime.now(timezone.utc).isoformat(), "telemetry_epoch_id": epoch_id,
            "cases": cases, "offline_join": join_status, "stream_counts": {k: len(v) for k, v in streams.items()},
            "health": health, "scope": "schema/runtime recording only; no performance/Alpha evaluation"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        result = inspect(args.root.resolve())
    except Exception as error:
        result = {"status": "FAIL", "reason": str(error), "category": type(error).__name__}
    if args.output:
        with exclusive_file_lock(args.output, operation="entry_telemetry_acceptance"):
            atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False))
    raise SystemExit(1 if result["status"] == "FAIL" else 0)


if __name__ == "__main__":
    main()
