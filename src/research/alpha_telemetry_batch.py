"""자연 runtime acceptance 이후의 Alpha 등록·분리 stream 동결 도구."""
from __future__ import annotations

import copy
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median

from scripts import entry_telemetry_acceptance as natural
from src.research import accelerated_alpha as core
from src.research import entry_telemetry_epoch as epoch, entry_telemetry_join as offline
from src.state_store import exclusive_file_lock, read_json

KST = timezone(timedelta(hours=9))
BATCH_ID = "ALPHA_BATCH_001"
MAX_STREAM_ROWS = 10000
MAX_TOTAL_BYTES = 64 * 1024 * 1024
LATENCY_METRICS = {
    "signal_to_analysis_ms": ("signal_detected_at", "analysis_started_at"),
    "analysis_to_preflight_ms": ("analysis_completed_at", "preflight_started_at"),
    "quote_ms": ("quote_buy_request_started_at", "quote_buy_received_at"),
    "preflight_to_paper_buy_ms": ("preflight_started_at", "paper_buy_created_at"),
    "total_decision_ms": ("signal_detected_at", "entry_decision_at"),
}
CONTRACT = {"completed_control_trades": 20, "active_kst_days": 7,
    "close_rule": "first_of_20th_completion_or_receipt_admitting_7th_distinct_KST_signal_date",
    "activity": "KST signal_detected_at date represented by admitted BUY receipt; includes entered open positions; rejects are not active days; close wall time and sequence come from admitting receipt",
    "membership": "BUY sequence >= start_seq and signal >= start_utc; predictors through close wall time; rejects lacking terminal timestamp use latest recorded entry clock and expose uncertainty",
    "late_outcomes": "open positions at cutoff retained as pending; later outcomes excluded from immutable batch"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bounded_document(path: Path) -> dict:
    if path.is_symlink() or path.stat().st_size > MAX_TOTAL_BYTES:
        raise ValueError("external evidence file byte bound/symlink")
    result = read_json(path, {})
    if not isinstance(result, dict):
        raise ValueError("evidence must be an object")
    return result


def read_evidence(path: Path | str) -> dict:
    """봉인한 latency 정책·baseline·acceptance만 입력으로 받는다."""
    document = _bounded_document(Path(path))
    body = {k: v for k, v in document.items() if k != "content_hash"}
    if document.get("content_hash") != core.digest(body):
        raise ValueError("evidence hash mismatch")
    return body


def write_evidence(path: Path | str, document: dict, *, runtime_root: Path | None = None) -> None:
    target = core._output(path)
    if runtime_root is not None:
        root = Path(runtime_root).resolve()
        if target == root or root in target.parents:
            raise ValueError("output must be outside runtime")
    with exclusive_file_lock(target, operation="alpha_telemetry_evidence"):
        core._write_immutable(target, document)


def load_streams(root: Path, epoch_id: str) -> dict:
    """연구 읽기도 전체 byte와 row 상한을 넘어 조용히 자르지 않는다."""
    streams, size = {}, 0
    for name in offline.SCHEMAS:
        rows = []
        for row in offline.iter_stream(root, epoch_id, name):
            size += len(offline._canonical(row))
            if len(rows) >= MAX_STREAM_ROWS or size > MAX_TOTAL_BYTES:
                raise ValueError("offline stream bound exceeded; partition research input")
            rows.append(row)
        streams[name] = rows
    return streams


def _stamp(timestamps: dict, name: str, receipt: dict | None = None) -> dict | None:
    value = (receipt or {}).get("execution_receipt", {}).get(name) if name == "paper_buy_created_at" else timestamps.get(name)
    return value if isinstance(value, dict) else None


def _elapsed(left: dict | None, right: dict | None, *, clock: str = "MONOTONIC") -> float | None:
    if left is None or right is None or left.get("wall_utc") is None or right.get("wall_utc") is None:
        return None
    start, end = core.utc(left.get("wall_utc")), core.utc(right.get("wall_utc"))
    if end < start:
        raise ValueError("latency timestamp order invalid")
    if clock == "UTC_WALL_CLOCK":
        return (end - start).total_seconds() * 1000
    if clock != "MONOTONIC":
        raise ValueError("unknown latency clock semantics")
    if left.get("monotonic_ns") is None or right.get("monotonic_ns") is None:
        return None
    if type(left["monotonic_ns"]) is not int or type(right["monotonic_ns"]) is not int:
        raise ValueError("latency monotonic clock type invalid")
    duration = (right["monotonic_ns"] - left["monotonic_ns"]) / 1_000_000
    if end < start or duration < 0:
        raise ValueError("latency timestamp order invalid")
    return duration


def compare_latency(baseline: dict, policy: dict, marker: dict, joined: dict) -> dict:
    """허용 증가량과 최소 표본은 activation 전 봉인한 정책만 사용한다."""
    if core.utc(policy.get("registered_at_utc")) > core.utc(marker["start_utc"]):
        raise ValueError("latency policy was not preregistered before activation")
    if policy.get("control_fingerprint") != marker["config_fingerprint"] or baseline.get("control_fingerprint") != marker["config_fingerprint"]:
        raise ValueError("latency Control fingerprint mismatch")
    if baseline.get("runtime_root") != policy.get("baseline_runtime_root") or not baseline.get("build_sha") or not baseline.get("source_reference"):
        raise ValueError("baseline runtime/build/source evidence missing")
    if baseline.get("phase") != "PRE_ACTIVATION_CONTROL" or core.utc(baseline.get("captured_at_utc")) > core.utc(marker["start_utc"]):
        raise ValueError("baseline must be captured before telemetry activation")
    samples = baseline.get("samples")
    clock = baseline.get("clock_semantics")
    if clock not in {"UTC_WALL_CLOCK", "MONOTONIC"}:
        raise ValueError("baseline latency clock semantics must be declared")
    rules = policy.get("metrics")
    if not isinstance(samples, list) or len(samples) > MAX_STREAM_ROWS or not isinstance(rules, dict) or not rules or set(rules) - set(LATENCY_METRICS):
        raise ValueError("latency samples/policy metric contract invalid")
    pre = {name: [] for name in rules}; post = {name: [] for name in rules}; identities = set()
    for sample in samples:
        identity = sample.get("signal_id")
        if not isinstance(identity, str) or not identity or identity in identities or not sample.get("source_reference"):
            raise ValueError("baseline identity duplicate/source missing")
        identities.add(identity)
        if core.utc(sample.get("observed_at_utc")) > core.utc(marker["start_utc"]):
            raise ValueError("baseline contains post-activation sample")
        for name, value in sample.get("metrics_ms", {}).items():
            if name in pre:
                if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                    raise ValueError("invalid baseline latency")
                pre[name].append(value)
    for item in joined["rows"]:
        predictor, receipt = item["predictor"], item["receipt"]
        if predictor["decision"]["outcome"] != "BUY" or receipt is None:
            continue
        timestamps = predictor["predictors"]["timestamps"]
        for name in rules:
            a, b = LATENCY_METRICS[name]
            value = _elapsed(_stamp(timestamps, a, receipt), _stamp(timestamps, b, receipt), clock=clock)
            if value is not None:
                post[name].append(value)
    result = {}
    for name, rule in rules.items():
        minimum, statistic = rule.get("minimum_samples"), rule.get("statistic")
        if type(minimum) is not int or minimum < 1 or statistic not in {"median", "p95"}:
            raise ValueError("latency minimum/statistic requires preregistration")
        tolerance = rule.get("maximum_increase_ms")
        if type(tolerance) not in (int, float) or not math.isfinite(tolerance) or tolerance < 0:
            raise ValueError("latency tolerance must be explicit finite milliseconds")
        if len(pre[name]) < minimum or len(post[name]) < minimum:
            result[name] = {"status": "BLOCKED", "reason": "missing real baseline/post sample coverage", "pre_count": len(pre[name]), "post_count": len(post[name])}
            continue
        def summarize(values):
            return median(values) if statistic == "median" else sorted(values)[math.ceil(len(values) * .95) - 1]
        before, after = summarize(pre[name]), summarize(post[name])
        result[name] = {"status": "PASS" if after - before <= tolerance else "DEGRADED",
            "pre_ms": before, "post_ms": after, "increase_ms": after - before,
            "maximum_increase_ms": tolerance, "statistic": statistic,
            "pre_count": len(pre[name]), "post_count": len(post[name])}
    status = "BLOCKED" if any(v["status"] == "BLOCKED" for v in result.values()) else "DEGRADED" if any(v["status"] == "DEGRADED" for v in result.values()) else "PASS"
    return {"status": status, "metrics": result, "policy_hash": core.digest(policy), "baseline_hash": core.digest(baseline), "clock_semantics": clock, "unavailable_metrics": sorted(set(LATENCY_METRICS) - set(rules))}


def acceptance_report(root: Path | str, *, baseline: dict | None, policy: dict | None, config: dict) -> dict:
    """기존 natural acceptance에 명시적 BUY predictor와 실제 latency 검증을 추가한다."""
    root = Path(root).resolve()
    result = {"schema_version": 1, "checked_utc": _now(), "runtime_root": str(root), "status": "TELEMETRY_RUNTIME_BLOCKED"}
    try:
        marker = epoch.require_epoch(root, config=config)
        streams = load_streams(root, marker["telemetry_epoch_id"])
        joined = offline.join_streams(*(streams[name] for name in offline.SCHEMAS))
        report = natural.inspect(root, config=config)
        if report.get("stream_counts") is not None and report["stream_counts"] != {name: len(rows) for name, rows in streams.items()}:
            raise ValueError("acceptance stream snapshot changed during inspection; retry")
        buys = [row for row in streams["predictors"] if row["decision_outcome"] == "BUY"]
        report["cases"]["first_buy_predictor"] = {"status": "PASS", "identity": min(buys, key=lambda r: core.utc(r["identity"]["signal_detected_at"]))["identity"]} if buys else {"status": "PENDING", "reason": "natural BUY not recorded"}
        for row in streams["predictors"]:
            project_features(row)
        result.update(epoch=marker, natural_acceptance=report,
            physical_streams={name: {"directory": str(epoch.directory(root) / "epochs" / marker["telemetry_epoch_id"] / name), "count": len(rows), "stream_hash": core.digest(rows)} for name, rows in streams.items()})
        if baseline is None or policy is None:
            result["reason"] = "real pre-activation latency baseline and preregistered tolerance required"
            return result
        latency = compare_latency(baseline, policy, marker, joined)
        result["latency"] = latency
        if report["status"] != "PASS" or not buys or latency["status"] == "BLOCKED":
            result["reason"] = "natural runtime cases or real latency coverage pending"
        elif latency["status"] == "DEGRADED":
            result.update(status="TELEMETRY_RUNTIME_DEGRADED", reason="Control latency regression exceeds preregistered tolerance")
        else:
            ledger = _bounded_document(root / "data/paper_trades.json")
            seq = ledger.get("next_event_seq")
            if type(seq) is not int or seq < marker["start_event_seq"]:
                raise ValueError("Control next event sequence invalid")
            result.update(status="TELEMETRY_RUNTIME_ACCEPTED", control_next_event_seq=seq)
    except (ValueError, RuntimeError, KeyError, TypeError, OSError) as error:
        reason = str(error)
        degraded = any(term in reason.lower() for term in ("degraded", "drop/error", "health stale", "queue bound"))
        result.update(status="TELEMETRY_RUNTIME_DEGRADED" if degraded else "TELEMETRY_RUNTIME_BLOCKED", reason=reason, category=type(error).__name__)
    return result


def register_batch(output: Path | str, acceptance: dict, *, start_seq: int, start_utc: str) -> dict:
    """Acceptance 후 경계를 불변 등록한다. 재시도는 동일 문서에만 허용한다."""
    path = core._output(output)
    if acceptance.get("status") != "TELEMETRY_RUNTIME_ACCEPTED" or acceptance.get("latency", {}).get("status") != "PASS":
        raise ValueError("runtime acceptance required before Alpha registration")
    if type(start_seq) is not int or start_seq < acceptance["control_next_event_seq"] or core.utc(start_utc) < core.utc(acceptance["checked_utc"]):
        raise ValueError("batch boundary must follow acceptance")
    runtime = Path(acceptance["runtime_root"]).resolve()
    if runtime == path or runtime in path.parents:
        raise ValueError("batch output must be outside runtime")
    marker = acceptance["epoch"]
    document = {"schema_version": 1, "batch_id": BATCH_ID, "status": "REGISTERED",
        "start_event_seq": start_seq, "start_utc": start_utc, "start_kst": core.utc(start_utc).astimezone(KST).isoformat(),
        "telemetry_epoch_id": marker["telemetry_epoch_id"], "build_sha": marker["build_sha"],
        "telemetry_schemas": epoch.SCHEMAS, "control_fingerprint": marker["config_fingerprint"],
        "runtime_root": str(runtime), "acceptance_hash": core.digest(acceptance), "sample_contract": CONTRACT}
    with exclusive_file_lock(path / "alpha_registration", operation="alpha_batch_register"):
        core._write_immutable(path / BATCH_ID / "registration.json", document)
        core._write_immutable(path / BATCH_ID / "acceptance.json", acceptance)
    return document


def project_features(predictor: dict) -> dict:
    """기존 허용 schema의 decision 봉인 당시 scalar만 명시적 source와 투영한다."""
    natural.validate_predictor(predictor)
    payload = predictor["predictors"]
    decision_stamp = payload["timestamps"].get("entry_decision_at")
    signal = core.utc(predictor["identity"]["signal_detected_at"])
    route = {"route_type": {"value": predictor["identity"].get("route_type"),
        "observed_at": signal.isoformat(), "source": "predictor_v2.identity.route_type"}}
    if not isinstance(decision_stamp, dict) or decision_stamp.get("wall_utc") is None:
        if predictor["decision_outcome"] == "BUY":
            raise ValueError("BUY entry decision timestamp missing")
        # 거절 후보의 마지막 decision clock은 기존 schema에 없을 수 있다.
        # 전체 raw row는 보존하지만 field 획득 시각을 만들어 투영하지 않는다.
        return route
    decision = core.utc(decision_stamp["wall_utc"])
    if signal > decision:
        raise ValueError("signal after decision")
    for name, stamp in payload["timestamps"].items():
        if stamp is not None and stamp.get("wall_utc") is not None and core.utc(stamp["wall_utc"]) > decision:
            raise ValueError("future predictor timestamp after decision")
    sections = payload["sections"]
    mapping = {
        "liquidity_usd": ("trajectory", "liquidity_now"),
        "pair_age_seconds": ("ages", "pair_age_seconds"),
        "signal_age_seconds": ("ages", "signal_age_sec"),
        "whale_paid_sol": ("signal", "sol_amount"),
        "unique_wallet_count": ("wallets", "unique_wallet_count"),
        "wallet_concentration_ratio": ("wallets", "concentration_ratio"),
        "concurrent_signals": ("pressure", "pending_signal_tasks"),
        "open_positions": ("pressure", "open_positions_count"),
        "market_activity_count": ("pressure", "recent_signal_count"),
        "entry_price_impact_pct": ("quote_buy", "price_impact_pct"),
        "quote_age_ms": ("quote_buy", "quote_age_ms"),
        "flow_bid_count": ("short_flow", "buy_count"),
        "flow_ask_count": ("short_flow", "sell_count"),
        "flow_combined_volume_usd": ("short_flow", "combined_volume_usd"),
    }
    features = route
    def put(name, value, source):
        if value is None or not isinstance(value, (str, bool, int, float)):
            return
        core._predictor_name(name)
        if type(value) in (int, float) and not math.isfinite(value):
            raise ValueError("nonfinite entry predictor")
        features[name] = {"value": value, "observed_at": decision.isoformat(),
            "source": "predictor_v2.sealed_entry_snapshot." + source}
    for name, (section, key) in mapping.items():
        put(name, (sections.get(section) or {}).get(key), "sections." + section + "." + key)
    for group in ("counters", "durations"):
        for name, value in payload[group].items():
            # 사전 청산견적도 진입 시점 근거지만 이름이 사후 feature로 오인되지 않게 한다.
            safe_name = name.replace("exit_preflight", "liquidation_precheck")
            put(safe_name, value, group + "." + name)
    for section in ("scores", "safety_components"):
        def visit(value, path):
            if not isinstance(value, dict):
                return
            for name, item in value.items():
                if name in {"missing_reason", "uncapped_total_missing_reason"}:
                    continue
                next_path = path + [name]
                if isinstance(item, dict):
                    visit(item, next_path)
                else:
                    put("_".join(next_path), item, "sections." + ".".join(next_path))
        visit(sections.get(section), [section])
    elapsed = _elapsed(payload["timestamps"].get("signal_detected_at"), decision_stamp)
    put("entry_latency_ms", elapsed, "timestamps.signal_detected_at_to_entry_decision_at.monotonic_ns")
    if len(features) > 128:
        raise ValueError("canonical feature bound exceeded")
    return features


def _receipt_time(receipt: dict) -> datetime:
    return core.utc(receipt["execution_receipt"]["paper_buy_created_at"]["wall_utc"])


def build_dataset(streams: dict, registration: dict, as_of: str) -> dict:
    """컷오프까지의 세 stream은 분리 보존하고 완료 행만 core 계약으로 연결한다."""
    start, end = core.utc(registration["start_utc"]), core.utc(as_of)
    if end < start:
        raise ValueError("as_of before batch start")
    joined = offline.join_streams(*(streams[name] for name in offline.SCHEMAS))
    if joined["orphan_receipts"] or joined["orphan_outcomes"]:
        raise ValueError("orphan telemetry blocks freeze")
    buy_sequences = [row["identity"].get("event_seq") for row in streams["receipts"]]
    if any(type(seq) is not int or seq < 1 for seq in buy_sequences) or len(set(buy_sequences)) != len(buy_sequences):
        raise ValueError("duplicate or invalid BUY sequence")
    members = []
    for item in joined["rows"]:
        predictor = item["predictor"]
        if any(predictor.get(k) != registration[k] for k in ("telemetry_epoch_id", "build_sha")) or predictor["provenance"]["config_fingerprint"] != registration["control_fingerprint"]:
            raise ValueError("batch epoch/build/Control fingerprint mismatch")
        features = project_features(predictor)
        signal = core.utc(predictor["identity"]["signal_detected_at"])
        timestamps = predictor["predictors"]["timestamps"]
        entry_clock = timestamps.get("entry_decision_at")
        has_decision_clock = isinstance(entry_clock, dict) and entry_clock.get("wall_utc") is not None
        stamp = core.utc(entry_clock["wall_utc"]) if has_decision_clock else max([signal] + [core.utc(clock["wall_utc"])
            for clock in timestamps.values() if isinstance(clock, dict) and clock.get("wall_utc") is not None])
        if signal < start or stamp > end:
            continue
        receipt, outcome = item["receipt"], item["outcome"]
        if receipt is not None:
            seq = receipt["identity"].get("event_seq")
            if type(seq) is not int or seq < registration["start_event_seq"]:
                continue
            if _receipt_time(receipt) < stamp:
                raise ValueError("BUY receipt before entry decision")
            if outcome is not None and core.utc(outcome["outcome"]["completed_at"]) < _receipt_time(receipt):
                raise ValueError("outcome completion before BUY receipt")
            if _receipt_time(receipt) > end:
                receipt, outcome = None, None
        if outcome is not None and core.utc(outcome["outcome"]["completed_at"]) > end:
            outcome = None
        members.append({"predictor": predictor, "receipt": receipt, "outcome": outcome, "features": features, "decision_at": stamp,
            "has_decision_clock": has_decision_clock})
    completed = sorted((r for r in members if r["outcome"] is not None), key=lambda r: (core.utc(r["outcome"]["outcome"]["completed_at"]), r["outcome"]["identity"]["event_seq"]))
    active = {}
    for item in members:
        if item["receipt"] is not None:
            receipt_at = _receipt_time(item["receipt"])
            day = core.utc(item["predictor"]["identity"]["signal_detected_at"]).astimezone(KST).date().isoformat()
            admission = (receipt_at, item["receipt"]["identity"]["event_seq"])
            active[day] = min(active.get(day, admission), admission)
    cutoffs = []
    if len(completed) >= 20:
        last = completed[19]["outcome"]
        cutoffs.append((core.utc(last["outcome"]["completed_at"]), last["identity"]["event_seq"], "20_COMPLETED"))
    if len(active) >= 7:
        active_time, active_seq = sorted(active.values())[6]
        cutoffs.append((active_time, active_seq, "7_ACTIVE_KST_DAYS"))
    if not cutoffs:
        return {"status": "PENDING", "completed_control_trades": len(completed), "active_kst_days": len(active), "sample_contract": CONTRACT}
    cutoff, cutoff_seq, reason = min(cutoffs)
    raw = {name: [] for name in offline.SCHEMAS}; canonical, summary = [], []
    for item in sorted(members, key=lambda r: (r["decision_at"], r["predictor"]["identity"]["signal_id"])):
        if item["decision_at"] > cutoff:
            continue
        predictor, receipt, outcome = item["predictor"], item["receipt"], item["outcome"]
        if receipt is not None and receipt["identity"]["event_seq"] > cutoff_seq:
            continue
        raw["predictors"].append(copy.deepcopy(predictor))
        if receipt is not None and _receipt_time(receipt) <= cutoff:
            raw["receipts"].append(copy.deepcopy(receipt))
        else:
            receipt, outcome = None, None
        if outcome is not None and core.utc(outcome["outcome"]["completed_at"]) <= cutoff and outcome["identity"]["event_seq"] <= cutoff_seq:
            raw["outcomes"].append(copy.deepcopy(outcome))
            body, identity = outcome["outcome"], outcome["identity"]
            if body.get("sell_legs_truncated") or body.get("sell_legs_complete") is not True:
                raise ValueError("incomplete SELL legs block dataset freeze")
            if any(type(body.get(key)) is not int for key in ("entry_cost_lamports", "cumulative_proceeds_lamports", "realized_pnl_lamports")) or body["entry_cost_lamports"] <= 0 or body["cumulative_proceeds_lamports"] < 0:
                raise ValueError("outcome cost type/range invalid")
            if body.get("cumulative_proceeds_lamports") - body.get("entry_cost_lamports") != body.get("realized_pnl_lamports"):
                raise ValueError("outcome cost basis invariant mismatch")
            legs = body.get("sell_legs")
            if not isinstance(legs, list) or not legs or len({leg.get("event_seq") for leg in legs}) != len(legs):
                raise ValueError("SELL legs absent/duplicate")
            if any(type(leg.get("event_seq")) is not int or not identity["buy_event_seq"] < leg["event_seq"] <= identity["event_seq"]
                or type(leg.get("proceeds_lamports")) is not int or leg["proceeds_lamports"] < 0
                or type(leg.get("realized_pnl_lamports")) is not int
                or not _receipt_time(receipt) <= core.utc(leg.get("at")) <= core.utc(body["completed_at"]) for leg in legs):
                raise ValueError("SELL leg sequence/time/cost invalid")
            final_leg = max(legs, key=lambda leg: leg["event_seq"])
            if final_leg["event_seq"] != identity["event_seq"] or final_leg["at"] != body["completed_at"] or final_leg["reason"] != body["exit_reason"]:
                raise ValueError("completed SELL linkage mismatch")
            if sum(leg["proceeds_lamports"] for leg in legs) != body["cumulative_proceeds_lamports"] or sum(leg["realized_pnl_lamports"] for leg in legs) != body["realized_pnl_lamports"]:
                raise ValueError("SELL leg totals mismatch")
            canonical.append({"position_id": identity["trade_id"], "buy_event_seq": identity["buy_event_seq"],
                "completion_event_seq": identity["event_seq"], "signal_timestamp": predictor["identity"]["signal_detected_at"],
                "entry_decision_timestamp": item["decision_at"].isoformat(), "closed_at": body["completed_at"],
                "entry_cost_lamports": body["entry_cost_lamports"], "realized_pnl_lamports": body["realized_pnl_lamports"],
                "route_type": identity["route_type"], "features": item["features"],
                "forensic": {"exit_reason": body["exit_reason"], "sell_legs": copy.deepcopy(legs),
                    "trajectory_missing_reason": "NO_POST_ENTRY_TRAJECTORY_IN_OUTCOME_SCHEMA_V1"}})
        else:
            outcome = None
        summary.append({"signal_id": predictor["identity"]["signal_id"], "decision_outcome": predictor["decision_outcome"],
            "trade_id": receipt["identity"]["trade_id"] if receipt else None,
            "decision_timestamp_available": item["has_decision_clock"],
            "decision_timestamp_missing_reason": None if item["has_decision_clock"] else "TERMINAL_DECISION_CLOCK_NOT_RECORDED_LATEST_ENTRY_CLOCK_USED_FOR_RAW_MEMBERSHIP",
            "join_status": "COMPLETED" if outcome else "BUY_RECEIPT" if receipt else "MISSING_RECEIPT" if predictor["decision_outcome"] == "BUY" else "PREDICTOR_ONLY"})
    if any(row["join_status"] == "MISSING_RECEIPT" for row in summary):
        raise ValueError("BUY receipt pending at cutoff; retry freeze after natural flush")
    canonical = core.validate_rows(canonical)
    return {"schema_version": 1, "batch_id": BATCH_ID, "status": "FROZEN", "registration": registration,
        "close_reason": reason, "end_utc": cutoff.isoformat(), "end_kst": cutoff.astimezone(KST).isoformat(),
        "end_event_seq": max([r["identity"]["event_seq"] for r in raw["receipts"] + raw["outcomes"]], default=registration["start_event_seq"] - 1),
        "completed_control_trades": len(canonical), "active_kst_days": len({core.utc(r["identity"]["signal_detected_at"]).astimezone(KST).date().isoformat() for r in raw["receipts"]}),
        "rows": canonical, "rows_hash": core.digest(canonical), "offline_join": summary,
        "raw_streams": raw, "raw_stream_hashes": {k: core.digest(v) for k, v in raw.items()},
        "projection_contract": "only predictor schema v2 sealed decision scalars; observed_at denotes snapshot sealing, not field acquisition",
        "sample_contract": CONTRACT}


def freeze_batch(root: Path | str, output: Path | str, *, as_of: str) -> dict:
    """실제 stream만 읽는다. pending에는 데이터셋 파일을 만들지 않는다."""
    root, output = Path(root).resolve(), core._output(output)
    if output == root or root in output.parents:
        raise ValueError("batch output must be outside runtime")
    directory = output / BATCH_ID
    with exclusive_file_lock(output / "alpha_registration", operation="alpha_batch_freeze"):
        registration = read_evidence(directory / "registration.json")
        acceptance = read_evidence(directory / "acceptance.json")
        if core.digest(acceptance) != registration["acceptance_hash"] or acceptance.get("status") != "TELEMETRY_RUNTIME_ACCEPTED":
            raise ValueError("registered acceptance evidence mismatch")
        if registration["runtime_root"] != str(root):
            raise ValueError("registration runtime root mismatch")
        if (directory / "dataset.json").exists():
            return read_dataset(output)
        streams = load_streams(root, registration["telemetry_epoch_id"])
        result = build_dataset(streams, registration, as_of)
        if result["status"] == "FROZEN":
            # 원본 stream은 별도 물리 파일로 남기고 dataset에는 연결 결과만 둔다.
            for name, rows in result.pop("raw_streams").items():
                core._write_immutable(directory / "streams" / (name + ".json"), {"stream": name, "rows": rows, "rows_hash": core.digest(rows)})
            core._write_immutable(directory / "dataset.json", result)
        return result


def read_dataset(output: Path | str) -> dict:
    """기존 동결도 세 물리 stream의 봉인과 canonical 행을 함께 확인한다."""
    directory = Path(output).resolve() / BATCH_ID
    result = read_evidence(directory / "dataset.json")
    registration = read_evidence(directory / "registration.json")
    if result.get("registration") != registration or result.get("batch_id") != BATCH_ID or result.get("status") != "FROZEN":
        raise ValueError("frozen batch registration mismatch")
    rows = core.validate_rows(result.get("rows"))
    if rows != result["rows"] or core.digest(rows) != result.get("rows_hash"):
        raise ValueError("frozen canonical row integrity mismatch")
    for name in offline.SCHEMAS:
        stream = read_evidence(directory / "streams" / (name + ".json"))
        if stream.get("stream") != name or core.digest(stream.get("rows")) != stream.get("rows_hash") or stream["rows_hash"] != result["raw_stream_hashes"][name]:
            raise ValueError("frozen physical stream integrity mismatch")
        for row in stream["rows"]:
            offline._validate(row, name)
    return result
