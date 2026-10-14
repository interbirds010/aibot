"""운영 원장을 변경하지 않는 불변 batch와 미래 shadow 후보 연구 도구."""
from __future__ import annotations

import copy
import hashlib
import json
import math
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.state_store import atomic_write_json, exclusive_file_lock, read_json

ROOT = Path(__file__).resolve().parents[2]
ORIGINAL_CHECKOUT = Path.home() / "Documents" / "solana-ai-bot"
FROZEN_RUNTIME = Path.home() / "Documents" / "aibot-local-paper"
STAGE_CONTRACT = {"stage1_completed": 20, "stage2_additional_completed": 30,
                  "stage3": "pre_registered_longer_validation_and_human_decision"}
TERMINAL = {"REJECT_EARLY", "INSUFFICIENT_COVERAGE", "RETIRED"}
STATUSES = {"DISCOVERY_ONLY", "FRESH_DIRECTION_CONFIRMED", "PROMISING_CHALLENGER",
            "ROBUST_PAPER_ALPHA", "ONLY_NEGATIVE_FILTER"} | TERMINAL
OUTCOME_NAMES = {"realized_pnl_lamports", "pnl", "return", "return_percent", "winner",
                 "loser", "mae", "mfe", "closed_at", "holding_duration", "exit_price",
                 "false_momentum", "label", "outcome", "forensic"}


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def utc(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("UTC timestamp must be a string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("invalid UTC timestamp") from exc
    if parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamp must explicitly use UTC")
    return parsed


def _positive_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _scalar(value: object) -> bool:
    return value is None or isinstance(value, (str, bool)) or (
        isinstance(value, (int, float)) and math.isfinite(value))


def _predictor_name(value: object) -> None:
    tokens = re.split(r"[^a-z0-9]+", value.lower()) if isinstance(value, str) else []
    outcome_tokens = {"pnl", "mae", "mfe", "holding", "trajectory", "closed", "exit", "outcome",
                      "forensic", "label", "winner", "loser", "realized"}
    if not isinstance(value, str) or not value.strip() or value.lower() in OUTCOME_NAMES or outcome_tokens.intersection(tokens):
        raise ValueError("predictor must name an entry-time feature, never an outcome")


def _validate_signal(row: dict) -> None:
    if not isinstance(row, dict) or not isinstance(row.get("position_id"), str) or not row["position_id"]:
        raise ValueError("position_id required")
    if not _positive_int(row.get("buy_event_seq")):
        raise ValueError("positive BUY event sequence required")
    signal, decision = utc(row.get("signal_timestamp")), utc(row.get("entry_decision_timestamp"))
    if signal > decision:
        raise ValueError("signal occurs after entry decision")
    features = row.get("features")
    if not isinstance(features, dict):
        raise ValueError("features must be an object")
    for name, feature in features.items():
        _predictor_name(name)
        if not isinstance(feature, dict) or set(feature) != {"value", "observed_at", "source"}:
            raise ValueError("feature requires value, observed_at and source")
        if not _scalar(feature["value"]) or not isinstance(feature["source"], str) or not feature["source"].strip():
            raise ValueError("feature value/source invalid")
        if utc(feature["observed_at"]) > decision:
            raise ValueError("post-decision predictor leakage")


def validate_rows(rows: list[dict]) -> list[dict]:
    """완료 거래의 식별자, 비용, 시간 및 진입 전 provenance를 검증한다."""
    if not isinstance(rows, list) or len(rows) > 2000:
        raise ValueError("offline rows must be a list bounded to 2000 trades")
    identifiers, sequences = set(), set()
    result = []
    for row in rows:
        _validate_signal(row)
        if row["position_id"] in identifiers or row["buy_event_seq"] in sequences:
            raise ValueError("duplicate position_id or BUY sequence")
        identifiers.add(row["position_id"])
        sequences.add(row["buy_event_seq"])
        if utc(row.get("closed_at")) < utc(row["entry_decision_timestamp"]):
            raise ValueError("close occurs before entry")
        if not _positive_int(row.get("entry_cost_lamports")):
            raise ValueError("positive entry cost required")
        pnl = row.get("realized_pnl_lamports")
        if not isinstance(pnl, int) or isinstance(pnl, bool):
            raise ValueError("realized PnL must be integer lamports")
        if "completion_event_seq" in row and (not _positive_int(row["completion_event_seq"]) or
                                                row["completion_event_seq"] <= row["buy_event_seq"]):
            raise ValueError("completion sequence must follow BUY")
        if not isinstance(row.get("forensic", {}), dict):
            raise ValueError("forensic must be an outcome-only object")
        canonical(row)
        result.append(copy.deepcopy(row))
    return sorted(result, key=lambda row: (utc(row["closed_at"]), row["buy_event_seq"]))


def _output(output: Path | str) -> Path:
    path = Path(output).resolve()
    for protected in (ROOT.resolve(), ORIGINAL_CHECKOUT.resolve(), FROZEN_RUNTIME.resolve()):
        if path == protected or protected in path.parents:
            raise ValueError("research output must be outside repository and frozen runtime")
    return path


def _read_immutable(path: Path) -> dict:
    document = read_json(path, {})
    expected = document.pop("content_hash", None)
    if not document or expected != digest(document):
        raise RuntimeError(f"immutable hash mismatch: {path.name}")
    return document


def _write_immutable(path: Path, document: dict) -> None:
    if path.exists():
        if _read_immutable(path) != document:
            raise RuntimeError(f"immutable conflict: {path.name}")
        return
    atomic_write_json(path, {**document, "content_hash": digest(document)})


def read_batch(path: Path | str) -> dict:
    result = _read_immutable(Path(path))
    if result.get("schema_version") != 1 or not re.fullmatch(r"BATCH_\d{3,}", result.get("batch_id", "")):
        raise RuntimeError("invalid batch schema")
    rows = validate_rows(result.get("rows"))
    if rows != result["rows"] or result.get("rows_hash") != digest(rows):
        raise RuntimeError("batch row integrity mismatch")
    return result


def freeze_batches(output: Path | str, rows: list[dict], as_of: str) -> list[dict]:
    """완료 시간 순으로 20건 또는 7개 KST 진입일에서 닫고 늦은 청산은 다음 batch로 보존한다."""
    path, normalized, frozen_at = _output(output), validate_rows(rows), utc(as_of)
    if any(utc(row["closed_at"]) > frozen_at for row in normalized):
        raise ValueError("dataset contains outcomes after as_of")
    with exclusive_file_lock(path / "batch_transaction", operation="alpha_batch_freeze"):
        existing_rows = {}
        for snapshot_path in sorted((path / "datasets").glob("*.json")):
            snapshot = _read_immutable(snapshot_path)
            if snapshot.get("rows_hash") != digest(snapshot.get("rows")):
                raise RuntimeError("dataset hash mismatch")
            for row in snapshot["rows"]:
                old = existing_rows.setdefault(row["position_id"], row)
                if old != row:
                    raise RuntimeError("conflicting frozen input datasets")
        current = {row["position_id"]: row for row in normalized}
        if any(current.get(key) != row for key, row in existing_rows.items()):
            raise RuntimeError("input must preserve all previously frozen rows")
        rows_hash = digest(normalized)
        snapshot_path = path / "datasets" / f"{rows_hash}.json"
        snapshot = {"schema_version": 1, "rows_hash": rows_hash, "rows": normalized, "frozen_at_utc": as_of}
        if snapshot_path.exists():
            snapshot = _read_immutable(snapshot_path)
        origin_path = path / "origin.json"
        if origin_path.exists():
            origin = _read_immutable(origin_path)
        else:
            orphan_paths = sorted((path / "datasets").glob("*.json"))
            if len(orphan_paths) > 1:
                raise RuntimeError("multiple input freezes without origin")
            initial = _read_immutable(orphan_paths[0]) if orphan_paths else snapshot
            origin = {"schema_version": 1, "dataset_hash": initial["rows_hash"],
                      "frozen_at_utc": initial["frozen_at_utc"],
                      "max_buy_event_seq": max((row["buy_event_seq"] for row in initial["rows"]), default=0)}
            if not orphan_paths:
                _write_immutable(snapshot_path, snapshot)
            _write_immutable(origin_path, origin)
        batches, consumed = [], set()
        for index, batch_path in enumerate(sorted((path / "batches").glob("BATCH_*.json")), 1):
            batch = read_batch(batch_path)
            if utc(batch["frozen_at_utc"]) > frozen_at:
                raise ValueError("as_of cannot precede previously frozen batches")
            if batch["batch_id"] != f"BATCH_{index:03d}" or batch["origin_hash"] != digest(origin):
                raise RuntimeError("batch sequence or origin mismatch")
            for row in batch["rows"]:
                if row["position_id"] in consumed or current.get(row["position_id"]) != row:
                    raise RuntimeError("batch overlap or frozen row change")
                consumed.add(row["position_id"])
            batches.append(batch)
        pending = [row for row in normalized if row["position_id"] not in consumed]
        chunk, days = [], set()
        for row in pending:
            chunk.append(row)
            days.add(utc(row["signal_timestamp"]).astimezone(timezone(timedelta(hours=9))).date().isoformat())
            if len(chunk) < 20 and len(days) < 7:
                continue
            # batch가 닫힐 때만 전체 입력을 저장하여 매 poll의 중복 이력 사본을 피한다.
            _write_immutable(snapshot_path, snapshot)
            index = len(batches) + 1
            sequences = [entry.get("completion_event_seq") for entry in chunk]
            batch = {"schema_version": 1, "batch_id": f"BATCH_{index:03d}",
                     "frozen_at_utc": as_of, "input_dataset_hash": rows_hash,
                     "origin_hash": digest(origin), "initial_discovery_dataset_hash": origin["dataset_hash"],
                     "rows": chunk, "rows_hash": digest(chunk), "completed_count": len(chunk),
                     "active_kst_days": sorted(days),
                     "close_reason": "20_COMPLETED" if len(chunk) == 20 else "7_ACTIVE_KST_DAYS",
                     "completion_boundary": {"first_closed_at": chunk[0]["closed_at"],
                                             "last_closed_at": chunk[-1]["closed_at"],
                                             "min_event_seq": min(sequences) if all(sequences) else None,
                                             "max_event_seq": max(sequences) if all(sequences) else None},
                     "discovery_endpoint": {"max_buy_event_seq": max(entry["buy_event_seq"] for entry in normalized),
                                            "latest_entry_decision_timestamp": max(
                                                (entry["entry_decision_timestamp"] for entry in normalized), key=utc),
                                            "frozen_at_utc": as_of}}
            _write_immutable(path / "batches" / f"BATCH_{index:03d}.json", batch)
            batches.append(batch)
            chunk, days = [], set()
        return batches


def import_frozen_telemetry_batch(input_path: Path | str, output: Path | str) -> dict:
    """원본의 열린 포지션 포함 7일 계약을 보존하여 별도 연구 namespace로 연결한다."""
    from src.research import alpha_telemetry_batch as telemetry
    source_path, path = Path(input_path).resolve(), _output(output)
    if source_path.name != "dataset.json" or source_path.parent.name != telemetry.BATCH_ID:
        raise ValueError("input must be ALPHA_BATCH_001/dataset.json")
    source_root = source_path.parent.parent
    if path == source_root or source_root in path.parents or path in source_root.parents:
        raise ValueError("import requires a separate research namespace outside source telemetry evidence")
    evidence_paths = [source_path, source_path.parent / "registration.json"] + [source_path.parent / "streams" / f"{name}.json" for name in telemetry.offline.SCHEMAS]
    if sum(document.stat().st_size for document in evidence_paths) > telemetry.MAX_TOTAL_BYTES:
        raise ValueError("telemetry import exceeds bounded evidence partition")
    source = telemetry.read_dataset(source_root)
    streams = {name: telemetry.read_evidence(source_path.parent / "streams" / f"{name}.json")["rows"]
               for name in telemetry.offline.SCHEMAS}
    if any(len(rows) > telemetry.MAX_STREAM_ROWS for rows in streams.values()):
        raise ValueError("telemetry import exceeds bounded stream rows")
    rebuilt = telemetry.build_dataset(streams, source["registration"], source["end_utc"])
    rebuilt.pop("raw_streams", None)
    if rebuilt != source:
        raise RuntimeError("source canonical dataset differs from sealed physical stream join")
    normalized = validate_rows(source["rows"])
    registration = source["registration"]
    runtime_root = Path(registration["runtime_root"]).resolve()
    if path == runtime_root or runtime_root in path.parents:
        raise ValueError("import output must be outside telemetry runtime")
    end_seq = source["end_event_seq"]
    if type(end_seq) is not int or end_seq < registration["start_event_seq"]:
        raise ValueError("invalid original telemetry completion boundary")
    source_hash, registration_hash = digest(source), digest(registration)
    frozen_at, rows_hash = source["end_utc"], digest(normalized)
    origin = {"schema_version": 1, "dataset_hash": rows_hash, "frozen_at_utc": frozen_at,
              "max_buy_event_seq": max([end_seq] + [row["buy_event_seq"] for row in normalized]),
              "source_batch_id": source["batch_id"], "source_dataset_hash": source_hash,
              "source_registration_hash": registration_hash}
    days = sorted({utc(receipt["identity"]["signal_detected_at"]).astimezone(timezone(timedelta(hours=9))).date().isoformat()
                   for receipt in streams["receipts"]})
    sequences = [row["completion_event_seq"] for row in normalized]
    batch = {"schema_version": 1, "batch_id": "BATCH_001", "source_batch_id": source["batch_id"],
             "source_dataset_hash": source_hash, "source_registration_hash": registration_hash,
             "source_dataset_path": str(source_path), "source_sample_contract": copy.deepcopy(source["sample_contract"]),
             "source_registration": copy.deepcopy(registration), "source_end_event_seq": end_seq,
             "frozen_at_utc": frozen_at, "input_dataset_hash": rows_hash, "origin_hash": digest(origin),
             "initial_discovery_dataset_hash": rows_hash, "rows": normalized, "rows_hash": rows_hash,
             "completed_count": len(normalized), "active_kst_days": days, "close_reason": source["close_reason"],
             "completion_boundary": {"first_closed_at": normalized[0]["closed_at"] if normalized else None,
                                     "last_closed_at": normalized[-1]["closed_at"] if normalized else None,
                                     "min_event_seq": min(sequences) if sequences else None,
                                     "max_event_seq": max(sequences) if sequences else None},
             "discovery_endpoint": {"max_buy_event_seq": origin["max_buy_event_seq"],
                                    "latest_entry_decision_timestamp": max((row["entry_decision_timestamp"] for row in normalized), key=utc, default=registration["start_utc"]),
                                    "frozen_at_utc": frozen_at},
             "source_population": {"predictors": len(streams["predictors"]), "buy_receipts": len(streams["receipts"]),
                                   "completed": len(normalized), "open_or_uncompleted": len(streams["receipts"]) - len(normalized),
                                   "decision_outcomes": {decision: sum(row["decision_outcome"] == decision for row in streams["predictors"])
                                                         for decision in sorted({row["decision_outcome"] for row in streams["predictors"]})}},
             "economics_population": "COMPLETED_CONTROL_ONLY_NO_REJECT_OR_OPEN_TRADE_COUNTERFACTUAL"}
    with exclusive_file_lock(path / "batch_transaction", operation="alpha_telemetry_import"):
        if (path / "origin.json").exists() and _read_immutable(path / "origin.json") != origin:
            raise RuntimeError("import conflicts with existing research namespace; use a new output directory")
        if not (path / "origin.json").exists() and any(path.glob("*.json")):
            raise RuntimeError("import requires a new namespace or matching interrupted import")
        allowed_orphans = {path / "datasets" / f"{rows_hash}.json", path / "batches" / "BATCH_001.json"}
        if not (path / "origin.json").exists() and any(document not in allowed_orphans for document in path.rglob("*.json")):
            raise RuntimeError("import found unrelated files; use a new research namespace")
        _write_immutable(path / "datasets" / f"{rows_hash}.json", {"schema_version": 1, "rows_hash": rows_hash,
                         "rows": normalized, "frozen_at_utc": frozen_at})
        _write_immutable(path / "origin.json", origin)
        _write_immutable(path / "batches" / "BATCH_001.json", batch)
    return read_batch(path / "batches" / "BATCH_001.json")


def _validate_rule(rule: dict, depth: int = 0) -> int:
    if not isinstance(rule, dict) or depth > 3:
        raise ValueError("invalid or overly deep rule")
    op = rule.get("op")
    if op in {"AND", "OR"}:
        parts = rule.get("parts")
        if set(rule) != {"op", "parts"} or not isinstance(parts, list) or not 2 <= len(parts) <= 5:
            raise ValueError("AND/OR requires 2..5 parts")
        count = 1 + sum(_validate_rule(part, depth + 1) for part in parts)
        if count > 15:
            raise ValueError("rule exceeds 15 nodes")
        return count
    _predictor_name(rule.get("feature"))
    if op == "in":
        values = rule.get("values")
        if set(rule) != {"feature", "op", "values"} or not isinstance(values, list) or not 1 <= len(values) <= 10:
            raise ValueError("in requires 1..10 scalar values")
        if any(value is None or not _scalar(value) for value in values):
            raise ValueError("invalid membership values")
    elif op in {"gt", "gte", "lt", "lte", "eq"}:
        if set(rule) != {"feature", "op", "threshold"} or rule["threshold"] is None or not _scalar(rule["threshold"]):
            raise ValueError("invalid threshold")
        if op != "eq" and (isinstance(rule["threshold"], bool) or not isinstance(rule["threshold"], (int, float))):
            raise ValueError("ordered comparisons require numeric threshold")
    else:
        raise ValueError("unsupported rule operation")
    return 1


def evaluate_rule(rule: dict, row: dict) -> bool | None:
    """미관측 입력은 UNKNOWN이며 사후 outcome에는 접근하지 않는다."""
    _validate_rule(rule)
    _validate_signal(row)
    def evaluate(node):
        op = node["op"]
        if op in {"AND", "OR"}:
            values = [evaluate(part) for part in node["parts"]]
            # 누락 입력을 단락 평가로 감추지 않고 별도 층으로 보존한다.
            if None in values:
                return None
            return all(values) if op == "AND" else any(values)
        value = row["features"].get(node["feature"], {}).get("value")
        if value is None:
            return None
        if op == "in":
            return any(type(value) is type(other) and value == other for other in node["values"])
        threshold = node["threshold"]
        numeric = lambda item: isinstance(item, (int, float)) and not isinstance(item, bool)
        if op == "eq":
            return value == threshold if (numeric(value) and numeric(threshold)) or type(value) is type(threshold) else None
        if not numeric(value):
            return None
        return {"gt": value > threshold, "gte": value >= threshold,
                "lt": value < threshold, "lte": value <= threshold}[op]
    return evaluate(rule)


def _definition(definition: dict) -> dict:
    result = copy.deepcopy(definition)
    candidate_id = result.get("candidate_id")
    if not isinstance(candidate_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]+-v[1-9]\d*", candidate_id):
        raise ValueError("candidate_id must end in -v<version>")
    if not _positive_int(result.get("version")) or not candidate_id.endswith(f"-v{result['version']}"):
        raise ValueError("candidate version mismatch")
    _validate_rule(result.get("rule"))
    if result.get("kind") not in {"negative_filter", "profitable_subset", "classification"}:
        raise ValueError("unsupported challenger kind")
    if result["kind"] == "classification" and (not isinstance(result.get("classification"), str) or not result["classification"].strip()):
        raise ValueError("classification label required")
    if not isinstance(result.get("discovery_evidence"), dict) or not result["discovery_evidence"]:
        raise ValueError("discovery evidence required")
    if not _positive_int(result.get("start_event_seq")):
        raise ValueError("future start sequence required")
    utc(result.get("start_timestamp"))
    if not re.fullmatch(r"BATCH_\d{3,}", result.get("creation_batch", "")):
        raise ValueError("creation batch required")
    rule_hash = digest(result["rule"])
    if result.get("rule_hash", rule_hash) != rule_hash:
        raise ValueError("rule hash mismatch")
    if result.get("stage_contract", STAGE_CONTRACT) != STAGE_CONTRACT:
        raise ValueError("stage contract mismatch")
    if "stage3_design" in result:
        design = result["stage3_design"]
        if not isinstance(design, dict) or not _positive_int(design.get("min_completed")) or design["min_completed"] <= 50:
            raise ValueError("Stage 3 must pre-register a longer sample gate")
    result.update(rule_hash=rule_hash, stage_contract=copy.deepcopy(STAGE_CONTRACT))
    canonical(result)
    return result


def validate_definition(definition: dict) -> dict:
    """외부 평가에서도 등록 시와 같은 불변 규칙 계약을 검증한다."""
    return _definition(definition)


def _registry(path: Path) -> dict:
    registry = read_json(path / "challenger_registry.json", {"schema_version": 1, "version": 0, "candidates": {}})
    version = registry.get("version")
    if registry.get("schema_version") != 1 or not isinstance(version, int) or isinstance(version, bool) or version < 0 or not isinstance(registry.get("candidates"), dict):
        raise RuntimeError("invalid challenger registry")
    for candidate_id, entry in registry["candidates"].items():
        if not isinstance(candidate_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]+-v[1-9]\d*", candidate_id) or not isinstance(entry, dict):
            raise RuntimeError("invalid registry entry")
        candidate = _read_immutable(path / "candidates" / f"{candidate_id}.json")
        if _definition(candidate) != candidate or candidate["candidate_id"] != candidate_id or entry.get("definition_hash") != digest(candidate) or entry.get("status") not in STATUSES or not isinstance(entry.get("events"), list):
            raise RuntimeError("registry definition or status mismatch")
        discovery = _read_immutable(path / "candidates" / f"{candidate_id}.discovery.json")
        if entry.get("discovery_report_hash") != digest(discovery) or discovery.get("candidate_id") != candidate_id or discovery.get("definition_hash") != digest(candidate):
            raise RuntimeError("registry discovery evidence mismatch")
        if discovery.get("discovery_batch") != candidate["creation_batch"] or any(discovery.get(key) != candidate.get(key) for key in ("rationale", "expected_mechanism")):
            raise RuntimeError("registry discovery definition metadata mismatch")
    if sum(entry["status"] not in TERMINAL for entry in registry["candidates"].values()) > 5:
        raise RuntimeError("registry exceeds five active challengers")
    return registry


def register_challenger(output: Path | str, definition: dict) -> dict:
    path, candidate = _output(output), _definition(definition)
    for name in ("rationale", "expected_mechanism"):
        if not isinstance(candidate.get(name), str) or not candidate[name].strip():
            raise ValueError("registration requires nonempty " + name)
    batch = read_batch(path / "batches" / f"{candidate['creation_batch']}.json")
    endpoint = batch["discovery_endpoint"]
    if candidate["start_event_seq"] <= endpoint["max_buy_event_seq"] or utc(candidate["start_timestamp"]) <= utc(endpoint["frozen_at_utc"]):
        raise ValueError("candidate start must be strictly after discovery freeze")
    candidate_path = path / "candidates" / f"{candidate['candidate_id']}.json"
    with exclusive_file_lock(path / "challenger_registry.json", operation="alpha_register"):
        registry = _registry(path)
        old = registry["candidates"].get(candidate["candidate_id"])
        if old:
            if old["definition_hash"] != digest(candidate):
                raise RuntimeError("candidate version is immutable; register a new version")
            return candidate
        from src.research.alpha_batch_forensics import discovery_screen
        screen = discovery_screen(batch["rows"], candidate["rule"], candidate["kind"],
                                  candidate["discovery_evidence"].get("discovery_screen_policy"))
        if screen["status"] != "PASS_DISCOVERY":
            raise ValueError("REJECT_DISCOVERY: " + ",".join(screen["reasons"]))
        if sum(entry["status"] not in TERMINAL for entry in registry["candidates"].values()) >= 5:
            raise RuntimeError("five active challenger limit")
        discovery = {"schema_version": 1, "candidate_id": candidate["candidate_id"], "version": candidate["version"],
                     "definition_hash": digest(candidate), "rule_hash": candidate["rule_hash"],
                     "discovery_batch": candidate["creation_batch"], "discovery_batch_hash": digest(batch),
                     "rationale": candidate["rationale"], "expected_mechanism": candidate["expected_mechanism"],
                     "discovery_coverage": {key: screen["economics"][key] for key in
                         ("decision_coverage", "exclusion_coverage", "inclusion_coverage", "missing_predictor_rate")},
                     "discovery_economics": screen["economics"], "discovery_screen": screen}
        _write_immutable(candidate_path, candidate)
        _write_immutable(path / "candidates" / f"{candidate['candidate_id']}.discovery.json", discovery)
        registry["candidates"][candidate["candidate_id"]] = {
            "definition_hash": digest(candidate), "status": "DISCOVERY_ONLY", "events": [],
            "registered_at_utc": datetime.now(timezone.utc).isoformat(), "discovery_screen_hash": digest(screen),
            "discovery_report_hash": digest(discovery)}
        registry["version"] += 1
        atomic_write_json(path / "challenger_registry.json", registry)
    return candidate


def registered_definition(output: Path | str, candidate_id: str) -> dict:
    """정식 미래 검증은 registry에 등록된 봉인 정의만 사용한다."""
    path = _output(output)
    if not (path / "challenger_registry.json").is_file():
        raise RuntimeError("registered challenger registry required")
    with exclusive_file_lock(path / "challenger_registry.json", operation="alpha_registered_read"):
        registry = _registry(path)
        if candidate_id not in registry["candidates"]:
            raise RuntimeError("candidate is not registered")
        candidate = _read_immutable(path / "candidates" / f"{candidate_id}.json")
        batch = read_batch(path / "batches" / f"{candidate['creation_batch']}.json")
        endpoint = batch["discovery_endpoint"]
        if (candidate["start_event_seq"] <= endpoint["max_buy_event_seq"] or
                utc(candidate["start_timestamp"]) <= utc(endpoint["frozen_at_utc"])):
            raise RuntimeError("registered candidate discovery boundary mismatch")
        return candidate


def shadow_decisions(definitions: list[dict], row: dict) -> list[dict]:
    _validate_signal(row)
    if len(definitions) > 5:
        raise ValueError("at most five active challengers")
    result, seen = [], set()
    for definition in definitions:
        candidate = _definition(definition)
        if candidate["candidate_id"] in seen:
            raise ValueError("duplicate challenger")
        seen.add(candidate["candidate_id"])
        fresh = (row["buy_event_seq"] >= candidate["start_event_seq"] and
                 utc(row["signal_timestamp"]) >= utc(candidate["start_timestamp"]))
        matched = evaluate_rule(candidate["rule"], row) if fresh else None
        enter = (not matched if candidate["kind"] == "negative_filter" else matched) if matched is not None else None
        classification = candidate.get("classification") if candidate["kind"] == "classification" and matched else None
        if candidate["kind"] == "classification":
            enter = None
        result.append({"candidate_id": candidate["candidate_id"], "position_id": row["position_id"],
                       "buy_event_seq": row["buy_event_seq"], "status": "NOT_FRESH" if not fresh else "UNKNOWN" if matched is None else "EVALUATED",
                       "would_enter": enter, "would_skip": not enter if enter is not None else None,
                       "alternative_classification": classification})
    return result


def validation_progress(definition: dict, completed_rows: list[dict]) -> dict:
    candidate = _definition(definition)
    rows = validate_rows(completed_rows)
    discovery_ids = set(candidate["discovery_evidence"].get("position_ids", []))
    rows = [row for row in rows if row.get("batch_id") != candidate["creation_batch"] and row["position_id"] not in discovery_ids]
    decisions = [shadow_decisions([candidate], row)[0] for row in rows]
    fresh = [item for item in decisions if item["status"] != "NOT_FRESH"]
    count = len(fresh)
    return {"fresh_completed": count, "stage1_ready": count >= 20, "stage2_ready": count >= 50,
            "stage1_position_ids": [item["position_id"] for item in fresh[:20]],
            "stage2_position_ids": [item["position_id"] for item in fresh[20:50]],
            "missing_predictor_count": sum(item["status"] == "UNKNOWN" for item in fresh),
            "control_promotion": False}


def record_shadow_predictor(output: Path | str, predictor: dict, recorded_at: str) -> dict:
    """외부 Control predictor를 소비해 결정만 봉인한다. 런타임 거래는 수행하지 않는다."""
    path = _output(output)
    allowed = {"control_predictor_id", "control_event_seq", "control_decision", "signal_timestamp",
               "entry_decision_timestamp", "features", "epoch_id", "build_sha", "strategy_fingerprint"}
    if not isinstance(predictor, dict) or set(predictor) - allowed:
        raise ValueError("shadow input must contain only the Control predictor contract")
    identity = predictor.get("control_predictor_id")
    if not isinstance(identity, str) or not identity or len(identity) > 256 or not _positive_int(predictor.get("control_event_seq")):
        raise ValueError("Control predictor identity and event sequence required")
    for name in ("control_decision", "epoch_id", "build_sha", "strategy_fingerprint"):
        if not isinstance(predictor.get(name), str) or not predictor[name].strip():
            raise ValueError("Control decision and runtime provenance required")
    projection = {"position_id": "signal:" + identity, "buy_event_seq": predictor["control_event_seq"],
                  "signal_timestamp": predictor.get("signal_timestamp"),
                  "entry_decision_timestamp": predictor.get("entry_decision_timestamp"), "features": predictor.get("features")}
    _validate_signal(projection)
    if utc(recorded_at) < utc(projection["entry_decision_timestamp"]):
        raise ValueError("shadow recording cannot precede Control decision")
    state_path = path / "shadow_state.json"
    with exclusive_file_lock(path / "challenger_registry.json", operation="alpha_shadow_record"):
        registry = _registry(path)
        active = [(candidate_id, entry) for candidate_id, entry in registry["candidates"].items() if entry["status"] not in TERMINAL]
        definitions = [_read_immutable(path / "candidates" / f"{candidate_id}.json") for candidate_id, _ in active]
        decisions = shadow_decisions(definitions, projection)
        for decision, (_, entry) in zip(decisions, active):
            decision["score"] = None
            registered_at = entry.get("registered_at_utc")
            decision["preregistered_before_signal"] = bool(registered_at and utc(registered_at) <= utc(projection["signal_timestamp"]))
            if not decision["preregistered_before_signal"]:
                decision.update(status="NOT_PROSPECTIVELY_REGISTERED", would_enter=None, would_skip=None, alternative_classification=None)
        document = {"schema_version": 1, "recorded_at_utc": recorded_at, "predictor": copy.deepcopy(predictor),
                    "predictor_hash": digest(predictor), "registry_version": registry["version"],
                    "decisions": decisions, "mode": "OFFLINE_EXTERNAL_PREDICTOR_CONSUMER", "control_changes": False}
        record_path = path / "shadow_records" / (digest(identity) + ".json")
        state = read_json(state_path, {"schema_version": 1, "version": 0, "recorded_count": 0})
        if state.get("schema_version") != 1 or type(state.get("version")) is not int or type(state.get("recorded_count")) is not int:
            raise RuntimeError("invalid shadow recorder state")
        record_paths = list((path / "shadow_records").glob("*.json"))
        if len(record_paths) > 2000:
            raise RuntimeError("shadow recorder exceeds bounded 2000-record research partition")
        for existing_path in record_paths:
            prior = _read_immutable(existing_path)
            old = prior["predictor"]
            if (old["epoch_id"], old["control_event_seq"]) == (predictor["epoch_id"], predictor["control_event_seq"]) and old["control_predictor_id"] != identity:
                raise RuntimeError("duplicate Control event sequence in epoch")
        if record_path.exists():
            prior = _read_immutable(record_path)
            if prior["predictor"] != predictor:
                raise RuntimeError("Control predictor identity was rewritten")
            if state["recorded_count"] != len(record_paths):
                state.update(version=state["version"] + 1, recorded_count=len(record_paths))
                atomic_write_json(state_path, state)
            return prior
        if len(record_paths) >= 2000:
            raise RuntimeError("shadow recorder partition full; create a new research partition")
        _write_immutable(record_path, document)
        # 기록이 성공하고 state 저장이 중단된 경우에도 재시도에서 중복은 만들지 않는다.
        state.update(version=state["version"] + 1, recorded_count=len(record_paths) + 1,
                     last_recorded_at_utc=recorded_at)
        atomic_write_json(state_path, state)
        return document


def persist_stage_result(output: Path | str, candidate_id: str, rows: list[dict], stage: int,
                         evaluated_at: str) -> dict:
    """고정 fresh 구간의 결과와 연구 상태만 기록하며 Control에는 적용하지 않는다."""
    from src.research.alpha_batch_forensics import evaluate_stage
    path, normalized = _output(output), validate_rows(rows)
    if any(utc(row["closed_at"]) > utc(evaluated_at) for row in normalized):
        raise ValueError("stage outcomes occur after evaluation")
    with exclusive_file_lock(path / "challenger_registry.json", operation="alpha_stage_result"):
        registry = _registry(path)
        entry = registry["candidates"].get(candidate_id)
        if entry is None:
            raise ValueError("unknown challenger")
        candidate = _read_immutable(path / "candidates" / f"{candidate_id}.json")
        registered_at = entry.get("registered_at_utc")
        if not registered_at:
            raise RuntimeError("durable registration timestamp required for prospective stages")
        excluded_pre_registration = sum(utc(row["signal_timestamp"]) < utc(registered_at) for row in normalized)
        normalized = [row for row in normalized if utc(row["signal_timestamp"]) >= utc(registered_at)]
        report = evaluate_stage(normalized, candidate, stage)
        if entry.get("stage1_position_ids") is not None:
            first_report = evaluate_stage(normalized, candidate, 1)
            if first_report["selected_position_ids"] != entry["stage1_position_ids"] or first_report["selected_rows_hash"] != entry.get("stage1_rows_hash"):
                raise RuntimeError("fixed Stage 1 sample cannot be replaced")
        if stage == 2 and entry.get("stage2_position_ids") not in (None, report["selected_position_ids"]):
            raise RuntimeError("fixed Stage 2 sample cannot be replaced")
        if stage == 2 and entry.get("stage2_rows_hash") not in (None, report["selected_rows_hash"]):
            raise RuntimeError("fixed Stage 2 outcomes cannot be replaced")
        if stage == 2 and report["status"] in {"PROMISING_CHALLENGER", "ONLY_NEGATIVE_FILTER"} and entry["status"] == "DISCOVERY_ONLY":
            raise ValueError("durable Stage 1 must be confirmed before Stage 2")
        report.update(definition_hash=digest(candidate), input_rows_hash=digest(normalized), evaluated_at_utc=evaluated_at,
                      registered_at_utc=registered_at, excluded_before_real_registration=excluded_pre_registration)
        report_hash = digest(report)
        stage_path = path / "stage_reports" / candidate_id / f"stage{stage}-{report_hash}.json"
        _write_immutable(stage_path, report)
        status = report["status"]
        allowed = {"DISCOVERY_ONLY": {"FRESH_DIRECTION_CONFIRMED", "REJECT_EARLY"},
                   "FRESH_DIRECTION_CONFIRMED": {"PROMISING_CHALLENGER", "ONLY_NEGATIVE_FILTER", "REJECT_EARLY"}}
        if status in allowed.get(entry["status"], set()):
            if stage == 2 and entry["status"] != "FRESH_DIRECTION_CONFIRMED":
                raise ValueError("durable Stage 1 must be confirmed before Stage 2")
            if stage == 1 and entry.get("stage1_position_ids") not in (None, report["selected_position_ids"]):
                raise RuntimeError("fixed Stage 1 sample cannot be replaced")
            if stage == 2:
                first = evaluate_stage(normalized, candidate, 1)
                if entry.get("stage1_position_ids") != first["selected_position_ids"]:
                    raise RuntimeError("Stage 2 input does not preserve durable Stage 1")
                if set(entry["stage1_position_ids"]) & set(report["selected_position_ids"]):
                    raise RuntimeError("Stage 1/2 overlap")
            entry["events"].append({"from": entry["status"], "to": status,
                                    "evidence": {"stage": stage, "report_hash": report_hash, "automatic_research_transition": True}})
            entry["status"] = status
            if stage == 1:
                entry["stage1_position_ids"] = report["selected_position_ids"]
                entry["stage1_rows_hash"] = report["selected_rows_hash"]
            if stage == 2:
                entry["stage2_position_ids"] = report["selected_position_ids"]
                entry["stage2_rows_hash"] = report["selected_rows_hash"]
            registry["version"] += 1
            atomic_write_json(path / "challenger_registry.json", registry)
        return report


def transition_candidate(output: Path | str, candidate_id: str, status: str, evidence: dict) -> dict:
    """정해진 fresh 경계와 사람의 판단만 상태에 기록하며 Control은 변경하지 않는다."""
    path = _output(output)
    if status not in STATUSES or not isinstance(evidence, dict) or not evidence or evidence.get("human_decision") is not True:
        raise ValueError("valid status and explicit human decision evidence required")
    with exclusive_file_lock(path / "challenger_registry.json", operation="alpha_transition"):
        registry = _registry(path)
        entry = registry["candidates"].get(candidate_id)
        if entry is None:
            raise ValueError("unknown challenger")
        if entry["status"] == status:
            if entry["events"] and entry["events"][-1]["evidence"] == evidence:
                return copy.deepcopy(entry)
            raise RuntimeError("conflicting repeated transition")
        transitions = {"DISCOVERY_ONLY": {"FRESH_DIRECTION_CONFIRMED"} | TERMINAL,
                       "FRESH_DIRECTION_CONFIRMED": {"PROMISING_CHALLENGER", "ONLY_NEGATIVE_FILTER"} | TERMINAL,
                       "PROMISING_CHALLENGER": {"ROBUST_PAPER_ALPHA", "ONLY_NEGATIVE_FILTER"} | TERMINAL,
                       "ROBUST_PAPER_ALPHA": TERMINAL, "ONLY_NEGATIVE_FILTER": TERMINAL}
        if status not in transitions.get(entry["status"], set()):
            raise ValueError("invalid lifecycle transition")
        candidate = _read_immutable(path / "candidates" / f"{candidate_id}.json")
        if status not in TERMINAL:
            from src.research.alpha_batch_forensics import evaluate_stage
            batch_ids = evidence.get("validation_batch_ids")
            if not isinstance(batch_ids, list) or not batch_ids or len(set(batch_ids)) != len(batch_ids):
                raise ValueError("future validation batches required")
            rows = []
            for batch_id in batch_ids:
                if not isinstance(batch_id, str) or not re.fullmatch(r"BATCH_\d{3,}", batch_id) or batch_id == candidate["creation_batch"]:
                    raise ValueError("discovery cannot be validation")
                rows.extend(read_batch(path / "batches" / f"{batch_id}.json")["rows"])
            registered_at = entry.get("registered_at_utc")
            if not registered_at:
                raise RuntimeError("durable registration timestamp required for prospective transition")
            rows = [row for row in rows if utc(row["signal_timestamp"]) >= utc(registered_at)]
            progress = validation_progress(candidate, rows)
            minimum = 50 if status in {"PROMISING_CHALLENGER", "ROBUST_PAPER_ALPHA"} else 20
            if progress["fresh_completed"] < minimum:
                raise ValueError("fresh stage sample gate missing")
            if status in {"FRESH_DIRECTION_CONFIRMED", "PROMISING_CHALLENGER", "ONLY_NEGATIVE_FILTER"}:
                report = evaluate_stage(rows, candidate, 1 if status == "FRESH_DIRECTION_CONFIRMED" else 2)
                if report["status"] != status:
                    raise ValueError("fresh economic stage report does not support requested transition")
                if report["stage"] == 2:
                    first_report = evaluate_stage(rows, candidate, 1)
                    if entry.get("stage1_rows_hash") != first_report["selected_rows_hash"]:
                        raise RuntimeError("Stage 2 must preserve durable Stage 1 outcomes")
                _write_immutable(path / "stage_reports" / candidate_id / f"stage{report['stage']}-{digest(report)}.json", report)
                entry[f"stage{report['stage']}_position_ids"] = report["selected_position_ids"]
                entry[f"stage{report['stage']}_rows_hash"] = report["selected_rows_hash"]
            if status == "ROBUST_PAPER_ALPHA":
                design = candidate.get("stage3_design")
                if not design or evidence.get("stage3_design_hash") != digest(design) or progress["fresh_completed"] < design["min_completed"]:
                    raise ValueError("Stage 3 requires its immutable longer design and fresh gate")
        entry["events"].append({"from": entry["status"], "to": status, "evidence": copy.deepcopy(evidence)})
        entry["status"] = status
        registry["version"] += 1
        atomic_write_json(path / "challenger_registry.json", registry)
        return copy.deepcopy(entry)
