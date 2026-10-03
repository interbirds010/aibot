"""분리된 immutable telemetry를 연구 시점에만 연결하는 읽기 전용 도구."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Iterable

SCHEMAS = {"predictors": ("predictor_schema_version", 2),
           "receipts": ("receipt_schema_version", 1),
           "outcomes": ("outcome_schema_version", 1)}
MAX_ROW_BYTES = 64 * 1024
MAX_STREAM_ROWS = 4096


def _canonical(value: dict) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _validate(document: dict, stream: str) -> None:
    field, schema = SCHEMAS[stream]
    if not isinstance(document, dict):
        raise ValueError("telemetry row must be an object")
    body = {key: value for key, value in document.items() if key != "content_hash"}
    if hashlib.sha256(_canonical(body)).hexdigest() != document.get("content_hash"):
        raise ValueError("telemetry content hash mismatch")
    if document.get("schema_version") != schema or document.get(field) != schema:
        raise ValueError("telemetry stream schema mismatch")
    identity = document.get("identity")
    if not isinstance(identity, dict) or not isinstance(identity.get("mint"), str) or not identity["mint"]:
        raise ValueError("telemetry identity missing")
    if any(not isinstance(document.get(key), str) or not document[key]
           for key in ("telemetry_epoch_id", "build_sha", "session_id")):
        raise ValueError("telemetry provenance missing")
    if stream == "predictors":
        if not isinstance(identity.get("signal_id"), str) or not identity["signal_id"] or "execution_receipt" in document or "outcome" in document:
            raise ValueError("invalid predictor stream separation")
    elif stream == "receipts":
        if any(not isinstance(identity.get(key), str) or not identity[key] for key in ("signal_id", "trade_id")):
            raise ValueError("BUY receipt join identity missing")
    elif not isinstance(identity.get("trade_id"), str) or not identity["trade_id"]:
        raise ValueError("completed outcome trade_id missing")


def load_stream(root: Path, epoch_id: str, stream: str) -> list[dict]:
    """운영 파일을 변경하지 않고 한 epoch의 별도 stream을 검증해서 읽는다."""
    if stream not in SCHEMAS or not isinstance(epoch_id, str) or not epoch_id or len(epoch_id) > 128 \
            or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in epoch_id):
        raise ValueError("invalid stream or epoch path")
    directory = Path(root) / "data/research/entry_telemetry/epochs" / epoch_id / stream
    rows = []
    for path in sorted(directory.glob("*/*.json")):
        if len(rows) >= MAX_STREAM_ROWS:
            raise ValueError("offline stream row bound exceeded")
        if path.stat().st_size > MAX_ROW_BYTES:
            raise ValueError("telemetry row byte bound exceeded")
        document = json.loads(path.read_text(encoding="utf-8"))
        _validate(document, stream)
        if document["telemetry_epoch_id"] != epoch_id:
            raise ValueError("telemetry epoch directory mismatch")
        rows.append(document)
    return rows


def _utc(stamp: object) -> str:
    try:
        value = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
        if value.tzinfo is None:
            raise ValueError("naive signal timestamp")
        return value.astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError) as exc:
        raise ValueError("invalid signal timestamp") from exc


def _consistent(left: dict, right: dict) -> None:
    for key in ("telemetry_epoch_id", "build_sha"):
        if left.get(key) != right.get(key):
            raise ValueError("offline join epoch/build conflict")
    left_id, right_id = left["identity"], right["identity"]
    for key in ("mint", "route_type", "strategy_family"):
        if left_id.get(key) != right_id.get(key):
            raise ValueError("offline join identity conflict")
    for key in ("source_wallet_hash", "source_signature_hash"):
        if key in left_id and key in right_id and left_id[key] != right_id[key]:
            raise ValueError("offline join source identity conflict")
    if _utc(left_id.get("signal_detected_at")) != _utc(right_id.get("signal_detected_at")):
        raise ValueError("offline join signal timestamp conflict")


def _index(rows: Iterable[dict], stream: str, key: str) -> dict[tuple[str, str], dict]:
    result = {}
    for row in rows:
        _validate(row, stream)
        identity = row["identity"]
        index = (row["telemetry_epoch_id"], identity.get(key))
        if index in result:
            raise ValueError("duplicate telemetry join identity")
        result[index] = row
        if len(result) > MAX_STREAM_ROWS:
            raise ValueError("offline stream row bound exceeded")
    return result


def join_streams(predictors: Iterable[dict], receipts: Iterable[dict] = (),
                 outcomes: Iterable[dict] = ()) -> dict:
    """runtime lookup/update 없이 메모리에서만 연결한다. 누락 receipt는 명시한다."""
    pred = _index(predictors, "predictors", "signal_id")
    buys = _index(receipts, "receipts", "signal_id")
    closed = _index(outcomes, "outcomes", "trade_id")
    seen_trades = set()
    used_receipts, used_outcomes = set(), set()
    rows = []
    for key, predictor in pred.items():
        receipt = buys.get(key)
        outcome = None
        decision = predictor.get("decision", {}).get("outcome")
        if receipt is not None:
            if decision != "BUY":
                raise ValueError("non-BUY predictor has execution receipt")
            _consistent(predictor, receipt)
            trade_key = (key[0], receipt["identity"]["trade_id"])
            if trade_key in seen_trades:
                raise ValueError("trade_id maps to multiple signals")
            seen_trades.add(trade_key)
            used_receipts.add(key)
            outcome = closed.get(trade_key)
            if outcome is not None:
                _consistent(receipt, outcome)
                receipt_seq = receipt["identity"].get("event_seq")
                buy_seq = outcome["identity"].get("buy_event_seq")
                if receipt_seq is not None and receipt_seq != buy_seq:
                    raise ValueError("offline join BUY event_seq conflict")
                used_outcomes.add(trade_key)
        rows.append({"predictor": predictor, "receipt": receipt, "outcome": outcome,
                     "join_status": "COMPLETED" if outcome is not None else
                     "BUY_RECEIPT" if receipt is not None else
                     "MISSING_RECEIPT" if decision == "BUY" else "PREDICTOR_ONLY"})
    return {"rows": rows,
            "orphan_receipts": [value for key, value in buys.items() if key not in used_receipts],
            "orphan_outcomes": [value for key, value in closed.items() if key not in used_outcomes]}


def join_epoch(root: Path, epoch_id: str) -> dict:
    """경로만 받아 실제 세 stream을 읽고 검증한다. 저장/마이그레이션은 하지 않는다."""
    return join_streams(*(load_stream(root, epoch_id, stream) for stream in SCHEMAS))
