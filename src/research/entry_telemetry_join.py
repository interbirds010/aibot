"""분리된 immutable telemetry를 연구 시점에만 연결하는 읽기 전용 도구."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Iterable, Iterator

SCHEMAS = {"predictors": ("predictor_schema_version", 2),
           "receipts": ("receipt_schema_version", 1),
           "outcomes": ("outcome_schema_version", 1)}
MAX_ROW_BYTES = 64 * 1024


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
    provenance = document.get("provenance")
    if not isinstance(provenance, dict) or not isinstance(provenance.get("config_fingerprint"), str) or not provenance["config_fingerprint"]:
        raise ValueError("telemetry config fingerprint missing")
    if stream == "predictors":
        if not isinstance(identity.get("signal_id"), str) or not identity["signal_id"] or "execution_receipt" in document or "outcome" in document:
            raise ValueError("invalid predictor stream separation")
    elif stream == "receipts":
        if any(not isinstance(identity.get(key), str) or not identity[key] for key in ("signal_id", "trade_id")):
            raise ValueError("BUY receipt join identity missing")
    elif not isinstance(identity.get("trade_id"), str) or not identity["trade_id"]:
        raise ValueError("completed outcome trade_id missing")


def _check_identity_index(directory: Path, path: Path, document: dict, stream: str) -> None:
    """새 writer의 sharded identity 봉인을 검증한다. 구 offline fixture는 index가 없다."""
    metadata = directory / "_identity"
    if not metadata.exists():
        return
    key = document["identity"]["trade_id" if stream == "outcomes" else "signal_id"]
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    index_path = metadata / digest[:2] / (digest + ".json")
    if index_path.is_symlink() or index_path.parent.is_symlink() or metadata.is_symlink():
        raise ValueError("telemetry identity index symlink forbidden")
    with index_path.open("rb") as handle:
        payload = handle.read(4097)
    if len(payload) > 4096:
        raise ValueError("telemetry identity index byte bound exceeded")
    index = json.loads(payload.decode("utf-8"))
    if not isinstance(index, dict):
        raise ValueError("telemetry identity index must be an object")
    body = {key: value for key, value in index.items() if key != "content_hash"}
    if hashlib.sha256(_canonical(body)).hexdigest() != index.get("content_hash"):
        raise ValueError("telemetry identity index hash mismatch")
    expected = {"index_schema_version": 1, "telemetry_epoch_id": document["telemetry_epoch_id"],
        "stream": stream, "identity": document["identity"],
        "record_relative_path": path.relative_to(directory).as_posix(),
        "record_content_hash": document["content_hash"]}
    if body != expected:
        raise ValueError("telemetry identity index linkage mismatch")


def iter_stream(root: Path, epoch_id: str, stream: str) -> Iterator[dict]:
    """일별 파일을 정렬·전체 복사하지 않고 검증하며 읽는다. 파일은 수정하지 않는다."""
    if stream not in SCHEMAS or not isinstance(epoch_id, str) or not epoch_id or len(epoch_id) > 128 \
            or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in epoch_id):
        raise ValueError("invalid stream or epoch path")
    directory = Path(root) / "data/research/entry_telemetry/epochs" / epoch_id / stream
    if not directory.exists():
        return
    with os.scandir(directory) as partitions:
        for partition in partitions:
            if not partition.is_dir(follow_symlinks=False):
                if partition.is_symlink():
                    raise ValueError("telemetry partition symlink forbidden")
                continue
            if partition.name == "_identity":
                continue
            try:
                if datetime.strptime(partition.name, "%Y-%m-%d").date().isoformat() != partition.name:
                    raise ValueError("noncanonical day")
            except ValueError as error:
                raise ValueError("invalid telemetry date partition") from error
            with os.scandir(partition.path) as files:
                for entry in files:
                    if not entry.name.endswith(".json"):
                        continue
                    if not entry.is_file(follow_symlinks=False):
                        raise ValueError("telemetry row must be a regular file")
                    # 읽는 도중 파일이 커져도 row 한도를 넘어 메모리를 사용하지 않는다.
                    with open(entry.path, "rb") as handle:
                        payload = handle.read(MAX_ROW_BYTES + 1)
                    if len(payload) > MAX_ROW_BYTES:
                        raise ValueError("telemetry row byte bound exceeded")
                    document = json.loads(payload.decode("utf-8"))
                    _validate(document, stream)
                    if document["telemetry_epoch_id"] != epoch_id:
                        raise ValueError("telemetry epoch directory mismatch")
                    _check_identity_index(directory, Path(entry.path), document, stream)
                    yield document


def load_stream(root: Path, epoch_id: str, stream: str) -> list[dict]:
    """호환 API: 검증된 stream을 offline 연구 메모리에만 모은다."""
    return list(iter_stream(root, epoch_id, stream))


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
    if left["provenance"]["config_fingerprint"] != right["provenance"]["config_fingerprint"]:
        raise ValueError("offline join config fingerprint conflict")
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
    return result


def join_streams(predictors: Iterable[dict], receipts: Iterable[dict] = (),
                 outcomes: Iterable[dict] = ()) -> dict:
    """Offline 전용 identity index를 메모리에 만든다. runtime 호출·행 수정은 없다.

    전체 epoch 연구 결과의 메모리는 행 수에 비례한다. writer의 큐/보관 한도와
    무관하며 데이터가 큰 연구 작업은 별도 프로세스에서 실행한다.
    """
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
    return join_streams(*(iter_stream(root, epoch_id, stream) for stream in SCHEMAS))
