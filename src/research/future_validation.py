"""Frozen hypothesis registry와 research-only Future Shadow 평가기."""

from __future__ import annotations

import argparse
import copy
import hashlib
import hmac
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from src.observation_analysis import performance_metrics
from src.research.alpha_discovery import (
    BUCKET_VERSION,
    FEATURES_BY_FAMILY,
    MAX_SUPPORTED_OBSERVATION_SCHEMA_VERSION,
    MIN_SUPPORTED_OBSERVATION_SCHEMA_VERSION,
    MIN_HOLDOUT_SAMPLED,
    MIN_TOTAL_SAMPLED,
    MIN_TRACKABLE_COVERAGE_PERCENT,
    PreparedRow,
    SCHEMA_VERSION as ALPHA_DISCOVERY_SCHEMA_VERSION,
    _bucket_configuration,
    _feature_value,
    _signal_family,
    _signal_identity_digest,
    _signal_timestamp,
    assign_bucket,
)
from src.research.prospective_features import (
    FEATURE_COLLECTION_SCHEMA_VERSION,
    MAX_PRE_SIGNAL_SNAPSHOTS,
    MOMENTUM_COLLECTOR_VERSION,
    SNAPSHOT_BUCKET_SECONDS,
    SNAPSHOT_TTL_SECONDS,
    normalize_prospective_feature_collection,
)
from src.research_archive import DEFAULT_COHORT, RESEARCH_ARCHIVE_PATH, load_research_archive
from src.state_store import atomic_write_json, read_json, update_json


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ALPHA_PATH = ROOT / "data" / "alpha_discovery.json"
DEFAULT_REGISTRY_PATH = ROOT / "data" / "hypothesis_registry.json"
DEFAULT_REPORT_PATH = ROOT / "data" / "future_validation.json"
REGISTRY_SCHEMA_VERSION = 1
REPORT_SCHEMA_VERSION = 1
HORIZONS = ("5m", "15m", "30m", "60m")
PRIMARY_HORIZON = "60m"
MAX_HYPOTHESES_PER_FAMILY = 8
MIN_FUTURE_UNIQUE_MINTS = MIN_HOLDOUT_SAMPLED
MIN_POSITIVE_UTC_DAYS = 2
REGISTRY_STATUSES = frozenset({
    "DISCOVERY_ONLY",
    "REJECTED_DISCOVERY",
    "READY_FOR_FUTURE_VALIDATION",
    "FUTURE_VALIDATING",
    "FUTURE_FAILED",
    "FUTURE_PROMISING",
})
FUTURE_TO_REGISTRY_STATUS = {
    "FUTURE_INSUFFICIENT": "FUTURE_VALIDATING",
    "FUTURE_NEGATIVE": "FUTURE_FAILED",
    "FUTURE_PROMISING": "FUTURE_PROMISING",
}
CONDITION_OPERATORS = frozenset({
    "gt", "gte", "lt", "lte", "eq", "bucket_eq", "lte_feature",
})
PROSPECTIVE_FEATURES = frozenset({
    "snapshot_count",
    "span_seconds",
    "volume_delta",
    "volume_delta_percent",
    "buys_delta",
    "sells_delta",
    "net_buy_delta",
    "buy_sell_ratio_delta",
    "liquidity_delta",
    "price_delta_percent",
    "positive_volume_steps",
    "positive_buy_steps",
    "last_volume_step",
})
PROSPECTIVE_DERIVATION_VERSION = "momentum_future_features_v1"
PROSPECTIVE_DERIVATION_CONTRACT = {
    "version": PROSPECTIVE_DERIVATION_VERSION,
    "collection_schema_version": FEATURE_COLLECTION_SCHEMA_VERSION,
    "collector_version": MOMENTUM_COLLECTOR_VERSION,
    "snapshot_bucket_seconds": SNAPSHOT_BUCKET_SECONDS,
    "snapshot_ttl_seconds": SNAPSHOT_TTL_SECONDS,
    "maximum_snapshots": MAX_PRE_SIGNAL_SNAPSHOTS,
    "ordering": "snapshot_at_epoch_ascending",
    "endpoints": "first_and_last_normalized_snapshots",
    "features": {
        "snapshot_count": "normalized_snapshot_count",
        "span_seconds": "last_epoch-first_epoch",
        "volume_delta": "last_volume-first_volume",
        "volume_delta_percent": "100*volume_delta/first_volume_else_zero",
        "buys_delta": "last_buys-first_buys",
        "sells_delta": "last_sells-first_sells",
        "net_buy_delta": "(last_buys-last_sells)-(first_buys-first_sells)",
        "buy_sell_ratio_delta": (
            "last_buys/max(1,last_sells)-first_buys/max(1,first_sells)"
        ),
        "liquidity_delta": "last_liquidity-first_liquidity",
        "price_delta_percent": "100*(last_price-first_price)/first_price",
        "positive_volume_steps": "count(current_volume>previous_volume)",
        "positive_buy_steps": "count(current_buys>previous_buys)",
        "last_volume_step": "last_volume-previous_volume",
    },
}


def _finite_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _iso_timestamp(value: float) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def _canonical(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    )


def _definition_payload(hypothesis: dict[str, Any]) -> dict[str, Any]:
    return {
        key: hypothesis.get(key)
        for key in (
            "family",
            "version",
            "rationale",
            "required_features",
            "pre_signal_eligibility",
            "condition_definition",
            "discovery_source",
            "bucket_version",
            "feature_contract_digest",
            "alpha_source_schema_version",
            "alpha_source_bucket_version",
            "alpha_source_bucket_contract_digest",
            "prospective_derivation_version",
            "prospective_derivation_digest",
        )
    }


def deterministic_hypothesis_id(definition: dict[str, Any]) -> str:
    """시간과 결과 status에 영향받지 않는 결정적 hypothesis ID다."""
    family = str(definition.get("family") or "UNKNOWN").strip().upper()
    version = str(definition.get("version") or "0").strip()
    digest = hashlib.sha256(
        _canonical(_definition_payload(definition)).encode("utf-8")
    ).hexdigest()[:16]
    return f"{family.lower()}-v{version}-{digest}"


def _fingerprint(hypothesis: dict[str, Any]) -> str:
    frozen = {
        key: hypothesis.get(key)
        for key in (
            "hypothesis_id",
            "family",
            "version",
            "rationale",
            "required_features",
            "pre_signal_eligibility",
            "condition_definition",
            "created_at",
            "discovery_source",
            "discovery_data_end",
            "validation_start",
            "bucket_version",
            "feature_contract_digest",
            "alpha_source_schema_version",
            "alpha_source_bucket_version",
            "alpha_source_bucket_contract_digest",
            "prospective_derivation_version",
            "prospective_derivation_digest",
        )
    }
    return hashlib.sha256(_canonical(frozen).encode("utf-8")).hexdigest()


def _feature_contract_digest(family: str, features: Iterable[str]) -> str:
    specs = FEATURES_BY_FAMILY[family]
    contract = {
        feature: {
            "source_path": list(specs[feature].source_path),
            "scale": specs[feature].scale,
            "minimum": specs[feature].minimum,
            "maximum": specs[feature].maximum,
            "cuts": list(specs[feature].buckets.cuts),
            "labels": list(specs[feature].buckets.labels),
        }
        for feature in sorted(features)
    }
    return hashlib.sha256(_canonical(contract).encode("utf-8")).hexdigest()


def _alpha_bucket_contract_digest() -> str:
    return hashlib.sha256(
        _canonical(_bucket_configuration()).encode("utf-8")
    ).hexdigest()


def _prospective_derivation_digest() -> str:
    return hashlib.sha256(
        _canonical(PROSPECTIVE_DERIVATION_CONTRACT).encode("utf-8")
    ).hexdigest()


def derive_momentum_features(row: Any) -> dict[str, float]:
    """저장된 pre-signal raw snapshot만으로 파생값을 계산한다."""
    if not isinstance(row, dict):
        return {}
    collection = normalize_prospective_feature_collection(
        row.get("prospective_feature_collection"),
        signal_timestamp=row.get("signal_detected_at"),
    )
    snapshots = collection.get("pre_signal_snapshots")
    if not isinstance(snapshots, list) or len(snapshots) < 2:
        return {}
    ordered = sorted(
        (item for item in snapshots if isinstance(item, dict)),
        key=lambda item: float(item.get("snapshot_at_epoch", 0) or 0),
    )
    if len(ordered) < 2:
        return {}
    first, last = ordered[0], ordered[-1]

    def value(item: dict[str, Any], key: str) -> float | None:
        return _finite_number(item.get(key))

    required = ("volume_m5_usd", "buys_m5", "sells_m5", "liquidity_usd")
    if any(value(first, key) is None or value(last, key) is None for key in required):
        return {}
    first_volume = float(value(first, "volume_m5_usd") or 0)
    last_volume = float(value(last, "volume_m5_usd") or 0)
    first_buys = float(value(first, "buys_m5") or 0)
    last_buys = float(value(last, "buys_m5") or 0)
    first_sells = float(value(first, "sells_m5") or 0)
    last_sells = float(value(last, "sells_m5") or 0)
    first_liquidity = float(value(first, "liquidity_usd") or 0)
    last_liquidity = float(value(last, "liquidity_usd") or 0)
    timestamps = [float(item["snapshot_at_epoch"]) for item in ordered]
    volumes = [float(value(item, "volume_m5_usd") or 0) for item in ordered]
    buys = [float(value(item, "buys_m5") or 0) for item in ordered]
    first_price = value(first, "price_usd")
    last_price = value(last, "price_usd")
    result = {
        "snapshot_count": float(len(ordered)),
        "span_seconds": timestamps[-1] - timestamps[0],
        "volume_delta": last_volume - first_volume,
        "volume_delta_percent": (
            (last_volume - first_volume) / first_volume * 100
            if first_volume > 0 else 0.0
        ),
        "buys_delta": last_buys - first_buys,
        "sells_delta": last_sells - first_sells,
        "net_buy_delta": (last_buys - last_sells) - (first_buys - first_sells),
        "buy_sell_ratio_delta": (
            last_buys / max(1.0, last_sells)
            - first_buys / max(1.0, first_sells)
        ),
        "liquidity_delta": last_liquidity - first_liquidity,
        "positive_volume_steps": float(sum(
            current > previous for previous, current in zip(volumes, volumes[1:])
        )),
        "positive_buy_steps": float(sum(
            current > previous for previous, current in zip(buys, buys[1:])
        )),
        "last_volume_step": volumes[-1] - volumes[-2],
    }
    if first_price is not None and last_price is not None and first_price > 0:
        result["price_delta_percent"] = (
            (last_price - first_price) / first_price * 100
        )
    return result


def _prospective_definitions() -> list[dict[str, Any]]:
    definitions = [
        {
            "family": "MOMENTUM",
            "version": 1,
            "rationale": "거래량 증가와 순매수 증가가 함께 나타나는 지속 수요를 검증한다.",
            "required_features": ["volume_delta", "net_buy_delta"],
            "pre_signal_eligibility": "prospective_snapshot_count_at_least_2",
            "condition_definition": [
                {"feature": "volume_delta", "operator": "gt", "value": 0},
                {"feature": "net_buy_delta", "operator": "gt", "value": 0},
            ],
            "discovery_source": "prospective_pre_registered_v1",
        },
        {
            "family": "MOMENTUM",
            "version": 1,
            "rationale": "순매수와 매수/매도 비율의 동시 개선이 단일 score보다 설명력이 있는지 검증한다.",
            "required_features": ["net_buy_delta", "buy_sell_ratio_delta"],
            "pre_signal_eligibility": "prospective_snapshot_count_at_least_2",
            "condition_definition": [
                {"feature": "net_buy_delta", "operator": "gt", "value": 0},
                {"feature": "buy_sell_ratio_delta", "operator": "gt", "value": 0},
            ],
            "discovery_source": "prospective_pre_registered_v1",
        },
        {
            "family": "MOMENTUM",
            "version": 1,
            "rationale": "유동성과 거래량이 함께 확장되는 시장 구조를 검증한다.",
            "required_features": ["liquidity_delta", "volume_delta"],
            "pre_signal_eligibility": "prospective_snapshot_count_at_least_2",
            "condition_definition": [
                {"feature": "liquidity_delta", "operator": "gt", "value": 0},
                {"feature": "volume_delta", "operator": "gt", "value": 0},
            ],
            "discovery_source": "prospective_pre_registered_v1",
        },
        {
            "family": "MOMENTUM",
            "version": 1,
            "rationale": "매수자 증가는 있으나 가격 상승이 거래량 증가보다 과도하지 않은 후보를 검증한다.",
            "required_features": [
                "buys_delta", "price_delta_percent", "volume_delta_percent",
            ],
            "pre_signal_eligibility": "prospective_snapshot_count_at_least_2_with_price",
            "condition_definition": [
                {"feature": "buys_delta", "operator": "gt", "value": 0},
                {
                    "feature": "price_delta_percent",
                    "operator": "lte_feature",
                    "value": "volume_delta_percent",
                },
            ],
            "discovery_source": "prospective_pre_registered_v1",
        },
        {
            "family": "MOMENTUM",
            "version": 1,
            "rationale": "마지막 spike 한 번보다 여러 minute bucket의 거래량·매수 증가 지속성을 검증한다.",
            "required_features": [
                "snapshot_count", "positive_volume_steps", "positive_buy_steps",
            ],
            "pre_signal_eligibility": "prospective_snapshot_count_at_least_3",
            "condition_definition": [
                {"feature": "snapshot_count", "operator": "gte", "value": 3},
                {"feature": "positive_volume_steps", "operator": "gte", "value": 2},
                {"feature": "positive_buy_steps", "operator": "gte", "value": 2},
            ],
            "discovery_source": "prospective_pre_registered_v1",
        },
    ]
    for definition in definitions:
        definition["prospective_derivation_version"] = (
            PROSPECTIVE_DERIVATION_VERSION
        )
        definition["prospective_derivation_digest"] = (
            _prospective_derivation_digest()
        )
    return definitions


def _alpha_definitions(alpha_report: Any) -> Iterable[dict[str, Any]]:
    if not isinstance(alpha_report, dict):
        return []
    configuration = alpha_report.get("configuration")
    if (
        alpha_report.get("schema_version") != ALPHA_DISCOVERY_SCHEMA_VERSION
        or not isinstance(configuration, dict)
        or configuration.get("bucket_version") != BUCKET_VERSION
        or configuration.get("bucket_definitions") != _bucket_configuration()
        or configuration.get("automatic_trading_changes") is not False
        or alpha_report.get("automatic_trading_changes") is not False
    ):
        return []
    source_contract_digest = _alpha_bucket_contract_digest()
    result: list[dict[str, Any]] = []
    candidates = alpha_report.get("top_candidates")
    for candidate in candidates if isinstance(candidates, list) else []:
        if not isinstance(candidate, dict) or candidate.get("status") != "PROMISING":
            continue
        family = str(candidate.get("family") or "").upper()
        labels = candidate.get("labels")
        if family not in FEATURES_BY_FAMILY or not isinstance(labels, dict):
            continue
        if any(feature not in FEATURES_BY_FAMILY[family] for feature in labels):
            continue
        result.append({
            "family": family,
            "version": 1,
            "rationale": "Alpha Discovery V2의 고정 bucket 조건을 새 미래 cohort에서 독립 검증한다.",
            "required_features": sorted(labels),
            "pre_signal_eligibility": "stored_signal_snapshot",
            "condition_definition": [
                {"feature": feature, "operator": "bucket_eq", "value": label}
                for feature, label in sorted(labels.items())
            ],
            "discovery_source": str(candidate.get("candidate_id") or "alpha_discovery_v2"),
            "bucket_version": BUCKET_VERSION,
            "feature_contract_digest": _feature_contract_digest(
                family, labels,
            ),
            "alpha_source_schema_version": ALPHA_DISCOVERY_SCHEMA_VERSION,
            "alpha_source_bucket_version": BUCKET_VERSION,
            "alpha_source_bucket_contract_digest": source_contract_digest,
        })
    return result


def build_hypothesis_registry(
    rows: list[Any],
    *,
    alpha_report: dict[str, Any] | None = None,
    created_at: str | None = None,
    discovery_data_end: str | None = None,
    maximum_per_family: int = MAX_HYPOTHESES_PER_FAMILY,
) -> dict[str, Any]:
    """현재 discovery 끝을 고정하고 future-only registry를 만든다."""
    if not isinstance(rows, list):
        raise TypeError("registry rows must be a list")
    if not 1 <= int(maximum_per_family) <= MAX_HYPOTHESES_PER_FAMILY:
        raise ValueError("maximum_per_family must be between 1 and 8")
    timestamps = [
        timestamp for row in rows if isinstance(row, dict)
        if (timestamp := _signal_timestamp(row)) is not None
    ]
    frozen_at = created_at or datetime.now(timezone.utc).isoformat()
    frozen_epoch = _signal_timestamp({"signal_detected_at": frozen_at})
    if frozen_epoch is None:
        raise ValueError("created_at is invalid")
    cutoff_epoch = None
    if discovery_data_end is not None:
        cutoff_epoch = _signal_timestamp({
            "signal_detected_at": discovery_data_end,
        })
        if cutoff_epoch is None:
            raise ValueError("discovery_data_end is invalid")
        if timestamps and cutoff_epoch < max(timestamps):
            raise ValueError(
                "discovery_data_end must not precede any discovery input row"
            )
    if cutoff_epoch is None:
        cutoff_epoch = max(timestamps) if timestamps else frozen_epoch
    cutoff = _iso_timestamp(cutoff_epoch)
    validation_start = _iso_timestamp(max(cutoff_epoch, frozen_epoch))
    definitions = [*_prospective_definitions(), *_alpha_definitions(alpha_report)]
    counts: Counter[str] = Counter()
    hypotheses: list[dict[str, Any]] = []
    for definition in definitions:
        family = definition["family"]
        if counts[family] >= int(maximum_per_family):
            continue
        hypothesis = {
            **definition,
            "hypothesis_id": deterministic_hypothesis_id(definition),
            "created_at": frozen_at,
            "discovery_data_end": cutoff,
            "validation_start": validation_start,
            "status": "READY_FOR_FUTURE_VALIDATION",
        }
        hypothesis["definition_fingerprint"] = _fingerprint(hypothesis)
        hypotheses.append(hypothesis)
        counts[family] += 1
    registry = {
        "schema_version": REGISTRY_SCHEMA_VERSION,
        "created_at": frozen_at,
        "maximum_hypotheses_per_family": int(maximum_per_family),
        "selection_iteration": 1,
        "multiple_testing_warning": True,
        "automatic_trading_changes": False,
        "hypotheses": hypotheses,
    }
    validate_registry(registry)
    return registry


def validate_registry(registry: Any) -> None:
    """Registry 정의와 cutoff가 바뀌면 fail-closed한다."""
    if not isinstance(registry, dict) or registry.get("schema_version") != 1:
        raise RuntimeError("hypothesis registry schema is unsupported")
    if registry.get("automatic_trading_changes") is not False:
        raise RuntimeError("hypothesis registry must remain research-only")
    hypotheses = registry.get("hypotheses")
    if not isinstance(hypotheses, list):
        raise RuntimeError("hypothesis registry is malformed")
    ids: set[str] = set()
    counts: Counter[str] = Counter()
    for item in hypotheses:
        if not isinstance(item, dict):
            raise RuntimeError("hypothesis registry row is malformed")
        family = str(item.get("family") or "").upper()
        if family not in FEATURES_BY_FAMILY:
            raise RuntimeError("hypothesis family is unsupported")
        hypothesis_id = str(item.get("hypothesis_id") or "")
        if hypothesis_id != deterministic_hypothesis_id(item) or hypothesis_id in ids:
            raise RuntimeError("hypothesis identity is invalid or duplicated")
        if item.get("definition_fingerprint") != _fingerprint(item):
            raise RuntimeError("hypothesis immutable definition changed")
        if item.get("status") not in REGISTRY_STATUSES:
            raise RuntimeError("hypothesis status is unsupported")
        if _signal_timestamp({"signal_detected_at": item.get("discovery_data_end")}) is None:
            raise RuntimeError("hypothesis discovery cutoff is invalid")
        if _signal_timestamp({"signal_detected_at": item.get("validation_start")}) is None:
            raise RuntimeError("hypothesis validation start is invalid")
        conditions = item.get("condition_definition")
        required = item.get("required_features")
        if not str(item.get("rationale") or "").strip():
            raise RuntimeError("hypothesis rationale is required")
        if not isinstance(required, list) or not required:
            raise RuntimeError("hypothesis required_features is malformed")
        if (
            any(not isinstance(feature, str) or not feature for feature in required)
            or len(set(required)) != len(required)
        ):
            raise RuntimeError("hypothesis required_features is malformed")
        if not isinstance(conditions, list) or not conditions:
            raise RuntimeError("hypothesis conditions are malformed")
        discovery_end = _signal_timestamp({
            "signal_detected_at": item.get("discovery_data_end"),
        })
        validation_start = _signal_timestamp({
            "signal_detected_at": item.get("validation_start"),
        })
        if discovery_end is None or validation_start is None:
            raise RuntimeError("hypothesis validation boundary is invalid")
        if validation_start < discovery_end:
            raise RuntimeError("hypothesis validation starts before discovery ends")
        bucket_features: set[str] = set()
        prospective_features: set[str] = set()
        referenced_features: set[str] = set()
        for condition in conditions:
            if not isinstance(condition, dict):
                raise RuntimeError("hypothesis condition is malformed")
            feature = str(condition.get("feature") or "")
            operator = str(condition.get("operator") or "")
            if feature not in required or operator not in CONDITION_OPERATORS:
                raise RuntimeError("hypothesis condition is unsupported")
            if feature not in PROSPECTIVE_FEATURES and feature not in FEATURES_BY_FAMILY[family]:
                raise RuntimeError("hypothesis feature is unsupported")
            if feature in PROSPECTIVE_FEATURES and family != "MOMENTUM":
                raise RuntimeError("prospective feature family is unsupported")
            if feature in PROSPECTIVE_FEATURES:
                prospective_features.add(feature)
            referenced_features.add(feature)
            if operator == "bucket_eq":
                if feature not in FEATURES_BY_FAMILY[family]:
                    raise RuntimeError("hypothesis bucket feature is unsupported")
                if str(condition.get("value")) not in (
                    FEATURES_BY_FAMILY[family][feature].buckets.labels
                ):
                    raise RuntimeError("hypothesis bucket label is unsupported")
                bucket_features.add(feature)
            elif operator == "lte_feature":
                compared = str(condition.get("value") or "")
                if compared not in required:
                    raise RuntimeError("hypothesis compared feature is not required")
                referenced_features.add(compared)
            elif _finite_number(condition.get("value")) is None:
                raise RuntimeError("hypothesis condition value is invalid")
        if bucket_features and (
            item.get("bucket_version") != BUCKET_VERSION
            or item.get("feature_contract_digest")
            != _feature_contract_digest(family, bucket_features)
            or item.get("alpha_source_schema_version")
            != ALPHA_DISCOVERY_SCHEMA_VERSION
            or item.get("alpha_source_bucket_version") != BUCKET_VERSION
            or item.get("alpha_source_bucket_contract_digest")
            != _alpha_bucket_contract_digest()
        ):
            raise RuntimeError("hypothesis bucket semantics changed")
        if prospective_features and (
            item.get("prospective_derivation_version")
            != PROSPECTIVE_DERIVATION_VERSION
            or item.get("prospective_derivation_digest")
            != _prospective_derivation_digest()
        ):
            raise RuntimeError("hypothesis prospective semantics changed")
        if referenced_features != set(required):
            raise RuntimeError("hypothesis required features are not fully defined")
        ids.add(hypothesis_id)
        counts[family] += 1
    if any(count > MAX_HYPOTHESES_PER_FAMILY for count in counts.values()):
        raise RuntimeError("hypothesis family exceeds the candidate cap")


def _features(row: dict[str, Any], family: str, timestamp: float) -> dict[str, float]:
    prepared = PreparedRow(
        raw=row,
        family=family,
        mint=str(row.get("mint") or ""),
        timestamp=timestamp,
        stable_id=str(row.get("observation_id") or ""),
        digest=_signal_identity_digest(row),
    )
    result = {
        feature: value
        for feature in FEATURES_BY_FAMILY[family]
        if (value := _feature_value(prepared, feature)) is not None
    }
    if family == "MOMENTUM":
        result.update(derive_momentum_features(row))
    return result


def _condition_matches(
    family: str, features: dict[str, float], condition: dict[str, Any],
) -> bool:
    feature = str(condition["feature"])
    operator = str(condition["operator"])
    left = features.get(feature)
    if left is None:
        return False
    right_raw = condition.get("value")
    if operator == "bucket_eq":
        return assign_bucket(family, feature, left) == str(right_raw)
    if operator == "lte_feature":
        right = features.get(str(right_raw))
    else:
        right = _finite_number(right_raw)
    if right is None:
        return False
    return {
        "gt": left > right,
        "gte": left >= right,
        "lt": left < right,
        "lte": left <= right,
        "eq": left == right,
        "lte_feature": left <= right,
    }.get(operator, False)


def _hypothesis_matches(
    hypothesis: dict[str, Any], row: dict[str, Any], family: str, timestamp: float,
) -> tuple[bool, bool]:
    features = _features(row, family, timestamp)
    required = [str(value) for value in hypothesis["required_features"]]
    if any(feature not in features for feature in required):
        return False, True
    return all(
        _condition_matches(family, features, condition)
        for condition in hypothesis["condition_definition"]
    ), False


def _event_identity(row: dict[str, Any]) -> str:
    return str(row.get("observation_id") or "").strip() or _signal_identity_digest(row)


def _outcome_map(
    row: dict[str, Any],
) -> tuple[dict[str, float | None], int]:
    result = {horizon: None for horizon in HORIZONS}
    samples = row.get("samples")
    if not isinstance(samples, list):
        return result, 0
    seen: set[str] = set()
    conflicts: set[str] = set()
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        interval = str(sample.get("interval") or "")
        if interval not in result:
            continue
        if interval in seen:
            conflicts.add(interval)
            result[interval] = None
            continue
        seen.add(interval)
        result[interval] = _finite_number(sample.get("return_percent"))
    for interval in conflicts:
        result[interval] = None
    return result, len(conflicts)


def _deduplicated_events(rows: list[Any]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    by_id: dict[str, tuple[str, dict[str, Any]]] = {}
    conflicts: set[str] = set()
    exact_duplicates = 0
    malformed = 0
    for row in rows:
        if not isinstance(row, dict):
            malformed += 1
            continue
        identity = _event_identity(row)
        if not identity:
            malformed += 1
            continue
        digest = hashlib.sha256(_canonical(row).encode("utf-8")).hexdigest()
        prior = by_id.get(identity)
        if prior is None:
            by_id[identity] = (digest, row)
        elif prior[0] == digest:
            exact_duplicates += 1
        else:
            conflicts.add(identity)
    events = [row for identity, (_, row) in by_id.items() if identity not in conflicts]
    return events, {
        "malformed_row_count": malformed,
        "exact_duplicate_count": exact_duplicates,
        "conflicting_identity_count": len(conflicts),
    }


def _first_signal_per_mint(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for event in sorted(events, key=lambda item: (
        float(item["_timestamp"]), str(item["_identity"])
    )):
        selected.setdefault(str(event["_mint"]), event)
    return list(selected.values())


def _period_metrics(
    events: list[dict[str, Any]], horizon: str, period: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for event in events:
        outcome = event["_outcomes"].get(horizon)
        if outcome is None or not event["_trackable"]:
            continue
        instant = datetime.fromtimestamp(float(event["_timestamp"]), timezone.utc)
        key = (
            instant.date().isoformat()
            if period == "day"
            else f"{instant.isocalendar().year}-W{instant.isocalendar().week:02d}"
        )
        grouped[key].append(float(outcome))
    return [
        {
            "period": key,
            "sampled_count": len(values),
            "expectancy_percent": round(math.fsum(values) / len(values), 4),
        }
        for key, values in sorted(grouped.items())
    ]


def _metrics(events: list[dict[str, Any]], horizon: str) -> dict[str, Any]:
    trackable = [event for event in events if event["_trackable"]]
    outcomes = [
        event["_outcomes"].get(horizon) if event["_trackable"] else None
        for event in events
    ]
    values = [float(value) for value in outcomes if value is not None]
    performance = performance_metrics(outcomes, minimum_samples=1)
    wins = [value for value in values if value > 0]
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    top_removed = list(values)
    if top_removed:
        top_removed.remove(max(top_removed))
    positive_total = math.fsum(wins)
    day_split = _period_metrics(events, horizon, "day")
    week_split = _period_metrics(events, horizon, "week")
    elapsed_days = None
    if events:
        elapsed = max(event["_timestamp"] for event in events) - min(
            event["_timestamp"] for event in events
        )
        elapsed_days = max(1.0, elapsed / 86_400)
    return {
        "eligible_signal_count": len(events),
        "trackable_count": len(trackable),
        "completed_outcome_count": len(values),
        "unique_mint_count": len({event["_mint"] for event in events}),
        "coverage_percent": (
            round(len(values) / len(trackable) * 100, 4) if trackable else None
        ),
        "expectancy_percent": performance["expectancy_percent"],
        "profit_factor": performance["profit_factor"],
        "profit_factor_above_one": (
            performance["profit_factor"] > 1
            if performance["profit_factor"] is not None
            else bool(values and wins and not any(value < 0 for value in values))
        ),
        "median_return_percent": performance["median_roi_percent"],
        "win_rate_percent": performance["win_rate_percent"],
        "average_win_percent": performance["average_win_percent"],
        "average_loss_percent": performance["average_loss_percent"],
        "max_loss_percent": (
            round(min(value for value in values if value < 0), 4)
            if any(value < 0 for value in values) else None
        ),
        "drawdown_proxy_percent": round(max_drawdown, 4) if values else None,
        "largest_winner_contribution_percent": (
            round(max(wins) / positive_total * 100, 4) if positive_total > 0 else None
        ),
        "top_winner_removed_expectancy_percent": (
            round(math.fsum(top_removed) / len(top_removed), 4)
            if top_removed else None
        ),
        "positive_utc_day_count": sum(
            item["expectancy_percent"] > 0 for item in day_split
        ),
        "utc_day_split": day_split,
        "utc_week_split": week_split,
        "signal_density_per_day": (
            round(len(events) / elapsed_days, 4) if elapsed_days else None
        ),
    }


def fast_falsification_reasons(metrics: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    sampled = int(metrics.get("completed_outcome_count", 0) or 0)
    coverage = metrics.get("coverage_percent")
    if sampled < MIN_TOTAL_SAMPLED:
        reasons.append("SAMPLE_TOO_SMALL")
    if int(metrics.get("unique_mint_count", 0) or 0) < MIN_FUTURE_UNIQUE_MINTS:
        reasons.append("INSUFFICIENT_UNIQUE_MINTS")
    if coverage is None or coverage < MIN_TRACKABLE_COVERAGE_PERCENT:
        reasons.append("INSUFFICIENT_COVERAGE")
    if sampled and int(metrics.get("unique_mint_count", 0) or 0) <= 1:
        reasons.append("ONE_MINT_DEPENDENCY")
    expectancy = metrics.get("expectancy_percent")
    if expectancy is not None and expectancy <= 0:
        reasons.append("NEGATIVE_EXPECTANCY")
    if sampled and metrics.get("profit_factor_above_one") is not True:
        reasons.append("PROFIT_FACTOR_NOT_ABOVE_ONE")
    median = metrics.get("median_return_percent")
    if median is not None and median < 0:
        reasons.append("NEGATIVE_MEDIAN")
    removed = metrics.get("top_winner_removed_expectancy_percent")
    if sampled > 1 and (removed is None or removed <= 0):
        reasons.append("EXTREME_WINNER_DEPENDENCY")
    if sampled and int(metrics.get("positive_utc_day_count", 0) or 0) < MIN_POSITIVE_UTC_DAYS:
        reasons.append("ONLY_ONE_OR_ZERO_POSITIVE_DAY")
    return reasons


def _future_status(metrics: dict[str, Any], reasons: list[str]) -> str:
    insufficient = {
        "SAMPLE_TOO_SMALL", "INSUFFICIENT_UNIQUE_MINTS", "INSUFFICIENT_COVERAGE",
    }
    if insufficient & set(reasons):
        return "FUTURE_INSUFFICIENT"
    return "FUTURE_NEGATIVE" if reasons else "FUTURE_PROMISING"


def build_future_validation(
    rows: list[Any], registry: dict[str, Any], *, generated_at: str | None = None,
) -> dict[str, Any]:
    """저장된 canonical outcome을 여러 hypothesis가 RPC 없이 공유한다."""
    if not isinstance(rows, list):
        raise TypeError("future validation rows must be a list")
    validate_registry(registry)
    deduplicated, quality = _deduplicated_events(rows)
    hypotheses = registry["hypotheses"]
    cutoffs = {
        item["hypothesis_id"]: max(
            float(_signal_timestamp({"signal_detected_at": item["discovery_data_end"]}) or 0),
            float(_signal_timestamp({"signal_detected_at": item["validation_start"]}) or 0),
        )
        for item in hypotheses
    }
    events: list[dict[str, Any]] = []
    quality["conflicting_horizon_sample_count"] = 0
    for row in deduplicated:
        family = _signal_family(row)
        timestamp = _signal_timestamp(row)
        mint = str(row.get("mint") or "").strip()
        if family is None or timestamp is None or not mint:
            quality["malformed_row_count"] += 1
            continue
        outcomes, horizon_conflicts = _outcome_map(row)
        quality["conflicting_horizon_sample_count"] += horizon_conflicts
        events.append({
            **row,
            "_identity": _event_identity(row),
            "_family": family,
            "_timestamp": timestamp,
            "_mint": mint,
            "_outcomes": outcomes,
            "_trackable": (
                str(row.get("quote_status") or "").upper() == "EXECUTABLE"
            ),
        })
    events.sort(key=lambda item: (item["_timestamp"], item["_identity"]))
    membership_counts: Counter[str] = Counter()
    results: list[dict[str, Any]] = []
    for hypothesis in hypotheses:
        family = hypothesis["family"]
        cutoff = cutoffs[hypothesis["hypothesis_id"]]
        missing_feature_count = 0
        future_family_events = [
            event for event in events
            if event["_family"] == family and event["_timestamp"] > cutoff
        ]

        def select(
            source: list[dict[str, Any]], *, count_missing: bool,
        ) -> list[dict[str, Any]]:
            nonlocal missing_feature_count
            selected: list[dict[str, Any]] = []
            for event in source:
                matched, missing = _hypothesis_matches(
                    hypothesis, event, family, float(event["_timestamp"])
                )
                if count_missing:
                    missing_feature_count += int(missing)
                if matched:
                    selected.append(event)
            return selected

        event_rows = select(future_family_events, count_missing=True)
        unique_rows = select(
            _first_signal_per_mint(future_family_events), count_missing=False,
        )
        for event in event_rows:
            membership_counts[event["_identity"]] += 1
        horizons = {
            horizon: {
                "event_level": _metrics(event_rows, horizon),
                "first_signal_per_mint": _metrics(unique_rows, horizon),
            }
            for horizon in HORIZONS
        }
        primary = horizons[PRIMARY_HORIZON]["first_signal_per_mint"]
        reasons = fast_falsification_reasons(primary)
        future_status = _future_status(primary, reasons)
        results.append({
            "hypothesis_id": hypothesis["hypothesis_id"],
            "family": family,
            "registry_status": hypothesis["status"],
            "future_status": future_status,
            "registry_status_after_evaluation": FUTURE_TO_REGISTRY_STATUS[
                future_status
            ],
            "falsification_reasons": reasons,
            "missing_required_feature_count": missing_feature_count,
            "discovery_data_end": hypothesis["discovery_data_end"],
            "validation_start": hypothesis["validation_start"],
            "horizons": horizons,
        })
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "generated_at": generated_at,
        "primary_horizon": PRIMARY_HORIZON,
        "analyzed_horizons": list(HORIZONS),
        "input_summary": {
            "input_row_count": len(rows),
            "deduplicated_event_count": len(events),
            **quality,
            "canonical_outcome_read_count": len(events),
            "rpc_request_count": 0,
        },
        "hypothesis_count": len(hypotheses),
        "hypotheses": results,
        "shared_outcome_summary": {
            "multi_hypothesis_event_count": sum(
                count > 1 for count in membership_counts.values()
            ),
            "maximum_hypotheses_per_event": max(membership_counts.values(), default=0),
        },
        "automatic_trading_changes": False,
    }
    report["registry_source_version"] = int(registry.get("version", 0) or 0)
    report["evaluation_run_id"] = _evaluation_run_id(report)
    return report


def _evaluation_run_id(report: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical({
        "generated_at": report.get("generated_at"),
        "registry_source_version": report.get("registry_source_version"),
        "input_summary": report.get("input_summary"),
        "hypotheses": report.get("hypotheses"),
    }).encode("utf-8")).hexdigest()


def _load_rows(path: Path) -> list[Any]:
    if path.is_dir() or path.resolve() == RESEARCH_ARCHIVE_PATH.resolve():
        rows, _ = load_research_archive(
            archive_path=path,
            tracking_profile=DEFAULT_COHORT,
            maximum_rows=10_000,
        )
        return rows
    document = read_json(path, {})
    schema_version = document.get("schema_version") if isinstance(document, dict) else None
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or not MIN_SUPPORTED_OBSERVATION_SCHEMA_VERSION
        <= schema_version
        <= MAX_SUPPORTED_OBSERVATION_SCHEMA_VERSION
    ):
        raise RuntimeError("future validation input schema is unsupported")
    rows = document.get("observations") if isinstance(document, dict) else None
    if not isinstance(rows, list):
        raise RuntimeError("future validation input is malformed")
    return rows


def _save(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    def mutate(document: dict[str, Any]) -> None:
        document.clear()
        document.update(payload)

    _, saved = update_json(path, {"schema_version": payload["schema_version"]}, mutate)
    return saved


def registry_with_evaluation_statuses(
    registry: dict[str, Any], report: dict[str, Any],
) -> dict[str, Any]:
    """평가 결과만 mutable status에 반영하고 frozen 정의는 보존한다."""
    validate_registry(registry)
    source_version = int(registry.get("version", 0) or 0)
    if report.get("registry_source_version") != source_version:
        raise RuntimeError("future validation report registry version is stale")
    run_id = str(report.get("evaluation_run_id") or "")
    if len(run_id) != 64 or not hmac.compare_digest(
        run_id, _evaluation_run_id(report),
    ):
        raise RuntimeError("future validation report run identity is invalid")
    report_rows = report.get("hypotheses")
    if not isinstance(report_rows, list):
        raise RuntimeError("future validation report is malformed")
    statuses: dict[str, str] = {}
    for item in report_rows:
        if not isinstance(item, dict):
            raise RuntimeError("future validation report row is malformed")
        hypothesis_id = str(item.get("hypothesis_id") or "")
        status = str(item.get("registry_status_after_evaluation") or "")
        future_status = str(item.get("future_status") or "")
        horizons = item.get("horizons")
        primary_views = (
            horizons.get(PRIMARY_HORIZON)
            if isinstance(horizons, dict) else None
        )
        primary_metrics = (
            primary_views.get("first_signal_per_mint")
            if isinstance(primary_views, dict) else None
        )
        if not isinstance(primary_metrics, dict):
            raise RuntimeError("future validation primary metrics are malformed")
        expected_reasons = fast_falsification_reasons(primary_metrics)
        expected_future_status = _future_status(
            primary_metrics, expected_reasons,
        )
        if (
            not hypothesis_id
            or item.get("falsification_reasons") != expected_reasons
            or future_status != expected_future_status
            or FUTURE_TO_REGISTRY_STATUS[expected_future_status] != status
        ):
            raise RuntimeError("future validation report status is invalid")
        if hypothesis_id in statuses:
            raise RuntimeError("future validation report identity is duplicated")
        statuses[hypothesis_id] = status
    expected_ids = {
        str(item["hypothesis_id"]) for item in registry["hypotheses"]
    }
    if set(statuses) != expected_ids:
        raise RuntimeError("future validation report does not match registry")
    updated = copy.deepcopy(registry)
    for item in updated["hypotheses"]:
        item["status"] = statuses[str(item["hypothesis_id"])]
    updated["last_evaluated_at"] = report.get("generated_at")
    updated["last_evaluation_run_id"] = run_id
    updated["last_evaluation_source_version"] = source_version
    validate_registry(updated)
    return updated


def _publish_evaluation(
    registry_path: Path,
    output_path: Path,
    registry: dict[str, Any],
    report: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    updated = registry_with_evaluation_statuses(registry, report)
    expected_version = int(registry.get("version", 0) or 0)
    published_report = copy.deepcopy(report)
    published_report["registry_published_version"] = expected_version + 1
    frozen = {
        str(item["hypothesis_id"]): str(item["definition_fingerprint"])
        for item in registry["hypotheses"]
    }

    def mutate(document: dict[str, Any]) -> None:
        validate_registry(document)
        current = {
            str(item["hypothesis_id"]): str(item["definition_fingerprint"])
            for item in document["hypotheses"]
        }
        if current != frozen:
            raise RuntimeError("hypothesis registry changed during evaluation")
        document.clear()
        document.update(updated)
        # Registry lock 안에서 report를 먼저 publish한다. 경쟁 실행의 loser는
        # expected_version 검사에서 mutator 진입 전에 실패하므로 덮어쓰지 못한다.
        atomic_write_json(output_path, published_report)

    _, saved = update_json(
        registry_path,
        {"schema_version": REGISTRY_SCHEMA_VERSION},
        mutate,
        expected_version=expected_version,
    )
    return saved, published_report


def _validate_cli_paths(
    *, input_path: Path, alpha_path: Path, registry_path: Path, output_path: Path,
) -> None:
    sources = {input_path.resolve(), alpha_path.resolve()}
    registry = registry_path.resolve()
    output = output_path.resolve()
    if registry in sources or output in sources or registry == output:
        raise ValueError("research outputs must differ from all inputs and each other")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run research-only Future Shadow")
    parser.add_argument("--input", type=Path, default=RESEARCH_ARCHIVE_PATH)
    parser.add_argument("--alpha", type=Path, default=DEFAULT_ALPHA_PATH)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_REPORT_PATH)
    parser.add_argument("--discovery-data-end")
    args = parser.parse_args(argv)
    _validate_cli_paths(
        input_path=args.input,
        alpha_path=args.alpha,
        registry_path=args.registry,
        output_path=args.output,
    )
    rows = _load_rows(args.input)
    if args.registry.exists():
        registry = read_json(args.registry, {})
        validate_registry(registry)
    else:
        alpha = read_json(args.alpha, {}) if args.alpha.exists() else {}
        registry = build_hypothesis_registry(
            rows,
            alpha_report=alpha,
            discovery_data_end=args.discovery_data_end,
        )
        registry = _save(args.registry, registry)
    report = build_future_validation(
        rows, registry, generated_at=datetime.now(timezone.utc).isoformat(),
    )
    registry, report = _publish_evaluation(
        args.registry, args.output, registry, report,
    )
    counts = Counter(item["future_status"] for item in report["hypotheses"])
    print("FUTURE_VALIDATION " + json.dumps({
        "hypotheses": report["hypothesis_count"],
        "statuses": dict(sorted(counts.items())),
        "rpc_requests": 0,
        "automatic_trading_changes": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
