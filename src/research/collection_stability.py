"""Research 수집 lifecycle과 RPC/WebSocket 상태를 secret 없이 진단한다."""

from __future__ import annotations

import argparse
import json
import math
import time
from collections import Counter
from pathlib import Path
from typing import Any, Sequence

from src import state_store
from src.observation_analysis import RESEARCH_HORIZONS, build_research_metrics
from src.observation_tracker import (
    DISCOVERY_PROCESSING_INTERRUPTED,
    OBSERVATION_PATH,
    empty_observations,
)
from src.solana_rpc import (
    RpcProvider,
    provider_configs_from_env,
    provider_states,
)


PROVIDER_COUNTERS = (
    "request_count",
    "success_count",
    "failure_count",
    "rate_limit_count",
    "circuit_open_count",
)
ALCHEMY_REVIEW_MIN_SIGNALS = 50
ALCHEMY_REVIEW_MIN_PUBLIC_REQUESTS = 100
ALCHEMY_REVIEW_EXHAUSTED_PERCENT = 25.0
ALCHEMY_REVIEW_MIN_EXHAUSTED = 5
ALCHEMY_REVIEW_MIN_PUBLIC_SUCCESS_PERCENT = 90.0


def _finite(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _counter(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError, OverflowError):
        return 0


def _decision_reasons(row: dict[str, Any]) -> set[str]:
    reasons = row.get("decision_reasons")
    if not isinstance(reasons, list):
        return set()
    return {
        str(reason).strip().upper()
        for reason in reasons
        if str(reason).strip()
    }


def build_research_lifecycle(
    rows: list[Any],
    *,
    since_epoch: float = 0.0,
) -> dict[str, Any]:
    """지정 시점 이후 Research V1 event의 funnel과 horizon 품질을 계산한다."""
    selected = [
        row for row in rows
        if isinstance(row, dict)
        and row.get("tracking_profile") == "research_v1_60m"
        and (_finite(row.get("started_at_epoch")) or 0.0) >= since_epoch
    ]
    research = build_research_metrics(selected)
    status_counts = Counter(
        str(row.get("status") or "UNKNOWN").upper() for row in selected
    )
    horizon_rows: dict[str, Any] = {}
    for horizon in RESEARCH_HORIZONS:
        source = research["horizons"][horizon]
        sample_record_count = sum(
            any(
                isinstance(sample, dict)
                and str(sample.get("interval")) == horizon
                for sample in (
                    row.get("samples")
                    if isinstance(row.get("samples"), list)
                    else []
                )
            )
            for row in selected
        )
        horizon_rows[horizon] = {
            key: source.get(key)
            for key in (
                "signal_count",
                "sampled_count",
                "missing_count",
                "coverage_rate_percent",
                "outcome_trackable_count",
                "outcome_untrackable_count",
                "trackable_coverage_rate_percent",
                "missing_reasons",
                "lag_sample_count",
                "median_sample_lag_seconds",
                "p90_sample_lag_seconds",
                "p95_sample_lag_seconds",
                "max_sample_lag_seconds",
            )
        }
        horizon_rows[horizon]["sample_record_count"] = sample_record_count

    completed_60m = sum(
        str(row.get("status") or "").upper() == "COMPLETE"
        and any(
            isinstance(sample, dict) and sample.get("interval") == "60m"
            for sample in (
                row.get("samples") if isinstance(row.get("samples"), list) else []
            )
        )
        for row in selected
    )
    return {
        "since_epoch": since_epoch,
        "signal_count": len(selected),
        "analyzer_success_count": sum(
            bool(row.get("analysis_completed_at"))
            and str(row.get("decision_status") or "").upper() != "DISCOVERED"
            for row in selected
        ),
        "executable_count": sum(
            str(row.get("quote_status") or "").upper() == "EXECUTABLE"
            for row in selected
        ),
        "pending_trackable_count": sum(
            str(row.get("quote_status") or "").upper() == "EXECUTABLE"
            and str(row.get("status") or "").upper() == "PENDING"
            for row in selected
        ),
        "rpc_all_providers_exhausted_count": sum(
            "RPC_ALL_PROVIDERS_EXHAUSTED" in _decision_reasons(row)
            for row in selected
        ),
        "processing_failed_count": sum(
            str(row.get("quote_status") or "").upper() == "PROCESSING_FAILED"
            for row in selected
        ),
        "completed_60m_count": completed_60m,
        "status_counts": dict(sorted(status_counts.items())),
        "horizons": horizon_rows,
        "missing_60m_reasons": horizon_rows["60m"]["missing_reasons"],
    }


def provider_baseline(
    providers: Sequence[RpcProvider] | None = None,
) -> dict[str, Any]:
    configured = tuple(providers or provider_configs_from_env())
    states = provider_states(configured)
    return {
        "created_at_epoch": time.time(),
        "providers": {
            name: {
                key: _counter(state.get(key))
                for key in PROVIDER_COUNTERS
            }
            for name, state in states.items()
        },
    }


def build_provider_report(
    *,
    baseline: dict[str, Any] | None = None,
    providers: Sequence[RpcProvider] | None = None,
) -> dict[str, Any]:
    configured = tuple(providers or provider_configs_from_env())
    baseline_states = (
        baseline.get("providers", {}) if isinstance(baseline, dict) else {}
    )
    report: dict[str, Any] = {}
    for name, state in provider_states(configured).items():
        previous = baseline_states.get(name, {})
        counters = {key: _counter(state.get(key)) for key in PROVIDER_COUNTERS}
        deltas = {
            f"{key}_delta": counters[key] - _counter(previous.get(key))
            for key in PROVIDER_COUNTERS
        }
        completed_delta = deltas["success_count_delta"] + deltas["failure_count_delta"]
        report[name] = {
            "enabled": bool(state.get("enabled")),
            **counters,
            "success_rate_percent": state.get("success_rate_percent"),
            **deltas,
            "window_success_rate_percent": (
                round(deltas["success_count_delta"] / completed_delta * 100, 4)
                if completed_delta > 0 else None
            ),
            "consecutive_failures": _counter(state.get("consecutive_failures")),
            "circuit_state": str(state.get("circuit_state") or "UNKNOWN"),
            "last_success_at_epoch": state.get("last_success_at_epoch"),
            "last_failure_at_epoch": state.get("last_failure_at_epoch"),
            "last_failure_category": state.get("last_failure_category"),
        }
    return report


def assess_alchemy_need(
    lifecycle: dict[str, Any],
    providers: dict[str, Any],
) -> dict[str, Any]:
    """계정을 자동 활성화하지 않는 보수적인 운영 review gate다."""
    signals = _counter(lifecycle.get("signal_count"))
    exhausted = _counter(lifecycle.get("rpc_all_providers_exhausted_count"))
    public = providers.get("solana_public", {})
    public_requests = _counter(public.get("request_count_delta"))
    public_success_rate = _finite(public.get("window_success_rate_percent"))
    exhausted_rate = round(exhausted / signals * 100, 4) if signals else None
    enough_window = (
        signals >= ALCHEMY_REVIEW_MIN_SIGNALS
        and public_requests >= ALCHEMY_REVIEW_MIN_PUBLIC_REQUESTS
    )
    exhausted_dominates = (
        exhausted >= ALCHEMY_REVIEW_MIN_EXHAUSTED
        and exhausted_rate is not None
        and exhausted_rate >= ALCHEMY_REVIEW_EXHAUSTED_PERCENT
    )
    public_unstable = (
        public_success_rate is not None
        and public_success_rate < ALCHEMY_REVIEW_MIN_PUBLIC_SUCCESS_PERCENT
    )
    recommend = enough_window and exhausted_dominates and public_unstable
    return {
        "recommend_alchemy_free": recommend,
        "decision": "ALCHEMY_FREE_NEEDED" if recommend else "NOT_YET_NEEDED",
        "window_sufficient": enough_window,
        "signal_count": signals,
        "public_request_count": public_requests,
        "rpc_all_providers_exhausted_count": exhausted,
        "rpc_all_providers_exhausted_rate_percent": exhausted_rate,
        "public_success_rate_percent": public_success_rate,
        "criteria": {
            "minimum_signals": ALCHEMY_REVIEW_MIN_SIGNALS,
            "minimum_public_requests": ALCHEMY_REVIEW_MIN_PUBLIC_REQUESTS,
            "minimum_exhausted_count": ALCHEMY_REVIEW_MIN_EXHAUSTED,
            "exhausted_rate_percent": ALCHEMY_REVIEW_EXHAUSTED_PERCENT,
            "public_success_rate_below_percent": (
                ALCHEMY_REVIEW_MIN_PUBLIC_SUCCESS_PERCENT
            ),
        },
        "automatic_provider_activation": False,
    }


def build_collection_report(
    *,
    since_epoch: float = 0.0,
    baseline: dict[str, Any] | None = None,
) -> dict[str, Any]:
    observations = state_store.read_json(OBSERVATION_PATH, empty_observations())
    rows = observations.get("observations")
    if not isinstance(rows, list):
        raise RuntimeError("signal observation rows are malformed")
    lifecycle = build_research_lifecycle(rows, since_epoch=since_epoch)
    providers = build_provider_report(baseline=baseline)
    metrics_document = state_store.read_json(
        state_store.GLOBAL_METRICS_PATH,
        {"metrics": {}},
    )
    metrics = metrics_document.get("metrics")
    if not isinstance(metrics, dict):
        raise RuntimeError("global metrics are malformed")
    websocket = {
        key: metrics.get(key)
        for key in (
            "wallet_ws_state",
            "wallet_ws_endpoint_kind",
            "wallet_ws_subscription_method",
            "wallet_ws_mode",
            "wallet_ws_connected_at",
            "wallet_ws_subscribed_at",
            "wallet_ws_last_success_at",
            "wallet_ws_reconnect_count",
            "wallet_ws_consecutive_failures",
            "wallet_ws_last_failure_at",
            "wallet_ws_last_failure_category",
            "wallet_ws_enhanced_fallback_reason",
        )
    }
    return {
        "generated_at_epoch": time.time(),
        "research": lifecycle,
        "providers": providers,
        "websocket": websocket,
        "reconciliation": {
            "reason": DISCOVERY_PROCESSING_INTERRUPTED,
            "terminal_count": sum(
                DISCOVERY_PROCESSING_INTERRUPTED in _decision_reasons(row)
                for row in rows if isinstance(row, dict)
            ),
            "last_count": metrics.get("discovery_reconciliation_last_count"),
            "last_at": metrics.get("discovery_reconciliation_last_at"),
        },
        "alchemy_assessment": assess_alchemy_need(lifecycle, providers),
    }


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--since-epoch", type=float, default=None)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--write-baseline", type=Path)
    return parser.parse_args()


def main() -> None:
    args = _arguments()
    if args.write_baseline:
        state_store.atomic_write_json(args.write_baseline, provider_baseline())
        print(f"RPC_BASELINE_WRITTEN path={args.write_baseline}")
        return
    baseline = (
        state_store.read_json(args.baseline, {}) if args.baseline else None
    )
    since_epoch = (
        float(args.since_epoch)
        if args.since_epoch is not None
        else float(baseline.get("created_at_epoch", 0) or 0)
        if isinstance(baseline, dict)
        else 0.0
    )
    print(json.dumps(
        build_collection_report(since_epoch=since_epoch, baseline=baseline),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ))


if __name__ == "__main__":
    main()
