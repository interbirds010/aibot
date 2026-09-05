"""기존 manual observation 동안 PM2 monitor memory를 bounded sampling한다."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any

from src import state_store


MONITOR_NAME = "aibot-monitor"
MIN_DURATION_SECONDS = 60
MAX_DURATION_SECONDS = 1_800
DEFAULT_INTERVAL_SECONDS = 10.0
SERIES_KEYS = (
    "asyncio_live_task_count",
    "signal_task_count",
    "signal_done_task_count",
    "shadow_signal_task_count",
    "shadow_signal_done_task_count",
    "analyzer_cache_entry_count",
    "analyzer_flight_task_count",
    "analyzer_done_flight_task_count",
    "signature_window_size",
    "signature_window_max_size",
    "whale_history_key_count",
    "whale_history_entry_count",
    "market_entry_cooldown_count",
    "market_shadow_cooldown_count",
    "ws_metric_source_bucket_count",
    "ws_failure_reason_bucket_count",
    "transaction_payload_count",
)
COUNTER_KEYS = (
    "wallet_ws_notification_process_count",
    "wallet_ws_dex_log_match_process_count",
    "wallet_ws_smart_money_candidate_process_count",
    "wallet_ws_research_discovered_process_count",
    "monitor_signal_task_created_count",
    "monitor_signal_task_completed_count",
    "monitor_shadow_signal_task_created_count",
    "monitor_shadow_signal_task_completed_count",
)


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _integer(value: Any) -> int | None:
    result = _number(value)
    return int(result) if result is not None else None


def _percentile(values: list[int], percentile: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = (len(ordered) - 1) * percentile
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    return round(
        ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)
    )


def _series(values: list[int]) -> dict[str, int | None]:
    return {
        "start": values[0] if values else None,
        "max": max(values) if values else None,
        "end": values[-1] if values else None,
    }


def _counter_delta(samples: list[dict[str, Any]], key: str) -> int:
    total = 0
    previous: int | None = None
    previous_pid: int | None = None
    for sample in samples:
        current = _integer(sample.get(key))
        pid = _integer(sample.get("pid"))
        if current is None:
            continue
        if previous is None:
            previous = current
            previous_pid = pid
            continue
        if pid != previous_pid or current < previous:
            total += current
        else:
            total += current - previous
        previous = current
        previous_pid = pid
    return total


def _final_pid_slope(samples: list[dict[str, Any]]) -> float | None:
    if len(samples) < 2:
        return None
    final_pid = samples[-1].get("pid")
    segment = [
        sample for sample in samples
        if sample.get("pid") == final_pid
        and _number(sample.get("rss_bytes")) is not None
    ]
    if len(segment) < 2:
        return None
    started = _number(segment[0].get("sampled_at_epoch"))
    ended = _number(segment[-1].get("sampled_at_epoch"))
    start_rss = _number(segment[0].get("rss_bytes"))
    end_rss = _number(segment[-1].get("rss_bytes"))
    if None in {started, ended, start_rss, end_rss} or ended <= started:
        return None
    return round(
        ((end_rss - start_rss) / 1024 / 1024) / ((ended - started) / 60),
        4,
    )


def summarize_memory_samples(
    samples: list[dict[str, Any]], *, deployed_sha: str
) -> dict[str, Any]:
    rss_values = [
        value
        for sample in samples
        if (value := _integer(sample.get("rss_bytes"))) is not None
    ]
    vms_values = [
        value
        for sample in samples
        if (value := _integer(sample.get("vms_bytes"))) is not None
    ]
    available_values = [
        value
        for sample in samples
        if (value := _integer(sample.get("system_available_memory_bytes")))
        is not None
    ]
    ceilings = [
        value
        for sample in samples
        if (value := _integer(sample.get("rss_ceiling_bytes"))) is not None
    ]
    restarts = [
        value
        for sample in samples
        if (value := _integer(sample.get("restart_count"))) is not None
    ]
    sha = str(deployed_sha).strip().lower()
    if re.fullmatch(r"[0-9a-f]{40}", sha) is None:
        sha = "UNKNOWN"
    ceiling = ceilings[-1] if ceilings else None
    maximum_rss = max(rss_values) if rss_values else None
    series: dict[str, dict[str, int | None]] = {}
    for key in SERIES_KEYS:
        metric_key = f"monitor_{key}"
        values = [
            value
            for sample in samples
            if (value := _integer(sample.get(metric_key))) is not None
        ]
        series[key] = _series(values)
    return {
        "schema_version": 1,
        "deployed_sha": sha,
        "sample_count": len(samples),
        "pid_count": len({sample.get("pid") for sample in samples if sample.get("pid")}),
        "restart_delta": (
            max(restarts) - min(restarts) if restarts else None
        ),
        "rss_bytes": {
            "start": rss_values[0] if rss_values else None,
            "p50": _percentile(rss_values, 0.5),
            "p95": _percentile(rss_values, 0.95),
            "max": maximum_rss,
            "end": rss_values[-1] if rss_values else None,
            "final_pid_slope_mib_per_min": _final_pid_slope(samples),
        },
        "vms_bytes": {
            "start": vms_values[0] if vms_values else None,
            "p95": _percentile(vms_values, 0.95),
            "max": max(vms_values) if vms_values else None,
            "end": vms_values[-1] if vms_values else None,
        },
        "rss_ceiling_bytes": ceiling,
        "minimum_rss_headroom_bytes": (
            ceiling - maximum_rss
            if ceiling is not None and maximum_rss is not None else None
        ),
        "minimum_system_available_memory_bytes": (
            min(available_values) if available_values else None
        ),
        "series": series,
        "counter_deltas": {
            key: _counter_delta(samples, key) for key in COUNTER_KEYS
        },
        "last_memory_phase_stats": (
            samples[-1].get("monitor_memory_phase_stats", {})
            if samples else {}
        ),
        "websocket_mode_at_end": (
            samples[-1].get("wallet_ws_mode") if samples else None
        ),
        "research_pending_at_end": (
            samples[-1].get("pending_research_observations")
            if samples else None
        ),
    }


def _global_metrics() -> dict[str, Any]:
    with state_store.exclusive_file_lock(state_store.GLOBAL_METRICS_PATH):
        document = state_store.read_json(
            state_store.GLOBAL_METRICS_PATH,
            {"schema_version": 2, "version": 0, "metrics": {}},
        )
    metrics = document.get("metrics")
    return metrics if isinstance(metrics, dict) else {}


def collect_memory_sample() -> dict[str, Any]:
    result = subprocess.run(
        ["pm2", "jlist"],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
        env={**os.environ, "PM2_HOME": os.getenv("PM2_HOME", "/home/deploy/.pm2")},
    )
    if result.returncode != 0:
        raise RuntimeError("pm2 jlist failed")
    apps = json.loads(result.stdout)
    app = next(
        (
            item for item in apps
            if isinstance(item, dict) and item.get("name") == MONITOR_NAME
        ),
        {},
    )
    environment = app.get("pm2_env")
    environment = environment if isinstance(environment, dict) else {}
    monitor = app.get("monit")
    monitor = monitor if isinstance(monitor, dict) else {}
    metrics = _global_metrics()
    sample: dict[str, Any] = {
        "sampled_at_epoch": time.time(),
        "pid": _integer(app.get("pid")),
        "restart_count": _integer(environment.get("restart_time")),
        "status": str(environment.get("status") or "UNKNOWN"),
        "rss_bytes": _integer(monitor.get("memory")),
        "rss_ceiling_bytes": _integer(environment.get("max_memory_restart")),
    }
    for key, value in metrics.items():
        if key.startswith("monitor_") or key in {
            "wallet_ws_mode",
            "pending_research_observations",
            "wallet_ws_notification_process_count",
            "wallet_ws_dex_log_match_process_count",
            "wallet_ws_smart_money_candidate_process_count",
            "wallet_ws_research_discovered_process_count",
        }:
            sample[key] = value
    sample["vms_bytes"] = metrics.get("monitor_memory_vms_bytes")
    sample["system_available_memory_bytes"] = metrics.get(
        "monitor_memory_system_available_bytes"
    )
    return sample


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, required=True)
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--deployed-sha", required=True)
    args = parser.parse_args()
    if not MIN_DURATION_SECONDS <= args.duration <= MAX_DURATION_SECONDS:
        raise SystemExit("duration must be between 60 and 1800 seconds")
    interval = min(60.0, max(5.0, float(args.interval)))
    deadline = time.monotonic() + args.duration
    samples: list[dict[str, Any]] = []
    while True:
        samples.append(collect_memory_sample())
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(interval, remaining))
    report = summarize_memory_samples(samples, deployed_sha=args.deployed_sha)
    print(
        "MONITOR_MEMORY_PROFILE "
        + json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
