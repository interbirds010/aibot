"""Offline benchmark for bounded state-lock follow-up decisions.

This script creates synthetic JSON documents in a temporary directory. It never
reads or writes production state and emits aggregate timings only.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import patch

from src import observation_tracker, state_store


MIB = 1024 * 1024


def _synthetic_document(*, rows: int, target_bytes: int) -> dict[str, Any]:
    filler_size = max(256, (target_bytes // max(1, rows)) - 512)
    observations = []
    for index in range(rows):
        observations.append({
            "observation_id": f"synthetic-{index}",
            "status": "PENDING",
            "decision_status": "APPROVED",
            "quote_status": "EXECUTABLE",
            "started_at_epoch": float(index),
            "samples": [],
            "sample_attempts": {},
            "archive_schema_version": None,
            "archived_at": None,
            "synthetic_padding": "x" * filler_size,
        })
    return {
        "schema_version": 5,
        "version": 1,
        "updated_at": "2026-09-28T00:00:00+00:00",
        "observations": observations,
    }


def _encoded_size(document: dict[str, Any]) -> int:
    return len(json.dumps(
        document,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")) + 1


def _observation_fixture() -> dict[str, Any]:
    document = _synthetic_document(rows=1_000, target_bytes=4 * MIB)
    for index, row in enumerate(document["observations"]):
        row.update({
            "observation_id": f"signature-{index}:wallet-{index}:mint-{index}",
            "mint": f"mint-{index}",
            "route_type": "B",
            "entry_cost_lamports": 1_000,
            "token_amount_raw": 500,
            "token_decimals": 6,
            "tracking_profile": "research_v1_60m",
            "analysis_completed_at": "2026-09-28T00:00:01+00:00",
            "entry_quote_at": "2026-09-28T00:00:02+00:00",
            "decision_reasons": [],
            "paper_experiment_status": "NOT_ELIGIBLE",
            "paper_experiment_position_id": None,
            "candidate_v2_paper_status": "NOT_ELIGIBLE",
            "candidate_v2_position_id": None,
        })
    observation_tracker.migrate_observation_document(document)
    document["version"] = 1
    return document


def _run_observation_operation(operation: str, path: Path) -> None:
    rows = state_store.read_json(path, {})["observations"]
    if operation == "observation_decision":
        rows[500]["quote_status"] = "NOT_REQUESTED"
        state_store.atomic_write_json(path, {**state_store.read_json(path, {}), "observations": rows})
        asyncio.run(observation_tracker.record_observation_decision(
            mint="mint-500",
            route_type="B",
            source_wallet="wallet-500",
            source_signature="signature-500",
            safety_score=100,
            entry_cost_lamports=1_000,
            token_amount_raw=500,
            token_decimals=6,
            entry_price_impact_pct=0.1,
            exit_price_impact_pct=0.2,
            expected_slippage_bps=100,
            dex_momentum_score=95.0,
            signal_detected_at="2026-09-28T00:00:00+00:00",
            analysis_completed_at="2026-09-28T00:00:01+00:00",
            entry_quote_at="2026-09-28T00:00:02+00:00",
            entry_latency_ms=2_000,
        ))
    elif operation == "sample_batch":
        observation_tracker.record_sample_batch([
            observation_tracker.SampleResult(
                row["observation_id"], "1m", 1_100
            )
            for row in rows[:20]
        ])
    elif operation == "archive_marker_persist":
        observation_tracker._persist_archive_markers([
            {
                "observation_id": row["observation_id"],
                "archive_schema_version": 1,
                "archived_at": "2026-09-28T00:00:00+00:00",
            }
            for row in rows[:100]
        ])
    elif operation == "candidate_discovery":
        asyncio.run(observation_tracker.record_candidate_discovery(
            mint="new-mint",
            route_type="B",
            source_wallet="new-wallet",
            source_signature="new-signature",
            token_amount_raw=500,
            token_decimals=6,
            signal_detected_at="2026-09-28T00:00:00+00:00",
        ))
    elif operation == "startup_reconciliation":
        rows[500].update({
            "status": "DISCOVERED",
            "decision_status": "DISCOVERED",
            "quote_status": "NOT_REQUESTED",
            "analysis_completed_at": None,
            "entry_quote_at": None,
            "paper_experiment_status": "NOT_EVALUATED",
            "candidate_v2_paper_status": "NOT_EVALUATED",
            "started_at_epoch": 0.0,
        })
        state_store.atomic_write_json(path, {**state_store.read_json(path, {}), "observations": rows})
        observation_tracker.reconcile_interrupted_discoveries(
            now_epoch=10_000.0,
            grace_seconds=60.0,
        )
    else:
        raise ValueError(operation)


def operation_costs(repetitions: int) -> dict[str, Any]:
    base = _observation_fixture()
    results: dict[str, Any] = {}
    operations = (
        "observation_decision",
        "sample_batch",
        "archive_marker_persist",
        "candidate_discovery",
        "startup_reconciliation",
    )
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        original_observation_path = observation_tracker.OBSERVATION_PATH
        for operation in operations:
            samples: list[dict[str, float]] = []
            size_bytes = 0
            for repetition in range(repetitions):
                path = root / f"{operation}-{repetition}.json"
                state_store.atomic_write_json(path, base)
                observation_tracker.OBSERVATION_PATH = path
                metrics: dict[str, int] = {}
                stages: dict[str, int] = {}
                original_lock = state_store.exclusive_file_lock
                original_read = state_store.read_json
                original_write = state_store.atomic_write_json

                @contextmanager
                def measured_lock(*args: Any, **kwargs: Any) -> Iterator[Any]:
                    metrics["wait_started"] = time.perf_counter_ns()
                    with original_lock(*args, **kwargs) as attempt:
                        metrics["acquired"] = time.perf_counter_ns()
                        try:
                            yield attempt
                        finally:
                            metrics["hold_finished"] = time.perf_counter_ns()

                def measured_read(*args: Any, **kwargs: Any) -> dict[str, Any]:
                    metrics["parse_started"] = time.perf_counter_ns()
                    document = original_read(*args, **kwargs)
                    metrics["parse_finished"] = time.perf_counter_ns()
                    return document

                def measured_write(*args: Any, **kwargs: Any) -> None:
                    metrics["mutation_finished"] = time.perf_counter_ns()
                    existing_observer = kwargs.get("lifecycle_observer")

                    def observe(stage: str, size: int) -> None:
                        stages[stage] = time.perf_counter_ns()
                        if existing_observer is not None:
                            existing_observer(stage, size)

                    kwargs["lifecycle_observer"] = observe
                    original_write(*args, **kwargs)

                with (
                    patch.object(state_store, "exclusive_file_lock", measured_lock),
                    patch.object(state_store, "read_json", measured_read),
                    patch.object(state_store, "atomic_write_json", measured_write),
                ):
                    _run_observation_operation(operation, path)
                size_bytes = path.stat().st_size
                samples.append({
                    "lock_wait_ms": (
                        metrics["acquired"] - metrics["wait_started"]
                    ) / 1_000_000,
                    "lock_hold_ms": (
                        metrics["hold_finished"] - metrics["acquired"]
                    ) / 1_000_000,
                    "parse_ms": (
                        metrics["parse_finished"] - metrics["parse_started"]
                    ) / 1_000_000,
                    "mutation_ms": (
                        metrics["mutation_finished"] - metrics["parse_finished"]
                    ) / 1_000_000,
                    "serialize_temp_write_ms": (
                        stages["serialized"] - stages["serialize"]
                    ) / 1_000_000,
                    "flush_fsync_ms": (
                        stages["flushed"] - stages["serialized"]
                    ) / 1_000_000,
                    "replace_ms": (
                        stages["replaced"] - stages["flushed"]
                    ) / 1_000_000,
                })
            results[operation] = {
                "document_bytes": size_bytes,
                **{
                    name: round(statistics.median(sample[name] for sample in samples), 3)
                    for name in samples[0]
                },
            }
        observation_tracker.OBSERVATION_PATH = original_observation_path
    return results


def _current_rss_kib() -> int:
    with open("/proc/self/status", "r", encoding="utf-8") as source:
        for line in source:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    raise RuntimeError("VmRSS unavailable")


def serialization_scenario(mode: str) -> dict[str, Any]:
    if os.name == "nt" or not Path("/proc/self/status").exists():
        raise RuntimeError("serialization scenario requires Linux /proc")
    observation = _synthetic_document(rows=1_000, target_bytes=4 * MIB)
    shadow = _synthetic_document(rows=10_000, target_bytes=47 * MIB)
    observation_bytes = _encoded_size(observation)
    shadow_bytes = _encoded_size(shadow)
    semaphore = threading.Semaphore(1)
    start = threading.Event()
    shadow_serializing = threading.Event()
    stop_sampling = threading.Event()
    peak_rss_kib = _current_rss_kib()
    observation_hold_ms = 0.0

    def sample_rss() -> None:
        nonlocal peak_rss_kib
        while not stop_sampling.is_set():
            peak_rss_kib = max(peak_rss_kib, _current_rss_kib())
            time.sleep(0.001)

    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)

        def write_shadow() -> None:
            start.wait()
            guard = semaphore if mode == "limited" else None
            if guard is not None:
                guard.acquire()
            try:
                shadow_serializing.set()
                state_store.atomic_write_json(root / "shadow.json", shadow)
            finally:
                if guard is not None:
                    guard.release()

        def write_observation() -> None:
            nonlocal observation_hold_ms
            start.wait()
            shadow_serializing.wait()
            guard = semaphore if mode == "limited" else None
            if guard is not None:
                guard.acquire()
            try:
                held_at = time.perf_counter()
                with state_store.exclusive_file_lock(
                    root / "observation.json",
                    operation="benchmark_observation",
                ) as attempt:
                    state_store.atomic_write_json(
                        root / "observation.json",
                        observation,
                        diagnostic_attempt=attempt,
                    )
                observation_hold_ms = (time.perf_counter() - held_at) * 1_000
            finally:
                if guard is not None:
                    guard.release()

        baseline_rss_kib = _current_rss_kib()
        sampler = threading.Thread(target=sample_rss, daemon=True)
        workers = [
            threading.Thread(target=write_shadow),
            threading.Thread(target=write_observation),
        ]
        sampler.start()
        for worker in workers:
            worker.start()
        wall_started = time.perf_counter()
        start.set()
        for worker in workers:
            worker.join()
        wall_ms = (time.perf_counter() - wall_started) * 1_000
        stop_sampling.set()
        sampler.join()
    return {
        "mode": mode,
        "observation_bytes": observation_bytes,
        "shadow_bytes": shadow_bytes,
        "observation_lock_hold_ms": round(observation_hold_ms, 3),
        "wall_ms": round(wall_ms, 3),
        "throughput_mib_s": round(
            ((observation_bytes + shadow_bytes) / MIB) / (wall_ms / 1_000),
            3,
        ),
        "baseline_rss_kib": baseline_rss_kib,
        "peak_rss_kib": peak_rss_kib,
        "rss_peak_delta_kib": peak_rss_kib - baseline_rss_kib,
    }


def lock_overhead(iterations: int) -> dict[str, Any]:
    enabled_samples: list[float] = []
    disabled_samples: list[float] = []
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / "state.json"

        def run_once(*, diagnostics_enabled: bool) -> float:
            started = time.perf_counter_ns()
            if diagnostics_enabled:
                for _ in range(iterations):
                    with state_store.exclusive_file_lock(
                        path,
                        operation="benchmark_uncontended",
                    ):
                        pass
            else:
                with patch.object(
                    state_store.state_lock_diagnostics,
                    "begin",
                    return_value=None,
                ):
                    for _ in range(iterations):
                        with state_store.exclusive_file_lock(
                            path,
                            operation="benchmark_uncontended",
                        ):
                            pass
            return (time.perf_counter_ns() - started) / iterations / 1_000

        for _ in range(5):
            disabled_samples.append(run_once(diagnostics_enabled=False))
            enabled_samples.append(run_once(diagnostics_enabled=True))
    disabled_us = statistics.median(disabled_samples)
    enabled_us = statistics.median(enabled_samples)
    return {
        "iterations_per_sample": iterations,
        "disabled_us_per_op": round(disabled_us, 3),
        "enabled_us_per_op": round(enabled_us, 3),
        "diagnostics_overhead_us_per_op": round(enabled_us - disabled_us, 3),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scenario",
        choices=("operations", "concurrent", "limited", "lock-overhead"),
        default="operations",
    )
    parser.add_argument("--repetitions", type=int, default=3)
    arguments = parser.parse_args()
    if arguments.scenario == "operations":
        result = operation_costs(max(1, arguments.repetitions))
    elif arguments.scenario == "lock-overhead":
        result = lock_overhead(max(100, arguments.repetitions))
    else:
        result = serialization_scenario(arguments.scenario)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
