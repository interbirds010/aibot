"""실제 serializer/원자 writer의 offline 측정. 운영 파일은 빈도만 읽는다."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import queue
import statistics
import tempfile
import threading
import time
from unittest.mock import patch

from src.research import entry_telemetry as t

STAMP = "2026-10-03T00:00:00+00:00"


def capture(kind, identity):
    c = t.begin_signal(mint="offline-mint", route_type="B", signal_detected_at=STAMP,
                       signal_id=str(identity))
    with t.bind(c):
        if kind != "minimal":
            t.mark("signal_enqueued_at")
            t.mark("analysis_started_at")
            t.set_section("scores", {"raw_components": {"volume_operand": 80, "volume_points": 60,
                "net_buy_operand": 42, "net_buy_points": 40}, "capped_total": 100,
                "uncapped_total": None, "missing_reason": "uncapped_total_not_computed_in_existing_flow"})
            t.set_section("safety_components", {"raw_inputs": {"developer_supply_percent_raw": 2.25,
                "lp_locked_percent_raw": 82.123, "liquidity_usd_raw": 20000.25},
                "allocated_components": [35, 30, 35], "capped_total": 100, "uncapped_total": 100})
            t.add_counter("rpc_attempt_count", 2)
            t.add_counter("rpc_retry_count", 1)
            t.duration("rpc_retry_sleep_sec", 0.25)
            t.duration("rpc_limiter_wait_duration_sec", 0.05)
            t.set_section("quote_buy", {"input_amount": 10000000, "expected_output": 100000,
                "price_impact_pct": 1.1, "route_count": 1, "dex_identifiers": [{"label": "offline-dex"}]})
            t.mark("quote_buy_request_started_at")
            t.mark("quote_buy_received_at")
            t.mark("analysis_completed_at")
        if kind in ("wallet_rich", "bounded_stress"):
            count = 20 if kind == "wallet_rich" else 128
            wallets = [f"offline-wallet-{n:03d}-" + "x" * (20 if count == 20 else 230) for n in range(count)]
            t.set_section("wallets", {"participating_wallet_ids": wallets,
                "paid_lamports_by_wallet": {w: 1500000000 for w in wallets},
                "observed_whale_count": count, "count_is_lower_bound": True})
        t.mark("entry_decision_at")
        if kind in ("normal_buy", "wallet_rich", "bounded_stress"):
            t.mark("paper_buy_created_at", trade_id="offline-trade-" + str(identity))
    t.finish(c, outcome="REJECT_ANALYZER" if kind == "reject" else "OTHER")
    return c


def summary(samples):
    ordered = sorted(samples)
    return {"n": len(samples), "mean_us": statistics.mean(samples),
            "median_us": statistics.median(samples), "p95_us": ordered[int((len(ordered)-1)*0.95)],
            "max_us": max(samples)}


def stage_counts(row):
    return sum(f.get("analyzer_started", {}).get("event_count", 0) +
               f.get("rpc_confirmation_failed", {}).get("event_count", 0)
               for f in row.get("families", {}).values())


def rates(live_root):
    # 미래 labels/수익/매도 값은 읽거나 계산하지 않는다. coverage count/time만 projection한다.
    hours = json.loads((live_root / "data/research_coverage_hourly.json").read_text(encoding="utf-8"))["hours"]
    complete = [r for r in hours if r.get("complete") and not r.get("overflow") and not r.get("saturation")][-24:]
    buckets = json.loads((live_root / "data/research_coverage_telemetry.json").read_text(encoding="utf-8"))
    counts = [stage_counts(r) for r in complete]
    bucket_counts = [stage_counts(r) for r in buckets["buckets"]]
    return {"basis": "analyzer_started + rpc_confirmation_failed event counts; telemetry candidate proxy, not exact new-row rate",
        "complete_hours": len(complete), "window_start_utc": complete[0]["hour_start_utc"] if complete else None,
        "window_last_hour_start_utc": complete[-1]["hour_start_utc"] if complete else None,
        "hour_counts": counts, "mean_per_hour": statistics.mean(counts) if counts else None,
        "min_per_hour": min(counts) if counts else None, "max_per_hour": max(counts) if counts else None,
        "bucket_seconds": buckets["bucket_seconds"], "bucket_count": len(bucket_counts),
        "max_bucket_events": max(bucket_counts) if bucket_counts else None,
        "unknown": "uncounted early rejects and new-envelope boundaries; subsecond burst rate unavailable"}


def measure(live_root):
    health = {"status": "TELEMETRY_READY", "dropped_row_count": 0, "write_error_count": 0,
              "duplicate_count": 0, "conflict_count": 0, "last_error": None}
    with tempfile.TemporaryDirectory(prefix="aibot-telemetry-readiness-") as temporary:
        root = Path(temporary)
        with patch.object(t, "_root", root), patch.object(t, "_queue", queue.Queue(maxsize=16)), \
                patch.object(t, "_health", health):
            # provenance 실제 구현으로 후보 소스 fingerprint. temporary writer 경로와 분리한다.
            with patch.object(t, "_root", Path(__file__).resolve().parents[1]):
                provenance = t._build_provenance({"TRADING_MODE": "PAPER"})
            with patch.object(t, "_provenance", provenance):
                kinds = ["minimal", "normal_buy", "wallet_rich", "reject", "bounded_stress"]
                rows = {}
                for kind in kinds:
                    c = capture(kind, kind)
                    t._queue.get_nowait(); t._queue.task_done()
                    doc = t._row(c)
                    sealed = {**doc, "content_hash": t._digest(doc)}
                    started = time.perf_counter_ns(); t._persist(c)
                    elapsed = (time.perf_counter_ns() - started) / 1000
                    path = root / "data/research/entry_telemetry/rows" / (c.identity["signal_id"] + ".json")
                    rows[kind] = {"canonical_utf8_bytes": len(t._canonical(sealed).encode("utf-8")),
                        "disk_bytes": path.stat().st_size, "write_us": elapsed,
                        "capture_budget_exhausted": doc["collection_limits"]["capture_budget_exhausted"]}
                c = capture("normal_buy", "serialization")
                t._queue.get_nowait(); t._queue.task_done()
                serial = []
                for _ in range(1000):
                    start = time.perf_counter_ns(); doc=t._row(c); t._canonical(doc); t._digest(doc)
                    serial.append((time.perf_counter_ns()-start)/1000)
                enqueue = []
                for i in range(1000):
                    c = t.begin_signal(mint="offline-mint", route_type="B", signal_detected_at=STAMP, signal_id=str(i))
                    start=time.perf_counter_ns(); t.finish(c); enqueue.append((time.perf_counter_ns()-start)/1000)
                    t._queue.get_nowait(); t._queue.task_done()
                hot_path = []
                for i in range(1000):
                    start=time.perf_counter_ns(); capture("normal_buy", "capture-"+str(i))
                    hot_path.append((time.perf_counter_ns()-start)/1000)
                    t._queue.get_nowait(); t._queue.task_done()
                write = []
                for i in range(100):
                    c = capture("normal_buy", "write-"+str(i)); t._queue.get_nowait(); t._queue.task_done()
                    start=time.perf_counter_ns(); t._persist(c); t._publish_health()
                    write.append((time.perf_counter_ns()-start)/1000)
                # 기존 worker의 persist+health 경로를 별도 offline consumer로 재현한다.
                stopping=threading.Event(); max_depth=0; drain_count=[0]
                def consume():
                    while not stopping.is_set() or not t._queue.empty():
                        try: item=t._queue.get(timeout=0.01)
                        except queue.Empty: continue
                        t._persist(item); t._publish_health(); drain_count[0]+=1; t._queue.task_done()
                consumer=threading.Thread(target=consume); consumer.start()
                start=time.perf_counter()
                for i in range(64):
                    capture("normal_buy", "burst-"+str(i)); max_depth=max(max_depth,t._queue.qsize())
                stopping.set(); consumer.join(30)
                if consumer.is_alive(): raise RuntimeError("offline consumer did not finish")
                burst_seconds=time.perf_counter()-start
                result = {"measured_utc": datetime.now(timezone.utc).isoformat(), "platform": provenance["platform"],
                    "build_sha": provenance["git_sha"], "rows": rows,
                    "fixture_mean_disk_bytes": statistics.mean(r["disk_bytes"] for r in rows.values()),
                    "fixture_upper_disk_bytes": max(r["disk_bytes"] for r in rows.values()),
                    "fixture_distribution_note": "five designed fixtures; upper estimate is not production p95",
                    "serialization": summary(serial), "enqueue_only": summary(enqueue),
                    "full_fixture_capture_enqueue": summary(hot_path), "write_including_health": summary(write),
                    "writer_drain_rows_per_sec": 1000000/statistics.mean(write),
                    "burst": {"produced":64,"drained":drain_count[0],"dropped":health["dropped_row_count"],
                        "max_queue_depth":max_depth,"elapsed_sec":burst_seconds},
                    "rate_proxy": rates(live_root), "constraints": "Windows local temporary storage; no RPC, runtime activation or Control latency measurement"}
                return result


if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--live-root", type=Path, required=True)
    args=parser.parse_args()
    print(json.dumps(measure(args.live_root), ensure_ascii=False, indent=2))
