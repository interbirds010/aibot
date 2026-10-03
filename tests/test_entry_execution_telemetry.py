from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from src import analyzer, executor, solana_rpc
from src.research import entry_telemetry as telemetry
from src.state_store import atomic_write_json


MINT = executor.WSOL_MINT


class Response:
    def __init__(self, status=200, payload=None):
        self.status, self.payload, self.headers = status, payload, {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError("terminal response")


class Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def get(self, *_args, **_kwargs):
        self.calls += 1
        return self.responses.pop(0)


def capture():
    return telemetry.begin_signal(mint=MINT, route_type="A",
                                  signal_detected_at="2026-10-03T00:00:00+00:00")


class EntryExecutionTelemetryTests(unittest.TestCase):
    def test_quote_legs_preserve_payload_and_only_selected_fields(self):
        buy = {"routePlan": [{"swapInfo": {"ammKey": "amm", "label": "DEX"}}],
               "outAmount": "20", "priceImpactPct": "0.12", "secret": "must_not_persist"}
        sell = {"routePlan": [{"swapInfo": {"ammKey": "amm2"}}],
                "outAmount": "10", "priceImpactPct": "0.2"}
        session = Session([Response(payload=buy), Response(payload=sell)])
        row = capture()

        async def scenario():
            with telemetry.bind(row), patch.object(executor, "_wait_for_global_jupiter_slot", new=AsyncMock()):
                self.assertIs(await executor.jupiter_quote(session, "secret_key", MINT, "token", 10), buy)
                self.assertIs(await executor.jupiter_quote(session, "secret_key", "token", MINT, 20), sell)

        asyncio.run(scenario())
        self.assertEqual(session.calls, 2)
        self.assertEqual(row.counters["quote_buy_attempt_count"], 1)
        self.assertEqual(row.counters["quote_exit_preflight_attempt_count"], 1)
        self.assertEqual(row.counters["quote_buy_retry_count"], 0)
        self.assertEqual(row.sections["quote_buy"]["expected_output"], "20")
        self.assertEqual(row.sections["quote_buy"]["dex_identifiers"], [{"ammKey": "amm", "label": "DEX"}])
        self.assertEqual(len(row.sections["quote_buy"]["selected_route_hash"]), 64)
        encoded = json.dumps(row.sections)
        self.assertNotIn("secret_key", encoded)
        self.assertNotIn("must_not_persist", encoded)
        started = row.timestamps["quote_buy_request_started_at"]["monotonic_ns"]
        self.assertLessEqual(started, row.timestamps["quote_buy_received_at"]["monotonic_ns"])

    def test_quote_429_records_actual_attempt_and_retry_without_extra_request(self):
        quote = {"routePlan": [{}], "outAmount": "2"}
        session = Session([Response(429, {}), Response(payload=quote)])
        row = capture()

        async def scenario():
            with (telemetry.bind(row),
                  patch.object(executor, "_wait_for_global_jupiter_slot", new=AsyncMock()),
                  patch.object(executor, "_defer_jupiter_until_sync"),
                  patch.object(executor.asyncio, "sleep", new=AsyncMock()) as sleep):
                result = await executor.jupiter_quote(session, "key", MINT, "token", 1)
                self.assertEqual(sleep.await_count, 1)
                return result

        self.assertIs(asyncio.run(scenario()), quote)
        self.assertEqual(session.calls, 2)
        self.assertEqual(row.counters["quote_buy_attempt_count"], 2)
        self.assertEqual(row.counters["quote_buy_retry_count"], 1)
        self.assertEqual(row.counters["quote_buy_transient_error_count"], 1)
        self.assertEqual(row.sections["quote_buy_errors"]["last_error_type"], "HTTP_429")
        self.assertGreaterEqual(row.durations["quote_buy_retry_sleep_sec"], 0)

    def test_terminal_no_route_has_same_exception_and_no_retry(self):
        row = capture()
        session = Session([Response(400, {})])

        async def scenario():
            with telemetry.bind(row), patch.object(executor, "_wait_for_global_jupiter_slot", new=AsyncMock()):
                await executor.jupiter_quote(session, "key", MINT, "token", 1, fail_fast_bad_request=True)

        with self.assertRaises(executor.JupiterNoRouteError):
            asyncio.run(scenario())
        self.assertEqual(session.calls, 1)
        self.assertEqual(row.counters["quote_buy_retry_count"], 0)

    def test_hook_failure_does_not_change_quote_success(self):
        quote = {"routePlan": [{}], "outAmount": "2"}

        async def scenario():
            with (patch.object(telemetry, "safe_hook", side_effect=RuntimeError("telemetry_down")),
                  patch.object(executor, "_wait_for_global_jupiter_slot", new=AsyncMock())):
                return await executor.jupiter_quote(Session([Response(payload=quote)]), "key", MINT, "token", 1)

        self.assertIs(asyncio.run(scenario()), quote)

    def test_rpc_failover_counts_real_network_work_without_urls_or_params(self):
        providers = [solana_rpc.RpcProvider("alchemy", "https://secret.invalid", 1000),
                     solana_rpc.RpcProvider("chainstack", "https://second.invalid", 1000)]
        failure = solana_rpc._ProviderRequestError(transient=True, rate_limited=True,
            status=429, category="RATE_LIMIT")
        row = capture()

        async def scenario():
            with (telemetry.bind(row),
                  patch.object(solana_rpc, "_reserve_provider_slot", new=AsyncMock(return_value=solana_rpc.ProviderReservation(False))),
                  patch.object(solana_rpc, "_provider_request_once", new=AsyncMock(side_effect=[failure, {"value": 7}])) as request,
                  patch.object(solana_rpc, "_record_provider_failure_sync"),
                  patch.object(solana_rpc, "_record_provider_success_sync")):
                result = await solana_rpc.solana_rpc_call(None, "getBalance", ["secret_parameter"], providers=providers)
                self.assertEqual(request.await_count, 2)
                return result

        self.assertEqual(asyncio.run(scenario()), {"value": 7})
        self.assertEqual(row.counters["rpc_attempt_count"], 2)
        self.assertEqual(row.counters["rpc_retry_count"], 1)
        self.assertEqual(row.counters["rpc_local_retry_count"], 0)
        self.assertEqual(row.counters["rpc_failover_attempt_count"], 1)
        self.assertEqual(row.sections["rpc_errors"]["last_error_type"], "RATE_LIMIT")
        self.assertNotIn("secret", json.dumps(row.sections))

    def test_actual_jupiter_limiter_sleep_is_distinct_from_reservation(self):
        row = capture()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "limiter.json"
            atomic_write_json(path, {"last_request_at_epoch": time.time(), "not_before_epoch": 0})
            with telemetry.bind(row), patch.object(executor, "JUPITER_RATE_LIMIT_PATH", path):
                delay = executor._wait_for_jupiter_slot_sync(0.03)
        self.assertGreater(delay, 0)
        self.assertEqual(row.counters["quote_limiter_wait_count"], 1)
        self.assertGreater(row.durations["quote_limiter_wait_duration_sec"], 0)

    def test_safety_raw_components_preserve_actual_precision_and_report_contract(self):
        account = {"value": {"data": {"parsed": {"type": "mint", "info": {"mintAuthority": None}}}}}
        supply = {"value": {"amount": "100000"}}
        rug = {"creator": "creator", "creatorBalance": "1234.567", "lpLockedPct": "91.2345", "liquidityUsd": "12345.6789"}
        row = capture()

        class Context:
            async def __aenter__(self): return None
            async def __aexit__(self, *_): pass

        async def scenario():
            with (telemetry.bind(row), patch.object(analyzer.aiohttp, "ClientSession", return_value=Context()),
                  patch.object(analyzer, "rpc_call", new=AsyncMock(side_effect=[account, supply])),
                  patch.object(analyzer, "rugcheck_get", new=AsyncMock(side_effect=[rug, None]))):
                return await analyzer._analyze_token_uncached(MINT, analyzer.AnalyzerSettings("unused"))

        report = asyncio.run(scenario())
        self.assertTrue(report.should_enter)
        self.assertEqual(report.safety_score, 100)
        self.assertEqual(report.developer_supply_percent, "1.23")
        raw = row.sections["safety_components"]
        self.assertEqual(raw["developer_supply_percent_raw"], "1.234567")
        self.assertEqual(raw["liquidity_usd_raw"], "12345.6789")
        self.assertEqual(raw["components"], {"mint_authority": 35, "developer_holding": 30, "lp_lock": 35})
        self.assertFalse(raw["cap_applied"])
        self.assertEqual(raw["uncapped_total"], raw["capped_total"])

    def test_analyzer_cache_does_not_attribute_original_work_to_new_signal(self):
        analyzer._analysis_cache.clear()
        analyzer._analysis_flights.clear()
        settings = analyzer.AnalyzerSettings("unused")
        report = analyzer.SafetyReport(MINT, safety_score=100, should_enter=True)
        first, second = capture(), capture()

        async def scenario():
            with patch.object(analyzer, "_analyze_token_uncached", new=AsyncMock(return_value=report)) as worker:
                with telemetry.bind(first):
                    await analyzer.analyze_token(MINT, settings)
                with telemetry.bind(second):
                    result = await analyzer.analyze_token(MINT, settings)
                self.assertEqual(worker.await_count, 1)
                return result

        try:
            self.assertEqual(asyncio.run(scenario()).safety_score, 100)
            self.assertTrue(second.sections["analyzer_source"]["cache_hit"])
            self.assertIsNone(second.counters["rpc_attempt_count"])
            self.assertEqual(second.sections["analyzer_source"]["rpc_attempt_attribution"], "no_current_request_cached_result")
        finally:
            analyzer._analysis_cache.clear()
            analyzer._analysis_flights.clear()

    def test_frozen_capture_ignores_background_rpc_hooks(self):
        row = capture()
        with telemetry.bind(row):
            telemetry.mark("entry_decision_at")
            solana_rpc._entry_hook("add_counter", "rpc_attempt_count")
            analyzer._entry_hook("set_section", "safety_components", {"uncapped_total": 100})
        self.assertIsNone(row.counters["rpc_attempt_count"])
        self.assertIsNone(row.sections["safety_components"]["uncapped_total"])

    def test_rpc_hook_failure_preserves_existing_return_and_attempt_count(self):
        provider = solana_rpc.RpcProvider("alchemy", "https://unused.invalid", 1000)

        async def scenario():
            with (patch.object(telemetry, "safe_hook", side_effect=RuntimeError("telemetry_down")),
                  patch.object(solana_rpc, "_reserve_provider_slot", new=AsyncMock(return_value=solana_rpc.ProviderReservation(False))),
                  patch.object(solana_rpc, "_provider_request_once", new=AsyncMock(return_value={"value": 7})) as request,
                  patch.object(solana_rpc, "_record_provider_success_sync")):
                result = await solana_rpc.solana_rpc_call(None, "getBalance", [], providers=[provider])
                self.assertEqual(request.await_count, 1)
                return result

        self.assertEqual(asyncio.run(scenario()), {"value": 7})

    def test_shared_analyzer_work_does_not_leak_into_waiter_or_finished_owner(self):
        analyzer._analysis_cache.clear()
        analyzer._analysis_flights.clear()
        first, second = capture(), capture()

        async def scenario():
            started, release = asyncio.Event(), asyncio.Event()
            settings = analyzer.AnalyzerSettings("unused")

            async def worker(*_):
                started.set()
                await release.wait()
                analyzer._entry_hook("set_section", "safety_components", {"uncapped_total": 100})
                analyzer._entry_hook("add_counter", "rpc_attempt_count")
                return analyzer.SafetyReport(MINT, safety_score=100)

            with patch.object(analyzer, "_analyze_token_uncached", new=worker):
                with telemetry.bind(first):
                    owner = asyncio.create_task(analyzer.analyze_token(MINT, settings))
                await started.wait()
                with telemetry.bind(second):
                    waiter = asyncio.create_task(analyzer.analyze_token(MINT, settings))
                await asyncio.sleep(0)
                # 소유 후보가 이미 끝난 뒤 shared task가 완료되는 실제 경계다.
                first.finished = first.frozen = True
                release.set()
                reports = await asyncio.gather(owner, waiter)
                self.assertEqual([report.safety_score for report in reports], [100, 100])

        try:
            asyncio.run(scenario())
            self.assertFalse(second.sections["analyzer_source"]["shared_flight_owner"])
            self.assertEqual(second.sections["analyzer_source"]["rpc_attempt_attribution"], "shared_work_not_attributed_to_waiter")
            self.assertIsNone(first.counters["rpc_attempt_count"])
            self.assertIsNone(second.counters["rpc_attempt_count"])
            self.assertIsNone(second.sections["safety_components"]["uncapped_total"])
        finally:
            analyzer._analysis_cache.clear()
            analyzer._analysis_flights.clear()
