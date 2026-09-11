from __future__ import annotations

import asyncio
import tempfile
import unittest
import weakref
from pathlib import Path
from unittest import mock

from src import monitor, phase_memory_telemetry as telemetry, research_archive
from src.solana_rpc import RpcProvider, _provider_request_once


class WeakTransaction(dict):
    __slots__ = ("__weakref__",)


class WeakList(list):
    __slots__ = ("__weakref__",)


def whale_transaction(wallet: str) -> WeakTransaction:
    return WeakTransaction({
        "transaction": {
            "signatures": [f"SIG-{wallet}"],
            "message": {
                "accountKeys": [{"pubkey": wallet, "signer": True}],
            },
        },
        "meta": {
            "fee": 5_000,
            "preBalances": [3_000_005_000],
            "postBalances": [1_000_000_000],
            "preTokenBalances": [],
            "postTokenBalances": [{
                "owner": wallet,
                "mint": "MINT",
                "uiTokenAmount": {"amount": "500", "decimals": 6},
            }],
        },
        "large_fixture_only": ["x" * 1024 for _ in range(128)],
    })


class SequentialWhaleRpc:
    def __init__(self) -> None:
        self.signatures_ref = None
        self.transaction_refs: list[weakref.ReferenceType[WeakTransaction]] = []
        self.previous_alive_before_next: list[bool] = []
        self.signatures_alive_during_fetch: list[bool] = []
        self.methods: list[str] = []

    async def __call__(self, _session, _url, method, _params):
        self.methods.append(method)
        if method == "getSignaturesForAddress":
            rows = WeakList([
                {"signature": "SIG-1", "err": None},
                {"signature": "SIG-2", "err": None},
                {"signature": "SIG-3", "err": None},
            ])
            self.signatures_ref = weakref.ref(rows)
            return rows
        if self.transaction_refs:
            self.previous_alive_before_next.append(
                self.transaction_refs[-1]() is not None
            )
        self.signatures_alive_during_fetch.append(self.signatures_ref() is not None)
        transaction = whale_transaction(f"WALLET-{len(self.transaction_refs) + 1}")
        self.transaction_refs.append(weakref.ref(transaction))
        return transaction


class FakeResponse:
    status = 200
    headers = {}
    content_length = 8192

    def __init__(self) -> None:
        self.payload = {
            "result": {"large_fixture_only": ["raw-marker"] * 256}
        }

    async def json(self):
        return self.payload


class FakeResponseContext:
    def __init__(self, response) -> None:
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, *_args):
        return False


class FakeSession:
    def __init__(self, response) -> None:
        self.response = response

    def post(self, *_args, **_kwargs):
        return FakeResponseContext(self.response)


class MemoryAttributionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.telemetry_path = self.root / "monitor_memory_phases.json"
        telemetry._active_phase_counts.clear()
        telemetry._active_contexts.clear()
        telemetry._active_context_overflow_count = 0
        telemetry._pending_batch = None
        telemetry._sampler_last_hwm_bytes = None
        telemetry._sampler_previous_contexts = []
        telemetry._sampler_previous_active_phases = []
        telemetry._sampler_thread = None
        telemetry._sampler_stop_event = None
        self.path_patch = mock.patch.object(
            telemetry, "MEMORY_PHASE_PATH", self.telemetry_path
        )
        self.flush_patch = mock.patch.object(
            telemetry, "_request_background_flush", return_value=False
        )
        self.path_patch.start()
        self.flush_patch.start()

    def tearDown(self) -> None:
        self.flush_patch.stop()
        self.path_patch.stop()
        telemetry._active_phase_counts.clear()
        telemetry._active_contexts.clear()
        telemetry._active_context_overflow_count = 0
        telemetry._pending_batch = None
        telemetry._sampler_last_hwm_bytes = None
        telemetry._sampler_previous_contexts = []
        telemetry._sampler_previous_active_phases = []
        telemetry._sampler_thread = None
        telemetry._sampler_stop_event = None
        self.temporary.cleanup()

    def test_whale_subphases_preserve_sequence_and_expose_raw_overlap(self) -> None:
        candidate = monitor.MomentumCandidate(
            "MINT", "PAIR", 20_000.0, 40, 10, 20_000.0, 1_000.0
        )
        rpc = SequentialWhaleRpc()

        async def scenario():
            with telemetry.phase_memory(
                "whale_confirmation",
                metadata={"workload": "momentum", "operation": "confirm"},
            ):
                return await monitor.confirm_unknown_whales(
                    object(), "", candidate, set()
                )

        with mock.patch.object(monitor, "_solana_rpc", new=rpc):
            result = asyncio.run(scenario())

        self.assertEqual(len(result), 3)
        self.assertEqual(
            rpc.methods,
            ["getSignaturesForAddress"] + ["getTransaction"] * 3,
        )
        self.assertEqual(rpc.previous_alive_before_next, [True, True])
        self.assertEqual(rpc.signatures_alive_during_fetch, [True, True, True])
        self.assertTrue(all(reference() is None for reference in rpc.transaction_refs))
        self.assertIsNone(rpc.signatures_ref())

        self.assertTrue(telemetry.flush_phase_memory_telemetry())
        document = telemetry.state_store.read_json(self.telemetry_path, {})
        expected = {
            "whale_signature_retrieval",
            "whale_signature_projection",
            "whale_transaction_fetch",
            "whale_transaction_matching",
            "whale_confirmation_aggregation",
            "whale_result_projection",
        }
        self.assertTrue(expected <= set(document["phases"]))
        self.assertEqual(
            document["phases"]["whale_transaction_fetch"]["metadata_maxima"][
                "retained_count"
            ],
            1,
        )
        self.assertEqual(
            document["phases"]["whale_result_projection"]["metadata_maxima"][
                "retained_count"
            ],
            1,
        )
        self.assertNotIn("raw-marker", repr(document))
        self.assertNotIn("SIG-1", repr(document))
        self.assertNotIn("MINT", repr(document))

    def test_rpc_json_parse_is_a_whale_only_payload_proxy_phase(self) -> None:
        provider = RpcProvider("test", "https://example.invalid", 1.0)

        async def scenario():
            with telemetry.phase_memory("whale_confirmation"):
                return await _provider_request_once(
                    FakeSession(FakeResponse()),
                    provider,
                    "getTransaction",
                    [],
                )

        result = asyncio.run(scenario())
        self.assertIn("large_fixture_only", result)
        self.assertTrue(telemetry.flush_phase_memory_telemetry())
        document = telemetry.state_store.read_json(self.telemetry_path, {})
        parse = document["phases"]["whale_transaction_parse"]
        self.assertEqual(parse["count"], 1)
        self.assertEqual(parse["metadata_maxima"]["response_bytes"], 8192)
        self.assertNotIn("raw-marker", repr(document))

    def test_archive_subphases_preserve_format_and_source_identity(self) -> None:
        row = {
            "observation_id": "OBS-LARGE",
            "status": "COMPLETE",
            "tracking_profile": "research_v1_60m",
            "signal_type": "SMART_MONEY",
            "signal_detected_at": "2026-09-11T00:00:00+00:00",
            "large_fixture_only": ["raw-marker" * 128 for _ in range(128)],
        }
        archive = self.root / "archive"
        metrics = self.root / "metrics.json"
        seen_identity: list[bool] = []
        original_write = research_archive.atomic_write_json

        def observe_write(path, document, **options):
            if path.name != "metrics.json":
                seen_identity.append(document["observation"] is row)
            return original_write(path, document, **options)

        with mock.patch.object(
            research_archive, "atomic_write_json", side_effect=observe_write
        ):
            created, _ = research_archive.archive_observation(
                row, archive_path=archive, metrics_path=metrics
            )

        self.assertTrue(created)
        self.assertEqual(seen_identity, [True])
        saved, _ = research_archive.load_research_archive(archive_path=archive)
        self.assertEqual(saved, [row])
        self.assertTrue(telemetry.flush_phase_memory_telemetry())
        document = telemetry.state_store.read_json(self.telemetry_path, {})
        self.assertIn("archive_record_preparation", document["phases"])
        serialized = document["phases"]["archive_serialization_write"]
        self.assertGreater(serialized["metadata_maxima"]["serialized_bytes"], 0)
        self.assertIn("archive_metric_write", document["phases"])
        self.assertNotIn("raw-marker", repr(document))

    def test_atomic_writer_lifecycle_is_ordered_and_fail_open(self) -> None:
        path = self.root / "state.json"
        stages: list[tuple[str, int]] = []
        telemetry.state_store.atomic_write_json(
            path,
            {"value": "x" * 4096},
            lifecycle_observer=lambda stage, size: stages.append((stage, size)),
        )
        self.assertEqual(
            [stage for stage, _ in stages],
            ["serialize", "serialized", "flushed", "replaced"],
        )
        self.assertEqual(stages[-1][1], path.stat().st_size)

        second = self.root / "observer-failure.json"
        telemetry.state_store.atomic_write_json(
            second,
            {"value": 2},
            lifecycle_observer=mock.Mock(side_effect=RuntimeError("telemetry")),
        )
        self.assertEqual(
            telemetry.state_store.read_json(second, {}), {"value": 2}
        )

    def test_atomic_writer_skips_size_probe_without_observer(self) -> None:
        path = self.root / "unobserved.json"
        original_named_temporary_file = (
            telemetry.state_store.tempfile.NamedTemporaryFile
        )
        real_context = original_named_temporary_file(
            "w", encoding="utf-8", dir=path.parent, delete=False
        )
        real_file = real_context.__enter__()
        wrapped_file = mock.Mock(wraps=real_file)
        wrapped_file.name = real_file.name
        wrapped_file.tell.side_effect = OSError("size probe unavailable")
        context = mock.MagicMock()
        context.__enter__.return_value = wrapped_file
        context.__exit__.side_effect = real_context.__exit__

        with mock.patch.object(
            telemetry.state_store.tempfile,
            "NamedTemporaryFile",
            return_value=context,
        ):
            telemetry.state_store.atomic_write_json(path, {"value": 3})

        wrapped_file.tell.assert_not_called()
        self.assertEqual(telemetry.state_store.read_json(path, {}), {"value": 3})


if __name__ == "__main__":
    unittest.main()
