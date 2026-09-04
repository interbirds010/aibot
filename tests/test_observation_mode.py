from __future__ import annotations

import asyncio
import concurrent.futures
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from src import analyzer, executor, monitor, observation_tracker, risk_manager


class ObservationLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.original_path = observation_tracker.OBSERVATION_PATH
        observation_tracker.OBSERVATION_PATH = (
            Path(self.temporary.name) / "signal_observations.json"
        )

    def tearDown(self) -> None:
        observation_tracker.OBSERVATION_PATH = self.original_path
        self.temporary.cleanup()

    def record(
        self,
        *,
        mint: str = "MINT",
        signature: str = "SIGNATURE",
        route: str = "B",
        score: float = 95.0,
        momentum_metrics: dict[str, int | float] | None = None,
    ) -> bool:
        return asyncio.run(observation_tracker.record_observation(
            mint=mint,
            route_type=route,
            source_wallet="WALLET",
            source_signature=signature,
            safety_score=100,
            entry_cost_lamports=1_000,
            token_amount_raw=500,
            token_decimals=6,
            entry_price_impact_pct=0.1,
            exit_price_impact_pct=0.2,
            expected_slippage_bps=100,
            dex_momentum_score=score,
            signal_detected_at="2026-07-30T00:00:00+00:00",
            analysis_completed_at="2026-07-30T00:00:01+00:00",
            entry_quote_at="2026-07-30T00:00:02+00:00",
            entry_latency_ms=2_000,
            momentum_metrics=momentum_metrics or {
                "volume_m5_usd": 20_000,
                "buys_m5": 30,
                "sells_m5": 10,
                "net_buys_m5": 20,
                "buy_sell_ratio_m5": 3,
                "liquidity_usd": 12_000,
                "pair_age_seconds": 1_000,
                "unknown_whale_count": 3,
            },
            safety_metrics={
                "developer_supply_percent": "3.5",
                "developer_below_ten_percent": True,
                "mint_authority_renounced": True,
                "lp_locked": True,
                "lp_locked_percent": "82.5",
                "liquidity_usd": "12000",
                "liquidity_above_minimum": True,
                "reasons": ["approved"],
                "sources": ["rugcheck"],
            },
        ))

    def test_observation_is_idempotent_and_does_not_create_position(self) -> None:
        self.assertTrue(self.record())
        self.assertFalse(self.record())
        document = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )
        self.assertEqual(len(document["observations"]), 1)
        self.assertNotIn("positions", document)
        self.assertNotIn("cash_lamports", document)
        self.assertTrue(document["observations"][0]["candidate_v2_eligible"])
        self.assertEqual(
            document["observations"][0]["strategy_variants"],
            [
                "baseline_v1",
                "route_b_baseline",
                "candidate_v2_score_90_to_below_100",
            ],
        )
        self.assertEqual(
            document["schema_version"],
            observation_tracker.OBSERVATION_SCHEMA_VERSION,
        )
        self.assertEqual(
            document["observations"][0]["safety_metrics"]["lp_locked_percent"],
            82.5,
        )

    def test_canonical_research_event_serializes_signal_type_and_shadow(self) -> None:
        self.assertTrue(self.record(route="B"))
        document = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )
        serialized = json.loads(json.dumps(document, ensure_ascii=False))
        row = serialized["observations"][0]
        self.assertEqual(row["signal_type"], "MOMENTUM")
        self.assertEqual(row["research_decision"], "SHADOW")
        self.assertEqual(row["observation_id"], "SIGNATURE:WALLET:MINT")

    def test_research_decision_transitions_are_explicit(self) -> None:
        discovery = asyncio.run(observation_tracker.record_candidate_discovery(
            mint="DISCOVERED",
            route_type="A",
            source_wallet="WALLET",
            source_signature="DISCOVERY",
            token_amount_raw=100,
            token_decimals=6,
            signal_detected_at="2026-07-30T00:00:00+00:00",
        ))
        rows = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"]
        self.assertEqual(rows[0]["signal_type"], "SMART_MONEY")
        self.assertEqual(rows[0]["research_decision"], "SHADOW")
        self.assertTrue(observation_tracker.finalize_candidate_without_quote(
            discovery.observation_id,
            decision_status="UNAVAILABLE",
            decision_reasons=("ENTRY_QUOTE_NO_ROUTE",),
            quote_status="NO_ROUTE",
        ))
        rows = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"]
        self.assertEqual(rows[0]["research_decision"], "REJECTED")

        self.assertTrue(self.record(mint="ENTERED", signature="ENTERED"))
        entered = next(row for row in observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"] if row["mint"] == "ENTERED")
        self.assertEqual(entered["research_decision"], "SHADOW")
        self.assertTrue(observation_tracker.mark_paper_experiment_status(
            entered["observation_id"], "OPENED", position_id="POSITION"
        ))
        self.assertTrue(observation_tracker.mark_paper_experiment_status(
            entered["observation_id"], "CLOSED", position_id="POSITION"
        ))
        entered = next(row for row in observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"] if row["mint"] == "ENTERED")
        self.assertEqual(entered["research_decision"], "ENTERED")

    def test_missing_momentum_features_remain_none(self) -> None:
        self.assertTrue(self.record(momentum_metrics={"volume_m5_usd": 20_000}))
        row = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        metrics = row["momentum_metrics"]
        self.assertEqual(metrics["volume_m5_usd"], 20_000.0)
        for name in (
            "buys_m5",
            "sells_m5",
            "net_buys_m5",
            "buy_sell_ratio_m5",
            "liquidity_usd",
            "pair_age_seconds",
            "unknown_whale_count",
        ):
            self.assertIsNone(metrics[name])

    def test_discovered_rejected_candidate_keeps_executable_shadow_samples(self) -> None:
        discovery = asyncio.run(observation_tracker.record_candidate_discovery(
            mint="REJECTED_MINT",
            route_type="A",
            source_wallet="WALLET",
            source_signature="REJECTED_SIGNATURE",
            token_amount_raw=500,
            token_decimals=6,
            signal_detected_at="2026-07-30T00:00:00+00:00",
            discovery_metadata={"whale_paid_lamports": 500_000_000},
        ))
        self.assertTrue(discovery.created)
        self.assertEqual(observation_tracker.due_observation_samples(10**12), [])

        finalized = asyncio.run(observation_tracker.record_observation_decision(
            mint="REJECTED_MINT",
            route_type="A",
            source_wallet="WALLET",
            source_signature="REJECTED_SIGNATURE",
            safety_score=60,
            entry_cost_lamports=1_000,
            token_amount_raw=500,
            token_decimals=6,
            entry_price_impact_pct=0.1,
            exit_price_impact_pct=0.2,
            expected_slippage_bps=100,
            dex_momentum_score=0,
            signal_detected_at="2026-07-30T00:00:00+00:00",
            analysis_completed_at="2026-07-30T00:00:01+00:00",
            entry_quote_at="2026-07-30T00:00:02+00:00",
            entry_latency_ms=2_000,
            decision_status="REJECTED",
            decision_reasons=("WHALE_AMOUNT_FILTER_REJECTED",),
        ))
        self.assertTrue(finalized.created)
        row = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        self.assertEqual(row["decision_status"], "REJECTED")
        self.assertEqual(row["paper_experiment_status"], "NOT_ELIGIBLE")
        self.assertEqual(row["status"], "PENDING")
        self.assertEqual(
            row["decision_reasons"], ["WHALE_AMOUNT_FILTER_REJECTED"]
        )
        self.assertTrue(observation_tracker.due_observation_samples(10**12))

    def test_same_transaction_keeps_each_watched_wallet_candidate_distinct(self) -> None:
        first = asyncio.run(observation_tracker.record_candidate_discovery(
            mint="MINT",
            route_type="A",
            source_wallet="WALLET_1",
            source_signature="SIGNATURE",
            token_amount_raw=100,
            token_decimals=6,
            signal_detected_at="2026-07-30T00:00:00+00:00",
        ))
        second = asyncio.run(observation_tracker.record_candidate_discovery(
            mint="MINT",
            route_type="A",
            source_wallet="WALLET_2",
            source_signature="SIGNATURE",
            token_amount_raw=200,
            token_decimals=6,
            signal_detected_at="2026-07-30T00:00:00+00:00",
        ))
        self.assertTrue(first.created)
        self.assertTrue(second.created)
        self.assertNotEqual(first.observation_id, second.observation_id)
        rows = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"]
        self.assertEqual(len(rows), 2)

    def test_candidate_without_entry_quote_is_completed_without_sampling(self) -> None:
        discovery = asyncio.run(observation_tracker.record_candidate_discovery(
            mint="NO_ROUTE",
            route_type="B",
            source_wallet="WALLET",
            source_signature="NO_ROUTE_SIGNATURE",
            token_amount_raw=0,
            token_decimals=0,
            signal_detected_at="2026-07-30T00:00:00+00:00",
        ))
        self.assertTrue(observation_tracker.finalize_candidate_without_quote(
            discovery.observation_id,
            decision_status="UNAVAILABLE",
            decision_reasons=("ENTRY_QUOTE_NO_ROUTE",),
            quote_status="NO_ROUTE",
        ))
        row = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        self.assertEqual(row["status"], "COMPLETE")
        self.assertEqual(row["quote_status"], "NO_ROUTE")
        self.assertEqual(observation_tracker.due_observation_samples(10**12), [])

    def test_v2_migration_preserves_samples_and_adds_analysis_fields(self) -> None:
        original_samples = [{"interval": "1m", "return_percent": 5.0}]

        def seed(document: dict) -> None:
            document.update({
                "schema_version": 2,
                "observations": [{
                    "observation_id": "OLD:MINT",
                    "status": "PENDING",
                    "samples": original_samples.copy(),
                }],
            })

        observation_tracker.update_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
            seed,
        )
        migrated = observation_tracker.ensure_observations_migrated()
        row = migrated["observations"][0]
        self.assertEqual(
            migrated["schema_version"],
            observation_tracker.OBSERVATION_SCHEMA_VERSION,
        )
        self.assertEqual(row["samples"], original_samples)
        self.assertEqual(row["safety_metrics"], {})
        self.assertEqual(row["tracking_profile"], "research_v1_60m")
        self.assertEqual(
            row["paper_experiment_status"],
            "LEGACY_OBSERVATION",
        )
        self.assertFalse(
            observation_tracker.migrate_observation_document(migrated)
        )

    def test_migration_recovers_stranded_complete_horizon_set(self) -> None:
        samples = [
            {
                "interval": interval,
                "return_percent": None if interval == "3m" else 1.0,
                "error": "HORIZON_MISSED" if interval == "3m" else None,
                "sampled_at": f"2026-07-30T00:{index:02d}:00+00:00",
            }
            for index, (interval, _) in enumerate(
                observation_tracker.OBSERVATION_INTERVALS,
                start=1,
            )
        ]

        def seed(document: dict) -> None:
            document.update({
                "schema_version": observation_tracker.OBSERVATION_SCHEMA_VERSION,
                "observations": [{
                    "observation_id": "STRANDED",
                    "status": "PENDING",
                    "tracking_profile": "research_v1_60m",
                    "samples": samples,
                }],
            })

        observation_tracker.update_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
            seed,
        )
        recovered = observation_tracker.ensure_observations_migrated()
        row = recovered["observations"][0]
        self.assertEqual(row["status"], "COMPLETE")
        self.assertEqual(row["completed_at"], "2026-07-30T00:06:00+00:00")
        self.assertFalse(observation_tracker.migrate_observation_document(recovered))

    def test_migration_fails_closed_for_malformed_observation_row(self) -> None:
        document = {"schema_version": 2, "observations": ["corrupt-row"]}
        with self.assertRaisesRegex(RuntimeError, "row is malformed"):
            observation_tracker.migrate_observation_document(document)

    def test_safety_snapshot_rejects_invalid_ranges_and_string_booleans(self) -> None:
        metrics = observation_tracker.normalized_safety_metrics({
            "developer_supply_percent": -1,
            "developer_below_ten_percent": "false",
            "lp_locked_percent": 101,
            "liquidity_usd": -1,
        })
        self.assertIsNone(metrics["developer_supply_percent"])
        self.assertFalse(metrics["developer_below_ten_percent"])
        self.assertIsNone(metrics["lp_locked_percent"])
        self.assertIsNone(metrics["liquidity_usd"])

    def test_retention_preserves_pending_rows_before_completed_history(self) -> None:
        rows = [
            {"observation_id": "PENDING", "status": "PENDING"},
            {"observation_id": "OLD", "status": "COMPLETE"},
            {"observation_id": "NEW", "status": "COMPLETE"},
        ]
        with patch.object(observation_tracker, "MAX_OBSERVATIONS", 2):
            retained = observation_tracker.retained_observations(rows)
        self.assertEqual(
            [row["observation_id"] for row in retained],
            ["PENDING", "NEW"],
        )

    def test_retention_preserves_completed_observation_while_position_is_open(self) -> None:
        rows = [
            {
                "observation_id": "OPEN",
                "status": "COMPLETE",
                "paper_experiment_status": "OPENED",
            },
            {"observation_id": "OLD", "status": "COMPLETE"},
            {"observation_id": "NEW", "status": "COMPLETE"},
        ]
        with patch.object(observation_tracker, "MAX_OBSERVATIONS", 2):
            retained = observation_tracker.retained_observations(rows)
        self.assertEqual(
            [row["observation_id"] for row in retained],
            ["OPEN", "NEW"],
        )

    def test_due_samples_are_bounded_per_tick(self) -> None:
        for index in range(3):
            with patch.object(observation_tracker.time, "time", return_value=1_000):
                self.record(
                    mint=f"MINT-{index}",
                    signature=f"SIGNATURE-{index}",
                )
        with patch.object(observation_tracker, "OBSERVATION_SAMPLE_BATCH_SIZE", 2):
            due = observation_tracker.due_observation_samples(2_000)
        self.assertEqual(len(due), 2)
        self.assertTrue(all(item[1] == "15m" for item in due))

    def test_active_backlog_expires_oldest_unopened_observation(self) -> None:
        with patch.object(observation_tracker, "MAX_ACTIVE_OBSERVATIONS", 2):
            for index in range(3):
                self.record(
                    mint=f"MINT-{index}",
                    signature=f"SIGNATURE-{index}",
                )
        rows = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"]
        self.assertEqual(rows[0]["status"], "EXPIRED_UNSAMPLED")
        self.assertEqual(rows[0]["expiration_reason"], "ACTIVE_OBSERVATION_LIMIT")
        self.assertEqual(
            sum(row["status"] == "PENDING" for row in rows),
            2,
        )

    def test_sample_failure_attempts_are_bounded_and_persisted(self) -> None:
        self.record()
        row = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        observation_id = row["observation_id"]
        self.assertEqual(observation_tracker.record_sample_attempt(
            observation_id, "1m", error="timeout"
        ), 1)
        self.assertEqual(observation_tracker.record_sample_attempt(
            observation_id, "1m", error="timeout"
        ), 2)
        self.assertEqual(observation_tracker.record_sample_attempt(
            observation_id, "1m", error="timeout"
        ), 3)

    def test_candidate_v2_score_boundaries(self) -> None:
        cases = (
            (89.9999, False),
            (90.0, True),
            (99.9999, True),
            (100.0, False),
        )
        for index, (score, eligible) in enumerate(cases):
            self.record(
                mint=f"MINT-{index}",
                signature=f"SIGNATURE-{index}",
                score=score,
            )
        rows = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"]
        self.assertEqual(
            [row["candidate_v2_eligible"] for row in rows],
            [eligible for _, eligible in cases],
        )

    def test_candidate_v2_limits_same_mint_to_once_per_24_hours(self) -> None:
        with patch.object(observation_tracker.time, "time", return_value=1_000):
            self.record(signature="FIRST")
        with patch.object(
            observation_tracker.time,
            "time",
            return_value=1_000 + 86_399,
        ):
            self.record(signature="TOO-SOON")
        with patch.object(
            observation_tracker.time,
            "time",
            return_value=1_000 + 86_400,
        ):
            self.record(signature="ALLOWED")
        rows = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"]
        self.assertEqual(
            [row["candidate_v2_eligible"] for row in rows],
            [True, False, True],
        )
        self.assertEqual(
            rows[1]["candidate_v2_filter_reasons"],
            ["MINT_SEEN_WITHIN_24H"],
        )

    def test_concurrent_same_mint_has_only_one_candidate(self) -> None:
        def write(index: int) -> bool:
            return self.record(signature=f"CONCURRENT-{index}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(write, range(2)))
        self.assertEqual(results, [True, True])
        rows = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"]
        self.assertEqual(
            sum(row["candidate_v2_eligible"] is True for row in rows),
            1,
        )

    def test_horizons_complete_in_normal_order_and_capture_excursions(self) -> None:
        self.assertEqual(
            observation_tracker.OBSERVATION_INTERVALS,
            (
                ("1m", 60),
                ("3m", 180),
                ("5m", 300),
                ("15m", 900),
                ("30m", 1_800),
                ("60m", 3_600),
            ),
        )
        self.record()
        row = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        observation_id = row["observation_id"]
        self.assertTrue(observation_tracker.record_sample(
            observation_id, "1m", proceeds_lamports=900
        ))
        self.assertTrue(observation_tracker.record_sample(
            observation_id, "3m", proceeds_lamports=800
        ))
        self.assertTrue(observation_tracker.record_sample(
            observation_id, "5m", proceeds_lamports=1_200
        ))
        self.assertTrue(observation_tracker.record_sample(
            observation_id, "15m", proceeds_lamports=1_100
        ))
        after_fifteen = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        self.assertEqual(after_fifteen["status"], "PENDING")
        self.assertEqual(after_fifteen["mfe_percent"], 20.0)
        self.assertEqual(after_fifteen["mae_percent"], -20.0)
        self.assertTrue(observation_tracker.record_sample(
            observation_id, "30m", proceeds_lamports=1_050
        ))
        with patch("src.shadow_trade_ledger.record_completed_shadow_trade") as archive:
            self.assertTrue(observation_tracker.record_sample(
                observation_id, "60m", proceeds_lamports=1_150
            ))
        archive.assert_called_once()
        updated = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        self.assertEqual(updated["status"], "COMPLETE")
        self.assertEqual(updated["min_return_percent"], -20.0)
        self.assertEqual(updated["max_return_percent"], 20.0)
        self.assertEqual(updated["mfe_percent"], 20.0)
        self.assertEqual(updated["mae_percent"], -20.0)
        self.assertEqual(updated["tracking_profile"], "research_v1_60m")
        self.assertEqual(
            updated["excursion_basis"],
            "scheduled_jupiter_executable_quotes",
        )
        self.assertTrue(updated["candidate_v2_early_failure"])

    def test_five_horizons_without_one_required_interval_remain_pending(self) -> None:
        self.record()
        row = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        with patch("src.shadow_trade_ledger.record_completed_shadow_trade") as archive:
            for interval in ("1m", "5m", "15m", "30m", "60m"):
                self.assertTrue(observation_tracker.record_sample(
                    row["observation_id"],
                    interval,
                    proceeds_lamports=1_100,
                ))
        updated = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        self.assertEqual(updated["status"], "PENDING")
        self.assertEqual(len(updated["samples"]), 5)
        archive.assert_not_called()

    def test_horizons_complete_after_reverse_order_backlog(self) -> None:
        self.record()
        row = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        observation_id = row["observation_id"]
        reverse_order = ("60m", "30m", "15m", "5m", "3m", "1m")
        with patch("src.shadow_trade_ledger.record_completed_shadow_trade") as archive:
            for interval in reverse_order[:-1]:
                self.assertTrue(observation_tracker.record_sample(
                    observation_id,
                    interval,
                    proceeds_lamports=1_100,
                ))
            pending = observation_tracker.read_json(
                observation_tracker.OBSERVATION_PATH,
                observation_tracker.empty_observations(),
            )["observations"][0]
            self.assertEqual(pending["status"], "PENDING")
            archive.assert_not_called()
            self.assertTrue(observation_tracker.record_sample(
                observation_id,
                reverse_order[-1],
                proceeds_lamports=1_100,
            ))
        completed = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        self.assertEqual(completed["status"], "COMPLETE")
        archive.assert_called_once()

    def test_missing_outcome_samples_still_complete_all_horizons(self) -> None:
        self.record()
        row = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        observation_id = row["observation_id"]
        with patch("src.shadow_trade_ledger.record_completed_shadow_trade") as archive:
            for interval, _ in observation_tracker.OBSERVATION_INTERVALS:
                missing = interval in {"3m", "30m"}
                self.assertTrue(observation_tracker.record_sample(
                    observation_id,
                    interval,
                    proceeds_lamports=None if missing else 1_100,
                    error="HORIZON_MISSED" if missing else None,
                ))
        completed = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        self.assertEqual(completed["status"], "COMPLETE")
        self.assertEqual(len(completed["samples"]), 6)
        self.assertEqual(
            {
                sample["interval"]
                for sample in completed["samples"]
                if sample["return_percent"] is None
            },
            {"3m", "30m"},
        )
        archive.assert_called_once()

    def test_complete_observation_rejects_duplicate_horizon_idempotently(self) -> None:
        self.record()
        row = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        observation_id = row["observation_id"]
        with patch("src.shadow_trade_ledger.record_completed_shadow_trade") as archive:
            for interval, _ in observation_tracker.OBSERVATION_INTERVALS:
                self.assertTrue(observation_tracker.record_sample(
                    observation_id,
                    interval,
                    proceeds_lamports=1_100,
                ))
            archive.reset_mock()
            self.assertFalse(observation_tracker.record_sample(
                observation_id,
                "60m",
                proceeds_lamports=900,
            ))
        completed = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        self.assertEqual(completed["status"], "COMPLETE")
        self.assertEqual(len(completed["samples"]), 6)
        sixty_minute = next(
            sample for sample in completed["samples"]
            if sample["interval"] == "60m"
        )
        self.assertEqual(sixty_minute["return_percent"], 10.0)
        archive.assert_not_called()

    def test_legacy_profile_completes_at_its_fifteen_minute_horizon(self) -> None:
        self.record()
        row = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        observation_id = row["observation_id"]

        def mark_legacy(document: dict) -> None:
            target = document["observations"][0]
            target["tracking_profile"] = "legacy_15m"

        observation_tracker.update_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
            mark_legacy,
        )
        with patch("src.shadow_trade_ledger.record_completed_shadow_trade") as archive:
            for interval in ("15m", "5m", "1m"):
                self.assertTrue(observation_tracker.record_sample(
                    observation_id,
                    interval,
                    proceeds_lamports=1_100,
                ))
        completed = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        self.assertEqual(completed["status"], "COMPLETE")
        self.assertEqual(len(completed["samples"]), 3)
        archive.assert_called_once()

    def test_excursions_include_zero_percent_entry_baseline(self) -> None:
        self.record(mint="WIN", signature="WIN")
        self.record(mint="LOSS", signature="LOSS")
        rows = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"]
        ids = {row["mint"]: row["observation_id"] for row in rows}
        observation_tracker.record_sample(
            ids["WIN"], "1m", proceeds_lamports=1_100
        )
        observation_tracker.record_sample(
            ids["LOSS"], "1m", proceeds_lamports=900
        )
        rows = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"]
        by_mint = {row["mint"]: row for row in rows}
        self.assertEqual(by_mint["WIN"]["mae_percent"], 0.0)
        self.assertEqual(by_mint["LOSS"]["mfe_percent"], 0.0)

    def test_unknown_route_is_not_mislabeled_as_smart_money(self) -> None:
        self.assertEqual(observation_tracker.signal_type_for_route(None), "UNKNOWN")
        self.assertEqual(observation_tracker.signal_type_for_route("C"), "UNKNOWN")

    def test_no_route_at_one_minute_is_not_an_early_failure(self) -> None:
        self.record()
        row = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        observation_tracker.record_sample(
            row["observation_id"],
            "1m",
            proceeds_lamports=None,
            error="no route",
        )
        updated = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        self.assertIsNone(updated["candidate_v2_early_failure"])
        self.assertEqual(updated["status"], "PENDING")

    def test_strategy_variants_cover_each_b_score_band(self) -> None:
        self.assertEqual(
            observation_tracker.strategy_variants("B", 89.999),
            (
                "baseline_v1",
                "route_b_baseline",
                "route_b_score_below_90",
            ),
        )
        self.assertIn(
            "candidate_v2_score_90_to_below_100",
            observation_tracker.strategy_variants("B", 95),
        )
        self.assertIn(
            "route_b_score_100_or_more",
            observation_tracker.strategy_variants("B", 100),
        )

    def test_paper_experiment_status_links_open_position(self) -> None:
        self.record()
        row = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        self.assertEqual(row["candidate_v2_paper_status"], "ELIGIBLE")
        self.assertTrue(observation_tracker.mark_paper_experiment_status(
            row["observation_id"], "OPENED", position_id="POSITION"
        ))
        updated = observation_tracker.read_json(
            observation_tracker.OBSERVATION_PATH,
            observation_tracker.empty_observations(),
        )["observations"][0]
        self.assertEqual(updated["paper_experiment_status"], "OPENED")
        self.assertEqual(updated["paper_experiment_position_id"], "POSITION")
        self.assertEqual(updated["candidate_v2_paper_status"], "OPENED")
        self.assertEqual(updated["candidate_v2_position_id"], "POSITION")
        self.assertEqual(updated["research_decision"], "ENTERED")


class ObservationEntryGateTests(unittest.TestCase):
    def test_shadow_backlog_is_bounded_before_task_creation(self) -> None:
        with (
            patch.object(
                monitor,
                "_shadow_signal_tasks",
                set(range(monitor.MAX_PENDING_SHADOW_SIGNALS)),
            ),
            patch.object(monitor.asyncio, "create_task") as create_task,
        ):
            monitor.schedule_paper_signal(
                "MINT",
                1,
                6,
                0,
                "WALLET",
                "SIGNATURE",
                prefilter_reasons=("PAYMENT_UNRESOLVED",),
            )
        create_task.assert_not_called()

    def test_shadow_analyzer_rejection_does_not_mutate_operational_ledgers(self) -> None:
        report = SimpleNamespace(
            safety_score=10,
            route_type=None,
            reasons=["unsafe"],
            liquidity_usd="100",
            lp_locked_percent="0",
        )
        quote = AsyncMock(side_effect=[
            {
                "outAmount": "2500",
                "routePlan": [{}],
                "priceImpactPct": "0.5",
                "slippageBps": "100",
            },
            {
                "outAmount": "990000",
                "routePlan": [{}],
                "priceImpactPct": "0.4",
                "slippageBps": "100",
            },
        ])
        decision = observation_tracker.ObservationDecision(
            True,
            "SIGNATURE:WALLET:MINT",
            False,
            ("baseline_v1", "route_a_baseline"),
        )
        reject_wallet = AsyncMock()
        reject_paper = AsyncMock()
        with (
            patch.dict("os.environ", {"OBSERVATION_MODE": "true"}),
            patch.object(
                analyzer, "analyze_token", new=AsyncMock(return_value=report)
            ),
            patch.object(
                monitor.state_store,
                "get_route_initial_stop_streak",
                return_value=(0, 0),
            ),
            patch.object(monitor, "token_cooldown_is_active", return_value=False),
            patch.object(
                risk_manager,
                "paper_cash_balance",
                new=AsyncMock(return_value=10_000_000_000),
            ),
            patch.object(risk_manager, "record_paper_rejection", new=reject_paper),
            patch.object(executor, "jupiter_quote", new=quote),
            patch.object(
                observation_tracker,
                "record_candidate_discovery",
                new=AsyncMock(return_value=decision),
            ),
            patch.object(
                observation_tracker,
                "record_observation_decision",
                new=AsyncMock(return_value=decision),
            ),
            patch("src.wallet_performance.reject_unsafe_buy", new=reject_wallet),
        ):
            asyncio.run(monitor.process_paper_signal(
                "MINT",
                1_000,
                6,
                500_000_000,
                "WALLET",
                "SIGNATURE",
                "2026-07-30T00:00:00+00:00",
                "A",
                prefilter_reasons=("WHALE_AMOUNT_FILTER_REJECTED",),
            ))
        reject_wallet.assert_not_awaited()
        reject_paper.assert_not_awaited()

    def test_route_a_amount_rejection_is_still_scheduled_for_observation(self) -> None:
        transaction = {
            "transaction": {
                "signatures": ["SIGNATURE"],
                "message": {"accountKeys": ["WALLET"]},
            },
            "meta": {
                "fee": 0,
                "preBalances": [1_000_000_000],
                "postBalances": [500_000_000],
                "preTokenBalances": [{
                    "owner": "WALLET",
                    "mint": "MINT",
                    "uiTokenAmount": {"amount": "0", "decimals": 6},
                }],
                "postTokenBalances": [{
                    "owner": "WALLET",
                    "mint": "MINT",
                    "uiTokenAmount": {"amount": "1000", "decimals": 6},
                }],
            },
        }

        async def run() -> None:
            with (
                patch.object(monitor, "whale_buy_amount_allowed", return_value=False),
                patch.object(monitor, "schedule_paper_signal") as schedule,
                patch(
                    "src.wallet_performance.observe_buy",
                    new=AsyncMock(),
                ),
            ):
                monitor.print_buys(transaction, "DEX", {"WALLET"})
                await asyncio.sleep(0)
            schedule.assert_called_once()
            self.assertEqual(
                schedule.call_args.kwargs["prefilter_reasons"],
                ("WHALE_AMOUNT_FILTER_REJECTED",),
            )

        asyncio.run(run())

    def test_prefilter_rejected_candidate_is_sampled_but_never_bought(self) -> None:
        report = SimpleNamespace(
            safety_score=100,
            route_type="A",
            reasons=[],
            liquidity_usd="10000",
            lp_locked_percent="80",
        )
        quote = AsyncMock(side_effect=[
            {
                "outAmount": "2500",
                "routePlan": [{}],
                "priceImpactPct": "0.5",
                "slippageBps": "100",
            },
            {
                "outAmount": "990000",
                "routePlan": [{}],
                "priceImpactPct": "0.4",
                "slippageBps": "100",
            },
        ])
        discovery = observation_tracker.ObservationDecision(
            True,
            "SIGNATURE:MINT",
            False,
            ("baseline_v1", "route_a_baseline"),
        )
        observe = AsyncMock(return_value=discovery)
        record_buy = AsyncMock()
        with (
            patch.dict("os.environ", {
                "OBSERVATION_MODE": "true",
                "APPROVED_SIGNAL_PAPER_MODE": "true",
            }),
            patch.object(
                analyzer, "analyze_token", new=AsyncMock(return_value=report)
            ),
            patch.object(
                monitor.state_store,
                "get_route_initial_stop_streak",
                return_value=(0, 0),
            ),
            patch.object(monitor, "token_cooldown_is_active", return_value=False),
            patch.object(
                risk_manager,
                "paper_cash_balance",
                new=AsyncMock(return_value=10_000_000_000),
            ),
            patch.object(risk_manager, "record_paper_buy", new=record_buy),
            patch.object(executor, "jupiter_quote", new=quote),
            patch.object(
                observation_tracker,
                "record_candidate_discovery",
                new=AsyncMock(return_value=discovery),
            ),
            patch.object(
                observation_tracker,
                "record_observation_decision",
                new=observe,
            ),
        ):
            asyncio.run(monitor.process_paper_signal(
                "MINT",
                1_000,
                6,
                500_000_000,
                "WALLET",
                "SIGNATURE",
                "2026-07-30T00:00:00+00:00",
                "A",
                prefilter_reasons=("WHALE_AMOUNT_FILTER_REJECTED",),
            ))
        self.assertEqual(
            observe.await_args.kwargs["decision_status"], "REJECTED"
        )
        self.assertEqual(
            observe.await_args.kwargs["decision_reasons"],
            ["WHALE_AMOUNT_FILTER_REJECTED"],
        )
        record_buy.assert_not_awaited()

    def test_approved_signal_is_observed_without_paper_buy(self) -> None:
        report = SimpleNamespace(
            safety_score=100,
            route_type="A",
            reasons=[],
            liquidity_usd="10000",
            lp_locked_percent="80",
        )
        quote = AsyncMock(side_effect=[
            {
                "outAmount": "2500",
                "routePlan": [{}],
                "priceImpactPct": "0.5",
                "slippageBps": "100",
            },
            {
                "outAmount": "990000",
                "routePlan": [{}],
                "priceImpactPct": "0.4",
                "slippageBps": "100",
            },
        ])
        observed = AsyncMock(return_value=observation_tracker.ObservationDecision(
            True,
            "SIGNATURE:MINT",
            False,
            ("baseline_v1", "route_a_baseline"),
        ))
        record_buy = AsyncMock()
        record_buy_success = AsyncMock()
        with (
            patch.dict("os.environ", {"OBSERVATION_MODE": "true"}),
            patch.object(
                analyzer, "analyze_token", new=AsyncMock(return_value=report)
            ),
            patch.object(
                monitor.state_store,
                "get_route_initial_stop_streak",
                return_value=(0, 0),
            ),
            patch.object(
                monitor, "token_cooldown_is_active", return_value=False
            ),
            patch.object(
                risk_manager,
                "paper_cash_balance",
                new=AsyncMock(return_value=10_000_000_000),
            ),
            patch.object(
                risk_manager, "record_paper_buy", new=record_buy
            ),
            patch.object(executor, "jupiter_quote", new=quote),
            patch.object(
                observation_tracker, "record_observation_decision", new=observed
            ),
            patch.object(
                observation_tracker,
                "record_candidate_discovery",
                new=AsyncMock(return_value=observation_tracker.ObservationDecision(
                    True,
                    "SIGNATURE:MINT",
                    False,
                    ("baseline_v1", "route_a_baseline"),
                )),
            ),
            patch(
                "src.wallet_performance.record_paper_buy_success",
                new=record_buy_success,
            ),
        ):
            asyncio.run(monitor.process_paper_signal(
                "MINT",
                1_000,
                6,
                2_000_000_000,
                "WALLET",
                "SIGNATURE",
                "2026-07-30T00:00:00+00:00",
                "A",
            ))
        observed.assert_awaited_once()
        record_buy.assert_not_awaited()
        record_buy_success.assert_not_awaited()
        self.assertEqual(quote.await_count, 2)

    def test_approved_observation_is_promoted_to_broad_paper_experiment(self) -> None:
        report = SimpleNamespace(
            safety_score=100,
            route_type="B",
            reasons=[],
            liquidity_usd="10000",
            lp_locked_percent="40",
        )
        quote = AsyncMock(side_effect=[
            {
                "outAmount": "2500",
                "routePlan": [{}],
                "priceImpactPct": "0.5",
                "slippageBps": "100",
            },
            {
                "outAmount": "990000",
                "routePlan": [{}],
                "priceImpactPct": "0.4",
                "slippageBps": "100",
            },
        ])
        decision = observation_tracker.ObservationDecision(
            True,
            "SIGNATURE:MINT",
            False,
            (
                "baseline_v1",
                "route_b_baseline",
                "route_b_score_100_or_more",
            ),
        )
        record_buy = AsyncMock(return_value="POSITION")
        with (
            patch.dict(
                "os.environ",
                {
                    "OBSERVATION_MODE": "true",
                    "APPROVED_SIGNAL_PAPER_MODE": "true",
                    "APPROVED_SIGNAL_MAX_OPEN_POSITIONS": "8",
                },
            ),
            patch.object(
                analyzer, "analyze_token", new=AsyncMock(return_value=report)
            ),
            patch.object(
                monitor, "token_cooldown_is_active", return_value=False
            ),
            patch.object(
                risk_manager,
                "paper_cash_balance",
                new=AsyncMock(return_value=10_000_000_000),
            ),
            patch.object(risk_manager, "record_paper_buy", new=record_buy),
            patch.object(executor, "jupiter_quote", new=quote),
            patch.object(
                observation_tracker,
                "record_observation_decision",
                new=AsyncMock(return_value=decision),
            ),
            patch.object(
                observation_tracker,
                "record_candidate_discovery",
                new=AsyncMock(return_value=decision),
            ),
            patch.object(
                observation_tracker,
                "mark_paper_experiment_status",
                return_value=True,
            ) as mark_status,
        ):
            asyncio.run(monitor.process_paper_signal(
                "MINT",
                1_000,
                6,
                2_000_000_000,
                "WALLET",
                "SIGNATURE",
                "2026-08-20T00:00:00+00:00",
                "B",
                100.0,
            ))
        record_buy.assert_awaited_once()
        self.assertEqual(
            record_buy.await_args.kwargs["strategy_version"],
            "broad_discovery_v1",
        )
        self.assertEqual(
            record_buy.await_args.kwargs["entry_reason"],
            "broad_discovery_approved_signal",
        )
        mark_status.assert_called_once_with(
            "SIGNATURE:MINT", "OPENED", position_id="POSITION"
        )


class ObservationRuntimeHealthTests(unittest.TestCase):
    def test_runtime_metrics_report_pending_success_and_missed_samples(self) -> None:
        metrics = observation_tracker.observation_runtime_metrics({
            "observations": [
                {
                    "status": "PENDING",
                    "samples": [
                        {
                            "return_percent": 2.5,
                            "sampled_at_epoch": 100.0,
                        },
                        {
                            "return_percent": None,
                            "error": "HORIZON_MISSED",
                            "sampled_at_epoch": 200.0,
                        },
                    ],
                },
                {
                    "status": "COMPLETE",
                    "samples": [
                        {
                            "return_percent": -1.0,
                            "sampled_at_epoch": 300.0,
                        }
                    ],
                },
            ]
        })

        self.assertEqual(metrics["pending_research_observations"], 1)
        self.assertEqual(metrics["last_successful_horizon_sample_at"], 300.0)
        self.assertEqual(metrics["horizon_missed_count"], 1)
        self.assertEqual(metrics["last_horizon_missed_at"], 200.0)

    def test_observer_supervisor_restarts_without_propagating_failure(self) -> None:
        observer = AsyncMock(side_effect=[
            RuntimeError("ledger unavailable"),
            asyncio.CancelledError(),
        ])
        publish = AsyncMock()

        async def run() -> None:
            with (
                patch.object(
                    observation_tracker,
                    "observation_loop",
                    new=observer,
                ),
                patch.object(
                    observation_tracker,
                    "_publish_observer_metrics",
                    new=publish,
                ),
            ):
                with self.assertRaises(asyncio.CancelledError):
                    await observation_tracker.observation_supervisor(
                        restart_delay_seconds=0,
                    )

        asyncio.run(run())
        self.assertEqual(observer.await_count, 2)
        publish.assert_awaited_once()
        self.assertEqual(
            publish.await_args.args[0]["observer_state"],
            "RESTARTING",
        )
        self.assertEqual(
            publish.await_args.args[0]["observer_last_error_type"],
            "RuntimeError",
        )


if __name__ == "__main__":
    unittest.main()
