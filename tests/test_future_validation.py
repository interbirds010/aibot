from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from src.research.alpha_discovery import (
    BUCKET_VERSION,
    SCHEMA_VERSION as ALPHA_DISCOVERY_SCHEMA_VERSION,
    _bucket_configuration,
)
from src.research.future_validation import (
    MAX_HYPOTHESES_PER_FAMILY,
    _evaluation_run_id,
    _fingerprint,
    _load_rows,
    _save,
    _publish_evaluation,
    _validate_cli_paths,
    build_future_validation,
    build_hypothesis_registry,
    derive_momentum_features,
    deterministic_hypothesis_id,
    fast_falsification_reasons,
    registry_with_evaluation_statuses,
    validate_registry,
)
from src.state_store import VersionConflict


FROZEN_AT = "2026-09-23T00:00:00Z"
FROZEN_TS = 1_790_121_600.0


def alpha_report() -> dict[str, object]:
    return {
        "schema_version": ALPHA_DISCOVERY_SCHEMA_VERSION,
        "configuration": {
            "bucket_version": BUCKET_VERSION,
            "bucket_definitions": _bucket_configuration(),
            "automatic_trading_changes": False,
        },
        "automatic_trading_changes": False,
        "top_candidates": [{
            "candidate_id": "SMART_MONEY:single:safety_score:95_or_more",
            "family": "SMART_MONEY",
            "status": "PROMISING",
            "labels": {"safety_score": "95_or_more"},
        }],
    }


def snapshot(timestamp: float, volume: float, buys: int, sells: int,
             liquidity: float = 10_000, price: float = 1.0) -> dict[str, object]:
    return {
        "snapshot_at_epoch": timestamp,
        "volume_m5_usd": volume,
        "buys_m5": buys,
        "sells_m5": sells,
        "liquidity_usd": liquidity,
        "price_usd": price,
    }


def row(
    identity: str,
    mint: str,
    timestamp: float,
    *,
    volume_start: float = 10_000,
    volume_end: float = 12_000,
    buys_start: int = 10,
    buys_end: int = 15,
    sells_start: int = 8,
    sells_end: int = 8,
    liquidity_start: float = 10_000,
    liquidity_end: float = 11_000,
    outcome: float | None = 5.0,
    quote_status: str = "EXECUTABLE",
) -> dict[str, object]:
    samples = [] if outcome is None else [
        {"interval": interval, "return_percent": outcome}
        for interval in ("5m", "15m", "30m", "60m")
    ]
    return {
        "observation_id": identity,
        "mint": mint,
        "route_type": "B",
        "signal_type": "MOMENTUM",
        "signal_detected_at": timestamp,
        "quote_status": quote_status,
        "samples": samples,
        "prospective_feature_collection": {
            "schema_version": 1,
            "collector_version": "momentum_pre_signal_v1",
            "pre_signal_snapshots": [
                snapshot(
                    timestamp - 120,
                    volume_start,
                    buys_start,
                    sells_start,
                    liquidity_start,
                    1.0,
                ),
                snapshot(
                    timestamp - 60,
                    (volume_start + volume_end) / 2,
                    (buys_start + buys_end) // 2,
                    sells_start,
                    (liquidity_start + liquidity_end) / 2,
                    1.01,
                ),
                snapshot(
                    timestamp,
                    volume_end,
                    buys_end,
                    sells_end,
                    liquidity_end,
                    1.02,
                ),
            ],
        },
        "momentum_metrics": {
            "volume_m5_usd": volume_end,
            "buys_m5": buys_end,
            "sells_m5": sells_end,
            "net_buys_m5": buys_end - sells_end,
            "buy_sell_ratio_m5": buys_end / max(1, sells_end),
            "liquidity_usd": liquidity_end,
            "pair_age_seconds": 600,
            "unknown_whale_count": 3,
        },
        "dex_momentum_score": 90,
        "safety_score": 80,
        "safety_metrics": {
            "lp_locked_percent": 80,
            "developer_supply_percent": 5,
        },
        "entry_price_impact_pct": 0.1,
        "exit_price_impact_pct": 0.2,
        "entry_latency_ms": 1000,
    }


class FutureValidationTests(unittest.TestCase):
    def registry(self) -> dict[str, object]:
        return build_hypothesis_registry(
            [row("DISCOVERY", "OLD", FROZEN_TS - 100)],
            created_at=FROZEN_AT,
        )

    def test_registry_has_five_pre_registered_candidates_and_deterministic_ids(self) -> None:
        first = self.registry()
        second = self.registry()
        self.assertEqual(len(first["hypotheses"]), 5)
        self.assertEqual(
            [item["hypothesis_id"] for item in first["hypotheses"]],
            [item["hypothesis_id"] for item in second["hypotheses"]],
        )
        self.assertTrue(all(
            item["hypothesis_id"] == deterministic_hypothesis_id(item)
            for item in first["hypotheses"]
        ))
        self.assertTrue(all(
            item["status"] == "READY_FOR_FUTURE_VALIDATION"
            for item in first["hypotheses"]
        ))

    def test_empty_discovery_preregisters_from_creation_time(self) -> None:
        registry = build_hypothesis_registry(
            [], created_at=FROZEN_AT,
        )
        self.assertEqual(len(registry["hypotheses"]), 5)
        self.assertTrue(all(
            item["discovery_data_end"] == "2026-09-23T00:00:00Z"
            for item in registry["hypotheses"]
        ))

    def test_semantic_change_changes_hypothesis_id(self) -> None:
        definition = copy.deepcopy(self.registry()["hypotheses"][0])
        original = deterministic_hypothesis_id(definition)
        definition["condition_definition"][0]["value"] = 1
        self.assertNotEqual(original, deterministic_hypothesis_id(definition))

    def test_registry_uses_creation_time_as_future_boundary(self) -> None:
        registry = self.registry()
        self.assertTrue(all(
            item["discovery_data_end"] == "2026-09-22T23:58:20Z"
            for item in registry["hypotheses"]
        ))
        self.assertTrue(all(
            item["validation_start"] == FROZEN_AT
            for item in registry["hypotheses"]
        ))

    def test_invalid_or_early_explicit_discovery_cutoff_fails_closed(self) -> None:
        rows = [row("DISCOVERY", "OLD", FROZEN_TS - 100)]
        with self.assertRaises(ValueError):
            build_hypothesis_registry(
                rows, created_at=FROZEN_AT, discovery_data_end="not-a-time",
            )
        with self.assertRaises(ValueError):
            build_hypothesis_registry(
                rows,
                created_at=FROZEN_AT,
                discovery_data_end="2026-09-22T23:58:19Z",
            )

    def test_registry_definition_and_family_cap_fail_closed(self) -> None:
        registry = self.registry()
        registry["hypotheses"][0]["condition_definition"][0]["value"] = 999
        with self.assertRaises(RuntimeError):
            validate_registry(registry)

        registry = self.registry()
        template = registry["hypotheses"][0]
        while len(registry["hypotheses"]) <= MAX_HYPOTHESES_PER_FAMILY:
            item = copy.deepcopy(template)
            item["rationale"] += str(len(registry["hypotheses"]))
            item["hypothesis_id"] = deterministic_hypothesis_id(item)
            item["definition_fingerprint"] = _fingerprint(item)
            registry["hypotheses"].append(item)
        with self.assertRaisesRegex(RuntimeError, "candidate cap"):
            validate_registry(registry)

    def test_prospective_derivation_contract_is_frozen(self) -> None:
        registry = self.registry()
        item = registry["hypotheses"][0]
        item["prospective_derivation_digest"] = "changed"
        item["hypothesis_id"] = deterministic_hypothesis_id(item)
        item["definition_fingerprint"] = _fingerprint(item)
        with self.assertRaisesRegex(RuntimeError, "prospective semantics"):
            validate_registry(registry)

    def test_alpha_bucket_contract_is_frozen(self) -> None:
        registry = build_hypothesis_registry(
            [row("DISCOVERY", "OLD", FROZEN_TS - 100)],
            alpha_report=alpha_report(),
            created_at=FROZEN_AT,
        )
        item = registry["hypotheses"][-1]
        item["feature_contract_digest"] = "changed"
        item["hypothesis_id"] = deterministic_hypothesis_id(item)
        item["definition_fingerprint"] = _fingerprint(item)
        with self.assertRaisesRegex(RuntimeError, "bucket semantics"):
            validate_registry(registry)

    def test_future_snapshot_is_excluded_from_derived_features(self) -> None:
        candidate = row("FUTURE", "M", FROZEN_TS + 200)
        candidate["prospective_feature_collection"]["pre_signal_snapshots"].append(
            snapshot(FROZEN_TS + 201, 1_000_000, 1_000, 0)
        )
        features = derive_momentum_features(candidate)
        self.assertEqual(features["volume_delta"], 2_000)
        self.assertEqual(features["snapshot_count"], 3)

    def test_strict_cutoff_excludes_discovery_and_equal_timestamp(self) -> None:
        registry = self.registry()
        report = build_future_validation([
            row("BEFORE", "C", FROZEN_TS - 1),
            row("EQUAL", "B", FROZEN_TS),
            row("AFTER", "C", FROZEN_TS + 1),
        ], registry)
        metrics = report["hypotheses"][0]["horizons"]["60m"]
        self.assertEqual(metrics["event_level"]["eligible_signal_count"], 1)
        self.assertEqual(
            metrics["first_signal_per_mint"]["eligible_signal_count"], 1
        )

    def test_same_event_matches_multiple_hypotheses_and_reuses_outcome(self) -> None:
        registry = self.registry()
        report = build_future_validation([
            row("ONE", "M", FROZEN_TS + 200)
        ], registry)
        self.assertGreaterEqual(
            report["shared_outcome_summary"]["maximum_hypotheses_per_event"], 3
        )
        self.assertEqual(report["input_summary"]["canonical_outcome_read_count"], 1)
        self.assertEqual(report["input_summary"]["rpc_request_count"], 0)

    def test_first_signal_per_mint_is_selected_before_hypothesis_filter(self) -> None:
        registry = self.registry()
        early = row(
            "EARLY", "SAME", FROZEN_TS + 150,
            volume_end=9_000, buys_end=8,
        )
        later = row("LATER", "SAME", FROZEN_TS + 200)
        report = build_future_validation([early, later], registry)
        metrics = report["hypotheses"][0]["horizons"]["60m"]
        self.assertEqual(metrics["event_level"]["eligible_signal_count"], 1)
        self.assertEqual(metrics["first_signal_per_mint"]["eligible_signal_count"], 0)

    def test_exact_duplicate_is_deduplicated_and_conflict_is_excluded(self) -> None:
        registry = self.registry()
        original = row("DUP", "M", FROZEN_TS + 200)
        report = build_future_validation([original, copy.deepcopy(original)], registry)
        self.assertEqual(report["input_summary"]["exact_duplicate_count"], 1)
        self.assertEqual(report["input_summary"]["deduplicated_event_count"], 1)

        conflict = copy.deepcopy(original)
        conflict["mint"] = "OTHER"
        report = build_future_validation([original, conflict], registry)
        self.assertEqual(report["input_summary"]["conflicting_identity_count"], 1)
        self.assertEqual(report["input_summary"]["deduplicated_event_count"], 0)

    def test_conflicting_horizon_samples_are_excluded(self) -> None:
        registry = self.registry()
        candidate = row("CONFLICT", "M", FROZEN_TS + 200)
        candidate["samples"].append({
            "interval": "60m", "return_percent": -99,
        })
        report = build_future_validation([candidate], registry)
        primary = report["hypotheses"][0]["horizons"]["60m"]["event_level"]
        self.assertEqual(primary["completed_outcome_count"], 0)
        self.assertEqual(
            report["input_summary"]["conflicting_horizon_sample_count"], 1,
        )

    def test_missing_feature_and_small_sample_are_insufficient(self) -> None:
        registry = self.registry()
        missing = row("MISSING", "M", FROZEN_TS + 200)
        missing.pop("prospective_feature_collection")
        report = build_future_validation([missing], registry)
        candidate = report["hypotheses"][0]
        self.assertEqual(candidate["missing_required_feature_count"], 1)
        self.assertEqual(candidate["future_status"], "FUTURE_INSUFFICIENT")

    def test_missing_quote_status_is_untrackable_and_positive_cohort_has_no_loss(self) -> None:
        registry = self.registry()
        candidate = row("UNKNOWN-QUOTE", "M", FROZEN_TS + 200)
        candidate.pop("quote_status")
        report = build_future_validation([candidate], registry)
        primary = report["hypotheses"][0]["horizons"]["60m"]["event_level"]
        self.assertEqual(primary["trackable_count"], 0)
        self.assertIsNone(primary["max_loss_percent"])

    def test_sufficient_positive_future_cohort_can_be_promising(self) -> None:
        registry = self.registry()
        rows = [
            row(
                f"ID-{index}", f"MINT-{index}",
                FROZEN_TS + 200 + index * 3600, outcome=5,
            )
            for index in range(60)
        ]
        report = build_future_validation(rows, registry)
        candidate = report["hypotheses"][0]
        self.assertEqual(candidate["future_status"], "FUTURE_PROMISING")
        primary = candidate["horizons"]["60m"]["first_signal_per_mint"]
        self.assertEqual(primary["completed_outcome_count"], 60)
        self.assertGreaterEqual(primary["positive_utc_day_count"], 2)
        self.assertIsNone(primary["max_loss_percent"])

        updated = registry_with_evaluation_statuses(registry, report)
        self.assertTrue(all(
            item["status"] == "FUTURE_PROMISING"
            for item in updated["hypotheses"]
        ))
        self.assertTrue(all(
            item["status"] == "READY_FOR_FUTURE_VALIDATION"
            for item in registry["hypotheses"]
        ))

    def test_sufficient_negative_future_cohort_is_failed_not_promising(self) -> None:
        registry = self.registry()
        rows = [
            row(
                f"ID-{index}", f"MINT-{index}",
                FROZEN_TS + 200 + index * 3600, outcome=-5,
            )
            for index in range(60)
        ]
        report = build_future_validation(rows, registry)
        candidate = report["hypotheses"][0]
        self.assertEqual(candidate["future_status"], "FUTURE_NEGATIVE")
        self.assertIn("NEGATIVE_EXPECTANCY", candidate["falsification_reasons"])

    def test_registry_status_update_is_atomic_and_versioned(self) -> None:
        registry = self.registry()
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "registry.json"
            saved = _save(path, registry)
            report = build_future_validation(
                [row("ONE", "M", FROZEN_TS + 200)],
                saved,
                generated_at="2026-09-23T01:00:00Z",
            )
            output = Path(temporary) / "report.json"
            updated, published = _publish_evaluation(
                path, output, saved, report,
            )
        self.assertEqual(updated["version"], 2)
        self.assertEqual(published["registry_source_version"], 1)
        self.assertEqual(published["registry_published_version"], 2)
        self.assertEqual(
            updated["last_evaluation_run_id"], published["evaluation_run_id"],
        )
        self.assertEqual(updated["last_evaluated_at"], "2026-09-23T01:00:00Z")
        self.assertTrue(all(
            item["status"] == "FUTURE_VALIDATING"
            for item in updated["hypotheses"]
        ))

    def test_stale_evaluation_cannot_overwrite_published_report(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            registry_path = Path(temporary) / "registry.json"
            output_path = Path(temporary) / "report.json"
            saved = _save(registry_path, self.registry())
            winner = build_future_validation(
                [row("WINNER", "M", FROZEN_TS + 200)],
                saved,
                generated_at="2026-09-23T01:00:00Z",
            )
            loser = build_future_validation(
                [row("LOSER", "M", FROZEN_TS + 300)],
                saved,
                generated_at="2026-09-23T01:00:01Z",
            )
            _publish_evaluation(
                registry_path, output_path, saved, winner,
            )
            winner_bytes = output_path.read_bytes()
            with self.assertRaises(VersionConflict):
                _publish_evaluation(
                    registry_path, output_path, saved, loser,
                )
            self.assertEqual(output_path.read_bytes(), winner_bytes)

    def test_tampered_report_cannot_promote_registry(self) -> None:
        registry = self.registry()
        report = build_future_validation(
            [row("ONE", "M", FROZEN_TS + 200)], registry,
        )
        report["hypotheses"][0][
            "registry_status_after_evaluation"
        ] = "FUTURE_PROMISING"
        with self.assertRaisesRegex(RuntimeError, "run identity"):
            registry_with_evaluation_statuses(registry, report)

        report["hypotheses"][0]["future_status"] = "FUTURE_PROMISING"
        report["hypotheses"][0]["falsification_reasons"] = []
        report["evaluation_run_id"] = _evaluation_run_id(report)
        with self.assertRaisesRegex(RuntimeError, "status is invalid"):
            registry_with_evaluation_statuses(registry, report)

    def test_extreme_winner_dependency_is_reported(self) -> None:
        reasons = fast_falsification_reasons({
            "completed_outcome_count": 60,
            "unique_mint_count": 60,
            "coverage_percent": 100,
            "expectancy_percent": 1,
            "profit_factor_above_one": True,
            "median_return_percent": 1,
            "top_winner_removed_expectancy_percent": -1,
            "positive_utc_day_count": 3,
        })
        self.assertIn("EXTREME_WINNER_DEPENDENCY", reasons)

    def test_alpha_promising_candidate_is_imported_without_auto_activation(self) -> None:
        registry = build_hypothesis_registry(
            [row("DISCOVERY", "OLD", FROZEN_TS - 100)],
            alpha_report=alpha_report(),
            created_at=FROZEN_AT,
        )
        smart = [
            item for item in registry["hypotheses"]
            if item["family"] == "SMART_MONEY"
        ]
        self.assertEqual(len(smart), 1)
        self.assertEqual(smart[0]["status"], "READY_FOR_FUTURE_VALIDATION")
        self.assertFalse(registry["automatic_trading_changes"])

    def test_stale_alpha_contract_is_not_imported(self) -> None:
        alpha = alpha_report()
        alpha["configuration"]["bucket_version"] = "stale"
        registry = build_hypothesis_registry(
            [row("DISCOVERY", "OLD", FROZEN_TS - 100)],
            alpha_report=alpha,
            created_at=FROZEN_AT,
        )
        self.assertFalse(any(
            item["family"] == "SMART_MONEY"
            for item in registry["hypotheses"]
        ))

    def test_pure_evaluation_does_not_mutate_inputs(self) -> None:
        registry = self.registry()
        rows = [row("ONE", "M", FROZEN_TS + 200)]
        before_registry = json.dumps(registry, sort_keys=True)
        before_rows = json.dumps(rows, sort_keys=True)
        build_future_validation(rows, registry)
        self.assertEqual(json.dumps(registry, sort_keys=True), before_registry)
        self.assertEqual(json.dumps(rows, sort_keys=True), before_rows)

    def test_cli_paths_and_explicit_schema_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "observations.json"
            source.write_text(
                json.dumps({"schema_version": 999, "observations": []}),
                encoding="utf-8",
            )
            with self.assertRaises(RuntimeError):
                _load_rows(source)
            with self.assertRaises(ValueError):
                _validate_cli_paths(
                    input_path=source,
                    alpha_path=Path(temporary) / "alpha.json",
                    registry_path=source,
                    output_path=Path(temporary) / "report.json",
                )


if __name__ == "__main__":
    unittest.main()
