from __future__ import annotations

import copy
import io
import json
import unittest
from pathlib import Path
from unittest import mock

from src.research.paper_mvp_review import (
    build_paper_mvp_review,
    read_paper_ledger_snapshot,
)


CUTOFF = 9_474
BEFORE_REVIEW = "2026-09-23T09:00:00+00:00"
AFTER_REVIEW = "2026-09-23T10:00:00+00:00"


def event(sequence: int, event_type: str, **values: object) -> dict[str, object]:
    return {"event_seq": sequence, "event_id": f"E-{sequence}", "type": event_type, **values}


def ledger() -> dict[str, object]:
    events = [
        event(
            9_470,
            "BUY",
            position_id="CARRY",
            mint="CARRY-MINT",
            cost_lamports=100,
            token_amount_raw=10,
            at="2026-09-16T08:00:00+00:00",
            route_type="A",
        ),
        event(
            9_471,
            "SELL",
            position_id="HISTORICAL",
            mint="OLD",
            token_amount_raw=1,
            proceeds_lamports=1,
            realized_pnl_lamports=-57_168_670,
            at="2026-09-16T08:10:00+00:00",
        ),
        event(9_472, "SKIPPED_BY_RPC_ERROR", mint="X"),
        event(9_473, "SIGNAL_REJECTED", mint="Y"),
        event(9_474, "SKIPPED_BY_RPC_ERROR", mint="Z"),
        event(
            9_475,
            "SELL",
            position_id="CARRY",
            mint="CARRY-MINT",
            token_amount_raw=10,
            proceeds_lamports=105,
            realized_pnl_lamports=5,
            at="2026-09-17T00:00:00+00:00",
        ),
        event(
            9_476,
            "BUY",
            position_id="P1",
            mint="M1",
            cost_lamports=100,
            token_amount_raw=10,
            at="2026-09-17T01:00:00+00:00",
            route_type="B",
            entry_price_impact_pct=0.1,
            exit_price_impact_pct=0.2,
            expected_slippage_bps=100,
            entry_latency_ms=1_000,
        ),
        event(
            9_477,
            "SELL",
            position_id="P1",
            mint="M1",
            token_amount_raw=10,
            proceeds_lamports=120,
            realized_pnl_lamports=20,
            at="2026-09-18T02:00:00+00:00",
            trigger_price_impact_pct=0.3,
            exit_trigger_latency_ms=200,
            quote_age_ms=100,
        ),
        event(
            9_478,
            "BUY",
            position_id="P2",
            mint="M2",
            cost_lamports=100,
            token_amount_raw=10,
            at="2026-09-18T03:00:00+00:00",
            route_type="A",
            entry_price_impact_pct=0.2,
            exit_price_impact_pct=0.4,
            expected_slippage_bps=120,
            entry_latency_ms=2_000,
        ),
        event(
            9_479,
            "SELL",
            position_id="P2",
            mint="M2",
            token_amount_raw=10,
            proceeds_lamports=90,
            realized_pnl_lamports=-10,
            at="2026-09-19T04:00:00+00:00",
            trigger_price_impact_pct=0.5,
            exit_trigger_latency_ms=400,
            quote_age_ms=300,
        ),
        event(
            9_480,
            "BUY",
            position_id="P3",
            mint="M3",
            cost_lamports=100,
            token_amount_raw=10,
            at="2026-09-20T05:00:00+00:00",
            route_type="B",
            entry_price_impact_pct=0.3,
            exit_price_impact_pct=0.5,
            expected_slippage_bps=140,
            entry_latency_ms=3_000,
        ),
        event(
            9_481,
            "SELL",
            position_id="P3",
            mint="M3",
            token_amount_raw=2,
            proceeds_lamports=22,
            realized_pnl_lamports=2,
            at="2026-09-21T06:00:00+00:00",
            trigger_price_impact_pct=0.7,
            exit_trigger_latency_ms=600,
            quote_age_ms=500,
        ),
    ]
    return {
        "schema_version": 2,
        "version": 10,
        "next_event_seq": 9_482,
        "cash_lamports": 1_000,
        "positions": {
            "M3": {
                "position_id": "P3",
                "mint": "M3",
                "token_amount_raw": 8,
                "remaining_cost_lamports": 80,
                "current_value_lamports": 70,
                "price_updated_at": "2026-09-23T08:59:00+00:00",
                "risk_state": "NORMAL",
            },
        },
        "events": events,
    }


class PaperMvpReviewTests(unittest.TestCase):
    def test_sequence_cohort_carry_in_and_open_position_are_separate(self) -> None:
        report = build_paper_mvp_review(ledger(), generated_at=BEFORE_REVIEW)

        self.assertEqual(report["verdict"], "NOT_DUE")
        self.assertFalse(report["review_due"])
        self.assertEqual(report["cohort_counts"]["new_buy_count"], 3)
        self.assertEqual(report["cohort_counts"]["post_cutoff_sell_event_count"], 4)
        self.assertEqual(
            report["cohort_counts"]["new_entry_cohort_sell_event_count"], 3
        )
        self.assertEqual(report["cohort_counts"]["completed_position_count"], 2)
        self.assertEqual(report["cohort_counts"]["open_position_count"], 1)
        self.assertEqual(
            report["cohort_counts"]["total_current_open_position_count"], 1
        )
        self.assertEqual(report["cohort_counts"]["carry_in_sell_event_count"], 1)
        self.assertEqual(report["realized_pnl"]["new_entry_cohort_lamports"], 12)
        self.assertEqual(report["realized_pnl"]["carry_in_lamports"], 5)
        self.assertEqual(report["realized_pnl"]["mvp_delta_lamports"], 17)
        self.assertTrue(
            report["realized_pnl"]["retained_total_reconciles_with_authoritative_delta"]
        )
        self.assertEqual(report["unrealized_pnl"]["unrealized_pnl_lamports"], -10)

    def test_completed_metrics_drawdown_extreme_and_family_views(self) -> None:
        report = build_paper_mvp_review(ledger(), generated_at=BEFORE_REVIEW)
        metrics = report["completed_performance"]

        self.assertEqual(metrics["win_count"], 1)
        self.assertEqual(metrics["loss_count"], 1)
        self.assertEqual(metrics["win_rate_percent"], 50.0)
        self.assertEqual(metrics["expectancy_lamports"], 5.0)
        self.assertEqual(metrics["profit_factor"], 2.0)
        self.assertEqual(metrics["median_return_percent"], 5.0)
        self.assertEqual(metrics["max_realized_drawdown_lamports"], 10)
        self.assertEqual(metrics["max_loss_lamports"], -10)
        self.assertEqual(metrics["largest_winner_lamports"], 20)
        self.assertEqual(metrics["largest_winner_contribution_percent"], 100.0)
        self.assertEqual(metrics["largest_winner_removed_expectancy_lamports"], -10.0)
        self.assertEqual(
            report["family_performance"]["MOMENTUM"]["realized_pnl_lamports"],
            20,
        )
        self.assertEqual(
            report["family_performance"]["SMART_MONEY"]["realized_pnl_lamports"],
            -10,
        )
        self.assertEqual(report["chronological_performance"]["completed_active_day_count"], 2)
        self.assertEqual(
            report["chronological_performance"]["early_half"]["realized_pnl_lamports"],
            20,
        )
        self.assertEqual(
            report["chronological_performance"]["late_half"]["realized_pnl_lamports"],
            -10,
        )

    def test_execution_fee_and_exposure_contract_is_explicit(self) -> None:
        report = build_paper_mvp_review(ledger(), generated_at=BEFORE_REVIEW)

        self.assertEqual(report["exposure"]["new_entry_cost_lamports"], 300)
        self.assertEqual(report["exposure"]["open_remaining_cost_lamports"], 80)
        self.assertEqual(report["execution_diagnostics"]["entry_latency_ms"]["count"], 3)
        self.assertEqual(report["execution_diagnostics"]["exit_quote_age_ms"]["count"], 3)
        self.assertIsNone(report["fee_accounting"]["network_fee_lamports"])
        self.assertFalse(report["fee_accounting"]["paper_ledger_explicit_fee_deduction"])
        self.assertFalse(report["automatic_trading_changes"])
        self.assertTrue(report["nav"]["current_available"])
        self.assertEqual(report["nav"]["current_nav_lamports"], 1_070)
        self.assertIsNone(report["nav"]["nav_delta_lamports"])
        self.assertEqual(
            report["nav"]["nav_delta_unavailable_reason"],
            "PRE_MVP_BASELINE_NAV_NOT_FROZEN_IN_PAPER_MVP_CONTRACT",
        )

        report = build_paper_mvp_review(
            ledger(),
            generated_at=BEFORE_REVIEW,
            baseline_nav_lamports=1_100,
        )
        self.assertEqual(report["nav"]["baseline_nav_lamports"], 1_100)
        self.assertEqual(report["nav"]["baseline_nav_source"], "EXPLICIT_ARGUMENT")
        self.assertEqual(report["nav"]["nav_delta_lamports"], -30)
        self.assertIsNone(report["nav"]["nav_delta_unavailable_reason"])

    def test_missing_open_mark_is_reported_not_converted_to_zero(self) -> None:
        source = ledger()
        source["positions"]["M3"].pop("current_value_lamports")
        report = build_paper_mvp_review(source, generated_at=BEFORE_REVIEW)

        self.assertFalse(report["unrealized_pnl"]["available"])
        self.assertIsNone(report["unrealized_pnl"]["unrealized_pnl_lamports"])
        self.assertEqual(report["unrealized_pnl"]["missing_position_ids"], ["P3"])
        self.assertFalse(report["nav"]["current_available"])
        self.assertIsNone(report["nav"]["current_nav_lamports"])

    def test_retention_gap_fails_closed(self) -> None:
        source = ledger()
        source["events"] = [
            item for item in source["events"] if item["event_seq"] != CUTOFF + 1
        ]
        with self.assertRaisesRegex(RuntimeError, "cohort is incomplete"):
            build_paper_mvp_review(source, generated_at=BEFORE_REVIEW)

        source = ledger()
        source["events"] = [
            item for item in source["events"] if item["event_seq"] != CUTOFF + 3
        ]
        with self.assertRaisesRegex(RuntimeError, "event sequence has a gap"):
            build_paper_mvp_review(source, generated_at=BEFORE_REVIEW)

    def test_partial_closed_linkage_fails_closed(self) -> None:
        source = ledger()
        source["positions"] = {}
        with self.assertRaisesRegex(RuntimeError, "SELL linkage is incomplete"):
            build_paper_mvp_review(source, generated_at=BEFORE_REVIEW)

    def test_open_token_linkage_fails_closed(self) -> None:
        source = ledger()
        source["positions"]["M3"]["token_amount_raw"] = 7
        with self.assertRaisesRegex(RuntimeError, "token linkage is incomplete"):
            build_paper_mvp_review(source, generated_at=BEFORE_REVIEW)

    def test_sequence_and_schema_malformation_fail_closed(self) -> None:
        source = ledger()
        source["events"][6]["event_seq"] = source["events"][5]["event_seq"]
        with self.assertRaisesRegex(RuntimeError, "duplicated or out of order"):
            build_paper_mvp_review(source, generated_at=BEFORE_REVIEW)

        source = ledger()
        source["schema_version"] = 999
        with self.assertRaisesRegex(RuntimeError, "schema is unsupported"):
            build_paper_mvp_review(source, generated_at=BEFORE_REVIEW)

        source = ledger()
        source["events"][6]["event_seq"] = 9_476.5
        with self.assertRaisesRegex(RuntimeError, "event_seq is malformed"):
            build_paper_mvp_review(source, generated_at=BEFORE_REVIEW)

        source = ledger()
        source["events"] = source["events"][:1]
        source["next_event_seq"] = 9474
        with self.assertRaisesRegex(RuntimeError, "has not reached"):
            build_paper_mvp_review(source, generated_at=BEFORE_REVIEW)

    def test_due_review_uses_official_enum_without_inventing_sample_gate(self) -> None:
        report = build_paper_mvp_review(ledger(), generated_at=AFTER_REVIEW)
        self.assertEqual(report["verdict"], "INSUFFICIENT_MVP_SAMPLE")
        self.assertEqual(
            report["verdict_reason"],
            "NUMERIC_SAMPLE_THRESHOLD_NOT_DEFINED_IN_PAPER_MVP_CONTRACT",
        )
        self.assertEqual(
            report["official_due_verdicts"],
            [
                "POSITIVE_MVP_SIGNAL",
                "NEGATIVE_MVP_SIGNAL",
                "INSUFFICIENT_MVP_SAMPLE",
            ],
        )

        report = build_paper_mvp_review(
            ledger(),
            generated_at=AFTER_REVIEW,
            minimum_completed_positions=5,
        )
        self.assertEqual(report["verdict"], "INSUFFICIENT_MVP_SAMPLE")
        with self.assertRaisesRegex(RuntimeError, "minimum_completed_positions"):
            build_paper_mvp_review(
                ledger(),
                generated_at=AFTER_REVIEW,
                minimum_completed_positions=0,
            )

    def test_odd_chronological_remainder_belongs_to_late_half(self) -> None:
        source = ledger()
        source["events"][8]["type"] = "SIGNAL_REJECTED"
        source["events"][9]["type"] = "SIGNAL_REJECTED"
        report = build_paper_mvp_review(source, generated_at=BEFORE_REVIEW)

        self.assertEqual(
            report["chronological_performance"]["early_half"][
                "completed_position_count"
            ],
            0,
        )
        self.assertEqual(
            report["chronological_performance"]["late_half"][
                "completed_position_count"
            ],
            1,
        )

    def test_builder_does_not_mutate_ledger(self) -> None:
        source = ledger()
        before = copy.deepcopy(source)
        build_paper_mvp_review(source, generated_at=BEFORE_REVIEW)
        self.assertEqual(source, before)

    def test_dash_input_reads_json_from_stdin_without_file_access(self) -> None:
        source = ledger()
        with mock.patch("sys.stdin", io.StringIO(json.dumps(source))):
            loaded = read_paper_ledger_snapshot(Path("-"))
        self.assertEqual(loaded, source)

        with mock.patch("sys.stdin", io.StringIO("[]")):
            with self.assertRaisesRegex(RuntimeError, "must be a JSON object"):
                read_paper_ledger_snapshot(Path("-"))


if __name__ == "__main__":
    unittest.main()
