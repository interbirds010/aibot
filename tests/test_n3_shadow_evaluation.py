"""N3 종료 코호트의 정규화·짝 비교·사전 고정 판정을 검증한다."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest

from src.research.n3_shadow_evaluation import (
    DEFAULT_SAMPLE_GATES,
    evaluate_closed_cohort,
    sample_counts,
)


SOL = 1_000_000_000


def trade(index: int, *, skip: bool | None = True, pnl: int = -100_000_000,
          cost: int = SOL, day: int | None = None, family: str = "MOMENTUM") -> dict:
    signal = datetime(2026, 10, 3, tzinfo=timezone.utc) + timedelta(
        days=index // 10 if day is None else day, seconds=index
    )
    return {
        "position_id": f"position-{index}",
        "buy_event_seq": index,
        "signal_timestamp": signal.isoformat(),
        "closed_at": (signal + timedelta(minutes=30)).isoformat(),
        "family": family,
        "entry_cost_lamports": cost,
        "realized_pnl_lamports": pnl,
        "would_skip": skip,
    }


def cohort() -> list[dict]:
    return [trade(index, skip=index % 2 == 0,
                  pnl=-100_000_000 if index % 2 == 0 else 50_000_000)
            for index in range(200)]


def contract(*, small: bool = False) -> dict:
    return {
        "sample_gates": {key: 1 for key in DEFAULT_SAMPLE_GATES} if small else dict(DEFAULT_SAMPLE_GATES),
        "route_sample_gates": dict(DEFAULT_SAMPLE_GATES),
        "bootstrap": {"seed": 20261003, "repetitions": 200, "confidence": 0.95},
        "cost_scenarios": [],
    }


class N3ShadowEvaluationTests(unittest.TestCase):
    def test_loss_avoidance_is_positive_paired_effect(self) -> None:
        report = evaluate_closed_cohort(cohort(), contract())
        normalized = report["primary_effect"]["normalized"]
        self.assertAlmostEqual(normalized["avoided_losses"], 10)
        self.assertEqual(normalized["missed_winners"], 0)
        self.assertAlmostEqual(normalized["mean_delta"], 0.05)
        self.assertEqual(report["verdict"], "ROBUST_NEGATIVE_FILTER")
        self.assertEqual(report["remaining"]["normalized"]["trade_count"], 100)
        self.assertEqual(report["remaining"]["normalized"]["win_rate"], 1)
        self.assertAlmostEqual(report["remaining"]["normalized"]["expectancy"], 0.05)
        self.assertEqual(report["remaining"]["strategy_alpha"]["verdict"], "POSITIVE_EXPECTANCY_EVIDENCE")
        self.assertEqual(report["filter_counts"]["hit_losers"], 100)
        self.assertEqual(report["filter_counts"]["hit_winners"], 0)
        self.assertEqual(report["filter_counts"]["total_losers"], 100)
        self.assertEqual(report["filter_counts"]["hit_coverage"], 0.5)
        self.assertEqual(report["filter_counts"]["loser_exclusion_rate"], 1)
        self.assertEqual(report["filter_counts"]["winner_exclusion_rate"], 0)
        self.assertEqual(report["primary_effect"]["actual"]["net_effect_sol"], 10)

    def test_missed_winners_are_negative_and_nonhits_are_zero_delta(self) -> None:
        rows = [trade(0, pnl=200_000_000), trade(1, skip=False, pnl=SOL)]
        report = evaluate_closed_cohort(rows, contract(small=True))
        self.assertAlmostEqual(report["primary_effect"]["normalized"]["net_effect"], -0.2)
        self.assertAlmostEqual(report["primary_effect"]["mean_normalized_delta"], -0.1)
        self.assertEqual(report["primary_effect"]["actual"]["missed_winners"], 200_000_000)
        self.assertEqual(report["verdict"], "NEGATIVE_FILTER_FAILED")

    def test_normalization_and_actual_size_conflict_block_robust(self) -> None:
        rows = cohort()
        for index, row in enumerate(rows):
            if row["would_skip"] and index % 4 == 0:
                row["entry_cost_lamports"] = 100 * SOL
                row["realized_pnl_lamports"] = SOL
        report = evaluate_closed_cohort(rows, contract())
        self.assertGreater(report["primary_effect"]["normalized"]["net_effect"], 0)
        self.assertLess(report["primary_effect"]["actual"]["net_effect"], 0)
        self.assertEqual(report["verdict"], "PROMISING_NEGATIVE_FILTER")
        self.assertEqual(report["normalization"]["entry_lamports"], SOL)

    def test_equal_return_ignores_position_size(self) -> None:
        rows = [trade(0, cost=SOL, pnl=-SOL // 10),
                trade(1, cost=20 * SOL, pnl=-2 * SOL),
                trade(2, skip=False, pnl=SOL // 20)]
        report = evaluate_closed_cohort(rows, contract(small=True))
        self.assertAlmostEqual(report["primary_effect"]["normalized"]["net_effect"], 0.2)
        self.assertEqual(report["primary_effect"]["actual"]["net_effect"], 2_100_000_000)

    def test_each_default_gate_is_required(self) -> None:
        base = cohort()
        for gate in DEFAULT_SAMPLE_GATES:
            with self.subTest(gate=gate):
                specification = contract()
                specification["sample_gates"][gate] = sample_counts(base)[gate] + 1
                report = evaluate_closed_cohort(base, specification)
                self.assertFalse(report["sample_gates_passed"])
                self.assertEqual(report["verdict"], "INSUFFICIENT_FRESH_SAMPLE")
                self.assertEqual(report["remaining"]["strategy_alpha"]["verdict"], "INSUFFICIENT_SAMPLE")

    def test_active_days_use_kst_signal_buy_date_not_sell_date(self) -> None:
        rows = [trade(0), trade(1, skip=False, pnl=SOL)]
        rows[0]["signal_timestamp"] = "2026-10-03T14:59:59+00:00"
        rows[1]["signal_timestamp"] = "2026-10-03T15:00:00Z"
        for row in rows:
            row["closed_at"] = "2026-10-10T00:00:00Z"
        self.assertEqual(sample_counts(rows)["active_kst_days"], 2)

    def test_missing_stratum_counts_control_and_blocks_robust(self) -> None:
        rows = cohort() + [trade(200, skip=None, pnl=SOL)]
        report = evaluate_closed_cohort(rows, contract())
        self.assertEqual(report["sample_counts"]["completed_control"], 201)
        self.assertEqual(report["sample_counts"]["control_winners"], 101)
        self.assertEqual(report["missing_would_skip_count"], 1)
        self.assertEqual(report["valid_contrast_count"], 200)
        self.assertEqual(report["unknown_stratum"]["actual"]["trade_count"], 1)
        self.assertAlmostEqual(report["primary_effect"]["mean_normalized_delta"], 0.05)
        self.assertEqual(report["remaining"]["normalized"]["trade_count"], 101)
        self.assertAlmostEqual(report["remaining"]["normalized"]["total_pnl"], 6)
        self.assertEqual(report["filter_counts"]["winner_exclusion_rate"], 0)
        self.assertEqual(report["verdict"], "PROMISING_NEGATIVE_FILTER")

    def test_unknown_loss_remains_in_strategy_and_cost_sensitivity(self) -> None:
        rows = [trade(0), trade(1, skip=False, pnl=50_000_000),
                trade(2, skip=None, pnl=-200_000_000)]
        specification = contract(small=True)
        specification["cost_scenarios"] = [{"name": "extra", "extra_round_trip_bps": 100,
                                             "fixed_round_trip_lamports": 10_000_000}]
        report = evaluate_closed_cohort(rows, specification)
        self.assertEqual(report["filter_counts"]["total_losers"], 2)
        self.assertEqual(report["filter_counts"]["loser_exclusion_rate"], 0.5)
        self.assertAlmostEqual(report["primary_effect"]["normalized"]["net_effect"], 0.1)
        self.assertEqual(report["remaining"]["normalized"]["trade_count"], 2)
        self.assertAlmostEqual(report["remaining"]["normalized"]["total_pnl"], -0.15)
        self.assertEqual(report["remaining"]["strategy_alpha"]["verdict"], "NO_POSITIVE_EXPECTANCY_EVIDENCE")
        cost = report["cost_scenarios"][0]
        self.assertEqual(cost["remaining"]["actual"]["trade_count"], 2)
        self.assertEqual(cost["remaining"]["actual"]["total_pnl"], -190_000_000)

    def test_temporal_failure_uses_buy_order_even_if_input_and_sells_reversed(self) -> None:
        rows = cohort()
        for index, row in enumerate(rows):
            if index >= 100 and row["would_skip"]:
                row["realized_pnl_lamports"] = 10_000_000
            row["closed_at"] = (datetime(2027, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=200-index)).isoformat()
        report = evaluate_closed_cohort(list(reversed(rows)), contract())
        self.assertEqual([half["trade_count"] for half in report["temporal_halves"]], [100, 100])
        self.assertEqual(report["temporal_halves"][0]["hit_losers"], 50)
        self.assertEqual(report["temporal_halves"][1]["hit_winners"], 50)
        self.assertGreater(report["primary_effect"]["normalized"]["net_effect"], 0)
        self.assertLess(report["temporal_halves"][1]["normalized"]["net_effect"], 0)
        self.assertEqual(report["verdict"], "NEGATIVE_FILTER_FAILED")

    def test_top_two_fragility_fails_and_top_three_only_is_promising(self) -> None:
        for loss_count, expected in ((2, "NEGATIVE_FILTER_FAILED"), (3, "PROMISING_NEGATIVE_FILTER")):
            rows = cohort()
            for row in rows:
                if row["would_skip"]:
                    row["realized_pnl_lamports"] = 0
            for index in (0, 100, 102)[:loss_count]:
                rows[index]["realized_pnl_lamports"] = -100_000_000
            rows[2]["realized_pnl_lamports"] = 50_000_000
            report = evaluate_closed_cohort(rows, contract())
            self.assertTrue(all(half["normalized"]["net_effect"] > 0
                                for half in report["temporal_halves"]))
            self.assertEqual(report["verdict"], expected)
            self.assertLessEqual(report["tail_removal"]["normalized"][str(loss_count)]["net_effect"], 0)

    def test_filter_evidence_does_not_imply_profitable_strategy(self) -> None:
        rows = cohort()
        for index, row in enumerate(rows):
            if not row["would_skip"]:
                row["realized_pnl_lamports"] = -50_000_000
            elif index < 40:
                row["realized_pnl_lamports"] = 10_000_000
        report = evaluate_closed_cohort(rows, contract())
        self.assertEqual(report["verdict"], "ROBUST_NEGATIVE_FILTER")
        self.assertLess(report["remaining"]["normalized"]["expectancy"], 0)
        self.assertEqual(report["remaining"]["strategy_alpha"]["verdict"],
                         "NO_POSITIVE_EXPECTANCY_EVIDENCE")

    def test_actual_and_normalized_tail_rankings_are_independent(self) -> None:
        rows = [trade(0, cost=100 * SOL, pnl=-SOL),
                trade(1, cost=SOL, pnl=-SOL // 2),
                trade(2, skip=False, pnl=SOL)]
        report = evaluate_closed_cohort(rows, contract(small=True))
        self.assertAlmostEqual(report["tail_removal"]["normalized"]["1"]["removed_positive_delta"], 0.5)
        self.assertEqual(report["tail_removal"]["actual"]["1"]["removed_positive_delta"], SOL)

    def test_cost_scenario_recomputes_fixed_and_proportional_costs(self) -> None:
        rows = [trade(0, cost=2 * SOL, pnl=-200_000_000),
                trade(1, skip=False, cost=SOL, pnl=50_000_000)]
        specification = contract(small=True)
        specification["cost_scenarios"] = [{"name": "fixed", "extra_round_trip_bps": 100,
                                             "fixed_round_trip_lamports": 10_000_000}]
        scenario = evaluate_closed_cohort(rows, specification)["cost_scenarios"][0]
        self.assertEqual(scenario["actual"]["net_effect"], 230_000_000)
        self.assertAlmostEqual(scenario["normalized"]["net_effect"], 0.115)
        self.assertEqual(scenario["remaining"]["actual"]["total_pnl"], 30_000_000)
        self.assertAlmostEqual(scenario["remaining"]["normalized"]["expectancy"], 0.03)

    def test_routes_require_their_own_full_sample_gates(self) -> None:
        rows = cohort()
        for index, row in enumerate(rows):
            row["family"] = "MOMENTUM" if index < 100 else "SMART_MONEY"
        report = evaluate_closed_cohort(rows, contract())
        self.assertTrue(report["sample_gates_passed"])
        for route in report["routes"].values():
            self.assertEqual(route["verdict"], "INSUFFICIENT_ROUTE_SAMPLE")
            self.assertEqual(route["sample_counts"]["completed_control"], 100)

    def test_drawdown_is_ordered_by_sell_timestamp(self) -> None:
        rows = [trade(0, skip=False, pnl=300_000_000),
                trade(1, skip=False, pnl=-200_000_000),
                trade(2, skip=False, pnl=-200_000_000)]
        rows[0]["closed_at"] = "2026-11-01T03:00:00Z"
        rows[1]["closed_at"] = "2026-11-01T01:00:00Z"
        rows[2]["closed_at"] = "2026-11-01T02:00:00Z"
        report = evaluate_closed_cohort(rows, contract(small=True))
        self.assertAlmostEqual(report["remaining"]["normalized"]["max_drawdown"], 0.4)
        self.assertEqual(report["remaining"]["actual"]["max_drawdown"], 400_000_000)
        self.assertAlmostEqual(report["remaining"]["normalized"]["profit_factor"], 0.75)
        self.assertEqual(report["remaining"]["strategy_alpha"]["verdict"], "INSUFFICIENT_SAMPLE")

    def test_bootstrap_whole_days_reproducible_and_not_trade_sampling(self) -> None:
        rows = [trade(index, pnl=(-SOL if index < 10 else SOL), day=0 if index < 10 else 1)
                for index in range(20)]
        specification = contract(small=True)
        specification["bootstrap"]["repetitions"] = 5000
        first = evaluate_closed_cohort(rows, specification)
        second = evaluate_closed_cohort(list(reversed(rows)), specification)
        self.assertEqual(first["bootstrap"], second["bootstrap"])
        self.assertEqual(first["bootstrap"]["cluster_count"], 2)
        self.assertEqual(first["bootstrap"]["mean_delta_ci"], [-1.0, 1.0])
        self.assertEqual(first["bootstrap"]["valid_effect_repetitions"], 5000)

    def test_empty_and_all_unknown_are_disclosed_without_invented_ci(self) -> None:
        for rows in ([], [trade(0, skip=None)]):
            report = evaluate_closed_cohort(rows, contract())
            self.assertIsNone(report["primary_effect"]["mean_normalized_delta"])
            self.assertEqual(report["bootstrap"]["mean_delta_ci"], [None, None])
            self.assertEqual(report["verdict"], "INSUFFICIENT_FRESH_SAMPLE")

    def test_inputs_unchanged_and_invalid_data_rejected(self) -> None:
        rows = cohort()
        specification = contract()
        original = deepcopy((rows, specification))
        evaluate_closed_cohort(rows, specification)
        self.assertEqual((rows, specification), original)
        for change in ({"entry_cost_lamports": 0}, {"would_skip": 1},
                       {"signal_timestamp": "2026-10-03T00:00:00"},
                       {"realized_pnl_lamports": True}, {"family": "UNKNOWN"}):
            malformed = [dict(trade(0), **change)]
            with self.subTest(change=change), self.assertRaises(ValueError):
                evaluate_closed_cohort(malformed, specification)
        with self.assertRaises(ValueError):
            evaluate_closed_cohort([trade(index) for index in range(409)], specification)
        with self.assertRaises(ValueError):
            evaluate_closed_cohort([trade(0), trade(0)], specification)


if __name__ == "__main__":
    unittest.main()
