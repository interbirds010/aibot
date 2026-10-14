"""승자 기회비용, 크기 정규화, 사후 누출 및 고정 미래 검증을 확인한다."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import unittest

from src.research.alpha_batch_forensics import (
    build_forensic_report, cost_stress_report, discovery_screen, economic_report, evaluate_stage,
)

SOL = 1_000_000_000
BASE = datetime(2026, 10, 7, tzinfo=timezone.utc)


def trade(index, *, pnl=None, cost=SOL, feature=True):
    signal = BASE + timedelta(minutes=index * 10)
    entry = signal + timedelta(seconds=5)
    row = {"position_id": f"p-{index}", "buy_event_seq": index + 100,
           "signal_timestamp": signal.isoformat(), "entry_decision_timestamp": entry.isoformat(),
           "closed_at": (entry + timedelta(minutes=5)).isoformat(),
           "entry_cost_lamports": cost,
           "realized_pnl_lamports": pnl if pnl is not None else (-SOL // 10 if index % 2 == 0 else SOL // 5),
           "features": {}}
    if feature:
        row["features"]["liquidity_usd"] = {"value": 1000 if index % 2 == 0 else 20_000,
                                            "observed_at": signal.isoformat(), "source": "entry_snapshot"}
    return row


def definition(**kwargs):
    return {"candidate_id": "liq-v1", "version": 1, "kind": "negative_filter",
            "rule": {"feature": "liquidity_usd", "op": "lt", "threshold": 5000},
            "creation_batch": "BATCH_001", "start_event_seq": 100,
            "start_timestamp": BASE.isoformat(),
            "discovery_evidence": {"position_ids": ["discovery-old"], "validation_policy": {
                "minimum_decision_coverage": 0.8, "minimum_exclusion_coverage": 0.1,
                "maximum_missing_predictor_rate": 0.2}}, **kwargs}


class EconomicsTests(unittest.TestCase):
    def test_opportunity_cost_is_subtracted_and_unknown_is_not_a_skip(self):
        rows = [trade(0, pnl=-SOL), trade(1, pnl=2 * SOL), trade(2, pnl=-3 * SOL)]
        result = economic_report(rows, {"p-0": False, "p-1": False, "p-2": None})
        self.assertEqual(result["effects"]["actual_sol"]["avoided_loss"], 1)
        self.assertEqual(result["effects"]["actual_sol"]["missed_profit"], 2)
        self.assertEqual(result["effects"]["actual_sol"]["net_effect"], -1)
        self.assertEqual(result["unknown_trades"], 1)
        self.assertEqual(result["winners_excluded"], 1)
        self.assertEqual(result["losers_excluded"], 1)

    def test_normalization_reveals_actual_size_sign_conflict(self):
        rows = [trade(0, cost=SOL, pnl=-SOL // 2), trade(1, cost=100 * SOL, pnl=SOL), trade(2)]
        result = economic_report(rows, {"p-0": False, "p-1": False, "p-2": True})
        self.assertEqual(result["effects"]["actual_sol"]["net_effect"], -.5)
        self.assertAlmostEqual(result["effects"]["equal_notional"]["net_effect"], .49)
        self.assertTrue(result["concentration"]["actual_normalized_effect_sign_disagreement"])

    def test_true_zero_infinity_and_undefined_are_json_safe(self):
        loss = economic_report([trade(0, pnl=-SOL)], {"p-0": True})
        win = economic_report([trade(0, pnl=SOL)], {"p-0": True})
        zero = economic_report([trade(0, pnl=0)], {"p-0": True})
        self.assertEqual(loss["included"]["actual"]["profit_factor"], 0)
        self.assertEqual(win["included"]["actual"]["profit_factor_state"], "POSITIVE_INFINITY")
        self.assertIsNone(win["included"]["actual"]["profit_factor"])
        self.assertEqual(zero["included"]["actual"]["profit_factor_state"], "UNDEFINED")
        self.assertEqual(zero["included"]["actual"]["expectancy"], 0)
        json.dumps([loss, win, zero], allow_nan=False)

    def test_drawdown_uses_close_order_and_equal_notional(self):
        rows = [trade(0, pnl=-SOL // 2), trade(1, cost=2 * SOL, pnl=SOL)]
        rows[0]["closed_at"] = (BASE + timedelta(hours=1)).isoformat()
        result = economic_report(rows, {"p-0": True, "p-1": True})
        self.assertEqual(result["included"]["normalized"]["max_drawdown"], .5)
        self.assertEqual(result["included"]["normalized"]["mean_return_percent"], 0)
        self.assertEqual(result["included"]["actual"]["total_pnl"], .5)

    def test_cost_stress_does_not_double_count_recorded_cost(self):
        rows = [trade(0, pnl=SOL // 10)]
        result = cost_stress_report(rows, {"p-0": True})
        self.assertEqual(result["scenarios"][0]["economic_report"]["included"]["actual"]["total_pnl"], .1)
        self.assertAlmostEqual(result["scenarios"][1]["economic_report"]["included"]["actual"]["total_pnl"], .095)
        self.assertEqual(result["semantics"], "SENSITIVITY_NOT_REAL_FILL")
        self.assertFalse(result["recorded_costs_double_counted"])
        with self.assertRaises(ValueError):
            cost_stress_report(rows, {"p-0": True}, scenarios=[{"extra_round_trip_bps": -1, "fixed_round_trip_lamports": 0}])


class ForensicTests(unittest.TestCase):
    def test_discovery_screens_explicit_all_axes_and_missing_route(self):
        from tests.test_accelerated_alpha import SCREEN_POLICY
        rows = [trade(index) for index in range(20)]
        for row in rows:
            row["features"]["route_type"] = {"value": "A", "observed_at": row["signal_timestamp"], "source": "entry_snapshot"}
        rule = definition()["rule"]
        self.assertEqual(discovery_screen(rows, rule, "negative_filter")["status"], "REJECT_DISCOVERY")
        self.assertEqual(discovery_screen(rows, rule, "negative_filter", SCREEN_POLICY)["status"], "PASS_DISCOVERY")
        checks = {"minimum_sample": (21, "MINIMUM_DISCOVERY_SAMPLE_OR_AFFECTED_FAILED"),
                  "maximum_single_trade_share": (.01, "SINGLE_TRADE_DEPENDENCE_FAILED"),
                  "maximum_single_day_share": (.5, "SINGLE_DAY_DEPENDENCE_FAILED"),
                  "maximum_route_share": (.5, "ROUTE_DEPENDENCE_FAILED"),
                  "maximum_position_size_share": (.5, "POSITION_SIZE_DEPENDENCE_FAILED")}
        for key, (limit, reason) in checks.items():
            with self.subTest(key=key):
                result = discovery_screen(rows, rule, "negative_filter", {**SCREEN_POLICY, key: limit})
                self.assertEqual(result["status"], "REJECT_DISCOVERY")
                self.assertIn(reason, result["reasons"])
        missing = deepcopy(rows)
        for row in missing:
            row["features"].pop("route_type")
        self.assertIn("ROUTE_DEPENDENCE_UNMEASURED", discovery_screen(missing, rule, "negative_filter", SCREEN_POLICY)["reasons"])
        for row in missing[:10]:
            row["features"] = {}
        self.assertIn("FEATURE_MISSINGNESS_FAILED", discovery_screen(missing, rule, "negative_filter", {**SCREEN_POLICY, "maximum_missing_predictor_rate": .1})["reasons"])
        self.assertIn("EXTREME_THRESHOLD_FAILED", discovery_screen(rows, {**rule, "threshold": 500}, "negative_filter", {**SCREEN_POLICY, "minimum_threshold_tail_fraction": .1})["reasons"])
        self.assertIn("WINNER_DESTRUCTION_FAILED_OR_UNMEASURED", discovery_screen(rows, {**rule, "op": "gt"}, "negative_filter", {**SCREEN_POLICY, "maximum_winner_profit_destroyed_share": .5})["reasons"])

    def test_winner_labels_use_explicit_contract_and_sampled_trajectory(self):
        row = trade(1)
        row["features"]["buy_sell_ratio_m5"] = {"value": 3, "observed_at": row["signal_timestamp"], "source": "entry_snapshot"}
        defaults = build_forensic_report([row], "BATCH_001")["trades"][0]
        self.assertTrue(all(value == "UNKNOWN" for value in defaults["winner_labels"].values()))
        entry = datetime.fromisoformat(row["entry_decision_timestamp"])
        row["forensic"] = {"price_trajectory": [{"observed_at": (entry + timedelta(seconds=seconds)).isoformat(), "return_percent": value}
                                                for seconds, value in ((10, 1), (20, 10), (30, 20))]}
        contract = {"tp_percent": 20, "fast_tp_seconds": 60, "continuation_min_samples": 3,
                    "continuation_max_pullback_percent": 2, "large_winner_percent": 15,
                    "long_hold_seconds": 200, "low_liquidity_max_usd": 25000, "high_flow_min_ratio": 2}
        labelled = build_forensic_report([row], "BATCH_001", label_contract=contract)["trades"][0]
        self.assertTrue(all(value is True for value in labelled["winner_labels"].values()))
        row["forensic"] = {}
        no_path = build_forensic_report([row], "BATCH_001", label_contract=contract)["trades"][0]
        self.assertEqual(no_path["winner_labels"]["fast_tp"], "UNKNOWN")
        self.assertEqual(no_path["winner_labels"]["gradual_continuation"], "UNKNOWN")

    def test_winners_and_losers_both_present_and_unmeasured_labels_unknown(self):
        report = build_forensic_report([trade(0), trade(1)], "BATCH_001")
        self.assertEqual(report["population"], {"completed": 2, "winners": 1, "losers": 1, "breakeven": 0})
        self.assertEqual([row["outcome"] for row in report["trades"]], ["LOSER", "WINNER"])
        self.assertEqual(report["trades"][0]["holding_seconds"], 300)
        self.assertTrue(all(label == "UNKNOWN" for label in report["trades"][0]["labels"].values()))
        self.assertIsNone(report["trades"][0]["sampled_mae_percent"])
        self.assertEqual(report["telemetry_contract"]["wallet"]["any_available_trades"], 0)

    def test_timestamp_leakage_and_outcome_feature_excluded_from_search(self):
        rows = [trade(index) for index in range(10)]
        for row in rows:
            row["features"]["sampled_mfe_percent"] = {"value": 100, "observed_at": row["signal_timestamp"], "source": "post_trade"}
            row["features"]["late_flow"] = {"value": 2, "observed_at": row["closed_at"], "source": "post_trade"}
            row["features"]["undated"] = {"value": 2, "source": "unknown"}
        report = build_forensic_report(rows, "BATCH_001")
        self.assertEqual(report["feature_comparison"]["sampled_mfe_percent"]["all"]["available"], 0)
        self.assertEqual(report["feature_comparison"]["late_flow"]["all"]["available"], 0)
        self.assertEqual(report["feature_comparison"]["undated"]["all"]["available"], 0)
        self.assertNotIn("sampled_mfe_percent", report["trades"][0]["pre_entry_features"])
        for proposal in report["candidate_generation"]["proposals"]:
            self.assertNotIn("late_flow", str(proposal["rule"]))
            self.assertNotIn("mfe", str(proposal["rule"]))

    def test_decision_time_snapshot_valid_before_buy_and_missing_coverage(self):
        rows = [trade(0), trade(1, feature=False)]
        rows[0]["features"]["liquidity_usd"]["observed_at"] = rows[0]["entry_decision_timestamp"]
        report = build_forensic_report(rows, "BATCH_001")
        self.assertEqual(report["feature_comparison"]["liquidity_usd"]["all"]["coverage"], .5)

    def test_sampled_trajectory_only_labels_when_contract_explicit(self):
        row = trade(0)
        entry = datetime.fromisoformat(row["entry_decision_timestamp"])
        row["forensic"] = {"exit_reason": "SL", "momentum_signal_at_entry": True,
                           "price_trajectory": [{"observed_at": (entry + timedelta(seconds=30)).isoformat(), "return_percent": 20},
                                                {"observed_at": (entry + timedelta(seconds=90)).isoformat(), "return_percent": -10}]}
        default = build_forensic_report([row], "BATCH_001")["trades"][0]
        self.assertEqual(default["sampled_mfe_percent"], 20)
        self.assertEqual(default["sampled_mae_percent"], -10)
        self.assertEqual(default["labels"]["reversal"], "UNKNOWN")
        contract = {"rapid_sl_seconds": 600, "reversal_peak_percent": 10, "reversal_drop_percent": 20,
                    "tp_percent": 30, "near_tp_distance_percent": 10, "small_loss_max_percent": 5, "medium_loss_max_percent": 15}
        labelled = build_forensic_report([row], "BATCH_001", label_contract=contract)["trades"][0]
        self.assertTrue(labelled["labels"]["reversal"])
        self.assertTrue(labelled["labels"]["rapid_sl"])
        self.assertTrue(labelled["labels"]["near_tp_failure"])
        self.assertTrue(labelled["labels"]["false_momentum"])
        self.assertEqual(labelled["loss_size"], "MEDIUM")

    def test_candidate_bounded_ranked_discovery_only_and_inputs_immutable(self):
        rows = [trade(index) for index in range(20)]
        before = deepcopy(rows)
        report = build_forensic_report(rows, "BATCH_001")
        generation = report["candidate_generation"]
        self.assertGreater(len(generation["proposals"]), 0)
        self.assertLessEqual(len(generation["proposals"]), 10)
        self.assertEqual(generation["maximum_proposals"], 10)
        self.assertLessEqual(generation["searched_rule_kind_trials"], 200)
        for proposal in generation["proposals"]:
            self.assertEqual(proposal["status"], "DISCOVERY_ONLY")
            self.assertFalse(proposal["validated"])
            self.assertEqual(proposal["future_validation"]["discovery_batch_excluded"], "BATCH_001")
            self.assertIn("temporal_split", proposal["discovery_evidence"]["screens"])
            self.assertIn("cost_stress", proposal["discovery_evidence"])
        self.assertEqual(rows, before)
        json.dumps(report, allow_nan=False)

    def test_unknown_domains_and_invalid_records_not_silently_fabricated(self):
        with self.assertRaises(ValueError):
            build_forensic_report([trade(0), trade(0)], "BATCH_001")
        with self.assertRaises(ValueError):
            build_forensic_report([trade(0, cost=0)], "BATCH_001")
        with self.assertRaises(ValueError):
            build_forensic_report([trade(0)], "BATCH_001", label_contract={"rapid_sl_seconds": float("nan")})


class StageTests(unittest.TestCase):
    def test_fixed_disjoint_stage1_stage2_future_only(self):
        rows = [trade(index) for index in range(55)]
        rules = definition()
        first = evaluate_stage(rows, rules, 1)
        second = evaluate_stage(rows, rules, 2)
        self.assertEqual(first["status"], "FRESH_DIRECTION_CONFIRMED")
        self.assertEqual(second["status"], "PROMISING_CHALLENGER")
        self.assertEqual(len(first["selected_position_ids"]), 20)
        self.assertEqual(len(second["selected_position_ids"]), 30)
        self.assertFalse(set(first["selected_position_ids"]) & set(second["selected_position_ids"]))
        self.assertFalse(first["automatic_control_promotion"])
        third = evaluate_stage(rows, rules, 3)
        self.assertEqual(third["status"], "HUMAN_REVIEW_REQUIRED")

    def test_creation_batch_and_old_signal_excluded(self):
        rows = [trade(index) for index in range(22)]
        rows[0]["batch_id"] = "BATCH_001"
        rows[1]["signal_timestamp"] = (BASE - timedelta(seconds=1)).isoformat()
        report = evaluate_stage(rows, definition(), 1)
        self.assertEqual(report["excluded_nonfresh_or_discovery"], 2)
        self.assertNotIn("p-0", report["selected_position_ids"])
        self.assertNotIn("p-1", report["selected_position_ids"])

    def test_fresh_reversal_and_two_trade_dependency_reject_early(self):
        rows = [trade(index, pnl=SOL // 10 if index % 2 == 0 else -SOL // 10) for index in range(20)]
        report = evaluate_stage(rows, definition(), 1)
        self.assertEqual(report["status"], "REJECT_EARLY")
        rows = [trade(index) for index in range(20)]
        for row in rows:
            if row["realized_pnl_lamports"] < 0:
                row["realized_pnl_lamports"] = 0
        rows[0]["realized_pnl_lamports"] = -SOL
        report = evaluate_stage(rows, definition(), 1)
        self.assertEqual(report["status"], "REJECT_EARLY")
        self.assertIn("EFFECT_DEPENDS_ON_AT_MOST_TWO_TRADES", report["reasons"])

    def test_stage_policy_must_be_preregistered_and_missing_not_ignored(self):
        rows = [trade(index) for index in range(20)]
        rules = definition()
        rules["discovery_evidence"].pop("validation_policy")
        report = evaluate_stage(rows, rules, 1)
        self.assertEqual(report["status"], "INSUFFICIENT_COVERAGE")
        self.assertIn("NUMERIC_COVERAGE_POLICY_NOT_PREREGISTERED", report["reasons"])
        for row in rows[:10]:
            row["features"] = {}
        report = evaluate_stage(rows, definition(), 1)
        self.assertEqual(report["status"], "INSUFFICIENT_COVERAGE")
        self.assertEqual(report["economic_report"]["missing_predictor_rate"], .5)

    def test_immutable_definition_hash_mismatch_fails(self):
        rules = definition(rule_hash="bad")
        with self.assertRaises(ValueError):
            evaluate_stage([trade(index) for index in range(20)], rules, 1)

    def test_future_evaluation_hard_rejects_corrupt_predictor_provenance(self):
        rows = [trade(index) for index in range(20)]
        rows[0]["features"]["liquidity_usd"]["observed_at"] = rows[0]["closed_at"]
        with self.assertRaises(ValueError):
            evaluate_stage(rows, definition(), 1)

    def test_sl_label_unknown_without_exit_reason_even_with_trajectory(self):
        row = trade(0)
        row["forensic"] = {"price_trajectory": [{"observed_at": row["closed_at"], "return_percent": -10}]}
        report = build_forensic_report([row], "BATCH_001", label_contract={"rapid_sl_seconds": 600})
        self.assertEqual(report["trades"][0]["labels"]["rapid_sl"], "UNKNOWN")

    def test_stage2_positive_aggregate_with_temporal_flip_stays_unpromoted(self):
        rows = [trade(index) for index in range(50)]
        for index in range(20, 35):
            if index % 2 == 0:
                rows[index]["realized_pnl_lamports"] = SOL // 20
        for index in range(35, 50):
            if index % 2 == 0:
                rows[index]["realized_pnl_lamports"] = -SOL // 2
        report = evaluate_stage(rows, definition(), 2)
        self.assertGreater(report["economic_report"]["effects"]["equal_notional"]["net_effect"], 0)
        self.assertEqual(report["status"], "FRESH_DIRECTION_CONFIRMED")
        self.assertIn("STAGE2_TEMPORAL_INSTABILITY", report["reasons"])

    def test_stage2_thin_positive_strategy_fails_additional_cost_stress(self):
        rows = [trade(index) for index in range(50)]
        for index in range(20, 50):
            if index % 2:
                rows[index]["realized_pnl_lamports"] = SOL // 1000
        report = evaluate_stage(rows, definition(), 2)
        self.assertGreater(report["economic_report"]["included"]["normalized"]["expectancy"], 0)
        self.assertEqual(report["status"], "FRESH_DIRECTION_CONFIRMED")
        self.assertIn("STAGE2_COST_STRESS_FRAGILE", report["reasons"])

    def test_filter_improvement_without_profitable_remaining_strategy_not_alpha(self):
        rows = [trade(index) for index in range(50)]
        for index in range(20, 50):
            if index % 2:
                rows[index]["realized_pnl_lamports"] = -SOL // 100
        report = evaluate_stage(rows, definition(), 2)
        self.assertEqual(report["status"], "ONLY_NEGATIVE_FILTER")
        self.assertFalse(report["automatic_control_promotion"])

    def test_classification_is_label_only_and_never_robust_alpha(self):
        rules = definition(kind="classification", classification="LOW_LIQUIDITY")
        report = evaluate_stage([trade(index) for index in range(20)], rules, 1)
        self.assertEqual(report["status"], "DISCOVERY_ONLY")
        self.assertNotIn("economic_report", report)


if __name__ == "__main__":
    unittest.main()
