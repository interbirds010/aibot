"""불변 연구 batch와 미래 후보 경계를 실제 임시 파일 및 프로세스로 검증한다."""
from __future__ import annotations

import copy
import json
import math
import multiprocessing
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from src.research import accelerated_alpha as alpha
from src.state_store import atomic_write_json, read_json

START = datetime(2026, 10, 1, tzinfo=timezone.utc)


def stamp(seconds=0):
    return (START + timedelta(seconds=seconds)).isoformat()


def trade(index, day=0, seconds=None):
    seconds = day * 86400 + index * 10 if seconds is None else seconds
    return {"position_id": f"p{index}", "buy_event_seq": index * 2 + 1,
            "completion_event_seq": index * 2 + 2, "signal_timestamp": stamp(seconds),
            "entry_decision_timestamp": stamp(seconds + 2), "closed_at": stamp(seconds + 5),
            "entry_cost_lamports": 1000, "realized_pnl_lamports": 100 if index % 2 else -100,
            "features": {"liquidity": {"value": 8000 + index, "observed_at": stamp(seconds + 1), "source": "entry_snapshot"}},
            "forensic": {"mfe_percent": 10, "mae_percent": -2}}


RULE = {"feature": "liquidity", "op": "gt", "threshold": 8005}
SCREEN_POLICY = {"minimum_sample": 20, "minimum_affected": 1, "maximum_single_trade_share": 1,
                 "maximum_single_day_share": 1, "maximum_route_share": 1, "maximum_position_size_share": 1,
                 "maximum_missing_predictor_rate": 1, "maximum_winner_profit_destroyed_share": 1,
                 "minimum_threshold_tail_fraction": 0}
VALIDATION_POLICY = {"minimum_decision_coverage": 0.8, "minimum_exclusion_coverage": 0.1,
                     "maximum_missing_predictor_rate": 0.2}


def definition(name="A", **overrides):
    return {"candidate_id": f"{name}-v1", "version": 1, "kind": "negative_filter", "rule": copy.deepcopy(RULE),
            "rationale": "진입 liquidity 구간과 손실의 관계를 탐색한다.", "expected_mechanism": "관측한 구간에서 손실을 회피하는 가설이다.",
            "creation_batch": "BATCH_001", "discovery_evidence": {"question": "quote/liquidity interaction",
            "discovery_screen_policy": SCREEN_POLICY, "validation_policy": VALIDATION_POLICY},
            "start_event_seq": 100, "start_timestamp": stamp(2000), **overrides}


def register_worker(output, candidate, start, pipe):
    pipe.send("READY")
    if not start.wait(30):
        raise RuntimeError("race start timeout")
    try:
        alpha.register_challenger(output, candidate)
        pipe.send("REGISTERED")
    except RuntimeError as exc:
        pipe.send(str(exc))
    finally:
        pipe.close()


def batch_worker(output, rows, start, pipe):
    pipe.send("READY")
    if not start.wait(30):
        raise RuntimeError("race start timeout")
    pipe.send([batch["batch_id"] for batch in alpha.freeze_batches(output, rows, stamp(1000))])
    pipe.close()


def shadow_worker(output, payload, start, pipe):
    pipe.send("READY")
    if not start.wait(30):
        raise RuntimeError("race start timeout")
    pipe.send(alpha.record_shadow_predictor(output, payload, stamp(3000))["predictor_hash"])
    pipe.close()


class AlphaTests(unittest.TestCase):
    def setUp(self):
        clock = patch.object(alpha, "datetime", wraps=datetime)
        self.addCleanup(clock.stop)
        clock.start().now.return_value = START + timedelta(seconds=1100)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name)
        self.rows = [trade(index) for index in range(20)]
        for row in self.rows:
            row["realized_pnl_lamports"] = -100 if row["features"]["liquidity"]["value"] > 8005 else 100
            row["features"]["route_type"] = {"value": "A", "observed_at": row["signal_timestamp"], "source": "entry_snapshot"}

    def freeze(self, rows=None):
        return alpha.freeze_batches(self.output, self.rows if rows is None else rows, stamp(1000))

    def test_twentieth_closes_exact_batch_repeat_preserves_bytes(self):
        self.assertEqual(self.freeze(self.rows[:19]), [])
        batches = self.freeze()
        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0]["completed_count"], 20)
        self.assertEqual(batches[0]["close_reason"], "20_COMPLETED")
        path = self.output / "batches" / "BATCH_001.json"
        before = path.read_bytes()
        self.assertEqual(self.freeze(), batches)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(batches[0]["initial_discovery_dataset_hash"], alpha.digest(self.rows[:19]))

    def test_seven_distinct_completed_signal_kst_days_not_elapsed_time(self):
        rows = [trade(index, day=index) for index in range(7)]
        self.assertEqual(alpha.freeze_batches(self.output, rows[:6], stamp(10 * 86400)), [])
        batches = alpha.freeze_batches(self.output, rows, stamp(10 * 86400))
        self.assertEqual(batches[0]["close_reason"], "7_ACTIVE_KST_DAYS")
        self.assertEqual(len(batches[0]["rows"]), 7)
        self.assertEqual(len(batches[0]["active_kst_days"]), 7)

    def test_large_input_nonoverlap_and_late_close_old_buy_in_later_batch(self):
        first = self.freeze()[0]
        late = trade(50, seconds=0)
        late["buy_event_seq"] = 2
        late["completion_event_seq"] = 200
        late["closed_at"] = stamp(2100)
        additions = [trade(index, seconds=2000 + index) for index in range(20, 39)]
        rows = self.rows + additions + [late]
        batches = alpha.freeze_batches(self.output, rows, stamp(3000))
        self.assertEqual(len(batches), 2)
        self.assertEqual(batches[0], first)
        self.assertIn(late, batches[1]["rows"])
        self.assertEqual(batches[1]["completion_boundary"]["max_event_seq"], 200)
        self.assertEqual(len({row["position_id"] for batch in batches for row in batch["rows"]}), 40)

    def test_only_close_creates_another_full_dataset_snapshot(self):
        self.freeze()
        for count in range(21, 26):
            self.freeze(self.rows + [trade(index) for index in range(20, count)])
        self.assertEqual(len(list((self.output / "datasets").glob("*.json"))), 1)

    def test_historical_row_mutation_or_removal_fails_closed(self):
        self.freeze()
        changed = copy.deepcopy(self.rows)
        changed[0]["realized_pnl_lamports"] += 1
        for rows in (changed, self.rows[1:]):
            with self.assertRaises(RuntimeError):
                self.freeze(rows)

    def test_batch_corruption_detected(self):
        self.freeze()
        path = self.output / "batches" / "BATCH_001.json"
        document = json.loads(path.read_text())
        document["rows"][0]["realized_pnl_lamports"] = 999
        atomic_write_json(path, document)
        with self.assertRaises(RuntimeError):
            alpha.read_batch(path)
        with self.assertRaises(RuntimeError):
            self.freeze()

    def test_missing_sequence_duplicate_bool_and_nonfinite_rejected(self):
        for field, value in (("buy_event_seq", True), ("entry_cost_lamports", 0),
                             ("realized_pnl_lamports", True), ("closed_at", stamp(-1))):
            row = trade(0)
            row[field] = value
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    alpha.validate_rows([row])
        with self.assertRaises(ValueError):
            alpha.validate_rows([trade(0), trade(0)])
        for value in (math.nan, math.inf, [], {}):
            row = trade(0)
            row["features"]["liquidity"]["value"] = value
            with self.assertRaises(ValueError):
                alpha.validate_rows([row])

    def test_predictor_leakage_and_outcome_names_rejected(self):
        row = trade(0)
        row["features"]["liquidity"]["observed_at"] = stamp(3)
        with self.assertRaises(ValueError):
            alpha.validate_rows([row])
        for name in ("realized_pnl_lamports", "mfe", "winner", "forensic"):
            with self.assertRaises(ValueError):
                alpha.evaluate_rule({"feature": name, "op": "gt", "threshold": 0}, trade(0))

    def test_at_decision_observation_known_and_one_microsecond_after_leaks(self):
        row = trade(0)
        row["features"]["liquidity"]["observed_at"] = row["entry_decision_timestamp"]
        self.assertEqual(alpha.validate_rows([row]), [row])
        row["features"]["liquidity"]["observed_at"] = stamp(2.000001)
        with self.assertRaises(ValueError):
            alpha.validate_rows([row])

    def test_origin_recovers_first_dataset_after_interrupted_write(self):
        original = alpha.atomic_write_json
        def fail_origin(path, document):
            if path.name == "origin.json":
                raise OSError("simulated interruption")
            original(path, document)
        with patch.object(alpha, "atomic_write_json", side_effect=fail_origin):
            with self.assertRaises(OSError):
                self.freeze(self.rows[:19])
        batches = alpha.freeze_batches(self.output, self.rows, stamp(1500))
        self.assertEqual(batches[0]["initial_discovery_dataset_hash"], alpha.digest(self.rows[:19]))
        origin = alpha._read_immutable(self.output / "origin.json")
        self.assertEqual(origin["frozen_at_utc"], stamp(1000))

    def test_candidate_orphan_write_recovers_without_consuming_duplicate_slot(self):
        self.freeze()
        original = alpha.atomic_write_json
        def fail_registry(path, document):
            if path.name == "challenger_registry.json":
                raise OSError("simulated interruption")
            original(path, document)
        with patch.object(alpha, "atomic_write_json", side_effect=fail_registry):
            with self.assertRaises(OSError):
                alpha.register_challenger(self.output, definition())
        alpha.register_challenger(self.output, definition())
        registry = read_json(self.output / "challenger_registry.json", {})
        self.assertEqual(len(registry["candidates"]), 1)
        self.assertEqual(registry["version"], 1)

    def test_no_outcome_influence_rule_boundaries_and_unknown(self):
        row = trade(5)
        self.assertIs(alpha.evaluate_rule(RULE, row), False)
        row["realized_pnl_lamports"] = 99999999
        row["forensic"] = {"winner": True, "mfe_percent": 9000}
        self.assertIs(alpha.evaluate_rule(RULE, row), False)
        self.assertIs(alpha.evaluate_rule({**RULE, "op": "gte"}, row), True)
        row["features"] = {}
        self.assertIsNone(alpha.evaluate_rule(RULE, row))
        row = trade(5)
        interaction = {"op": "AND", "parts": [RULE, {"feature": "missing", "op": "gt", "threshold": 1}]}
        self.assertIsNone(alpha.evaluate_rule(interaction, row))
        with self.assertRaises(ValueError):
            alpha.evaluate_rule({"op": "AND", "parts": [RULE] * 6}, row)

    def test_future_boundary_hash_version_and_idempotent_registration(self):
        self.freeze()
        candidate = alpha.register_challenger(self.output, definition())
        registry_path = self.output / "challenger_registry.json"
        registry_before = registry_path.read_bytes()
        self.assertEqual(alpha.register_challenger(self.output, definition()), candidate)
        self.assertEqual(registry_path.read_bytes(), registry_before)
        changed = definition()
        changed["rule"]["threshold"] = 9999
        with self.assertRaises(RuntimeError):
            alpha.register_challenger(self.output, changed)
        for changes in ({"start_event_seq": 39}, {"start_timestamp": stamp(1000)}, {"rule_hash": "bad"}, {"version": 2}):
            with self.assertRaises(ValueError):
                alpha.register_challenger(self.output, definition("B", **changes))
        alpha.register_challenger(self.output, definition(candidate_id="A-v2", version=2))

    def test_shadow_evaluates_only_future_once_no_trade_writes(self):
        self.freeze()
        candidate = alpha.register_challenger(self.output, definition())
        self.assertEqual(alpha.shadow_decisions([candidate], self.rows[0])[0]["status"], "NOT_FRESH")
        future = trade(50, seconds=2000)
        decision = alpha.shadow_decisions([candidate], future)[0]
        self.assertEqual(decision["status"], "EVALUATED")
        self.assertTrue(decision["would_skip"])
        self.assertFalse(decision["would_enter"])
        subset = definition("subset", kind="profitable_subset")
        self.assertTrue(alpha.shadow_decisions([subset], future)[0]["would_enter"])
        label = definition("regime", kind="classification", classification="flow_regime")
        self.assertEqual(alpha.shadow_decisions([label], future)[0]["alternative_classification"], "flow_regime")
        self.assertIsNone(alpha.shadow_decisions([label], future)[0]["would_enter"])
        future["features"] = {}
        self.assertEqual(alpha.shadow_decisions([candidate], future)[0]["status"], "UNKNOWN")
        with self.assertRaises(ValueError):
            alpha.shadow_decisions([candidate, candidate], future)

    def test_fixed_stages_twenty_then_additional_thirty_and_no_promotion(self):
        candidate = definition()
        future = [trade(index, seconds=2000 + index * 10) for index in range(50, 100)]
        self.assertFalse(alpha.validation_progress(candidate, future[:19])["stage1_ready"])
        progress = alpha.validation_progress(candidate, future[:20])
        self.assertTrue(progress["stage1_ready"])
        self.assertFalse(progress["stage2_ready"])
        self.assertFalse(alpha.validation_progress(candidate, future[:49])["stage2_ready"])
        progress = alpha.validation_progress(candidate, future)
        self.assertTrue(progress["stage2_ready"])
        self.assertEqual(len(progress["stage2_position_ids"]), 30)
        self.assertFalse(progress["control_promotion"])

    def test_transition_requires_future_batch_and_human_evidence(self):
        self.freeze()
        alpha.register_challenger(self.output, definition())
        for evidence in ({"human_decision": False}, {"human_decision": True, "validation_batch_ids": ["BATCH_001"]}):
            with self.assertRaises(ValueError):
                alpha.transition_candidate(self.output, "A-v1", "FRESH_DIRECTION_CONFIRMED", evidence)
        future = [trade(index, seconds=2000 + index) for index in range(50, 70)]
        for index, row in enumerate(future):
            row["features"]["liquidity"]["value"] = 9000 if index % 2 == 0 else 8000
            row["realized_pnl_lamports"] = -100 if index % 2 == 0 else 100
            row["features"]["route_type"] = {"value": "A", "observed_at": row["signal_timestamp"], "source": "entry_snapshot"}
        alpha.freeze_batches(self.output, self.rows + future, stamp(3000))
        evidence = {"human_decision": True, "validation_batch_ids": ["BATCH_002"], "rationale": "direction checked"}
        entry = alpha.transition_candidate(self.output, "A-v1", "FRESH_DIRECTION_CONFIRMED", evidence)
        self.assertEqual(entry["status"], "FRESH_DIRECTION_CONFIRMED")
        self.assertEqual(alpha.transition_candidate(self.output, "A-v1", "FRESH_DIRECTION_CONFIRMED", evidence), entry)
        with self.assertRaises(ValueError):
            alpha.transition_candidate(self.output, "A-v1", "PROMISING_CHALLENGER", evidence)
        alpha.transition_candidate(self.output, "A-v1", "REJECT_EARLY", {"human_decision": True, "reason": "direction reversal"})
        with self.assertRaises(ValueError):
            alpha.transition_candidate(self.output, "A-v1", "DISCOVERY_ONLY", {"human_decision": True})

    def test_registry_corruption_and_output_runtime_guard(self):
        self.freeze()
        alpha.register_challenger(self.output, definition())
        path = self.output / "challenger_registry.json"
        registry = read_json(path, {})
        registry["candidates"]["A-v1"]["definition_hash"] = "bad"
        atomic_write_json(path, registry)
        with self.assertRaises(RuntimeError):
            alpha.register_challenger(self.output, definition("B"))
        for path in (alpha.ROOT / "outputs" / "research", alpha.FROZEN_RUNTIME / "research"):
            with self.assertRaises(ValueError):
                alpha.freeze_batches(path, self.rows, stamp(1000))

    def test_registration_requires_recomputed_discovery_screen(self):
        self.freeze()
        candidate = definition()
        candidate["discovery_evidence"] = {"question": "unverified", "discovery_screen": {"status": "PASS_DISCOVERY"}}
        with self.assertRaisesRegex(ValueError, "REJECT_DISCOVERY"):
            alpha.register_challenger(self.output, candidate)
        self.assertFalse((self.output / "challenger_registry.json").exists())

    def test_registry_requires_rationale_and_persists_computed_discovery_metadata(self):
        from src.research.alpha_batch_forensics import discovery_screen
        self.freeze()
        for name in ("rationale", "expected_mechanism"):
            for value in (None, "", "   ", True):
                candidate = definition(**{name: value})
                with self.subTest(name=name, value=value), self.assertRaisesRegex(ValueError, name):
                    alpha.register_challenger(self.output, candidate)
        candidate = definition()
        candidate["discovery_evidence"]["economic_report"] = {"fabricated_net_effect": 9999999}
        registered = alpha.register_challenger(self.output, candidate)
        report_path = self.output / "candidates" / "A-v1.discovery.json"
        artifact = alpha._read_immutable(report_path)
        screen = discovery_screen(self.rows, RULE, "negative_filter", SCREEN_POLICY)
        self.assertEqual(artifact["discovery_screen"], screen)
        self.assertEqual(artifact["discovery_economics"], screen["economics"])
        self.assertEqual(artifact["discovery_coverage"]["decision_coverage"], 1)
        self.assertEqual(artifact["rationale"], registered["rationale"])
        self.assertEqual(artifact["expected_mechanism"], registered["expected_mechanism"])
        self.assertEqual(artifact["discovery_batch"], "BATCH_001")
        entry = read_json(self.output / "challenger_registry.json", {})["candidates"]["A-v1"]
        self.assertEqual(entry["discovery_report_hash"], alpha.digest(artifact))
        changed = copy.deepcopy(candidate)
        changed["rationale"] = "다른 설명"
        with self.assertRaisesRegex(RuntimeError, "immutable"):
            alpha.register_challenger(self.output, changed)
        damaged = json.loads(report_path.read_text(encoding="utf-8"))
        damaged["discovery_coverage"]["decision_coverage"] = 0
        atomic_write_json(report_path, damaged)
        with self.assertRaisesRegex(RuntimeError, "hash mismatch"):
            alpha.registered_definition(self.output, "A-v1")

    def future_rows(self, count=50):
        result = [trade(index + 50, seconds=2100 + index * 10) for index in range(count)]
        for index, row in enumerate(result):
            row["entry_cost_lamports"] = 1_000_000_000
            row["realized_pnl_lamports"] = -100_000_000 if index % 2 == 0 else 200_000_000
            row["features"]["liquidity"]["value"] = 9000 if index % 2 == 0 else 8000
            row["features"]["route_type"] = {"value": "A", "observed_at": row["signal_timestamp"], "source": "entry_snapshot"}
        return result

    def test_durable_stages_disjoint_idempotent_and_preserve_outcomes(self):
        self.freeze()
        alpha.register_challenger(self.output, definition())
        future = self.future_rows()
        with self.assertRaisesRegex(ValueError, "durable Stage 1"):
            alpha.persist_stage_result(self.output, "A-v1", future, 2, stamp(4000))
        first = alpha.persist_stage_result(self.output, "A-v1", future[:20], 1, stamp(4000))
        self.assertEqual(first["status"], "FRESH_DIRECTION_CONFIRMED")
        second = alpha.persist_stage_result(self.output, "A-v1", future, 2, stamp(4000))
        self.assertEqual(second["status"], "PROMISING_CHALLENGER")
        self.assertFalse(set(first["selected_position_ids"]) & set(second["selected_position_ids"]))
        registry = read_json(self.output / "challenger_registry.json", {})
        self.assertEqual(alpha.persist_stage_result(self.output, "A-v1", future, 2, stamp(4000)), second)
        self.assertEqual(read_json(self.output / "challenger_registry.json", {}), registry)
        future[0]["realized_pnl_lamports"] -= 1
        with self.assertRaisesRegex(RuntimeError, "fixed Stage 1"):
            alpha.persist_stage_result(self.output, "A-v1", future, 2, stamp(4000))

    def test_stage_rejection_releases_slot_and_reports_remain(self):
        self.freeze()
        alpha.register_challenger(self.output, definition())
        future = self.future_rows(20)
        for row in future:
            row["realized_pnl_lamports"] = -row["realized_pnl_lamports"]
        result = alpha.persist_stage_result(self.output, "A-v1", future, 1, stamp(4000))
        self.assertEqual(result["status"], "REJECT_EARLY")
        registry = read_json(self.output / "challenger_registry.json", {})
        self.assertEqual(registry["candidates"]["A-v1"]["status"], "REJECT_EARLY")
        self.assertTrue(list((self.output / "stage_reports" / "A-v1").glob("*.json")))

    def test_historical_future_start_does_not_prove_actual_preregistration(self):
        self.freeze()
        alpha.datetime.now.return_value = START + timedelta(seconds=5000)
        alpha.register_challenger(self.output, definition())
        result = alpha.persist_stage_result(self.output, "A-v1", self.future_rows(20), 1, stamp(6000))
        self.assertEqual(result["status"], "INSUFFICIENT_COVERAGE")
        self.assertEqual(result["excluded_before_real_registration"], 20)
        self.assertEqual(result["eligible_fresh_completed"], 0)
        self.assertEqual(read_json(self.output / "challenger_registry.json", {})["candidates"]["A-v1"]["status"], "DISCOVERY_ONLY")

    def test_external_shadow_records_same_predictor_without_outcomes_or_trade(self):
        self.freeze()
        alpha.register_challenger(self.output, definition())
        registry = read_json(self.output / "challenger_registry.json", {})
        registered = alpha.utc(registry["candidates"]["A-v1"]["registered_at_utc"])
        signal = max(registered + timedelta(seconds=1), alpha.utc(stamp(2000)))
        predictor = {"control_predictor_id": "predictor-1", "control_event_seq": 101, "control_decision": "RPC_SKIPPED",
                     "signal_timestamp": signal.isoformat(), "entry_decision_timestamp": signal.isoformat(),
                     "features": {"liquidity": {"value": 9000, "observed_at": signal.isoformat(), "source": "Control_predictor"}},
                     "epoch_id": "telemetry-1", "build_sha": "fixture-build", "strategy_fingerprint": "fixture-Control"}
        recorded = (signal + timedelta(seconds=1)).isoformat()
        result = alpha.record_shadow_predictor(self.output, predictor, recorded)
        self.assertTrue(result["decisions"][0]["would_skip"])
        self.assertFalse(result["control_changes"])
        self.assertTrue(result["decisions"][0]["preregistered_before_signal"])
        self.assertEqual(alpha.record_shadow_predictor(self.output, predictor, recorded), result)
        self.assertEqual(read_json(self.output / "shadow_state.json", {})["recorded_count"], 1)
        rewritten = copy.deepcopy(predictor)
        rewritten["control_predictor_id"] = "predictor-2"
        with self.assertRaisesRegex(RuntimeError, "duplicate Control event"):
            alpha.record_shadow_predictor(self.output, rewritten, recorded)
        with self.assertRaises(ValueError):
            alpha.record_shadow_predictor(self.output, {**predictor, "realized_pnl_lamports": 10}, recorded)
        old = copy.deepcopy(predictor)
        old.update(control_predictor_id="old", control_event_seq=102,
                   signal_timestamp=stamp(1050), entry_decision_timestamp=stamp(1050), features={})
        before = alpha.record_shadow_predictor(self.output, old, recorded)
        self.assertEqual(before["decisions"][0]["status"], "NOT_PROSPECTIVELY_REGISTERED")
        self.assertIsNone(before["decisions"][0]["would_enter"])

    def _race(self, worker, payloads):
        context = multiprocessing.get_context("spawn")
        start = context.Event()
        processes, pipes = [], []
        try:
            for payload in payloads:
                parent, child = context.Pipe(duplex=False)
                process = context.Process(target=worker, args=(str(self.output), payload, start, child))
                process.start()
                child.close()
                processes.append(process)
                pipes.append(parent)
            for pipe in pipes:
                self.assertTrue(pipe.poll(30))
                self.assertEqual(pipe.recv(), "READY")
            start.set()
            result = []
            for pipe in pipes:
                self.assertTrue(pipe.poll(30))
                result.append(pipe.recv())
            for process in processes:
                process.join(30)
                self.assertEqual(process.exitcode, 0)
            return result
        finally:
            for process in processes:
                if process.is_alive():
                    process.terminate()
                process.join(10)
            for pipe in pipes:
                pipe.close()

    def test_two_process_batch_freeze_no_duplicate_or_overlap(self):
        results = self._race(batch_worker, [self.rows, self.rows])
        self.assertEqual(results, [["BATCH_001"], ["BATCH_001"]])
        self.assertEqual(len(list((self.output / "batches").glob("*.json"))), 1)

    def test_two_process_registration_respects_fifth_slot(self):
        self.freeze()
        for name in ("A", "B", "C", "D"):
            alpha.register_challenger(self.output, definition(name))
        results = self._race(register_worker, [definition("E"), definition("F")])
        self.assertEqual(results.count("REGISTERED"), 1)
        self.assertEqual(results.count("five active challenger limit"), 1)
        registry = read_json(self.output / "challenger_registry.json", {})
        self.assertEqual(len(registry["candidates"]), 5)

    def test_two_process_external_shadow_duplicate_is_one_record(self):
        self.freeze()
        alpha.register_challenger(self.output, definition())
        predictor = {"control_predictor_id": "shared", "control_event_seq": 101, "control_decision": "BUY",
                     "signal_timestamp": stamp(2001), "entry_decision_timestamp": stamp(2002),
                     "features": {}, "epoch_id": "fixture-epoch", "build_sha": "fixture-build",
                     "strategy_fingerprint": "fixture-Control"}
        result = self._race(shadow_worker, [predictor, predictor])
        self.assertEqual(result, [alpha.digest(predictor), alpha.digest(predictor)])
        self.assertEqual(len(list((self.output / "shadow_records").glob("*.json"))), 1)
        state = read_json(self.output / "shadow_state.json", {})
        self.assertEqual(state["version"], 1)
        self.assertEqual(state["recorded_count"], 1)


if __name__ == "__main__":
    unittest.main()
