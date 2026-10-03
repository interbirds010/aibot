from __future__ import annotations

import asyncio
import copy
import json
import math
import multiprocessing
import queue
import tempfile
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from src import analyzer, executor, monitor, observation_tracker, risk_manager, state_store
from src.research import n3_shadow as n3


BUILD = {"git_sha": "test-build", "source_digest": "test-digest", "source_files": {}}
START = "2026-10-03T00:00:00+00:00"


def stamp(seconds: int | float = 0) -> str:
    return (datetime.fromisoformat(START) + timedelta(seconds=seconds)).isoformat()


def immutable_race_worker(path: str, document: dict, start, result) -> None:
    """spawn 자식은 준비 신호 후 같은 불변 기록을 동시에 저장한다."""
    result.send("READY")
    if not start.wait(30):
        raise RuntimeError("test race start timeout")
    result.send(n3.immutable(Path(path), document))
    result.close()


class N3RuleTests(unittest.TestCase):
    def test_canonical_rule_is_exact_historical_sha(self) -> None:
        self.assertEqual(n3.digest(n3.RULE), "b219661c31c132e1b2f11901664981482a9e42dc847ab300f590072eb3271165")
        self.assertEqual(n3.digest(dict(reversed(list(n3.RULE.items())))), n3.RULE_HASH)
        with self.assertRaises(ValueError):
            n3.canonical({"bad": math.nan})

    def test_all_exact_boundaries(self) -> None:
        low, high = (n3.RULE["parts"][0][key] for key in ("low", "high"))
        threshold = n3.RULE["parts"][1]["threshold"]
        for quote, analysis, expected in (
            (low, math.nextafter(threshold, math.inf), True),
            (math.nextafter(low, math.inf), threshold + 1, False),
            (high, threshold + 1, False),
            (math.nextafter(high, math.inf), threshold + 1, True),
            (0, threshold, False),
            (0, math.nextafter(threshold, -math.inf), False),
        ):
            with self.subTest(quote=quote, analysis=analysis):
                self.assertIs(n3.classify({"quote_preflight_duration_sec": quote,
                                          "analysis_duration_sec": analysis}), expected)

    def test_missing_boolean_nonfinite_string_negative_are_unknown(self) -> None:
        for key in ("quote_preflight_duration_sec", "analysis_duration_sec"):
            for bad in (None, True, False, math.nan, math.inf, -math.inf, "5", -1, [], {}):
                inputs = {"quote_preflight_duration_sec": 1, "analysis_duration_sec": 3, key: bad}
                with self.subTest(key=key, bad=bad):
                    self.assertIsNone(n3.classify(inputs))
            self.assertIsNone(n3.classify({other: 3 for other in
                                          ("quote_preflight_duration_sec", "analysis_duration_sec") if other != key}))

    def test_epoch_requires_valid_timezone_timestamp(self) -> None:
        self.assertEqual(n3.epoch(START), n3.epoch("2026-10-03T09:00:00+09:00"))
        for bad in (None, True, 1, "", "not-a-date", "2026-10-03T00:00:00", "2026-13-03T00:00:00Z"):
            with self.subTest(value=bad):
                self.assertIsNone(n3.epoch(bad))
                with self.assertRaises(ValueError):
                    n3.kst_day(bad)

    def snapshot(self, **overrides) -> dict:
        args = {"signal_timestamp": stamp(0), "analysis_start": stamp(2),
                "analysis_end": stamp(5), "preflight_start": stamp(8),
                "preflight_end": stamp(10), "entry_decision": stamp(12), "quote_timestamp": stamp(9)}
        return n3.prepare_entry_snapshot(**(args | overrides))

    def test_predictors_use_legacy_epoch_intervals_and_preserve_actual_steps(self) -> None:
        result = self.snapshot()
        self.assertEqual(result["raw_inputs"], {"analysis_duration_sec": 5.0, "quote_preflight_duration_sec": 5.0})
        self.assertIs(result["would_skip"], True)
        self.assertEqual(result["analysis_start_timestamp"], stamp(2))
        self.assertEqual(result["preflight_start_timestamp"], stamp(8))
        self.assertEqual(result["preflight_end_timestamp"], stamp(10))
        self.assertEqual(result["entry_decision_timestamp"], stamp(12))
        self.assertEqual(result["quote_timestamp"], stamp(9))
        self.assertEqual(result["quote_age_ms"], 3000)
        self.assertEqual(result["missing_fields"], [])

    def test_invalid_required_timestamp_or_order_is_unknown(self) -> None:
        for key in ("signal_timestamp", "analysis_end", "preflight_end", "entry_decision"):
            for value in (None, "invalid", "2026-10-03T00:00:00"):
                with self.subTest(key=key, value=value):
                    result = self.snapshot(**{key: value})
                    self.assertIsNone(result["would_skip"])
                    self.assertEqual(set(result["raw_inputs"].values()), {None})
                    self.assertEqual(result["missing_reason"], "invalid_or_missing_timestamp")
        self.assertIsNone(self.snapshot(analysis_end=stamp(-1))["would_skip"])
        self.assertIsNone(self.snapshot(preflight_end=stamp(4))["would_skip"])
        self.assertIsNone(self.snapshot(entry_decision=stamp(9))["would_skip"])

    def test_missing_actual_step_or_quote_does_not_fabricate_values(self) -> None:
        result = self.snapshot(analysis_start=None, preflight_start=None, quote_timestamp=None)
        self.assertIs(result["would_skip"], True)
        self.assertIsNone(result["quote_age_ms"])
        self.assertEqual(set(result["missing_fields"]), {"analysis_start_timestamp", "preflight_start_timestamp",
                                                        "quote_timestamp", "quote_age_ms"})
        self.assertIsNone(self.snapshot(quote_timestamp="invalid")["quote_age_ms"])
        self.assertIsNone(self.snapshot(quote_timestamp=stamp(13))["quote_age_ms"])

    def test_frozen_historical_classifications_and_delta(self) -> None:
        fixture = json.loads((Path(__file__).parent / "fixtures" / "n3_historical.json").read_text(encoding="utf-8"))
        self.assertEqual(fixture["rule_hash"], n3.RULE_HASH)
        self.assertEqual(fixture["source_dataset_sha256"], "eb3296bdd2995d9e48080071f6a9a1338b17e594994bdd4ccb275713b88d6563")
        self.assertEqual(len(fixture["rows"]), 147)
        for split, hits, losers, winners, delta in (("DISCOVERY", 56, 46, 10, 61_868_821),
                                                   ("VALIDATION", 17, 16, 1, 26_278_372)):
            rows = [row for row in fixture["rows"] if row["split"] == split]
            selected = []
            for row in rows:
                actual = n3.classify(row["inputs"])
                self.assertIs(actual, row["would_skip"], row["position_id"])
                if actual:
                    selected.append(row)
            self.assertEqual(len(selected), hits)
            self.assertEqual(sum(row["pnl_lamports"] < 0 for row in selected), losers)
            self.assertEqual(sum(row["pnl_lamports"] > 0 for row in selected), winners)
            self.assertEqual(-sum(row["pnl_lamports"] for row in selected), delta)


class N3SidecarTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "한글 경로 연구"
        self.sidecar = self.root / "data" / "n3_shadow"
        self.locked = self.root / "locked.json"
        state_store.atomic_write_json(self.locked, {"selected_candidates": [
            {"candidate_id": "N3", "rule": n3.RULE, "exclude_condition": True}]})
        self.write_ledger([])
        self.build_patch = patch.object(n3, "build_identity", return_value=BUILD)
        self.build_patch.start()
        self.addCleanup(self.build_patch.stop)
        with patch.object(n3, "now_iso", return_value=START):
            self.manifest = n3.register(self.root, self.sidecar, self.locked)

    def write_ledger(self, events: list[dict], *, next_seq: int | None = None) -> bytes:
        path = self.root / "data" / "paper_trades.json"
        state_store.atomic_write_json(path, {"schema_version": 2, "events": events,
                                             "next_event_seq": next_seq if next_seq is not None else
                                             max((event["event_seq"] for event in events), default=0) + 1})
        return path.read_bytes()

    def buy(self, pid="POSITION", seq=1, signal=START) -> dict:
        return {"type": "BUY", "event_seq": seq, "event_id": f"BUY-{seq}", "position_id": pid,
                "at": stamp(20 + seq), "signal_detected_at": signal, "mint": "MINT", "route_type": "A",
                "analysis_completed_at": stamp(3), "entry_quote_at": stamp(4),
                "strategy_version": "broad_discovery_v1", "cost_lamports": 1000, "token_amount_raw": 100}

    def sell(self, pid="POSITION", seq=2, amount=100, proceeds=1100, pnl=100) -> dict:
        return {"type": "SELL", "event_seq": seq, "event_id": f"SELL-{seq}", "position_id": pid,
                "at": stamp(seq), "token_amount_raw": amount, "proceeds_lamports": proceeds,
                "realized_pnl_lamports": pnl}

    def capture(self, pid="POSITION", signal=START, skip=True) -> None:
        duration = 3 if skip else 1
        snapshot = n3.prepare_entry_snapshot(signal_timestamp=signal, analysis_start=signal,
                                            analysis_end=stamp(duration), preflight_start=stamp(duration),
                                            preflight_end=stamp(duration + 1), entry_decision=stamp(duration + 1),
                                            quote_timestamp=stamp(duration + 1))
        n3.persist_capture(self.sidecar, pid, snapshot, BUILD)

    def observe(self) -> dict:
        return n3.observe_once(self.root, self.sidecar, snapshot_grace_seconds=0)

    def replace_contract(self, contract: dict) -> None:
        manifest = {**self.manifest, "contract": contract, "contract_hash": n3.digest(contract)}
        state_store.atomic_write_json(self.sidecar / "manifest.json", {**manifest, "content_hash": n3.digest(manifest)})

    def test_registration_is_stable_no_rewrite_and_wrong_rule_rejected(self) -> None:
        path = self.sidecar / "manifest.json"
        before = path.read_bytes(), path.stat().st_mtime_ns
        self.write_ledger([self.buy()])
        self.assertEqual(n3.register(self.root, self.sidecar, self.locked), self.manifest)
        self.assertEqual((path.read_bytes(), path.stat().st_mtime_ns), before)
        with patch.object(n3, "build_identity", return_value={"git_sha": "another"}):
            with self.assertRaisesRegex(RuntimeError, "build mismatch"):
                n3.register(self.root, self.sidecar, self.locked)
        altered = copy.deepcopy(n3.RULE)
        altered["parts"][0]["low"] += 0.01
        state_store.atomic_write_json(self.locked, {"selected_candidates": [
            {"candidate_id": "N3", "rule": altered, "exclude_condition": True}]})
        with self.assertRaisesRegex(RuntimeError, "historical N3 differs"):
            n3.register(self.root, self.sidecar, self.locked)

    def test_prestart_signal_excluded_despite_poststart_buy_and_seq_boundary(self) -> None:
        self.write_ledger([self.buy(signal=stamp(-1))])
        self.assertEqual(self.observe()["admitted_count"], 0)
        self.assertFalse(n3.eligible(self.manifest, self.buy(seq=0)))
        self.assertTrue(n3.eligible(self.manifest, self.buy(seq=1)))

    def test_legacy_buy_is_excluded_after_postmarker_final_sell(self) -> None:
        legacy_buy = self.buy(pid="LEGACY", seq=1, signal=stamp(-30))
        legacy_buy["at"] = stamp(-20)
        self.write_ledger([legacy_buy])
        self.sidecar = self.root / "data" / "legacy_shadow"
        with patch.object(n3, "now_iso", return_value=START):
            self.manifest = n3.register(self.root, self.sidecar, self.locked)
        self.assertEqual(self.manifest["start_event_seq"], 2)
        self.assertFalse(n3.eligible(self.manifest, {**legacy_buy, "signal_detected_at": START}))

        final_sell = self.sell(pid="LEGACY", seq=2)
        final_sell["at"] = stamp(30)
        self.assertGreater(n3.epoch(final_sell["at"]), n3.epoch(self.manifest["start_utc"]))
        control = self.write_ledger([legacy_buy, final_sell])
        state = self.observe()
        self.assertEqual((state["cursor"], state["admitted_count"], state["completed_count"],
                          state["open_count"], state["sell_record_count"]), (2, 0, 0, 0, 0))
        self.assertEqual(n3.completed_trades(self.sidecar), [])
        for kind in ("entries", "sells", "completed"):
            self.assertEqual(n3.read_records(self.sidecar, kind, 408), [])
        self.assertEqual(self.observe()["completed_count"], 0)
        self.assertEqual((self.root / "data" / "paper_trades.json").read_bytes(), control)

    def test_two_legacy_positions_close_but_only_fresh_boundary_buy_completes(self) -> None:
        legacy_buys = [self.buy(pid=f"LEGACY-{seq}", seq=seq, signal=stamp(-30)) for seq in (1, 2)]
        for event in legacy_buys:
            event["at"] = stamp(-20 + event["event_seq"])
        self.write_ledger(legacy_buys)
        self.sidecar = self.root / "data" / "carry_in_shadow"
        with patch.object(n3, "now_iso", return_value=START):
            self.manifest = n3.register(self.root, self.sidecar, self.locked)
        self.assertEqual(self.manifest["start_event_seq"], 3)
        for event in legacy_buys:
            self.assertFalse(n3.eligible(self.manifest, {**event, "signal_detected_at": START}))

        self.capture(pid="FRESH")
        fresh_buy = self.buy(pid="FRESH", seq=self.manifest["start_event_seq"])
        self.assertTrue(n3.eligible(self.manifest, fresh_buy))
        self.write_ledger([*legacy_buys, fresh_buy])
        opened = self.observe()
        self.assertEqual((opened["cursor"], opened["admitted_count"], opened["open_count"],
                          opened["completed_count"]), (3, 1, 1, 0))

        legacy_sells = [self.sell(pid=f"LEGACY-{seq}", seq=seq + 3) for seq in (1, 2)]
        for event in legacy_sells:
            event["at"] = stamp(30 + event["event_seq"])
            self.assertGreaterEqual(event["event_seq"], self.manifest["start_event_seq"])
            self.assertGreater(n3.epoch(event["at"]), n3.epoch(self.manifest["start_utc"]))
        self.write_ledger([*legacy_buys, fresh_buy, *legacy_sells])
        carry_in_closed = self.observe()
        self.assertEqual((carry_in_closed["cursor"], carry_in_closed["admitted_count"],
                          carry_in_closed["completed_count"], carry_in_closed["open_count"],
                          carry_in_closed["sell_record_count"]), (5, 1, 0, 1, 0))
        self.assertEqual(n3.completed_trades(self.sidecar), [])
        self.assertEqual(n3.read_records(self.sidecar, "sells", 1632), [])

        fresh_sell = self.sell(pid="FRESH", seq=6)
        fresh_sell["at"] = stamp(40)
        control = self.write_ledger([*legacy_buys, fresh_buy, *legacy_sells, fresh_sell])
        completed = self.observe()
        self.assertEqual((completed["cursor"], completed["admitted_count"], completed["completed_count"],
                          completed["open_count"], completed["sell_record_count"]), (6, 1, 1, 0, 1))
        trade, = n3.completed_trades(self.sidecar)
        self.assertEqual((trade["position_id"], trade["buy_event_seq"], trade["closed_at"]),
                         ("FRESH", self.manifest["start_event_seq"], fresh_sell["at"]))
        for kind, limit in (("entries", 408), ("sells", 1632), ("completed", 408)):
            self.assertEqual([record["position_id"] for record in n3.read_records(self.sidecar, kind, limit)],
                             ["FRESH"])
        self.assertEqual(self.observe()["completed_count"], 1)
        self.assertEqual((self.root / "data" / "paper_trades.json").read_bytes(), control)

    def test_immutable_retry_noop_conflict_fails_under_unicode_path(self) -> None:
        self.capture()
        path = n3.record_path(self.sidecar, "snapshots", "POSITION")
        before = path.read_bytes(), path.stat().st_mtime_ns
        self.capture()
        self.assertEqual((path.read_bytes(), path.stat().st_mtime_ns), before)
        with self.assertRaisesRegex(RuntimeError, "conflicting immutable"):
            self.capture(skip=False)
        self.assertEqual(path.read_bytes(), before[0])

    def test_multiprocess_immutable_duplicate_race_writes_once(self) -> None:
        context = multiprocessing.get_context("spawn")
        start = context.Event()
        path = self.sidecar / "race" / "동일 기록.json"
        document = {"schema_version": 1, "position_id": "RACE", "message": "동일 문서", "inputs": [1, 2, 3]}
        children = []
        receivers = []
        senders = []
        try:
            for _ in range(4):
                receiver, sender = context.Pipe(duplex=False)
                child = context.Process(target=immutable_race_worker, args=(str(path), document, start, sender))
                children.append(child)
                receivers.append(receiver)
                senders.append(sender)
                child.start()
                sender.close()
            for receiver in receivers:
                self.assertTrue(receiver.poll(30), "race child did not become ready")
                self.assertEqual(receiver.recv(), "READY")
            start.set()
            results = []
            for receiver in receivers:
                self.assertTrue(receiver.poll(30), "race child did not return")
                results.append(receiver.recv())
            for child in children:
                child.join(30)
                self.assertEqual(child.exitcode, 0)
            self.assertEqual(results.count(True), 1)
            self.assertEqual(results.count(False), 3)
            self.assertEqual(n3.read_immutable(path), document)
            self.assertEqual(len(list(path.parent.glob("*.json"))), 1)
        finally:
            start.set()
            for child in children:
                if child.is_alive():
                    child.terminate()
                child.join(5)
                child.close()
            for connection in receivers + senders:
                connection.close()

    def test_malformed_sidecar_and_build_mismatch_fail_without_control_change(self) -> None:
        control = self.write_ledger([self.buy()])
        path = n3.record_path(self.sidecar, "snapshots", "POSITION")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{broken", encoding="utf-8")
        with self.assertRaises(json.JSONDecodeError):
            self.observe()
        self.assertEqual((self.root / "data" / "paper_trades.json").read_bytes(), control)
        with patch.object(n3, "build_identity", return_value={"git_sha": "other"}):
            with self.assertRaisesRegex(RuntimeError, "build mismatch"):
                self.observe()

    def test_parseable_immutable_tampering_is_blocked_without_control_change(self) -> None:
        self.capture()
        path = n3.record_path(self.sidecar, "snapshots", "POSITION")
        document = state_store.read_json(path, {})
        document["control_session_id"] = "tampered"
        state_store.atomic_write_json(path, document)
        control = self.write_ledger([self.buy()])
        with self.assertRaisesRegex(RuntimeError, "hash"):
            self.observe()
        self.assertEqual((self.root / "data" / "paper_trades.json").read_bytes(), control)

    def test_missing_and_internal_event_gaps_stop_without_cursor_commit(self) -> None:
        for events in ([self.buy(seq=2)], [self.buy(), self.sell(seq=3)]):
            with self.subTest(events=events):
                control = self.write_ledger(events)
                with self.assertRaisesRegex(RuntimeError, "event sequence"):
                    self.observe()
                self.assertFalse((self.sidecar / "observer_state.json").exists())
                self.assertEqual((self.root / "data" / "paper_trades.json").read_bytes(), control)

    def test_partial_tp_then_final_sell_aggregates_original_cost_and_delta_sign(self) -> None:
        self.capture()
        self.write_ledger([self.buy(), self.sell(seq=2, amount=40, proceeds=600, pnl=200)])
        state = self.observe()
        self.assertEqual((state["completed_count"], state["open_count"]), (0, 1))
        self.write_ledger([self.buy(), self.sell(seq=2, amount=40, proceeds=600, pnl=200),
                           self.sell(seq=3, amount=60, proceeds=300, pnl=-300)])
        self.assertEqual(self.observe()["completed_count"], 1)
        trade, = n3.completed_trades(self.sidecar)
        self.assertEqual(trade["realized_pnl_lamports"], -100)
        self.assertAlmostEqual(trade["normalized_return"], -0.1)
        self.assertEqual(trade["shadow_delta_actual_lamports"], 100)
        self.assertAlmostEqual(trade["normalized_delta"], 0.1)
        self.assertEqual(trade["sell_event_count"], 2)
        self.assertEqual(trade["entry_cost_lamports"], 1000)

    def test_skipped_control_winner_delta_negative_and_nonhit_delta_zero(self) -> None:
        self.capture()
        self.capture(pid="NONHIT", skip=False)
        nonhit_buy = self.buy(pid="NONHIT", seq=2)
        nonhit_buy.update(analysis_completed_at=stamp(1), entry_quote_at=stamp(2))
        self.write_ledger([self.buy(), nonhit_buy, self.sell(seq=3), self.sell(pid="NONHIT", seq=4)])
        self.observe()
        trades = {item["position_id"]: item for item in n3.completed_trades(self.sidecar)}
        self.assertEqual(trades["POSITION"]["shadow_delta_actual_lamports"], -100)
        self.assertAlmostEqual(trades["POSITION"]["normalized_delta"], -0.1)
        self.assertEqual(trades["NONHIT"]["shadow_delta_actual_lamports"], 0)
        self.assertEqual(trades["NONHIT"]["normalized_delta"], 0)

    def test_invalid_sell_cost_or_excess_quantity_fail_closed(self) -> None:
        self.capture()
        self.write_ledger([self.buy()])
        self.observe()
        for amount, proceeds, pnl, message in ((100, 1100, 101, "cost invariant"),
                                              (101, 1100, 100, "exceeds entry")):
            path = n3.record_path(self.sidecar, "sells", "2")
            document = self.sell(amount=amount, proceeds=proceeds, pnl=pnl)
            if path.exists():
                path.unlink()
            n3.immutable(path, document)
            with self.subTest(amount=amount):
                with self.assertRaisesRegex(RuntimeError, message):
                    n3.completed_trades(self.sidecar)

    def test_recorder_missing_snapshot_null_and_control_unchanged(self) -> None:
        control = self.write_ledger([self.buy(), self.sell()])
        state = self.observe()
        self.assertEqual(state["missing_snapshot_count"], 1)
        self.assertEqual(state["recorder_health"], "MISSING_SNAPSHOTS")
        trade, = n3.completed_trades(self.sidecar)
        self.assertIsNone(trade["would_skip"])
        self.assertIsNone(trade["normalized_delta"])
        self.assertIsNone(trade["shadow_delta_actual_lamports"])
        self.assertEqual(trade["snapshot"]["missing_reason"], "recorder_snapshot_unavailable")
        self.assertEqual((self.root / "data" / "paper_trades.json").read_bytes(), control)

    def test_interrupted_unknown_admission_retries_without_reclassification(self) -> None:
        self.write_ledger([self.buy()])
        original = n3.atomic_write_json

        def interrupt_cursor(path, document):
            if Path(path).name == "observer_state.json":
                raise OSError("test interrupted cursor write")
            return original(path, document)

        with patch.object(n3, "atomic_write_json", side_effect=interrupt_cursor):
            with self.assertRaisesRegex(OSError, "interrupted cursor"):
                self.observe()
        entry_path = n3.record_path(self.sidecar, "entries", "POSITION")
        before = entry_path.read_bytes()
        retried = self.observe()
        self.assertEqual(retried["cursor"], 1)
        self.assertEqual(retried["missing_snapshot_count"], 1)
        self.assertEqual(entry_path.read_bytes(), before)

    def test_late_capture_after_interrupted_unknown_admission_keeps_unknown(self) -> None:
        control = self.write_ledger([self.buy()])
        original = n3.atomic_write_json

        def interrupt_cursor(path, document):
            if Path(path).name == "observer_state.json":
                raise OSError("test interrupted cursor write")
            return original(path, document)

        with patch.object(n3, "atomic_write_json", side_effect=interrupt_cursor):
            with self.assertRaisesRegex(OSError, "interrupted cursor"):
                self.observe()
        entry_path = n3.record_path(self.sidecar, "entries", "POSITION")
        before = entry_path.read_bytes()
        self.capture()
        captured = n3.read_immutable(n3.record_path(self.sidecar, "snapshots", "POSITION"))
        self.assertIs(captured["would_skip"], True)
        retried = self.observe()
        self.assertEqual(retried["cursor"], 1)
        self.assertEqual(retried["missing_snapshot_count"], 1)
        self.assertIsNone(n3.read_immutable(entry_path)["would_skip"])
        self.assertEqual(entry_path.read_bytes(), before)
        self.assertEqual((self.root / "data" / "paper_trades.json").read_bytes(), control)

    def assert_committed_deletion_blocks(self, kind: str, identity: str, control: bytes) -> None:
        state_path = self.sidecar / "observer_state.json"
        state_before = state_path.read_bytes()
        n3.record_path(self.sidecar, kind, identity).unlink()
        with self.assertRaisesRegex(RuntimeError, "committed sidecar records missing or inconsistent"):
            self.observe()
        self.assertEqual(state_path.read_bytes(), state_before)
        self.assertEqual((self.root / "data" / "paper_trades.json").read_bytes(), control)

    def test_deleted_committed_entry_blocks_restart_without_control_change(self) -> None:
        self.capture()
        control = self.write_ledger([self.buy()])
        self.assertEqual(self.observe()["admitted_count"], 1)
        self.assert_committed_deletion_blocks("entries", "POSITION", control)

    def test_deleted_committed_partial_sell_blocks_restart_without_control_change(self) -> None:
        self.capture()
        control = self.write_ledger([self.buy(), self.sell(seq=2, amount=40, proceeds=600, pnl=200)])
        state = self.observe()
        self.assertEqual((state["completed_count"], state["sell_record_count"]), (0, 1))
        self.assert_committed_deletion_blocks("sells", "2", control)

    def test_deleted_committed_final_sell_blocks_restart_without_control_change(self) -> None:
        self.capture()
        control = self.write_ledger([self.buy(), self.sell(seq=2, amount=40, proceeds=600, pnl=200),
                                    self.sell(seq=3, amount=60, proceeds=300, pnl=-300)])
        state = self.observe()
        self.assertEqual((state["completed_count"], state["sell_record_count"]), (1, 2))
        self.assert_committed_deletion_blocks("sells", "3", control)

    def test_tampered_decision_timestamp_provenance_is_rejected(self) -> None:
        self.capture()
        path = n3.record_path(self.sidecar, "snapshots", "POSITION")
        snapshot = state_store.read_json(path, {})
        snapshot["raw_inputs"]["analysis_duration_sec"] = 4.0
        if "content_hash" in snapshot:
            snapshot["content_hash"] = n3.digest({key: value for key, value in snapshot.items() if key != "content_hash"})
        state_store.atomic_write_json(path, snapshot)
        control = self.write_ledger([self.buy()])
        with self.assertRaisesRegex(RuntimeError, "provenance mismatch"):
            self.observe()
        self.assertEqual((self.root / "data" / "paper_trades.json").read_bytes(), control)

    def test_restart_continues_from_cursor_without_rewriting_entries(self) -> None:
        self.capture()
        self.write_ledger([self.buy()])
        first = self.observe()
        entry_path = n3.record_path(self.sidecar, "entries", "POSITION")
        before = entry_path.read_bytes(), entry_path.stat().st_mtime_ns
        self.write_ledger([self.buy(), self.sell()])
        next_state = self.observe()
        self.assertEqual(first["cursor"], 1)
        self.assertEqual(next_state["cursor"], 2)
        self.assertEqual(next_state["completed_count"], 1)
        self.assertEqual((entry_path.read_bytes(), entry_path.stat().st_mtime_ns), before)
        self.assertEqual(len(n3.read_records(self.sidecar, "completed", 408)), 1)

    def test_completed_count_hardcap_freezes_before_next_buy(self) -> None:
        contract = {**n3.CONTRACT, "hard_cap": {"active_kst_days": 28, "completed_control": 1}}
        with patch.object(n3, "CONTRACT", contract):
            self.replace_contract(contract)
            self.capture()
            control = self.write_ledger([self.buy(), self.sell(), self.buy(pid="AFTER-CLOSE", seq=3)])
            state = self.observe()
            self.assertEqual((state["status"], state["cursor"], state["admitted_count"], state["completed_count"]),
                             ("CLOSED", 2, 1, 1))
            self.assertEqual(state_store.read_json(self.sidecar / "closure.json", {})["end_event_seq"], 2)
            self.assertEqual((self.root / "data" / "paper_trades.json").read_bytes(), control)

    def test_interrupted_hardcap_replay_uses_event_cursor_for_completion_count(self) -> None:
        contract = {**n3.CONTRACT, "hard_cap": {"active_kst_days": 28, "completed_control": 1}}
        with patch.object(n3, "CONTRACT", contract):
            self.replace_contract(contract)
            self.capture()
            self.write_ledger([self.buy(), self.sell(), self.buy(pid="AFTER-CLOSE", seq=3)])
            original = n3.atomic_write_json

            def interrupt_close(path, document):
                if Path(path).name == "closure.json":
                    raise OSError("test interrupted closure write")
                return original(path, document)

            with patch.object(n3, "atomic_write_json", side_effect=interrupt_close):
                with self.assertRaisesRegex(OSError, "interrupted closure"):
                    self.observe()
            retried = self.observe()
            closure = state_store.read_json(self.sidecar / "closure.json", {})
            self.assertEqual(closure["end_event_seq"], 2)
            self.assertEqual(retried["completed_count"], 1)
            self.assertEqual(retried["admitted_count"], 1)

    def test_restart_continuity_and_hardcap_closes_admissions_and_censors(self) -> None:
        contract = {**n3.CONTRACT, "hard_cap": {"active_kst_days": 1, "completed_control": 400}}
        with patch.object(n3, "CONTRACT", contract):
            self.replace_contract(contract)
            self.capture()
            self.write_ledger([self.buy()])
            first = self.observe()
            self.assertEqual(first["status"], "CLOSED")
            closure = (self.sidecar / "closure.json").read_bytes()
            restarted = self.observe()
            self.assertEqual(restarted["cursor"], first["cursor"])
            self.assertEqual(restarted["admitted_count"], 1)
            self.assertEqual(restarted["version"], first["version"] + 1)
            control = self.write_ledger([self.buy(), self.buy(pid="AFTER-CLOSE", seq=2), self.sell(seq=3)])
            report_before = (self.sidecar / "final_report.json").read_bytes()
            last = self.observe()
            self.assertEqual((last["status"], last["admitted_count"], last["completed_count"]), ("CLOSED", 1, 0))
            self.assertEqual(last["cursor"], 1)
            self.assertEqual((self.sidecar / "final_report.json").read_bytes(), report_before)
            report = state_store.read_json(self.sidecar / "final_report.json", {})
            self.assertEqual(report["censored_position_ids"], ["POSITION"])
            self.assertEqual((self.sidecar / "closure.json").read_bytes(), closure)
            self.assertEqual((self.root / "data" / "paper_trades.json").read_bytes(), control)


class N3ControlIsolationTests(unittest.TestCase):
    def test_unstarted_worker_and_full_queue_fail_without_starting_thread(self) -> None:
        with patch.object(n3, "_worker", None), patch.object(n3.threading, "Thread") as thread:
            self.assertFalse(n3.submit_control_entry("POSITION", {"would_skip": True}))
            thread.assert_not_called()
        bounded = queue.Queue(maxsize=1)
        bounded.put_nowait(("FIRST", {}))
        with patch.object(n3, "_worker", Mock(is_alive=Mock(return_value=True))), patch.object(n3, "_queue", bounded), \
                patch.object(n3, "_capture_failures", 0):
            self.assertFalse(n3.submit_control_entry("SECOND", {"would_skip": True}))
            self.assertEqual(n3._capture_failures, 1)
            self.assertEqual(bounded.qsize(), 1)

    def test_shadow_hits_nonhits_and_recorder_failures_have_identical_control_buy(self) -> None:
        decisions = observation_tracker.ObservationDecision(True, "SIGNATURE:MINT", False, ("baseline_v1",))
        report = SimpleNamespace(safety_score=100, route_type="A", reasons=[], liquidity_usd="10000", lp_locked_percent="80")
        frozen = datetime.fromisoformat(stamp(20))
        calls = []
        for mode in ("hit", "nonhit", "prepare_failure", "submit_failure", "queue_rejected"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
                stack.enter_context(patch.dict("os.environ", {"OBSERVATION_MODE": "true", "APPROVED_SIGNAL_PAPER_MODE": "true",
                                                               "APPROVED_SIGNAL_MAX_OPEN_POSITIONS": "8", "JUPITER_API_KEY": ""}))
                for name in ("record_funnel_stage", "record_wallet_ws_activity", "record_memory_phase"):
                    stack.enter_context(patch.object(monitor, name))
                stack.enter_context(patch.object(monitor, "current_rss_bytes", return_value=0))
                clock = stack.enter_context(patch.object(monitor, "datetime", wraps=datetime))
                clock.now.return_value = frozen
                stack.enter_context(patch.object(monitor.state_store, "get_route_initial_stop_streak", return_value=(0, 0)))
                stack.enter_context(patch.object(monitor, "token_cooldown_is_active", return_value=False))
                stack.enter_context(patch.object(analyzer, "analyze_token", new=AsyncMock(return_value=report)))
                stack.enter_context(patch.object(risk_manager, "paper_cash_balance", new=AsyncMock(return_value=10_000_000_000)))
                buy = stack.enter_context(patch.object(risk_manager, "record_paper_buy", new=AsyncMock(return_value="POSITION")))
                stack.enter_context(patch.object(risk_manager, "record_paper_rejection", new=AsyncMock()))
                stack.enter_context(patch.object(executor, "jupiter_quote", new=AsyncMock(side_effect=[
                    {"outAmount": "2500", "routePlan": [{}], "priceImpactPct": "0.5", "slippageBps": "100"},
                    {"outAmount": "990000", "routePlan": [{}], "priceImpactPct": "0.4", "slippageBps": "100"}])))
                for name in ("record_candidate_discovery", "record_observation_decision"):
                    stack.enter_context(patch.object(observation_tracker, name, new=AsyncMock(return_value=decisions)))
                stack.enter_context(patch.object(observation_tracker, "mark_paper_experiment_status", return_value=True))
                stack.enter_context(patch("src.wallet_performance.record_paper_buy_success", new=AsyncMock()))
                stack.enter_context(patch.object(state_store, "PAPER_TRADES_PATH", Path(temporary) / "paper_trades.json"))
                prepare = stack.enter_context(patch.object(n3, "prepare_entry_snapshot", return_value={"would_skip": mode != "nonhit"},
                                                          side_effect=RuntimeError("prepare failure") if mode == "prepare_failure" else None))
                submit = stack.enter_context(patch.object(n3, "submit_control_entry", return_value=mode != "queue_rejected",
                                                         side_effect=RuntimeError("submit failure") if mode == "submit_failure" else None))
                asyncio.run(monitor.process_paper_signal("MINT", 1000, 6, 2_000_000_000, "WALLET", "SIGNATURE", START, "A"))
                prepare.assert_called_once()
                buy.assert_awaited_once()
                submit.assert_called_once_with("POSITION", None if mode == "prepare_failure" else {"would_skip": mode != "nonhit"})
                calls.append(buy.await_args)
        self.assertTrue(all(call == calls[0] for call in calls))


if __name__ == "__main__":
    unittest.main()
