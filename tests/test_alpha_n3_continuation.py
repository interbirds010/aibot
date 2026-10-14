"""분리된 N3 continuation의 rule·provenance·불변 기록을 검증한다."""
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.research import alpha_n3_continuation as continuation
from src.research import n3_shadow as n3
from src.state_store import atomic_write_json
from tests.test_entry_telemetry_join import row, seal


class ContinuationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="N3 경로 with space ")
        self.addCleanup(self.tmp.cleanup)
        self.output = Path(self.tmp.name)
        self.manifest = {"cohort_id": "cohort", "contract": deepcopy(n3.CONTRACT),
            "contract_hash": n3.digest(n3.CONTRACT), "build": {"git_sha": "old"},
            "start_utc": "2026-10-03T00:00:00+00:00"}
        self.marker = {"n3_closure": {"decision": "N3_CONTINUE_PROSPECTIVE_VALIDATION", "cohort_id": "cohort",
            "original_manifest_hash": n3.digest(self.manifest)},
            "start_event_seq": 10, "start_utc": "2026-10-04T00:00:00+00:00",
            "telemetry_epoch_id": "epoch-1", "build_sha": "f" * 40, "config_fingerprint": "c" * 64}
        continuation.register_continuation(self.output, self.manifest, self.marker)

    def streams(self, preflight=4.0, analysis=2.0):
        predictor = row("predictors")
        from datetime import datetime, timedelta
        signal = datetime.fromisoformat(predictor["identity"]["signal_detected_at"])
        stamps = {"analysis_completed_at": analysis,
                  "preflight_completed_at": analysis + preflight,
                  "entry_decision_at": analysis + preflight + .1}
        predictor["decision_outcome"] = "BUY"
        predictor["predictors"] = {"timestamps": {k: {"wall_utc": (signal + timedelta(seconds=v)).isoformat()} for k, v in stamps.items()}}
        return [seal(predictor)], [row("receipts")], [row("outcomes")]

    def ledger(self, streams):
        if not streams[1]:
            return None
        predictor, receipt = streams[0][0], streams[1][0]
        stamps = predictor["predictors"]["timestamps"]
        from datetime import timedelta
        from src.research import accelerated_alpha as core
        decision = core.utc(stamps["entry_decision_at"]["wall_utc"])
        seq, pid = receipt["identity"]["event_seq"], receipt["identity"]["trade_id"]
        buy = {"type": "BUY", "event_seq": seq, "position_id": pid, "token_amount_raw": 10,
               "mint": "MINT", "route_type": "A", "signal_detected_at": predictor["identity"]["signal_detected_at"],
               "analysis_completed_at": stamps.get("analysis_completed_at", {}).get("wall_utc"),
               "entry_quote_at": stamps.get("preflight_completed_at", {}).get("wall_utc"),
               "at": (decision + timedelta(seconds=.1)).isoformat()}
        return {"schema_version": 2, "next_event_seq": seq + 3, "events": [buy,
                {"type": "SELL", "event_seq": seq + 1, "position_id": pid, "token_amount_raw": 4, "at": buy["at"]},
                {"type": "SELL", "event_seq": seq + 2, "position_id": pid, "token_amount_raw": 6, "at": buy["at"]}]}

    def observe(self, *streams, ledger="AUTO"):
        with patch("scripts.entry_telemetry_acceptance.validate_predictor"):
            return continuation.observe_rows(self.output, *streams, ledger=self.ledger(streams) if ledger == "AUTO" else ledger)

    def test_rule_exact_and_separated_outcome_idempotence(self):
        streams = self.streams()
        self.assertEqual(self.observe(*streams)["appended_entries"], 1)
        path = next((self.output / "entries").glob("*.json"))
        before = path.read_bytes()
        self.assertEqual(self.observe(*streams)["appended_entries"], 0)
        self.assertEqual(path.read_bytes(), before)
        entry = n3.read_immutable(path)
        self.assertTrue(entry["would_skip"])
        self.assertEqual(entry["raw_inputs"], {"analysis_duration_sec": 2.0, "quote_preflight_duration_sec": 4.0})
        self.assertNotIn("outcome", entry)
        self.assertTrue(list((self.output / "outcomes").glob("*.json")))
        outcome = n3.read_immutable(next((self.output / "outcomes").glob("*.json")))
        self.assertEqual(outcome["entry_hash"], n3.digest(entry))

    def test_exact_rule_edges(self):
        self.assertFalse(n3.classify({"analysis_duration_sec": n3.RULE["parts"][1]["threshold"], "quote_preflight_duration_sec": 0}))
        self.assertTrue(n3.classify({"analysis_duration_sec": 2, "quote_preflight_duration_sec": n3.RULE["parts"][0]["low"]}))
        self.assertFalse(n3.classify({"analysis_duration_sec": 2, "quote_preflight_duration_sec": n3.RULE["parts"][0]["high"]}))

    def test_future_leakage_validation_is_not_skipped(self):
        with patch("scripts.entry_telemetry_acceptance.validate_predictor", side_effect=ValueError("future trajectory")), self.assertRaisesRegex(ValueError, "future"):
            continuation.observe_rows(self.output, *self.streams())

    def test_build_mismatch(self):
        streams = self.streams()
        for stream in streams:
            stream[0]["build_sha"] = "e" * 40
            stream[0] = seal(stream[0])
        with self.assertRaisesRegex(ValueError, "epoch/build/config"):
            self.observe(*streams)

    def test_missing_timing_is_unknown(self):
        streams = self.streams()
        streams[0][0]["predictors"]["timestamps"].pop("analysis_completed_at")
        streams[0][0] = seal(streams[0][0])
        self.observe(*streams)
        entry = n3.read_immutable(next((self.output / "entries").glob("*.json")))
        self.assertIsNone(entry["would_skip"])

    def test_pending_flush_never_fabricates_buy(self):
        streams = self.streams()
        result = self.observe(streams[0], [], [])
        self.assertEqual(result["pending_buy_receipts"], 1)
        self.assertFalse((self.output / "entries").exists())

    def test_duplicate_rejected(self):
        streams = self.streams()
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.observe(streams[0] * 2, *streams[1:])

    def test_original_contract_and_case_a_refused(self):
        marker = {**self.marker, "n3_closure": {"decision": "N3_LOW_FRESH_COVERAGE", "cohort_id": "cohort"}}
        with self.assertRaisesRegex(ValueError, "CASE B"):
            continuation.register_continuation(self.output, self.manifest, marker)
        manifest = deepcopy(self.manifest)
        manifest["contract"]["sample_gates"]["completed_control"] = 20
        with self.assertRaisesRegex(ValueError, "contract"):
            continuation.register_continuation(self.output, manifest, self.marker)

    def test_original_400_completion_hard_cap_preserved(self):
        output = self.output / "hard cap"
        marker = {**self.marker, "start_event_seq": 1000}
        baseline = [{"position_id": f"old-{i}", "cohort_id": "cohort", "rule_hash": n3.RULE_HASH,
            "signal_timestamp": self.manifest["start_utc"], "buy_event_seq": i + 1,
            "entry_token_amount_raw": 1, "would_skip": False} for i in range(399)]
        continuation.register_continuation(output, self.manifest, marker,
            baseline_entries=baseline, baseline_completed=baseline)
        streams = self.streams()
        streams[1][0]["identity"]["event_seq"] = 1000
        streams[1][0] = seal(streams[1][0])
        streams[2][0]["identity"].update(buy_event_seq=1000, event_seq=1001)
        streams[2][0] = seal(streams[2][0])
        ledger = {"schema_version": 2, "next_event_seq": 1002, "events": [
            {"type": "BUY", "event_seq": 1000, "position_id": "trade-1", "token_amount_raw": 1,
             "mint": "MINT", "route_type": "A", "signal_detected_at": marker["start_utc"],
             "analysis_completed_at": "2026-10-04T00:00:02+00:00", "entry_quote_at": "2026-10-04T00:00:06+00:00",
             "at": "2026-10-04T00:00:06.200000+00:00"},
            {"type": "SELL", "event_seq": 1001, "position_id": "trade-1", "token_amount_raw": 1, "at": "2026-10-04T00:00:07+00:00"}]}
        with patch("scripts.entry_telemetry_acceptance.validate_predictor"):
            continuation.observe_rows(output, *streams, ledger=ledger)
        closure = n3.read_immutable(output / "segment_close.json")
        self.assertEqual(closure["completed_control"], 400)
        self.assertEqual(closure["reason"], "ORIGINAL_HARD_CAP")
        self.assertEqual(closure["end_event_seq"], 1001)

    def test_carried_open_position_partial_sell_preserved(self):
        output = self.output / "carried"
        entry = {"position_id": "old", "cohort_id": "cohort", "rule_hash": n3.RULE_HASH,
            "signal_timestamp": self.manifest["start_utc"], "buy_event_seq": 1,
            "entry_token_amount_raw": 10, "would_skip": True}
        continuation.register_continuation(output, self.manifest, self.marker, baseline_entries=[entry])
        ledger = {"schema_version": 2, "next_event_seq": 11, "events": [
            {"type": "BUY", "event_seq": 1, "position_id": "old", "token_amount_raw": 10},
            {"type": "SELL", "event_seq": 5, "position_id": "old", "token_amount_raw": 4},
            {"type": "SELL", "event_seq": 10, "position_id": "old", "token_amount_raw": 6},
            *({"type": "SIGNAL_REJECTED", "event_seq": seq} for seq in range(2, 10) if seq != 5)]}
        continuation.observe_rows(output, [], [], [], ledger=ledger)
        carried = n3.read_immutable(output / "carried_sells/10.json")
        self.assertEqual(carried["source_event"]["token_amount_raw"], 6)
        self.assertEqual(n3.read_immutable(output / "continuity.json")["baseline_entries"][0]["would_skip"], True)
        with self.assertRaisesRegex(ValueError, "carried"):
            continuation.observe_rows(output, [], [], [])

    def test_real_predictor_schema_passes(self):
        from contextlib import ExitStack
        import queue
        from src.research import entry_telemetry as telemetry
        from datetime import datetime, timedelta
        with ExitStack() as stack:
            provenance = {"git_sha": "f" * 40, "session_id": "session", "safe_config": {},
                "config_fingerprint": n3.digest({})}
            marker = {**self.marker, "config_fingerprint": n3.digest({})}
            output = self.output / "real-schema"
            continuation.register_continuation(output, self.manifest, marker)
            stack.enter_context(patch.object(telemetry, "_epoch", marker))
            stack.enter_context(patch.object(telemetry, "_provenance", provenance))
            capture = telemetry.begin_signal(mint="MINT", route_type="A", signal_detected_at=marker["start_utc"], signal_id="signal-1")
            base = datetime.fromisoformat(marker["start_utc"])
            for name, offset in (("analysis_completed_at", 2), ("preflight_completed_at", 6), ("entry_decision_at", 6.1)):
                capture.timestamps[name] = {"wall_utc": (base + timedelta(seconds=offset)).isoformat(),
                    "monotonic_ns": int(offset * 1e9), "semantics": "test decision-time"}
            capture.sections["decision"]["outcome"] = "BUY"
            predictor = seal(telemetry._row(capture))
            receipt = row("receipts")
            receipt["provenance"] = {"config_fingerprint": marker["config_fingerprint"]}
            receipt["identity"].update({k: predictor["identity"][k] for k in ("signal_id", "strategy_family", "source_wallet_hash", "source_signature_hash")})
            receipt = seal(receipt)
            result = continuation.observe_rows(output, [predictor], [receipt], [])
            self.assertEqual(result["appended_entries"], 1)

    def test_no_native_control_clocks_remains_unknown(self):
        self.observe(*self.streams(), ledger=None)
        entry = n3.read_immutable(next((self.output / "entries").glob("*.json")))
        self.assertIsNone(entry["would_skip"])
        self.assertEqual(entry["timestamp_semantics"], "UNVERIFIED_NO_CONTROL_BUY_CLOCK")

    def test_native_clock_threshold_is_not_approximated_from_later_physical_mark(self):
        from datetime import timedelta
        from src.research import accelerated_alpha as core
        streams = self.streams(preflight=1.0, analysis=1.497461)
        ledger = self.ledger(streams)
        signal = core.utc(streams[0][0]["identity"]["signal_detected_at"])
        ledger["events"][0]["analysis_completed_at"] = (signal + timedelta(seconds=1.497460)).isoformat()
        ledger["events"][0]["entry_quote_at"] = (signal + timedelta(seconds=2.497460)).isoformat()
        self.observe(*streams, ledger=ledger)
        entry = n3.read_immutable(next((self.output / "entries").glob("*.json")))
        self.assertFalse(entry["would_skip"])
        self.assertTrue(n3.classify({"analysis_duration_sec": 1.497461, "quote_preflight_duration_sec": 1}))

    def test_ledger_gaps_identity_and_sell_quantity_fail_before_any_record(self):
        for change in ("gap", "mint", "route", "signal", "oversold", "next_bool", "partial_outcome"):
            streams = self.streams()
            ledger = self.ledger(streams)
            if change == "gap":
                ledger["events"].pop(1)
            elif change == "mint":
                ledger["events"][0]["mint"] = "FOREIGN"
            elif change == "route":
                ledger["events"][0]["route_type"] = "B"
            elif change == "signal":
                ledger["events"][0]["signal_detected_at"] = "2026-10-04T00:00:01+00:00"
            elif change == "oversold":
                ledger["events"][1]["token_amount_raw"] = 11
            elif change == "next_bool":
                ledger["next_event_seq"] = True
            else:
                streams[2][0]["identity"]["event_seq"] = 11
                streams[2][0] = seal(streams[2][0])
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.observe(*streams, ledger=ledger)
            self.assertFalse((self.output / "entries").exists())
            self.assertFalse((self.output / "segment_close.json").exists())

    def test_duplicate_buy_position_cannot_overwrite_baseline_admission(self):
        output = self.output / "duplicate-pid"
        baseline = {"position_id": "trade-1", "cohort_id": "cohort", "rule_hash": n3.RULE_HASH,
            "signal_timestamp": self.manifest["start_utc"], "buy_event_seq": 1,
            "entry_token_amount_raw": 10, "would_skip": False}
        continuation.register_continuation(output, self.manifest, self.marker, baseline_entries=[baseline])
        streams = self.streams()
        ledger = self.ledger(streams)
        ledger["events"].append({"type": "BUY", "event_seq": 1, "position_id": "trade-1", "token_amount_raw": 10})
        ledger["events"].extend({"type": "SIGNAL_REJECTED", "event_seq": seq} for seq in range(2, 10))
        with patch("scripts.entry_telemetry_acceptance.validate_predictor"), self.assertRaisesRegex(ValueError, "duplicate BUY"):
            continuation.observe_rows(output, *streams, ledger=ledger)
        self.assertFalse((output / "entries").exists())

    def test_invalid_clock_after_hardcap_scan_creates_no_irreversible_closure(self):
        output = self.output / "invalid-hardcap"
        baseline = [{"position_id": f"old-{i}", "cohort_id": "cohort", "rule_hash": n3.RULE_HASH,
            "signal_timestamp": self.manifest["start_utc"], "buy_event_seq": i + 1,
            "entry_token_amount_raw": 1, "would_skip": False} for i in range(399)]
        marker = {**self.marker, "start_event_seq": 1000}
        continuation.register_continuation(output, self.manifest, marker, baseline_entries=baseline, baseline_completed=baseline)
        streams = self.streams()
        streams[1][0]["identity"]["event_seq"] = 1000
        streams[1][0] = seal(streams[1][0])
        streams[2][0]["identity"].update(buy_event_seq=1000, event_seq=1002)
        streams[2][0] = seal(streams[2][0])
        ledger = self.ledger(streams)
        ledger["events"][0]["analysis_completed_at"] = "2026-10-04T00:00:02.000001+00:00"
        with patch("scripts.entry_telemetry_acceptance.validate_predictor"), self.assertRaisesRegex(ValueError, "physical"):
            continuation.observe_rows(output, *streams, ledger=ledger)
        self.assertFalse((output / "entries").exists())
        self.assertFalse((output / "segment_close.json").exists())
        self.assertFalse((output / "carried_sells").exists())

    def test_registration_rejects_manifest_reseal_and_original_hardcap(self):
        forged = {**self.manifest, "build": {"git_sha": "foreign"}}
        with self.assertRaisesRegex(ValueError, "manifest boundary hash"):
            continuation.register_continuation(self.output / "foreign", forged, self.marker)
        baseline = [{"position_id": f"old-{i}", "cohort_id": "cohort", "rule_hash": n3.RULE_HASH,
            "signal_timestamp": self.manifest["start_utc"], "buy_event_seq": i + 1,
            "entry_token_amount_raw": 1, "would_skip": False} for i in range(400)]
        marker = {**self.marker, "start_event_seq": 1000}
        with self.assertRaisesRegex(ValueError, "hard cap"):
            continuation.register_continuation(self.output / "already-closed", self.manifest, marker,
                baseline_entries=baseline, baseline_completed=baseline)

    def test_registration_sources_pin_original_manifest_and_archive_bytes(self):
        from src.research import alpha_transition, n3_feasibility
        frozen = self.output / "frozen"
        target = self.output / "target"
        sidecar = frozen / "data/n3_shadow"
        n3.immutable(sidecar / "manifest.json", self.manifest)
        entry = {"position_id": "old", "cohort_id": "cohort", "rule_hash": n3.RULE_HASH,
                 "signal_timestamp": self.manifest["start_utc"], "buy_event_seq": 1,
                 "entry_token_amount_raw": 10, "would_skip": True}
        path = n3.record_path(sidecar, "entries", "old")
        n3.immutable(path, entry)
        atomic_write_json(sidecar / "runtime_supervisor.disabled", {"disabled": True, "at_kst": "2026-10-03T09:00:00+09:00"})
        boundary = {"frozen_runtime": str(frozen), "original_manifest_hash": n3.digest(self.manifest),
                    "frozen_n3_archive_hashes": continuation.epoch.state_hashes(frozen)}
        with patch.object(alpha_transition, "read_boundary", return_value=boundary), \
                patch.object(n3_feasibility, "source_guard", return_value=self.manifest), \
                patch("scripts.entry_telemetry_cutover.stopped_proof"):
            manifest, entries, completed = continuation.registration_sources(target, sidecar / "manifest.json")
            self.assertEqual(manifest, self.manifest)
            self.assertEqual(entries, [entry])
            self.assertEqual(completed, [])
            with self.assertRaisesRegex(ValueError, "must reference"):
                continuation.registration_sources(target, self.output / "other/manifest.json")
            changed = {**entry, "would_skip": False}
            atomic_write_json(path, {**changed, "content_hash": n3.digest(changed)})
            with self.assertRaisesRegex(ValueError, "archive hash mismatch"):
                continuation.registration_sources(target, sidecar / "manifest.json")

    def test_original_28_day_cap_censors_new_open_trade_at_buy_boundary(self):
        from datetime import datetime, timedelta, timezone
        output = self.output / "day-cap"
        manifest = {**self.manifest, "start_utc": "2026-09-07T00:00:00+00:00"}
        marker = {**self.marker, "start_event_seq": 1000,
                  "n3_closure": {**self.marker["n3_closure"], "original_manifest_hash": n3.digest(manifest)}}
        baseline = [{"position_id": f"old-{index}", "cohort_id": "cohort", "rule_hash": n3.RULE_HASH,
            "signal_timestamp": (datetime(2026, 9, 7, tzinfo=timezone.utc) + timedelta(days=index)).isoformat(),
            "buy_event_seq": index + 1, "entry_token_amount_raw": 1, "would_skip": False} for index in range(27)]
        continuation.register_continuation(output, manifest, marker, baseline_entries=baseline, baseline_completed=baseline)
        streams = self.streams()
        streams[1][0]["identity"]["event_seq"] = 1000
        streams[1][0] = seal(streams[1][0])
        streams[2][0]["identity"].update(buy_event_seq=1000, event_seq=1002)
        streams[2][0] = seal(streams[2][0])
        with patch("scripts.entry_telemetry_acceptance.validate_predictor"):
            continuation.observe_rows(output, *streams, ledger=self.ledger(streams))
        closure = n3.read_immutable(output / "segment_close.json")
        self.assertEqual(closure["active_kst_days"], 28)
        self.assertEqual(closure["end_event_seq"], 1000)
        self.assertEqual(closure["censored_position_ids"], ["trade-1"])
        self.assertFalse((output / "outcomes").exists())
