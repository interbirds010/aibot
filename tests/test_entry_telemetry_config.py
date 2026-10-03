"""비밀 제외 설정 fingerprint의 실제 기본값과 결정성을 검증한다."""
from __future__ import annotations

import json
import os
import unittest
from unittest import mock

from src.research import entry_telemetry_config as config
from src.research import entry_telemetry_epoch as epoch


class ConfigTests(unittest.TestCase):
    def test_same_effective_config_and_key_order_hash(self):
        first = config.effective_config({})
        second = config.effective_config({})
        self.assertEqual(config.config_fingerprint(first), config.config_fingerprint(second))
        self.assertEqual(config.config_fingerprint(first), config.config_fingerprint(dict(reversed(list(first.items())))))
        self.assertGreater(len(first), 90)

    def test_actual_defaults_missing_and_explicit_are_same(self):
        missing = config.effective_config({})
        explicit = config.effective_config({"TRADING_MODE": " PAPER ", "OBSERVATION_MODE": "false",
            "APPROVED_SIGNAL_PAPER_MODE": "0", "APPROVED_SIGNAL_MAX_OPEN_POSITIONS": "8",
            "WALLET_RELOAD_SECONDS": "5", "SOLANA_RPC_OVERALL_ATTEMPT_BUDGET": "6",
            "SOLANA_RPC_PROVIDER_ATTEMPTS": "2", "SOLANA_PUBLIC_RPC_MAX_RPS": "2"})
        self.assertEqual(missing, explicit)
        self.assertEqual(missing["quote.default_slippage_bps"], 100)
        self.assertTrue(missing["rpc.solana_public.enabled"])
        self.assertFalse(missing["rpc.helius.enabled"])
        self.assertEqual(missing["risk_manager.stop_loss_ratio"], .85)

    def test_secrets_and_url_rotation_ignored(self):
        first = {"HELIUS_API_KEY": "FAKE_A", "HELIUS_RPC_HTTP_URL": "https://fake.test/?api-key=FAKE_A",
                 "JUPITER_API_KEY": "FAKE_J", "SOLANA_KEY_ENCRYPTION_KEY": "FAKE_K"}
        second = {**first, "HELIUS_API_KEY": "FAKE_B", "HELIUS_RPC_HTTP_URL": "https://other.test/private/FAKE_B",
                  "JUPITER_API_KEY": "FAKE_OTHER", "SOLANA_KEY_ENCRYPTION_KEY": "FAKE_OTHER_K"}
        self.assertEqual(config.effective_config(first), config.effective_config(second))
        encoded = json.dumps(config.effective_config(first))
        for forbidden in ("FAKE", "https://", "api-key", "encrypted_private", "HELIUS_API_KEY"):
            self.assertNotIn(forbidden, encoded)
        self.assertEqual(config.project_config({"api_key": "FAKE", "paper_buy_basis_points": 50}), {"paper_buy_basis_points": 50})

    def test_influential_env_change_alters_hash(self):
        initial = config.config_fingerprint(config.effective_config({}))
        for setting, value in {"TRADING_MODE": "live", "OBSERVATION_MODE": "true",
            "APPROVED_SIGNAL_PAPER_MODE": "true", "APPROVED_SIGNAL_MAX_OPEN_POSITIONS": "9",
            "WALLET_RELOAD_SECONDS": "10", "SOLANA_RPC_OVERALL_ATTEMPT_BUDGET": "7",
            "SOLANA_RPC_PROVIDER_ATTEMPTS": "3", "SOLANA_PUBLIC_RPC_MAX_RPS": "3",
            "WALLET_MAX_WALLETS": "21", "WALLET_REFRESH_HOURS": "2", "WALLET_MIN_SOL_BALANCE": "0.2",
            "WALLET_MAX_DAILY_TX": "301", "WALLET_SIGNATURES_PER_PROGRAM": "51", "WALLET_MAX_CANDIDATES": "251",
            "WALLET_RPC_MIN_INTERVAL_SECONDS": "0.4", "WALLET_ELITE_RESERVED_SLOTS": "8",
            "ANKR_SOLANA_RPC_URL": "https://fake.test"}.items():
            with self.subTest(setting=setting):
                self.assertNotEqual(initial, config.config_fingerprint(config.effective_config({setting: value})))

    def test_wss_presence_and_secret_rotation(self):
        missing = config.effective_config({})
        first = config.effective_config({"HELIUS_RPC_WS_URL": "wss://fake.test/${HELIUS_API_KEY}", "HELIUS_API_KEY": "FAKE_A"})
        second = config.effective_config({"HELIUS_RPC_WS_URL": "wss://other.test/${HELIUS_API_KEY}", "HELIUS_API_KEY": "FAKE_B"})
        unresolved = config.effective_config({"HELIUS_RPC_WS_URL": "wss://fake.test/${HELIUS_API_KEY}"})
        self.assertFalse(missing["monitor.helius_ws_configured"])
        self.assertEqual(missing, unresolved)
        self.assertTrue(first["monitor.helius_ws_configured"])
        self.assertEqual(first, second)
        self.assertNotEqual(config.config_fingerprint(missing), config.config_fingerprint(first))

    def test_threshold_sizing_sltp_scheduler_schema_change(self):
        from src import executor, monitor, risk_manager
        initial = config.config_fingerprint(config.effective_config({}))
        for module, name, value in ((monitor, "MOMENTUM_MIN_VOLUME_M5_USD", 18000),
            (executor, "PAPER_BUY_BASIS_POINTS", 51), (risk_manager, "STOP_LOSS_RATIO", .84),
            (monitor, "MOMENTUM_MAX_CANDIDATES", 9)):
            with self.subTest(name=name), mock.patch.object(module, name, value):
                self.assertNotEqual(initial, config.config_fingerprint(config.effective_config({})))
        with mock.patch.object(epoch, "SCHEMAS", {"predictor": 3, "receipt": 1, "outcome": 1}):
            self.assertNotEqual(initial, config.config_fingerprint(config.effective_config({})))

    def test_invalid_missing_values_are_deterministic_failures(self):
        for environ in ({"TRADING_MODE": ""}, {"APPROVED_SIGNAL_MAX_OPEN_POSITIONS": "0"},
            {"OBSERVATION_MODE": "unknown"}, {"SOLANA_PUBLIC_RPC_MAX_RPS": "nan"},
            {"WALLET_RELOAD_SECONDS": "infinity"}):
            with self.subTest(environ=environ), self.assertRaises((RuntimeError, ValueError)):
                config.effective_config(environ)
        for invalid in ({"api_key": "FAKE"}, {"trading_mode": "https://fake"},
            {"paper_buy_basis_points": True}, {"observation_mode": 1}):
            with self.assertRaises(RuntimeError):
                config.validate_config(invalid)

    def test_monitor_and_risk_same_shared_config(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            expected = config.effective_config({})
            self.assertEqual(epoch.safe_runtime_config(), expected)
            self.assertEqual(config.project_config(expected), expected)
            self.assertEqual(epoch._safe_config(expected), expected)

    def test_future_tool_dotenv_priority_matches_runtime_startup(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from scripts.entry_telemetry_cutover import _paper_config
        from dotenv import load_dotenv
        from scripts import local_paper_runner as runner
        with TemporaryDirectory() as temporary, mock.patch.dict(os.environ, {}, clear=True):
            root = Path(temporary)
            (root / ".env").write_text("TRADING_MODE=paper\nOBSERVATION_MODE=false\n"
                "APPROVED_SIGNAL_MAX_OPEN_POSITIONS=12\nWALLET_RELOAD_SECONDS=11\n"
                "WALLET_MAX_DAILY_TX=321\nWALLET_MAX_CANDIDATES=${APPROVED_SIGNAL_MAX_OPEN_POSITIONS}\nJUPITER_API_KEY=FAKE_SECRET\n"
                "ANKR_SOLANA_RPC_URL=https://fake.test/private\n", encoding="utf-8")
            # 배포/서비스 실행 없이 미래 runner -> dotenv 우선순위를 재현한다.
            child_environment = runner.paper_environment(root)
            with mock.patch.dict(os.environ, child_environment, clear=True):
                load_dotenv(root / ".env", override=False)
                expected = epoch.safe_runtime_config()
            before_environment = dict(os.environ)
            actual = _paper_config(root)
            self.assertEqual(dict(os.environ), before_environment)
            self.assertEqual(actual, expected)
            self.assertEqual(actual["feeder.max_candidates"], 8)
            self.assertEqual(actual["wallet_reload_seconds"], 11)
            self.assertEqual(actual["approved_signal_max_open_positions"], 8)
            self.assertEqual(actual["feeder.max_daily_transactions"], 321)
            self.assertTrue(actual["observation_mode"])
            self.assertTrue(actual["rpc.ankr.enabled"])
            self.assertNotIn("FAKE_SECRET", json.dumps(actual))

    def test_all_effective_fields_survive_recorder_row_projection(self):
        from src.research import entry_telemetry as telemetry
        effective = config.effective_config({})
        with mock.patch.object(telemetry, "_provenance", {"safe_config": effective}):
            envelope = telemetry._envelope("predictor")
        self.assertEqual(envelope["provenance"]["safe_config"], effective)
        self.assertGreater(len(envelope["provenance"]["safe_config"]), telemetry.MAX_ITEMS)



if __name__ == "__main__":
    unittest.main()
