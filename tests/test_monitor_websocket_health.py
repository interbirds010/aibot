from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from src import monitor, state_store


class HttpStatusError(Exception):
    def __init__(self, status: int) -> None:
        self.response = SimpleNamespace(status_code=status)


class MonitorWebSocketHealthTests(unittest.TestCase):
    def test_handshake_statuses_are_canonical(self) -> None:
        self.assertEqual(
            monitor.canonical_websocket_failure_reason(HttpStatusError(401)),
            "WS_AUTH_FAILED",
        )
        self.assertEqual(
            monitor.canonical_websocket_failure_reason(HttpStatusError(429)),
            "WS_RATE_LIMITED",
        )

    def test_subscription_errors_do_not_use_raw_messages_as_categories(self) -> None:
        self.assertEqual(
            monitor.websocket_subscription_failure_reason({
                "code": -32000,
                "message": "changing provider-specific rejection text",
            }),
            "WS_SUBSCRIPTION_FAILED",
        )
        self.assertEqual(
            monitor.websocket_subscription_failure_reason({
                "code": 429,
                "message": "provider detail",
            }),
            "WS_RATE_LIMITED",
        )

    def test_transport_and_malformed_response_are_distinct(self) -> None:
        malformed = json.JSONDecodeError("bad", "{", 0)
        self.assertEqual(
            monitor.canonical_websocket_failure_reason(malformed),
            "WS_MALFORMED_RESPONSE",
        )
        self.assertEqual(
            monitor.canonical_websocket_failure_reason(asyncio.TimeoutError()),
            "WS_TRANSPORT_ERROR",
        )

    def test_failure_metrics_increment_atomically_without_raw_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            original = state_store.GLOBAL_METRICS_PATH
            state_store.GLOBAL_METRICS_PATH = Path(temporary) / "global.json"
            try:
                monitor.record_wallet_ws_failure(
                    "WS_RATE_LIMITED",
                    now_epoch=100.0,
                )
                monitor.record_wallet_ws_failure(
                    "WS_TRANSPORT_ERROR",
                    now_epoch=110.0,
                )
                document = state_store.read_json(
                    state_store.GLOBAL_METRICS_PATH,
                    {},
                )
            finally:
                state_store.GLOBAL_METRICS_PATH = original

        metrics = document["metrics"]
        self.assertEqual(metrics["wallet_ws_reconnect_count"], 2)
        self.assertEqual(metrics["wallet_ws_consecutive_failures"], 2)
        self.assertEqual(metrics["wallet_ws_last_failure_at"], 110.0)
        self.assertEqual(
            metrics["wallet_ws_last_failure_category"],
            "WS_TRANSPORT_ERROR",
        )
        self.assertNotIn("raw_error", metrics)


if __name__ == "__main__":
    unittest.main()
