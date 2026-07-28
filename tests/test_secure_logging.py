from __future__ import annotations

import io
import logging
import unittest

from src.logging_utils import (
    RedactingFormatter,
    install_redacting_formatters,
    redact_sensitive_text,
)


class SecureLoggingTests(unittest.TestCase):
    def test_redacts_http_and_websocket_api_key_queries(self) -> None:
        samples = [
            "https://rpc.example/?api-key=secret",
            "wss://rpc.example/?api_key=secret&x=1",
            "https://rpc.example/path?apiKey=secret-value",
        ]
        for sample in samples:
            with self.subTest(sample=sample):
                redacted = redact_sensitive_text(sample)
                self.assertNotIn("secret", redacted)
                self.assertIn("apiKey=HIDDEN_MASKED", redacted)
        self.assertIn(
            "&x=1",
            redact_sensitive_text(samples[1]),
        )

    def test_formatter_redacts_exception_traceback(self) -> None:
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(
            RedactingFormatter(
                "%(levelname)s %(name)s: %(message)s"
            )
        )
        logger = logging.getLogger("secure-logging-test")
        logger.handlers = [handler]
        logger.propagate = False
        logger.setLevel(logging.INFO)
        try:
            raise RuntimeError(
                "request failed: "
                "wss://rpc.example/?api-key=traceback-secret"
            )
        except RuntimeError:
            logger.exception("connection failed")
        output = stream.getvalue()
        self.assertIn("Traceback", output)
        self.assertIn("apiKey=HIDDEN_MASKED", output)
        self.assertNotIn("traceback-secret", output)

    def test_installer_preserves_brace_style_and_is_idempotent(self) -> None:
        root = logging.getLogger()
        original_handlers = root.handlers[:]
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter("{levelname}: {message}", style="{"))
        root.handlers = [handler]
        try:
            install_redacting_formatters()
            first_formatter = handler.formatter
            install_redacting_formatters()
            self.assertIs(handler.formatter, first_formatter)
            record = logging.LogRecord(
                "secure",
                logging.ERROR,
                __file__,
                1,
                "https://rpc.example/?api-key=handler-secret",
                (),
                None,
            )
            handler.emit(record)
        finally:
            root.handlers = original_handlers
        self.assertEqual(
            stream.getvalue().strip(),
            "ERROR: https://rpc.example/?apiKey=HIDDEN_MASKED",
        )


if __name__ == "__main__":
    unittest.main()
