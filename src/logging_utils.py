"""로그에 포함될 수 있는 URL 자격 증명을 공통으로 마스킹한다."""

from __future__ import annotations

import logging
import re
from typing import Any


_API_KEY_QUERY = re.compile(
    r"(?i)([?&])(?:api[-_]?key|apikey)=([^&\s'\"<>]+)"
)
_DEFAULT_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def redact_sensitive_text(value: Any) -> str:
    """HTTP/WSS URL query의 API 키를 안전한 고정 문자열로 치환한다."""
    text = str(value)
    return _API_KEY_QUERY.sub(r"\1apiKey=HIDDEN_MASKED", text)


class RedactingFormatter(logging.Formatter):
    """traceback을 포함한 최종 로그 문자열 전체를 마스킹한다."""

    def format(self, record: logging.LogRecord) -> str:
        return redact_sensitive_text(super().format(record))


def _redacting_formatter_from(
    formatter: logging.Formatter | None,
) -> RedactingFormatter:
    if isinstance(formatter, RedactingFormatter):
        return formatter
    if formatter is None:
        return RedactingFormatter(_DEFAULT_FORMAT)
    style_name = formatter._style.__class__.__name__
    style = {
        "StrFormatStyle": "{",
        "StringTemplateStyle": "$",
    }.get(style_name, "%")
    return RedactingFormatter(
        getattr(formatter._style, "_fmt", _DEFAULT_FORMAT),
        datefmt=formatter.datefmt,
        style=style,
    )


def install_redacting_formatters() -> None:
    """이미 설치된 root handler에도 멱등으로 보안 formatter를 적용한다."""
    root = logging.getLogger()
    for handler in root.handlers:
        if not isinstance(handler.formatter, RedactingFormatter):
            handler.setFormatter(_redacting_formatter_from(handler.formatter))


def configure_safe_logging(level: int = logging.INFO) -> None:
    """독립 프로세스용 공통 보안 로그 설정을 설치한다."""
    handler = logging.StreamHandler()
    handler.setFormatter(RedactingFormatter(_DEFAULT_FORMAT))
    logging.basicConfig(level=level, handlers=[handler], force=True)
