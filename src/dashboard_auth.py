"""대시보드 요청 쿠키 인증 판정."""

from __future__ import annotations

import hmac
from collections.abc import Mapping


def request_is_authenticated(
    cookies: Mapping[str, str],
    cookie_name: str,
    expected_token: str,
    *,
    session_authenticated: bool,
) -> bool:
    """현재 HTTP 요청 쿠키 또는 검증된 세션 상태로 인증한다."""
    cookie_token = str(cookies.get(cookie_name, "") or "")
    return session_authenticated or hmac.compare_digest(cookie_token, expected_token)
