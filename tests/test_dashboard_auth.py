import unittest

from src.dashboard_auth import request_is_authenticated


class DashboardAuthTests(unittest.TestCase):
    def test_matching_request_cookie_authenticates_without_component_rerun(self):
        authenticated = request_is_authenticated(
            {"solana_ai_bot_auth": "expected"},
            "solana_ai_bot_auth",
            "expected",
            session_authenticated=False,
        )

        self.assertTrue(authenticated)

    def test_missing_or_invalid_request_cookie_fails_closed(self):
        self.assertFalse(
            request_is_authenticated(
                {},
                "solana_ai_bot_auth",
                "expected",
                session_authenticated=False,
            )
        )
        self.assertFalse(
            request_is_authenticated(
                {"solana_ai_bot_auth": "wrong"},
                "solana_ai_bot_auth",
                "expected",
                session_authenticated=False,
            )
        )

    def test_verified_streamlit_session_remains_authenticated(self):
        self.assertTrue(
            request_is_authenticated(
                {},
                "solana_ai_bot_auth",
                "expected",
                session_authenticated=True,
            )
        )


if __name__ == "__main__":
    unittest.main()
