import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from aiohttp import web

from bot.dashboard.auth_mixin import _DashboardAuthMixin


def _make_request(
    *,
    query: dict | None = None,
    cookies: dict | None = None,
    path_qs: str = "/twitch/dashboard",
    secure: bool = True,
) -> SimpleNamespace:
    return SimpleNamespace(
        query=query or {},
        cookies=cookies or {},
        rel_url=SimpleNamespace(path_qs=path_qs),
        headers={},
        secure=secure,
        remote="127.0.0.1",
        transport=None,
    )


class _AuthHarness(_DashboardAuthMixin):
    def __init__(self) -> None:
        self._oauth_client_id = "client-id"
        self._oauth_client_secret = "client-secret"
        self._oauth_redirect_uri = "https://dashboard.example/twitch/auth/callback"
        self._session_cookie_name = "twitch_dash_session"
        self._oauth_states: dict[str, dict] = {}
        self._auth_sessions: dict[str, dict] = {}
        self._oauth_state_ttl_seconds = 600
        self._session_ttl_seconds = 6 * 3600
        self._sessions_db_loaded = True
        self._rate_limits: dict[str, list[float]] = {}
        self.exchange_calls: list[tuple[str, str]] = []
        self.created_sessions: list[dict[str, str]] = []

    def _check_v2_auth(self, request) -> bool:
        del request
        return False

    def _check_rate_limit(
        self, request, *, max_requests: int = 10, window_seconds: float = 60.0
    ) -> bool:
        del request, max_requests, window_seconds
        return True

    def _is_secure_request(self, request) -> bool:
        return bool(getattr(request, "secure", False))

    def _sanitize_log_value(self, value):
        return str(value or "")

    def _peer_host(self, request):
        return str(getattr(request, "remote", "") or "")

    async def _exchange_code_for_user(self, code: str, redirect_uri: str):
        self.exchange_calls.append((code, redirect_uri))
        return {
            "twitch_login": "partner_one",
            "twitch_user_id": "1001",
            "display_name": "Partner One",
        }

    def _is_partner_allowed(self, *, twitch_login: str, twitch_user_id: str):
        del twitch_login, twitch_user_id
        return {"twitch_login": "partner_one", "twitch_user_id": "1001"}

    def _create_dashboard_session(
        self, *, twitch_login: str, twitch_user_id: str, display_name: str
    ) -> str:
        self.created_sessions.append(
            {
                "twitch_login": twitch_login,
                "twitch_user_id": twitch_user_id,
                "display_name": display_name,
            }
        )
        return "session-123"


class DashboardOAuthStateBindingTests(unittest.IsolatedAsyncioTestCase):
    async def test_auth_login_sets_context_cookie_and_binds_state(self) -> None:
        handler = _AuthHarness()
        request = _make_request(query={"next": "/twitch/dashboard"})
        context_token = "ctx_token_abcdefghijklmnop"
        state = "state_token_abcdefghijklmnop"

        with patch(
            "bot.dashboard.auth_mixin.secrets.token_urlsafe",
            side_effect=[context_token, state],
        ):
            with self.assertRaises(web.HTTPFound) as ctx:
                await handler.auth_login(request)

        self.assertIn(f"state={state}", ctx.exception.location)
        self.assertIn(state, handler._oauth_states)
        self.assertEqual(handler._oauth_states[state].get("context_token"), context_token)
        cookie = ctx.exception.cookies.get(handler._oauth_context_cookie_name())
        self.assertIsNotNone(cookie)
        self.assertEqual(cookie.value, context_token)

    async def test_auth_callback_rejects_missing_context_cookie_and_consumes_state(self) -> None:
        handler = _AuthHarness()
        state = "state_token_missing_cookie_123"
        handler._oauth_states[state] = {
            "created_at": time.time(),
            "next_path": "/twitch/dashboard",
            "redirect_uri": "https://dashboard.example/twitch/auth/callback",
            "context_token": "ctx_token_abcdefghijklmnop",
        }
        request = _make_request(query={"state": state, "code": "oauth-code"}, cookies={})

        response = await handler.auth_callback(request)

        self.assertEqual(response.status, 400)
        self.assertIn("OAuth state ungültig oder abgelaufen.", response.text)
        self.assertNotIn(state, handler._oauth_states)
        self.assertEqual(handler.exchange_calls, [])

    async def test_auth_callback_requires_bound_cookie_and_state_is_one_time(self) -> None:
        handler = _AuthHarness()
        context_token = "ctx_token_abcdefghijklmnop"
        state = "state_token_abcdefghijklmnop"
        handler._oauth_states[state] = {
            "created_at": time.time(),
            "next_path": "/twitch/dashboard",
            "redirect_uri": "https://dashboard.example/twitch/auth/callback",
            "context_token": context_token,
        }
        request = _make_request(
            query={"state": state, "code": "oauth-code"},
            cookies={handler._oauth_context_cookie_name(): context_token},
        )

        with self.assertRaises(web.HTTPFound) as ctx:
            await handler.auth_callback(request)

        self.assertEqual(ctx.exception.location, "/twitch/dashboard")
        self.assertEqual(
            handler.exchange_calls,
            [("oauth-code", "https://dashboard.example/twitch/auth/callback")],
        )
        self.assertEqual(len(handler.created_sessions), 1)
        session_cookie = ctx.exception.cookies.get(handler._session_cookie_name)
        self.assertIsNotNone(session_cookie)
        self.assertEqual(session_cookie.value, "session-123")
        oauth_cookie = ctx.exception.cookies.get(handler._oauth_context_cookie_name())
        self.assertIsNotNone(oauth_cookie)
        self.assertEqual(oauth_cookie.value, "")
        self.assertEqual(oauth_cookie["max-age"], "0")

        replay = await handler.auth_callback(request)
        self.assertEqual(replay.status, 400)
        self.assertIn("OAuth state ungültig oder abgelaufen.", replay.text)


if __name__ == "__main__":
    unittest.main()
