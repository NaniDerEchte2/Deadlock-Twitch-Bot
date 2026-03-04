import unittest
from types import SimpleNamespace

from bot.dashboard.mixin import (
    RAID_OAUTH_SUCCESS_REDIRECT_URL,
    TwitchDashboardMixin,
)


class _FakeUsersResponse:
    def __init__(self, payload: dict):
        self.status = 200
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self) -> str:
        return ""

    async def json(self) -> dict:
        return self._payload


class _FakeSession:
    def __init__(self, payload: dict):
        self._payload = payload

    def get(self, *_args, **_kwargs):
        return _FakeUsersResponse(self._payload)


class _FakeAuthManager:
    client_id = "client-id"
    redirect_uri = "https://raid.earlysalty.com/twitch/raid/callback"

    def verify_state(self, _state: str) -> str:
        return "discord:123456789"

    async def exchange_code_for_token(self, _code: str, _session) -> dict:
        return {
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "expires_in": 3600,
            "scope": [],
        }

    def save_auth(self, **_kwargs) -> None:
        return None


class _DummyDashboardMixin(TwitchDashboardMixin):
    def __init__(self) -> None:
        self._raid_bot = SimpleNamespace(
            auth_manager=_FakeAuthManager(),
            session=_FakeSession(payload={"data": [{"id": "1001", "login": "partner_one"}]}),
        )


class DashboardMixinRaidCallbackRedirectTests(unittest.IsolatedAsyncioTestCase):
    async def test_dashboard_callback_payload_contains_redirect_url(self) -> None:
        handler = _DummyDashboardMixin()

        payload = await handler._dashboard_raid_oauth_callback(
            code="oauth-code",
            state="valid-state",
            error="",
        )

        self.assertEqual(payload.get("status"), 200)
        self.assertEqual(payload.get("redirect_url"), RAID_OAUTH_SUCCESS_REDIRECT_URL)


if __name__ == "__main__":
    unittest.main()
