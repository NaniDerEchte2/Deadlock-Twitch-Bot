import unittest

from bot.api.twitch_api import TwitchAPI
from bot.api.twitch_auth import TwitchClientConfigError
from bot.raid.auth import RaidAuthManager


class _FakeResponse:
    def __init__(self, *, status: int, text: str = "", payload: dict | None = None) -> None:
        self.status = int(status)
        self._text = text
        self._payload = dict(payload or {})
        self.history = ()
        self.headers = {}
        self.reason = "Bad Request"
        self.request_info = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self) -> str:
        return self._text

    async def json(self) -> dict:
        return dict(self._payload)

    def raise_for_status(self) -> None:
        raise RuntimeError(f"unexpected raise_for_status for HTTP {self.status}")


class _RecordingSession:
    def __init__(self, responses: list[_FakeResponse] | None = None) -> None:
        self._responses = list(responses or [])
        self.closed = False
        self.calls: list[dict[str, object]] = []

    def post(self, url, data=None):
        self.calls.append({"url": url, "data": dict(data or {})})
        if not self._responses:
            raise AssertionError("No fake response configured")
        return self._responses.pop(0)


class TwitchApiAuthGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_blank_credentials_are_rejected_before_http_call(self) -> None:
        session = _RecordingSession()
        api = TwitchAPI("   ", "   ", session=session)

        with self.assertRaises(TwitchClientConfigError) as ctx:
            await api._ensure_token()

        self.assertIn("missing or blank", str(ctx.exception).lower())
        self.assertEqual(session.calls, [])
        self.assertTrue(api.is_auth_blocked())

    async def test_invalid_client_response_blocks_follow_up_requests(self) -> None:
        session = _RecordingSession(
            responses=[
                _FakeResponse(
                    status=400,
                    text='{"status":400,"message":"invalid client"}',
                )
            ]
        )
        api = TwitchAPI("client-id", "bad-secret", session=session)

        with self.assertRaises(TwitchClientConfigError):
            await api._ensure_token()

        with self.assertRaises(TwitchClientConfigError):
            await api._ensure_token()

        self.assertEqual(len(session.calls), 1)
        self.assertTrue(api.is_auth_blocked())


class RaidAuthManagerAuthGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_client_refresh_blocks_follow_up_requests(self) -> None:
        session = _RecordingSession(
            responses=[
                _FakeResponse(
                    status=400,
                    text='{"status":400,"message":"invalid client"}',
                )
            ]
        )
        manager = RaidAuthManager(
            client_id="client-id",
            client_secret="bad-secret",
            redirect_uri="https://raid.example.com/twitch/raid/callback",
        )

        with self.assertRaises(TwitchClientConfigError):
            await manager.refresh_token(
                "refresh-token",
                session,
                twitch_user_id="1001",
                twitch_login="partner_one",
            )

        with self.assertRaises(TwitchClientConfigError):
            await manager.refresh_token(
                "refresh-token",
                session,
                twitch_user_id="1001",
                twitch_login="partner_one",
            )

        self.assertEqual(len(session.calls), 1)
        self.assertTrue(manager.is_client_auth_blocked())
