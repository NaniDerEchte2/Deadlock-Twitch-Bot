import asyncio
import unittest
from unittest.mock import patch

from bot.dashboard_service.app import build_dashboard_service_app
from bot.dashboard_service.client import BotApiClient, BotApiClientError
from bot.internal_api import INTERNAL_API_BASE_PATH


class _FakeResponse:
    def __init__(self, *, status: int, text: str) -> None:
        self.status = int(status)
        self._text = text
        self.released = False

    async def text(self) -> str:
        return self._text

    def release(self) -> None:
        self.released = True


class _FakeSession:
    def __init__(self, *, response: _FakeResponse | None = None, exc: Exception | None = None) -> None:
        self._response = response
        self._exc = exc
        self.closed = False
        self.calls: list[dict] = []

    async def request(self, method: str, url: str, **kwargs):
        self.calls.append({"method": method, "url": url, "kwargs": kwargs})
        if self._exc is not None:
            raise self._exc
        return self._response

    async def close(self) -> None:
        self.closed = True


class BotApiClientErrorMappingTests(unittest.IsolatedAsyncioTestCase):
    async def test_maps_upstream_auth_failures_to_safe_bad_gateway(self) -> None:
        session = _FakeSession(
            response=_FakeResponse(status=401, text='{"error":"missing token"}')
        )
        client = BotApiClient(
            base_url="http://127.0.0.1:8766",
            token="secret",
            session=session,
        )

        with self.assertRaises(BotApiClientError) as ctx:
            await client.get_streamers()

        self.assertEqual(ctx.exception.status, 502)
        self.assertEqual(ctx.exception.code, "upstream_auth_failed")
        self.assertIn("authenticate", ctx.exception.message.lower())

    async def test_maps_timeout_to_gateway_timeout(self) -> None:
        session = _FakeSession(exc=asyncio.TimeoutError())
        client = BotApiClient(
            base_url="http://127.0.0.1:8766",
            token="secret",
            session=session,
        )

        with self.assertRaises(BotApiClientError) as ctx:
            await client.get_streamers()

        self.assertEqual(ctx.exception.status, 504)
        self.assertEqual(ctx.exception.code, "upstream_timeout")

    async def test_invalid_json_success_response_maps_to_safe_error(self) -> None:
        response = _FakeResponse(status=200, text="<html>not json</html>")
        session = _FakeSession(response=response)
        client = BotApiClient(
            base_url="http://127.0.0.1:8766",
            token="secret",
            session=session,
        )

        with self.assertRaises(BotApiClientError) as ctx:
            await client.healthz()

        self.assertEqual(ctx.exception.status, 502)
        self.assertEqual(ctx.exception.code, "upstream_invalid_json")
        self.assertTrue(response.released)

    async def test_bad_request_message_is_sanitized(self) -> None:
        session = _FakeSession(
            response=_FakeResponse(status=400, text='{"message":"bad\\ninput"}')
        )
        client = BotApiClient(
            base_url="http://127.0.0.1:8766",
            token="secret",
            session=session,
        )

        with self.assertRaises(BotApiClientError) as ctx:
            await client.add_streamer("bad_login", require_link=False)

        self.assertEqual(ctx.exception.status, 400)
        self.assertEqual(ctx.exception.code, "bad_request")
        self.assertNotIn("\n", ctx.exception.message)

    async def test_base_url_with_internal_prefix_is_not_duplicated(self) -> None:
        session = _FakeSession(
            response=_FakeResponse(status=200, text="[]")
        )
        client = BotApiClient(
            base_url=f"http://127.0.0.1:8766{INTERNAL_API_BASE_PATH}",
            token="secret",
            session=session,
        )

        payload = await client.get_streamers()

        self.assertEqual(payload, [])
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(
            session.calls[0]["url"],
            "http://127.0.0.1:8766/internal/twitch/v1/streamers",
        )

    def test_rejects_non_loopback_base_url_by_default(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            BotApiClient(
                base_url="http://10.0.0.20:8766",
                token="secret",
            )

        self.assertIn("loopback", str(ctx.exception).lower())

    def test_allows_non_loopback_base_url_when_override_enabled(self) -> None:
        client = BotApiClient(
            base_url="http://10.0.0.20:8766",
            token="secret",
            allow_non_loopback=True,
        )

        self.assertEqual(client._base_url, "http://10.0.0.20:8766")

    def test_dashboard_service_fails_fast_when_noauth_without_allow_override(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "TWITCH_DASHBOARD_NOAUTH": "1",
                "TWITCH_ALLOW_DASHBOARD_NOAUTH": "0",
                "TWITCH_INTERNAL_API_TOKEN": "secret",
                "TWITCH_INTERNAL_API_BASE_URL": "http://127.0.0.1:8766",
            },
            clear=False,
        ):
            with self.assertRaises(RuntimeError) as ctx:
                build_dashboard_service_app()
        self.assertIn("TWITCH_ALLOW_DASHBOARD_NOAUTH=1", str(ctx.exception))

    def test_dashboard_service_allows_noauth_only_with_explicit_override(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "TWITCH_DASHBOARD_NOAUTH": "1",
                "TWITCH_ALLOW_DASHBOARD_NOAUTH": "1",
                "TWITCH_INTERNAL_API_TOKEN": "secret",
                "TWITCH_INTERNAL_API_BASE_URL": "http://127.0.0.1:8766",
            },
            clear=False,
        ):
            app = build_dashboard_service_app()
        self.assertIsNotNone(app)


if __name__ == "__main__":
    unittest.main()
