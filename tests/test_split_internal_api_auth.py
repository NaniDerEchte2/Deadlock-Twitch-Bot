import unittest

from aiohttp.test_utils import TestClient, TestServer

from bot.internal_api import INTERNAL_API_BASE_PATH, INTERNAL_TOKEN_HEADER, build_internal_api_app


class InternalApiAuthTests(unittest.IsolatedAsyncioTestCase):
    async def test_healthz_rejects_missing_token(self) -> None:
        app = build_internal_api_app(token="secret-token")
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                response = await client.get(f"{INTERNAL_API_BASE_PATH}/healthz")
                payload = await response.json()

        self.assertEqual(response.status, 401)
        self.assertEqual(payload.get("error"), "unauthorized")

    async def test_healthz_rejects_invalid_token(self) -> None:
        app = build_internal_api_app(token="secret-token")
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                response = await client.get(
                    f"{INTERNAL_API_BASE_PATH}/healthz",
                    headers={INTERNAL_TOKEN_HEADER: "wrong-token"},
                )
                payload = await response.json()

        self.assertEqual(response.status, 401)
        self.assertEqual(payload.get("error"), "unauthorized")

    async def test_healthz_allows_valid_token(self) -> None:
        app = build_internal_api_app(token="secret-token")
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                response = await client.get(
                    f"{INTERNAL_API_BASE_PATH}/healthz",
                    headers={INTERNAL_TOKEN_HEADER: "secret-token"},
                )
                payload = await response.json()

        self.assertEqual(response.status, 200)
        self.assertTrue(payload.get("ok"))

    async def test_healthz_fails_closed_when_server_token_is_missing(self) -> None:
        app = build_internal_api_app(token=None)
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                response = await client.get(
                    f"{INTERNAL_API_BASE_PATH}/healthz",
                    headers={INTERNAL_TOKEN_HEADER: "any-token"},
                )
                payload = await response.json()

        self.assertEqual(response.status, 401)
        self.assertEqual(payload.get("error"), "unauthorized")

    async def test_streamer_add_invalid_json_uses_stable_safe_error_message(self) -> None:
        app = build_internal_api_app(token="secret-token")
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                response = await client.post(
                    f"{INTERNAL_API_BASE_PATH}/streamers",
                    data='{"login"',
                    headers={
                        INTERNAL_TOKEN_HEADER: "secret-token",
                        "Content-Type": "application/json",
                    },
                )
                payload = await response.json()

        self.assertEqual(response.status, 400)
        self.assertEqual(payload.get("error"), "bad_request")
        self.assertEqual(payload.get("message"), "invalid request body")

    async def test_stats_invalid_query_uses_stable_safe_error_message(self) -> None:
        app = build_internal_api_app(token="secret-token")
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                response = await client.get(
                    f"{INTERNAL_API_BASE_PATH}/stats?hour_from=abc",
                    headers={INTERNAL_TOKEN_HEADER: "secret-token"},
                )
                payload = await response.json()

        self.assertEqual(response.status, 400)
        self.assertEqual(payload.get("error"), "bad_request")
        self.assertEqual(payload.get("message"), "invalid query parameters")

    async def test_raid_auth_url_allows_discord_state_target(self) -> None:
        seen_logins: list[str] = []

        async def _raid_auth_url_cb(login: str) -> str:
            seen_logins.append(login)
            return f"https://auth.example/{login}"

        app = build_internal_api_app(token="secret-token", raid_auth_url_cb=_raid_auth_url_cb)
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                response = await client.get(
                    f"{INTERNAL_API_BASE_PATH}/raid/auth-url?login=discord:123456789",
                    headers={INTERNAL_TOKEN_HEADER: "secret-token"},
                )
                payload = await response.json()

        self.assertEqual(response.status, 200)
        self.assertEqual(payload.get("login"), "discord:123456789")
        self.assertEqual(payload.get("auth_url"), "https://auth.example/discord:123456789")
        self.assertEqual(seen_logins, ["discord:123456789"])

    async def test_raid_auth_url_rejects_invalid_discord_state_target(self) -> None:
        async def _raid_auth_url_cb(login: str) -> str:
            return f"https://auth.example/{login}"

        app = build_internal_api_app(token="secret-token", raid_auth_url_cb=_raid_auth_url_cb)
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                response = await client.get(
                    f"{INTERNAL_API_BASE_PATH}/raid/auth-url?login=discord:not-a-number",
                    headers={INTERNAL_TOKEN_HEADER: "secret-token"},
                )
                payload = await response.json()

        self.assertEqual(response.status, 400)
        self.assertEqual(payload.get("error"), "bad_request")
        self.assertEqual(payload.get("message"), "invalid or missing login")


if __name__ == "__main__":
    unittest.main()
