import asyncio
import unittest
from unittest.mock import patch

from aiohttp.test_utils import TestClient, TestServer

from bot.internal_api import INTERNAL_API_BASE_PATH, INTERNAL_TOKEN_HEADER, build_internal_api_app
from bot.internal_api.app import IDEMPOTENCY_KEY_HEADER, InternalApiServer


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

    async def test_healthz_rejects_non_loopback_host_header(self) -> None:
        app = build_internal_api_app(token="secret-token")
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                response = await client.get(
                    f"{INTERNAL_API_BASE_PATH}/healthz",
                    headers={
                        INTERNAL_TOKEN_HEADER: "secret-token",
                        "Host": "dashboard.example",
                    },
                )
                payload = await response.json()

        self.assertEqual(response.status, 403)
        self.assertEqual(payload.get("error"), "forbidden")

    async def test_healthz_rejects_non_loopback_origin_header(self) -> None:
        app = build_internal_api_app(token="secret-token")
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                response = await client.get(
                    f"{INTERNAL_API_BASE_PATH}/healthz",
                    headers={
                        INTERNAL_TOKEN_HEADER: "secret-token",
                        "Origin": "https://dashboard.example",
                    },
                )
                payload = await response.json()

        self.assertEqual(response.status, 403)
        self.assertEqual(payload.get("error"), "forbidden")

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

    async def test_streamer_add_replays_idempotent_request(self) -> None:
        seen: list[tuple[str, bool]] = []

        async def _add_cb(login: str, require_link: bool) -> str:
            seen.append((login, require_link))
            return "added"

        app = build_internal_api_app(token="secret-token", add_cb=_add_cb)
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                headers = {
                    INTERNAL_TOKEN_HEADER: "secret-token",
                    IDEMPOTENCY_KEY_HEADER: "idem-streamer-add-1",
                }
                first = await client.post(
                    f"{INTERNAL_API_BASE_PATH}/streamers",
                    headers=headers,
                    json={"login": "some_streamer", "require_link": True},
                )
                first_payload = await first.json()

                second = await client.post(
                    f"{INTERNAL_API_BASE_PATH}/streamers",
                    headers=headers,
                    json={"login": "some_streamer", "require_link": True},
                )
                second_payload = await second.json()

        self.assertEqual(first.status, 201)
        self.assertEqual(second.status, 201)
        self.assertEqual(first_payload, second_payload)
        self.assertEqual(second.headers.get("X-Idempotency-Replayed"), "1")
        self.assertEqual(seen, [("some_streamer", True)])

    async def test_streamer_add_rejects_idempotency_key_reuse_with_different_payload(self) -> None:
        seen: list[tuple[str, bool]] = []

        async def _add_cb(login: str, require_link: bool) -> str:
            seen.append((login, require_link))
            return "added"

        app = build_internal_api_app(token="secret-token", add_cb=_add_cb)
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                headers = {
                    INTERNAL_TOKEN_HEADER: "secret-token",
                    IDEMPOTENCY_KEY_HEADER: "idem-streamer-add-2",
                }
                first = await client.post(
                    f"{INTERNAL_API_BASE_PATH}/streamers",
                    headers=headers,
                    json={"login": "alpha_streamer"},
                )
                self.assertEqual(first.status, 201)

                second = await client.post(
                    f"{INTERNAL_API_BASE_PATH}/streamers",
                    headers=headers,
                    json={"login": "beta_streamer"},
                )
                second_payload = await second.json()

        self.assertEqual(second.status, 409)
        self.assertEqual(second_payload.get("error"), "idempotency_conflict")
        self.assertEqual(seen, [("alpha_streamer", False)])

    def test_loopback_host_parser_accepts_ipv6_literals(self) -> None:
        self.assertEqual(InternalApiServer._host_without_port("::1"), "::1")
        self.assertEqual(InternalApiServer._host_without_port("0:0:0:0:0:0:0:1"), "0:0:0:0:0:0:0:1")
        self.assertTrue(InternalApiServer._is_loopback_host("::1"))
        self.assertTrue(InternalApiServer._is_loopback_host("[::1]:8777"))
        self.assertTrue(InternalApiServer._is_loopback_host("0:0:0:0:0:0:0:1"))

    async def test_healthz_allows_ipv6_loopback_host_variants(self) -> None:
        app = build_internal_api_app(token="secret-token")
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                response_1 = await client.get(
                    f"{INTERNAL_API_BASE_PATH}/healthz",
                    headers={
                        INTERNAL_TOKEN_HEADER: "secret-token",
                        "Host": "[::1]:8777",
                    },
                )
                payload_1 = await response_1.json()

                response_2 = await client.get(
                    f"{INTERNAL_API_BASE_PATH}/healthz",
                    headers={
                        INTERNAL_TOKEN_HEADER: "secret-token",
                        "Host": "0:0:0:0:0:0:0:1",
                    },
                )
                payload_2 = await response_2.json()

        self.assertEqual(response_1.status, 200)
        self.assertTrue(payload_1.get("ok"))
        self.assertEqual(response_2.status, 200)
        self.assertTrue(payload_2.get("ok"))

    async def test_streamer_add_concurrent_idempotent_requests_execute_once(self) -> None:
        seen: list[tuple[str, bool]] = []
        entered = asyncio.Event()
        release = asyncio.Event()

        async def _add_cb(login: str, require_link: bool) -> str:
            seen.append((login, require_link))
            entered.set()
            await release.wait()
            return "added"

        app = build_internal_api_app(token="secret-token", add_cb=_add_cb)
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                headers = {
                    INTERNAL_TOKEN_HEADER: "secret-token",
                    IDEMPOTENCY_KEY_HEADER: "idem-concurrent-add-1",
                }
                req_payload = {"login": "parallel_streamer", "require_link": True}

                first_task = asyncio.create_task(
                    client.post(
                        f"{INTERNAL_API_BASE_PATH}/streamers",
                        headers=headers,
                        json=req_payload,
                    )
                )
                await entered.wait()
                second_task = asyncio.create_task(
                    client.post(
                        f"{INTERNAL_API_BASE_PATH}/streamers",
                        headers=headers,
                        json=req_payload,
                    )
                )
                await asyncio.sleep(0.05)
                release.set()

                first = await first_task
                second = await second_task
                first_payload = await first.json()
                second_payload = await second.json()

        self.assertEqual(first.status, 201)
        self.assertEqual(second.status, 201)
        self.assertEqual(first_payload, second_payload)
        self.assertIsNone(first.headers.get("X-Idempotency-Replayed"))
        self.assertEqual(second.headers.get("X-Idempotency-Replayed"), "1")
        self.assertEqual(seen, [("parallel_streamer", True)])

    async def test_raid_auth_url_runtime_error_response_is_sanitized(self) -> None:
        async def _raid_auth_url_cb(login: str) -> str:
            raise RuntimeError(f"database connection failed for {login}: secret=super-secret")

        app = build_internal_api_app(token="secret-token", raid_auth_url_cb=_raid_auth_url_cb)
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                response = await client.get(
                    f"{INTERNAL_API_BASE_PATH}/raid/auth-url?login=some_streamer",
                    headers={INTERNAL_TOKEN_HEADER: "secret-token"},
                )
                payload = await response.json()

        self.assertEqual(response.status, 503)
        self.assertEqual(payload.get("error"), "upstream_unavailable")
        self.assertEqual(payload.get("message"), "upstream unavailable")
        self.assertNotIn("super-secret", payload.get("message", ""))

    async def test_discord_profile_scope_allowlist_rejects_unlisted_ids(self) -> None:
        seen: list[dict[str, str | bool | None]] = []

        async def _discord_profile_cb(
            login: str,
            discord_user_id: str | None = None,
            discord_display_name: str | None = None,
            mark_member: bool = True,
        ) -> str:
            seen.append(
                {
                    "login": login,
                    "discord_user_id": discord_user_id,
                    "discord_display_name": discord_display_name,
                    "mark_member": mark_member,
                }
            )
            return "updated"

        with patch.dict(
            "os.environ",
            {
                "TWITCH_INTERNAL_API_ALLOWED_GUILD_IDS": "111",
                "TWITCH_INTERNAL_API_ALLOWED_CHANNEL_IDS": "222",
                "TWITCH_INTERNAL_API_ALLOWED_ROLE_IDS": "333",
            },
            clear=False,
        ):
            app = build_internal_api_app(token="secret-token", discord_profile_cb=_discord_profile_cb)

        async with TestServer(app) as server:
            async with TestClient(server) as client:
                response = await client.post(
                    f"{INTERNAL_API_BASE_PATH}/streamers/some_streamer/discord-profile",
                    headers={INTERNAL_TOKEN_HEADER: "secret-token"},
                    json={
                        "discord_user_id": "123",
                        "guild_id": 111,
                        "channel_id": 999,
                        "role_id": 333,
                    },
                )
                payload = await response.json()

        self.assertEqual(response.status, 403)
        self.assertEqual(payload.get("error"), "forbidden")
        self.assertEqual(payload.get("message"), "action outside configured scope")
        self.assertEqual(seen, [])

    async def test_discord_profile_scope_allowlist_accepts_listed_ids(self) -> None:
        seen: list[str] = []

        async def _discord_profile_cb(
            login: str,
            discord_user_id: str | None = None,
            discord_display_name: str | None = None,
            mark_member: bool = True,
        ) -> str:
            del discord_user_id, discord_display_name, mark_member
            seen.append(login)
            return "updated"

        with patch.dict(
            "os.environ",
            {
                "TWITCH_INTERNAL_API_ALLOWED_GUILD_IDS": "111",
                "TWITCH_INTERNAL_API_ALLOWED_CHANNEL_IDS": "222",
                "TWITCH_INTERNAL_API_ALLOWED_ROLE_IDS": "333",
            },
            clear=False,
        ):
            app = build_internal_api_app(token="secret-token", discord_profile_cb=_discord_profile_cb)

        async with TestServer(app) as server:
            async with TestClient(server) as client:
                response = await client.post(
                    f"{INTERNAL_API_BASE_PATH}/streamers/some_streamer/discord-profile",
                    headers={INTERNAL_TOKEN_HEADER: "secret-token"},
                    json={
                        "discord_user_id": "123",
                        "guild_id": 111,
                        "channel_id": 222,
                        "role_id": 333,
                    },
                )
                payload = await response.json()

        self.assertEqual(response.status, 200)
        self.assertTrue(payload.get("ok"))
        self.assertEqual(seen, ["some_streamer"])

    async def test_discord_profile_scope_allowlist_not_configured_keeps_existing_behavior(self) -> None:
        seen: list[str] = []

        async def _discord_profile_cb(
            login: str,
            discord_user_id: str | None = None,
            discord_display_name: str | None = None,
            mark_member: bool = True,
        ) -> str:
            del discord_user_id, discord_display_name, mark_member
            seen.append(login)
            return "updated"

        with patch.dict("os.environ", {}, clear=True):
            app = build_internal_api_app(token="secret-token", discord_profile_cb=_discord_profile_cb)

        async with TestServer(app) as server:
            async with TestClient(server) as client:
                response = await client.post(
                    f"{INTERNAL_API_BASE_PATH}/streamers/some_streamer/discord-profile",
                    headers={INTERNAL_TOKEN_HEADER: "secret-token"},
                    json={
                        "discord_user_id": "123",
                        "guild_id": 999001,
                        "channel_id": 999002,
                        "role_id": 999003,
                    },
                )
                payload = await response.json()

        self.assertEqual(response.status, 200)
        self.assertTrue(payload.get("ok"))
        self.assertEqual(seen, ["some_streamer"])

    async def test_discord_profile_scope_allowlist_configured_invalid_values_denies_all(self) -> None:
        seen: list[str] = []

        async def _discord_profile_cb(
            login: str,
            discord_user_id: str | None = None,
            discord_display_name: str | None = None,
            mark_member: bool = True,
        ) -> str:
            del discord_user_id, discord_display_name, mark_member
            seen.append(login)
            return "updated"

        with patch.dict(
            "os.environ",
            {
                "TWITCH_INTERNAL_API_ALLOWED_GUILD_IDS": "abc, -1, ;",
                "TWITCH_INTERNAL_API_ALLOWED_CHANNEL_IDS": "NaN",
                "TWITCH_INTERNAL_API_ALLOWED_ROLE_IDS": "0, none",
            },
            clear=False,
        ):
            with self.assertLogs("TwitchStreams", level="WARNING") as captured:
                app = build_internal_api_app(token="secret-token", discord_profile_cb=_discord_profile_cb)

        async with TestServer(app) as server:
            async with TestClient(server) as client:
                response = await client.post(
                    f"{INTERNAL_API_BASE_PATH}/streamers/some_streamer/discord-profile",
                    headers={INTERNAL_TOKEN_HEADER: "secret-token"},
                    json={
                        "discord_user_id": "123",
                        "guild_id": 111,
                        "channel_id": 222,
                        "role_id": 333,
                    },
                )
                payload = await response.json()

        self.assertEqual(response.status, 403)
        self.assertEqual(payload.get("error"), "forbidden")
        self.assertEqual(payload.get("message"), "action outside configured scope")
        self.assertEqual(seen, [])
        self.assertTrue(
            any(
                "TWITCH_INTERNAL_API_ALLOWED_GUILD_IDS configured but no valid positive IDs parsed"
                in line
                for line in captured.output
            )
        )
        self.assertTrue(
            any(
                "TWITCH_INTERNAL_API_ALLOWED_CHANNEL_IDS configured but no valid positive IDs parsed"
                in line
                for line in captured.output
            )
        )
        self.assertTrue(
            any(
                "TWITCH_INTERNAL_API_ALLOWED_ROLE_IDS configured but no valid positive IDs parsed"
                in line
                for line in captured.output
            )
        )


if __name__ == "__main__":
    unittest.main()
