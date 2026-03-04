from __future__ import annotations

import json
import unittest

from aiohttp import web

from bot.dashboard.live_announcement_mixin import DashboardLiveAnnouncementMixin


class _FakeRequest:
    def __init__(
        self,
        *,
        query: dict | None = None,
        headers: dict | None = None,
        body: dict | None = None,
    ) -> None:
        self.query = query or {}
        self.headers = headers or {}
        self._body = body or {}

    async def json(self):
        return self._body


class _DummyLiveAnnouncementApi(DashboardLiveAnnouncementMixin):
    def __init__(self) -> None:
        self.auth_level = "partner"
        self.session = {"twitch_login": "earlysalty"}
        self.records: dict[str, dict] = {}
        self.allow_auth = True
        self.valid_csrf = "csrf-ok"

    def _require_token(self, _request):
        if not self.allow_auth:
            raise web.HTTPUnauthorized(text="unauthorized")
        return None

    def _get_auth_level(self, _request):
        return self.auth_level

    def _get_dashboard_auth_session(self, _request):
        return self.session

    def _get_discord_admin_session(self, _request):
        return None

    @staticmethod
    def _normalize_login(value: str) -> str:
        return (value or "").strip().lower()

    def _la_ensure_storage(self) -> None:
        return None

    def _la_list_streamers(self, *, session_login: str, is_admin: bool) -> list[str]:
        if not is_admin:
            return [session_login]
        return ["earlysalty", "otherstreamer"]

    def _la_load(self, streamer_login: str) -> dict:
        stored = self.records.get(streamer_login)
        if stored:
            return stored
        return {"config": {}, "allowed_editor_role_ids": []}

    def _la_save(
        self,
        streamer_login: str,
        cfg: dict,
        allowed_editor_role_ids: list[int],
        actor: str,
    ) -> None:
        self.records[streamer_login] = {
            "config": cfg,
            "allowed_editor_role_ids": allowed_editor_role_ids,
            "actor": actor,
        }

    def _la_csrf_generate(self, _request):
        return self.valid_csrf

    def _la_csrf_verify(self, _request, token: str) -> bool:
        return token == self.valid_csrf

    async def _la_send_test_dm(self, user_id: int, streamer_login: str, preview_payload: dict):
        del user_id, streamer_login, preview_payload
        return True, "sent"

    async def _la_member_role_ids(self, user_id: int) -> set[int]:
        del user_id
        return {111}

    def _la_dm_target_user_id(self, request, *, streamer_login: str):
        del request, streamer_login
        return 12345


class DashboardLiveAnnouncementApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_config_save_and_load_roundtrip(self) -> None:
        handler = _DummyLiveAnnouncementApi()

        cfg = {
            "content": "{mention_role} test content",
            "button": {"label": "EarlySalty auf Twitch zuschauen"},
        }

        save_request = _FakeRequest(
            query={"streamer": "earlysalty"},
            headers={"X-CSRF-Token": "csrf-ok"},
            body={
                "csrf_token": "csrf-ok",
                "streamer_login": "earlysalty",
                "config": cfg,
                "allowed_editor_role_ids": [111, "222"],
            },
        )
        save_response = await handler.api_live_announcement_save_config(save_request)
        self.assertEqual(save_response.status, 200)
        save_payload = json.loads(save_response.text)
        self.assertTrue(save_payload.get("ok"))

        load_request = _FakeRequest(query={"streamer": "earlysalty"})
        load_response = await handler.api_live_announcement_config(load_request)
        self.assertEqual(load_response.status, 200)
        load_payload = json.loads(load_response.text)
        self.assertEqual(load_payload["config"]["button"]["label"], "EarlySalty auf Twitch zuschauen")
        self.assertEqual(load_payload["allowed_editor_role_ids"], [111, 222])

    async def test_auth_rejection(self) -> None:
        handler = _DummyLiveAnnouncementApi()
        handler.allow_auth = False
        request = _FakeRequest(query={"streamer": "earlysalty"})
        with self.assertRaises(web.HTTPUnauthorized):
            await handler.api_live_announcement_config(request)


if __name__ == "__main__":
    unittest.main()
