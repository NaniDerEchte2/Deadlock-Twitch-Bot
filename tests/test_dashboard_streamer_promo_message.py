from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import patch

from aiohttp import web

from bot.dashboard.routes_mixin import _DashboardRoutesMixin


class _ConnCtx:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def __enter__(self) -> sqlite3.Connection:
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeRequest:
    def __init__(self, body: dict[str, str]) -> None:
        self._body = body

    async def post(self):
        return self._body


class _DummyStreamerPromoHandler(_DashboardRoutesMixin):
    def _check_v2_auth(self, request):
        del request
        return True

    def _get_dashboard_auth_session(self, request):
        del request
        return {"twitch_login": "partner_one"}


class DashboardStreamerPromoMessageTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            """
            CREATE TABLE streamer_plans (
                twitch_login TEXT PRIMARY KEY,
                promo_message TEXT
            )
            """
        )
        self.conn.execute(
            "INSERT INTO streamer_plans (twitch_login, promo_message) VALUES (?, ?)",
            ("partner_one", None),
        )
        self.conn.commit()
        self.handler = _DummyStreamerPromoHandler()

    def tearDown(self) -> None:
        self.conn.close()

    def _conn_patch(self):
        return patch(
            "bot.dashboard.routes_mixin.storage.get_conn",
            return_value=_ConnCtx(self.conn),
        )

    async def test_save_accepts_valid_multiline_message_with_invite(self) -> None:
        request = _FakeRequest(
            {"promo_message": "Zeile eins\nZeile zwei {invite}"}
        )

        with self._conn_patch():
            with self.assertRaises(web.HTTPFound) as ctx:
                await self.handler.abbo_promo_message(request)

        saved = self.conn.execute(
            "SELECT promo_message FROM streamer_plans WHERE twitch_login = ?",
            ("partner_one",),
        ).fetchone()
        self.assertEqual(ctx.exception.location, "/twitch/abbo?promo_saved=1")
        self.assertEqual(saved["promo_message"], "Zeile eins\nZeile zwei {invite}")

    async def test_save_rejects_message_without_invite(self) -> None:
        request = _FakeRequest({"promo_message": "Nur Text ohne Invite"})

        with self._conn_patch():
            with self.assertRaises(web.HTTPFound) as ctx:
                await self.handler.abbo_promo_message(request)

        self.assertEqual(ctx.exception.location, "/twitch/abbo?promo_error=missing_invite")

    async def test_save_rejects_message_over_500_characters(self) -> None:
        request = _FakeRequest({"promo_message": ("x" * 493) + "{invite}"})

        with self._conn_patch():
            with self.assertRaises(web.HTTPFound) as ctx:
                await self.handler.abbo_promo_message(request)

        self.assertEqual(ctx.exception.location, "/twitch/abbo?promo_error=too_long")


if __name__ == "__main__":
    unittest.main()
