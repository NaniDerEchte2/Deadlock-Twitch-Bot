from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import patch

from bot.chat.promos import PromoMixin
from bot.promo_mode import ensure_global_promo_mode_storage, save_global_promo_mode


class _ConnCtx:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def __enter__(self) -> sqlite3.Connection:
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _DummyPromoChat(PromoMixin):
    def __init__(self) -> None:
        self.announcement_calls: list[dict[str, str]] = []
        self._last_promo_sent: dict[str, float] = {}

    async def _get_promo_invite(self, login: str):
        del login
        return "https://discord.gg/example", False

    async def _send_announcement(self, channel, text: str, color: str = "purple", source: str = ""):
        self.announcement_calls.append(
            {
                "login": str(getattr(channel, "name", "") or ""),
                "channel_id": str(getattr(channel, "id", "") or ""),
                "text": text,
                "color": color,
                "source": source,
            }
        )
        return True


class ChatPromoOverrideTests(unittest.IsolatedAsyncioTestCase):
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
        ensure_global_promo_mode_storage(self.conn)
        self.handler = _DummyPromoChat()

    def tearDown(self) -> None:
        self.conn.close()

    def _conn_patch(self):
        return patch("bot.chat.promos.get_conn", return_value=_ConnCtx(self.conn))

    async def test_active_global_event_overrides_streamer_message(self) -> None:
        self.conn.execute(
            "INSERT INTO streamer_plans (twitch_login, promo_message) VALUES (?, ?)",
            ("partner_one", "Streamer Override {invite}"),
        )
        save_global_promo_mode(
            self.conn,
            config={
                "mode": "custom_event",
                "custom_message": "Global Event {invite}",
                "is_enabled": True,
            },
            updated_by="discord:55",
        )

        with self._conn_patch(), patch(
            "bot.chat.promos.PROMO_MESSAGES",
            ["Default Fallback {invite}"],
        ):
            ok = await self.handler._send_promo_message(
                "partner_one",
                "1001",
                0.0,
                reason="chat_activity",
            )

        self.assertTrue(ok)
        self.assertEqual(self.handler.announcement_calls[0]["text"], "Global Event https://discord.gg/example")

    async def test_active_global_event_without_invite_uses_fixed_text(self) -> None:
        self.conn.execute(
            "INSERT INTO streamer_plans (twitch_login, promo_message) VALUES (?, ?)",
            ("partner_one", "Streamer Override {invite}"),
        )
        save_global_promo_mode(
            self.conn,
            config={
                "mode": "custom_event",
                "custom_message": "Global Event ohne Invite",
                "is_enabled": True,
            },
            updated_by="discord:55",
        )

        with self._conn_patch(), patch(
            "bot.chat.promos.PROMO_MESSAGES",
            ["Default Fallback {invite}"],
        ):
            ok = await self.handler._send_promo_message(
                "partner_one",
                "1001",
                0.0,
                reason="chat_activity",
            )

        self.assertTrue(ok)
        self.assertEqual(self.handler.announcement_calls[0]["text"], "Global Event ohne Invite")

    async def test_inactive_global_event_keeps_streamer_override(self) -> None:
        self.conn.execute(
            "INSERT INTO streamer_plans (twitch_login, promo_message) VALUES (?, ?)",
            ("partner_one", "Streamer Override {invite}"),
        )
        save_global_promo_mode(
            self.conn,
            config={
                "mode": "custom_event",
                "custom_message": "Expired Event {invite}",
                "is_enabled": True,
                "ends_at": "2020-03-06T20:00:00+00:00",
            },
            updated_by="discord:55",
        )

        with self._conn_patch(), patch(
            "bot.chat.promos.PROMO_MESSAGES",
            ["Default Fallback {invite}"],
        ):
            ok = await self.handler._send_promo_message(
                "partner_one",
                "1001",
                0.0,
                reason="chat_activity",
            )

        self.assertTrue(ok)
        self.assertEqual(
            self.handler.announcement_calls[0]["text"],
            "Streamer Override https://discord.gg/example",
        )

    async def test_without_any_override_falls_back_to_default_messages(self) -> None:
        with self._conn_patch(), patch(
            "bot.chat.promos.PROMO_MESSAGES",
            ["Default Fallback {invite}"],
        ):
            ok = await self.handler._send_promo_message(
                "partner_one",
                "1001",
                0.0,
                reason="chat_activity",
            )

        self.assertTrue(ok)
        self.assertEqual(
            self.handler.announcement_calls[0]["text"],
            "Default Fallback https://discord.gg/example",
        )

    async def test_invalid_streamer_override_without_invite_falls_back_to_default(self) -> None:
        self.conn.execute(
            "INSERT INTO streamer_plans (twitch_login, promo_message) VALUES (?, ?)",
            ("partner_one", "Streamer Override ohne Invite"),
        )

        with self._conn_patch(), patch(
            "bot.chat.promos.PROMO_MESSAGES",
            ["Default Fallback {invite}"],
        ):
            ok = await self.handler._send_promo_message(
                "partner_one",
                "1001",
                0.0,
                reason="chat_activity",
            )

        self.assertTrue(ok)
        self.assertEqual(
            self.handler.announcement_calls[0]["text"],
            "Default Fallback https://discord.gg/example",
        )


if __name__ == "__main__":
    unittest.main()
