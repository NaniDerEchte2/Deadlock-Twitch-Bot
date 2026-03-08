import contextlib
import sqlite3
import time
import unittest
from unittest.mock import AsyncMock, patch

import aiohttp

from bot.raid.bot import RaidBot
from bot.storage import proxy as storage_proxy


class RaidPartnerScoreSelectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        storage_proxy.ensure_schema(self.conn)
        self.session = aiohttp.ClientSession()
        self.raid_bot = RaidBot(
            client_id="client-id",
            client_secret="client-secret",
            redirect_uri="http://localhost/raid/callback",
            session=self.session,
        )

    async def asyncTearDown(self) -> None:
        await self.raid_bot.cleanup()
        await self.session.close()
        self.conn.close()

    def _conn_patch(self):
        return patch(
            "bot.raid.bot.get_conn",
            side_effect=lambda: contextlib.nullcontext(self.conn),
        )

    @staticmethod
    def _score_map(rows: dict[str, dict[str, object]]):
        def _loader(user_ids: list[str]):
            return {user_id: dict(rows[user_id]) for user_id in user_ids if user_id in rows}

        return _loader

    async def test_select_partner_candidate_prefers_highest_final_score(self) -> None:
        candidates = [
            {
                "user_id": "1001",
                "user_login": "alpha",
                "viewer_count": 50,
                "followers_total": 1000,
                "started_at": "2026-03-08T18:00:00+00:00",
            },
            {
                "user_id": "2002",
                "user_login": "bravo",
                "viewer_count": 10,
                "followers_total": 200,
                "started_at": "2026-03-08T18:10:00+00:00",
            },
        ]
        score_rows = {
            "1001": {"is_live": True, "final_score": 0.91, "today_received_raids": 5},
            "2002": {"is_live": True, "final_score": 0.66, "today_received_raids": 0},
        }

        with (
            self._conn_patch(),
            patch(
                "bot.raid.bot.load_partner_raid_score_map",
                side_effect=self._score_map(score_rows),
            ),
            patch.object(self.raid_bot, "_attach_followers_totals", new=AsyncMock()) as attach_mock,
        ):
            selected = await self.raid_bot._select_partner_candidate_by_score(
                candidates,
                "source-1",
            )

        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected["user_login"], "alpha")
        attach_mock.assert_not_awaited()

    async def test_select_partner_candidate_uses_today_received_raids_for_close_scores(self) -> None:
        candidates = [
            {
                "user_id": "1001",
                "user_login": "alpha",
                "viewer_count": 50,
                "followers_total": 1000,
                "started_at": "2026-03-08T18:00:00+00:00",
            },
            {
                "user_id": "2002",
                "user_login": "bravo",
                "viewer_count": 75,
                "followers_total": 800,
                "started_at": "2026-03-08T17:00:00+00:00",
            },
        ]
        score_rows = {
            "1001": {"is_live": True, "final_score": 0.90, "today_received_raids": 4},
            "2002": {"is_live": True, "final_score": 0.86, "today_received_raids": 1},
        }

        with (
            self._conn_patch(),
            patch(
                "bot.raid.bot.load_partner_raid_score_map",
                side_effect=self._score_map(score_rows),
            ),
            patch.object(self.raid_bot, "_attach_followers_totals", new=AsyncMock()) as attach_mock,
        ):
            selected = await self.raid_bot._select_partner_candidate_by_score(
                candidates,
                "source-2",
            )

        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected["user_login"], "bravo")
        attach_mock.assert_not_awaited()

    async def test_select_partner_candidate_falls_back_to_viewers_followers_started_at(self) -> None:
        candidates = [
            {
                "user_id": "1001",
                "user_login": "alpha",
                "viewer_count": 40,
                "followers_total": 500,
                "started_at": "2026-03-08T18:00:00+00:00",
            },
            {
                "user_id": "2002",
                "user_login": "bravo",
                "viewer_count": 10,
                "followers_total": 800,
                "started_at": "2026-03-08T17:00:00+00:00",
            },
            {
                "user_id": "3003",
                "user_login": "charlie",
                "viewer_count": 10,
                "followers_total": 300,
                "started_at": "2026-03-08T16:00:00+00:00",
            },
        ]
        score_rows = {
            "1001": {"is_live": True, "final_score": 0.90, "today_received_raids": 1},
            "2002": {"is_live": True, "final_score": 0.86, "today_received_raids": 1},
            "3003": {"is_live": True, "final_score": 0.87, "today_received_raids": 1},
        }

        with (
            self._conn_patch(),
            patch(
                "bot.raid.bot.load_partner_raid_score_map",
                side_effect=self._score_map(score_rows),
            ),
            patch.object(self.raid_bot, "_attach_followers_totals", new=AsyncMock()) as attach_mock,
        ):
            selected = await self.raid_bot._select_partner_candidate_by_score(
                candidates,
                "source-3",
            )

        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected["user_login"], "charlie")
        attach_mock.assert_awaited_once()

    async def test_select_partner_candidate_ignores_recent_target_cooldown_for_partners(self) -> None:
        candidates = [
            {
                "user_id": "1001",
                "user_login": "alpha",
                "viewer_count": 25,
                "followers_total": 400,
                "started_at": "2026-03-08T18:00:00+00:00",
            },
            {
                "user_id": "2002",
                "user_login": "bravo",
                "viewer_count": 20,
                "followers_total": 300,
                "started_at": "2026-03-08T17:00:00+00:00",
            },
            {
                "user_id": "3003",
                "user_login": "charlie",
                "viewer_count": 10,
                "followers_total": 100,
                "started_at": "2026-03-08T16:00:00+00:00",
            },
        ]
        score_rows = {
            "1001": {"is_live": True, "final_score": 0.95, "today_received_raids": 0},
            "3003": {"is_live": True, "final_score": 0.80, "today_received_raids": 0},
        }

        with (
            self._conn_patch(),
            patch(
                "bot.raid.bot.load_partner_raid_score_map",
                side_effect=self._score_map(score_rows),
            ),
            patch.object(self.raid_bot, "_get_recent_raid_targets", return_value={"1001"}),
            patch.object(self.raid_bot, "_attach_followers_totals", new=AsyncMock()),
        ):
            selected = await self.raid_bot._select_partner_candidate_by_score(
                candidates,
                "source-4",
            )

        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected["user_login"], "alpha")

    async def test_select_fairest_candidate_keeps_recent_target_cooldown_for_fallback(self) -> None:
        candidates = [
            {
                "user_id": "1001",
                "user_login": "alpha",
                "viewer_count": 10,
                "followers_total": 100,
                "started_at": "2026-03-08T18:00:00+00:00",
            },
            {
                "user_id": "2002",
                "user_login": "bravo",
                "viewer_count": 25,
                "followers_total": 100,
                "started_at": "2026-03-08T17:00:00+00:00",
            },
        ]

        with (
            self._conn_patch(),
            patch.object(self.raid_bot, "_get_recent_raid_targets", return_value={"1001"}),
            patch.object(self.raid_bot, "_attach_followers_totals", new=AsyncMock()) as attach_mock,
        ):
            selected = await self.raid_bot._select_fairest_candidate(
                candidates,
                "source-5",
            )

        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected["user_login"], "bravo")
        attach_mock.assert_awaited_once()

    async def test_on_raid_arrival_refreshes_cache_after_partner_confirmation(self) -> None:
        self.conn.execute(
            """
            INSERT INTO twitch_streamers (twitch_login, twitch_user_id, silent_raid)
            VALUES (?, ?, 0)
            """,
            ("targetlogin", "9009"),
        )
        self.raid_bot._pending_raids["9009"] = (
            "source_login",
            None,
            time.time(),
            True,
            42,
            None,
        )

        with (
            self._conn_patch(),
            patch(
                "bot.raid.bot.refresh_partner_raid_score_async",
                new=AsyncMock(return_value={"twitch_user_id": "9009"}),
            ) as refresh_mock,
            patch.object(
                self.raid_bot,
                "_send_partner_raid_message",
                new=AsyncMock(return_value=None),
            ) as send_mock,
        ):
            await self.raid_bot.on_raid_arrival(
                to_broadcaster_id="9009",
                to_broadcaster_login="targetlogin",
                from_broadcaster_login="source_login",
                from_broadcaster_id="source-id",
                viewer_count=42,
            )

        refresh_mock.assert_awaited_once_with("9009")
        send_mock.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
