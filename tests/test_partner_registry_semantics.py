from __future__ import annotations

import sqlite3
import unittest

from bot.storage.partner_registry import archive_active_partner, bulk_update_partner_flags


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE twitch_streamer_identities (
            twitch_user_id TEXT PRIMARY KEY,
            twitch_login TEXT NOT NULL,
            discord_user_id TEXT,
            discord_display_name TEXT,
            is_on_discord INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE twitch_partners (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            twitch_user_id TEXT NOT NULL,
            twitch_login TEXT NOT NULL,
            require_discord_link INTEGER DEFAULT 0,
            last_description TEXT,
            last_link_ok INTEGER,
            added_by TEXT,
            last_link_checked_at TEXT,
            next_link_check_at TEXT,
            manual_verified_permanent INTEGER DEFAULT 0,
            manual_verified_until TEXT,
            manual_verified_at TEXT,
            manual_partner_opt_out INTEGER DEFAULT 0,
            raid_bot_enabled INTEGER DEFAULT 0,
            silent_ban INTEGER DEFAULT 0,
            silent_raid INTEGER DEFAULT 0,
            live_ping_role_id TEXT,
            live_ping_enabled INTEGER DEFAULT 1,
            partnered_at TEXT,
            departnered_at TEXT,
            status TEXT NOT NULL DEFAULT 'active'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE twitch_streamers (
            twitch_login TEXT PRIMARY KEY,
            twitch_user_id TEXT,
            require_discord_link INTEGER DEFAULT 0,
            next_link_check_at TEXT,
            discord_user_id TEXT,
            discord_display_name TEXT,
            is_on_discord INTEGER DEFAULT 0,
            manual_verified_permanent INTEGER DEFAULT 0,
            manual_verified_until TEXT,
            manual_verified_at TEXT,
            manual_partner_opt_out INTEGER DEFAULT 0,
            archived_at TEXT,
            raid_bot_enabled INTEGER DEFAULT 0,
            silent_ban INTEGER DEFAULT 0,
            silent_raid INTEGER DEFAULT 0,
            is_monitored_only INTEGER DEFAULT 0,
            live_ping_role_id TEXT,
            live_ping_enabled INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE twitch_raid_auth (
            twitch_user_id TEXT PRIMARY KEY,
            twitch_login TEXT,
            raid_enabled INTEGER DEFAULT 1,
            needs_reauth INTEGER DEFAULT 0
        )
        """
    )
    return conn


class PartnerRegistrySemanticsTests(unittest.TestCase):
    def test_archive_active_partner_keeps_non_partner_table_empty(self) -> None:
        conn = _make_conn()
        conn.execute(
            """
            INSERT INTO twitch_streamer_identities (
                twitch_user_id, twitch_login, discord_user_id, discord_display_name, is_on_discord
            ) VALUES (?, ?, ?, ?, ?)
            """,
            ("1001", "alpha", "123", "Alpha", 1),
        )
        conn.execute(
            """
            INSERT INTO twitch_partners (
                twitch_user_id, twitch_login, raid_bot_enabled, partnered_at, status
            ) VALUES (?, ?, 1, '2026-03-01T10:00:00+00:00', 'active')
            """,
            ("1001", "alpha"),
        )
        conn.execute(
            """
            INSERT INTO twitch_raid_auth (twitch_user_id, twitch_login, raid_enabled, needs_reauth)
            VALUES (?, ?, 1, 0)
            """,
            ("1001", "alpha"),
        )

        result = archive_active_partner(conn, twitch_login="alpha")

        self.assertIsNotNone(result)
        status_row = conn.execute(
            "SELECT status, departnered_at FROM twitch_partners WHERE twitch_user_id = ?",
            ("1001",),
        ).fetchone()
        self.assertEqual(status_row["status"], "archived")
        self.assertTrue(status_row["departnered_at"])
        streamer_count = conn.execute("SELECT COUNT(*) AS total FROM twitch_streamers").fetchone()
        self.assertEqual(int(streamer_count["total"]), 0)
        auth_row = conn.execute(
            "SELECT raid_enabled FROM twitch_raid_auth WHERE twitch_user_id = ?",
            ("1001",),
        ).fetchone()
        self.assertEqual(int(auth_row["raid_enabled"]), 0)

    def test_bulk_update_scope_all_only_mutates_active_rows(self) -> None:
        conn = _make_conn()
        conn.executemany(
            """
            INSERT INTO twitch_partners (
                twitch_user_id,
                twitch_login,
                silent_raid,
                partnered_at,
                departnered_at,
                status
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                ("1001", "alpha", 0, "2026-03-01T10:00:00+00:00", None, "active"),
                (
                    "2002",
                    "bravo",
                    0,
                    "2026-03-01T10:00:00+00:00",
                    "2026-03-10T10:00:00+00:00",
                    "archived",
                ),
            ],
        )

        total = bulk_update_partner_flags(conn, scope="all", silent_raid=1)

        self.assertEqual(total, 1)
        rows = conn.execute(
            "SELECT twitch_user_id, silent_raid, status FROM twitch_partners ORDER BY twitch_user_id"
        ).fetchall()
        self.assertEqual(int(rows[0]["silent_raid"]), 1)
        self.assertEqual(rows[0]["status"], "active")
        self.assertEqual(int(rows[1]["silent_raid"]), 0)
        self.assertEqual(rows[1]["status"], "archived")


if __name__ == "__main__":
    unittest.main()
