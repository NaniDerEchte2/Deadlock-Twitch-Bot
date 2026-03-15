import unittest

from bot.storage import pg as storage_pg


class _RecordingConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql, params=None):
        self.calls.append((str(sql), tuple(params or ())))
        return self


class PgLiveStateMaintenanceTests(unittest.TestCase):
    def test_cleanup_duplicate_live_state_rows_executes_targeted_delete(self) -> None:
        conn = _RecordingConn()

        storage_pg._cleanup_duplicate_live_state_rows(conn)

        self.assertEqual(len(conn.calls), 1)
        sql = conn.calls[0][0]
        self.assertIn("DELETE FROM twitch_live_state legacy", sql)
        self.assertIn("USING twitch_live_state canonical", sql)

    def test_ensure_unique_live_state_login_index_creates_unique_index(self) -> None:
        conn = _RecordingConn()

        storage_pg._ensure_unique_live_state_login_index(conn)

        self.assertEqual(len(conn.calls), 1)
        sql = conn.calls[0][0]
        self.assertIn("CREATE UNIQUE INDEX IF NOT EXISTS idx_twitch_live_state_login_lower", sql)
        self.assertIn("ON twitch_live_state(LOWER(streamer_login))", sql)


if __name__ == "__main__":
    unittest.main()
