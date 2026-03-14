import unittest

from bot.storage import pg as storage_pg


class _FakeCursor:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self, data_type: str | None) -> None:
        self.data_type = data_type
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql, params=None):
        sql_text = str(sql)
        params_tuple = tuple(params or ())
        self.calls.append((sql_text, params_tuple))
        if "SELECT data_type FROM information_schema.columns" in sql_text:
            row = None if self.data_type is None else (self.data_type,)
            return _FakeCursor(row)
        return _FakeCursor(None)


class PgSchemaBooleanMigrationTests(unittest.TestCase):
    def test_coerce_column_to_boolean_converts_legacy_integer_flag(self) -> None:
        conn = _FakeConn("integer")

        storage_pg._coerce_column_to_boolean(
            conn,
            "twitch_chat_messages",
            "is_command",
            default=False,
        )

        alter_calls = [sql for sql, _ in conn.calls if sql.strip().startswith("ALTER TABLE")]
        self.assertEqual(len(alter_calls), 2)
        self.assertIn("ALTER COLUMN is_command TYPE BOOLEAN", alter_calls[0])
        self.assertIn("ALTER COLUMN is_command SET DEFAULT FALSE", alter_calls[1])

    def test_coerce_column_to_boolean_keeps_existing_boolean_column(self) -> None:
        conn = _FakeConn("boolean")

        storage_pg._coerce_column_to_boolean(
            conn,
            "twitch_session_chatters",
            "seen_via_chatters_api",
            default=False,
        )

        alter_calls = [sql for sql, _ in conn.calls if sql.strip().startswith("ALTER TABLE")]
        self.assertEqual(len(alter_calls), 1)
        self.assertIn("ALTER COLUMN seen_via_chatters_api SET DEFAULT FALSE", alter_calls[0])


if __name__ == "__main__":
    unittest.main()
