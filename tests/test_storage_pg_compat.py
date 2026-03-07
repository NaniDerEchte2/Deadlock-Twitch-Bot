import unittest
from types import SimpleNamespace

from bot.storage.pg import _CompatConnection


class _RecordingConnection:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, params=(), *args, **kwargs):
        self.executed.append((sql, tuple(params or ())))
        return SimpleNamespace(rowcount=0)


class CompatConnectionExecuteScriptTests(unittest.TestCase):
    def test_executescript_splits_statements_without_breaking_quoted_sections(self) -> None:
        raw = _RecordingConnection()
        conn = _CompatConnection(raw)

        conn.executescript(
            """
            -- semicolon in comment;
            CREATE FUNCTION demo() RETURNS text
            LANGUAGE plpgsql
            AS $func$
            BEGIN
              RETURN 'hello;world';
            END;
            $func$;

            CREATE TABLE affiliate_demo (
              id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
              note TEXT DEFAULT 'semi;colon'
            );
            """
        )

        self.assertEqual(len(raw.executed), 2)
        self.assertIn("CREATE FUNCTION demo()", raw.executed[0][0])
        self.assertIn("RETURN 'hello;world'", raw.executed[0][0])
        self.assertIn("CREATE TABLE affiliate_demo", raw.executed[1][0])
        self.assertIn("DEFAULT 'semi;colon'", raw.executed[1][0])


if __name__ == "__main__":
    unittest.main()
