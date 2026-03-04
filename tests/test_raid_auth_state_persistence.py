import contextlib
import unittest
from unittest.mock import patch

from bot.raid.auth import RaidAuthManager


class _FakeCursor:
    def __init__(self, row=None, rowcount: int = 0):
        self._row = row
        self.rowcount = rowcount

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self, rows_by_fragment: dict[str, object] | None = None) -> None:
        self.rows_by_fragment = rows_by_fragment or {}
        self.calls: list[tuple[str, tuple]] = []

    def execute(self, sql: str, params=()):
        params_tuple = tuple(params or ())
        self.calls.append((sql, params_tuple))
        for fragment, row in self.rows_by_fragment.items():
            if fragment in sql:
                return _FakeCursor(row=row, rowcount=1 if row is not None else 0)
        return _FakeCursor(row=None, rowcount=0)


class RaidAuthStatePersistenceTests(unittest.TestCase):
    def test_generate_auth_url_persists_state_token_in_db(self) -> None:
        fake_conn = _FakeConn()

        manager = RaidAuthManager(
            client_id="cid",
            client_secret="secret",
            redirect_uri="https://raid.earlysalty.com/twitch/raid/callback",
        )

        with (
            patch("bot.raid.auth.secrets.token_urlsafe", return_value="state-123"),
            patch("bot.raid.auth.time.time", return_value=1700000000.0),
            patch(
                "bot.raid.auth.get_conn",
                side_effect=lambda: contextlib.nullcontext(fake_conn),
            ),
        ):
            auth_url = manager.generate_auth_url("discord:123456789")

        self.assertIn("state=state-123", auth_url)
        insert_calls = [call for call in fake_conn.calls if "INSERT INTO oauth_state_tokens" in call[0]]
        self.assertEqual(len(insert_calls), 1)
        _, params = insert_calls[0]
        self.assertEqual(params[0], "state-123")
        self.assertEqual(params[1], "twitch_raid")
        self.assertEqual(params[2], "discord:123456789")

    def test_get_pending_auth_url_rebuilds_from_persisted_state(self) -> None:
        fake_conn = _FakeConn(rows_by_fragment={"SELECT streamer_login": {"streamer_login": "discord:42"}})
        manager = RaidAuthManager(
            client_id="cid",
            client_secret="secret",
            redirect_uri="https://raid.earlysalty.com/twitch/raid/callback",
        )

        with patch(
            "bot.raid.auth.get_conn",
            side_effect=lambda: contextlib.nullcontext(fake_conn),
        ):
            full_url = manager.get_pending_auth_url("state-xyz")

        assert full_url is not None
        self.assertIn("id.twitch.tv/oauth2/authorize", full_url)
        self.assertIn("state=state-xyz", full_url)
        self.assertIn("force_verify=true", full_url)

    def test_verify_state_consumes_state_from_db(self) -> None:
        fake_conn = _FakeConn(
            rows_by_fragment={
                "DELETE FROM oauth_state_tokens": {"streamer_login": "discord:777"},
            }
        )
        manager = RaidAuthManager(
            client_id="cid",
            client_secret="secret",
            redirect_uri="https://raid.earlysalty.com/twitch/raid/callback",
        )

        with patch(
            "bot.raid.auth.get_conn",
            side_effect=lambda: contextlib.nullcontext(fake_conn),
        ):
            login = manager.verify_state("state-consume")

        self.assertEqual(login, "discord:777")
        delete_calls = [call for call in fake_conn.calls if "DELETE FROM oauth_state_tokens" in call[0]]
        self.assertEqual(len(delete_calls), 1)


if __name__ == "__main__":
    unittest.main()
