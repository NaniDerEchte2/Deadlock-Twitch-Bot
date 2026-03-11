import unittest
from unittest.mock import patch

from bot.chat.moderation import ModerationMixin


class _DummyTokenManager:
    async def get_valid_token(self, force_refresh: bool = False):
        del force_refresh
        return ("token", "refresh")


class _DummyRaidBot:
    def __init__(self) -> None:
        self.blacklist_calls: list[tuple[str | None, str, str]] = []

    def _add_to_blacklist(self, target_id: str | None, target_login: str, reason: str) -> None:
        self.blacklist_calls.append((target_id, target_login, reason))


class _DummyModerationChat(ModerationMixin):
    def __init__(self) -> None:
        self._client_id = "client"
        self.bot_id_safe = "sender-1"
        self.bot_id = "sender-1"
        self._token_manager = _DummyTokenManager()
        self._raid_bot = _DummyRaidBot()

    @staticmethod
    def _normalize_channel_login(login: str) -> str:
        return str(login or "").strip().lower().lstrip("#")

    def _normalize_channel_login_safe(self, channel) -> str:
        return self._normalize_channel_login(getattr(channel, "name", ""))

    @staticmethod
    def _is_partner_channel_for_chat_tracking(login: str) -> bool:
        del login
        return False


class _DummyResponse:
    def __init__(self, status: int, payload_text: str) -> None:
        self.status = status
        self._payload_text = payload_text

    async def text(self) -> str:
        return self._payload_text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        del exc_type, exc, tb
        return False


class _DummyClientSession:
    def __init__(self, response: _DummyResponse) -> None:
        self._response = response

    def post(self, url, headers=None, json=None):
        del url, headers, json
        return self._response

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        del exc_type, exc, tb
        return False


class _DummyChannel:
    def __init__(self, name: str, channel_id: str) -> None:
        self.name = name
        self.id = channel_id


class ChatMessageBlacklistTests(unittest.IsolatedAsyncioTestCase):
    async def test_dropped_recruitment_message_blacklists_banned_phone_alias(self) -> None:
        handler = _DummyModerationChat()
        channel = _DummyChannel("cemo_336", "494921554")
        response = _DummyResponse(
            200,
            (
                '{"data":[{"is_sent":false,"drop_reason":{"code":"banned_phone_alias",'
                '"message":"Your message was not sent because your phone number is banned from this channel."}}]}'
            ),
        )

        with patch("aiohttp.ClientSession", return_value=_DummyClientSession(response)):
            ok = await handler._send_chat_message(channel, "hello", source="recruitment")

        self.assertFalse(ok)
        self.assertEqual(len(handler._raid_bot.blacklist_calls), 1)
        target_id, target_login, reason = handler._raid_bot.blacklist_calls[0]
        self.assertEqual(target_id, "494921554")
        self.assertEqual(target_login, "cemo_336")
        self.assertIn("recruitment_bot_banned", reason)
        self.assertIn("banned_phone_alias", reason)


if __name__ == "__main__":
    unittest.main()
