import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from bot.chat.commands import RaidCommandsMixin


class _DummyCtx:
    def __init__(self):
        self.author = SimpleNamespace(
            name="tester",
            is_moderator=False,
            moderator=False,
            is_broadcaster=True,
            broadcaster=True,
        )
        self.channel = SimpleNamespace(name="source_login")
        self.sent_messages: list[str] = []

    async def send(self, message: str):
        self.sent_messages.append(message)


class _DummyChatCommands(RaidCommandsMixin):
    def __init__(self):
        self._raid_bot = SimpleNamespace(
            auth_manager=SimpleNamespace(has_enabled_auth=lambda _user_id: True),
            session=object(),
            start_manual_raid=AsyncMock(
                return_value={"status": "started", "target_login": "deadlocker"}
            ),
        )

    def _get_streamer_by_channel(self, channel_name: str):
        if channel_name == "source_login":
            return ("source_login", "1001", 1)
        return None

    async def _is_fully_authed(self, twitch_user_id: str) -> bool:
        return twitch_user_id == "1001"


class ChatRaidCommandTests(unittest.IsolatedAsyncioTestCase):
    async def test_cmd_raid_delegates_to_shared_manual_flow(self) -> None:
        cog = _DummyChatCommands()
        ctx = _DummyCtx()

        await _DummyChatCommands.cmd_raid.callback(cog, ctx)

        cog._raid_bot.start_manual_raid.assert_awaited_once_with(
            broadcaster_id="1001",
            broadcaster_login="source_login",
        )
        self.assertEqual(
            ctx.sent_messages,
            ["@tester Raid auf deadlocker gestartet! (Twitch-Countdown ~90s)"],
        )


if __name__ == "__main__":
    unittest.main()
