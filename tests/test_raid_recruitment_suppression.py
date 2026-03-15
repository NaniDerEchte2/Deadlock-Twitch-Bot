import unittest
from unittest.mock import AsyncMock

from bot.raid.bot import RaidBot


class _DummyChatBot:
    def __init__(self, suppression: dict | None) -> None:
        self._suppression = suppression
        self.join = AsyncMock()
        self.follow_channel = AsyncMock()
        self._send_chat_message = AsyncMock(return_value=True)

    def _get_outbound_chat_suppression(self, channel, source: str) -> dict | None:
        del channel, source
        return self._suppression


class RaidRecruitmentSuppressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_recruitment_message_skips_known_channel_settings_target(self) -> None:
        raid_bot = object.__new__(RaidBot)
        raid_bot.chat_bot = _DummyChatBot(
            {
                "reason_code": "channel_settings",
                "reason_detail": "channel_settings: moderation settings",
                "suppressed_until": "2026-03-22T12:00:00+00:00",
            }
        )

        await RaidBot._send_recruitment_message_now(
            raid_bot,
            from_broadcaster_login="earlysalty",
            to_broadcaster_login="realclassik",
            target_stream_data={"user_id": "471205134"},
        )

        raid_bot.chat_bot.join.assert_not_awaited()
        raid_bot.chat_bot.follow_channel.assert_not_awaited()
        raid_bot.chat_bot._send_chat_message.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
