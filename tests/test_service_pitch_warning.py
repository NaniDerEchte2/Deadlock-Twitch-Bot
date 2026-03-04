import unittest

from bot.chat.service_pitch_warning import ServicePitchWarningMixin


class _ServicePitchHarness(ServicePitchWarningMixin):
    def __init__(self) -> None:
        self.prefix = "!"
        self._init_service_pitch_warning()


class _FakeAuthor:
    def __init__(self, *, name: str, user_id: str) -> None:
        self.name = name
        self.id = user_id
        self.moderator = False
        self.broadcaster = False


class _FakeChannel:
    def __init__(self, *, login: str) -> None:
        self.login = login
        self.name = login


class _FakeMessage:
    def __init__(self, *, content: str, author: _FakeAuthor, channel: _FakeChannel) -> None:
        self.content = content
        self.author = author
        self.channel = channel


class _AsyncServicePitchHarness(ServicePitchWarningMixin):
    def __init__(self, *, account_age_days: int | None, follower_count: int | None) -> None:
        self.prefix = "!"
        self._account_age_days = account_age_days
        self._follower_count = follower_count
        self.sent_messages: list[str] = []
        self._init_service_pitch_warning()

    async def _get_account_age_days(self, author_id: str, author_login: str) -> int | None:
        return self._account_age_days

    def _is_low_follower_target(self, channel_login: str) -> tuple[bool, int | None]:
        return True, self._follower_count

    async def _send_chat_message(self, channel, text: str, source: str = "") -> bool:
        self.sent_messages.append(text)
        return True


class ServicePitchWarningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = _ServicePitchHarness()

    def test_trusted_twitch_collab_invite_does_not_score_as_external(self) -> None:
        score, _, features = self.harness._score_service_pitch_message(
            "https://twitch.tv/collab/invite/mY23S2Y2"
        )
        self.assertEqual(score, 0)
        self.assertIn("trusted_twitch_collab_invite", features)
        self.assertNotIn("external_link_or_handle", features)

    def test_discord_teamup_handle_drop_is_detected(self) -> None:
        score, _, features = self.harness._score_service_pitch_message(
            "Yo mate, into gameplay? Lets team up on Discord: vouch_match"
        )
        self.assertGreaterEqual(score, 10)
        self.assertIn("offplatform", features)
        self.assertIn("discord_teamup_pitch", features)
        self.assertIn("discord_handle_drop", features)
        self.assertIn("external_link_or_handle", features)

    def test_quick_action_requires_new_account_and_first_observed_message(self) -> None:
        self.assertTrue(
            self.harness._is_quick_action_eligible(
                is_new_account=True,
                is_first_observed_message=True,
            )
        )
        self.assertFalse(
            self.harness._is_quick_action_eligible(
                is_new_account=False,
                is_first_observed_message=True,
            )
        )
        self.assertFalse(
            self.harness._is_quick_action_eligible(
                is_new_account=True,
                is_first_observed_message=False,
            )
        )

    def test_benign_social_checkin_only_for_greeting_and_wellbeing(self) -> None:
        self.assertTrue(
            self.harness._is_benign_social_checkin(
                "@queen_snippo guten morgen na wie geht's dir?",
                {"greeting", "wellbeing"},
            )
        )
        self.assertFalse(
            self.harness._is_benign_social_checkin(
                "na wie geht's dir? check mal https://discord.gg/test",
                {"greeting", "wellbeing"},
            )
        )
        self.assertFalse(
            self.harness._is_benign_social_checkin(
                "na wie geht's dir?",
                {"greeting", "offplatform"},
            )
        )

    def test_high_confidence_single_message_signal_for_teamup_pitch(self) -> None:
        self.assertTrue(
            self.harness._has_high_confidence_single_message_signal(
                {"offplatform", "discord_teamup_pitch"}
            )
        )
        self.assertFalse(
            self.harness._has_high_confidence_single_message_signal({"wellbeing"})
        )


class ServicePitchWarningAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_account_how_are_you_stays_hint_only(self) -> None:
        harness = _AsyncServicePitchHarness(account_age_days=15, follower_count=209)
        author = _FakeAuthor(name="earlysalty", user_id="1445007207")
        channel = _FakeChannel(login="gracie_carter_")

        first = _FakeMessage(content="how are u?", author=author, channel=channel)
        second = _FakeMessage(content="how are u?", author=author, channel=channel)

        first_result = await harness._maybe_warn_service_pitch(first, channel_login=channel.login)
        second_result = await harness._maybe_warn_service_pitch(second, channel_login=channel.login)

        self.assertFalse(first_result)
        self.assertTrue(second_result)
        self.assertEqual(harness.sent_messages, [])

    async def test_new_account_teamup_with_discord_handle_triggers_quick_action(self) -> None:
        harness = _AsyncServicePitchHarness(account_age_days=2, follower_count=39)
        author = _FakeAuthor(name="albiiionlu", user_id="1445630869")
        channel = _FakeChannel(login="elara_foster_4")

        message = _FakeMessage(
            content="Yo mate, into gameplay? Lets team up on Discord: vouch_match",
            author=author,
            channel=channel,
        )

        triggered = await harness._maybe_warn_service_pitch(message, channel_login=channel.login)
        self.assertTrue(triggered)
        self.assertEqual(len(harness.sent_messages), 1)


if __name__ == "__main__":
    unittest.main()
