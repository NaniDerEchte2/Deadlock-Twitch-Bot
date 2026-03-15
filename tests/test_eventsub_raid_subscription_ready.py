import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from bot.monitoring.eventsub_mixin import _EventSubMixin


class _DummyEventSubMixin(_EventSubMixin):
    def __init__(self) -> None:
        self.api = SimpleNamespace()
        self._webhook_secret = "secret"
        self.tracked: list[tuple[str, str]] = []

    def _get_eventsub_webhook_url(self) -> str:
        return "https://example.com/twitch/eventsub/callback"

    def _eventsub_track_sub(self, sub_type: str, broadcaster_user_id: str) -> None:
        self.tracked.append((sub_type, str(broadcaster_user_id)))

    async def _record_eventsub_capacity_snapshot(
        self,
        reason: str,
        *,
        force: bool = False,
    ) -> None:
        return None


def _raid_subscription(status: str, broadcaster_id: str = "123") -> dict:
    return {
        "type": "channel.raid",
        "status": status,
        "condition": {"to_broadcaster_user_id": broadcaster_id},
        "transport": {"callback": "https://example.com/twitch/eventsub/callback"},
    }


class EventSubRaidSubscriptionReadyTests(unittest.IsolatedAsyncioTestCase):
    async def test_ready_check_returns_immediately_when_subscription_is_enabled(self) -> None:
        mixin = _DummyEventSubMixin()
        mixin.api.list_eventsub_subscriptions = AsyncMock(
            return_value=[_raid_subscription("enabled")]
        )
        mixin.api.subscribe_eventsub_webhook = AsyncMock()

        ready, detail = await mixin.ensure_raid_target_dynamic_ready("123", "targetlogin")

        self.assertTrue(ready)
        self.assertEqual(detail, "already_enabled")
        mixin.api.subscribe_eventsub_webhook.assert_not_awaited()
        self.assertEqual(mixin.tracked, [("channel.raid", "123")])

    async def test_ready_check_waits_until_verification_is_enabled(self) -> None:
        mixin = _DummyEventSubMixin()
        mixin.api.subscribe_eventsub_webhook = AsyncMock(return_value={"data": [{"id": "sub-1"}]})
        mixin.api.list_eventsub_subscriptions = AsyncMock(
            side_effect=[
                [],
                [_raid_subscription("webhook_callback_verification_pending")],
                [_raid_subscription("enabled")],
            ]
        )

        ready, detail = await mixin.ensure_raid_target_dynamic_ready(
            "123",
            "targetlogin",
            wait_timeout_seconds=0.1,
            poll_interval_seconds=0.0,
        )

        self.assertTrue(ready)
        self.assertEqual(detail, "enabled")
        mixin.api.subscribe_eventsub_webhook.assert_awaited_once()
        self.assertEqual(mixin.tracked[-1], ("channel.raid", "123"))


if __name__ == "__main__":
    unittest.main()
