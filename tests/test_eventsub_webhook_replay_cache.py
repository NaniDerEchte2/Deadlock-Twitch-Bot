import unittest
from unittest.mock import patch

from bot.monitoring.eventsub_webhook import EventSubWebhookHandler, _MAX_MESSAGE_AGE_SECONDS


class EventSubWebhookReplayCacheTests(unittest.TestCase):
    def test_replay_ids_are_not_evicted_by_count_within_ttl_window(self) -> None:
        handler = EventSubWebhookHandler(secret="test-secret")
        clock = {"now": 1_000.0}
        handler._now_timestamp = lambda: clock["now"]  # type: ignore[method-assign]

        for idx in range(2_500):
            self.assertTrue(handler._track_message_id(f"msg-{idx}"))

        self.assertTrue(handler._is_duplicate("msg-0"))

    def test_expired_ids_are_cleaned_up(self) -> None:
        handler = EventSubWebhookHandler(secret="test-secret")
        clock = {"now": 5_000.0}
        handler._now_timestamp = lambda: clock["now"]  # type: ignore[method-assign]

        self.assertTrue(handler._track_message_id("msg-a"))
        self.assertTrue(handler._track_message_id("msg-b"))
        self.assertEqual(len(handler._seen_message_ids), 2)

        clock["now"] += _MAX_MESSAGE_AGE_SECONDS + 1

        self.assertFalse(handler._is_duplicate("msg-a"))
        self.assertEqual(handler._seen_message_ids, {})
        self.assertEqual(handler._seen_expiry_heap, [])

    @patch("bot.monitoring.eventsub_webhook._SEEN_ID_HARD_LIMIT", 2)
    def test_optional_hard_limit_is_fail_closed_without_ttl_eviction(self) -> None:
        handler = EventSubWebhookHandler(secret="test-secret")
        clock = {"now": 10_000.0}
        handler._now_timestamp = lambda: clock["now"]  # type: ignore[method-assign]

        self.assertTrue(handler._track_message_id("msg-1"))
        self.assertTrue(handler._track_message_id("msg-2"))
        self.assertFalse(handler._track_message_id("msg-3"))
        self.assertTrue(handler._is_duplicate("msg-1"))


if __name__ == "__main__":
    unittest.main()
