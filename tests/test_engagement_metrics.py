import unittest

from bot.analytics.engagement_metrics import EngagementInputs, calculate_engagement


class EngagementMetricsTests(unittest.TestCase):
    def test_no_data_returns_null_penetration_and_no_data_method(self) -> None:
        result = calculate_engagement(
            EngagementInputs(
                total_messages=0,
                active_chatters=0,
                tracked_chat_accounts=0,
                chatters_api_seen=0,
                viewer_minutes=0.0,
                viewer_minutes_has_real_samples=False,
                avg_viewers=0.0,
                session_count=0,
                sessions_with_chat=0,
            )
        )
        self.assertIsNone(result.chat_penetration_pct)
        self.assertFalse(result.chat_penetration_reliable)
        self.assertIsNone(result.messages_per_100_viewer_minutes)
        self.assertEqual(result.viewer_minutes, 0.0)
        self.assertEqual(result.method, "no_data")

    def test_only_active_chatters_is_unreliable(self) -> None:
        result = calculate_engagement(
            EngagementInputs(
                total_messages=250,
                active_chatters=20,
                tracked_chat_accounts=20,
                chatters_api_seen=0,
                viewer_minutes=800.0,
                viewer_minutes_has_real_samples=True,
                avg_viewers=12.0,
                session_count=6,
                sessions_with_chat=6,
            )
        )
        self.assertEqual(result.chat_penetration_pct, 100.0)
        self.assertFalse(result.chat_penetration_reliable)
        self.assertEqual(result.method, "low_coverage")

    def test_reliable_penetration_when_passive_and_coverage_threshold_met(self) -> None:
        result = calculate_engagement(
            EngagementInputs(
                total_messages=800,
                active_chatters=40,
                tracked_chat_accounts=100,
                chatters_api_seen=40,
                viewer_minutes=2200.0,
                viewer_minutes_has_real_samples=True,
                avg_viewers=30.0,
                session_count=10,
                sessions_with_chat=9,
            )
        )
        self.assertEqual(result.chat_penetration_pct, 40.0)
        self.assertTrue(result.chat_penetration_reliable)
        self.assertEqual(result.method, "real_samples")
        self.assertEqual(result.chatters_coverage, 0.4)
        self.assertEqual(result.passive_viewer_samples, 60)

    def test_viewer_minutes_fallback_forces_low_coverage(self) -> None:
        result = calculate_engagement(
            EngagementInputs(
                total_messages=450,
                active_chatters=18,
                tracked_chat_accounts=36,
                chatters_api_seen=12,
                viewer_minutes=1350.0,
                viewer_minutes_has_real_samples=False,
                avg_viewers=22.0,
                session_count=8,
                sessions_with_chat=8,
            )
        )
        self.assertTrue(result.chat_penetration_reliable)
        self.assertEqual(result.method, "low_coverage")
        self.assertAlmostEqual(result.messages_per_100_viewer_minutes or 0.0, 33.33, places=2)

    def test_message_before_lurker_does_not_affect_penetration_logic(self) -> None:
        result = calculate_engagement(
            EngagementInputs(
                total_messages=120,
                active_chatters=6,
                tracked_chat_accounts=10,
                chatters_api_seen=1,
                viewer_minutes=540.0,
                viewer_minutes_has_real_samples=True,
                avg_viewers=8.0,
                session_count=4,
                sessions_with_chat=4,
            )
        )
        self.assertFalse(result.chat_penetration_reliable)
        self.assertEqual(result.method, "low_coverage")


if __name__ == "__main__":
    unittest.main()
