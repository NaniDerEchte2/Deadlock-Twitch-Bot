from __future__ import annotations

import unittest
from datetime import UTC, datetime

from bot.promo_mode import (
    PROMO_MODE_CUSTOM_EVENT,
    PROMO_MODE_STANDARD,
    evaluate_global_promo_mode,
    validate_global_promo_mode_config,
    validate_streamer_promo_message,
)


class PromoModeTests(unittest.TestCase):
    def test_standard_mode_keeps_default_behavior(self) -> None:
        evaluation = evaluate_global_promo_mode(
            {
                "mode": PROMO_MODE_STANDARD,
                "custom_message": "Event {invite}",
                "is_enabled": True,
            },
            now=datetime(2026, 3, 7, 12, 0, tzinfo=UTC),
        )

        self.assertEqual(evaluation["status"], "standard")
        self.assertFalse(evaluation["is_active"])
        self.assertIsNone(evaluation["active_message"])

    def test_custom_event_inside_window_is_active(self) -> None:
        evaluation = evaluate_global_promo_mode(
            {
                "mode": PROMO_MODE_CUSTOM_EVENT,
                "custom_message": "Nur heute live dabei",
                "is_enabled": True,
                "starts_at": "2026-03-07T10:00:00+00:00",
                "ends_at": "2026-03-07T14:00:00+00:00",
            },
            now=datetime(2026, 3, 7, 12, 0, tzinfo=UTC),
        )

        self.assertEqual(evaluation["status"], "active")
        self.assertTrue(evaluation["is_active"])
        self.assertEqual(evaluation["active_message"], "Nur heute live dabei")

    def test_custom_event_before_start_is_scheduled(self) -> None:
        evaluation = evaluate_global_promo_mode(
            {
                "mode": PROMO_MODE_CUSTOM_EVENT,
                "custom_message": "Später: {invite}",
                "is_enabled": True,
                "starts_at": "2026-03-07T15:00:00+00:00",
            },
            now=datetime(2026, 3, 7, 12, 0, tzinfo=UTC),
        )

        self.assertEqual(evaluation["status"], "scheduled")
        self.assertFalse(evaluation["is_active"])

    def test_custom_event_after_end_is_expired(self) -> None:
        evaluation = evaluate_global_promo_mode(
            {
                "mode": PROMO_MODE_CUSTOM_EVENT,
                "custom_message": "Vorbei: {invite}",
                "is_enabled": True,
                "ends_at": "2026-03-07T11:00:00+00:00",
            },
            now=datetime(2026, 3, 7, 12, 0, tzinfo=UTC),
        )

        self.assertEqual(evaluation["status"], "expired")
        self.assertFalse(evaluation["is_active"])

    def test_validation_rejects_empty_or_invalid_custom_message(self) -> None:
        _config, issues = validate_global_promo_mode_config(
            {
                "mode": PROMO_MODE_CUSTOM_EVENT,
                "custom_message": "",
                "is_enabled": True,
            }
        )
        self.assertTrue(issues)
        self.assertIn("Event-Text", issues[0]["message"])

        _config, issues = validate_global_promo_mode_config(
            {
                "mode": PROMO_MODE_CUSTOM_EVENT,
                "custom_message": "Falscher Platzhalter {channel}",
                "is_enabled": True,
            }
        )
        self.assertTrue(issues)
        self.assertIn("Nicht unterstützter Platzhalter", issues[0]["message"])

    def test_streamer_message_requires_invite_and_max_500_characters(self) -> None:
        issues = validate_streamer_promo_message("Komm auf den Discord")
        self.assertTrue(issues)
        self.assertEqual(issues[0]["code"], "missing_invite")

        issues = validate_streamer_promo_message(("x" * 493) + "{invite}")
        self.assertTrue(issues)
        self.assertEqual(issues[0]["code"], "too_long")

        issues = validate_streamer_promo_message("Mehrere Zeilen\nsind okay: {invite}")
        self.assertEqual(issues, [])


if __name__ == "__main__":
    unittest.main()
