import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bot.analytics.api_v2 import AnalyticsV2Mixin


class _InternalHomeLogHarness(AnalyticsV2Mixin):
    pass


class InternalHomeActivityLogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.handler = _InternalHomeLogHarness()

    def test_parse_internal_home_autoban_line_includes_full_details(self) -> None:
        raw = (
            "2026-03-03T07:38:14.973126+00:00\t[BANNED]\tdonnsotfd\t"
            "sonicbodyguardvkkhlnt\t1451662346\t-\t@donnsotfd Top Viewers streamboo .live"
        )

        parsed = self.handler._parse_internal_home_autoban_line(raw)

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed["event_type"], "ban")
        self.assertEqual(parsed["actor_login"], "donnsotfd")
        self.assertEqual(parsed["target_login"], "sonicbodyguardvkkhlnt")
        self.assertEqual(parsed["target_id"], "1451662346")
        self.assertEqual(parsed["status_label"], "[BANNED]")
        self.assertEqual(parsed["source"], "autoban_log")
        self.assertIn("streamboo .live", parsed["summary"])
        self.assertIn("Nachricht:", parsed["description"])

    def test_load_internal_home_autoban_events_filters_by_channel_and_since(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "twitch_autobans.log"
            log_path.write_text(
                "\n".join(
                    [
                        (
                            "2026-03-02T11:00:00+00:00\t[BANNED]\tdonnsotfd\told_user\t123\t-\t"
                            "old entry"
                        ),
                        (
                            "2026-03-04T08:00:00+00:00\t[BANNED]\tother_channel\twrong_user\t555\t-\t"
                            "must be ignored"
                        ),
                        (
                            "2026-03-04T09:00:00+00:00\t[SUSPICIOUS]\tdonnsotfd\tnot_banned\t777\t-\t"
                            "must be ignored too"
                        ),
                        (
                            "2026-03-04T10:00:00+00:00\t[BANNED]\tdonnsotfd\tsecond_ratelever18cowevdt\t"
                            "1451408577\t-\t@donnsotfd Top Viewers streamboo .com"
                        ),
                    ]
                ),
                encoding="utf-8",
            )

            with patch.object(
                _InternalHomeLogHarness,
                "_internal_home_autoban_log_candidates",
                return_value=(log_path,),
            ):
                events = self.handler._load_internal_home_autoban_events(
                    streamer_login="donnsotfd",
                    since_date="2026-03-03T00:00:00+00:00",
                )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["target_login"], "second_ratelever18cowevdt")
        self.assertEqual(events[0]["target_id"], "1451408577")
        self.assertEqual(events[0]["actor_login"], "donnsotfd")


if __name__ == "__main__":
    unittest.main()
