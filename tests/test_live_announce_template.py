import unittest
from datetime import UTC, datetime

from bot.live_announce.template import (
    AnnouncementField,
    LiveAnnouncementConfig,
    build_template_context,
    is_valid_http_url,
    render_announcement_payload,
    render_placeholders,
    validate_live_announcement_config,
)


class LiveAnnouncementTemplateTests(unittest.TestCase):
    def test_placeholder_substitution_and_unknown_passthrough(self) -> None:
        context = {
            "channel": "EarlySalty",
            "viewer_count": "321",
        }
        rendered = render_placeholders(
            "{channel} hat {viewer_count} Viewer - {unknown}",
            context,
        )
        self.assertEqual(rendered, "EarlySalty hat 321 Viewer - {unknown}")

    def test_url_validation_blocks_non_http_schemes(self) -> None:
        self.assertTrue(is_valid_http_url("https://twitch.tv/earlysalty"))
        self.assertTrue(is_valid_http_url("http://localhost:8080/test"))
        self.assertFalse(is_valid_http_url("javascript:alert(1)"))
        self.assertFalse(is_valid_http_url("data:text/plain;base64,AA=="))
        self.assertFalse(is_valid_http_url("file:///etc/passwd"))

    def test_limit_validation_catches_overflow(self) -> None:
        cfg = LiveAnnouncementConfig()
        cfg.title_template = "X" * 300
        cfg.fields = [AnnouncementField(name_template="A" * 300, value_template="B" * 1200)]
        issues = validate_live_announcement_config(cfg)
        self.assertTrue(any("embed.title exceeds 256 chars" in issue for issue in issues))
        self.assertTrue(any("embed.fields[1].name exceeds 256 chars" in issue for issue in issues))
        self.assertTrue(any("embed.fields[1].value exceeds 1024 chars" in issue for issue in issues))

    def test_field_count_limit_validation(self) -> None:
        cfg = LiveAnnouncementConfig()
        cfg.fields = [
            AnnouncementField(name_template=f"Field {idx}", value_template="ok", inline=True)
            for idx in range(26)
        ]
        issues = validate_live_announcement_config(cfg)
        self.assertTrue(any("embed.fields exceeds 25 entries" in issue for issue in issues))

    def test_default_config_renders_game_in_title(self) -> None:
        cfg = LiveAnnouncementConfig()
        context = build_template_context(
            "earlysalty",
            {
                "user_name": "EarlySalty",
                "user_login": "earlysalty",
                "title": "Deadlock Ranked Session",
                "game_name": "Deadlock",
                "viewer_count": 77,
                "started_at": "2026-03-03T12:00:00+00:00",
                "url": "https://www.twitch.tv/earlysalty",
                "thumbnail_url": "https://static-cdn.jtvnw.net/previews/live_user_earlysalty-{width}x{height}.jpg",
                "language": "de",
            },
        )
        payload = render_announcement_payload(cfg, context)
        self.assertEqual(payload["embed"]["title"], "EarlySalty ist LIVE in Deadlock!")

    def test_render_announcement_payload_uses_stable_cache_buster_seed(self) -> None:
        cfg = LiveAnnouncementConfig()
        context = build_template_context(
            "earlysalty",
            {
                "user_name": "EarlySalty",
                "user_login": "earlysalty",
                "title": "Deadlock Ranked Session",
                "game_name": "Deadlock",
                "viewer_count": 77,
                "started_at": "2026-03-03T12:00:00+00:00",
                "url": "https://www.twitch.tv/earlysalty",
                "thumbnail_url": "https://static-cdn.jtvnw.net/previews/live_user_earlysalty-{width}x{height}.jpg",
                "language": "de",
            },
            now=datetime(2026, 3, 15, 12, 0, tzinfo=UTC),
        )

        payload = render_announcement_payload(
            cfg,
            context,
            now=datetime(2026, 3, 15, 12, 1, tzinfo=UTC),
            cache_buster_seed="track-1",
        )

        self.assertEqual(
            payload["embed"]["image"]["url"],
            "https://static-cdn.jtvnw.net/previews/live_user_earlysalty-1280x720.jpg?cb=90953515094fab84",
        )


if __name__ == "__main__":
    unittest.main()
