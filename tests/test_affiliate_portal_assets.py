from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from aiohttp import web

from bot.analytics.api_overview import _AnalyticsOverviewMixin


class _WebsiteAssetHarness(_AnalyticsOverviewMixin):
    pass


class AffiliatePortalAssetRouteTests(unittest.TestCase):
    def test_website_dist_asset_resolver_serves_file_and_rejects_traversal(self) -> None:
        handler = _WebsiteAssetHarness()

        with tempfile.TemporaryDirectory() as tmp_dir:
            dist_root = Path(tmp_dir)
            asset_dir = dist_root / "assets"
            asset_dir.mkdir()
            asset_path = asset_dir / "portal.js"
            asset_path.write_text("console.log('ok');", encoding="utf-8")

            with patch("bot.analytics.api_overview.WEBSITE_DIST_ROOT_PATH", dist_root):
                response = handler._resolve_website_dist_asset_response("assets/portal.js")
                self.assertIsInstance(response, web.FileResponse)

                blocked = handler._resolve_website_dist_asset_response("../secret.txt")
                self.assertEqual(blocked.status, 404)


if __name__ == "__main__":
    unittest.main()
