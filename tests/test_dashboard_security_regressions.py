import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from aiohttp import web

from bot.analytics.api_overview import _AnalyticsOverviewMixin
from bot.analytics.api_v2 import AnalyticsV2Mixin
from bot.dashboard.billing_mixin import _DashboardBillingMixin
from bot.dashboard.routes_mixin import _DashboardRoutesMixin
from bot.social_media.clip_manager import ClipManager
from bot.social_media.dashboard import SocialMediaDashboard


class _DummyBilling(_DashboardBillingMixin):
    def __init__(self) -> None:
        self.dashboard_session = {}
        self.discord_admin_session = {}

    def _get_dashboard_auth_session(self, request):
        return self.dashboard_session

    def _get_discord_admin_session(self, request):
        return self.discord_admin_session


class _DummyRoutes(_DashboardRoutesMixin):
    def __init__(self) -> None:
        self.dashboard_session = None
        self.discord_admin_session = None

    def _get_dashboard_auth_session(self, request):
        return self.dashboard_session

    def _get_discord_admin_session(self, request):
        return self.discord_admin_session


class _DummyRoutesApi(_DashboardRoutesMixin, AnalyticsV2Mixin):
    def _check_v2_auth(self, request):
        return True

    def _is_local_request(self, request):
        return False

    def _is_discord_admin_request(self, request):
        return False

    def _check_admin_token(self, token):
        return False

    def _get_auth_level(self, request):
        return "partner"


class _DummyV2HeaderOnlyAuth(AnalyticsV2Mixin):
    def __init__(self) -> None:
        self._noauth = False
        self._token = "admin-secret"
        self._partner_token = "partner-secret"


class _DummyOverviewAssetsAuth(_AnalyticsOverviewMixin):
    def _check_v2_auth(self, request):
        return False

    def _should_use_discord_admin_login(self, request):
        return False


class _WebhookConn:
    def execute(self, sql, params=None):
        if "INSERT INTO twitch_billing_events" in sql:
            raise RuntimeError("UNIQUE constraint failed: twitch_billing_events.stripe_event_id")
        return self

    def fetchone(self):
        return None


class _ConnCtx:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyWebhookRoutes(_DashboardRoutesMixin, _DashboardBillingMixin):
    def __init__(self) -> None:
        self._billing_stripe_webhook_secret = "whsec_test"
        self.apply_calls = 0

    def _billing_refresh_runtime_secrets(self) -> None:
        return None

    def _billing_ensure_storage_tables(self, conn) -> None:
        return None

    def _billing_import_stripe(self):
        class _Stripe:
            class Webhook:
                @staticmethod
                def construct_event(payload, sig_header, secret):
                    return {
                        "id": "evt_test",
                        "type": "invoice.payment_succeeded",
                        "data": {"object": {"id": "in_test"}},
                        "livemode": False,
                    }

        return _Stripe, None

    def _billing_apply_webhook_event(self, conn, **kwargs):
        self.apply_calls += 1
        return "applied"


class DashboardSecurityRegressionTests(unittest.IsolatedAsyncioTestCase):
    def test_billing_candidate_refs_ignore_query_tampering(self) -> None:
        handler = _DummyBilling()
        handler.dashboard_session = {"twitch_login": "owner_login", "twitch_user_id": "12345"}
        request = SimpleNamespace(query={"customer_reference": "attacker_ref"})

        refs = handler._billing_candidate_refs_for_request(request)

        self.assertEqual(refs, ["12345", "owner_login"])

    def test_billing_candidate_refs_include_discord_admin_namespace(self) -> None:
        handler = _DummyBilling()
        handler.discord_admin_session = {"user_id": 77, "display_name": "Admin"}
        request = SimpleNamespace(query={})

        refs = handler._billing_candidate_refs_for_request(request)

        self.assertEqual(refs, ["discord_admin:77"])

    def test_csrf_uses_discord_admin_session(self) -> None:
        handler = _DummyRoutes()
        handler.discord_admin_session = {"user_id": 1001}
        request = SimpleNamespace()

        token = handler._csrf_generate_token(request)

        self.assertEqual(handler.discord_admin_session.get("csrf_token"), token)
        self.assertTrue(handler._csrf_verify_token(request, token))

    async def test_abbo_cancel_get_does_not_mutate_state(self) -> None:
        class _CancelGuard(_DummyRoutes):
            def _check_v2_auth(self, request):
                return True

        handler = _CancelGuard()
        request = SimpleNamespace(method="GET")

        with self.assertRaises(web.HTTPFound) as ctx:
            await handler.abbo_cancel(request)
        self.assertEqual(ctx.exception.location, "/twitch/abbo?cancel=post_required")

    async def test_stripe_sync_products_requires_admin_scope(self) -> None:
        handler = _DummyRoutesApi()
        request = SimpleNamespace(headers={}, query={})

        response = await handler.api_billing_stripe_sync_products(request)

        self.assertEqual(response.status, 403)
        payload = json.loads(response.text)
        self.assertEqual(payload.get("error"), "admin_required")

    async def test_market_data_allows_discord_admin_session(self) -> None:
        class _MarketAuth(_DummyRoutesApi):
            def _is_discord_admin_request(self, request):
                return True

        handler = _MarketAuth()
        request = SimpleNamespace(headers={}, query={})

        with patch("bot.dashboard.routes_mixin.storage.get_conn", side_effect=RuntimeError("boom")):
            response = await handler.api_market_data(request)
        self.assertEqual(response.status, 500)

    async def test_market_dashboard_uses_html_escaping_in_templates(self) -> None:
        class _MarketPage(_DummyRoutes):
            def _require_token(self, request):
                return None

        handler = _MarketPage()
        request = SimpleNamespace()

        response = await handler.market_research(request)

        self.assertEqual(response.status, 200)
        self.assertIn("const escapeHtml", response.text)
        self.assertIn("${escapeHtml(q.content)}", response.text)
        self.assertIn("${escapeHtml(c.login)}", response.text)

    async def test_stripe_webhook_duplicate_event_returns_idempotent_success(self) -> None:
        handler = _DummyWebhookRoutes()

        class _Req:
            headers = {"Stripe-Signature": "sig_test"}

            async def read(self):
                return b'{"id":"evt_test"}'

        request = _Req()
        with patch(
            "bot.dashboard.routes_mixin.storage.get_conn",
            return_value=_ConnCtx(_WebhookConn()),
        ):
            response = await handler.api_billing_stripe_webhook(request)

        self.assertEqual(response.status, 200)
        payload = json.loads(response.text)
        self.assertEqual(payload.get("status"), "duplicate")
        self.assertEqual(handler.apply_calls, 0)

    def test_api_v2_auth_ignores_query_tokens(self) -> None:
        handler = _DummyV2HeaderOnlyAuth()
        request_query_only = SimpleNamespace(
            headers={"Host": "dashboard.example"},
            query={"token": "admin-secret", "partner_token": "partner-secret"},
            host="dashboard.example",
            remote="203.0.113.10",
            transport=None,
        )
        request_partner_header = SimpleNamespace(
            headers={"Host": "dashboard.example", "X-Partner-Token": "partner-secret"},
            query={},
            host="dashboard.example",
            remote="203.0.113.10",
            transport=None,
        )
        request_admin_header = SimpleNamespace(
            headers={"Host": "dashboard.example", "X-Admin-Token": "admin-secret"},
            query={},
            host="dashboard.example",
            remote="203.0.113.10",
            transport=None,
        )

        self.assertFalse(handler._check_v2_auth(request_query_only))
        self.assertEqual(handler._get_auth_level(request_query_only), "none")
        self.assertTrue(handler._check_v2_auth(request_partner_header))
        self.assertEqual(handler._get_auth_level(request_partner_header), "partner")
        self.assertTrue(handler._check_v2_auth(request_admin_header))
        self.assertEqual(handler._get_auth_level(request_admin_header), "admin")

    async def test_dashboard_v2_assets_redirect_when_unauthenticated(self) -> None:
        handler = _DummyOverviewAssetsAuth()
        request = SimpleNamespace(match_info={"path": "index.html"})

        with self.assertRaises(web.HTTPFound) as ctx:
            await handler._serve_dashboard_v2_assets(request)
        self.assertEqual(ctx.exception.location, "/twitch/auth/login?next=%2Ftwitch%2Fdashboard-v2")

    async def test_social_media_fetch_clips_without_twitch_api_returns_503(self) -> None:
        handler = SocialMediaDashboard(clip_manager=ClipManager(), auth_checker=lambda _req: True)

        class _Request:
            query = {}

            async def json(self):
                return {"streamer": "streamer_a", "limit": 5, "days": 3}

        response = await handler.api_fetch_clips(_Request())
        payload = json.loads(response.text)

        self.assertEqual(response.status, 503)
        self.assertEqual(payload.get("error"), "twitch_api_unavailable")


if __name__ == "__main__":
    unittest.main()
