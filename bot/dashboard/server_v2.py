"""Embedded aiohttp app serving only the Twitch analytics dashboard v2."""

from __future__ import annotations

import ipaddress
import os
import re
import secrets
import time
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import parse_qsl, unquote, urlencode, urlparse, urlsplit, urlunsplit

from aiohttp import web

from ..analytics.api_v2 import AnalyticsV2Mixin
from ..core.constants import log
from .auth_mixin import _DashboardAuthMixin
from .billing_mixin import _DashboardBillingMixin
from .legal_mixin import _DashboardLegalMixin
from .live import DashboardLiveMixin
from .raid_mixin import _DashboardRaidMixin
from .routes_mixin import _DashboardRoutesMixin
from .stats import DashboardStatsMixin
from .templates import DashboardTemplateMixin

TWITCH_OAUTH_AUTHORIZE_URL = "https://id.twitch.tv/oauth2/authorize"
TWITCH_OAUTH_TOKEN_URL = "https://id.twitch.tv/oauth2/token"  # noqa: S105
TWITCH_HELIX_USERS_URL = "https://api.twitch.tv/helix/users"
DISCORD_API_BASE_URL = "https://discord.com/api/v10"
TWITCH_DASHBOARDS_LOGIN_URL = "/twitch/auth/login?next=%2Ftwitch%2Fdashboards"
TWITCH_DASHBOARD_V2_LOGIN_URL = "/twitch/auth/login?next=%2Ftwitch%2Fdashboard-v2"
TWITCH_DASHBOARDS_DISCORD_LOGIN_URL = "/twitch/auth/discord/login?next=%2Ftwitch%2Fdashboards"
LOGIN_RE = re.compile(r"^[A-Za-z0-9_]{3,25}$")
DEFAULT_DASHBOARD_MODERATOR_ROLE_ID = 1337518124647579661
KEYRING_SERVICE_NAME = "DeadlockBot"
TWITCH_ADMIN_PUBLIC_URL = (
    os.getenv("TWITCH_ADMIN_PUBLIC_URL")
    or os.getenv("MASTER_DASHBOARD_PUBLIC_URL")
    or "https://admin.earlysalty.de"
).strip()
TWITCH_ADMIN_DISCORD_REDIRECT_URI = (
    os.getenv("TWITCH_ADMIN_DISCORD_REDIRECT_URI")
    or f"{TWITCH_ADMIN_PUBLIC_URL.rstrip('/')}/twitch/auth/discord/callback"
).strip()


class DashboardV2Server(
    _DashboardAuthMixin,
    _DashboardRaidMixin,
    _DashboardLegalMixin,
    _DashboardBillingMixin,
    _DashboardRoutesMixin,
    DashboardLiveMixin,
    DashboardStatsMixin,
    DashboardTemplateMixin,
    AnalyticsV2Mixin,
):
    """Minimal dashboard server exposing only v2 routes and APIs."""

    def __init__(
        self,
        *,
        app_token: str | None,
        noauth: bool,
        partner_token: str | None,
        oauth_client_id: str | None = None,
        oauth_client_secret: str | None = None,
        oauth_redirect_uri: str | None = None,
        session_ttl_seconds: int = 6 * 3600,
        legacy_stats_url: str | None = None,
        add_cb: Callable[[str, bool], Awaitable[str]] | None = None,
        remove_cb: Callable[[str], Awaitable[str]] | None = None,
        list_cb: Callable[[], Awaitable[list[dict]]] | None = None,
        stats_cb: Callable[..., Awaitable[dict]] | None = None,
        verify_cb: Callable[[str, str], Awaitable[str]] | None = None,
        archive_cb: Callable[[str, str], Awaitable[str]] | None = None,
        discord_flag_cb: Callable[[str, bool], Awaitable[str]] | None = None,
        discord_profile_cb: Callable[[str, str | None, str | None, bool], Awaitable[str]]
        | None = None,
        raid_history_cb: Callable[..., Awaitable[list[dict]]] | None = None,
        raid_bot: Any | None = None,
        reload_cb: Callable[[], Awaitable[str]] | None = None,
    ) -> None:
        self._token = app_token
        self._noauth = noauth
        self._partner_token = partner_token
        self._oauth_client_id = oauth_client_id
        self._oauth_client_secret = oauth_client_secret
        self._oauth_redirect_uri = oauth_redirect_uri
        self._billing_stripe_publishable_key = self._load_secret_value(
            "STRIPE_PUBLISHABLE_KEY",
            "TWITCH_BILLING_STRIPE_PUBLISHABLE_KEY",
        )
        self._billing_stripe_secret_key = self._load_secret_value(
            "STRIPE_SECRET_KEY",
            "TWITCH_BILLING_STRIPE_SECRET_KEY",
        )
        self._billing_stripe_webhook_secret = self._load_secret_value(
            "STRIPE_WEBHOOK_SECRET",
            "TWITCH_BILLING_STRIPE_WEBHOOK_SECRET",
        )
        self._billing_checkout_success_url = self._load_secret_value(
            "STRIPE_CHECKOUT_SUCCESS_URL",
            "TWITCH_BILLING_CHECKOUT_SUCCESS_URL",
        )
        self._billing_checkout_cancel_url = self._load_secret_value(
            "STRIPE_CHECKOUT_CANCEL_URL",
            "TWITCH_BILLING_CHECKOUT_CANCEL_URL",
        )
        self._billing_stripe_price_map_raw = self._load_secret_value(
            "STRIPE_PRICE_ID_MAP",
            "TWITCH_BILLING_STRIPE_PRICE_ID_MAP",
        )
        self._billing_stripe_product_map_raw = self._load_secret_value(
            "STRIPE_PRODUCT_ID_MAP",
            "TWITCH_BILLING_STRIPE_PRODUCT_ID_MAP",
        )
        self._session_ttl_seconds = max(6 * 3600, int(session_ttl_seconds or 6 * 3600))
        self._legacy_stats_url = (legacy_stats_url or "").strip() or None
        self._reload_cb = reload_cb
        self._session_cookie_name = "twitch_dash_session"
        self._oauth_states: dict[str, dict[str, Any]] = {}
        self._auth_sessions: dict[str, dict[str, Any]] = {}
        self._oauth_state_ttl_seconds = 600
        self._rate_limits: dict[str, list[float]] = {}
        self._add = add_cb if callable(add_cb) else self._empty_add
        self._remove = remove_cb if callable(remove_cb) else self._empty_remove
        self._list = list_cb if callable(list_cb) else self._empty_list
        self._stats = stats_cb if callable(stats_cb) else self._empty_stats
        self._verify = verify_cb if callable(verify_cb) else self._empty_verify
        self._archive = archive_cb if callable(archive_cb) else self._empty_archive
        self._discord_flag = (
            discord_flag_cb if callable(discord_flag_cb) else self._empty_discord_flag
        )
        self._discord_profile = discord_profile_cb
        self._raid_history_cb = (
            raid_history_cb if callable(raid_history_cb) else self._empty_raid_history
        )
        self._raid_bot = raid_bot
        self._redirect_uri = str(
            getattr(getattr(raid_bot, "auth_manager", None), "redirect_uri", "") or ""
        ).strip()
        self._master_dashboard_href = "/admin"
        keyring_client_id = self._read_keyring_secret("DISCORD_OAUTH_CLIENT_ID")
        discord_bot = None
        auth_manager = getattr(raid_bot, "auth_manager", None) if raid_bot else None
        if auth_manager is not None:
            discord_bot = getattr(auth_manager, "_discord_bot", None)
        if discord_bot is None and raid_bot is not None:
            discord_bot = getattr(raid_bot, "_discord_bot", None)
        app_client_id = str(getattr(discord_bot, "application_id", "") or "").strip()
        self._discord_admin_client_id = (keyring_client_id or app_client_id).strip()
        self._discord_admin_client_secret = self._read_keyring_secret(
            "DISCORD_OAUTH_CLIENT_SECRET"
        ).strip()
        self._discord_admin_redirect_uri = TWITCH_ADMIN_DISCORD_REDIRECT_URI
        self._discord_admin_enabled = True
        owner_user_id_raw = self._load_secret_value(
            "TWITCH_ADMIN_OWNER_USER_ID",
            "DISCORD_ADMIN_OWNER_USER_ID",
        )
        self._discord_admin_owner_user_id = self._parse_optional_int(owner_user_id_raw)
        if self._discord_admin_owner_user_id:
            log.warning(
                "Discord admin owner override enabled for user_id=%s. "
                "Use only for explicit recovery scenarios.",
                self._discord_admin_owner_user_id,
            )
        self._discord_admin_moderator_role_id = DEFAULT_DASHBOARD_MODERATOR_ROLE_ID
        self._discord_admin_guild_ids: tuple[int, ...] = ()
        self._discord_admin_cookie_name = "twitch_admin_session"
        self._discord_admin_session_ttl = self._session_ttl_seconds
        self._discord_admin_state_ttl = 600
        self._discord_admin_oauth_states: dict[str, dict[str, Any]] = {}
        self._discord_admin_sessions: dict[str, dict[str, Any]] = {}
        self._discord_admin_required = self._discord_admin_enabled and bool(
            self._discord_admin_client_id
            and self._discord_admin_client_secret
            and self._discord_admin_redirect_uri
        )
        if self._discord_admin_enabled and not self._discord_admin_required:
            log.error(
                "Twitch Admin Discord OAuth ist unvollständig (Client ID/Secret/Redirect fehlen). "
                "Admin-Zugriff bleibt deaktiviert, bis die Konfiguration vollständig ist."
            )

    async def _empty_add(self, _: str, __: bool) -> str:
        return "Add-Funktion ist aktuell nicht verfügbar"

    async def _empty_remove(self, _: str) -> str:
        return "Remove-Funktion ist aktuell nicht verfügbar"

    async def _empty_list(self) -> list[dict]:
        return []

    async def _empty_stats(self, **_: Any) -> dict:
        return {"tracked": {}, "category": {}}

    async def _empty_verify(self, _: str, __: str) -> str:
        return "Verify-Funktion ist aktuell nicht verfügbar"

    async def _empty_archive(self, _: str, __: str) -> str:
        return "Archive-Funktion ist aktuell nicht verfügbar"

    async def _empty_discord_flag(self, _: str, __: bool) -> str:
        return "Discord-Flag-Funktion ist aktuell nicht verfügbar"

    async def _empty_raid_history(self, **_: Any) -> list[dict]:
        return []

    @classmethod
    def _load_secret_value(cls, *keys: str) -> str:
        for raw_key in keys:
            key = str(raw_key or "").strip()
            if not key:
                continue
            value = cls._read_keyring_secret(key)
            if value:
                return value
        for raw_key in keys:
            key = str(raw_key or "").strip()
            if not key:
                continue
            value = str(os.getenv(key) or "").strip()
            if value:
                return value
        return ""

    @staticmethod
    def _read_keyring_secret(key: str) -> str:
        secret_key = (key or "").strip()
        if not secret_key:
            return ""
        try:
            import keyring
        except Exception:
            return ""
        try:
            value = keyring.get_password(KEYRING_SERVICE_NAME, secret_key)
            if not value:
                value = keyring.get_password(f"{secret_key}@{KEYRING_SERVICE_NAME}", secret_key)
        except Exception:
            return ""
        return str(value or "").strip()

    @staticmethod
    def _write_keyring_secret(key: str, value: str | None) -> bool:
        secret_key = (key or "").strip()
        if not secret_key:
            return False
        try:
            import keyring
        except Exception:
            return False
        try:
            keyring.set_password(KEYRING_SERVICE_NAME, secret_key, str(value or ""))
        except Exception:
            return False
        return True

    @staticmethod
    def _parse_optional_int(value: Any) -> int | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        try:
            parsed = int(raw)
        except (TypeError, ValueError):
            return None
        return parsed if parsed > 0 else None

    def _check_admin_token(self, token: str | None) -> bool:
        if self._noauth:
            return True
        if not token or not self._token:
            return False
        try:
            return secrets.compare_digest(str(token), str(self._token))
        except Exception:
            return False

    @staticmethod
    def _host_without_port(raw: str | None) -> str:
        if not raw:
            return ""
        host = raw.split(",")[0].strip()
        if not host:
            return ""
        if host.startswith("["):
            end = host.find("]")
            if end != -1:
                host = host[1:end]
        elif ":" in host:
            host = host.split(":", 1)[0]
        return host.lower()

    @staticmethod
    def _is_loopback_host(raw: str | None) -> bool:
        host = DashboardV2Server._host_without_port(raw)
        if not host:
            return False
        if host == "localhost":
            return True
        try:
            return ipaddress.ip_address(host).is_loopback
        except ValueError:
            return False

    @staticmethod
    def _peer_host(request: web.Request) -> str:
        remote = (request.remote or "").strip() if hasattr(request, "remote") else ""
        if remote:
            return remote
        transport = getattr(request, "transport", None)
        if transport is None:
            return ""
        peer = transport.get_extra_info("peername")
        if isinstance(peer, tuple) and peer:
            return str(peer[0]).strip()
        if isinstance(peer, str):
            return peer.strip()
        return ""

    def _effective_client_host(self, request: web.Request, peer_host: str) -> str:
        # Deliberately ignore forwarded headers. Without an explicit trusted-proxy
        # allowlist, user-controlled forwarding headers can be spoofed.
        del request
        return self._host_without_port(peer_host)

    def _rate_limit_key(self, request: web.Request) -> str:
        peer = self._peer_host(request)
        resolved = self._effective_client_host(request, peer)
        return resolved or self._host_without_port(peer) or "unknown"

    def _is_local_request(self, request: web.Request) -> bool:
        host_header = request.headers.get("Host") or request.host or ""
        request_host = self._host_without_port(host_header)
        if not self._is_loopback_host(request_host):
            return False

        peer_host = self._peer_host(request)
        if not peer_host:
            return False
        client_host = self._effective_client_host(request, peer_host)
        return self._is_loopback_host(client_host)

    @staticmethod
    def _normalize_login(value: str) -> str | None:
        if not value:
            return None
        s = unquote(value).strip()
        if not s:
            return None
        if s.startswith("@"):
            s = s[1:].strip()
        if "twitch.tv" in s or "://" in s or "/" in s:
            if "://" not in s:
                s = f"https://{s}"
            try:
                parts = urlsplit(s)
                segs = [p for p in (parts.path or "").split("/") if p]
                if segs:
                    s = segs[0]
            except Exception:
                return None
        s = s.strip().lower()
        if LOGIN_RE.match(s):
            return s
        return None

    @staticmethod
    def _sanitize_log_value(value: Any) -> str:
        text = "" if value is None else str(value)
        return text.replace("\r", "\\r").replace("\n", "\\n")

    @staticmethod
    def _normalize_discord_admin_next_path(raw: str | None) -> str:
        fallback = "/twitch/admin"
        candidate = (raw or "").strip()
        if not candidate:
            return fallback
        try:
            parts = urlsplit(candidate)
        except Exception:
            return fallback
        if parts.scheme or parts.netloc:
            return fallback
        if not candidate.startswith("/") or not candidate.startswith("/twitch"):
            return fallback
        return candidate

    def _build_discord_admin_login_url(
        self, request: web.Request, *, next_path: str | None = None
    ) -> str:
        if not self._discord_admin_required:
            return "/twitch/dashboards"
        normalized_next = self._normalize_discord_admin_next_path(
            next_path or (request.rel_url.path_qs if request.rel_url else "/twitch/admin")
        )
        return f"/twitch/auth/discord/login?{urlencode({'next': normalized_next})}"

    @staticmethod
    def _safe_discord_admin_login_redirect(raw_url: str | None) -> str:
        fallback = "/twitch/auth/discord/login"
        candidate = (raw_url or "").strip()
        if not candidate:
            return fallback
        try:
            parsed = urlsplit(candidate)
        except Exception:
            return fallback
        if parsed.scheme or parsed.netloc:
            return fallback
        if not (parsed.path or "").startswith("/twitch/auth/discord/login"):
            return fallback
        return candidate

    @staticmethod
    def _canonical_discord_admin_post_login_path(raw: str | None) -> str:
        normalized = DashboardV2Server._normalize_discord_admin_next_path(raw)
        normalized_path = (urlsplit(normalized).path or "").rstrip("/") or "/"
        if normalized_path == "/twitch/abo":
            return "/twitch/abbo"
        if normalized_path == "/twitch/abbo":
            return "/twitch/abbo"
        if normalized_path == "/twitch/abos":
            return "/twitch/abbo"
        if normalized_path == "/twitch/abbo/stripe-settings":
            return "/twitch/abbo/stripe-settings"
        if normalized_path == "/twitch/abbo/rechnungen":
            return "/twitch/abbo/rechnungen"
        if normalized_path == "/twitch/abbo/rechnung":
            return "/twitch/abbo/rechnung"
        if normalized_path == "/twitch/abbo/kündigen":
            return "/twitch/abbo/kündigen"
        if normalized_path == "/twitch/dashboads":
            return "/twitch/dashboards"
        if normalized_path == "/twitch/dashboards":
            return "/twitch/dashboards"
        return "/twitch/admin"

    def _normalized_discord_admin_redirect_uri(self) -> str | None:
        raw = (self._discord_admin_redirect_uri or "").strip()
        if not raw:
            return None
        candidate = raw if "://" in raw else f"https://{raw}"
        try:
            parsed = urlparse(candidate)
        except Exception:
            return None
        scheme = (parsed.scheme or "").strip().lower()
        host = (parsed.hostname or "").strip().lower()
        if scheme not in {"http", "https"}:
            return None
        if scheme == "http" and host not in {"127.0.0.1", "localhost", "::1"}:
            return None
        if parsed.username or parsed.password or not parsed.netloc:
            return None
        if (parsed.path or "").rstrip("/") != "/twitch/auth/discord/callback":
            return None
        return urlunsplit((scheme, parsed.netloc, "/twitch/auth/discord/callback", "", ""))

    def _require_token(self, request: web.Request) -> None:
        admin_only_prefixes = (
            "/twitch/admin",
            "/twitch/live",
            "/twitch/add_any",
            "/twitch/add_url",
            "/twitch/add_login",
            "/twitch/add_streamer",
            "/twitch/remove",
            "/twitch/verify",
            "/twitch/archive",
            "/twitch/discord_flag",
            "/twitch/discord_link",
            "/twitch/raid/auth",
            "/twitch/raid/requirements",
            "/twitch/raid/history",
            "/twitch/raid/analytics",
            "/twitch/reload",
            "/twitch/market",
            "/twitch/stats",
        )
        if request.path.startswith(admin_only_prefixes):
            if not self._discord_admin_required:
                raise web.HTTPServiceUnavailable(
                    text=(
                        "Discord Admin OAuth ist nicht konfiguriert. "
                        "Admin-Zugriff ist bis zur vollständigen Konfiguration deaktiviert."
                    )
                )
            if self._is_discord_admin_request(request):
                return
            login_url = self._build_discord_admin_login_url(
                request,
                next_path=request.rel_url.path_qs if request.rel_url else request.path,
            )
            safe_login_url = self._safe_discord_admin_login_redirect(login_url)
            if request.method in {"GET", "HEAD"}:
                raise web.HTTPFound(safe_login_url)
            raise web.HTTPUnauthorized(
                text="Discord admin authentication required",
                headers={"X-Auth-Login": safe_login_url},
            )

        if self._check_v2_auth(request):
            return
        raise web.HTTPUnauthorized(text="missing or invalid authentication")

    def _require_partner_token(self, request: web.Request) -> None:
        if self._check_v2_auth(request):
            return
        if self._noauth:
            return
        partner_header = request.headers.get("X-Partner-Token")
        admin_header = request.headers.get("X-Admin-Token")

        if self._partner_token:
            if partner_header == self._partner_token:
                return
            if admin_header == self._token:
                return
            raise web.HTTPUnauthorized(text="missing or invalid partner token")
        raise web.HTTPUnauthorized(text="missing or invalid partner token")

    def _redirect_location(
        self,
        request: web.Request,
        *,
        ok: str | None = None,
        err: str | None = None,
        default_path: str = "/twitch/stats",
    ) -> str:
        if default_path == "/twitch/stats":
            admin_action_prefixes = (
                "/twitch/admin",
                "/twitch/live",
                "/twitch/add_any",
                "/twitch/add_url",
                "/twitch/add_login",
                "/twitch/add_streamer",
                "/twitch/remove",
                "/twitch/verify",
                "/twitch/archive",
                "/twitch/discord_flag",
                "/twitch/raid/auth",
                "/twitch/raid/requirements",
                "/twitch/raid/history",
                "/twitch/raid/analytics",
            )
            if request.path.startswith(admin_action_prefixes):
                default_path = "/twitch/admin"

        referer = request.headers.get("Referer")
        if referer:
            try:
                parts = urlsplit(referer)
                if parts.path:
                    params = dict(parse_qsl(parts.query, keep_blank_values=True))
                    params.pop("ok", None)
                    params.pop("err", None)
                    if ok:
                        params["ok"] = ok
                    if err:
                        params["err"] = err
                    return urlunsplit(("", "", parts.path, urlencode(params), "")) or default_path
            except Exception:
                log.debug("Could not construct redirect from referer", exc_info=True)

        params: dict[str, str] = {}
        if ok:
            params["ok"] = ok
        if err:
            params["err"] = err
        if params:
            return f"{default_path}?{urlencode(params)}"
        return default_path

    def _check_rate_limit(
        self,
        request: web.Request,
        *,
        max_requests: int = 10,
        window_seconds: float = 60.0,
    ) -> bool:
        """Sliding-window rate limiter per peer IP. Returns True if allowed."""
        key = self._rate_limit_key(request)
        now = time.time()
        hits = self._rate_limits.get(key, [])
        hits = [t for t in hits if now - t < window_seconds]
        if len(hits) >= max_requests:
            self._rate_limits[key] = hits
            return False
        hits.append(now)
        self._rate_limits[key] = hits
        # Prevent unbounded growth – clear when tracking too many distinct IPs
        if len(self._rate_limits) > 1000:
            self._rate_limits.clear()
        return True

    def _is_secure_request(self, request: web.Request) -> bool:
        # Only trust X-Forwarded-Proto when the TCP peer is a loopback address
        # (i.e. a trusted local proxy).  Accepting this header from arbitrary
        # clients would allow spoofing the "secure" flag and mis-marking cookies.
        peer = self._peer_host(request)
        if self._is_loopback_host(peer):
            forwarded_proto = (
                (request.headers.get("X-Forwarded-Proto") or "").split(",")[0].strip().lower()
            )
            if forwarded_proto:
                return forwarded_proto == "https"
        return bool(request.secure)

    def _resolve_legacy_stats_url(self) -> str:
        # The legacy stats dashboard is now always served locally.
        return "/twitch/stats"

    def _should_use_discord_admin_login(self, request: web.Request) -> bool:
        if not self._discord_admin_required:
            return False
        admin_context_prefixes = (
            "/twitch/admin",
            "/twitch/live",
            "/twitch/add_any",
            "/twitch/add_url",
            "/twitch/add_login",
            "/twitch/add_streamer",
            "/twitch/remove",
            "/twitch/verify",
            "/twitch/archive",
            "/twitch/discord_flag",
            "/twitch/discord_link",
            "/twitch/raid/auth",
            "/twitch/raid/requirements",
            "/twitch/raid/history",
            "/twitch/raid/analytics",
            "/twitch/reload",
            "/twitch/market",
        )
        return request.path.startswith(admin_context_prefixes)

    async def _do_add(self, raw: str) -> str:
        login = self._normalize_login(raw)
        if not login:
            raise web.HTTPBadRequest(text="invalid twitch login or url")
        msg = await self._add(login, False)
        return msg or "added"


@web.middleware
async def _security_headers_middleware(request: web.Request, handler: Any) -> web.StreamResponse:
    """Attach minimal security headers to every response."""
    response = await handler(request)
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-XSS-Protection", "1; mode=block")
    return response


def build_v2_app(
    *,
    noauth: bool,
    token: str | None,
    partner_token: str | None = None,
    oauth_client_id: str | None = None,
    oauth_client_secret: str | None = None,
    oauth_redirect_uri: str | None = None,
    session_ttl_seconds: int = 6 * 3600,
    legacy_stats_url: str | None = None,
    add_cb: Callable[[str, bool], Awaitable[str]] | None = None,
    remove_cb: Callable[[str], Awaitable[str]] | None = None,
    list_cb: Callable[[], Awaitable[list[dict]]] | None = None,
    stats_cb: Callable[..., Awaitable[dict]] | None = None,
    verify_cb: Callable[[str, str], Awaitable[str]] | None = None,
    archive_cb: Callable[[str, str], Awaitable[str]] | None = None,
    discord_flag_cb: Callable[[str, bool], Awaitable[str]] | None = None,
    discord_profile_cb: Callable[[str, str | None, str | None, bool], Awaitable[str]] | None = None,
    raid_history_cb: Callable[..., Awaitable[list[dict]]] | None = None,
    raid_bot: Any | None = None,
    reload_cb: Callable[[], Awaitable[str]] | None = None,
    eventsub_webhook_handler: Any | None = None,
) -> web.Application:
    app = web.Application(middlewares=[_security_headers_middleware])
    DashboardV2Server(
        app_token=token,
        noauth=noauth,
        partner_token=partner_token,
        oauth_client_id=oauth_client_id,
        oauth_client_secret=oauth_client_secret,
        oauth_redirect_uri=oauth_redirect_uri,
        session_ttl_seconds=session_ttl_seconds,
        legacy_stats_url=legacy_stats_url,
        add_cb=add_cb,
        remove_cb=remove_cb,
        list_cb=list_cb,
        stats_cb=stats_cb,
        verify_cb=verify_cb,
        archive_cb=archive_cb,
        discord_flag_cb=discord_flag_cb,
        discord_profile_cb=discord_profile_cb,
        raid_history_cb=raid_history_cb,
        raid_bot=raid_bot,
        reload_cb=reload_cb,
    ).attach(app)
    if eventsub_webhook_handler is not None:
        app.router.add_post(
            "/twitch/eventsub/callback",
            eventsub_webhook_handler.handle_request,
        )
    return app


__all__ = ["DashboardV2Server", "build_v2_app"]
