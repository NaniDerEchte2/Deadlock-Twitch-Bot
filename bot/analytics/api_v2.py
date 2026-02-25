"""
Analytics API v2 - Backend endpoints for the new React TypeScript dashboard.
"""

from __future__ import annotations

import ipaddress
import json
import logging
from urllib.parse import urlencode, urlsplit

from aiohttp import web

from .api_audience import _AnalyticsAudienceMixin
from .api_insights import _AnalyticsInsightsMixin
from .api_overview import _AnalyticsOverviewMixin
from .api_performance import _AnalyticsPerformanceMixin

log = logging.getLogger("TwitchStreams.AnalyticsV2")
DASHBOARD_V2_LOGIN_URL = "/twitch/auth/login?next=%2Ftwitch%2Fdashboard-v2"
DASHBOARD_V2_DISCORD_LOGIN_URL = "/twitch/auth/discord/login?next=%2Ftwitch%2Fdashboard-v2"


def _is_loopback_host(raw_value: str) -> bool:
    value = (raw_value or "").strip()
    if not value:
        return False

    token = value.split(",")[0].strip()  # noqa: S105
    if token.startswith("["):
        end = token.find("]")
        if end != -1:
            token = token[1:end]  # noqa: S105
    elif token.count(":") == 1:
        host_part, port_part = token.rsplit(":", 1)
        if port_part.isdigit():
            token = host_part  # noqa: S105

    token = token.strip().lower()  # noqa: S105
    if token == "localhost":  # noqa: S105
        return True

    try:
        return ipaddress.ip_address(token).is_loopback
    except ValueError:
        return False


def _is_localhost(request: web.Request) -> bool:
    """Allow localhost bypass only for local host+peer socket metadata."""
    host_header = request.headers.get("Host") or request.host or ""
    if not _is_loopback_host(host_header):
        return False

    remote = (request.remote or "").strip() if hasattr(request, "remote") else ""
    if remote and _is_loopback_host(remote):
        return True

    transport = getattr(request, "transport", None)
    if transport is not None:
        peer = transport.get_extra_info("peername")
        if isinstance(peer, tuple) and peer:
            peer_host = str(peer[0]).strip()
            if _is_loopback_host(peer_host):
                return True
        if isinstance(peer, str) and _is_loopback_host(peer.strip()):
            return True
    return False


class AnalyticsV2Mixin(
    _AnalyticsOverviewMixin,
    _AnalyticsAudienceMixin,
    _AnalyticsPerformanceMixin,
    _AnalyticsInsightsMixin,
):
    """Mixin providing v2 analytics API endpoints for the dashboard."""

    # Reusable SQL: filter out sessions where Twitch API returned 0 followers (missing token)
    _FOLLOWER_DELTA_SUM = """SUM(CASE WHEN s.follower_delta IS NOT NULL
         AND NOT (s.followers_end = 0 AND s.followers_start > 0)
         THEN s.follower_delta ELSE 0 END)"""
    _FOLLOWER_DELTA_AVG = """AVG(CASE WHEN s.follower_delta IS NOT NULL
         AND NOT (s.followers_end = 0 AND s.followers_start > 0)
         THEN s.follower_delta ELSE NULL END)"""

    def _classify_message(self, content: str) -> str:
        if not content:
            return "Other"
        content_lower = content.lower()

        if content.startswith("!"):
            return "Command"

        if any(
            w in content_lower
            for w in [
                "hi",
                "hello",
                "hey",
                "moin",
                "nabend",
                "guten",
                "welcome",
                "hallo",
            ]
        ):
            return "Greeting"

        if "?" in content or any(
            w in content_lower
            for w in [
                "was",
                "wo",
                "wer",
                "wie",
                "wann",
                "why",
                "how",
                "warum",
                "weshalb",
            ]
        ):
            return "Question"

        if any(
            w in content_lower
            for w in [
                "lol",
                "lmao",
                "haha",
                "gg",
                "pog",
                "lul",
                "kek",
                "xd",
                ":)",
                ":d",
                "f",
                "o7",
                "wow",
                "omg",
            ]
        ):
            return "Reaction"

        if any(
            w in content_lower
            for w in [
                "deadlock",
                "hero",
                "build",
                "skill",
                "rank",
                "elo",
                "match",
                "play",
                "game",
                "win",
                "lose",
                "mmr",
                "lane",
                "ult",
            ]
        ):
            return "Game-Related"

        if any(
            w in content_lower
            for w in [
                "follow",
                "sub",
                "prime",
                "raid",
                "host",
                "danke",
                "thanks",
                "thx",
                "discord",
                "social",
            ]
        ):
            return "Engagement"

        return "Other"

    def _get_dashboard_session(self, request: web.Request) -> dict | None:
        admin_getter = getattr(self, "_get_discord_admin_session", None)
        if callable(admin_getter):
            try:
                admin_session = admin_getter(request)
            except Exception:
                log.debug("Could not resolve Discord admin dashboard session", exc_info=True)
                admin_session = None
            if isinstance(admin_session, dict):
                session_copy = dict(admin_session)
                session_copy.setdefault("auth_type", "discord_admin")
                return session_copy

        getter = getattr(self, "_get_dashboard_auth_session", None)
        if not callable(getter):
            return None
        try:
            session = getter(request)
        except Exception:
            log.debug("Could not resolve dashboard OAuth session", exc_info=True)
            return None
        return session if isinstance(session, dict) else None

    @staticmethod
    def _normalize_dashboard_next_path(raw_path: str | None) -> str:
        fallback = "/twitch/dashboard-v2"
        candidate = (raw_path or "").strip()
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

    @staticmethod
    def _safe_internal_login_redirect(candidate: str | None) -> str:
        fallback = DASHBOARD_V2_LOGIN_URL
        value = (candidate or "").strip()
        if not value:
            return fallback
        try:
            parts = urlsplit(value)
        except Exception:
            return fallback
        if parts.scheme or parts.netloc:
            return fallback
        if not value.startswith("/"):
            return fallback
        return value

    def _get_dashboard_login_url(self, request: web.Request) -> str:
        builder = getattr(self, "_build_dashboard_login_url", None)
        if callable(builder):
            try:
                url = builder(request)
                if url:
                    return self._safe_internal_login_redirect(str(url))
            except Exception:
                log.debug("Could not build dashboard login URL via host class", exc_info=True)
        next_path = self._normalize_dashboard_next_path(
            request.rel_url.path_qs if request.rel_url else "/twitch/dashboard-v2"
        )
        return self._safe_internal_login_redirect(
            f"/twitch/auth/login?{urlencode({'next': next_path})}"
        )

    def _check_v2_auth(self, request: web.Request) -> bool:
        """Check if request is authorized for v2 API.

        Returns True if:
        - Request is from localhost (no auth needed)
        - noauth mode is enabled
        - Valid Twitch OAuth partner session exists
        - Valid partner_token or admin token is provided
        """
        # Localhost = always allowed (dev mode)
        if _is_localhost(request):
            return True

        # Check noauth mode from parent
        if getattr(self, "_noauth", False):
            return True

        # Twitch OAuth session (partner access)
        if self._get_dashboard_session(request):
            return True

        # Check tokens
        partner_token = getattr(self, "_partner_token", None)
        admin_token = getattr(self, "_token", None)

        partner_header = request.headers.get("X-Partner-Token")
        partner_query = request.query.get("partner_token")
        admin_header = request.headers.get("X-Admin-Token")
        admin_query = request.query.get("token")

        # Partner token check
        if partner_token:
            if partner_header == partner_token or partner_query == partner_token:
                return True

        # Admin token check (admin can access everything)
        if admin_token:
            if admin_header == admin_token or admin_query == admin_token:
                return True

        return False

    def _require_v2_auth(self, request: web.Request):
        """Require authentication for v2 API, but allow localhost."""
        if not self._check_v2_auth(request):
            login_url = self._get_dashboard_login_url(request)
            if request.path.startswith("/twitch/api/"):
                should_use_discord = getattr(self, "_should_use_discord_admin_login", None)
                if callable(should_use_discord) and bool(should_use_discord(request)):
                    login_url = "/twitch/auth/discord/login?next=%2Ftwitch%2Fdashboard-v2"
                else:
                    login_url = "/twitch/auth/login?next=%2Ftwitch%2Fdashboard-v2"
            payload = {
                "error": "Authentication required. Use Twitch login, partner_token, or access from localhost.",
                "loginUrl": login_url,
            }
            if request.path.startswith("/twitch/api/"):
                raise web.HTTPUnauthorized(
                    text=json.dumps(payload), content_type="application/json"
                )
            raise web.HTTPUnauthorized(text=payload["error"])

    def _get_auth_level(self, request: web.Request) -> str:
        """Get the authentication level for the request.

        Returns:
        - 'localhost': Local development access (full admin)
        - 'admin': Admin token (full access)
        - 'partner': Partner token (partner access)
        - 'none': No authentication
        """
        # Localhost = admin level
        if _is_localhost(request):
            return "localhost"

        # Check noauth mode
        if getattr(self, "_noauth", False):
            return "localhost"

        dashboard_session = self._get_dashboard_session(request)
        if dashboard_session:
            auth_type = str(dashboard_session.get("auth_type") or "").strip().lower()
            if auth_type == "discord_admin":
                return "admin"
            return "partner"

        admin_token = getattr(self, "_token", None)
        partner_token = getattr(self, "_partner_token", None)

        admin_header = request.headers.get("X-Admin-Token")
        admin_query = request.query.get("token")
        partner_header = request.headers.get("X-Partner-Token")
        partner_query = request.query.get("partner_token")

        # Admin token = full access
        if admin_token and (admin_header == admin_token or admin_query == admin_token):
            return "admin"

        # Partner token
        if partner_token and (partner_header == partner_token or partner_query == partner_token):
            return "partner"

        return "none"

    async def _api_v2_streamers(self, request: web.Request) -> web.Response:
        """Get list of streamers for dropdown."""
        self._require_v2_auth(request)

        from ..storage import pg as storage

        try:
            with storage.get_conn() as conn:
                # Detect optional partner table
                has_partner_table = True
                try:
                    conn.execute("SELECT 1 FROM twitch_streamers_partner_state LIMIT 1")
                except Exception:
                    has_partner_table = False

                # Partners (verified) - only if table exists
                partners = []
                if has_partner_table:
                    partners = conn.execute("""
                        SELECT twitch_login
                        FROM twitch_streamers_partner_state
                        WHERE is_partner_active = 1
                        ORDER BY twitch_login
                    """).fetchall()

                # Only show our verified/partner streamers when the partner view exists.
                if has_partner_table:
                    data = [{"login": r[0], "isPartner": True} for r in partners]
                else:
                    # Fallback (should not normally happen): recent streamers
                    others = conn.execute("""
                        SELECT DISTINCT s.streamer_login
                        FROM twitch_stream_sessions s
                        WHERE s.started_at >= (NOW() - INTERVAL '30 days')
                        ORDER BY s.streamer_login
                    """).fetchall()
                    data = [{"login": r[0], "isPartner": False} for r in others]

                return web.json_response(data)
        except Exception as exc:
            log.exception("Error in streamers API")
            return web.json_response({"error": str(exc)}, status=500)

    async def _api_v2_session_detail(self, request: web.Request) -> web.Response:
        """Get detailed session data."""
        self._require_v2_auth(request)

        from ..storage import pg as storage

        session_id = request.match_info.get("id", "")
        try:
            session_id = int(session_id)
        except ValueError:
            return web.json_response({"error": "Invalid session ID"}, status=400)

        try:
            with storage.get_conn() as conn:
                # Session data
                row = conn.execute(
                    """
                    SELECT
                        s.id, s.streamer_login, s.started_at, s.ended_at,
                        s.duration_seconds, s.start_viewers, s.peak_viewers, s.end_viewers,
                        s.avg_viewers, s.retention_5m, s.retention_10m, s.retention_20m,
                        s.dropoff_pct, s.unique_chatters, s.first_time_chatters,
                        s.returning_chatters, s.stream_title
                    FROM twitch_stream_sessions s
                    WHERE s.id = ?
                """,
                    [session_id],
                ).fetchone()

                if not row:
                    return web.json_response({"error": "Session not found"}, status=404)

                # Timeline
                timeline = conn.execute(
                    """
                    SELECT minutes_from_start, viewer_count
                    FROM twitch_session_viewers
                    WHERE session_id = ?
                    ORDER BY minutes_from_start
                """,
                    [session_id],
                ).fetchall()

                # Top chatters
                chatters = conn.execute(
                    """
                    SELECT chatter_login, messages
                    FROM twitch_session_chatters
                    WHERE session_id = ?
                    ORDER BY messages DESC
                    LIMIT 20
                """,
                    [session_id],
                ).fetchall()

                return web.json_response(
                    {
                        "id": row[0],
                        "streamerLogin": row[1],
                        "startedAt": row[2].isoformat() if hasattr(row[2], "isoformat") else row[2],
                        "endedAt": row[3].isoformat() if hasattr(row[3], "isoformat") else row[3],
                        "duration": row[4] or 0,
                        "startViewers": row[5] or 0,
                        "peakViewers": row[6] or 0,
                        "endViewers": row[7] or 0,
                        "avgViewers": float(row[8]) if row[8] else 0,
                        "retention5m": float(row[9]) * 100 if row[9] else 0,
                        "retention10m": float(row[10]) * 100 if row[10] else 0,
                        "retention20m": float(row[11]) * 100 if row[11] else 0,
                        "dropoffPct": float(row[12]) * 100 if row[12] else 0,
                        "uniqueChatters": row[13] or 0,
                        "firstTimeChatters": row[14] or 0,
                        "returningChatters": row[15] or 0,
                        "title": row[16] or "",
                        "timeline": [{"minute": t[0], "viewers": t[1]} for t in timeline],
                        "chatters": [{"login": c[0], "messages": c[1]} for c in chatters],
                    }
                )
        except Exception as exc:
            log.exception("Error in session detail API")
            return web.json_response({"error": str(exc)}, status=500)

    async def _api_v2_auth_status(self, request: web.Request) -> web.Response:
        """Get current authentication status and permissions."""
        auth_level = self._get_auth_level(request)
        session = self._get_dashboard_session(request) or {}
        is_authenticated = auth_level != "none"
        can_view_all_streamers = is_authenticated

        return web.json_response(
            {
                "authenticated": is_authenticated,
                "level": auth_level,
                "isAdmin": auth_level in ("localhost", "admin"),
                "isLocalhost": auth_level == "localhost",
                "canViewAllStreamers": can_view_all_streamers,
                "twitchLogin": session.get("twitch_login"),
                "displayName": session.get("display_name"),
                "permissions": {
                    "viewAllStreamers": can_view_all_streamers,
                    "viewComparison": is_authenticated,
                    "viewChatAnalytics": is_authenticated,
                    "viewOverlap": is_authenticated,
                },
            }
        )


__all__ = ["AnalyticsV2Mixin"]
