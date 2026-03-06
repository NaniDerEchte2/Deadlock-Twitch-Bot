"""
Analytics API v2 - Backend endpoints for the new React TypeScript dashboard.
"""

from __future__ import annotations

import ipaddress
import json
import logging
import os
from collections import deque
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

from aiohttp import web

from ..core.chat_bots import build_known_chat_bot_not_in_clause
from .api_ai import _AnalyticsAIMixin
from .api_audience import _AnalyticsAudienceMixin
from .api_experimental import _AnalyticsExperimentalMixin
from .api_insights import _AnalyticsInsightsMixin
from .api_overview import _AnalyticsOverviewMixin
from .api_performance import _AnalyticsPerformanceMixin
from .api_chat_deep import _AnalyticsChatDeepMixin
from .api_raids import _AnalyticsRaidsMixin
from .api_viewers import _AnalyticsViewersMixin

log = logging.getLogger("TwitchStreams.AnalyticsV2")
INTERNAL_HOME_LOGIN_URL = "/twitch/auth/login?next=%2Ftwitch%2Fdashboard"
INTERNAL_HOME_DISCORD_CONNECT_URL = "/twitch/auth/discord/login?next=%2Ftwitch%2Fdashboard"
DASHBOARD_V2_LOGIN_URL = "/twitch/auth/login?next=%2Ftwitch%2Fdashboard-v2"

# Twitch logins that receive admin-level access (same as Discord admin / localhost)
_TWITCH_ADMIN_LOGINS: frozenset[str] = frozenset({"earlysalty"})
DASHBOARD_V2_DISCORD_LOGIN_URL = "/twitch/auth/discord/login?next=%2Ftwitch%2Fdashboard-v2"
_INTERNAL_HOME_DEFAULT_DAYS = 30
_INTERNAL_HOME_BAN_REASON_KEYWORDS: tuple[str, ...] = (
    "bot",
    "spam",
    "scam",
    "phish",
    "link",
    "promo",
    "werbung",
)
_INTERNAL_HOME_REQUIRED_SCOPES: tuple[str, ...] = (
    "channel:manage:raids",
    "moderator:read:followers",
    "moderator:manage:banned_users",
    "moderator:manage:chat_messages",
    "moderator:read:chatters",
)
_INTERNAL_HOME_CHANGELOG_MAX_ENTRIES = 20
_INTERNAL_HOME_CHANGELOG_TITLE_MAX_LENGTH = 160
_INTERNAL_HOME_CHANGELOG_CONTENT_MAX_LENGTH = 4000
_INTERNAL_HOME_RATE_LIMIT_MAX_REQUESTS = 30
_INTERNAL_HOME_RATE_LIMIT_WINDOW_SECONDS = 60.0
_INTERNAL_HOME_CHANGELOG_WRITE_RATE_LIMIT_MAX_REQUESTS = 10
_INTERNAL_HOME_CHANGELOG_WRITE_RATE_LIMIT_WINDOW_SECONDS = 60.0
_INTERNAL_HOME_SERVICE_WARNING_LOG_FILENAME = "twitch_service_warnings.log"
_INTERNAL_HOME_SERVICE_WARNING_MAX_SCAN_LINES = 5000
_INTERNAL_HOME_SERVICE_WARNING_MAX_EVENTS = 20
_INTERNAL_HOME_AUTOBAN_LOG_FILENAME = "twitch_autobans.log"
_INTERNAL_HOME_AUTOBAN_MAX_SCAN_LINES = 5000
_INTERNAL_HOME_AUTOBAN_MAX_EVENTS = 20
_INTERNAL_HOME_ACTIVITY_MAX_EVENTS = 10
_INTERNAL_HOME_ACTIVITY_PRIORITY_TYPES: frozenset[str] = frozenset(
    {"ban", "ban_keyword_hit", "unban", "service_pitch_warning"}
)


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
    _AnalyticsRaidsMixin,
    _AnalyticsViewersMixin,
    _AnalyticsChatDeepMixin,
    _AnalyticsExperimentalMixin,
    _AnalyticsAIMixin,
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

    @staticmethod
    def _normalize_host_header(raw_value: str | None) -> str:
        value = str(raw_value or "").strip()
        if not value:
            return ""
        token = value.split(",")[0].strip()
        if not token:
            return ""
        candidate = token if "://" in token else f"//{token}"
        try:
            parsed = urlsplit(candidate)
        except Exception:
            return ""
        return str(parsed.hostname or "").strip().lower()

    @classmethod
    def _host_from_origin_like(cls, raw_value: str | None) -> str:
        value = str(raw_value or "").strip()
        if not value:
            return ""
        candidate = value if "://" in value else f"https://{value}"
        try:
            parsed = urlsplit(candidate)
        except Exception:
            return ""
        host = str(parsed.hostname or "").strip().lower()
        if host:
            return host
        return cls._normalize_host_header(value)

    def _configured_admin_dashboard_host(self) -> str:
        candidates = (
            os.getenv("TWITCH_ADMIN_PUBLIC_URL"),
            os.getenv("MASTER_DASHBOARD_PUBLIC_URL"),
            getattr(self, "_discord_admin_redirect_uri", ""),
            "https://admin.earlysalty.de",
        )
        for candidate in candidates:
            host = self._host_from_origin_like(candidate)
            if host:
                return host
        return "admin.earlysalty.de"

    def _is_admin_dashboard_host_request(self, request: web.Request) -> bool:
        request_host = self._normalize_host_header(request.headers.get("Host") or request.host or "")
        if not request_host:
            return False
        return request_host == self._configured_admin_dashboard_host()

    def _check_v2_auth(self, request: web.Request) -> bool:
        """Check if request is authorized for v2 API.

        Returns True if:
        - Request is from localhost (no auth needed)
        - noauth mode is enabled
        - Valid Twitch OAuth partner session exists
        - Valid partner_token or admin token is provided
        """
        auth_level = self._get_auth_level(request)
        if self._is_admin_dashboard_host_request(request):
            return auth_level in ("localhost", "admin")
        return auth_level != "none"

    def _require_v2_auth(self, request: web.Request):
        """Require authentication for v2 API, but allow localhost."""
        if not self._check_v2_auth(request):
            auth_level = self._get_auth_level(request)
            on_admin_host = self._is_admin_dashboard_host_request(request)
            login_url = self._get_dashboard_login_url(request)
            if request.path.startswith("/twitch/api/"):
                should_use_discord = getattr(self, "_should_use_discord_admin_login", None)
                if callable(should_use_discord) and bool(should_use_discord(request)):
                    login_url = "/twitch/auth/discord/login?next=%2Ftwitch%2Fdashboard-v2"
                else:
                    oauth_ready_checker = getattr(self, "_is_twitch_oauth_ready", None)
                    oauth_ready = True
                    if callable(oauth_ready_checker):
                        try:
                            oauth_ready = bool(oauth_ready_checker())
                        except Exception:
                            oauth_ready = True
                    if not oauth_ready and bool(getattr(self, "_discord_admin_required", False)):
                        login_url = "/twitch/auth/discord/login?next=%2Ftwitch%2Fdashboard-v2"
                    else:
                        login_url = "/twitch/auth/login?next=%2Ftwitch%2Fdashboard-v2"
            if on_admin_host and auth_level not in ("none", "localhost", "admin"):
                payload = {
                    "error": "admin_required",
                    "required": "admin",
                    "auth_level": auth_level,
                    "message": "Admin access required on admin dashboard host.",
                }
                if request.path.startswith("/twitch/api/"):
                    raise web.HTTPForbidden(
                        text=json.dumps(payload), content_type="application/json"
                    )
                raise web.HTTPForbidden(text=payload["message"])
            payload = {
                "error": "Authentication required. Use Twitch login, partner_token, or access from localhost.",
                "loginUrl": login_url,
            }
            if request.path.startswith("/twitch/api/"):
                raise web.HTTPUnauthorized(
                    text=json.dumps(payload), content_type="application/json"
                )
            raise web.HTTPUnauthorized(text=payload["error"])

    def _check_v2_admin_auth(self, request: web.Request) -> bool:
        """Check if request has admin-level API access."""
        return self._get_auth_level(request) in ("localhost", "admin")

    def _require_v2_admin_api(self, request: web.Request) -> web.Response | None:
        """Return JSON error response when request lacks admin privileges."""
        auth_level = self._get_auth_level(request)
        if auth_level in ("localhost", "admin"):
            return None
        if auth_level == "none":
            return web.json_response(
                {
                    "error": "auth_required",
                    "required": "admin",
                },
                status=401,
            )
        return web.json_response(
            {
                "error": "admin_required",
                "required": "admin",
                "auth_level": auth_level,
            },
            status=403,
        )

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
            twitch_login = str(dashboard_session.get("twitch_login") or "").strip().lower()
            if twitch_login in _TWITCH_ADMIN_LOGINS:
                return "admin"
            return "partner"

        admin_token = getattr(self, "_token", None)
        partner_token = getattr(self, "_partner_token", None)

        admin_header = request.headers.get("X-Admin-Token")
        partner_header = request.headers.get("X-Partner-Token")

        # Admin token = full access
        if admin_token and admin_header == admin_token:
            return "admin"

        # Partner token
        if partner_token and partner_header == partner_token:
            return "partner"

        return "none"

    @staticmethod
    def _internal_home_keyword_clause(column_expr: str) -> tuple[str, list[str]]:
        if not _INTERNAL_HOME_BAN_REASON_KEYWORDS:
            return "1=0", []
        like_parts = [f"LOWER(COALESCE({column_expr}, '')) LIKE ?" for _ in _INTERNAL_HOME_BAN_REASON_KEYWORDS]
        like_params = [f"%{keyword}%" for keyword in _INTERNAL_HOME_BAN_REASON_KEYWORDS]
        return f"({' OR '.join(like_parts)})", like_params

    @staticmethod
    def _internal_home_iso(value: Any) -> str:
        if value is None:
            return ""
        if hasattr(value, "isoformat"):
            return str(value.isoformat())
        return str(value)

    @staticmethod
    def _internal_home_parse_iso_datetime(value: Any) -> datetime | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        normalized = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    @staticmethod
    def _internal_home_parse_prefixed_int(token: str, prefix: str) -> int | None:
        normalized = str(token or "").strip()
        if not normalized.lower().startswith(prefix.lower()):
            return None
        raw_value = normalized[len(prefix):].strip()
        if raw_value in {"", "-"}:
            return None
        try:
            return int(raw_value)
        except ValueError:
            return None

    @classmethod
    def _internal_home_service_warning_log_candidates(cls) -> tuple[Path, ...]:
        local_path = Path("logs") / _INTERNAL_HOME_SERVICE_WARNING_LOG_FILENAME
        project_root = Path(__file__).resolve().parents[2]
        project_path = project_root / "logs" / _INTERNAL_HOME_SERVICE_WARNING_LOG_FILENAME
        if project_path == local_path:
            return (local_path,)
        return local_path, project_path

    @classmethod
    def _internal_home_autoban_log_candidates(cls) -> tuple[Path, ...]:
        local_path = Path("logs") / _INTERNAL_HOME_AUTOBAN_LOG_FILENAME
        project_root = Path(__file__).resolve().parents[2]
        project_path = project_root / "logs" / _INTERNAL_HOME_AUTOBAN_LOG_FILENAME
        # Twitch-Bot läuft mit CWD = Deadlock/ (Geschwister-Verzeichnis)
        sibling_path = project_root.parent / "Deadlock" / "logs" / _INTERNAL_HOME_AUTOBAN_LOG_FILENAME
        candidates = dict.fromkeys([local_path, project_path, sibling_path])
        return tuple(candidates)

    @staticmethod
    def _internal_home_service_warning_title(severity_code: str) -> str:
        normalized = str(severity_code or "").strip().upper()
        if normalized == "ESCALATED_TIMEOUT":
            return "Service-Pitch eskaliert (Timeout)"
        if normalized == "WARNING_STRONG":
            return "Service-Pitch Warnung (stark)"
        if normalized == "WARNING_PUBLIC":
            return "Service-Pitch Warnung"
        if normalized == "HINT":
            return "Service-Pitch Hinweis"
        return "Service-Pitch Ereignis"

    @staticmethod
    def _internal_home_service_warning_severity(severity_code: str) -> str:
        normalized = str(severity_code or "").strip().upper()
        if normalized == "ESCALATED_TIMEOUT":
            return "critical"
        if normalized == "WARNING_STRONG":
            return "warning"
        if normalized == "WARNING_PUBLIC":
            return "warning"
        if normalized == "HINT":
            return "info"
        return "warning"

    def _parse_internal_home_service_warning_line(
        self,
        raw_line: str,
    ) -> dict[str, Any] | None:
        line = str(raw_line or "").strip()
        if not line:
            return None
        parts = line.split("\t", 10)
        if len(parts) < 10:
            return None
        if len(parts) == 10:
            parts.append("")

        timestamp_raw = parts[0].strip()
        severity_code = parts[1].strip().upper()
        channel_login = parts[2].strip().lower()
        chatter_login = parts[3].strip().lower()
        chatter_id = parts[4].strip()
        age_days = self._internal_home_parse_prefixed_int(parts[5], "age_days=")
        follower_count = self._internal_home_parse_prefixed_int(parts[6], "followers=")
        score = self._internal_home_parse_prefixed_int(parts[7], "score=")
        message_count = self._internal_home_parse_prefixed_int(parts[8], "msgs=")
        reasons_text = parts[9].strip()
        content_text = parts[10].strip()

        parsed_ts = self._internal_home_parse_iso_datetime(timestamp_raw)
        timestamp = (
            parsed_ts.isoformat()
            if parsed_ts is not None
            else self._internal_home_iso(timestamp_raw)
        )

        metric_parts: list[str] = []
        if score is not None:
            metric_parts.append(f"Score {score}")
        if message_count is not None:
            metric_parts.append(f"Msgs {message_count}")
        if age_days is not None and age_days >= 0:
            metric_parts.append(f"Account {age_days}d")
        if follower_count is not None:
            metric_parts.append(f"Followers {follower_count}")
        metric = " | ".join(metric_parts)

        reason = "" if reasons_text in {"", "-"} else reasons_text
        description_parts: list[str] = []
        if reason:
            description_parts.append(f"Signale: {reason}")
        if content_text:
            description_parts.append(f"Nachricht: {content_text}")
        description = " | ".join(description_parts)

        chatter_label = f"@{chatter_login}" if chatter_login and chatter_login != "-" else "Unbekannt"
        summary_parts: list[str] = [chatter_label]
        if metric:
            summary_parts.append(metric)
        summary = " | ".join(summary_parts)

        return {
            "type": "service_pitch_warning",
            "event_type": "service_pitch_warning",
            "timestamp": timestamp,
            "target_login": "" if chatter_login == "-" else chatter_login,
            "target_id": "" if chatter_id == "-" else chatter_id,
            "actor_login": channel_login,
            "status_label": f"[{severity_code or 'WARNING'}]",
            "title": self._internal_home_service_warning_title(severity_code),
            "summary": summary,
            "description": description,
            "reason": reason,
            "metric": metric,
            "severity": self._internal_home_service_warning_severity(severity_code),
            "source": "service_warning_log",
        }

    def _load_internal_home_service_warning_events(
        self,
        *,
        streamer_login: str,
        since_date: str,
        max_events: int = _INTERNAL_HOME_SERVICE_WARNING_MAX_EVENTS,
    ) -> list[dict[str, Any]]:
        channel_key = str(streamer_login or "").strip().lower()
        if not channel_key:
            return []

        log_path: Path | None = None
        for candidate in self._internal_home_service_warning_log_candidates():
            try:
                if candidate.exists():
                    log_path = candidate
                    break
            except OSError:
                continue
        if log_path is None:
            return []

        since_dt = self._internal_home_parse_iso_datetime(since_date)
        recent_lines: deque[str] = deque(maxlen=_INTERNAL_HOME_SERVICE_WARNING_MAX_SCAN_LINES)
        try:
            with log_path.open("r", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    if line:
                        recent_lines.append(line.rstrip("\n"))
        except OSError:
            log.debug(
                "Could not read service warning log for internal-home: %s",
                log_path,
                exc_info=True,
            )
            return []

        events: list[dict[str, Any]] = []
        for raw_line in reversed(recent_lines):
            parsed = self._parse_internal_home_service_warning_line(raw_line)
            if not parsed:
                continue

            severity_label = str(parsed.get("status_label") or "").upper()
            if "HINT" in severity_label:
                continue

            event_channel = str(parsed.get("actor_login") or "").strip().lower()
            if event_channel != channel_key:
                continue

            if since_dt is not None:
                event_dt = self._internal_home_parse_iso_datetime(parsed.get("timestamp"))
                if event_dt is None or event_dt < since_dt:
                    continue

            events.append(parsed)
            if len(events) >= int(max_events):
                break

        return events

    def _parse_internal_home_autoban_line(
        self,
        raw_line: str,
    ) -> dict[str, Any] | None:
        line = str(raw_line or "").strip()
        if not line:
            return None
        parts = line.split("\t", 6)
        if len(parts) < 6:
            return None
        if len(parts) == 6:
            parts.append("")

        timestamp_raw = parts[0].strip()
        status_raw = parts[1].strip()
        channel_login = parts[2].strip().lower()
        chatter_login = parts[3].strip().lower()
        chatter_id = parts[4].strip()
        reason_text = parts[5].strip()
        content_text = parts[6].strip()

        normalized_status = status_raw.strip().strip("[]").upper()
        if normalized_status != "BANNED":
            return None

        parsed_ts = self._internal_home_parse_iso_datetime(timestamp_raw)
        timestamp = (
            parsed_ts.isoformat()
            if parsed_ts is not None
            else self._internal_home_iso(timestamp_raw)
        )

        reason = "" if reason_text in {"", "-"} else reason_text
        content = "" if content_text in {"", "-"} else content_text
        target_login = "" if chatter_login in {"", "-"} else chatter_login
        target_id = "" if chatter_id in {"", "-"} else chatter_id
        status_label = (
            status_raw
            if status_raw.startswith("[") and status_raw.endswith("]")
            else "[BANNED]"
        )

        summary_parts: list[str] = []
        if reason:
            summary_parts.append(reason)
        if content:
            summary_parts.append(content)
        if channel_login:
            summary_parts.append(f"Mod: @{channel_login}")
        summary = " | ".join(summary_parts) if summary_parts else "Ban ausgeführt"

        description_parts: list[str] = []
        if reason:
            description_parts.append(f"Signale: {reason}")
        if content:
            description_parts.append(f"Nachricht: {content}")
        description = " | ".join(description_parts)

        return {
            "type": "ban",
            "event_type": "ban",
            "timestamp": timestamp,
            "target_login": target_login,
            "target_id": target_id,
            "moderator_login": channel_login,
            "actor_login": channel_login,
            "reason": reason,
            "status_label": status_label,
            "title": (
                f"Ban gegen @{target_login}"
                if target_login
                else "Ban ausgeführt"
            ),
            "summary": summary,
            "description": description,
            "severity": "warning",
            "source": "autoban_log",
        }

    def _load_internal_home_autoban_events(
        self,
        *,
        streamer_login: str,
        since_date: str,
        max_events: int = _INTERNAL_HOME_AUTOBAN_MAX_EVENTS,
    ) -> list[dict[str, Any]]:
        channel_key = str(streamer_login or "").strip().lower()
        if not channel_key:
            return []

        log_path: Path | None = None
        for candidate in self._internal_home_autoban_log_candidates():
            try:
                if candidate.exists():
                    log_path = candidate
                    break
            except OSError:
                continue
        if log_path is None:
            return []

        since_dt = self._internal_home_parse_iso_datetime(since_date)
        recent_lines: deque[str] = deque(maxlen=_INTERNAL_HOME_AUTOBAN_MAX_SCAN_LINES)
        try:
            with log_path.open("r", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    if line:
                        recent_lines.append(line.rstrip("\n"))
        except OSError:
            log.debug(
                "Could not read autoban log for internal-home: %s",
                log_path,
                exc_info=True,
            )
            return []

        events: list[dict[str, Any]] = []
        for raw_line in reversed(recent_lines):
            parsed = self._parse_internal_home_autoban_line(raw_line)
            if not parsed:
                continue

            event_channel = str(
                parsed.get("actor_login") or parsed.get("moderator_login") or ""
            ).strip().lower()
            if event_channel != channel_key:
                continue

            if since_dt is not None:
                event_dt = self._internal_home_parse_iso_datetime(parsed.get("timestamp"))
                if event_dt is None or event_dt < since_dt:
                    continue

            events.append(parsed)
            if len(events) >= int(max_events):
                break

        return events

    @staticmethod
    def _internal_home_entry_date_iso(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        text = str(value).strip()
        return text[:10] if text else ""

    @staticmethod
    def _empty_internal_home_changelog_payload(*, can_write: bool) -> dict[str, Any]:
        return {
            "entries": [],
            "can_write": bool(can_write),
            "max_entries": _INTERNAL_HOME_CHANGELOG_MAX_ENTRIES,
        }

    def _ensure_internal_home_changelog_storage(self, conn: Any) -> None:
        if getattr(self, "_internal_home_changelog_storage_ready", False):
            return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS internal_home_changelog (
                id BIGSERIAL PRIMARY KEY,
                entry_date DATE NOT NULL DEFAULT CURRENT_DATE,
                title TEXT NOT NULL DEFAULT '',
                content TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            "ALTER TABLE internal_home_changelog ADD COLUMN IF NOT EXISTS entry_date DATE"
        )
        conn.execute(
            "ALTER TABLE internal_home_changelog ADD COLUMN IF NOT EXISTS title TEXT"
        )
        conn.execute(
            "ALTER TABLE internal_home_changelog ADD COLUMN IF NOT EXISTS content TEXT"
        )
        conn.execute(
            "ALTER TABLE internal_home_changelog ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ"
        )
        conn.execute(
            """
            UPDATE internal_home_changelog
            SET
                created_at = COALESCE(created_at, CURRENT_TIMESTAMP),
                entry_date = COALESCE(
                    entry_date,
                    CAST(COALESCE(created_at, CURRENT_TIMESTAMP) AS DATE),
                    CURRENT_DATE
                ),
                title = COALESCE(title, ''),
                content = COALESCE(content, '')
            WHERE created_at IS NULL
               OR entry_date IS NULL
               OR title IS NULL
               OR content IS NULL
            """
        )
        conn.execute(
            "ALTER TABLE internal_home_changelog ALTER COLUMN entry_date SET DEFAULT CURRENT_DATE"
        )
        conn.execute(
            "ALTER TABLE internal_home_changelog ALTER COLUMN title SET DEFAULT ''"
        )
        conn.execute(
            "ALTER TABLE internal_home_changelog ALTER COLUMN content SET DEFAULT ''"
        )
        conn.execute(
            """
            ALTER TABLE internal_home_changelog
            ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_internal_home_changelog_order
            ON internal_home_changelog (entry_date DESC, created_at DESC, id DESC)
            """
        )
        self._internal_home_changelog_storage_ready = True

    def _serialize_internal_home_changelog_entry(self, row: Any) -> dict[str, Any]:
        if not row:
            return {
                "id": None,
                "entry_date": None,
                "title": "",
                "content": "",
                "created_at": None,
            }

        raw_id = row[0]
        try:
            entry_id = int(raw_id) if raw_id is not None else None
        except Exception:
            entry_id = raw_id

        entry_date = self._internal_home_entry_date_iso(row[1]) or None
        created_at = self._internal_home_iso(row[4]) or None
        return {
            "id": entry_id,
            "entry_date": entry_date,
            "title": str(row[2] or ""),
            "content": str(row[3] or ""),
            "created_at": created_at,
        }

    def _fetch_internal_home_changelog_entries(self, conn: Any) -> list[dict[str, Any]]:
        rows = conn.execute(
            """
            SELECT id, entry_date, title, content, created_at
            FROM internal_home_changelog
            ORDER BY entry_date DESC, created_at DESC, id DESC
            LIMIT ?
            """,
            [_INTERNAL_HOME_CHANGELOG_MAX_ENTRIES],
        ).fetchall()
        return [self._serialize_internal_home_changelog_entry(row) for row in rows]

    def _get_internal_home_changelog_payload(self, *, can_write: bool) -> dict[str, Any]:
        from ..storage import pg as storage

        with storage.get_conn() as conn:
            self._ensure_internal_home_changelog_storage(conn)
            payload = self._empty_internal_home_changelog_payload(can_write=can_write)
            payload["entries"] = self._fetch_internal_home_changelog_entries(conn)
            return payload

    def _create_internal_home_changelog_entry(
        self,
        *,
        title: str,
        content: str,
        entry_date: date,
    ) -> dict[str, Any]:
        from ..storage import pg as storage

        with storage.get_conn() as conn:
            self._ensure_internal_home_changelog_storage(conn)
            row = conn.execute(
                """
                INSERT INTO internal_home_changelog (entry_date, title, content)
                VALUES (?, ?, ?)
                RETURNING id, entry_date, title, content, created_at
                """,
                [entry_date, title, content],
            ).fetchone()
            conn.execute(
                """
                DELETE FROM internal_home_changelog
                WHERE id IN (
                    SELECT id
                    FROM internal_home_changelog
                    ORDER BY entry_date DESC, created_at DESC, id DESC
                    OFFSET ?
                )
                """,
                [_INTERNAL_HOME_CHANGELOG_MAX_ENTRIES],
            )
        return self._serialize_internal_home_changelog_entry(row)

    def _raise_internal_home_unauthorized(self, code: str, message: str) -> None:
        payload = {
            "error": code,
            "message": message,
            "loginUrl": self._safe_internal_login_redirect(INTERNAL_HOME_LOGIN_URL),
        }
        raise web.HTTPUnauthorized(text=json.dumps(payload), content_type="application/json")

    @staticmethod
    def _normalize_origin_value(raw_value: str | None) -> str:
        value = str(raw_value or "").strip()
        if not value or value.lower() == "null":
            return ""
        try:
            parts = urlsplit(value)
        except Exception:
            return ""
        if not parts.scheme or not parts.netloc:
            return ""
        return f"{parts.scheme.lower()}://{parts.netloc.lower()}"

    def _request_origin(self, request: web.Request) -> str:
        host = str(request.headers.get("Host") or request.host or "").strip()
        if not host:
            return ""

        is_secure = bool(getattr(request, "secure", False))
        secure_getter = getattr(self, "_is_secure_request", None)
        if callable(secure_getter):
            try:
                is_secure = bool(secure_getter(request))
            except Exception:
                log.debug("Could not resolve request security state", exc_info=True)

        scheme = "https" if is_secure else "http"
        return f"{scheme}://{host.lower()}"

    def _has_dashboard_bound_session(self, request: web.Request) -> bool:
        try:
            return isinstance(self._get_dashboard_session(request), dict)
        except Exception:
            log.debug("Could not resolve dashboard session state", exc_info=True)
            return False

    def _is_same_origin_session_request(self, request: web.Request) -> bool:
        expected_origin = self._request_origin(request)
        if not expected_origin:
            return False

        header_origin = self._normalize_origin_value(request.headers.get("Origin"))
        if header_origin:
            return header_origin == expected_origin

        referer_origin = self._normalize_origin_value(request.headers.get("Referer"))
        if referer_origin:
            return referer_origin == expected_origin

        return False

    def _internal_home_rate_limit_response(
        self,
        request: web.Request,
        *,
        max_requests: int,
        window_seconds: float,
    ) -> web.Response | None:
        check_rate_limit = getattr(self, "_check_rate_limit", None)
        if not callable(check_rate_limit):
            return None

        allowed = True
        try:
            allowed = bool(
                check_rate_limit(
                    request,
                    max_requests=max_requests,
                    window_seconds=window_seconds,
                )
            )
        except TypeError:
            try:
                allowed = bool(check_rate_limit(request))
            except Exception:
                log.debug("internal-home rate-limit hook failed", exc_info=True)
        except Exception:
            log.debug("internal-home rate-limit hook failed", exc_info=True)

        if allowed:
            return None

        retry_after = str(int(window_seconds)) if window_seconds >= 1 else "1"
        return web.json_response(
            {
                "error": "rate_limit_exceeded",
                "message": "Too many requests. Please retry shortly.",
            },
            status=429,
            headers={"Retry-After": retry_after},
        )

    @staticmethod
    def _strip_internal_home_target_ids(payload: dict[str, Any]) -> None:
        for section_key in ("bot_impact", "bot_activity"):
            section = payload.get(section_key)
            if not isinstance(section, dict):
                continue
            events = section.get("events")
            if not isinstance(events, list):
                continue
            for event in events:
                if isinstance(event, dict):
                    event.pop("target_id", None)

    def _normalize_internal_home_streamer_override(self, raw_value: str | None) -> str:
        candidate = str(raw_value or "").strip()
        if not candidate:
            return ""
        normalizer = getattr(self, "_normalize_login", None)
        if callable(normalizer):
            try:
                normalized = normalizer(candidate)
            except Exception:
                log.debug("Could not normalize internal-home streamer override", exc_info=True)
                normalized = None
            if normalized:
                return str(normalized).strip().lower()
            return ""
        return candidate.lower()

    def _resolve_internal_home_identity(
        self,
        request: web.Request,
        *,
        streamer_override: str | None = None,
    ) -> tuple[str, str, str]:
        session = self._get_dashboard_session(request)
        if not isinstance(session, dict):
            self._raise_internal_home_unauthorized(
                "auth_required",
                "A valid dashboard session is required.",
            )
        assert isinstance(session, dict)

        requested_streamer = self._normalize_internal_home_streamer_override(streamer_override)
        is_admin = False
        try:
            is_admin = self._check_v2_admin_auth(request)
        except Exception:
            log.debug("Could not resolve admin state for internal-home override", exc_info=True)

        twitch_login = str(session.get("twitch_login") or "").strip().lower()
        twitch_user_id = str(session.get("twitch_user_id") or "").strip()
        display_name = str(session.get("display_name") or twitch_login).strip() or twitch_login

        if requested_streamer:
            if not is_admin and requested_streamer != twitch_login:
                raise web.HTTPForbidden(
                    text=json.dumps(
                        {
                            "error": "streamer_override_requires_admin",
                            "message": "Only admin sessions may view another streamer's profile.",
                        }
                    ),
                    content_type="application/json",
                )
            return requested_streamer, "", requested_streamer

        if not twitch_login and not twitch_user_id:
            self._raise_internal_home_unauthorized(
                "streamer_session_required",
                "The dashboard session must be bound to a Twitch streamer account.",
            )
        return twitch_login, twitch_user_id, display_name

    def _build_internal_home_payload(
        self,
        *,
        twitch_login: str,
        twitch_user_id: str,
        display_name: str,
        days: int,
    ) -> dict[str, Any]:
        from ..storage import pg as storage

        generated_at = datetime.now(UTC).isoformat()
        since_date = (datetime.now(UTC) - timedelta(days=days)).isoformat()
        resolved_login = twitch_login
        resolved_user_id = twitch_user_id

        streams_count = 0
        avg_viewers = 0.0
        follower_delta = 0
        bot_bans_keyword_count = 0
        granted_scopes: list[str] = []
        missing_scopes: list[str] = []
        oauth_status = "missing"
        discord_connected = False
        recent_streams: list[dict[str, Any]] = []
        raid_events: list[dict[str, Any]] = []
        autoban_events: list[dict[str, Any]] = []
        service_warning_events: list[dict[str, Any]] = []
        bot_events: list[dict[str, Any]] = []

        with storage.get_conn() as conn:
            identity_row = conn.execute(
                """
                SELECT
                    LOWER(twitch_login),
                    COALESCE(twitch_user_id, ''),
                    CASE
                        WHEN COALESCE(is_on_discord, 0) = 1 THEN 1
                        WHEN COALESCE(discord_user_id, '') <> '' THEN 1
                        ELSE 0
                    END AS discord_connected
                FROM twitch_streamers
                WHERE (COALESCE(?, '') != '' AND LOWER(twitch_login) = ?)
                   OR (COALESCE(?, '') != '' AND twitch_user_id = ?)
                ORDER BY CASE
                    WHEN (COALESCE(?, '') != '' AND LOWER(twitch_login) = ?) THEN 0
                    ELSE 1
                END
                LIMIT 1
                """,
                [
                    twitch_login,
                    twitch_login,
                    twitch_user_id,
                    twitch_user_id,
                    twitch_login,
                    twitch_login,
                ],
            ).fetchone()
            if identity_row:
                resolved_login = str(identity_row[0] or resolved_login or "").strip().lower()
                resolved_user_id = str(identity_row[1] or resolved_user_id or "").strip()
                discord_connected = bool(identity_row[2])

            if resolved_login:
                oauth_row = conn.execute(
                    """
                    SELECT scopes
                    FROM twitch_raid_auth
                    WHERE LOWER(twitch_login) = ?
                    LIMIT 1
                    """,
                    [resolved_login],
                ).fetchone()
                if oauth_row:
                    scope_set = {
                        str(scope or "").strip().lower()
                        for scope in str(oauth_row[0] or "").split()
                        if str(scope or "").strip()
                    }
                    granted_scopes = sorted(scope_set)
                    missing_scopes = [
                        scope for scope in _INTERNAL_HOME_REQUIRED_SCOPES if scope not in scope_set
                    ]
                    oauth_status = "connected" if not missing_scopes else "partial"
                else:
                    missing_scopes = list(_INTERNAL_HOME_REQUIRED_SCOPES)
                    oauth_status = "missing"

            if resolved_login:
                kpi_row = conn.execute(
                    """
                    SELECT
                        COUNT(*) AS streams_count,
                        COALESCE(AVG(s.avg_viewers), 0) AS avg_viewers,
                        COALESCE(SUM(CASE
                            WHEN s.follower_delta IS NOT NULL
                                 AND NOT (s.followers_end = 0 AND s.followers_start > 0)
                            THEN s.follower_delta
                            ELSE 0
                        END), 0) AS follower_delta
                    FROM twitch_stream_sessions s
                    WHERE s.started_at >= ?
                      AND s.ended_at IS NOT NULL
                      AND LOWER(s.streamer_login) = ?
                    """,
                    [since_date, resolved_login],
                ).fetchone()
                if kpi_row:
                    streams_count = int(kpi_row[0] or 0)
                    avg_viewers = float(kpi_row[1] or 0.0)
                    follower_delta = int(kpi_row[2] or 0)

                recent_rows = conn.execute(
                    """
                    SELECT
                        s.started_at,
                        s.ended_at,
                        s.duration_seconds,
                        s.avg_viewers,
                        s.peak_viewers,
                        CASE
                            WHEN s.follower_delta IS NOT NULL
                                 AND NOT (s.followers_end = 0 AND s.followers_start > 0)
                            THEN s.follower_delta
                            ELSE 0
                        END AS follower_delta,
                        s.stream_title
                    FROM twitch_stream_sessions s
                    WHERE s.started_at >= ?
                      AND s.ended_at IS NOT NULL
                      AND LOWER(s.streamer_login) = ?
                    ORDER BY s.started_at DESC
                    LIMIT 5
                    """,
                    [since_date, resolved_login],
                ).fetchall()
                for row in recent_rows:
                    started_iso = self._internal_home_iso(row[0])
                    recent_streams.append(
                        {
                            "date": started_iso[:10] if started_iso else "",
                            "started_at": started_iso,
                            "ended_at": self._internal_home_iso(row[1]),
                            "duration_seconds": int(row[2] or 0),
                            "avg_viewers": round(float(row[3] or 0.0), 1),
                            "peak_viewers": int(row[4] or 0),
                            "follower_delta": int(row[5] or 0),
                            "title": str(row[6] or ""),
                        }
                    )

            ban_clause, ban_params = self._internal_home_keyword_clause("b.reason")
            if resolved_user_id:
                ban_count_row = conn.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM twitch_ban_events b
                    WHERE b.received_at >= ?
                      AND b.twitch_user_id = ?
                      AND LOWER(COALESCE(b.event_type, '')) = 'ban'
                      AND {ban_clause}
                    """,
                    [since_date, resolved_user_id, *ban_params],
                ).fetchone()
                bot_bans_keyword_count = int((ban_count_row[0] if ban_count_row else 0) or 0)

                ban_event_rows = conn.execute(
                    """
                    SELECT b.received_at, b.target_login, b.target_id, b.moderator_login, b.reason
                    FROM twitch_ban_events b
                    WHERE b.received_at >= ?
                      AND b.twitch_user_id = ?
                      AND LOWER(COALESCE(b.event_type, '')) = 'ban'
                    ORDER BY b.received_at DESC
                    LIMIT 20
                    """,
                    [since_date, resolved_user_id],
                ).fetchall()
                for row in ban_event_rows:
                    target_login = str(row[1] or "").strip()
                    moderator_login = str(row[3] or "").strip()
                    reason_text = str(row[4] or "").strip()
                    summary_parts: list[str] = []
                    if reason_text:
                        summary_parts.append(reason_text)
                    if moderator_login:
                        summary_parts.append(f"Mod: @{moderator_login}")
                    bot_events.append(
                        {
                            "type": "ban",
                            "event_type": "ban",
                            "timestamp": self._internal_home_iso(row[0]),
                            "target_login": target_login,
                            "target_id": str(row[2] or ""),
                            "moderator_login": moderator_login,
                            "reason": reason_text,
                            "status_label": "[BANNED]",
                            "title": (
                                f"Ban gegen @{target_login}"
                                if target_login
                                else "Ban ausgeführt"
                            ),
                            "summary": (
                                " | ".join(summary_parts)
                                if summary_parts
                                else "Ban ausgeführt"
                            ),
                            "severity": "warning",
                        }
                    )

            if resolved_user_id or resolved_login:
                raid_rows = conn.execute(
                    """
                    SELECT
                        r.executed_at,
                        r.to_broadcaster_login,
                        r.to_broadcaster_id,
                        r.viewer_count,
                        r.reason,
                        r.success
                    FROM twitch_raid_history r
                    WHERE r.executed_at >= ?
                      AND (
                          (COALESCE(?, '') != '' AND r.from_broadcaster_id = ?)
                          OR (COALESCE(?, '') != '' AND LOWER(r.from_broadcaster_login) = ?)
                      )
                    ORDER BY r.executed_at DESC
                    LIMIT 10
                    """,
                    [since_date, resolved_user_id, resolved_user_id, resolved_login, resolved_login],
                ).fetchall()
                for row in raid_rows:
                    raid_event = {
                        "type": "raid_history",
                        "timestamp": self._internal_home_iso(row[0]),
                        "target_login": str(row[1] or ""),
                        "target_id": str(row[2] or ""),
                        "viewer_count": int(row[3] or 0),
                        "reason": str(row[4] or ""),
                        "success": bool(row[5]) if row[5] is not None else True,
                        "status_label": "[RAID]",
                    }
                    raid_events.append(raid_event)
                    bot_events.append(raid_event)

        if resolved_login:
            autoban_events = self._load_internal_home_autoban_events(
                streamer_login=resolved_login,
                since_date=since_date,
            )
            bot_events.extend(autoban_events)

            service_warning_events = self._load_internal_home_service_warning_events(
                streamer_login=resolved_login,
                since_date=since_date,
            )
            bot_events.extend(service_warning_events)

        bot_events.sort(key=lambda item: item.get("timestamp") or "", reverse=True)
        prioritized_events: list[dict[str, Any]] = []
        regular_events: list[dict[str, Any]] = []
        for event in bot_events:
            event_type = str(event.get("event_type") or event.get("type") or "").strip().lower()
            if event_type in _INTERNAL_HOME_ACTIVITY_PRIORITY_TYPES:
                prioritized_events.append(event)
            else:
                regular_events.append(event)
        bot_events = (prioritized_events + regular_events)[:_INTERNAL_HOME_ACTIVITY_MAX_EVENTS]

        overview_query = {"days": days}
        if resolved_login:
            overview_query["streamer"] = resolved_login

        oauth_reconnect_url = "/twitch/raid/auth" if resolved_login else INTERNAL_HOME_LOGIN_URL

        return {
            "profile": {
                "twitch_login": resolved_login,
                "twitch_user_id": resolved_user_id,
                "display_name": display_name or resolved_login,
            },
            "status": {
                "authenticated": True,
                "streamer_bound": bool(resolved_login or resolved_user_id),
                "period_days": days,
                "oauth": {
                    "connected": oauth_status != "missing",
                    "status": oauth_status,
                    "granted_scopes": granted_scopes,
                    "missing_scopes": missing_scopes,
                    "reconnect_url": oauth_reconnect_url,
                    "profile_url": "/twitch/dashboard",
                    "last_checked_at": generated_at,
                },
                "discord": {
                    "connected": discord_connected,
                    "status": "connected" if discord_connected else "missing",
                    "connect_url": INTERNAL_HOME_DISCORD_CONNECT_URL,
                    "last_checked_at": generated_at,
                },
                "raid_status": {
                    "state": "active",
                    "read_only": True,
                },
            },
            "kpis": {
                "streams_count": streams_count,
                "avg_viewers": round(avg_viewers, 1),
                "follower_delta": follower_delta,
                "bot_bans_keyword_count": bot_bans_keyword_count,
            },
            "recent_streams": recent_streams,
            "bot_impact": {
                "events": bot_events,
                "summary": {
                    "ban_keyword_hits_30d": bot_bans_keyword_count,
                    "recent_raid_events": len(raid_events),
                    "recent_autoban_events": len(autoban_events),
                    "recent_service_warnings": len(service_warning_events),
                },
                "note": (
                    "Raid automation is active in read-only mode. "
                    "Bot impact events are informational and no write action is triggered here."
                ),
            },
            "bot_activity": {
                "events": bot_events,
            },
            "links": {
                "dashboard": "/twitch/dashboard",
                "dashboard_v2": "/twitch/dashboard-v2",
                "raid_history": "/twitch/raid/history",
                "raid_requirements": "/twitch/raid/requirements",
                "billing": "/twitch/abbo",
                "oauth_reconnect": oauth_reconnect_url,
                "profile_status": "/twitch/dashboard",
                "discord_connect": INTERNAL_HOME_DISCORD_CONNECT_URL,
                "internal_home_api": f"/twitch/api/v2/internal-home?{urlencode({'days': days})}",
                "overview_api": f"/twitch/api/v2/overview?{urlencode(overview_query)}",
            },
            "generated_at": generated_at,
        }

    async def _api_v2_internal_home(self, request: web.Request) -> web.Response:
        """Bundled internal dashboard payload for the logged-in streamer."""
        rate_limit_response = self._internal_home_rate_limit_response(
            request,
            max_requests=_INTERNAL_HOME_RATE_LIMIT_MAX_REQUESTS,
            window_seconds=_INTERNAL_HOME_RATE_LIMIT_WINDOW_SECONDS,
        )
        if rate_limit_response is not None:
            return rate_limit_response

        raw_days = (request.query.get("days") or "").strip() if request.query else ""
        try:
            days = int(raw_days) if raw_days else _INTERNAL_HOME_DEFAULT_DAYS
        except ValueError:
            days = _INTERNAL_HOME_DEFAULT_DAYS
        days = min(max(days, 1), 365)
        requested_streamer = (
            str(request.query.get("streamer") or "").strip() if request.query else ""
        )

        try:
            twitch_login, twitch_user_id, display_name = self._resolve_internal_home_identity(
                request,
                streamer_override=requested_streamer,
            )
            payload = self._build_internal_home_payload(
                twitch_login=twitch_login,
                twitch_user_id=twitch_user_id,
                display_name=display_name,
                days=days,
            )
            try:
                has_admin_access = self._check_v2_admin_auth(request)
            except Exception:
                log.debug(
                    "Could not resolve internal-home changelog write permissions",
                    exc_info=True,
                )
                has_admin_access = False
            if not has_admin_access:
                self._strip_internal_home_target_ids(payload)
            try:
                payload["changelog"] = self._get_internal_home_changelog_payload(
                    can_write=has_admin_access
                )
            except Exception:
                log.exception("Error loading internal-home changelog")
                payload["changelog"] = self._empty_internal_home_changelog_payload(
                    can_write=has_admin_access
                )
            return web.json_response(payload)
        except web.HTTPException:
            raise
        except Exception:
            log.exception("Error in internal-home API")
            return web.json_response({"error": "internal_home_failed"}, status=500)

    async def _api_v2_internal_home_changelog_create(self, request: web.Request) -> web.Response:
        """Create a new internal-home changelog entry (admin/localhost only)."""
        err = self._require_v2_admin_api(request)
        if err is not None:
            return err

        if self._has_dashboard_bound_session(request) and not self._is_same_origin_session_request(
            request
        ):
            return web.json_response(
                {
                    "error": "csrf_origin_invalid",
                    "message": (
                        "Session-based POST requests must use same-origin Origin or Referer."
                    ),
                },
                status=403,
            )

        rate_limit_response = self._internal_home_rate_limit_response(
            request,
            max_requests=_INTERNAL_HOME_CHANGELOG_WRITE_RATE_LIMIT_MAX_REQUESTS,
            window_seconds=_INTERNAL_HOME_CHANGELOG_WRITE_RATE_LIMIT_WINDOW_SECONDS,
        )
        if rate_limit_response is not None:
            return rate_limit_response

        try:
            body = await request.json()
        except Exception:
            return web.json_response(
                {
                    "error": "invalid_json",
                    "message": "Request body must be valid JSON.",
                },
                status=400,
            )

        if not isinstance(body, dict):
            return web.json_response(
                {
                    "error": "invalid_json",
                    "message": "Request body must be a JSON object.",
                },
                status=400,
            )

        title = str(body.get("title") or "").strip()
        content = str(body.get("content") or "").strip()
        raw_entry_date = body.get("entry_date")

        if not content:
            return web.json_response(
                {
                    "error": "content_required",
                    "message": "content is required.",
                },
                status=400,
            )
        if len(title) > _INTERNAL_HOME_CHANGELOG_TITLE_MAX_LENGTH:
            return web.json_response(
                {
                    "error": "title_too_long",
                    "message": (
                        f"title must be {_INTERNAL_HOME_CHANGELOG_TITLE_MAX_LENGTH} "
                        "characters or fewer."
                    ),
                },
                status=400,
            )
        if len(content) > _INTERNAL_HOME_CHANGELOG_CONTENT_MAX_LENGTH:
            return web.json_response(
                {
                    "error": "content_too_long",
                    "message": (
                        f"content must be {_INTERNAL_HOME_CHANGELOG_CONTENT_MAX_LENGTH} "
                        "characters or fewer."
                    ),
                },
                status=400,
            )

        try:
            entry_date = (
                date.fromisoformat(str(raw_entry_date).strip())
                if raw_entry_date not in (None, "")
                else datetime.now(UTC).date()
            )
        except ValueError:
            return web.json_response(
                {
                    "error": "invalid_entry_date",
                    "message": "entry_date must use YYYY-MM-DD.",
                },
                status=400,
            )

        try:
            entry = self._create_internal_home_changelog_entry(
                title=title,
                content=content,
                entry_date=entry_date,
            )
            return web.json_response(entry, status=201)
        except Exception:
            log.exception("Error creating internal-home changelog entry")
            return web.json_response(
                {
                    "error": "internal_home_changelog_write_failed",
                    "message": "Could not persist changelog entry.",
                },
                status=500,
            )

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
                partner_logins: set[str] = set()
                if has_partner_table:
                    rows = conn.execute("""
                        SELECT twitch_login
                        FROM twitch_streamers_partner_state
                        WHERE is_partner_active = 1
                        ORDER BY twitch_login
                    """).fetchall()
                    partner_logins = {r[0].lower() for r in rows}

                # Always include streamers who were live in the last 90 days so
                # recently-added or re-verified streamers are never invisible.
                recent_rows = conn.execute("""
                    SELECT DISTINCT LOWER(streamer_login) AS login
                    FROM twitch_stream_sessions
                    WHERE started_at >= NOW() - INTERVAL '90 days'
                    ORDER BY login
                """).fetchall()
                recent_logins: set[str] = {r[0] for r in recent_rows}

                all_logins = partner_logins | recent_logins
                data = [
                    {"login": login, "isPartner": login in partner_logins}
                    for login in sorted(all_logins)
                ]

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

                session_bot_clause, session_bot_params = build_known_chat_bot_not_in_clause(
                    column_expr="sc.chatter_login"
                )

                chatter_presence = conn.execute(
                    """
                    SELECT 1
                    FROM twitch_session_chatters
                    WHERE session_id = ?
                    LIMIT 1
                    """,
                    [session_id],
                ).fetchone()

                chatter_stats = conn.execute(
                    f"""
                    SELECT
                        COUNT(
                            DISTINCT CASE
                                WHEN sc.messages > 0
                                THEN COALESCE(NULLIF(sc.chatter_login, ''), sc.chatter_id)
                                ELSE NULL
                            END
                        ) AS unique_chatters,
                        COUNT(
                            DISTINCT CASE
                                WHEN sc.messages > 0
                                     AND LOWER(COALESCE(CAST(sc.is_first_time_streamer AS TEXT), '0')) IN ('1', 't', 'true')
                                THEN COALESCE(NULLIF(sc.chatter_login, ''), sc.chatter_id)
                                ELSE NULL
                            END
                        ) AS first_time_chatters,
                        COUNT(
                            DISTINCT CASE
                                WHEN sc.messages > 0
                                     AND LOWER(COALESCE(CAST(sc.is_first_time_streamer AS TEXT), '0')) NOT IN ('1', 't', 'true')
                                THEN COALESCE(NULLIF(sc.chatter_login, ''), sc.chatter_id)
                                ELSE NULL
                            END
                        ) AS returning_chatters
                    FROM twitch_session_chatters sc
                    WHERE sc.session_id = ?
                      AND {session_bot_clause}
                    """,
                    [session_id, *session_bot_params],
                ).fetchone()

                if chatter_presence:
                    unique_chatters = int(chatter_stats[0]) if chatter_stats else 0
                    first_time_chatters = int(chatter_stats[1]) if chatter_stats else 0
                    returning_chatters = int(chatter_stats[2]) if chatter_stats else 0
                else:
                    unique_chatters = int(row[13] or 0)
                    first_time_chatters = int(row[14] or 0)
                    returning_chatters = int(row[15] or 0)

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
                    f"""
                    SELECT chatter_login, messages
                    FROM twitch_session_chatters sc
                    WHERE sc.session_id = ?
                      AND {session_bot_clause}
                    ORDER BY messages DESC
                    LIMIT 20
                """,
                    [session_id, *session_bot_params],
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
                        "uniqueChatters": unique_chatters,
                        "firstTimeChatters": first_time_chatters,
                        "returningChatters": returning_chatters,
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
        can_view_all_streamers = auth_level in ("localhost", "admin")

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
