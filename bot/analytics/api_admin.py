"""Admin-only analytics API endpoints for the Twitch admin dashboard."""

from __future__ import annotations

import os
import platform
import re
import time
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from aiohttp import web

from ..logging_setup import log_path, logs_dir
from ..promo_mode import (
    evaluate_global_promo_mode,
    load_global_promo_mode,
    save_global_promo_mode,
    validate_global_promo_mode_config,
)
from ..storage import pg as storage

LOGIN_RE = re.compile(r"^[A-Za-z0-9_]{3,25}$")
_ERROR_LOG_MAX_SCAN_LINES = 4000
_ERROR_LOG_MAX_RETURNED = 200
_DATABASE_STATS_TABLES: tuple[str, ...] = (
    "twitch_streamers",
    "twitch_live_state",
    "twitch_stream_sessions",
    "twitch_stats_tracked",
    "twitch_stats_category",
    "streamer_plans",
    "twitch_billing_subscriptions",
    "affiliate_accounts",
    "twitch_eventsub_capacity_snapshot",
    "dashboard_sessions",
)


def _row_get_value(row: Any, key: str, index: int, default: Any = None) -> Any:
    if row is None:
        return default
    if hasattr(row, "get"):
        return row.get(key, default)
    values = tuple(row)
    return values[index] if index < len(values) else default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_login(raw_value: str) -> str | None:
    login = str(raw_value or "").strip().lower()
    if not LOGIN_RE.fullmatch(login):
        return None
    return login


class _AnalyticsAdminMixin:
    """Admin-only endpoints for the `/twitch/api/admin/*` namespace."""

    def _register_v2_admin_api_routes(self, router: web.UrlDispatcher) -> None:
        router.add_get("/twitch/api/admin/streamers", self._api_admin_streamers)
        router.add_get("/twitch/api/admin/streamers/{login}", self._api_admin_streamer_detail)
        router.add_get("/twitch/api/admin/system/health", self._api_admin_system_health)
        router.add_get("/twitch/api/admin/system/eventsub", self._api_admin_system_eventsub)
        router.add_get("/twitch/api/admin/system/database", self._api_admin_system_database)
        router.add_get("/twitch/api/admin/system/errors", self._api_admin_system_errors)
        router.add_get("/twitch/api/admin/config/overview", self._api_admin_config_overview)
        router.add_post("/twitch/api/admin/config/promo", self._api_admin_config_promo)
        router.add_post("/twitch/api/admin/config/polling", self._api_admin_config_polling)
        router.add_get(
            "/twitch/api/admin/billing/subscriptions",
            self._api_admin_billing_subscriptions,
        )
        router.add_get(
            "/twitch/api/admin/billing/affiliates",
            self._api_admin_billing_affiliates,
        )

    @staticmethod
    def _admin_auth_error(request: web.Request, checker: Any) -> web.Response | None:
        if callable(checker):
            return checker(request)
        return web.json_response({"error": "admin_required", "required": "admin"}, status=403)

    @staticmethod
    def _admin_actor_label(request: web.Request, getter: Any) -> str:
        if not callable(getter):
            return "admin"
        try:
            session = getter(request) or {}
        except Exception:
            session = {}
        user_id = str(session.get("user_id") or "").strip()
        if user_id.isdigit():
            return f"discord:{user_id}"
        return "admin"

    async def _admin_json_body(self, request: web.Request) -> dict[str, Any]:
        cache_key = "_admin_json_body"
        cached = request.get(cache_key)
        if isinstance(cached, dict):
            return cached
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        request[cache_key] = payload
        return payload

    async def _admin_extract_csrf(self, request: web.Request) -> tuple[str, dict[str, Any]]:
        payload = await self._admin_json_body(request)
        header = str(request.headers.get("X-CSRF-Token") or "").strip()
        if header:
            return header, payload
        return str(payload.get("csrf_token") or "").strip(), payload

    def _admin_verify_csrf(self, request: web.Request, provided_token: str) -> bool:
        verifier = getattr(self, "_csrf_verify_token", None)
        if not callable(verifier):
            return False
        try:
            return bool(verifier(request, provided_token))
        except Exception:
            return False

    @staticmethod
    def _admin_eventsub_transport(value: Any) -> str:
        if isinstance(value, dict):
            method = str(value.get("method") or "").strip().lower()
            if method:
                return method
        return str(value or "").strip().lower()

    @staticmethod
    def _admin_error_log_candidates() -> tuple[Path, ...]:
        candidates = [
            log_path("twitch_bot.log"),
            log_path("twitch_service_warnings.log"),
            log_path("twitch_autobans.log"),
        ]
        try:
            for candidate in logs_dir().glob("*.log"):
                candidates.append(candidate)
        except OSError:
            pass
        return tuple(dict.fromkeys(candidates))

    @staticmethod
    def _admin_error_log_entry(source: str, line_number: int, raw_line: str) -> dict[str, Any] | None:
        line = str(raw_line or "").strip()
        if not line:
            return None
        upper_line = line.upper()
        if not any(token in upper_line for token in ("ERROR", "CRITICAL", "TRACEBACK", "EXCEPTION")):
            return None

        timestamp = ""
        level = ""
        message = line
        parts = line.split(" - ", 3)
        if len(parts) == 4:
            timestamp = str(parts[0]).strip()
            level = str(parts[2]).strip()
            message = str(parts[3]).strip() or line
        elif len(parts) >= 2:
            timestamp = str(parts[0]).strip()
            message = str(parts[-1]).strip() or line

        return {
            "id": f"{source}:{line_number}",
            "timestamp": timestamp or None,
            "level": level or None,
            "source": source,
            "message": message[:1200],
            "context": line[:2000],
        }

    def _load_admin_error_log_entries(self) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        for candidate in self._admin_error_log_candidates():
            try:
                if not candidate.exists():
                    continue
            except OSError:
                continue
            recent_lines: deque[tuple[int, str]] = deque(maxlen=_ERROR_LOG_MAX_SCAN_LINES)
            try:
                with candidate.open("r", encoding="utf-8", errors="replace") as handle:
                    for index, line in enumerate(handle, start=1):
                        recent_lines.append((index, line.rstrip("\n")))
            except OSError:
                continue

            for line_number, line in reversed(recent_lines):
                entry = self._admin_error_log_entry(candidate.name, line_number, line)
                if entry is not None:
                    entries.append(entry)
                    if len(entries) >= _ERROR_LOG_MAX_RETURNED:
                        return entries
        return entries

    @staticmethod
    def _admin_database_row_count(conn: Any, table_name: str) -> int | None:
        try:
            row = conn.execute(f"SELECT COUNT(*) AS total FROM {table_name}").fetchone()
        except Exception:
            return None
        if row is None:
            return None
        return _safe_int(_row_get_value(row, "total", 0, None), default=0)

    @staticmethod
    def _admin_database_table_size_bytes(conn: Any, table_name: str) -> int | None:
        try:
            row = conn.execute(
                f"SELECT pg_total_relation_size('{table_name}') AS size_bytes"
            ).fetchone()
        except Exception:
            return None
        if row is None:
            return None
        return _safe_int(_row_get_value(row, "size_bytes", 0, None), default=0)

    @staticmethod
    def _admin_database_size_bytes(conn: Any) -> int | None:
        try:
            row = conn.execute("SELECT pg_database_size(current_database()) AS size_bytes").fetchone()
        except Exception:
            return None
        if row is None:
            return None
        return _safe_int(_row_get_value(row, "size_bytes", 0, None), default=0)

    async def _api_admin_streamers(self, request: web.Request) -> web.Response:
        auth_error = self._admin_auth_error(request, getattr(self, "_require_v2_admin_api", None))
        if auth_error is not None:
            return auth_error

        try:
            with storage.get_conn() as conn:
                rows = conn.execute(
                    """
                    WITH latest_billing AS (
                        SELECT
                            customer_reference,
                            plan_id,
                            status,
                            updated_at,
                            ROW_NUMBER() OVER (
                                PARTITION BY LOWER(customer_reference)
                                ORDER BY updated_at DESC
                            ) AS rn
                        FROM twitch_billing_subscriptions
                    )
                    SELECT
                        s.twitch_login,
                        s.twitch_user_id,
                        s.discord_user_id,
                        s.discord_display_name,
                        s.created_at,
                        s.archived_at,
                        s.require_discord_link,
                        s.is_on_discord,
                        s.raid_bot_enabled,
                        s.silent_ban,
                        s.silent_raid,
                        s.is_monitored_only,
                        COALESCE(ps.is_verified, 0) AS is_verified,
                        COALESCE(ps.is_partner_active, 0) AS is_partner_active,
                        COALESCE(l.is_live, 0) AS is_live,
                        l.last_seen_at,
                        l.last_viewer_count,
                        l.active_session_id,
                        l.last_game,
                        sp.promo_disabled,
                        sp.promo_message,
                        sp.raid_boost_enabled,
                        sp.manual_plan_id,
                        sp.manual_plan_expires_at,
                        sp.manual_plan_notes,
                        lb.plan_id AS billing_plan_id,
                        lb.status AS billing_status,
                        lb.updated_at AS billing_updated_at
                    FROM twitch_streamers s
                    LEFT JOIN twitch_streamers_partner_state ps
                        ON LOWER(ps.twitch_login) = LOWER(s.twitch_login)
                    LEFT JOIN twitch_live_state l
                        ON s.twitch_user_id = l.twitch_user_id
                        OR LOWER(s.twitch_login) = LOWER(l.streamer_login)
                    LEFT JOIN streamer_plans sp
                        ON LOWER(sp.twitch_login) = LOWER(s.twitch_login)
                    LEFT JOIN latest_billing lb
                        ON LOWER(lb.customer_reference) = LOWER(s.twitch_login)
                       AND lb.rn = 1
                    ORDER BY LOWER(s.twitch_login) ASC
                    """
                ).fetchall()
        except Exception as exc:
            return web.json_response({"error": str(exc)}, status=500)

        payload = []
        for row in rows:
            login = str(_row_get_value(row, "twitch_login", 0, "") or "").strip().lower()
            archived = bool(_row_get_value(row, "archived_at", 5, None))
            is_live = bool(_row_get_value(row, "is_live", 14, 0))
            verified = bool(_row_get_value(row, "is_verified", 12, 0))
            status = "archived" if archived else "live" if is_live else "verified" if verified else "offline"
            payload.append(
                {
                    "login": login,
                    "displayName": str(
                        _row_get_value(row, "discord_display_name", 3, "") or login
                    ).strip()
                    or login,
                    "twitchUserId": str(_row_get_value(row, "twitch_user_id", 1, "") or "").strip()
                    or None,
                    "verified": verified,
                    "archived": archived,
                    "isLive": is_live,
                    "viewerCount": _safe_int(_row_get_value(row, "last_viewer_count", 16, 0), default=0),
                    "activeSessionId": _row_get_value(row, "active_session_id", 17, None),
                    "lastSeenAt": _row_get_value(row, "last_seen_at", 15, None),
                    "lastGame": _row_get_value(row, "last_game", 18, None),
                    "planId": str(
                        _row_get_value(row, "manual_plan_id", 22, "")
                        or _row_get_value(row, "billing_plan_id", 25, "")
                        or ""
                    ).strip()
                    or None,
                    "billingStatus": str(_row_get_value(row, "billing_status", 26, "") or "").strip()
                    or None,
                    "promoDisabled": bool(_row_get_value(row, "promo_disabled", 19, 0)),
                    "notes": str(_row_get_value(row, "manual_plan_notes", 24, "") or "").strip()
                    or None,
                    "status": status,
                }
            )
        return web.json_response({"items": payload, "count": len(payload)})

    async def _api_admin_streamer_detail(self, request: web.Request) -> web.Response:
        auth_error = self._admin_auth_error(request, getattr(self, "_require_v2_admin_api", None))
        if auth_error is not None:
            return auth_error

        login = _normalize_login(request.match_info.get("login", ""))
        if not login:
            return web.json_response({"error": "invalid_login"}, status=400)

        try:
            with storage.get_conn() as conn:
                row = conn.execute(
                    """
                    WITH latest_billing AS (
                        SELECT
                            customer_reference,
                            plan_id,
                            status,
                            updated_at,
                            ROW_NUMBER() OVER (
                                PARTITION BY LOWER(customer_reference)
                                ORDER BY updated_at DESC
                            ) AS rn
                        FROM twitch_billing_subscriptions
                    )
                    SELECT
                        s.twitch_login,
                        s.twitch_user_id,
                        s.discord_user_id,
                        s.discord_display_name,
                        s.created_at,
                        s.archived_at,
                        s.require_discord_link,
                        s.is_on_discord,
                        s.raid_bot_enabled,
                        s.silent_ban,
                        s.silent_raid,
                        s.is_monitored_only,
                        COALESCE(ps.is_verified, 0) AS is_verified,
                        COALESCE(ps.is_partner_active, 0) AS is_partner_active,
                        COALESCE(l.is_live, 0) AS is_live,
                        l.last_seen_at,
                        l.last_viewer_count,
                        l.active_session_id,
                        l.last_started_at,
                        l.last_game,
                        sp.plan_name,
                        sp.promo_disabled,
                        sp.promo_message,
                        sp.raid_boost_enabled,
                        sp.notes,
                        sp.manual_plan_id,
                        sp.manual_plan_expires_at,
                        sp.manual_plan_notes,
                        lb.plan_id AS billing_plan_id,
                        lb.status AS billing_status,
                        lb.updated_at AS billing_updated_at
                    FROM twitch_streamers s
                    LEFT JOIN twitch_streamers_partner_state ps
                        ON LOWER(ps.twitch_login) = LOWER(s.twitch_login)
                    LEFT JOIN twitch_live_state l
                        ON s.twitch_user_id = l.twitch_user_id
                        OR LOWER(s.twitch_login) = LOWER(l.streamer_login)
                    LEFT JOIN streamer_plans sp
                        ON LOWER(sp.twitch_login) = LOWER(s.twitch_login)
                    LEFT JOIN latest_billing lb
                        ON LOWER(lb.customer_reference) = LOWER(s.twitch_login)
                       AND lb.rn = 1
                    WHERE LOWER(s.twitch_login) = LOWER(?)
                    LIMIT 1
                    """,
                    (login,),
                ).fetchone()
                if row is None:
                    return web.json_response({"error": "not_found"}, status=404)

                stats_row = conn.execute(
                    """
                    SELECT
                        COUNT(*) AS total_sessions,
                        COALESCE(SUM(duration_seconds), 0) AS total_duration_seconds,
                        COALESCE(AVG(avg_viewers), 0) AS avg_viewers,
                        COALESCE(MAX(peak_viewers), 0) AS peak_viewers,
                        COALESCE(SUM(follower_delta), 0) AS follower_delta
                    FROM twitch_stream_sessions
                    WHERE LOWER(streamer_login) = LOWER(?)
                    """,
                    (login,),
                ).fetchone()
                sessions = conn.execute(
                    """
                    SELECT
                        id,
                        started_at,
                        ended_at,
                        stream_title,
                        game_name,
                        avg_viewers,
                        peak_viewers,
                        duration_seconds,
                        follower_delta
                    FROM twitch_stream_sessions
                    WHERE LOWER(streamer_login) = LOWER(?)
                    ORDER BY started_at DESC
                    LIMIT 10
                    """,
                    (login,),
                ).fetchall()
        except Exception as exc:
            return web.json_response({"error": str(exc)}, status=500)

        session_payload = []
        for session in sessions:
            duration_seconds = _safe_int(_row_get_value(session, "duration_seconds", 7, 0), default=0)
            session_payload.append(
                {
                    "sessionId": _row_get_value(session, "id", 0, None),
                    "startedAt": _row_get_value(session, "started_at", 1, None),
                    "endedAt": _row_get_value(session, "ended_at", 2, None),
                    "title": _row_get_value(session, "stream_title", 3, None),
                    "category": _row_get_value(session, "game_name", 4, None),
                    "averageViewers": _row_get_value(session, "avg_viewers", 5, None),
                    "peakViewers": _row_get_value(session, "peak_viewers", 6, None),
                    "watchTimeHours": round(duration_seconds / 3600.0, 2),
                    "followerDelta": _row_get_value(session, "follower_delta", 8, None),
                }
            )

        total_duration_seconds = _safe_int(
            _row_get_value(stats_row, "total_duration_seconds", 1, 0) if stats_row else 0,
            default=0,
        )
        payload = {
            "login": login,
            "displayName": str(_row_get_value(row, "discord_display_name", 3, "") or login).strip()
            or login,
            "twitchUserId": str(_row_get_value(row, "twitch_user_id", 1, "") or "").strip() or None,
            "verified": bool(_row_get_value(row, "is_verified", 12, 0)),
            "archived": bool(_row_get_value(row, "archived_at", 5, None)),
            "isLive": bool(_row_get_value(row, "is_live", 14, 0)),
            "planId": str(
                _row_get_value(row, "manual_plan_id", 25, "")
                or _row_get_value(row, "billing_plan_id", 28, "")
                or _row_get_value(row, "plan_name", 20, "")
                or ""
            ).strip()
            or None,
            "stats": {
                "totalSessions": _safe_int(_row_get_value(stats_row, "total_sessions", 0, 0), default=0)
                if stats_row
                else 0,
                "totalWatchHours": round(total_duration_seconds / 3600.0, 2),
                "averageViewers": round(float(_row_get_value(stats_row, "avg_viewers", 2, 0.0) or 0.0), 2)
                if stats_row
                else 0.0,
                "peakViewers": _safe_int(_row_get_value(stats_row, "peak_viewers", 3, 0), default=0)
                if stats_row
                else 0,
                "followerDelta": _safe_int(_row_get_value(stats_row, "follower_delta", 4, 0), default=0)
                if stats_row
                else 0,
                "viewerCount": _safe_int(_row_get_value(row, "last_viewer_count", 16, 0), default=0),
                "lastSeenAt": _row_get_value(row, "last_seen_at", 15, None),
                "lastStartedAt": _row_get_value(row, "last_started_at", 18, None),
                "lastGame": _row_get_value(row, "last_game", 19, None),
            },
            "settings": {
                "requireDiscordLink": bool(_row_get_value(row, "require_discord_link", 6, 0)),
                "isOnDiscord": bool(_row_get_value(row, "is_on_discord", 7, 0)),
                "raidBotEnabled": bool(_row_get_value(row, "raid_bot_enabled", 8, 0)),
                "silentBan": bool(_row_get_value(row, "silent_ban", 9, 0)),
                "silentRaid": bool(_row_get_value(row, "silent_raid", 10, 0)),
                "isMonitoredOnly": bool(_row_get_value(row, "is_monitored_only", 11, 0)),
                "promoDisabled": bool(_row_get_value(row, "promo_disabled", 21, 0)),
                "promoMessage": _row_get_value(row, "promo_message", 22, None),
                "raidBoostEnabled": bool(_row_get_value(row, "raid_boost_enabled", 23, 0)),
                "notes": _row_get_value(row, "notes", 24, None),
                "manualPlanExpiresAt": _row_get_value(row, "manual_plan_expires_at", 26, None),
                "manualPlanNotes": _row_get_value(row, "manual_plan_notes", 27, None),
                "billingStatus": _row_get_value(row, "billing_status", 29, None),
                "billingUpdatedAt": _row_get_value(row, "billing_updated_at", 30, None),
            },
            "sessions": session_payload,
            "recentActivity": session_payload[:5],
        }
        return web.json_response(payload)

    async def _api_admin_system_health(self, request: web.Request) -> web.Response:
        auth_error = self._admin_auth_error(request, getattr(self, "_require_v2_admin_api", None))
        if auth_error is not None:
            return auth_error

        memory_bytes = None
        memory_rss_bytes = None
        uptime_seconds = None
        process_id = os.getpid()
        try:
            import psutil  # type: ignore

            process = psutil.Process(process_id)
            mem_info = process.memory_info()
            memory_bytes = int(getattr(mem_info, "rss", 0) or 0)
            memory_rss_bytes = memory_bytes
            uptime_seconds = max(0, int(time.time() - float(process.create_time())))
        except Exception:
            runtime_started_at = getattr(self, "_admin_runtime_started_at", None)
            if not runtime_started_at:
                runtime_started_at = time.time()
                setattr(self, "_admin_runtime_started_at", runtime_started_at)
            uptime_seconds = max(0, int(time.time() - float(runtime_started_at)))

        last_tick_at = None
        try:
            with storage.get_conn() as conn:
                row = conn.execute(
                    """
                    SELECT MAX(COALESCE(last_seen_at, last_started_at)) AS last_tick_at
                    FROM twitch_live_state
                    """
                ).fetchone()
                last_tick_at = _row_get_value(row, "last_tick_at", 0, None) if row else None
        except Exception:
            last_tick_at = None

        last_tick_age_seconds = None
        if last_tick_at:
            try:
                parsed = datetime.fromisoformat(str(last_tick_at).replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=UTC)
                last_tick_age_seconds = max(
                    0,
                    int((datetime.now(UTC) - parsed.astimezone(UTC)).total_seconds()),
                )
            except ValueError:
                last_tick_age_seconds = None

        return web.json_response(
            {
                "uptimeSeconds": uptime_seconds,
                "memoryBytes": memory_bytes,
                "memoryRssBytes": memory_rss_bytes,
                "pythonVersion": platform.python_version(),
                "processId": process_id,
                "lastTickAt": last_tick_at,
                "lastTickAgeSeconds": last_tick_age_seconds,
                "serviceWarnings": [],
            }
        )

    async def _api_admin_system_eventsub(self, request: web.Request) -> web.Response:
        auth_error = self._admin_auth_error(request, getattr(self, "_require_v2_admin_api", None))
        if auth_error is not None:
            return auth_error

        overview_getter = getattr(self, "_get_eventsub_capacity_overview", None)
        overview: dict[str, Any] = {}
        if callable(overview_getter):
            try:
                overview = await overview_getter(hours=24)
            except Exception:
                overview = {}

        current = overview.get("current") if isinstance(overview, dict) else {}
        subscriptions = list(overview.get("active_subscriptions") or []) if isinstance(overview, dict) else []
        websocket_status = "inactive"
        if subscriptions:
            transports = {
                self._admin_eventsub_transport(item.get("transport"))
                for item in subscriptions
                if isinstance(item, dict)
            }
            if "websocket" in transports:
                websocket_status = "connected"
            elif "webhook" in transports:
                websocket_status = "webhook"

        return web.json_response(
            {
                "websocketStatus": websocket_status,
                "websocketSessionId": getattr(self, "_eventsub_session_id", None),
                "websocketConnectedAt": None,
                "websocketReconnectedAt": None,
                "activeSubscriptionCount": len(subscriptions),
                "capacity": {
                    "used": _safe_int((current or {}).get("used_slots"), default=0)
                    if isinstance(current, dict)
                    else 0,
                    "max": _safe_int((current or {}).get("total_slots"), default=0)
                    if isinstance(current, dict)
                    else 0,
                    "remaining": max(
                        0,
                        _safe_int((current or {}).get("headroom_slots"), default=0),
                    )
                    if isinstance(current, dict)
                    else 0,
                    "lastSnapshotAt": overview.get("last_snapshot_at") if isinstance(overview, dict) else None,
                },
                "subscriptions": subscriptions,
                "transportMode": websocket_status,
                "raw": overview,
            }
        )

    async def _api_admin_system_database(self, request: web.Request) -> web.Response:
        auth_error = self._admin_auth_error(request, getattr(self, "_require_v2_admin_api", None))
        if auth_error is not None:
            return auth_error

        tables: list[dict[str, Any]] = []
        database_size_bytes = None
        try:
            with storage.get_conn() as conn:
                database_size_bytes = self._admin_database_size_bytes(conn)
                for table_name in _DATABASE_STATS_TABLES:
                    row_count = self._admin_database_row_count(conn, table_name)
                    if row_count is None:
                        continue
                    tables.append(
                        {
                            "table": table_name,
                            "rowCount": row_count,
                            "sizeBytes": self._admin_database_table_size_bytes(conn, table_name),
                        }
                    )
        except Exception as exc:
            return web.json_response({"error": str(exc)}, status=500)

        return web.json_response(
            {
                "databaseSizeBytes": database_size_bytes,
                "tables": tables,
            }
        )

    async def _api_admin_system_errors(self, request: web.Request) -> web.Response:
        auth_error = self._admin_auth_error(request, getattr(self, "_require_v2_admin_api", None))
        if auth_error is not None:
            return auth_error

        try:
            page = max(1, int(request.query.get("page", "1")))
        except ValueError:
            page = 1
        try:
            page_size = min(100, max(1, int(request.query.get("page_size", "25"))))
        except ValueError:
            page_size = 25

        entries = self._load_admin_error_log_entries()
        total = len(entries)
        start = (page - 1) * page_size
        end = start + page_size
        return web.json_response(
            {
                "page": page,
                "pageSize": page_size,
                "total": total,
                "hasMore": end < total,
                "entries": entries[start:end],
            }
        )

    async def _api_admin_config_overview(self, request: web.Request) -> web.Response:
        auth_error = self._admin_auth_error(request, getattr(self, "_require_v2_admin_api", None))
        if auth_error is not None:
            return auth_error

        promo_config: dict[str, Any] = {}
        raid_snapshot: dict[str, Any] = {}
        chat_snapshot: dict[str, Any] = {}
        csrf_token = ""
        csrf_getter = getattr(self, "_csrf_get_token", None)
        csrf_generator = getattr(self, "_csrf_generate_token", None)
        if callable(csrf_getter):
            try:
                csrf_token = str(csrf_getter(request) or "")
            except Exception:
                csrf_token = ""
        if not csrf_token and callable(csrf_generator):
            try:
                csrf_token = str(csrf_generator(request) or "")
            except Exception:
                csrf_token = ""

        try:
            with storage.get_conn() as conn:
                promo_config = evaluate_global_promo_mode(load_global_promo_mode(conn))
                raid_row = conn.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE raid_bot_enabled = 1) AS enabled_count,
                        COUNT(*) FILTER (WHERE COALESCE(live_ping_enabled, 1) = 1) AS live_ping_enabled_count
                    FROM twitch_streamers
                    """
                ).fetchone()
                chat_row = conn.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE silent_ban = 1) AS silent_ban_count,
                        COUNT(*) FILTER (WHERE silent_raid = 1) AS silent_raid_count
                    FROM twitch_streamers
                    """
                ).fetchone()
                raid_snapshot = {
                    "enabledStreamerCount": _safe_int(_row_get_value(raid_row, "enabled_count", 0, 0), default=0)
                    if raid_row
                    else 0,
                    "livePingEnabledCount": _safe_int(
                        _row_get_value(raid_row, "live_ping_enabled_count", 1, 0),
                        default=0,
                    )
                    if raid_row
                    else 0,
                }
                chat_snapshot = {
                    "silentBanCount": _safe_int(
                        _row_get_value(chat_row, "silent_ban_count", 0, 0),
                        default=0,
                    )
                    if chat_row
                    else 0,
                    "silentRaidCount": _safe_int(
                        _row_get_value(chat_row, "silent_raid_count", 1, 0),
                        default=0,
                    )
                    if chat_row
                    else 0,
                }
        except Exception as exc:
            return web.json_response({"error": str(exc)}, status=500)

        polling_interval = _safe_int(
            getattr(self, "_admin_polling_interval_seconds", 60),
            default=60,
        )
        return web.json_response(
            {
                "promo": promo_config,
                "polling": {
                    "intervalSeconds": polling_interval,
                    "persisted": False,
                    "source": "runtime_fallback",
                },
                "raids": raid_snapshot,
                "chat": chat_snapshot,
                "announcements": promo_config.get("config", {}) if isinstance(promo_config, dict) else {},
                "csrfToken": csrf_token or None,
                "csrf_token": csrf_token or None,
            }
        )

    async def _api_admin_config_promo(self, request: web.Request) -> web.Response:
        auth_error = self._admin_auth_error(request, getattr(self, "_require_v2_admin_api", None))
        if auth_error is not None:
            return auth_error

        csrf_token, payload = await self._admin_extract_csrf(request)
        if not self._admin_verify_csrf(request, csrf_token):
            return web.json_response({"error": "invalid_csrf"}, status=403)

        normalized, issues = validate_global_promo_mode_config(payload)
        if issues:
            return web.json_response(
                {
                    "error": "validation_failed",
                    "validation": issues,
                },
                status=400,
            )

        actor_label = self._admin_actor_label(request, getattr(self, "_get_discord_admin_session", None))
        try:
            with storage.get_conn() as conn:
                saved = save_global_promo_mode(conn, config=normalized, updated_by=actor_label)
                evaluation = evaluate_global_promo_mode(saved)
        except ValueError as exc:
            return web.json_response({"error": str(exc)}, status=400)
        except Exception as exc:
            return web.json_response({"error": str(exc)}, status=500)

        return web.json_response({"ok": True, "config": saved, "evaluation": evaluation})

    async def _api_admin_config_polling(self, request: web.Request) -> web.Response:
        auth_error = self._admin_auth_error(request, getattr(self, "_require_v2_admin_api", None))
        if auth_error is not None:
            return auth_error

        csrf_token, payload = await self._admin_extract_csrf(request)
        if not self._admin_verify_csrf(request, csrf_token):
            return web.json_response({"error": "invalid_csrf"}, status=403)

        interval_seconds = _safe_int(
            payload.get("intervalSeconds", payload.get("interval_seconds", 0)),
            default=0,
        )
        if interval_seconds < 5 or interval_seconds > 3600:
            return web.json_response(
                {
                    "error": "invalid_interval_seconds",
                    "message": "intervalSeconds muss zwischen 5 und 3600 liegen.",
                },
                status=400,
            )

        setattr(self, "_admin_polling_interval_seconds", interval_seconds)
        return web.json_response(
            {
                "ok": True,
                "polling": {
                    "intervalSeconds": interval_seconds,
                    "persisted": False,
                    "source": "runtime_fallback",
                    "message": (
                        "Kein persistenter Polling-Speicherpfad gefunden; Wert gilt bis zum Neustart."
                    ),
                },
            }
        )

    async def _api_admin_billing_subscriptions(self, request: web.Request) -> web.Response:
        auth_error = self._admin_auth_error(request, getattr(self, "_require_v2_admin_api", None))
        if auth_error is not None:
            return auth_error

        try:
            with storage.get_conn() as conn:
                rows = conn.execute(
                    """
                    SELECT
                        b.customer_reference,
                        b.plan_id,
                        b.status,
                        b.current_period_start,
                        b.current_period_end,
                        b.updated_at,
                        b.canceled_at,
                        b.ended_at,
                        sp.manual_plan_id,
                        sp.manual_plan_expires_at
                    FROM twitch_billing_subscriptions b
                    LEFT JOIN streamer_plans sp
                        ON LOWER(sp.twitch_login) = LOWER(b.customer_reference)
                    ORDER BY b.updated_at DESC
                    """
                ).fetchall()
        except Exception as exc:
            return web.json_response({"error": str(exc)}, status=500)

        payload = [
            {
                "login": str(_row_get_value(row, "customer_reference", 0, "") or "").strip().lower()
                or None,
                "customerReference": _row_get_value(row, "customer_reference", 0, None),
                "planId": _row_get_value(row, "plan_id", 1, None),
                "status": _row_get_value(row, "status", 2, None),
                "trialEndsAt": None,
                "currentPeriodEnd": _row_get_value(row, "current_period_end", 4, None),
                "updatedAt": _row_get_value(row, "updated_at", 5, None),
                "manualPlanId": _row_get_value(row, "manual_plan_id", 8, None),
                "manualPlanExpiresAt": _row_get_value(row, "manual_plan_expires_at", 9, None),
            }
            for row in rows
        ]
        return web.json_response({"items": payload, "count": len(payload)})

    async def _api_admin_billing_affiliates(self, request: web.Request) -> web.Response:
        auth_error = self._admin_auth_error(request, getattr(self, "_require_v2_admin_api", None))
        if auth_error is not None:
            return auth_error

        try:
            with storage.get_conn() as conn:
                rows = conn.execute(
                    """
                    SELECT
                        twitch_login,
                        email,
                        stripe_account_id,
                        stripe_connect_status,
                        commission_rate,
                        updated_at,
                        created_at
                    FROM affiliate_accounts
                    ORDER BY COALESCE(updated_at, created_at) DESC
                    """
                ).fetchall()
        except Exception as exc:
            return web.json_response({"error": str(exc)}, status=500)

        payload = []
        for row in rows:
            payload.append(
                {
                    "twitchLogin": _row_get_value(row, "twitch_login", 0, None),
                    "stripeAccountId": _row_get_value(row, "stripe_account_id", 2, None),
                    "status": _row_get_value(row, "stripe_connect_status", 3, None),
                    "payoutEmail": _row_get_value(row, "email", 1, None),
                    "commissionRate": _row_get_value(row, "commission_rate", 4, None),
                    "updatedAt": _row_get_value(row, "updated_at", 5, None)
                    or _row_get_value(row, "created_at", 6, None),
                }
            )
        return web.json_response({"items": payload, "count": len(payload)})


__all__ = ["_AnalyticsAdminMixin"]
