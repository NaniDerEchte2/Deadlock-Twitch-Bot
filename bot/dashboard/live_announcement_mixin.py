"""Standalone dashboard module for configurable go-live announcements."""

from __future__ import annotations

import html
import json
import re
from datetime import UTC, datetime
from typing import Any

import discord
from aiohttp import web

from .. import storage as _storage
from ..core.constants import log
from ..live_announce.template import (
    LiveAnnouncementConfig,
    build_template_context,
    render_announcement_payload,
    validate_live_announcement_config,
)

_MAX_PREVIEW_CONFIG_CHARS = 50_000
_ROLE_NAME_SAFE_RE = re.compile(r"[^A-Za-z0-9 _-]+")
SUPPORTED_PLACEHOLDERS: tuple[str, ...] = (
    "channel",
    "url",
    "rolle",
    "title",
    "viewer_count",
    "started_at",
    "language",
    "tags",
    "uptime",
    "game",
)


def _default_live_announcement_config() -> dict[str, Any]:
    return {
        "content": "{rolle} **{channel}** ist live! Schau ueber den Button unten rein.",
        "mentions": {"enabled": True, "role_id": ""},
        "embed": {
            "color": "#9146ff",
            "author": {
                "enabled": True,
                "name": "LIVE: {channel}",
                "icon_mode": "twitch_logo",
                "link_to_channel": True,
            },
            "title": "{channel} ist LIVE in {game}!",
            "title_link_enabled": True,
            "description_mode": "stream_title",
            "description": "{title}",
            "shorten": False,
            "fields": [
                {"name": "Viewer", "value": "{viewer_count}", "inline": True},
                {"name": "Kategorie", "value": "{game}", "inline": True},
            ],
            "thumbnail": {"mode": "none", "custom_url": ""},
            "image": {
                "use_stream_thumbnail": True,
                "custom_url": "",
                "format": "16:9",
                "cache_buster": True,
            },
            "footer": {
                "text": "Auf Twitch ansehen fuer mehr Action!",
                "icon_mode": "none",
                "timestamp_mode": "started_at",
            },
        },
        "button": {"enabled": True, "label": "Auf Twitch ansehen", "url_template": "{url}"},
        "allowed_editor_role_ids": [],
    }


def _deep_merge(dst: dict[str, Any], src: dict[str, Any]) -> dict[str, Any]:
    out = json.loads(json.dumps(dst))
    for key, value in src.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out.get(key) or {}, value)
        else:
            out[key] = value
    return out


def _parse_config_json(raw: str | None) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return _default_live_announcement_config()
    try:
        parsed = json.loads(text)
    except Exception:
        return _default_live_announcement_config()
    if not isinstance(parsed, dict):
        return _default_live_announcement_config()
    return _deep_merge(_default_live_announcement_config(), parsed)


def _to_template_config(cfg: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(cfg, dict):
        return {}
    if "embed" not in cfg:
        return cfg

    embed = cfg.get("embed") if isinstance(cfg.get("embed"), dict) else {}
    author = embed.get("author") if isinstance(embed.get("author"), dict) else {}
    footer = embed.get("footer") if isinstance(embed.get("footer"), dict) else {}
    fields = embed.get("fields") if isinstance(embed.get("fields"), list) else []
    image = embed.get("image") if isinstance(embed.get("image"), dict) else {}
    thumbnail = embed.get("thumbnail") if isinstance(embed.get("thumbnail"), dict) else {}
    mentions = cfg.get("mentions") if isinstance(cfg.get("mentions"), dict) else {}
    button = cfg.get("button") if isinstance(cfg.get("button"), dict) else {}
    mention_role_id = str(mentions.get("role_id") or "").strip()
    static_ping_role_ids = [int(mention_role_id)] if mention_role_id.isdigit() else []

    normalized_fields: list[dict[str, Any]] = []
    for field in fields:
        if not isinstance(field, dict):
            continue
        normalized_fields.append(
            {
                "name_template": str(field.get("name") or ""),
                "value_template": str(field.get("value") or ""),
                "inline": bool(field.get("inline", True)),
            }
        )

    image_mode = "stream_thumbnail"
    if not bool(image.get("use_stream_thumbnail", True)):
        image_mode = "custom" if str(image.get("custom_url") or "").strip() else "none"

    return {
        "content_template": str(cfg.get("content") or "").replace("{rolle}", "{mention_role}"),
        "color": embed.get("color", "#9146ff"),
        "author": {
            "name_template": str(author.get("name") or "LIVE: {channel}"),
            "icon_mode": (
                "twitch"
                if str(author.get("icon_mode") or "").strip().lower() in {"twitch_logo", "twitch"}
                else str(author.get("icon_mode") or "none").strip().lower()
            ),
            "link_to_stream": bool(author.get("link_to_channel", True)),
        },
        "title_template": str(embed.get("title") or "{channel} ist LIVE in {game}!"),
        "title_link_to_stream": bool(embed.get("title_link_enabled", True)),
        "description_mode": str(embed.get("description_mode") or "stream_title"),
        "description_template": str(embed.get("description") or "{title}"),
        "short_description": bool(embed.get("shorten", False)),
        "fields": normalized_fields,
        "images": {
            "thumbnail_mode": (
                "custom"
                if str(thumbnail.get("mode") or "").strip().lower() == "custom_url"
                else str(thumbnail.get("mode") or "none").strip().lower()
            ),
            "thumbnail_url_template": str(thumbnail.get("custom_url") or ""),
            "image_mode": image_mode,
            "image_url_template": str(image.get("custom_url") or ""),
            "image_ratio": str(image.get("format") or "16:9"),
            "cache_buster": bool(image.get("cache_buster", True)),
        },
        "footer": {
            "text_template": str(footer.get("text") or ""),
            "icon_mode": (
                "twitch"
                if str(footer.get("icon_mode") or "").strip().lower() in {"twitch_logo", "twitch"}
                else "none"
            ),
            "timestamp_mode": str(footer.get("timestamp_mode") or "started_at"),
        },
        "button": {
            "label_template": str(button.get("label") or "Auf Twitch ansehen"),
            "url_template": "{url}",
            "force_stream_url": True,
        },
        "mentions": {
            "use_streamer_ping_role": bool(mentions.get("enabled", True)),
            "streamer_ping_role_name_template": "{channel} LIVE PING",
            "allowed_editor_role_ids": [
                int(role_id)
                for role_id in (cfg.get("allowed_editor_role_ids") or [])
                if str(role_id).isdigit()
            ],
            "static_ping_role_ids": static_ping_role_ids,
            "allow_everyone": False,
        },
    }


def _validate_config_dict(cfg: dict[str, Any]) -> list[dict[str, str]]:
    try:
        config_obj = LiveAnnouncementConfig.from_dict(_to_template_config(cfg))
    except Exception as exc:
        return [{"path": "config", "message": f"config_parse_failed: {exc}"}]
    try:
        raw_errors = list(validate_live_announcement_config(config_obj))
    except Exception as exc:
        return [{"path": "config", "message": f"config_validation_failed: {exc}"}]
    return [{"path": "config", "message": str(err)} for err in raw_errors]


class DashboardLiveAnnouncementMixin:
    """Routes + UI for the configurable Go-Live builder."""

    async def live_announcement_page(self, request: web.Request) -> web.StreamResponse:
        self._la_require_auth(request)
        auth_level = self._la_auth_level(request)
        is_admin = auth_level in {"admin", "localhost"}
        session = self._la_session(request)
        session_login = str((session or {}).get("twitch_login") or "").strip().lower()
        streamer = await self._la_resolve_streamer(
            request=request,
            session_login=session_login,
            is_admin=is_admin,
        )
        if not streamer:
            return web.Response(text="Kein Streamer verfuegbar.", status=404)

        row = self._la_load(streamer)
        role_state = await self._la_ensure_streamer_ping_role(
            streamer_login=streamer,
            create_if_missing=True,
        )
        streamer_ping_role_id = self._la_coerce_role_id(role_state.get("role_id"))
        preview = self._la_preview_payload(
            streamer,
            row["config"],
            streamer_ping_role_id=streamer_ping_role_id,
        )
        csrf = self._la_csrf_generate(request)
        streamers = self._la_list_streamers(session_login=session_login, is_admin=is_admin)
        page = self._la_render_page(
            streamer_login=streamer,
            streamers=streamers,
            csrf_token=csrf,
            auth_level=auth_level,
            config=row["config"],
            allowed_editor_role_ids=row["allowed_editor_role_ids"],
            preview=preview,
            streamer_ping_role_id=streamer_ping_role_id,
            streamer_ping_role_name=str(role_state.get("role_name") or "") or None,
            role_status_message=str(role_state.get("message") or ""),
        )
        return web.Response(text=page, content_type="text/html")

    async def api_live_announcement_config(self, request: web.Request) -> web.StreamResponse:
        self._la_require_auth(request)
        auth_level = self._la_auth_level(request)
        is_admin = auth_level in {"admin", "localhost"}
        session = self._la_session(request)
        session_login = str((session or {}).get("twitch_login") or "").strip().lower()
        streamer = await self._la_resolve_streamer(
            request=request,
            session_login=session_login,
            is_admin=is_admin,
        )
        if not streamer:
            return web.json_response({"error": "streamer_not_found"}, status=404)

        row = self._la_load(streamer)
        role_state = await self._la_ensure_streamer_ping_role(
            streamer_login=streamer,
            create_if_missing=True,
        )
        streamer_ping_role_id = self._la_coerce_role_id(role_state.get("role_id"))
        return web.json_response(
            {
                "streamer_login": streamer,
                "auth_level": auth_level,
                "is_admin": is_admin,
                "config": row["config"],
                "allowed_editor_role_ids": row["allowed_editor_role_ids"],
                "streamer_ping_role_id": streamer_ping_role_id,
                "streamer_ping_role_name": str(role_state.get("role_name") or ""),
                "role_status_message": str(role_state.get("message") or ""),
                "validation": _validate_config_dict(row["config"]),
            }
        )

    async def api_live_announcement_save_config(self, request: web.Request) -> web.StreamResponse:
        self._la_require_auth(request)
        csrf = await self._la_csrf_extract(request)
        if not self._la_csrf_verify(request, csrf):
            return web.json_response({"error": "invalid_csrf"}, status=403)

        auth_level = self._la_auth_level(request)
        is_admin = auth_level in {"admin", "localhost"}
        session = self._la_session(request)
        session_login = str((session or {}).get("twitch_login") or "").strip().lower()

        body = await self._la_json_body(request)
        streamer = await self._la_resolve_streamer(
            request=request,
            session_login=session_login,
            is_admin=is_admin,
            requested_streamer=str(body.get("streamer_login") or "").strip(),
        )
        if not streamer:
            return web.json_response({"error": "streamer_not_found"}, status=404)
        if not is_admin and session_login and streamer != session_login:
            return web.json_response(
                {
                    "error": "forbidden_streamer_scope",
                    "message": "Partner duerfen nur den eigenen Streamer konfigurieren.",
                },
                status=403,
            )

        raw_cfg = body.get("config")
        if not isinstance(raw_cfg, dict):
            return web.json_response({"error": "invalid_config_payload"}, status=400)
        cfg = _deep_merge(_default_live_announcement_config(), raw_cfg)
        cfg["content"] = self._la_sanitize_disallowed_mentions_text(cfg.get("content"))
        issues = _validate_config_dict(cfg)
        if issues:
            return web.json_response({"error": "validation_failed", "validation": issues}, status=400)

        current_row = self._la_load(streamer)
        existing_role_ids = self._la_parse_role_ids(current_row.get("allowed_editor_role_ids"))
        can_edit, reason = await self._la_can_edit(
            request=request,
            streamer_login=streamer,
            session_login=session_login,
            is_admin=is_admin,
            allowed_editor_role_ids=existing_role_ids,
        )
        if not can_edit:
            return web.json_response({"error": "editor_role_required", "message": reason}, status=403)

        role_ids = self._la_parse_role_ids(body.get("allowed_editor_role_ids"))
        actor = self._la_actor_label(request)
        self._la_save(streamer, cfg, role_ids, actor)
        role_state = await self._la_ensure_streamer_ping_role(
            streamer_login=streamer,
            create_if_missing=True,
        )
        streamer_ping_role_id = self._la_coerce_role_id(role_state.get("role_id"))
        preview = self._la_preview_payload(
            streamer,
            cfg,
            streamer_ping_role_id=streamer_ping_role_id,
        )
        return web.json_response(
            {
                "ok": True,
                "streamer_login": streamer,
                "updated_by": actor,
                "preview": preview,
                "streamer_ping_role_id": streamer_ping_role_id,
                "streamer_ping_role_name": str(role_state.get("role_name") or ""),
                "role_status_message": str(role_state.get("message") or ""),
                "validation": [],
            }
        )

    async def api_live_announcement_test_send(self, request: web.Request) -> web.StreamResponse:
        self._la_require_auth(request)
        csrf = await self._la_csrf_extract(request)
        if not self._la_csrf_verify(request, csrf):
            return web.json_response({"error": "invalid_csrf"}, status=403)

        auth_level = self._la_auth_level(request)
        is_admin = auth_level in {"admin", "localhost"}
        session = self._la_session(request)
        session_login = str((session or {}).get("twitch_login") or "").strip().lower()

        body = await self._la_json_body(request)
        streamer = await self._la_resolve_streamer(
            request=request,
            session_login=session_login,
            is_admin=is_admin,
            requested_streamer=str(body.get("streamer_login") or "").strip(),
        )
        if not streamer:
            return web.json_response({"error": "streamer_not_found"}, status=404)

        cfg = _deep_merge(
            _default_live_announcement_config(),
            body.get("config") if isinstance(body.get("config"), dict) else self._la_load(streamer)["config"],
        )
        role_state = await self._la_ensure_streamer_ping_role(
            streamer_login=streamer,
            create_if_missing=True,
        )
        streamer_ping_role_id = self._la_coerce_role_id(role_state.get("role_id"))
        preview = self._la_preview_payload(
            streamer,
            cfg,
            streamer_ping_role_id=streamer_ping_role_id,
        )

        dm_user_id = self._la_dm_target_user_id(request, streamer_login=streamer)
        if not dm_user_id:
            return web.json_response(
                {
                    "error": "dm_target_not_found",
                    "message": "Kein Discord-User fuer den Testversand gefunden.",
                    "preview": preview,
                    "streamer_ping_role_id": streamer_ping_role_id,
                    "streamer_ping_role_name": str(role_state.get("role_name") or ""),
                },
                status=404,
            )

        sent, message = await self._la_send_test_dm(dm_user_id, streamer, preview)
        return web.json_response(
            {
                "ok": sent,
                "message": message,
                "preview": preview,
                "streamer_ping_role_id": streamer_ping_role_id,
                "streamer_ping_role_name": str(role_state.get("role_name") or ""),
            },
            status=(200 if sent else 503),
        )

    async def api_live_announcement_preview(self, request: web.Request) -> web.StreamResponse:
        self._la_require_auth(request)
        auth_level = self._la_auth_level(request)
        is_admin = auth_level in {"admin", "localhost"}
        session = self._la_session(request)
        session_login = str((session or {}).get("twitch_login") or "").strip().lower()
        streamer = await self._la_resolve_streamer(
            request=request,
            session_login=session_login,
            is_admin=is_admin,
        )
        if not streamer:
            return web.json_response({"error": "streamer_not_found"}, status=404)

        cfg = self._la_load(streamer)["config"]
        raw_cfg = str(request.query.get("config") or "").strip()
        if raw_cfg:
            if len(raw_cfg) > _MAX_PREVIEW_CONFIG_CHARS:
                return web.json_response({"error": "config_too_large"}, status=413)
            try:
                parsed = json.loads(raw_cfg)
            except Exception:
                return web.json_response({"error": "invalid_config_json"}, status=400)
            if isinstance(parsed, dict):
                cfg = _deep_merge(_default_live_announcement_config(), parsed)

        streamer_entry = self._la_load_streamer_entry(streamer)
        streamer_ping_role_id = self._la_coerce_role_id(streamer_entry.get("live_ping_role_id"))
        preview = self._la_preview_payload(
            streamer,
            cfg,
            streamer_ping_role_id=streamer_ping_role_id,
        )
        issues = _validate_config_dict(cfg)
        return web.json_response(
            {
                "streamer_login": streamer,
                "streamer_ping_role_id": streamer_ping_role_id,
                "streamer_ping_role_name": "",
                "preview": preview,
                "validation": issues,
            }
        )

    def _la_require_auth(self, request: web.Request) -> None:
        require_token = getattr(self, "_require_token", None)
        if callable(require_token):
            require_token(request)
        else:
            checker = getattr(self, "_check_v2_auth", None)
            if not (callable(checker) and checker(request)):
                raise web.HTTPUnauthorized(text="missing or invalid authentication")
        # Disallow token-only API access; the builder must run in an authenticated dashboard session.
        # Keep localhost/noauth developer workflow usable.
        if not isinstance(self._la_session(request), dict):
            if self._la_auth_level(request) != "localhost":
                raise web.HTTPUnauthorized(text="dashboard session required")

    def _la_auth_level(self, request: web.Request) -> str:
        getter = getattr(self, "_get_auth_level", None)
        if callable(getter):
            try:
                return str(getter(request) or "none")
            except Exception:
                pass
        return "none"

    def _la_session(self, request: web.Request) -> dict[str, Any] | None:
        admin_getter = getattr(self, "_get_discord_admin_session", None)
        if callable(admin_getter):
            try:
                admin = admin_getter(request)
            except Exception:
                admin = None
            if isinstance(admin, dict):
                copied = dict(admin)
                copied.setdefault("auth_type", "discord_admin")
                return copied
        partner_getter = getattr(self, "_get_dashboard_auth_session", None)
        if callable(partner_getter):
            try:
                partner = partner_getter(request)
            except Exception:
                partner = None
            if isinstance(partner, dict):
                return partner
        return None

    async def _la_resolve_streamer(
        self,
        *,
        request: web.Request,
        session_login: str,
        is_admin: bool,
        requested_streamer: str = "",
    ) -> str:
        candidate = requested_streamer or str(request.query.get("streamer") or "").strip()
        normalized = ""
        normalizer = getattr(self, "_normalize_login", None)
        if callable(normalizer):
            try:
                normalized = str(normalizer(candidate) or "")
            except Exception:
                normalized = ""
        if not normalized:
            normalized = candidate.strip().lower()

        if not normalized:
            if session_login:
                return session_login
            streamers = self._la_list_streamers(session_login=session_login, is_admin=is_admin)
            return streamers[0] if streamers else ""

        if not is_admin and session_login and normalized != session_login:
            return session_login
        return normalized

    def _la_ensure_storage(self) -> None:
        if getattr(self, "_live_announcement_storage_ready", False):
            return
        with _storage.get_conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS twitch_live_announcement_configs (
                    streamer_login TEXT PRIMARY KEY,
                    config_json TEXT NOT NULL,
                    allowed_editor_role_ids TEXT,
                    updated_at TEXT NOT NULL,
                    updated_by TEXT
                )
                """
            )
        self._live_announcement_storage_ready = True

    def _la_list_streamers(self, *, session_login: str, is_admin: bool) -> list[str]:
        if not is_admin and session_login:
            return [session_login]
        self._la_ensure_storage()
        try:
            with _storage.get_conn() as conn:
                rows = conn.execute(
                    "SELECT twitch_login FROM twitch_streamers_partner_state "
                    "WHERE COALESCE(is_partner_active, 0) = 1 ORDER BY twitch_login"
                ).fetchall()
            streamers = [str(row[0] or "").strip().lower() for row in rows if row and row[0]]
        except Exception:
            streamers = []
        return streamers

    def _la_load(self, streamer_login: str) -> dict[str, Any]:
        self._la_ensure_storage()
        login = str(streamer_login or "").strip().lower()
        fallback = {"config": _default_live_announcement_config(), "allowed_editor_role_ids": []}
        if not login:
            return fallback

        try:
            with _storage.get_conn() as conn:
                row = conn.execute(
                    """
                    SELECT config_json, allowed_editor_role_ids
                      FROM twitch_live_announcement_configs
                     WHERE LOWER(streamer_login) = LOWER(?)
                     LIMIT 1
                    """,
                    (login,),
                ).fetchone()
        except Exception:
            log.debug("Could not load live announcement config for %s", login, exc_info=True)
            row = None

        if not row:
            return fallback

        cfg_raw = row[0] if not hasattr(row, "keys") else row["config_json"]
        role_raw = row[1] if not hasattr(row, "keys") else row["allowed_editor_role_ids"]
        return {
            "config": _parse_config_json(str(cfg_raw or "")),
            "allowed_editor_role_ids": self._la_parse_role_ids(role_raw),
        }

    def _la_save(
        self,
        streamer_login: str,
        cfg: dict[str, Any],
        allowed_editor_role_ids: list[int],
        actor: str,
    ) -> None:
        self._la_ensure_storage()
        now_iso = datetime.now(tz=UTC).isoformat(timespec="seconds")
        cfg_json = json.dumps(cfg, ensure_ascii=True, separators=(",", ":"))
        roles_json = json.dumps(allowed_editor_role_ids, ensure_ascii=True, separators=(",", ":"))
        with _storage.get_conn() as conn:
            conn.execute(
                """
                INSERT INTO twitch_live_announcement_configs (
                    streamer_login, config_json, allowed_editor_role_ids, updated_at, updated_by
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (streamer_login) DO UPDATE SET
                    config_json = EXCLUDED.config_json,
                    allowed_editor_role_ids = EXCLUDED.allowed_editor_role_ids,
                    updated_at = EXCLUDED.updated_at,
                    updated_by = EXCLUDED.updated_by
                """,
                (streamer_login.lower(), cfg_json, roles_json, now_iso, actor),
            )

    @staticmethod
    def _la_parse_role_ids(raw: Any) -> list[int]:
        if isinstance(raw, list):
            payload = raw
        else:
            text = str(raw or "").strip()
            if not text:
                return []
            try:
                payload = json.loads(text)
            except Exception:
                return []
        if not isinstance(payload, list):
            return []

        out: list[int] = []
        seen: set[int] = set()
        for item in payload:
            text = str(item or "").strip()
            if not text.isdigit():
                continue
            role_id = int(text)
            if role_id <= 0 or role_id in seen:
                continue
            out.append(role_id)
            seen.add(role_id)
        return out

    @staticmethod
    def _la_coerce_role_id(value: Any) -> int | None:
        text = str(value or "").strip()
        if not text.isdigit():
            return None
        try:
            parsed = int(text)
        except Exception:
            return None
        return parsed if parsed > 0 else None

    @staticmethod
    def _la_is_live_ping_enabled(value: Any) -> bool:
        if isinstance(value, str):
            return value.strip().lower() not in {"0", "false", "no", "off"}
        return bool(value)

    @staticmethod
    def _la_sanitize_live_ping_role_name(login: str) -> str:
        cleaned = _ROLE_NAME_SAFE_RE.sub("", str(login or "").strip())
        cleaned = " ".join(cleaned.split())
        if not cleaned:
            cleaned = "STREAMER"
        return f"{cleaned.upper()} LIVE PING"[:100]

    @staticmethod
    def _la_sanitize_disallowed_mentions_text(value: Any) -> str:
        sanitized = str(value or "")
        sanitized = re.sub(r"@everyone", "@\u200beveryone", sanitized, flags=re.IGNORECASE)
        sanitized = re.sub(r"@here", "@\u200bhere", sanitized, flags=re.IGNORECASE)
        return sanitized

    def _la_load_streamer_entry(self, streamer_login: str) -> dict[str, Any]:
        login = str(streamer_login or "").strip().lower()
        fallback: dict[str, Any] = {
            "twitch_login": login,
            "discord_user_id": None,
            "live_ping_role_id": None,
            "live_ping_enabled": 1,
        }
        if not login:
            return fallback
        try:
            with _storage.get_conn() as conn:
                row = conn.execute(
                    """
                    SELECT twitch_login, discord_user_id, live_ping_role_id, COALESCE(live_ping_enabled, 1) AS live_ping_enabled
                      FROM twitch_streamers
                     WHERE LOWER(twitch_login) = LOWER(?)
                     LIMIT 1
                    """,
                    (login,),
                ).fetchone()
        except Exception:
            log.debug("Could not load streamer entry for live ping role sync (%s)", login, exc_info=True)
            return fallback
        if not row:
            return fallback
        if hasattr(row, "keys"):
            return {
                "twitch_login": str(row.get("twitch_login") or login).strip().lower(),
                "discord_user_id": row.get("discord_user_id"),
                "live_ping_role_id": row.get("live_ping_role_id"),
                "live_ping_enabled": row.get("live_ping_enabled", 1),
            }
        return {
            "twitch_login": str(row[0] or login).strip().lower(),
            "discord_user_id": row[1] if len(row) > 1 else None,
            "live_ping_role_id": row[2] if len(row) > 2 else None,
            "live_ping_enabled": row[3] if len(row) > 3 else 1,
        }

    def _la_persist_streamer_ping_role_id(self, streamer_login: str, role_id: int) -> None:
        login = str(streamer_login or "").strip().lower()
        if not login or role_id <= 0:
            return
        try:
            with _storage.get_conn() as conn:
                conn.execute(
                    """
                    UPDATE twitch_streamers
                       SET live_ping_role_id = ?, live_ping_enabled = COALESCE(live_ping_enabled, 1)
                     WHERE LOWER(twitch_login) = LOWER(?)
                    """,
                    (role_id, login),
                )
        except Exception:
            log.debug("Could not persist live_ping_role_id for %s", login, exc_info=True)

    async def _la_ensure_streamer_ping_role(
        self,
        *,
        streamer_login: str,
        create_if_missing: bool,
    ) -> dict[str, Any]:
        login = str(streamer_login or "").strip().lower()
        if not login:
            return {"role_id": None, "role_name": None, "created": False, "message": "Kein Streamer gewaehlt."}

        entry = self._la_load_streamer_entry(login)
        existing_role_id = self._la_coerce_role_id(entry.get("live_ping_role_id"))
        if not self._la_is_live_ping_enabled(entry.get("live_ping_enabled", 1)):
            return {
                "role_id": existing_role_id,
                "role_name": None,
                "created": False,
                "message": "Live-Ping ist fuer diesen Streamer deaktiviert.",
            }

        bot = self._la_discord_bot()
        if bot is None:
            return {
                "role_id": existing_role_id,
                "role_name": None,
                "created": False,
                "message": "Discord-Bot nicht verbunden. Rolle wird beim naechsten Live-Event erstellt.",
            }

        guilds = list(getattr(bot, "guilds", []) or [])
        guild = guilds[0] if guilds else None
        if guild is None:
            return {
                "role_id": existing_role_id,
                "role_name": None,
                "created": False,
                "message": "Discord-Guild nicht verfuegbar. Rolle kann aktuell nicht synchronisiert werden.",
            }

        role = guild.get_role(existing_role_id) if existing_role_id else None
        role_name = self._la_sanitize_live_ping_role_name(login)
        if role is None:
            role = discord.utils.get(guild.roles, name=role_name)

        created = False
        if role is None and create_if_missing:
            try:
                role = await guild.create_role(
                    name=role_name,
                    reason=f"Auto-created Twitch live ping role for {login}",
                    mentionable=True,
                )
                created = True
            except Exception:
                log.debug("Could not create live ping role for %s via dashboard", login, exc_info=True)
                role = None

        if role is None:
            return {
                "role_id": existing_role_id,
                "role_name": None,
                "created": False,
                "message": "Keine Ping-Rolle gefunden. Sie wird beim Live-Post automatisch erstellt.",
            }

        role_id = int(getattr(role, "id", 0) or 0) or None
        if role_id and role_id != existing_role_id:
            self._la_persist_streamer_ping_role_id(login, role_id)

        if role is not None and not bool(getattr(role, "mentionable", False)):
            try:
                await role.edit(
                    mentionable=True,
                    reason=f"Enable mentions for Twitch live ping role ({login})",
                )
            except Exception:
                log.debug("Could not set live ping role mentionable for %s", login, exc_info=True)

        discord_user_id = self._la_coerce_role_id(entry.get("discord_user_id"))
        if role is not None and discord_user_id:
            member = guild.get_member(discord_user_id)
            if member is None:
                try:
                    member = await guild.fetch_member(discord_user_id)
                except Exception:
                    member = None
            if member is not None and role not in getattr(member, "roles", []):
                try:
                    await member.add_roles(role, reason=f"Live ping role mapping for {login}")
                except Exception:
                    log.debug(
                        "Could not assign live ping role in dashboard for member=%s",
                        discord_user_id,
                        exc_info=True,
                    )

        if created:
            message = f"Ping-Rolle automatisch erstellt: {role.name}"
        else:
            message = f"Ping-Rolle aktiv: {role.name}"
        return {"role_id": role_id, "role_name": role.name if role else None, "created": created, "message": message}

    def _la_csrf_generate(self, request: web.Request) -> str:
        generator = getattr(self, "_csrf_generate_token", None)
        if callable(generator):
            try:
                return str(generator(request) or "")
            except Exception:
                pass
        return ""

    async def _la_csrf_extract(self, request: web.Request) -> str:
        header = str(request.headers.get("X-CSRF-Token") or "").strip()
        if header:
            return header
        body = await self._la_json_body(request)
        return str(body.get("csrf_token") or "").strip()

    def _la_csrf_verify(self, request: web.Request, token: str) -> bool:
        verifier = getattr(self, "_csrf_verify_token", None)
        if not callable(verifier):
            return False
        try:
            return bool(verifier(request, token))
        except Exception:
            return False

    async def _la_json_body(self, request: web.Request) -> dict[str, Any]:
        try:
            data = await request.json()
        except Exception:
            data = {}
        return data if isinstance(data, dict) else {}

    async def _la_can_edit(
        self,
        *,
        request: web.Request,
        streamer_login: str,
        session_login: str,
        is_admin: bool,
        allowed_editor_role_ids: list[int],
    ) -> tuple[bool, str]:
        if is_admin:
            return True, "admin"
        if not session_login:
            return False, "Partner-Session ohne Twitch-Login."
        if session_login and streamer_login != session_login:
            return False, "Nur eigener Streamer erlaubt."
        if not allowed_editor_role_ids:
            return True, "ok"
        user_id = self._la_dm_target_user_id(request, streamer_login=streamer_login)
        if not user_id:
            return False, "Keine Discord-ID fuer Rollencheck gefunden."
        member_roles = await self._la_member_role_ids(user_id)
        if member_roles.intersection(set(allowed_editor_role_ids)):
            return True, "ok"
        return False, "Dir fehlt eine erlaubte Editor-Rolle."

    def _la_actor_label(self, request: web.Request) -> str:
        level = self._la_auth_level(request)
        if level in {"admin", "localhost"}:
            uid = self._la_dm_target_user_id(request, streamer_login="")
            return f"discord:{uid}" if uid else "admin"
        session = self._la_session(request)
        login = str((session or {}).get("twitch_login") or "").strip().lower()
        return f"twitch:{login}" if login else level

    def _la_dm_target_user_id(self, request: web.Request, *, streamer_login: str) -> int | None:
        session = self._la_session(request) or {}
        uid = str(session.get("user_id") or "").strip()
        if uid.isdigit():
            return int(uid)
        if not streamer_login:
            return None
        try:
            with _storage.get_conn() as conn:
                row = conn.execute(
                    "SELECT discord_user_id FROM twitch_streamers WHERE LOWER(twitch_login)=LOWER(?) LIMIT 1",
                    (streamer_login,),
                ).fetchone()
        except Exception:
            row = None
        value = row[0] if row and not hasattr(row, "keys") else (row["discord_user_id"] if row else "")
        text = str(value or "").strip()
        return int(text) if text.isdigit() else None

    async def _la_member_role_ids(self, user_id: int) -> set[int]:
        bot = self._la_discord_bot()
        if bot is None:
            return set()
        role_ids: set[int] = set()
        for guild in list(getattr(bot, "guilds", []) or []):
            member = guild.get_member(user_id)
            if member is None:
                try:
                    member = await guild.fetch_member(user_id)
                except Exception:
                    member = None
            if member is None:
                continue
            for role in getattr(member, "roles", []) or []:
                role_id = getattr(role, "id", None)
                if role_id:
                    role_ids.add(int(role_id))
        return role_ids

    def _la_discord_bot(self) -> Any | None:
        raid_bot = getattr(self, "_raid_bot", None)
        if raid_bot is None:
            return None
        auth_manager = getattr(raid_bot, "auth_manager", None)
        bot = getattr(auth_manager, "_discord_bot", None) if auth_manager else None
        if bot is None:
            bot = getattr(raid_bot, "_discord_bot", None)
        return bot

    async def _la_send_test_dm(
        self,
        user_id: int,
        streamer_login: str,
        preview_payload: dict[str, Any],
    ) -> tuple[bool, str]:
        bot = self._la_discord_bot()
        if bot is None:
            return False, "Discord Bot ist nicht verfuegbar."
        try:
            user = bot.get_user(user_id)
            if user is None:
                user = await bot.fetch_user(user_id)
        except Exception:
            user = None
        if user is None:
            return False, "Discord User konnte nicht geladen werden."

        content = str(preview_payload.get("content") or "").strip()
        embed_payload = (
            preview_payload.get("embed")
            if isinstance(preview_payload.get("embed"), dict)
            else {}
        )
        button_payload = (
            preview_payload.get("button")
            if isinstance(preview_payload.get("button"), dict)
            else {}
        )

        embed = None
        if embed_payload:
            embed_color_raw = embed_payload.get("color")
            try:
                embed_color = int(embed_color_raw or 0x9146FF)
            except Exception:
                embed_color = 0x9146FF
            embed = discord.Embed(
                title=str(embed_payload.get("title") or "").strip()[:256] or None,
                description=str(embed_payload.get("description") or "").strip()[:4096] or None,
                color=embed_color,
            )
            author_payload = (
                embed_payload.get("author")
                if isinstance(embed_payload.get("author"), dict)
                else {}
            )
            author_name = str(author_payload.get("name") or "").strip()
            if author_name and bool(author_payload.get("enabled", True)):
                embed.set_author(
                    name=author_name[:256],
                    icon_url=str(author_payload.get("icon_url") or "").strip() or None,
                    url=str(author_payload.get("url") or "").strip() or None,
                )
            for field in embed_payload.get("fields") or []:
                if not isinstance(field, dict):
                    continue
                name = str(field.get("name") or "").strip()
                value = str(field.get("value") or "").strip()
                if not name or not value:
                    continue
                embed.add_field(
                    name=name[:256],
                    value=value[:1024],
                    inline=bool(field.get("inline", True)),
                )
            thumb_payload = (
                embed_payload.get("thumbnail")
                if isinstance(embed_payload.get("thumbnail"), dict)
                else {}
            )
            thumb_url = str(thumb_payload.get("url") or "").strip()
            if thumb_url:
                embed.set_thumbnail(url=thumb_url)
            image_payload = (
                embed_payload.get("image")
                if isinstance(embed_payload.get("image"), dict)
                else {}
            )
            image_url = str(image_payload.get("url") or "").strip()
            if image_url:
                embed.set_image(url=image_url)
            footer_payload = (
                embed_payload.get("footer")
                if isinstance(embed_payload.get("footer"), dict)
                else {}
            )
            footer_text = str(footer_payload.get("text") or "").strip()
            footer_with_label = "TEST-Preview"
            if footer_text:
                footer_with_label = f"{footer_text} | TEST-Preview"
            embed.set_footer(
                text=footer_with_label[:2048],
                icon_url=str(footer_payload.get("icon_url") or "").strip() or None,
            )

        view = None
        if bool(button_payload.get("enabled", True)):
            button_url = str(button_payload.get("url") or "").strip()
            if button_url:
                view = discord.ui.View(timeout=180)
                view.add_item(
                    discord.ui.Button(
                        style=discord.ButtonStyle.link,
                        label=str(button_payload.get("label") or "Auf Twitch ansehen")[:80],
                        url=button_url,
                    )
                )

        try:
            await user.send(
                content=content or f"**TEST-Preview** fuer `{streamer_login}`",
                embed=embed,
                view=view,
                allowed_mentions=discord.AllowedMentions.none(),
            )
        except Exception:
            return False, "DM konnte nicht gesendet werden."
        return True, "Test-DM mit Embed gesendet."

    def _la_preview_payload(
        self,
        streamer_login: str,
        cfg: dict[str, Any],
        *,
        streamer_ping_role_id: int | None = None,
    ) -> dict[str, Any]:
        normalized_login = str(streamer_login or "").strip().lower() or "streamer"
        sample_stream = {
            "user_login": normalized_login,
            "user_name": normalized_login.capitalize() if normalized_login else "Streamer",
            "title": "Ranked Grind bis Top 100",
            "viewer_count": 42,
            "started_at": datetime.now(tz=UTC).isoformat(timespec="seconds"),
            "language": "de",
            "game_name": "Deadlock",
            "tags": ["Deadlock", "Community"],
            "thumbnail_url": f"https://static-cdn.jtvnw.net/previews-ttv/live_user_{normalized_login}-{{width}}x{{height}}.jpg",
            "profile_image_url": "https://static-cdn.jtvnw.net/jtv_user_pictures/xarth/404_user_70x70.png",
        }
        mentions = cfg.get("mentions") if isinstance(cfg.get("mentions"), dict) else {}
        mention_role = ""
        if bool(mentions.get("enabled", True)) and streamer_ping_role_id:
            mention_role = f"<@&{streamer_ping_role_id}>"
        else:
            role_id = str(mentions.get("role_id") or "").strip()
            if role_id.isdigit():
                mention_role = f"<@&{role_id}>"
        context = build_template_context(normalized_login, sample_stream)
        context["mention_role"] = mention_role
        context["rolle"] = mention_role
        context["url"] = f"https://www.twitch.tv/{normalized_login}"
        config_obj = LiveAnnouncementConfig.from_dict(_to_template_config(cfg))
        payload = render_announcement_payload(config_obj, context)
        payload["content"] = self._la_sanitize_disallowed_mentions_text(payload.get("content"))
        embed_cfg = cfg.get("embed") if isinstance(cfg.get("embed"), dict) else {}
        author_cfg = embed_cfg.get("author") if isinstance(embed_cfg.get("author"), dict) else {}
        payload.setdefault("embed", {}).setdefault("author", {})
        payload["embed"]["author"]["enabled"] = bool(author_cfg.get("enabled", True))
        payload.setdefault("button", {})
        payload["button"]["enabled"] = bool((cfg.get("button") or {}).get("enabled", True))
        payload["button"]["label"] = str((cfg.get("button") or {}).get("label") or payload["button"].get("label") or "Auf Twitch ansehen")
        embed_color = payload.get("embed", {}).get("color")
        if isinstance(embed_color, int):
            payload.setdefault("embed", {})["color"] = embed_color
        payload["meta"] = {
            "placeholders": list(SUPPORTED_PLACEHOLDERS),
            "sample_stream": sample_stream,
        }
        payload["validation"] = _validate_config_dict(cfg)
        return payload

    def _la_render_page(
        self,
        *,
        streamer_login: str,
        streamers: list[str],
        csrf_token: str,
        auth_level: str,
        config: dict[str, Any],
        allowed_editor_role_ids: list[int],
        preview: dict[str, Any],
        streamer_ping_role_id: int | None,
        streamer_ping_role_name: str | None,
        role_status_message: str,
    ) -> str:
        options = "".join(
            f"<option value='{html.escape(login, quote=True)}' {'selected' if login == streamer_login else ''}>{html.escape(login)}</option>"
            for login in streamers
        )
        initial = json.dumps(
            {
                "streamer_login": streamer_login,
                "auth_level": auth_level,
                "csrf_token": csrf_token,
                "config": config,
                "allowed_editor_role_ids": allowed_editor_role_ids,
                "streamer_ping_role_id": streamer_ping_role_id,
                "streamer_ping_role_name": streamer_ping_role_name or "",
                "role_status_message": role_status_message,
                "preview": preview,
                "placeholders": list(SUPPORTED_PLACEHOLDERS),
            },
            ensure_ascii=True,
        )
        initial = initial.replace("</", "<\\/")
        return f"""<!doctype html>
<html lang='de'>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width,initial-scale=1'>
  <title>Live Announcement Builder</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700&family=Sora:wght@500;600;700&display=swap');
    :root {{
      color-scheme:dark;
      --bg:#07151d;
      --card:#102635;
      --card2:#0f2230;
      --bd:rgba(194,221,240,.14);
      --bd-strong:rgba(194,221,240,.3);
      --txt:#e9f1f7;
      --muted:#9bb3c5;
      --primary:#ff7a18;
      --primary-hover:#ff8d39;
      --accent:#10b7ad;
      --accent-hover:#1dd4ca;
      --ok:#2ecc71;
      --err:#ff6b5e;
      --discord-bg:#313338;
      --discord-card:#2b2d31;
      --discord-embed:#1f2023;
      --discord-link:#00a8fc;
      --discord-btn:#5865f2;
      --discord-btn-hover:#6d77ff;
    }}
    * {{ box-sizing:border-box; }}
    body {{
      margin:0;
      padding:20px;
      color:var(--txt);
      font-family:"Manrope","Segoe UI",sans-serif;
      background:
        radial-gradient(1200px 520px at 90% -10%, rgba(255,122,24,.18), transparent 65%),
        radial-gradient(900px 460px at 12% -20%, rgba(16,183,173,.24), transparent 60%),
        linear-gradient(160deg, #07151d 0%, #081a24 55%, #0a202c 100%);
      min-height:100vh;
    }}
    .layout {{ display:grid; grid-template-columns:1.1fr .9fr; gap:14px; align-items:start; }}
    @media (max-width:1100px) {{ .layout {{ grid-template-columns:1fr; }} }}
    .card {{
      position:relative;
      overflow:hidden;
      background:linear-gradient(160deg, rgba(16,38,53,.92) 0%, rgba(10,30,42,.92) 100%);
      border:1px solid var(--bd);
      border-radius:16px;
      padding:14px;
      box-shadow:0 10px 30px rgba(0,0,0,.26), inset 0 1px 0 rgba(255,255,255,.05);
      backdrop-filter:blur(10px);
    }}
    .card::after {{
      content:"";
      position:absolute;
      inset:0;
      pointer-events:none;
      background:linear-gradient(120deg, rgba(255,255,255,.06), transparent 35%);
      opacity:.35;
    }}
    .card > * {{ position:relative; z-index:1; }}
    .top {{ display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin-bottom:10px; }}
    .builder-stack {{ display:grid; gap:10px; }}
    .cfg-block {{
      border:1px solid var(--bd);
      border-radius:12px;
      padding:10px;
      background:linear-gradient(160deg, rgba(13,34,47,.78), rgba(8,27,39,.78));
      box-shadow:inset 0 1px 0 rgba(255,255,255,.04);
      backdrop-filter:blur(8px);
    }}
    .cfg-head {{
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:10px;
      margin-bottom:8px;
    }}
    .cfg-head h3 {{ margin:0; }}
    .cfg-tools {{ display:flex; gap:6px; }}
    .move-btn {{
      border:1px solid var(--bd);
      background:rgba(17,43,59,.9);
      color:#d8e5ef;
      border-radius:8px;
      width:30px;
      height:30px;
      cursor:pointer;
      font-weight:700;
    }}
    .move-btn:hover:not(:disabled) {{ border-color:var(--accent); color:#d6fffb; }}
    .move-btn:disabled {{ opacity:.45; cursor:not-allowed; }}
    .row {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:8px; }}
    @media (max-width:780px) {{ .row {{ grid-template-columns:1fr; }} }}
    input,textarea,select {{
      width:100%;
      border:1px solid var(--bd);
      background:var(--card2);
      color:var(--txt);
      border-radius:10px;
      padding:9px 10px;
      font:inherit;
    }}
    input:focus,textarea:focus,select:focus {{
      outline:none;
      border-color:var(--accent);
      box-shadow:0 0 0 2px rgba(16,183,173,.16);
    }}
    input[readonly] {{ color:var(--muted); background:rgba(7,21,29,.7); }}
    textarea {{ min-height:88px; resize:vertical; }}
    h3 {{
      margin:10px 0 7px;
      font-size:13px;
      font-family:"Sora","Manrope",sans-serif;
      letter-spacing:.02em;
      color:#d8e5ef;
    }}
    .btn {{
      border:1px solid transparent;
      border-radius:10px;
      padding:9px 12px;
      font-weight:700;
      cursor:pointer;
      text-decoration:none;
      transition:transform .14s ease, border-color .14s ease, background .14s ease;
    }}
    .btn:hover {{ transform:translateY(-1px); }}
    .btn.primary {{ background:linear-gradient(135deg,var(--primary),#ff9c4f); color:#3b1500; }}
    .btn.primary:hover {{ background:linear-gradient(135deg,var(--primary-hover),#ffb06d); }}
    .btn.warn {{ background:linear-gradient(135deg,var(--accent),#6ae0d8); color:#022e2b; }}
    .btn.ghost {{ background:rgba(17,43,59,.85); color:#d1e4f3; border-color:var(--bd); text-decoration:none; }}
    .btn.ghost:hover {{ border-color:var(--bd-strong); }}
    .actions {{ display:flex; align-items:center; gap:8px; flex-wrap:wrap; }}
    .pill {{
      display:inline-flex;
      align-items:center;
      border-radius:999px;
      padding:5px 9px;
      background:rgba(16,183,173,.12);
      color:#c6f4f1;
      border:1px solid var(--bd);
      font-size:12px;
      font-weight:700;
      margin:2px;
      cursor:pointer;
    }}
    .pill:hover {{ border-color:var(--accent); background:rgba(16,183,173,.2); }}
    .small {{ font-size:12px; color:var(--muted); }}
    #roleStatus {{
      margin-top:4px;
    }}
    details.advanced {{
      margin-top:10px;
      border:1px solid var(--bd);
      border-radius:10px;
      padding:10px;
      background:rgba(8,23,32,.54);
    }}
    details.advanced > summary {{
      cursor:pointer;
      list-style:none;
      font-weight:700;
      color:#d8e5ef;
      margin-bottom:8px;
    }}
    details.advanced > summary::-webkit-details-marker {{ display:none; }}
    .field-row {{
      display:grid;
      grid-template-columns:minmax(100px,.9fr) minmax(140px,1.3fr) auto auto auto auto;
      gap:8px;
      align-items:center;
      margin-bottom:6px;
      border:1px solid var(--bd);
      border-radius:10px;
      padding:8px;
      background:rgba(8,23,32,.74);
    }}
    .field-row .btn {{ padding:7px 9px; border-radius:8px; font-size:12px; }}
    .field-row label {{ display:inline-flex; align-items:center; gap:6px; color:var(--muted); font-size:12px; }}
    .field-row label input {{ width:14px; height:14px; margin:0; }}
    @media (max-width:860px) {{ .field-row {{ grid-template-columns:1fr; }} }}
    .validation {{ display:grid; gap:6px; }}
    .validation .item {{
      border:1px solid rgba(255,107,94,.45);
      background:rgba(255,107,94,.12);
      border-radius:10px;
      padding:8px;
      color:#ffc6c0;
      font-size:12px;
    }}
    .status {{ min-height:1.3em; margin-top:4px; font-size:13px; color:var(--muted); font-weight:600; }}
    .status.ok {{ color:var(--ok); }}
    .status.err {{ color:var(--err); }}
    .preview {{ background:var(--discord-bg); border:1px solid #4e5058; border-radius:12px; padding:12px; }}
    .discord-head {{ display:flex; gap:10px; align-items:flex-start; }}
    .discord-avatar {{ width:40px; height:40px; border-radius:999px; border:1px solid rgba(255,255,255,.2); object-fit:cover; }}
    .discord-meta {{ display:flex; gap:8px; align-items:baseline; flex-wrap:wrap; }}
    .discord-name {{ font-weight:700; color:#fff; font-size:14px; }}
    .discord-time {{ color:#b4b7bd; font-size:12px; }}
    .embed {{
      margin-top:8px;
      background:var(--discord-embed);
      border-left:4px solid var(--accent);
      border-radius:4px;
      padding:10px 10px 10px 12px;
      position:relative;
      display:grid;
      gap:8px;
    }}
    .pv-author {{ display:flex; align-items:center; gap:6px; font-size:12px; font-weight:700; color:#fff; }}
    .pv-author img {{ width:18px; height:18px; border-radius:999px; object-fit:cover; display:none; }}
    .pv-title {{ color:var(--discord-link); text-decoration:none; font-size:16px; font-weight:700; }}
    .pv-title.no-link {{ color:#fff; pointer-events:none; }}
    .pv-title:hover {{ text-decoration:underline; }}
    .pv-desc {{ color:#dbdee1; font-size:14px; white-space:pre-wrap; line-height:1.45; }}
    .pv-fields {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:8px; }}
    .pv-field {{ min-width:0; }}
    .pv-field.full {{ grid-column:1/-1; }}
    .pv-field-name {{ font-size:12px; font-weight:700; color:#fff; margin-bottom:2px; }}
    .pv-field-value {{ font-size:13px; color:#dbdee1; white-space:pre-wrap; word-break:break-word; }}
    .pv-image {{ width:100%; max-height:260px; object-fit:cover; border-radius:8px; border:1px solid rgba(255,255,255,.1); display:none; }}
    .pv-thumb {{ position:absolute; top:10px; right:10px; width:76px; height:76px; border-radius:8px; border:1px solid rgba(255,255,255,.1); object-fit:cover; display:none; }}
    .pv-footer {{ display:flex; align-items:center; gap:6px; color:#b4b7bd; font-size:12px; flex-wrap:wrap; }}
    .pv-footer img {{ width:16px; height:16px; border-radius:999px; object-fit:cover; display:none; }}
    #pvBtn {{ display:inline-flex; margin-top:8px; padding:9px 12px; border-radius:10px; background:var(--discord-btn); color:#fff; text-decoration:none; font-weight:700; }}
    #pvBtn:hover {{ background:var(--discord-btn-hover); }}
    @media (max-width:780px) {{
      .pv-fields {{ grid-template-columns:1fr; }}
      .pv-thumb {{ position:static; width:96px; height:96px; }}
    }}
  </style>
</head>
<body>
  <div class='actions' style='margin-bottom:10px; justify-content:space-between;'>
      <div>
        <div class='small' style='text-transform:uppercase;letter-spacing:.12em;font-weight:700;'>Go-Live Builder</div>
        <strong style='font-family:Sora,Manrope,sans-serif;font-size:20px;'>Discord Announcement Designer</strong>
        <div class='small'>Schnell-Setup fuer Streamer: Rolle automatisch, Nachricht anpassen, Preview direkt sehen.</div>
      </div>
    <a class='btn ghost' href='/twitch/dashboard?streamer={html.escape(streamer_login, quote=True)}'>Zurueck zum Dashboard</a>
  </div>
  <div class='layout'>
    <section class='card'>
      <div class='top'>
        <select id='streamerSelect'>{options}</select>
        <button class='btn warn' id='testBtn' type='button'>Test per DM</button>
        <button class='btn primary' id='saveBtn' type='button'>Aenderungen speichern</button>
      </div>

      <div id='builderBlocks' class='builder-stack'>
        <section class='cfg-block' data-block='ping' style='border-color:var(--accent); background:linear-gradient(160deg, rgba(16,183,173,.08), rgba(8,27,39,.78));'>
          <div class='cfg-head'>
            <h3 style='font-size:15px; display:flex; align-items:center; gap:6px;'>
              <span style='font-size:18px;'>&#128276;</span> Ping-Rolle
            </h3>
            <div class='cfg-tools'>
              <button type='button' class='move-btn' data-move='up' aria-label='Block nach oben'>&#8593;</button>
              <button type='button' class='move-btn' data-move='down' aria-label='Block nach unten'>&#8595;</button>
            </div>
          </div>
          <div style='display:grid; gap:8px;'>
            <div style='display:grid; grid-template-columns:auto 1fr auto; gap:8px; align-items:center;'>
              <span class='small' style='white-space:nowrap; font-weight:700;'>Rolle:</span>
              <span id='pingRoleName' style='font-weight:600; color:var(--accent);'>&mdash;</span>
              <span class='small' style='color:var(--muted);'>ID:</span>
            </div>
            <div style='display:flex; gap:6px; align-items:center;'>
              <input id='pingRoleId' readonly placeholder='Wird automatisch beim ersten Go-Live erstellt' style='flex:1; font-family:monospace; font-size:13px;'>
              <button type='button' class='btn ghost' id='copyRoleIdBtn' style='padding:7px 10px; font-size:12px; white-space:nowrap;' title='Rolle-ID kopieren'>Kopieren</button>
            </div>
            <div id='roleStatus' style='padding:8px 10px; border-radius:10px; border:1px solid var(--bd); background:rgba(16,183,173,.08); color:#c6f4f1; font-size:13px;'></div>
            <div class='small' style='color:var(--muted); line-height:1.5;'>
              User koennen sich selbst fuer Go-Live-Benachrichtigungen eintragen &mdash;
              ueber das <strong>Self-Role-Menue</strong> im Discord oder per <strong>Bot-Command</strong>.
              Die Rolle wird automatisch vom Bot erstellt und verwaltet.
            </div>
            <label class='small'><input id='mentionsEnabled' type='checkbox'> Rolle in der Go-Live-Nachricht erwaehnen (<code style='background:rgba(255,255,255,.08); padding:1px 5px; border-radius:4px;'>{{{{rolle}}}}</code>)</label>
          </div>
          <details class='advanced' style='margin-top:8px;'>
            <summary>Zugriff &amp; Editor-Rollen</summary>
            <div style='margin-top:6px;'>
              <input id='editorRoles' placeholder='Editor Role IDs (kommagetrennt)'>
              <div class='small' style='margin-top:4px; color:var(--muted);'>Discord-Rollen-IDs, die diese Konfiguration bearbeiten duerfen.</div>
            </div>
          </details>
        </section>

        <section class='cfg-block' data-block='message'>
          <div class='cfg-head'>
            <h3>Nachricht</h3>
            <div class='cfg-tools'>
              <button type='button' class='move-btn' data-move='up' aria-label='Block nach oben'>&#8593;</button>
              <button type='button' class='move-btn' data-move='down' aria-label='Block nach unten'>&#8595;</button>
            </div>
          </div>
          <textarea id='contentTpl' placeholder='Nachrichtentext ueber dem Embed (Platzhalter: {{{{channel}}}}, {{{{rolle}}}}, ...)'></textarea>
          <div id='placeholderPills' style='margin-top:6px;'></div>
        </section>

        <section class='cfg-block' data-block='embed'>
          <div class='cfg-head'>
            <h3>Embed-Designer</h3>
            <div class='cfg-tools'>
              <button type='button' class='move-btn' data-move='up' aria-label='Block nach oben'>&#8593;</button>
              <button type='button' class='move-btn' data-move='down' aria-label='Block nach unten'>&#8595;</button>
            </div>
          </div>
          <div class='row'>
            <input id='embedColor' placeholder='#18c5e5'>
            <input id='authorName' placeholder='LIVE: {{channel}}'>
            <select id='authorIconMode'>
              <option value='twitch_logo'>Twitch Logo</option>
              <option value='channel_avatar'>Kanal Avatar</option>
              <option value='none'>Kein Icon</option>
            </select>
          </div>
          <div class='actions'>
            <label class='small'><input id='authorEnabled' type='checkbox'> Author anzeigen</label>
            <label class='small'><input id='authorLinkEnabled' type='checkbox'> Author auf Kanal verlinken</label>
          </div>
          <div class='row' style='margin-top:8px;'>
            <input id='embedTitle' placeholder='{{channel}} ist LIVE in Deadlock!'>
            <select id='descMode'>
              <option value='stream_title'>Auto Streamtitel</option>
              <option value='custom'>Custom Text</option>
              <option value='custom_plus_title'>Custom + Streamtitel</option>
            </select>
            <textarea id='embedDesc' style='grid-column:1/-1;' placeholder='Beschreibung'></textarea>
          </div>
          <div class='actions'>
            <label class='small'><input id='titleLinkEnabled' type='checkbox'> Titel-Link auf {{url}}</label>
            <label class='small'><input id='shortenEnabled' type='checkbox'> Zu lange Texte kuerzen</label>
          </div>
        </section>

        <section class='cfg-block' data-block='button'>
          <div class='cfg-head'>
            <h3>Button</h3>
            <div class='cfg-tools'>
              <button type='button' class='move-btn' data-move='up' aria-label='Block nach oben'>↑</button>
              <button type='button' class='move-btn' data-move='down' aria-label='Block nach unten'>↓</button>
            </div>
          </div>
          <div class='row'>
            <label class='small'><input id='buttonEnabled' type='checkbox'> Button anzeigen</label>
            <input id='buttonLabel' placeholder='Auf Twitch ansehen'>
            <input id='buttonUrl' value='{{url}}' readonly>
          </div>
        </section>

        <section class='cfg-block' data-block='advanced'>
          <div class='cfg-head'>
            <h3>Mehr Optionen (Optional)</h3>
            <div class='cfg-tools'>
              <button type='button' class='move-btn' data-move='up' aria-label='Block nach oben'>↑</button>
              <button type='button' class='move-btn' data-move='down' aria-label='Block nach unten'>↓</button>
            </div>
          </div>
          <details class='advanced'>
            <summary>Felder, Bilder, Footer</summary>
            <h3>Fields</h3>
            <div id='fieldsWrap'></div>
            <div class='actions'>
              <button class='btn ghost' type='button' id='addFieldBtn'>Feld +</button>
              <button class='btn ghost' type='button' id='presetBtn'>Preset Viewer + Kategorie</button>
              <button class='btn ghost' type='button' id='presetMetaBtn'>Preset Start + Sprache + Tags</button>
            </div>

            <h3>Bilder & Footer</h3>
            <div class='row'>
              <select id='thumbMode'>
                <option value='none'>Thumbnail aus</option>
                <option value='channel_avatar'>Kanal Avatar</option>
                <option value='custom_url'>Custom URL</option>
              </select>
              <input id='thumbUrl' placeholder='Thumbnail URL'>
              <label class='small'><input id='useStreamImage' type='checkbox'> Stream-Thumbnail verwenden</label>
              <input id='imageUrl' placeholder='Custom Image URL'>
              <select id='imageFormat'><option value='16:9'>16:9</option><option value='4:3'>4:3</option></select>
              <label class='small'><input id='imageCb' type='checkbox'> Cache-Buster</label>
              <input id='footerText' placeholder='Footer Text'>
              <select id='footerTs'><option value='started_at'>Startzeit</option><option value='now'>Jetzt</option><option value='none'>Aus</option></select>
            </div>
          </details>
        </section>
      </div>

      <div class='validation' id='validation'></div>
      <div class='status' id='status'></div>
    </section>

    <aside class='card'>
      <h3>Live Preview</h3>
      <div class='preview'>
        <div class='discord-head'>
          <img id='pvBotAvatar' class='discord-avatar' alt='Bot Avatar'>
          <div style='min-width:0; width:100%;'>
            <div class='discord-meta'>
              <span class='discord-name'>Deadlock Bot</span>
              <span id='pvMsgTime' class='discord-time'>Heute um --:--</span>
            </div>
            <div id='pvContent' class='pv-desc'>(leer)</div>
          </div>
        </div>
        <div class='embed' id='pvEmbed'>
          <img id='pvThumb' class='pv-thumb' alt='Thumbnail'>
          <div id='pvAuthorWrap' class='pv-author'>
            <img id='pvAuthorIcon' alt='Author Icon'>
            <a id='pvAuthor' class='pv-title no-link' href='#' target='_blank' rel='noopener noreferrer'>LIVE</a>
          </div>
          <a id='pvTitle' class='pv-title no-link' href='#' target='_blank' rel='noopener noreferrer'></a>
          <div id='pvDesc' class='pv-desc'></div>
          <img id='pvImage' class='pv-image' alt='Stream Preview'>
          <div id='pvFields' class='pv-fields'></div>
          <div id='pvFooter' class='pv-footer'>
            <img id='pvFooterIcon' alt='Footer Icon'>
            <span id='pvFooterText'></span>
            <span id='pvFooterTime'></span>
          </div>
        </div>
        <a id='pvBtn' href='#' target='_blank' rel='noopener noreferrer'>Auf Twitch ansehen</a>
      </div>
    </aside>
  </div>

  <script>
    const ST = {initial};
    const DEFAULT_BLOCK_ORDER = ['ping', 'message', 'embed', 'button', 'advanced'];
    const S = {{
      streamer: ST.streamer_login || '',
      csrf: ST.csrf_token || '',
      config: structuredClone(ST.config || {{}}),
      allowedRoles: [...(ST.allowed_editor_role_ids || [])],
      streamerPingRoleId: Number(ST.streamer_ping_role_id || 0) || null,
      streamerPingRoleName: String(ST.streamer_ping_role_name || ''),
      roleStatusMessage: String(ST.role_status_message || ''),
      preview: ST.preview || {{}},
      blockOrder: [],
      timer: null
    }};
    const E = (id) => document.getElementById(id);
    const esc = (value) =>
      String(value || '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    const WATCH_IDS = ['editorRoles','mentionsEnabled','contentTpl','embedColor','authorName','authorIconMode','authorEnabled','authorLinkEnabled','embedTitle','descMode','embedDesc','titleLinkEnabled','shortenEnabled','thumbMode','thumbUrl','useStreamImage','imageUrl','imageFormat','imageCb','footerText','footerTs','buttonEnabled','buttonLabel'];
    const BOT_AVATAR = 'https://static-cdn.jtvnw.net/jtv_user_pictures/2f6f9be7-41f7-4fd1-8ca8-13213e63ed05-profile_image-300x300.png';

    function toHexColor(raw) {{
      if (typeof raw === 'number') return '#' + raw.toString(16).padStart(6, '0');
      const text = String(raw || '').trim();
      if (text.startsWith('#') && text.length === 7) return text;
      if (/^[0-9a-fA-F]{{6}}$/.test(text)) return '#' + text;
      return '#10b7ad';
    }}

    function formatTime(isoText) {{
      if (!isoText) return '';
      const dt = new Date(isoText);
      if (Number.isNaN(dt.getTime())) return '';
      return dt.toLocaleString('de-DE', {{ day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' }});
    }}

    function setImage(node, url, fallbackUrl = '') {{
      const primary = String(url || '').trim();
      const fallback = String(fallbackUrl || '').trim();
      let triedFallback = false;
      const applySrc = (src) => {{
        node.style.display = src ? 'block' : 'none';
        node.src = src || '';
      }};
      node.onerror = () => {{
        if (!triedFallback && fallback) {{
          triedFallback = true;
          applySrc(fallback);
          return;
        }}
        node.style.display = 'none';
      }};
      if (primary) {{
        applySrc(primary);
        return;
      }}
      if (fallback) {{
        triedFallback = true;
        applySrc(fallback);
        return;
      }}
      applySrc('');
    }}

    function norm() {{
      S.config.mentions = S.config.mentions || {{}};
      S.config.embed = S.config.embed || {{}};
      S.config.embed.author = S.config.embed.author || {{}};
      S.config.embed.thumbnail = S.config.embed.thumbnail || {{}};
      S.config.embed.image = S.config.embed.image || {{}};
      S.config.embed.footer = S.config.embed.footer || {{}};
      S.config.button = S.config.button || {{}};
      S.config.ui = S.config.ui || {{}};
      S.config.embed.fields = Array.isArray(S.config.embed.fields) ? S.config.embed.fields : [];
      S.config.button.url_template = '{{url}}';
      S.blockOrder = normalizeBlockOrder(S.config.ui.block_order);
    }}

    function normalizeBlockOrder(raw) {{
      const src = Array.isArray(raw) ? raw : [];
      const out = [];
      src.forEach((value) => {{
        const id = String(value || '').trim();
        if (!DEFAULT_BLOCK_ORDER.includes(id)) return;
        if (out.includes(id)) return;
        out.push(id);
      }});
      DEFAULT_BLOCK_ORDER.forEach((id) => {{ if (!out.includes(id)) out.push(id); }});
      return out;
    }}

    function roleIdsCsv(text) {{
      return String(text || '').split(',').map((v) => v.trim()).filter((v) => /^\\d+$/.test(v)).map((v) => Number(v));
    }}

    function applyBlockOrder() {{
      const container = E('builderBlocks');
      if (!container) return;
      S.blockOrder = normalizeBlockOrder(S.blockOrder);
      S.blockOrder.forEach((blockId) => {{
        const node = container.querySelector(`.cfg-block[data-block="${{blockId}}"]`);
        if (node) container.appendChild(node);
      }});
      S.blockOrder.forEach((blockId, index) => {{
        const node = container.querySelector(`.cfg-block[data-block="${{blockId}}"]`);
        if (!node) return;
        const up = node.querySelector('[data-move="up"]');
        const down = node.querySelector('[data-move="down"]');
        if (up) up.disabled = index <= 0;
        if (down) down.disabled = index >= S.blockOrder.length - 1;
      }});
    }}

    function moveBlock(blockId, direction) {{
      const idx = S.blockOrder.indexOf(blockId);
      if (idx < 0) return;
      const next = direction === 'up' ? idx - 1 : idx + 1;
      if (next < 0 || next >= S.blockOrder.length) return;
      const tmp = S.blockOrder[next];
      S.blockOrder[next] = S.blockOrder[idx];
      S.blockOrder[idx] = tmp;
      applyBlockOrder();
      onMutate();
    }}

    function bindBlockMoveButtons() {{
      document.querySelectorAll('#builderBlocks .cfg-block').forEach((node) => {{
        const blockId = String(node.getAttribute('data-block') || '');
        node.querySelectorAll('[data-move]').forEach((btn) => {{
          btn.addEventListener('click', () => moveBlock(blockId, String(btn.getAttribute('data-move') || '')));
        }});
      }});
    }}

    function renderRoleInfo() {{
      const roleIdText = S.streamerPingRoleId ? String(S.streamerPingRoleId) : '';
      const roleName = S.streamerPingRoleName || '';
      E('pingRoleId').value = roleIdText;
      E('pingRoleName').textContent = roleName || (roleIdText ? `ID: ${{roleIdText}}` : 'Noch nicht erstellt');
      E('pingRoleName').style.color = roleName ? 'var(--accent)' : 'var(--muted)';
      const copyBtn = E('copyRoleIdBtn');
      if (copyBtn) {{ copyBtn.style.display = roleIdText ? '' : 'none'; }}
      const fallbackStatus = roleIdText
        ? `Ping-Rolle verknuepft (ID: ${{roleIdText}}).`
        : 'Wird automatisch beim ersten Go-Live erstellt.';
      E('roleStatus').textContent = S.roleStatusMessage || fallbackStatus;
    }}

    function fillForm() {{
      norm();
      E('mentionsEnabled').checked = Boolean(S.config.mentions.enabled);
      E('editorRoles').value = (S.allowedRoles || []).join(',');
      E('contentTpl').value = S.config.content || '';
      E('embedColor').value = toHexColor(S.config.embed.color || '#9146ff');
      E('authorEnabled').checked = Boolean(S.config.embed.author.enabled);
      E('authorName').value = S.config.embed.author.name || '';
      E('authorIconMode').value = S.config.embed.author.icon_mode || 'twitch_logo';
      E('authorLinkEnabled').checked = Boolean(S.config.embed.author.link_to_channel);
      E('embedTitle').value = S.config.embed.title || '';
      E('descMode').value = S.config.embed.description_mode || 'stream_title';
      E('embedDesc').value = S.config.embed.description || '';
      E('titleLinkEnabled').checked = Boolean(S.config.embed.title_link_enabled);
      E('shortenEnabled').checked = Boolean(S.config.embed.shorten);
      E('thumbMode').value = S.config.embed.thumbnail.mode || 'none';
      E('thumbUrl').value = S.config.embed.thumbnail.custom_url || '';
      E('useStreamImage').checked = Boolean(S.config.embed.image.use_stream_thumbnail);
      E('imageUrl').value = S.config.embed.image.custom_url || '';
      E('imageFormat').value = S.config.embed.image.format || '16:9';
      E('imageCb').checked = Boolean(S.config.embed.image.cache_buster);
      E('footerText').value = S.config.embed.footer.text || '';
      E('footerTs').value = S.config.embed.footer.timestamp_mode || 'started_at';
      E('buttonEnabled').checked = Boolean(S.config.button.enabled);
      E('buttonLabel').value = S.config.button.label || '';
      applyBlockOrder();
      renderRoleInfo();
      renderFields();
      renderPlaceholderPills();
    }}

    function readForm() {{
      norm();
      S.config.content = E('contentTpl').value;
      S.config.mentions.enabled = Boolean(E('mentionsEnabled').checked);
      S.config.mentions.role_id = '';
      S.allowedRoles = roleIdsCsv(E('editorRoles').value);
      S.config.allowed_editor_role_ids = S.allowedRoles;
      S.config.embed.color = E('embedColor').value.trim() || '#9146ff';
      S.config.embed.author.enabled = Boolean(E('authorEnabled').checked);
      S.config.embed.author.name = E('authorName').value;
      S.config.embed.author.icon_mode = E('authorIconMode').value;
      S.config.embed.author.link_to_channel = Boolean(E('authorLinkEnabled').checked);
      S.config.embed.title = E('embedTitle').value;
      S.config.embed.description_mode = E('descMode').value;
      S.config.embed.description = E('embedDesc').value;
      S.config.embed.title_link_enabled = Boolean(E('titleLinkEnabled').checked);
      S.config.embed.shorten = Boolean(E('shortenEnabled').checked);
      S.config.embed.thumbnail.mode = E('thumbMode').value;
      S.config.embed.thumbnail.custom_url = E('thumbUrl').value;
      S.config.embed.image.use_stream_thumbnail = Boolean(E('useStreamImage').checked);
      S.config.embed.image.custom_url = E('imageUrl').value;
      S.config.embed.image.format = E('imageFormat').value;
      S.config.embed.image.cache_buster = Boolean(E('imageCb').checked);
      S.config.embed.footer.text = E('footerText').value;
      S.config.embed.footer.timestamp_mode = E('footerTs').value;
      S.config.button.enabled = Boolean(E('buttonEnabled').checked);
      S.config.button.label = E('buttonLabel').value;
      S.config.button.url_template = '{{url}}';
      S.config.ui.block_order = [...S.blockOrder];
      S.config.embed.fields = collectFields();
      return S.config;
    }}

    function renderFields() {{
      const wrap = E('fieldsWrap');
      wrap.innerHTML = '';
      (S.config.embed.fields || []).forEach((field, idx) => {{
        const row = document.createElement('div');
        row.className = 'field-row';
        row.innerHTML = `<input data-name='${{idx}}' value='${{esc(field.name)}}' placeholder='Field Name'><input data-value='${{idx}}' value='${{esc(field.value)}}' placeholder='Field Value'><label class='small'><input data-inline='${{idx}}' type='checkbox' ${{field.inline ? 'checked' : ''}}> Inline</label><button class='btn ghost' type='button' data-up='${{idx}}'>▲</button><button class='btn ghost' type='button' data-down='${{idx}}'>▼</button><button class='btn ghost' type='button' data-remove='${{idx}}'>Entfernen</button>`;
        wrap.appendChild(row);
      }});
      wrap.querySelectorAll('[data-remove]').forEach((btn) => btn.addEventListener('click', () => {{ S.config.embed.fields.splice(Number(btn.getAttribute('data-remove')), 1); renderFields(); onMutate(); }}));
      wrap.querySelectorAll('[data-up]').forEach((btn) => btn.addEventListener('click', () => {{
        const idx = Number(btn.getAttribute('data-up'));
        if (idx <= 0) return;
        const tmp = S.config.embed.fields[idx - 1];
        S.config.embed.fields[idx - 1] = S.config.embed.fields[idx];
        S.config.embed.fields[idx] = tmp;
        renderFields();
        onMutate();
      }}));
      wrap.querySelectorAll('[data-down]').forEach((btn) => btn.addEventListener('click', () => {{
        const idx = Number(btn.getAttribute('data-down'));
        if (idx >= S.config.embed.fields.length - 1) return;
        const tmp = S.config.embed.fields[idx + 1];
        S.config.embed.fields[idx + 1] = S.config.embed.fields[idx];
        S.config.embed.fields[idx] = tmp;
        renderFields();
        onMutate();
      }}));
      wrap.querySelectorAll('input').forEach((inp) => inp.addEventListener('input', onMutate));
      wrap.querySelectorAll('input[type="checkbox"]').forEach((inp) => inp.addEventListener('change', onMutate));
    }}

    function collectFields() {{
      const rows = E('fieldsWrap').querySelectorAll('.field-row');
      const out = [];
      rows.forEach((row) => {{
        const n = (row.querySelector('[data-name]')?.value || '').trim();
        const v = (row.querySelector('[data-value]')?.value || '').trim();
        const inline = Boolean(row.querySelector('[data-inline]')?.checked);
        if (n && v) out.push({{ name: n, value: v, inline }});
      }});
      return out;
    }}

    function renderPlaceholderPills() {{
      const wrap = E('placeholderPills');
      wrap.innerHTML = '';
      (ST.placeholders || []).forEach((ph) => {{
        const pill = document.createElement('button');
        pill.type = 'button';
        pill.className = 'pill';
        pill.textContent = '{' + ph + '}';
        pill.addEventListener('click', () => {{
          const active = document.activeElement;
          if (!(active instanceof HTMLInputElement || active instanceof HTMLTextAreaElement)) return;
          const token = '{' + ph + '}';
          const s = active.selectionStart ?? active.value.length;
          const e = active.selectionEnd ?? active.value.length;
          active.value = active.value.slice(0, s) + token + active.value.slice(e);
          active.focus();
          active.selectionStart = active.selectionEnd = s + token.length;
          onMutate();
        }});
        wrap.appendChild(pill);
      }});
    }}

    function renderValidation(items) {{
      const wrap = E('validation');
      wrap.innerHTML = '';
      (items || []).forEach((item) => {{
        const el = document.createElement('div');
        el.className = 'item';
        el.textContent = `${{item.path || 'config'}}: ${{item.message || 'ungueltig'}}`;
        wrap.appendChild(el);
      }});
    }}

    function setStatus(msg, isErr = false) {{
      const el = E('status');
      el.textContent = msg || '';
      el.className = 'status' + (msg ? (isErr ? ' err' : ' ok') : '');
    }}

    function renderPreview() {{
      const p = S.preview || {{}};
      const embed = p.embed || {{}};
      const author = embed.author || {{}};
      const footer = embed.footer || {{}};
      const button = p.button || {{}};
      const fallbackStreamPreview = 'https://static-cdn.jtvnw.net/ttv-static/404_preview-640x360.jpg';
      const fallbackAvatar = 'https://static-cdn.jtvnw.net/jtv_user_pictures/xarth/404_user_70x70.png';
      E('pvBotAvatar').src = BOT_AVATAR;
      E('pvMsgTime').textContent = 'Heute um ' + new Date().toLocaleTimeString('de-DE', {{ hour: '2-digit', minute: '2-digit' }});
      E('pvContent').textContent = p.content || '(leer)';
      E('pvEmbed').style.borderLeftColor = toHexColor(embed.color);

      const authorEnabled = author.enabled !== false && Boolean(author.name || author.icon_url);
      E('pvAuthorWrap').style.display = authorEnabled ? 'flex' : 'none';
      const authorLink = E('pvAuthor');
      authorLink.textContent = author.name || '';
      authorLink.href = author.url || '#';
      authorLink.classList.toggle('no-link', !author.url);
      setImage(E('pvAuthorIcon'), author.icon_url || '', fallbackAvatar);

      const titleNode = E('pvTitle');
      const titleText = String(embed.title || '').trim();
      const titleUrl = String(embed.url || '').trim();
      if (!titleText) {{
        titleNode.style.display = 'none';
      }} else {{
        titleNode.style.display = 'block';
        titleNode.textContent = titleText;
        titleNode.href = titleUrl || '#';
        titleNode.classList.toggle('no-link', !titleUrl);
      }}

      E('pvDesc').textContent = embed.description || '';
      const fieldsWrap = E('pvFields');
      fieldsWrap.innerHTML = '';
      (embed.fields || []).forEach((field) => {{
        const div = document.createElement('div');
        div.className = 'pv-field ' + (field.inline ? '' : 'full');
        const label = document.createElement('div');
        label.className = 'pv-field-name';
        label.textContent = String(field.name || '');
        const value = document.createElement('div');
        value.className = 'pv-field-value';
        value.textContent = String(field.value || '');
        div.appendChild(label);
        div.appendChild(value);
        fieldsWrap.appendChild(div);
      }});

      setImage(E('pvImage'), (embed.image || {{}}).url || '', fallbackStreamPreview);
      setImage(E('pvThumb'), (embed.thumbnail || {{}}).url || '', fallbackAvatar);

      E('pvFooterText').textContent = footer.text || '';
      E('pvFooterTime').textContent = embed.timestamp ? ('• ' + formatTime(embed.timestamp)) : '';
      setImage(E('pvFooterIcon'), footer.icon_url || '', fallbackAvatar);
      E('pvFooter').style.display = (footer.text || footer.icon_url || embed.timestamp) ? 'flex' : 'none';

      const pvBtn = E('pvBtn');
      pvBtn.style.display = button.enabled !== false ? 'inline-flex' : 'none';
      pvBtn.textContent = button.label || 'Auf Twitch ansehen';
      pvBtn.href = button.url || '#';
    }}

    async function fetchPreview() {{
      const cfg = readForm();
      const url = `/twitch/api/live-announcement/preview?streamer=${{encodeURIComponent(S.streamer)}}&config=${{encodeURIComponent(JSON.stringify(cfg))}}`;
      try {{
        const res = await fetch(url, {{ credentials: 'same-origin' }});
        const data = await res.json();
        if (!res.ok) {{ setStatus(data.error || 'Preview fehlgeschlagen.', true); return; }}
        S.streamerPingRoleId = Number(data.streamer_ping_role_id || S.streamerPingRoleId || 0) || null;
        if (data.streamer_ping_role_name) {{ S.streamerPingRoleName = String(data.streamer_ping_role_name); }}
        if (typeof data.role_status_message === 'string' && data.role_status_message) {{
          S.roleStatusMessage = data.role_status_message;
        }}
        renderRoleInfo();
        S.preview = data.preview || {{}};
        renderPreview();
        renderValidation(data.validation || []);
      }} catch (_err) {{
        setStatus('Preview Request fehlgeschlagen.', true);
      }}
    }}

    async function saveConfig() {{
      const cfg = readForm();
      setStatus('Speichere...');
      try {{
        const res = await fetch(`/twitch/api/live-announcement/config?streamer=${{encodeURIComponent(S.streamer)}}`, {{
          method: 'POST',
          credentials: 'same-origin',
          headers: {{ 'Content-Type': 'application/json', 'X-CSRF-Token': S.csrf }},
          body: JSON.stringify({{ csrf_token: S.csrf, streamer_login: S.streamer, config: cfg, allowed_editor_role_ids: S.allowedRoles }})
        }});
        const data = await res.json();
        if (!res.ok) {{ renderValidation(data.validation || []); setStatus(data.message || data.error || 'Speichern fehlgeschlagen.', true); return; }}
        S.streamerPingRoleId = Number(data.streamer_ping_role_id || S.streamerPingRoleId || 0) || null;
        if (data.streamer_ping_role_name) {{ S.streamerPingRoleName = String(data.streamer_ping_role_name); }}
        if (typeof data.role_status_message === 'string' && data.role_status_message) {{
          S.roleStatusMessage = data.role_status_message;
        }}
        renderRoleInfo();
        S.preview = data.preview || S.preview;
        renderPreview();
        renderValidation([]);
        setStatus('Gespeichert.');
      }} catch (_err) {{
        setStatus('Speichern fehlgeschlagen.', true);
      }}
    }}

    async function sendTestDm() {{
      const cfg = readForm();
      setStatus('Sende Test-DM...');
      try {{
        const res = await fetch(`/twitch/api/live-announcement/test?streamer=${{encodeURIComponent(S.streamer)}}`, {{
          method: 'POST',
          credentials: 'same-origin',
          headers: {{ 'Content-Type': 'application/json', 'X-CSRF-Token': S.csrf }},
          body: JSON.stringify({{ csrf_token: S.csrf, streamer_login: S.streamer, config: cfg }})
        }});
        const data = await res.json();
        S.streamerPingRoleId = Number(data.streamer_ping_role_id || S.streamerPingRoleId || 0) || null;
        if (data.streamer_ping_role_name) {{ S.streamerPingRoleName = String(data.streamer_ping_role_name); }}
        renderRoleInfo();
        setStatus(data.message || (data.ok ? 'Test versendet.' : 'Test fehlgeschlagen.'), !data.ok);
      }} catch (_err) {{
        setStatus('Test-DM fehlgeschlagen.', true);
      }}
    }}

    function onMutate() {{ if (S.timer) clearTimeout(S.timer); S.timer = setTimeout(fetchPreview, 190); }}

    function bindEvents() {{
      bindBlockMoveButtons();
      WATCH_IDS.forEach((id) => {{
        const node = E(id);
        if (!node) return;
        const evt = (node instanceof HTMLInputElement && node.type === 'checkbox') || node instanceof HTMLSelectElement ? 'change' : 'input';
        node.addEventListener(evt, onMutate);
      }});
      E('streamerSelect').addEventListener('change', () => {{ const next = E('streamerSelect').value; if (next) window.location.href = `/twitch/live-announcement?streamer=${{encodeURIComponent(next)}}`; }});
      E('addFieldBtn').addEventListener('click', () => {{ S.config.embed.fields.push({{ name: 'Neues Feld', value: '{{title}}', inline: true }}); renderFields(); onMutate(); }});
      E('presetBtn').addEventListener('click', () => {{ S.config.embed.fields = [{{ name: 'Viewer', value: '{{viewer_count}}', inline: true }}, {{ name: 'Kategorie', value: '{{game}}', inline: true }}]; renderFields(); onMutate(); }});
      E('presetMetaBtn').addEventListener('click', () => {{ S.config.embed.fields = [{{ name: 'Startzeit', value: '{{started_at}}', inline: true }}, {{ name: 'Sprache', value: '{{language}}', inline: true }}, {{ name: 'Tags', value: '{{tags}}', inline: false }}]; renderFields(); onMutate(); }});
      E('saveBtn').addEventListener('click', saveConfig);
      E('testBtn').addEventListener('click', sendTestDm);
      const copyBtn = E('copyRoleIdBtn');
      if (copyBtn) {{ copyBtn.addEventListener('click', () => {{
        const rid = E('pingRoleId').value;
        if (rid) {{ navigator.clipboard.writeText(rid).then(() => {{ copyBtn.textContent = 'Kopiert!'; setTimeout(() => {{ copyBtn.textContent = 'Kopieren'; }}, 1500); }}); }}
      }}); }}
    }}

    fillForm();
    bindEvents();
    renderPreview();
    renderValidation((ST.preview || {{}}).validation || []);
  </script>
</body>
</html>"""
