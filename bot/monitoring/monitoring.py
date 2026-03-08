"""Background polling and monitoring helpers for Twitch streams."""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import UTC, datetime, timedelta

import discord
from discord.ext import tasks

from .. import storage
from ..api.twitch_auth import TwitchClientConfigError
from ..core.constants import (
    INVITES_REFRESH_INTERVAL_HOURS,
    POLL_INTERVAL_SECONDS,
    TWITCH_TARGET_GAME_NAME,
    TWITCH_VOD_BUTTON_LABEL,
    log,
)
from .embeds_mixin import _EmbedsMixin
from .eventsub_mixin import _EventSubMixin
from .exp_sessions_mixin import _ExpSessionsMixin
from .sessions_mixin import _SessionsMixin


class TwitchMonitoringMixin(_EventSubMixin, _ExpSessionsMixin, _SessionsMixin, _EmbedsMixin):
    """Polling loops and helpers used by the Twitch cog."""

    @staticmethod
    def _normalize_poll_interval_seconds(raw_value: object) -> int | None:
        raw_text = str(raw_value or "").strip()
        if not raw_text:
            return None
        try:
            value = int(raw_text)
        except (TypeError, ValueError):
            return None
        if value < 5 or value > 3600:
            return None
        return value

    def _default_poll_interval_seconds(self) -> int:
        default_value = self._normalize_poll_interval_seconds(POLL_INTERVAL_SECONDS)
        return default_value if default_value is not None else 15

    def _poll_interval_debug(
        self,
        message: str,
        *args: object,
        exc_info: bool = False,
    ) -> None:
        now = time.monotonic()
        last_logged = float(getattr(self, "_poll_interval_last_error_log_at", 0.0) or 0.0)
        if last_logged and (now - last_logged) < 300.0:
            return
        self._poll_interval_last_error_log_at = now
        log.debug(message, *args, exc_info=exc_info)

    def _ensure_poll_interval_settings_storage(self, conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS twitch_global_settings (
                setting_key TEXT PRIMARY KEY,
                setting_value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_twitch_global_settings_updated_at "
            "ON twitch_global_settings(updated_at)"
        )

    def _read_persisted_poll_interval_seconds(self) -> int | None:
        table_name = str(
            getattr(self, "_poll_interval_settings_table", "twitch_global_settings")
            or "twitch_global_settings"
        ).strip()
        setting_key = str(
            getattr(self, "_poll_interval_settings_key", "poll_interval_seconds")
            or "poll_interval_seconds"
        ).strip()

        try:
            with storage.get_conn() as conn:
                self._ensure_poll_interval_settings_storage(conn)
                row = conn.execute(
                    f"SELECT setting_value FROM {table_name} WHERE setting_key = ? LIMIT 1",
                    (setting_key,),
                ).fetchone()
        except Exception:
            self._poll_interval_debug(
                "Polling-Intervall: Konnte persistente Einstellung nicht lesen",
                exc_info=True,
            )
            return None

        if row is None:
            self._poll_interval_last_invalid_value = None
            return None

        if hasattr(row, "get"):
            raw_value = row.get("setting_value")
        else:
            raw_value = row[0] if row else None

        normalized = self._normalize_poll_interval_seconds(raw_value)
        if normalized is not None:
            self._poll_interval_last_invalid_value = None
            return normalized

        invalid_marker = str(raw_value or "").strip() or "<empty>"
        if invalid_marker != getattr(self, "_poll_interval_last_invalid_value", None):
            self._poll_interval_last_invalid_value = invalid_marker
            log.debug(
                "Polling-Intervall: Ungueltiger DB-Wert %r, verwende Fallback %ss",
                invalid_marker,
                self._default_poll_interval_seconds(),
            )
        return None

    def _apply_poll_interval_seconds(self, seconds: int, *, reason: str) -> int:
        normalized = self._normalize_poll_interval_seconds(seconds)
        target_seconds = normalized if normalized is not None else self._default_poll_interval_seconds()
        current_seconds = self._normalize_poll_interval_seconds(
            getattr(self, "_poll_interval_seconds", self._default_poll_interval_seconds())
        )
        current_seconds = (
            current_seconds if current_seconds is not None else self._default_poll_interval_seconds()
        )

        if current_seconds == target_seconds:
            self._poll_interval_seconds = target_seconds
            self._admin_polling_interval_seconds = target_seconds
            return target_seconds

        self.poll_streams.change_interval(seconds=target_seconds)
        self._poll_interval_seconds = target_seconds
        self._admin_polling_interval_seconds = target_seconds

        if reason == "startup":
            log.info("Polling-Intervall initialisiert auf %ss", target_seconds)
        else:
            log.info("Polling-Intervall geaendert auf %ss (%s)", target_seconds, reason)
        return target_seconds

    def _sync_poll_interval_from_storage(
        self,
        *,
        force: bool = False,
        startup: bool = False,
    ) -> int:
        now = time.monotonic()
        resync_interval = float(
            getattr(self, "_poll_interval_resync_interval_seconds", 60.0) or 60.0
        )
        last_sync = float(getattr(self, "_poll_interval_last_sync_monotonic", 0.0) or 0.0)
        if not force and last_sync and (now - last_sync) < max(15.0, resync_interval):
            current_seconds = self._normalize_poll_interval_seconds(
                getattr(self, "_poll_interval_seconds", self._default_poll_interval_seconds())
            )
            return current_seconds if current_seconds is not None else self._default_poll_interval_seconds()

        self._poll_interval_last_sync_monotonic = now
        persisted_seconds = self._read_persisted_poll_interval_seconds()
        target_seconds = (
            persisted_seconds if persisted_seconds is not None else self._default_poll_interval_seconds()
        )
        reason = "startup" if startup else "storage_resync"
        if persisted_seconds is None and target_seconds == self._default_poll_interval_seconds():
            reason = "fallback_default"
        return self._apply_poll_interval_seconds(target_seconds, reason=reason)

    @staticmethod
    def _reauth_chat_reminder_text() -> str:
        return (
            "Kurze Erinnerung: Für den Raid-/Stats-Bot fehlt noch die neue Twitch-Autorisierung. "
            "Du hast dazu bereits eine Discord-DM mit dem Re-Auth-Link erhalten. Danke dir!"
        )

    async def _resolve_live_stream_id_for_login(self, login_lower: str) -> str | None:
        if not login_lower or not getattr(self, "api", None):
            return None
        try:
            streams = await self.api.get_streams_by_logins([login_lower])
            if not streams:
                return None
            stream_id = str((streams[0] or {}).get("id") or "").strip()
            return stream_id or None
        except Exception:
            log.debug(
                "ReAuth reminder: Konnte aktuelle stream_id nicht laden für %s",
                login_lower,
                exc_info=True,
            )
            return None

    async def _maybe_send_reauth_chat_reminder(
        self,
        *,
        chat_bot,
        broadcaster_id: str,
        login_lower: str,
    ) -> bool:
        """Sendet beim Streamstart einmalig eine freundliche Re-Auth-Erinnerung in den Twitch-Chat."""
        if not chat_bot or not broadcaster_id or not login_lower:
            return False

        broadcaster_key = str(broadcaster_id).strip()
        login_key = str(login_lower).strip().lower()
        if not broadcaster_key or not login_key:
            return False

        # Primärer Dedupe über stream_id (pro Streamstart genau eine Nachricht).
        stream_id = await self._resolve_live_stream_id_for_login(login_key)
        stream_guard = getattr(self, "_reauth_reminder_last_stream_id", None)
        if not isinstance(stream_guard, dict):
            stream_guard = {}
            self._reauth_reminder_last_stream_id = stream_guard
        if stream_id:
            if stream_guard.get(broadcaster_key) == stream_id:
                return False
            # Guard VOR dem Senden setzen – verhindert Doppel-Trigger durch
            # gleichzeitige EventSub- und Polling-Pfade (race condition fix).
            stream_guard[broadcaster_key] = stream_id
        else:
            # Fallback-Dedupe, falls stream_id temporär nicht geladen werden kann.
            fallback_guard = getattr(self, "_reauth_reminder_last_sent_ts", None)
            if not isinstance(fallback_guard, dict):
                fallback_guard = {}
                self._reauth_reminder_last_sent_ts = fallback_guard
            now_ts = time.time()
            last_ts = float(fallback_guard.get(broadcaster_key) or 0.0)
            if now_ts - last_ts < 300.0:
                return False
            fallback_guard[broadcaster_key] = now_ts

        send_chat = getattr(chat_bot, "_send_chat_message", None)
        if not callable(send_chat):
            return False

        make_channel = getattr(chat_bot, "_make_promo_channel", None)
        if callable(make_channel):
            channel = make_channel(login_key, broadcaster_key)
        else:

            class _Channel:
                __slots__ = ("name", "id")

                def __init__(self, name: str, cid: str):
                    self.name = name
                    self.id = cid

            channel = _Channel(login_key, broadcaster_key)

        ok = await send_chat(
            channel,
            self._reauth_chat_reminder_text(),
            source="migration_reminder",
        )
        if ok:
            log.info(
                "ReAuth reminder: Chat-Hinweis bei Streamstart gesendet für %s (%s)",
                login_key,
                broadcaster_key,
            )
        return bool(ok)

    def _get_target_game_lower(self) -> str:
        target = getattr(self, "_target_game_lower", None)
        if isinstance(target, str) and target:
            return target
        resolved = (TWITCH_TARGET_GAME_NAME or "").strip().lower()
        # Cache for subsequent lookups to avoid repeated normalization
        self._target_game_lower = resolved
        return resolved

    def _stream_is_in_target_category(self, stream: dict | None) -> bool:
        if not stream:
            return False
        target_game_lower = self._get_target_game_lower()
        if not target_game_lower:
            return False
        game_name = (stream.get("game_name") or "").strip().lower()
        return game_name == target_game_lower

    @staticmethod
    def _normalize_stream_meta(
        stream: dict,
    ) -> tuple[str | None, str | None, str | None]:
        game_name = (stream.get("game_name") or "").strip() or None
        stream_title = (stream.get("title") or "").strip() or None

        tags_raw = stream.get("tags")
        tags_serialized: str | None = None
        if isinstance(tags_raw, list):
            clean_tags = [str(tag).strip() for tag in tags_raw if str(tag).strip()]
            if clean_tags:
                tags_serialized = json.dumps(clean_tags, ensure_ascii=True, separators=(",", ":"))
        elif isinstance(tags_raw, str):
            tag_value = tags_raw.strip()
            if tag_value:
                tags_serialized = tag_value

        return game_name, stream_title, tags_serialized

    def _language_filter_values(self) -> list[str | None]:
        filters: list[str] | None = getattr(self, "_language_filters", None)
        if not filters:
            return [None]
        seen: list[str] = []
        for entry in filters:
            normalized = (entry or "").strip().lower()
            if not normalized or normalized in seen:
                continue
            seen.append(normalized)
        return [*seen] or [None]

    @tasks.loop(seconds=POLL_INTERVAL_SECONDS)
    async def poll_streams(self):
        try:
            self._sync_poll_interval_from_storage()
        except Exception:
            self._poll_interval_debug(
                "Polling-Intervall: Runtime-Resync fehlgeschlagen",
                exc_info=True,
            )
        if self.api is None:
            return
        try:
            await self._tick()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Polling-Tick fehlgeschlagen")

    @poll_streams.before_loop
    async def _before_poll(self):
        await self.bot.wait_until_ready()
        try:
            self._sync_poll_interval_from_storage(force=True, startup=True)
        except Exception:
            self._poll_interval_debug(
                "Polling-Intervall: Start-Resync fehlgeschlagen",
                exc_info=True,
            )

    @tasks.loop(hours=INVITES_REFRESH_INTERVAL_HOURS)
    async def invites_refresh(self):
        try:
            await self._refresh_all_invites()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Invite-Refresh fehlgeschlagen")

    @invites_refresh.before_loop
    async def _before_invites(self):
        await self.bot.wait_until_ready()

    async def _ensure_category_id(self):
        if self.api is None:
            return
        try:
            self._category_id = await self.api.get_category_id(TWITCH_TARGET_GAME_NAME)
            if self._category_id:
                log.debug("Deadlock category_id = %s", self._category_id)
        except TwitchClientConfigError:
            return
        except Exception:
            log.exception("Konnte Twitch-Kategorie-ID nicht ermitteln")

    async def _tick(self):
        """Ein Tick: tracked Streamer + Kategorie-Streams prüfen, Postings/DB aktualisieren, Stats loggen."""
        if self.api is None:
            return
        if self.api.is_auth_blocked():
            return

        if not self._category_id:
            await self._ensure_category_id()
            if self.api.is_auth_blocked():
                return

        partner_logins: set[str] = set()
        try:
            with storage.get_conn() as c:
                rows = c.execute(
                    "SELECT twitch_login, twitch_user_id, require_discord_link, "
                    "       archived_at, is_partner, discord_user_id, live_ping_role_id, "
                    "       COALESCE(live_ping_enabled, 1) AS live_ping_enabled "
                    "FROM twitch_streamers_partner_state"
                ).fetchall()
            tracked: list[dict[str, object]] = []
            for row in rows:
                row_dict = dict(row)
                login = str(row_dict.get("twitch_login") or "").strip()
                if not login:
                    continue
                user_id = str(row_dict.get("twitch_user_id") or "").strip()
                require_link = bool(row_dict.get("require_discord_link"))
                archived_at_raw = row_dict.get("archived_at")
                archived_dt: datetime | None = None
                if archived_at_raw:
                    try:
                        archived_dt = datetime.fromisoformat(str(archived_at_raw))
                    except Exception:
                        archived_dt = None
                is_archived = archived_dt is not None
                is_verified = bool(row_dict.get("is_partner"))

                tracked.append(
                    {
                        "login": login,
                        "twitch_user_id": user_id,
                        "require_link": require_link,
                        "is_verified": is_verified,
                        "archived_at": archived_at_raw,
                        "is_archived": is_archived,
                        "discord_user_id": row_dict.get("discord_user_id"),
                        "live_ping_role_id": row_dict.get("live_ping_role_id"),
                        "live_ping_enabled": row_dict.get("live_ping_enabled", 1),
                    }
                )
                login_lower = login.lower()
                if login_lower and is_verified and not is_archived:
                    partner_logins.add(login_lower)
        except Exception:
            log.exception("Konnte tracked Streamer nicht aus DB lesen")
            tracked = []
            partner_logins = set()

        logins = [str(entry.get("login") or "") for entry in tracked if entry.get("login")]
        language_filters = self._language_filter_values()
        streams_by_login: dict[str, dict] = {}

        if logins:
            for language in language_filters:
                try:
                    streams = await self.api.get_streams_by_logins(logins, language=language)
                except TwitchClientConfigError:
                    return
                except Exception:
                    label = language or "any"
                    log.exception(
                        "Konnte Streams für tracked Logins nicht abrufen (language=%s)",
                        label,
                    )
                    continue
                for stream in streams:
                    login = (stream.get("user_login") or "").lower()
                    if login:
                        streams_by_login[login] = stream

        for login, stream in list(streams_by_login.items()):
            if login in partner_logins:
                stream["is_partner"] = True

        category_streams: list[dict] = []
        if self._category_id:
            collected: dict[str, dict] = {}
            for language in language_filters:
                remaining = self._category_sample_limit - len(collected)
                if remaining <= 0:
                    break
                try:
                    streams = await self.api.get_streams_by_category(
                        self._category_id,
                        language=language,
                        limit=max(1, remaining),
                    )
                except TwitchClientConfigError:
                    return
                except Exception:
                    label = language or "any"
                    log.exception("Konnte Kategorie-Streams nicht abrufen (language=%s)", label)
                    continue
                for stream in streams:
                    login = (stream.get("user_login") or "").lower()
                    if login and login not in collected:
                        collected[login] = stream
            category_streams = list(collected.values())

        for stream in category_streams:
            login = (stream.get("user_login") or "").lower()
            if login in partner_logins:
                stream["is_partner"] = True

        try:
            await self._process_postings(tracked, streams_by_login)
        except Exception:
            log.exception("Fehler in _process_postings")

        try:
            await self._record_eventsub_capacity_snapshot("poll_tick")
        except Exception:
            log.debug("EventSub: Snapshot im Poll-Tick fehlgeschlagen", exc_info=True)

        self._tick_count += 1
        if self._tick_count % self._log_every_n == 0:
            try:
                await self._log_stats(streams_by_login, category_streams)
            except Exception:
                log.exception("Fehler beim Stats-Logging")

        # Partner-Rekrutierung (intern rate-limitiert auf 30 min)
        try:
            await self._run_partner_recruit(category_streams)
        except Exception:
            log.exception("Fehler bei Partner-Rekrutierung")

    async def _process_postings(
        self,
        tracked: list[dict[str, object]],
        streams_by_login: dict[str, dict],
    ):
        notify_ch: discord.TextChannel | None = None
        if self._notify_channel_id:
            notify_ch = self.bot.get_channel(self._notify_channel_id) or None  # type: ignore[assignment]

        now_utc = datetime.now(tz=UTC)
        now_iso = now_utc.isoformat(timespec="seconds")
        pending_state_rows: list[
            tuple[
                str,
                str,
                int,
                str,
                str | None,
                str | None,
                int,
                str | None,
                str | None,
                str | None,
                str | None,
                int,
                int | None,
                str | None,
            ]
        ] = []

        with storage.get_conn() as c:
            live_state_rows = c.execute("SELECT * FROM twitch_live_state").fetchall()

        live_state: dict[str, dict] = {}
        for row in live_state_rows:
            row_dict = dict(row)
            key = str(row_dict.get("streamer_login") or "").lower()
            if key:
                live_state[key] = row_dict

        target_game_lower = self._get_target_game_lower()

        for entry in tracked:
            login = str(entry.get("login") or "").strip()
            if not login:
                continue

            referral_url = self._build_referral_url(login)
            login_lower = login.lower()
            stream = streams_by_login.get(login_lower)
            previous_state = live_state.get(login_lower, {})
            is_archived = bool(entry.get("is_archived"))
            was_live = bool(previous_state.get("is_live", 0))
            is_live = bool(stream)
            twitch_user_id = str(entry.get("twitch_user_id") or "").strip() or None

            # Go-Live Detection: Subscribe stream.offline für raid-enabled Streamer
            if not was_live and is_live and twitch_user_id:
                # Stream ist gerade live gegangen!
                handler = getattr(self, "_handle_stream_went_live", None)
                if handler:
                    # Checke ob der Streamer raid_bot_enabled hat
                    try:
                        with storage.get_conn() as c:
                            raid_enabled_row = c.execute(
                                "SELECT raid_bot_enabled FROM twitch_streamers WHERE twitch_user_id = ?",
                                (twitch_user_id,),
                            ).fetchone()
                        if raid_enabled_row and bool(raid_enabled_row[0]):
                            # Asynchron aufrufen (fire-and-forget, blockiert nicht den Tick)
                            asyncio.create_task(
                                handler(twitch_user_id, login_lower),
                                name=f"golive.{login_lower}",
                            )
                    except Exception:
                        log.debug(
                            "Go-Live: Konnte raid_enabled Status nicht checken für %s",
                            login_lower,
                            exc_info=True,
                        )

            # Auto-Entarchivierung sobald jemand wieder streamt
            if is_live and is_archived:
                try:
                    await self._dashboard_archive(login, "unarchive")
                    is_archived = False
                    entry["is_archived"] = False
                except Exception:
                    log.debug("Auto-Unarchive fehlgeschlagen für %s", login, exc_info=True)
            previous_game = (previous_state.get("last_game") or "").strip()
            previous_game_lower = previous_game.lower()
            was_deadlock = previous_game_lower == target_game_lower
            stream_started_at_value = self._extract_stream_start(stream, previous_state)
            previous_stream_id = (previous_state.get("last_stream_id") or "").strip()
            current_stream_id_raw = stream.get("id") if stream else ""
            current_stream_id = str(current_stream_id_raw or "").strip()
            stream_id_value = current_stream_id or previous_stream_id or None
            had_deadlock_prev = bool(int(previous_state.get("had_deadlock_in_session", 0) or 0))
            active_session_id: int | None = None
            previous_last_deadlock_seen = (
                previous_state.get("last_deadlock_seen_at") or ""
            ).strip() or None

            if is_live and stream:
                try:
                    active_session_id = await self._ensure_stream_session(
                        login=login_lower,
                        stream=stream,
                        previous_state=previous_state,
                        twitch_user_id=twitch_user_id,
                    )
                except Exception:
                    log.exception("Konnte Streamsitzung nicht starten: %s", login)
            elif was_live and not is_live:
                try:
                    await self._finalize_stream_session(login=login_lower, reason="offline")
                except Exception:
                    log.exception("Konnte Streamsitzung nicht abschliessen: %s", login)
            elif not is_live and previous_state.get("active_session_id"):
                try:
                    await self._finalize_stream_session(login=login_lower, reason="stale")
                except Exception:
                    log.debug("Konnte alte Session nicht bereinigen: %s", login, exc_info=True)

            if not was_live:
                had_deadlock_prev = False
            elif (
                is_live
                and previous_stream_id
                and current_stream_id
                and previous_stream_id != current_stream_id
            ):
                had_deadlock_prev = False

            message_id_previous = (
                str(previous_state.get("last_discord_message_id") or "").strip() or None
            )
            message_id_to_store = message_id_previous
            tracking_token_previous = (
                str(previous_state.get("last_tracking_token") or "").strip() or None
            )
            tracking_token_to_store = tracking_token_previous

            need_link = bool(entry.get("require_link"))
            is_verified = bool(entry.get("is_verified"))

            game_name = (stream.get("game_name") or "").strip() if stream else ""
            game_name_lower = game_name.lower()
            is_deadlock = (
                is_live and bool(target_game_lower) and game_name_lower == target_game_lower
            )
            had_deadlock_in_session = had_deadlock_prev or is_deadlock

            # --- Experimental hook: game transition ---
            if (
                is_live
                and was_live
                and game_name
                and previous_game
                and game_name_lower != previous_game_lower
            ):
                try:
                    exp_transition = getattr(self, "_exp_on_game_transition", None)
                    exp_get_id = getattr(self, "_get_exp_session_id", None)
                    if callable(exp_transition) and callable(exp_get_id):
                        exp_id = exp_get_id(login_lower)
                        if exp_id is not None:
                            viewer_count_now = int(stream.get("viewer_count") or 0)
                            exp_transition(
                                login=login_lower,
                                exp_session_id=exp_id,
                                from_game=previous_game,
                                to_game=game_name,
                                viewer_count=viewer_count_now,
                            )
                except Exception:
                    log.debug("exp: game_transition fehlgeschlagen für %s", login_lower, exc_info=True)
            had_deadlock_to_store = had_deadlock_in_session if is_live else False
            last_title_value = (
                stream.get("title") if stream else previous_state.get("last_title")
            ) or None
            last_game_value = (game_name or previous_state.get("last_game") or "").strip() or None
            last_viewer_count_value = (
                int(stream.get("viewer_count") or 0)
                if stream
                else int(previous_state.get("last_viewer_count") or 0)
            )
            last_deadlock_seen_at_value: str | None = None
            if is_deadlock:
                last_deadlock_seen_at_value = now_iso
            elif had_deadlock_to_store and previous_last_deadlock_seen:
                last_deadlock_seen_at_value = previous_last_deadlock_seen

            should_post = (
                notify_ch is not None
                and is_deadlock
                and (not was_live or not was_deadlock or not message_id_previous)
                and is_verified
                and not is_archived
            )

            if should_post:
                content, embed, view, allowed_mentions, new_tracking_token = (
                    await self._build_live_announcement_message(
                        login=login,
                        stream=stream,
                        streamer_entry=entry,
                        notify_channel=notify_ch,
                    )
                )
                if self._alert_mention:
                    prefix = self._sanitize_live_content(str(self._alert_mention).strip())
                    if prefix:
                        content = f"{prefix} {content}".strip()
                        alert_role_id = self._extract_role_id_from_mention(prefix)
                        if alert_role_id:
                            role_ids: list[int] = []
                            current_roles = allowed_mentions.roles
                            if isinstance(current_roles, (list, tuple, set)):
                                for role_obj in current_roles:
                                    role_id = getattr(role_obj, "id", None)
                                    if role_id:
                                        role_ids.append(int(role_id))
                            if alert_role_id not in role_ids:
                                role_ids.append(alert_role_id)
                            allowed_mentions = discord.AllowedMentions(
                                everyone=False,
                                users=False,
                                roles=[discord.Object(id=role_id) for role_id in role_ids]
                                if role_ids
                                else False,
                                replied_user=False,
                            )

                try:
                    message = await notify_ch.send(
                        content=content or None,
                        embed=embed,
                        view=view,
                        allowed_mentions=allowed_mentions,
                    )
                except Exception:
                    log.exception("Konnte Go-Live-Posting nicht senden: %s", login)
                else:
                    message_id_to_store = str(message.id)
                    tracking_token_to_store = new_tracking_token if view is not None else None
                    if view is not None:
                        view.bind_to_message(
                            channel_id=getattr(notify_ch, "id", None),
                            message_id=message.id,
                        )
                        self._register_live_view(
                            tracking_token=new_tracking_token,
                            view=view,
                            message_id=message.id,
                        )
                    # Store notification text if we have an active session
                    if active_session_id:
                        try:
                            with storage.get_conn() as c:
                                c.execute(
                                    "UPDATE twitch_stream_sessions SET notification_text = ? WHERE id = ?",
                                    (content or "", active_session_id),
                                )
                        except Exception:
                            log.debug(
                                "Could not save notification text for %s",
                                login,
                                exc_info=True,
                            )

            ended_deadlock_posting = (
                notify_ch is not None and message_id_previous and (not is_live or not is_deadlock)
            )
            # Auto-Raid per Polling für Partner deaktiviert – EventSub ist Primärpfad
            should_auto_raid = False

            if ended_deadlock_posting:
                display_name = (
                    stream.get("user_name") if stream else previous_state.get("streamer_login")
                ) or login
                try:
                    message_id_int = int(message_id_previous)
                except (TypeError, ValueError):
                    message_id_int = None

                if message_id_int is None:
                    log.warning(
                        "Ungültige Message-ID für Deadlock-Ende bei %s: %r",
                        login,
                        message_id_previous,
                    )
                else:
                    try:
                        fetched_message = await notify_ch.fetch_message(message_id_int)
                    except discord.NotFound:
                        log.warning(
                            "Deadlock-Ende-Posting nicht mehr vorhanden für %s (ID %s)",
                            login,
                            message_id_previous,
                        )
                        message_id_to_store = None
                        tracking_token_to_store = None
                        self._drop_live_view(tracking_token_previous)
                    except Exception:
                        log.exception("Konnte Deadlock-Ende-Posting nicht laden: %s", login)
                    else:
                        preview_image_url = await self._get_latest_vod_preview_url(
                            login=login,
                            twitch_user_id=twitch_user_id or previous_state.get("twitch_user_id"),
                        )

                        ended_content = f"**{display_name}** ist OFFLINE - VOD per Button."
                        offline_embed = self._build_offline_embed(
                            login=login,
                            display_name=display_name,
                            last_title=last_title_value,
                            last_game=last_game_value,
                            preview_image_url=preview_image_url,
                        )
                        offline_view = self._build_offline_link_view(
                            referral_url, label=TWITCH_VOD_BUTTON_LABEL
                        )
                        try:
                            await fetched_message.edit(
                                content=ended_content,
                                embed=offline_embed,
                                view=offline_view,
                            )
                        except Exception:
                            log.exception(
                                "Konnte Deadlock-Ende-Posting nicht aktualisieren: %s",
                                login,
                            )
                        else:
                            message_id_to_store = None
                            tracking_token_to_store = None
                            self._drop_live_view(tracking_token_previous)

            db_user_id = twitch_user_id or previous_state.get("twitch_user_id") or login_lower
            db_user_id = str(db_user_id)
            db_message_id = str(message_id_to_store) if message_id_to_store else None
            db_streamer_login = login_lower

            pending_state_rows.append(
                (
                    db_user_id,
                    db_streamer_login,
                    int(is_live),
                    now_iso,
                    last_title_value,
                    last_game_value,
                    last_viewer_count_value,
                    db_message_id,
                    tracking_token_to_store,
                    stream_id_value,
                    stream_started_at_value,
                    int(had_deadlock_to_store),
                    active_session_id,
                    last_deadlock_seen_at_value,
                )
            )

            if need_link and self._alert_channel_id and (now_utc.minute % 10 == 0) and is_live:
                # Platzhalter für deinen Profil-/Panel-Check
                pass

        await self._persist_live_state_rows(pending_state_rows)
        await self._auto_archive_inactive_streamers()

    async def _persist_live_state_rows(
        self,
        rows: list[
            tuple[
                str,
                str,
                int,
                str,
                str | None,
                str | None,
                int,
                str | None,
                str | None,
                str | None,
                str | None,
                int,
                int | None,
            ]
        ],
    ) -> None:
        if not rows:
            return

        retry_delay = 0.5
        for attempt in range(3):
            try:
                with storage.get_conn() as c:
                    c.executemany(
                        "INSERT INTO twitch_live_state ("
                        "twitch_user_id, streamer_login, is_live, last_seen_at, last_title, last_game, "
                        "last_viewer_count, last_discord_message_id, last_tracking_token, last_stream_id, "
                        "last_started_at, had_deadlock_in_session, active_session_id, last_deadlock_seen_at"
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT (twitch_user_id) DO UPDATE SET "
                        "streamer_login = EXCLUDED.streamer_login, "
                        "is_live = EXCLUDED.is_live, "
                        "last_seen_at = EXCLUDED.last_seen_at, "
                        "last_title = EXCLUDED.last_title, "
                        "last_game = EXCLUDED.last_game, "
                        "last_viewer_count = EXCLUDED.last_viewer_count, "
                        "last_discord_message_id = EXCLUDED.last_discord_message_id, "
                        "last_tracking_token = EXCLUDED.last_tracking_token, "
                        "last_stream_id = EXCLUDED.last_stream_id, "
                        "last_started_at = EXCLUDED.last_started_at, "
                        "had_deadlock_in_session = EXCLUDED.had_deadlock_in_session, "
                        "active_session_id = EXCLUDED.active_session_id, "
                        "last_deadlock_seen_at = EXCLUDED.last_deadlock_seen_at",
                        rows,
                    )
                return
            except Exception as exc:
                locked = "locked" in str(exc).lower()
                if not locked or attempt == 2:
                    log.exception(
                        "Konnte Live-State-Updates nicht speichern (%s Eintraege)",
                        len(rows),
                    )
                    return
                await asyncio.sleep(retry_delay)
                retry_delay *= 2

    async def _auto_archive_inactive_streamers(self, *, days: int = 10) -> None:
        """
        Archiviert Partner automatisch, wenn sie länger als `days` Tage nicht gestreamt haben.
        Läuft maximal alle 15 Minuten, um DB-Load gering zu halten.
        """
        now = datetime.now(UTC)
        last_run = getattr(self, "_last_archive_check", 0.0)
        if time.time() - last_run < 900:
            return
        self._last_archive_check = time.time()

        cutoff = now - timedelta(days=days)

        try:
            target_game = (
                os.getenv("TWITCH_TARGET_GAME_NAME") or TWITCH_TARGET_GAME_NAME or ""
            ).strip()
            with storage.get_conn() as c:
                rows = c.execute(
                    """
                    SELECT s.twitch_login,
                           s.archived_at,
                           MAX(
                               CASE
                                 WHEN LOWER(COALESCE(sess.game_name,'')) = LOWER(?)
                                 THEN COALESCE(sess.ended_at, sess.started_at)
                               END
                            ) AS last_deadlock_stream_at
                      FROM twitch_streamers_partner_state s
                      LEFT JOIN twitch_stream_sessions sess
                        ON LOWER(sess.streamer_login) = LOWER(s.twitch_login)
                     WHERE s.is_partner = 1
                     GROUP BY s.twitch_login, s.archived_at
                    """,
                    (target_game,),
                ).fetchall()
        except Exception:
            log.debug("Auto-Archivierung: konnte Streamer-Liste nicht laden", exc_info=True)
            return

        for row in rows:
            try:
                login = (row["twitch_login"] if hasattr(row, "keys") else row[0] or "").strip()
            except Exception:
                continue
            if not login:
                continue

            archived_at = row["archived_at"] if hasattr(row, "keys") else row[1]
            if archived_at:
                continue

            last_stream_raw = row["last_deadlock_stream_at"] if hasattr(row, "keys") else row[2]
            if not last_stream_raw:
                # Keine Historie -> keine automatische Archivierung
                continue

            try:
                last_dt = datetime.fromisoformat(str(last_stream_raw).replace("Z", "+00:00"))
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=UTC)
            except Exception:
                log.debug(
                    "Auto-Archivierung: Datum unlesbar für %s (%r)",
                    login,
                    last_stream_raw,
                    exc_info=True,
                )
                continue

            if last_dt < cutoff:
                try:
                    result = await self._dashboard_archive(login, "archive")
                    if "bereits archiviert" not in result:
                        log.info(
                            "Auto-archiviert %s (letzter Stream %s, cutoff %s)",
                            login,
                            last_dt.date().isoformat(),
                            cutoff.date().isoformat(),
                        )
                except Exception:
                    log.debug("Auto-Archivierung fehlgeschlagen für %s", login, exc_info=True)

    @staticmethod
    def _parse_dt(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)

    def _extract_stream_start(self, stream: dict | None, previous_state: dict) -> str | None:
        candidate = None
        if stream:
            candidate = stream.get("started_at") or stream.get("start_time")
        if not candidate:
            candidate = previous_state.get("last_started_at")
        dt = self._parse_dt(candidate)
        if dt:
            return dt.isoformat(timespec="seconds")
        return None

    async def _log_stats(self, streams_by_login: dict[str, dict], category_streams: list[dict]):
        now_utc = datetime.now(tz=UTC).isoformat(timespec="seconds")

        # 1) Tracked stats (only our target game)
        try:
            rows: list[tuple] = []
            for stream in streams_by_login.values():
                if not self._stream_is_in_target_category(stream):
                    continue
                login = (stream.get("user_login") or "").lower()
                viewers = int(stream.get("viewer_count") or 0)
                is_partner = bool(stream.get("is_partner"))
                game_name, stream_title, tags = self._normalize_stream_meta(stream)
                rows.append((now_utc, login, viewers, is_partner, game_name, stream_title, tags))

            if rows:
                with storage.get_conn() as c:
                    c.executemany(
                        """
                        INSERT INTO twitch_stats_tracked (
                            ts_utc, streamer, viewer_count, is_partner, game_name, stream_title, tags
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        rows,
                    )
        except Exception:
            log.exception("Konnte tracked-Stats nicht loggen")

        # 2) Session samples – alle Spiele erfassen, nicht nur Deadlock
        try:
            for stream in streams_by_login.values():
                login = (stream.get("user_login") or "").lower()
                self._record_session_sample(login=login, stream=stream)
        except Exception:
            log.debug("Konnte Session-Metrik nicht loggen", exc_info=True)

        # 3) Category-wide stats (all streams in category, regardless of Deadlock)
        try:
            rows: list[tuple] = []
            for stream in category_streams:
                login = (stream.get("user_login") or "").lower()
                viewers = int(stream.get("viewer_count") or 0)
                is_partner = bool(stream.get("is_partner"))
                game_name, stream_title, tags = self._normalize_stream_meta(stream)
                rows.append((now_utc, login, viewers, is_partner, game_name, stream_title, tags))

            if rows:
                with storage.get_conn() as c:
                    c.executemany(
                        """
                        INSERT INTO twitch_stats_category (
                            ts_utc, streamer, viewer_count, is_partner, game_name, stream_title, tags
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        rows,
                    )
        except Exception:
            log.exception("Konnte category-Stats nicht loggen")

    async def _get_latest_vod_preview_url(
        self, *, login: str, twitch_user_id: str | None
    ) -> str | None:
        """Hole das juengste VOD-Thumbnail; faellt bei Fehler still auf None."""
        if self.api is None:
            return None
        try:
            return await self.api.get_latest_vod_thumbnail(user_id=twitch_user_id, login=login)
        except Exception:
            log.exception("Konnte VOD-Thumbnail nicht laden: %s", login)
            return None
