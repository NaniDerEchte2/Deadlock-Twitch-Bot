"""_SessionsMixin – Stream session lifecycle management."""
from __future__ import annotations

from datetime import UTC, datetime

from .. import storage
from ..core.constants import log
try:
    from ..raid.partner_raid_score_tracking import resolve_partner_raid_tracking_for_session
except Exception:  # pragma: no cover - partial deploy safety
    resolve_partner_raid_tracking_for_session = None  # type: ignore[assignment]


class _SessionsMixin:

    def _get_active_sessions_cache(self) -> dict[str, int]:
        cache = getattr(self, "_active_sessions", None)
        if cache is None:
            cache = {}
            self._active_sessions = cache
        return cache

    def _rehydrate_active_sessions(self) -> None:
        cache = self._get_active_sessions_cache()
        cache.clear()
        try:
            with storage.get_conn() as c:
                rows = c.execute(
                    "SELECT id, streamer_login FROM twitch_stream_sessions WHERE ended_at IS NULL"
                ).fetchall()
        except Exception:
            log.debug("Konnte offene Twitch-Sessions nicht laden", exc_info=True)
            return
        for row in rows:
            try:
                session_id = int(row["id"] if hasattr(row, "keys") else row[0])
                login = str(row["streamer_login"] if hasattr(row, "keys") else row[1]).lower()
            except Exception:
                continue
            if login:
                cache[login] = session_id

    def _lookup_open_session_id(self, login: str) -> int | None:
        try:
            with storage.get_conn() as c:
                row = c.execute(
                    "SELECT id FROM twitch_stream_sessions WHERE streamer_login = ? AND ended_at IS NULL "
                    "ORDER BY started_at DESC LIMIT 1",
                    (login.lower(),),
                ).fetchone()
        except Exception:
            log.debug("Lookup offene Session fehlgeschlagen fuer %s", login, exc_info=True)
            return None
        if not row:
            return None
        session_id = int(row["id"] if hasattr(row, "keys") else row[0])
        cache = self._get_active_sessions_cache()
        cache[login.lower()] = session_id
        return session_id

    def _get_active_session_id(self, login: str) -> int | None:
        cache = self._get_active_sessions_cache()
        cached = cache.get(login.lower())
        if cached:
            return cached
        return self._lookup_open_session_id(login)

    async def _ensure_stream_session(
        self,
        *,
        login: str,
        stream: dict,
        previous_state: dict,
        twitch_user_id: str | None,
    ) -> int | None:
        login_lower = login.lower()
        stream_id = str(stream.get("id") or "").strip() or None

        session_id = self._get_active_session_id(login_lower)
        if session_id:
            try:
                with storage.get_conn() as c:
                    row = c.execute(
                        "SELECT stream_id FROM twitch_stream_sessions WHERE id = ?",
                        (session_id,),
                    ).fetchone()
                current_stream_id = (
                    str(row["stream_id"] if hasattr(row, "keys") else row[0] or "").strip()
                    if row
                    else ""
                )
            except Exception:
                current_stream_id = ""
            if current_stream_id and stream_id and current_stream_id != stream_id:
                await self._finalize_stream_session(login=login_lower, reason="restarted")
                session_id = None

        if session_id:
            return session_id

        followers_start = await self._fetch_followers_total_safe(
            twitch_user_id=twitch_user_id,
            login=login_lower,
            stream=stream,
        )
        started_at_iso = self._extract_stream_start(stream, previous_state)
        stream_title = str(stream.get("title") or "").strip()
        language = str(stream.get("language") or "").strip()
        is_mature = bool(stream.get("is_mature"))
        tags_list = stream.get("tags") or []
        tags_str = ",".join(tags_list) if isinstance(tags_list, list) else ""

        session_id = self._start_stream_session(
            login=login_lower,
            stream=stream,
            started_at_iso=started_at_iso,
            twitch_user_id=twitch_user_id,
            followers_start=followers_start,
            title=stream_title,
            language=language,
            is_mature=is_mature,
            tags=tags_str,
        )
        # --- Experimental hook: session start ---
        try:
            exp_on_start = getattr(self, "_exp_on_session_start", None)
            if callable(exp_on_start):
                exp_on_start(login=login_lower, stream=stream, started_at_iso=started_at_iso)
        except Exception:
            log.debug("exp: _exp_on_session_start fehlgeschlagen für %s", login_lower, exc_info=True)
        return session_id

    def _start_stream_session(
        self,
        *,
        login: str,
        stream: dict,
        started_at_iso: str | None,
        twitch_user_id: str | None,
        followers_start: int | None,
        title: str = "",
        language: str = "",
        is_mature: bool = False,
        tags: str = "",
    ) -> int | None:
        start_ts = started_at_iso or datetime.now(UTC).isoformat(timespec="seconds")
        viewer_count = int(stream.get("viewer_count") or 0)
        stream_id = str(stream.get("id") or "").strip() or None
        game_name = (stream.get("game_name") or "").strip() or None
        had_deadlock_initial = bool(self._stream_is_in_target_category(stream))
        session_id: int | None = None
        try:
            with storage.get_conn() as c:
                cur = c.execute(
                    """
                    INSERT INTO twitch_stream_sessions (
                        streamer_login, stream_id, started_at, start_viewers, peak_viewers,
                        end_viewers, avg_viewers, samples, followers_start, stream_title,
                        language, is_mature, tags, game_name, had_deadlock_in_session
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    RETURNING id
                    """,
                    (
                        login,
                        stream_id,
                        start_ts,
                        viewer_count,
                        viewer_count,
                        viewer_count,
                        float(viewer_count),
                        0,
                        followers_start,
                        title,
                        language,
                        bool(is_mature),
                        tags,
                        game_name,
                        had_deadlock_initial,
                    ),
                )
                session_id = int(cur.fetchone()[0])
                c.execute(
                    "UPDATE twitch_live_state SET active_session_id = ? WHERE streamer_login = ?",
                    (session_id, login),
                )
        except Exception:
            log.debug("Konnte neue Twitch-Session nicht speichern: %s", login, exc_info=True)
            return None
        if session_id is not None:
            self._get_active_sessions_cache()[login] = session_id
        return session_id

    def _record_session_sample(self, *, login: str, stream: dict) -> None:
        session_id = self._get_active_session_id(login)
        if session_id is None:
            return
        now_dt = datetime.now(UTC)
        viewer_count = int(stream.get("viewer_count") or 0)
        try:
            with storage.get_conn() as c:
                session_row = c.execute(
                    "SELECT started_at, samples, avg_viewers, start_viewers, peak_viewers "
                    "FROM twitch_stream_sessions WHERE id = ?",
                    (session_id,),
                ).fetchone()
                if not session_row:
                    return
                start_dt = (
                    self._parse_dt(
                        session_row["started_at"]
                        if hasattr(session_row, "keys")
                        else session_row[0]
                    )
                    or now_dt
                )
                minutes_from_start = int(max(0, (now_dt - start_dt).total_seconds() // 60))
                c.execute(
                    """
                    INSERT INTO twitch_session_viewers
                        (session_id, ts_utc, minutes_from_start, viewer_count)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (session_id, ts_utc) DO UPDATE SET
                        minutes_from_start = EXCLUDED.minutes_from_start,
                        viewer_count = EXCLUDED.viewer_count
                    """,
                    (
                        session_id,
                        now_dt.isoformat(timespec="seconds"),
                        minutes_from_start,
                        viewer_count,
                    ),
                )
                samples = int(
                    session_row["samples"] if hasattr(session_row, "keys") else session_row[1] or 0
                )
                avg_prev = float(
                    session_row["avg_viewers"]
                    if hasattr(session_row, "keys")
                    else session_row[2] or 0.0
                )
                new_samples = samples + 1
                new_avg = ((avg_prev * samples) + viewer_count) / max(1, new_samples)
                start_viewers = (
                    int(
                        session_row["start_viewers"]
                        if hasattr(session_row, "keys")
                        else session_row[3] or 0
                    )
                    or viewer_count
                )
                peak_viewers = int(
                    session_row["peak_viewers"]
                    if hasattr(session_row, "keys")
                    else session_row[4] or 0
                )
                peak_viewers = max(peak_viewers, viewer_count)
                c.execute(
                    """
                    UPDATE twitch_stream_sessions
                       SET samples = ?, avg_viewers = ?, peak_viewers = ?, end_viewers = ?, start_viewers = ?
                     WHERE id = ?
                    """,
                    (
                        new_samples,
                        new_avg,
                        peak_viewers,
                        viewer_count,
                        start_viewers,
                        session_id,
                    ),
                )
        except Exception:
            log.debug("Konnte Session-Sample nicht speichern fuer %s", login, exc_info=True)
        else:
            # --- Experimental hook: sample ---
            try:
                exp_sample = getattr(self, "_exp_on_session_sample", None)
                exp_get_id = getattr(self, "_get_exp_session_id", None)
                if callable(exp_sample) and callable(exp_get_id):
                    exp_id = exp_get_id(login)
                    if exp_id is not None:
                        exp_sample(login=login, exp_session_id=exp_id, stream=stream)
            except Exception:
                log.debug("exp: _exp_on_session_sample fehlgeschlagen für %s", login, exc_info=True)

    async def _finalize_stream_session(self, *, login: str, reason: str = "done") -> None:
        login_lower = login.lower()
        cache = self._get_active_sessions_cache()
        session_id = cache.pop(login_lower, None) or self._lookup_open_session_id(login_lower)
        if session_id is None:
            return

        now_dt = datetime.now(UTC)
        try:
            with storage.get_conn() as c:
                session_row = c.execute(
                    "SELECT * FROM twitch_stream_sessions WHERE id = ?",
                    (session_id,),
                ).fetchone()
        except Exception:
            log.debug("Konnte Session nicht laden fuer Abschluss: %s", login, exc_info=True)
            return
        if not session_row:
            return

        def _row_val(row, key, idx, default=None):
            if hasattr(row, "keys"):
                try:
                    return row[key]
                except Exception:
                    return default
            try:
                return row[idx]
            except Exception:
                return default

        started_at_raw = _row_val(session_row, "started_at", 3, None)
        start_dt = self._parse_dt(started_at_raw) or now_dt
        duration_seconds = int(max(0, (now_dt - start_dt).total_seconds()))

        try:
            with storage.get_conn() as c:
                viewer_rows = c.execute(
                    "SELECT minutes_from_start, viewer_count FROM twitch_session_viewers WHERE session_id = ? ORDER BY ts_utc",
                    (session_id,),
                ).fetchall()
        except Exception:
            viewer_rows = []

        def _retention_at(minutes: int, start_viewers: int) -> float | None:
            if not viewer_rows:
                return None
            # Find peak viewer count BEFORE the target minute as baseline
            peak_before = start_viewers
            for row in viewer_rows:
                mins = int(_row_val(row, "minutes_from_start", 0, 0) or 0)
                val = int(_row_val(row, "viewer_count", 1, 0) or 0)
                if mins < minutes:
                    peak_before = max(peak_before, val)
            if peak_before <= 0:
                return None
            # Find closest viewer count AT or AFTER target minute
            best: tuple[int, int] | None = None
            for row in viewer_rows:
                mins = int(_row_val(row, "minutes_from_start", 0, 0) or 0)
                val = int(_row_val(row, "viewer_count", 1, 0) or 0)
                if mins < minutes:
                    continue
                if best is None or mins < best[0]:
                    best = (mins, val)
            # Fallback to last data point if stream ended before target
            if best is None:
                last = viewer_rows[-1]
                best = (
                    int(_row_val(last, "minutes_from_start", 0, 0) or 0),
                    int(_row_val(last, "viewer_count", 1, 0) or 0),
                )
            if best is None:
                return None
            return max(0.0, min(1.0, best[1] / peak_before))

        start_viewers = int(_row_val(session_row, "start_viewers", 6, 0) or 0)
        end_viewers = int(_row_val(session_row, "end_viewers", 8, 0) or 0)
        peak_viewers = int(_row_val(session_row, "peak_viewers", 7, 0) or 0)
        avg_viewers = float(_row_val(session_row, "avg_viewers", 9, 0.0) or 0.0)
        samples = int(_row_val(session_row, "samples", 10, 0) or 0)

        if viewer_rows:
            end_viewers = int(
                _row_val(viewer_rows[-1], "viewer_count", 1, end_viewers) or end_viewers
            )
            peak_viewers = max(
                peak_viewers,
                *(int(_row_val(vr, "viewer_count", 1, 0) or 0) for vr in viewer_rows),
            )
            samples = max(samples, len(viewer_rows))
            try:
                avg_viewers = sum(
                    int(_row_val(vr, "viewer_count", 1, 0) or 0) for vr in viewer_rows
                ) / max(1, len(viewer_rows))
            except Exception as exc:
                log.debug("Konnte Durchschnitts-Viewerzahl nicht berechnen", exc_info=exc)

        retention_5 = _retention_at(5, start_viewers)
        retention_10 = _retention_at(10, start_viewers)
        retention_20 = _retention_at(20, start_viewers)

        dropoff_pct: float | None = None
        dropoff_label = ""
        prev_val = start_viewers or (viewer_rows[0]["viewer_count"] if viewer_rows else 0)
        for row in viewer_rows:
            current_val = int(_row_val(row, "viewer_count", 1, 0) or 0)
            mins = int(_row_val(row, "minutes_from_start", 0, 0) or 0)
            if prev_val > 0 and current_val < prev_val:
                delta = prev_val - current_val
                pct = delta / prev_val
                if dropoff_pct is None or pct > dropoff_pct:
                    dropoff_pct = pct
                    dropoff_label = f"t={mins}m ({prev_val}->{current_val})"
            prev_val = current_val

        try:
            with storage.get_conn() as c:
                chatter_row = c.execute(
                    """
                    SELECT COUNT(*) AS uniq,
                           SUM(is_first_time_streamer) AS firsts
                      FROM twitch_session_chatters
                     WHERE session_id = ?
                    """,
                    (session_id,),
                ).fetchone()
        except Exception:
            chatter_row = None
        unique_chatters = int(_row_val(chatter_row, "uniq", 0, 0) or 0) if chatter_row else 0
        first_time_chatters = int(_row_val(chatter_row, "firsts", 1, 0) or 0) if chatter_row else 0
        returning_chatters = max(0, unique_chatters - first_time_chatters)

        followers_start = _row_val(session_row, "followers_start", 19, None)

        twitch_user_id: str | None = None
        had_deadlock_state = False
        try:
            with storage.get_conn() as c:
                state_row = c.execute(
                    "SELECT twitch_user_id, last_game, had_deadlock_in_session FROM twitch_live_state WHERE streamer_login = ?",
                    (login_lower,),
                ).fetchone()
            if state_row is not None:
                twitch_user_id = _row_val(state_row, "twitch_user_id", 0, None)
                last_game_value = _row_val(state_row, "last_game", 1, None)
                had_deadlock_state = bool(
                    int(_row_val(state_row, "had_deadlock_in_session", 2, 0) or 0)
                )
            else:
                last_game_value = None
        except Exception:
            last_game_value = None
            twitch_user_id = None
            had_deadlock_state = False

        followers_end = await self._fetch_followers_total_safe(
            twitch_user_id=twitch_user_id,
            login=login_lower,
            stream=None,
        )
        follower_delta = None
        if followers_start is not None and followers_end is not None:
            if int(followers_end) == 0 and int(followers_start) > 0:
                # API returned 0 without user token — treat as missing data
                followers_end = None
                follower_delta = None
            else:
                follower_delta = int(followers_end) - int(followers_start)

        target_game_lower = self._get_target_game_lower()
        last_game_lower = (last_game_value or "").strip().lower() if last_game_value else ""
        had_deadlock_session = had_deadlock_state or (
            bool(target_game_lower) and last_game_lower == target_game_lower
        )

        try:
            with storage.get_conn() as c:
                c.execute(
                    """
                    UPDATE twitch_stream_sessions
                       SET ended_at = ?,
                           duration_seconds = ?,
                           end_viewers = ?,
                           peak_viewers = ?,
                           avg_viewers = ?,
                           samples = ?,
                           retention_5m = ?,
                           retention_10m = ?,
                           retention_20m = ?,
                           dropoff_pct = ?,
                           dropoff_label = ?,
                           unique_chatters = ?,
                           first_time_chatters = ?,
                           returning_chatters = ?,
                           followers_end = ?,
                           follower_delta = ?,
                           notes = ?,
                           had_deadlock_in_session = ?,
                           game_name = COALESCE(game_name, ?)
                     WHERE id = ?
                    """,
                    (
                        now_dt.isoformat(timespec="seconds"),
                        duration_seconds,
                        end_viewers,
                        peak_viewers,
                        avg_viewers,
                        samples,
                        retention_5,
                        retention_10,
                        retention_20,
                        dropoff_pct,
                        dropoff_label,
                        unique_chatters,
                        first_time_chatters,
                        returning_chatters,
                        followers_end,
                        follower_delta,
                        reason,
                        bool(had_deadlock_session),
                        last_game_value,
                        session_id,
                    ),
                )
                c.execute(
                    "UPDATE twitch_live_state SET active_session_id = NULL WHERE streamer_login = ?",
                    (login_lower,),
                )
        except Exception:
            log.debug(
                "Konnte Session-Abschluss nicht speichern: %s",
                login_lower,
                exc_info=True,
            )
        finally:
            cache.pop(login_lower, None)

        if callable(resolve_partner_raid_tracking_for_session):
            try:
                resolve_partner_raid_tracking_for_session(
                    twitch_user_id=twitch_user_id,
                    streamer_login=login_lower,
                    session_id=session_id,
                    session_ended_at=now_dt,
                )
            except Exception:
                log.debug(
                    "Partner raid score tracking resolve failed for %s session=%s",
                    login_lower,
                    session_id,
                    exc_info=True,
                )

        # --- Experimental hook: session finalize ---
        try:
            exp_finalize = getattr(self, "_exp_on_session_finalize", None)
            exp_get_id = getattr(self, "_get_exp_session_id", None)
            if callable(exp_finalize) and callable(exp_get_id):
                exp_id = exp_get_id(login_lower)
                if exp_id is not None:
                    exp_finalize(
                        login=login_lower,
                        exp_session_id=exp_id,
                        follower_delta=follower_delta,
                        now_dt=now_dt,
                    )
        except Exception:
            log.debug("exp: _exp_on_session_finalize fehlgeschlagen für %s", login_lower, exc_info=True)

    async def _fetch_followers_total_safe(
        self,
        *,
        twitch_user_id: str | None,
        login: str,
        stream: dict | None,
    ) -> int | None:
        if self.api is None:
            return None
        user_id = twitch_user_id
        if not user_id and stream:
            user_id = stream.get("user_id")

        user_token: str | None = None
        try:
            if hasattr(self, "_raid_bot") and self._raid_bot and self.api is not None:
                session = self.api.get_http_session()
                result = await self._raid_bot.auth_manager.get_valid_token_for_login(login, session)
                if result:
                    auth_user_id, token = result
                    user_id = user_id or auth_user_id
                    user_token = token
        except Exception:
            log.debug(
                "Konnte OAuth-Daten fuer Follower-Check nicht laden: %s",
                login,
                exc_info=True,
            )

        if not user_id:
            return None
        try:
            return await self.api.get_followers_total(str(user_id), user_token=user_token)
        except Exception:
            log.debug("Follower-Abfrage fehlgeschlagen fuer %s", login, exc_info=True)
            return None
