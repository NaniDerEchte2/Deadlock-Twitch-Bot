"""
Analytics API v2 - Audience Mixin.

Viewer/audience analytics: watch time, follower funnel, viewer overlap,
audience insights, audience demographics.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from aiohttp import web

from ..storage import pg as storage

log = logging.getLogger("TwitchStreams.AnalyticsV2")


class _AnalyticsAudienceMixin:
    """Mixin providing audience analytics endpoints."""

    def _calc_watch_distribution(
        self, sessions, conn=None, session_ids: list = None
    ) -> dict[str, Any]:
        """Calculate watch time distribution.

        Tries to use real per-viewer watch-time from chatters-API snapshots
        (last_seen_at - first_message_at) when sufficient data is available.
        Falls back to a retention-curve proxy otherwise.
        """
        if not sessions:
            return {
                "under5min": 0,
                "min5to15": 0,
                "min15to30": 0,
                "min30to60": 0,
                "over60min": 0,
                "avgWatchTime": 0,
                "medianWatchTime": 0,
                "sessionCount": 0,
                "dataQuality": {"method": "no_data", "coverage": 0.0},
            }

        total_sessions = len(sessions)
        total_viewers_in_sessions = sum(int(s[4] or 0) for s in sessions)  # avg_viewers col

        # --- Attempt: real watch-time from chatters-API snapshots ---
        real_minutes: list[float] = []
        if conn is not None and session_ids:
            if session_ids:
                session_ids_json = json.dumps([int(sid) for sid in session_ids])
                rows = conn.execute(
                    """
                    SELECT
                        ROUND(
                            GREATEST(
                                EXTRACT(EPOCH FROM COALESCE(last_seen_at, first_message_at))
                                - EXTRACT(EPOCH FROM COALESCE(first_message_at, last_seen_at)),
                                0
                            ) / 60.0
                        ) as watch_minutes
                    FROM twitch_session_chatters
                    WHERE session_id IN (
                        SELECT CAST(value AS BIGINT)
                        FROM json_array_elements_text(%s) AS t(value)
                    )
                      AND last_seen_at IS NOT NULL
                    """,
                    (session_ids_json,),
                ).fetchall()
                real_minutes = [float(r[0]) for r in rows if r[0] is not None and r[0] >= 0]

        if total_viewers_in_sessions > 0:
            coverage_real = len(real_minutes) / max(1, total_viewers_in_sessions)
        else:
            coverage_real = len(real_minutes) / max(1, total_sessions)

        # Decide whether real data is sufficient (>=10% coverage and >=5 Samples)
        use_real = coverage_real >= 0.1 and len(real_minutes) >= 5

        if use_real:
            total_viewers = len(real_minutes)
            under_5min = sum(1 for m in real_minutes if m < 5) / total_viewers * 100
            min_5_to_15 = sum(1 for m in real_minutes if 5 <= m < 15) / total_viewers * 100
            min_15_to_30 = sum(1 for m in real_minutes if 15 <= m < 30) / total_viewers * 100
            min_30_to_60 = sum(1 for m in real_minutes if 30 <= m < 60) / total_viewers * 100
            over_60min = sum(1 for m in real_minutes if m >= 60) / total_viewers * 100
            avg_watch_time = sum(real_minutes) / total_viewers
            # Median
            sorted_m = sorted(real_minutes)
            mid = len(sorted_m) // 2
            median_watch_time = (
                sorted_m[mid] if len(sorted_m) % 2 == 1 else (sorted_m[mid - 1] + sorted_m[mid]) / 2
            )
            # Approximate total-session coverage ratio
            coverage = min(1.0, coverage_real)
            data_quality = {
                "method": "chatters_api_snapshots",
                "coverage": round(coverage, 2),
            }
        else:
            # Not enough real data: surface that explicitly instead of proxying.
            data_quality = {
                "method": "insufficient_real_data",
                "coverage": round(min(1.0, coverage_real), 2),
            }
            return {
                "under5min": 0,
                "min5to15": 0,
                "min15to30": 0,
                "min30to60": 0,
                "over60min": 0,
                "avgWatchTime": 0,
                "medianWatchTime": 0,
                "sessionCount": total_sessions,
                "dataQuality": data_quality,
            }

        return {
            "under5min": round(max(0, under_5min), 1),
            "min5to15": round(max(0, min_5_to_15), 1),
            "min15to30": round(max(0, min_15_to_30), 1),
            "min30to60": round(max(0, min_30_to_60), 1),
            "over60min": round(max(0, over_60min), 1),
            "avgWatchTime": round(avg_watch_time, 1),
            "medianWatchTime": round(median_watch_time, 1),
            "sessionCount": total_sessions,
            "dataQuality": data_quality,
        }

    async def _api_v2_watch_time_distribution(self, request: web.Request) -> web.Response:
        """Get watch time distribution with previous period comparison."""
        self._require_v2_auth(request)

        streamer = request.query.get("streamer", "").strip() or None
        days = min(max(int(request.query.get("days", "30")), 7), 365)

        if not streamer:
            return web.json_response({"error": "Streamer required"}, status=400)

        try:
            with storage.get_conn() as conn:
                since_date = (datetime.now(UTC) - timedelta(days=days)).isoformat()
                prev_since_date = (datetime.now(UTC) - timedelta(days=days * 2)).isoformat()

                # Current period
                current_sessions = conn.execute(
                    """
                    SELECT s.id, s.retention_5m, s.retention_10m,
                           s.retention_20m, s.avg_viewers, s.start_viewers, s.end_viewers,
                           s.duration_seconds
                    FROM twitch_stream_sessions s
                    WHERE s.started_at >= ? AND LOWER(s.streamer_login) = ? AND s.ended_at IS NOT NULL
                """,
                    [since_date, streamer.lower()],
                ).fetchall()
                # Remap: (duration_seconds, ret5, ret10, ret20, avg_viewers, ...) for _calc
                current_sessions_remapped = [
                    (r[7], r[1], r[2], r[3], r[4]) for r in current_sessions
                ]

                # Previous period
                prev_sessions = conn.execute(
                    """
                    SELECT s.id, s.retention_5m, s.retention_10m,
                           s.retention_20m, s.avg_viewers, s.start_viewers, s.end_viewers,
                           s.duration_seconds
                    FROM twitch_stream_sessions s
                    WHERE s.started_at >= ? AND s.started_at < ? AND LOWER(s.streamer_login) = ? AND s.ended_at IS NOT NULL
                """,
                    [prev_since_date, since_date, streamer.lower()],
                ).fetchall()
                prev_sessions_remapped = [(r[7], r[1], r[2], r[3], r[4]) for r in prev_sessions]

                current_ids = [r[0] for r in current_sessions]
                prev_ids = [r[0] for r in prev_sessions]
                current = self._calc_watch_distribution(
                    current_sessions_remapped, conn=conn, session_ids=current_ids
                )
                previous = self._calc_watch_distribution(
                    prev_sessions_remapped, conn=conn, session_ids=prev_ids
                )

                # Calculate deltas
                deltas = {}
                for key in [
                    "under5min",
                    "min5to15",
                    "min15to30",
                    "min30to60",
                    "over60min",
                    "avgWatchTime",
                ]:
                    curr_val = current.get(key, 0)
                    prev_val = previous.get(key, 0)
                    if prev_val > 0:
                        deltas[key] = round(((curr_val - prev_val) / prev_val) * 100, 1)
                    else:
                        deltas[key] = None

                session_count = int(current.get("sessionCount", 0) or 0)
                if session_count >= 20:
                    confidence = "medium"
                elif session_count >= 8:
                    confidence = "low"
                else:
                    confidence = "very_low"

                dq = current.get("dataQuality", {})
                return web.json_response(
                    {
                        **current,
                        "previous": previous,
                        "deltas": deltas,
                        "dataQuality": {
                            "confidence": confidence,
                            "sessions": session_count,
                            "method": dq.get("method", "retention_curve_proxy"),
                            "coverage": dq.get("coverage", 0.0),
                        },
                    }
                )
        except Exception as exc:
            log.exception("Error in watch time distribution API")
            return web.json_response({"error": str(exc)}, status=500)

    async def _api_v2_follower_funnel(self, request: web.Request) -> web.Response:
        """Get follower conversion funnel data."""
        self._require_v2_auth(request)

        streamer = request.query.get("streamer", "").strip() or None
        days = min(max(int(request.query.get("days", "30")), 7), 365)

        if not streamer:
            return web.json_response({"error": "Streamer required"}, status=400)

        try:
            with storage.get_conn() as conn:
                since_date = (datetime.now(UTC) - timedelta(days=days)).isoformat()
                streamer_login = streamer.lower()

                # Base session and follower stats.
                stats = conn.execute(
                    """
                    SELECT
                        COUNT(*) as session_count,
                        COALESCE(SUM(s.duration_seconds), 0) as total_duration,
                        AVG(s.avg_viewers) as avg_viewers,
                        COALESCE(SUM(s.avg_viewers * s.duration_seconds / 3600.0), 0) as total_hours_watched,
                        AVG(s.retention_10m) as avg_retention_10m,
                        SUM(CASE WHEN s.follower_delta IS NOT NULL
                             AND NOT (s.followers_end = 0 AND s.followers_start > 0)
                             THEN s.follower_delta ELSE 0 END) as net_followers,
                        SUM(CASE WHEN s.follower_delta > 0
                             AND NOT (s.followers_end = 0 AND s.followers_start > 0)
                             THEN s.follower_delta ELSE 0 END) as gained_followers,
                        COUNT(CASE WHEN s.follower_delta IS NOT NULL
                             AND NOT (s.followers_end = 0 AND s.followers_start > 0)
                             THEN 1 END) as follower_valid_samples
                    FROM twitch_stream_sessions s
                    WHERE s.started_at >= ?
                      AND LOWER(s.streamer_login) = ?
                      AND s.ended_at IS NOT NULL
                    """,
                    [since_date, streamer_login],
                ).fetchone()

                session_count = int(stats[0]) if stats and stats[0] else 0
                if session_count == 0:
                    return web.json_response(
                        {
                            "uniqueViewers": 0,
                            "returningViewers": 0,
                            "newFollowers": 0,
                            "followsDuringStream": 0,
                            "netFollowerDelta": 0,
                            "conversionRate": 0,
                            "conversionDataSource": "session_delta_fallback",
                            "avgTimeToFollow": 0,
                            "followersBySource": {
                                "organic": 0,
                                "raids": 0,
                                "hosts": 0,
                                "other": 0,
                            },
                            "dataQuality": {
                                "confidence": "low",
                                "reason": "no_sessions",
                            },
                        }
                    )

                total_duration = float(stats[1]) if stats[1] else 0.0
                avg_viewers = float(stats[2]) if stats[2] else 0.0
                total_hours_watched = float(stats[3]) if stats[3] else 0.0
                avg_retention_10m = float(stats[4]) if stats[4] else 0.0  # 0..1
                net_followers = int(stats[5]) if stats[5] else 0
                gained_followers = int(stats[6]) if stats[6] else 0
                follower_valid_samples = int(stats[7]) if stats[7] else 0

                # Distinct chatter cohorts in selected period (less inflation than SUM per session).
                chatter_stats = conn.execute(
                    """
                    SELECT
                        COUNT(DISTINCT COALESCE(NULLIF(sc.chatter_login, ''), sc.chatter_id)) as unique_chatters,
                        COUNT(
                            DISTINCT CASE
                                WHEN COALESCE(sc.is_first_time_global, FALSE) IS FALSE
                                THEN COALESCE(NULLIF(sc.chatter_login, ''), sc.chatter_id)
                            END
                        ) as returning_chatters,
                        COUNT(DISTINCT COALESCE(NULLIF(sc.chatter_login, ''), sc.chatter_id)) as tracked_viewers
                    FROM twitch_session_chatters sc
                    JOIN twitch_stream_sessions s ON s.id = sc.session_id
                    WHERE s.started_at >= ?
                      AND LOWER(s.streamer_login) = ?
                      AND s.ended_at IS NOT NULL
                    """,
                    [since_date, streamer_login],
                ).fetchone()
                unique_chatters = int(chatter_stats[0]) if chatter_stats and chatter_stats[0] else 0
                returning_chatters = (
                    int(chatter_stats[1]) if chatter_stats and chatter_stats[1] else 0
                )
                total_viewers_tracked = (
                    int(chatter_stats[2]) if chatter_stats and chatter_stats[2] else 0
                )

                # Real follow events during streams (primary source)
                follow_events_row = conn.execute(
                    """
                    SELECT COUNT(DISTINCT fe.id) as follows_during_stream
                    FROM twitch_follow_events fe
                    JOIN twitch_stream_sessions ss
                        ON ss.streamer_login = fe.streamer_login
                       AND fe.followed_at BETWEEN ss.started_at
                           AND COALESCE(ss.ended_at, NOW())
                    WHERE LOWER(ss.streamer_login) = ?
                      AND ss.started_at >= ?
                      AND ss.ended_at IS NOT NULL
                    """,
                    [streamer_login, since_date],
                ).fetchone()
                follows_during_stream = (
                    int(follow_events_row[0]) if follow_events_row and follow_events_row[0] else 0
                )

                # Primäre und einzige Quelle: distinct Session-Chatters (inkl. Chatters-API Lurker).
                # Keine Schätzung, wenn keine Samples vorhanden.
                unique_viewers = total_viewers_tracked or unique_chatters
                unique_viewers_method = (
                    "distinct_session_chatters" if unique_viewers > 0 else "no_viewer_data"
                )

                # Follow conversion: prefer real events, fallback to session delta
                if follows_during_stream > 0:
                    conversion_source = "follow_events"
                    gained_followers_for_conversion = follows_during_stream
                else:
                    conversion_source = "session_delta_fallback"
                    gained_followers_for_conversion = gained_followers

                conversion_rate = (
                    (gained_followers_for_conversion / unique_viewers * 100.0)
                    if unique_viewers > 0
                    else 0.0
                )

                # Estimated time-to-follow: scaled to session length, bounded to practical range.
                avg_session_mins = (
                    (total_duration / max(1, session_count) / 60.0) if total_duration > 0 else 0.0
                )
                avg_time_to_follow = max(5.0, min(45.0, avg_session_mins * 0.4))

                # Raid inflow proxy for source split.
                raids_received = conn.execute(
                    """
                    SELECT COUNT(*), COALESCE(SUM(viewer_count), 0)
                    FROM twitch_raid_history
                    WHERE LOWER(to_broadcaster_login) = ?
                      AND executed_at >= ?
                      AND COALESCE(success, FALSE) IS TRUE
                    """,
                    [streamer_login, since_date],
                ).fetchone()
                raid_count = int(raids_received[0]) if raids_received and raids_received[0] else 0
                raid_viewers = int(raids_received[1]) if raids_received and raids_received[1] else 0

                # Conservative conversion assumption for raid-origin follows.
                raid_followers = min(int(raid_viewers * 0.05), gained_followers_for_conversion)
                organic_followers = max(0, gained_followers_for_conversion - raid_followers)

                if unique_viewers == 0:
                    confidence = "low"
                elif follower_valid_samples >= max(3, int(session_count * 0.6)):
                    confidence = "high"
                elif follower_valid_samples >= 1:
                    confidence = "medium"
                else:
                    confidence = "low"

                return web.json_response(
                    {
                        "uniqueViewers": unique_viewers,
                        "returningViewers": returning_chatters,
                        "newFollowers": gained_followers_for_conversion,
                        "followsDuringStream": follows_during_stream,
                        "netFollowerDelta": net_followers,
                        "conversionRate": round(conversion_rate, 2),
                        "conversionDataSource": conversion_source,
                        "avgTimeToFollow": round(avg_time_to_follow, 0),
                        "followersBySource": {
                            "organic": organic_followers,
                            "raids": raid_followers,
                            "hosts": 0,  # Host-specific attribution is not tracked.
                            "other": 0,
                        },
                        "dataQuality": {
                            "confidence": confidence,
                            "sessions": session_count,
                            "followerValidSamples": follower_valid_samples,
                            "raidEvents": raid_count,
                            "uniqueViewersMethod": unique_viewers_method,
                        },
                    }
                )
        except Exception as exc:
            log.exception("Error in follower funnel API")
            return web.json_response({"error": str(exc)}, status=500)

    async def _api_v2_viewer_overlap(self, request: web.Request) -> web.Response:
        """Get viewer overlap with other channels."""
        self._require_v2_auth(request)

        streamer = request.query.get("streamer", "").strip()
        limit = min(max(int(request.query.get("limit", "20")), 5), 50)

        if not streamer:
            return web.json_response({"error": "Streamer required"}, status=400)

        try:
            with storage.get_conn() as conn:
                base = streamer.lower()
                rows = conn.execute(
                    """
                    SELECT
                        c2.streamer_login as other_streamer,
                        COUNT(DISTINCT c1.chatter_login) as shared_chatters
                    FROM twitch_chatter_rollup c1
                    JOIN twitch_chatter_rollup c2 ON c1.chatter_login = c2.chatter_login
                    WHERE LOWER(c1.streamer_login) = ?
                      AND LOWER(c2.streamer_login) != ?
                    GROUP BY c2.streamer_login
                    ORDER BY shared_chatters DESC
                    LIMIT ?
                """,
                    [base, base, limit],
                ).fetchall()

                # Totals for A and B
                total_a = (
                    conn.execute(
                        """
                        SELECT COUNT(DISTINCT chatter_login)
                        FROM twitch_chatter_rollup
                        WHERE LOWER(streamer_login) = ?
                    """,
                        [base],
                    ).fetchone()[0]
                    or 1
                )

                totals_b = {
                    r[0].lower(): (
                        conn.execute(
                            """
                        SELECT COUNT(DISTINCT chatter_login)
                        FROM twitch_chatter_rollup
                        WHERE LOWER(streamer_login) = ?
                        """,
                            [r[0].lower()],
                        ).fetchone()[0]
                        or 1
                    )
                    for r in rows
                }

                data = []
                for r in rows:
                    other = r[0]
                    shared = r[1]
                    total_b = totals_b.get(other.lower(), 1)
                    jaccard = shared / max(1, (total_a + total_b - shared))
                    overlap_pct = round(jaccard * 100, 1)
                    data.append(
                        {
                            "streamerA": streamer,
                            "streamerB": other,
                            "sharedChatters": shared,
                            "totalChattersA": total_a,
                            "totalChattersB": total_b,
                            "overlapAtoB": round(shared / total_a * 100, 1),
                            "overlapBtoA": round(shared / total_b * 100, 1),
                            "jaccard": overlap_pct,
                            # Backwards compatible field expected by the React dashboard
                            # (used for bar width + display). Uses the symmetric Jaccard percentage.
                            "overlapPercentage": overlap_pct,
                        }
                    )

                return web.json_response(data)
        except Exception as exc:
            log.exception("Error in viewer overlap API")
            return web.json_response({"error": str(exc)}, status=500)

    async def _api_v2_audience_insights(self, request: web.Request) -> web.Response:
        """Get combined audience insights (all in one call)."""
        self._require_v2_auth(request)

        streamer = request.query.get("streamer", "").strip() or None
        days = min(max(int(request.query.get("days", "30")), 7), 365)

        if not streamer:
            return web.json_response({"error": "Streamer required"}, status=400)

        try:
            # Fetch all data in parallel-ish (reuse endpoints)
            watch_time_req = type(
                "Request", (), {"query": {"streamer": streamer, "days": str(days)}}
            )()
            watch_time_req.headers = request.headers
            funnel_req = type("Request", (), {"query": {"streamer": streamer, "days": str(days)}})()
            funnel_req.headers = request.headers
            tags_req = type(
                "Request",
                (),
                {"query": {"streamer": streamer, "days": str(days), "limit": "10"}},
            )()
            tags_req.headers = request.headers
            titles_req = type(
                "Request",
                (),
                {"query": {"streamer": streamer, "days": str(days), "limit": "10"}},
            )()
            titles_req.headers = request.headers

            # Call internal methods directly
            with storage.get_conn() as conn:
                since_date = (datetime.now(UTC) - timedelta(days=days)).isoformat()
                prev_since = (datetime.now(UTC) - timedelta(days=days * 2)).isoformat()

                # Current period metrics
                current = conn.execute(
                    """
                    SELECT
                        AVG(s.retention_10m) as retention,
                        SUM(CASE WHEN s.follower_delta IS NOT NULL
                             AND NOT (s.followers_end = 0 AND s.followers_start > 0)
                             THEN s.follower_delta ELSE 0 END) as followers,
                        SUM(s.returning_chatters) as returning,
                        SUM(s.unique_chatters) as unique_chatters
                    FROM twitch_stream_sessions s
                    WHERE s.started_at >= ? AND LOWER(s.streamer_login) = ? AND s.ended_at IS NOT NULL
                """,
                    [since_date, streamer.lower()],
                ).fetchone()

                # Previous period for comparison
                prev = conn.execute(
                    """
                    SELECT
                        AVG(s.retention_10m) as retention,
                        SUM(CASE WHEN s.follower_delta IS NOT NULL
                             AND NOT (s.followers_end = 0 AND s.followers_start > 0)
                             THEN s.follower_delta ELSE 0 END) as followers,
                        SUM(s.returning_chatters) as returning,
                        SUM(s.unique_chatters) as unique_chatters
                    FROM twitch_stream_sessions s
                    WHERE s.started_at >= ? AND s.started_at < ? AND LOWER(s.streamer_login) = ? AND s.ended_at IS NOT NULL
                """,
                    [prev_since, since_date, streamer.lower()],
                ).fetchone()

                # Calculate trends
                def calc_trend(curr, prev):
                    if not prev or prev == 0:
                        return 0
                    return round(((curr - prev) / prev) * 100, 1)

                curr_retention = float(current[0]) * 100 if current and current[0] else 0
                prev_retention = float(prev[0]) * 100 if prev and prev[0] else 0
                curr_unique = int(current[3]) if current and current[3] else 0
                prev_unique = int(prev[3]) if prev and prev[3] else 0
                curr_returning = int(current[2]) if current and current[2] else 0
                prev_returning = int(prev[2]) if prev and prev[2] else 0

                return_rate = (curr_returning / curr_unique * 100) if curr_unique > 0 else 0
                prev_return_rate = (prev_returning / prev_unique * 100) if prev_unique > 0 else 0

                return web.json_response(
                    {
                        "trends": {
                            "watchTimeChange": calc_trend(curr_retention, prev_retention),
                            "conversionChange": 0,  # Would need follower tracking improvement
                            "viewerReturnRate": round(return_rate, 1),
                            "viewerReturnChange": calc_trend(return_rate, prev_return_rate),
                        }
                    }
                )
        except Exception as exc:
            log.exception("Error in audience insights API")
            return web.json_response({"error": str(exc)}, status=500)

    async def _api_v2_audience_demographics(self, request: web.Request) -> web.Response:
        """Get estimated audience demographics based on available data."""
        self._require_v2_auth(request)

        streamer = request.query.get("streamer", "").strip() or None
        days = min(max(int(request.query.get("days", "30")), 7), 365)

        if not streamer:
            return web.json_response({"error": "Streamer required"}, status=400)

        try:
            with storage.get_conn() as conn:
                since_date = (datetime.now(UTC) - timedelta(days=days)).isoformat()

                # Language mix (avoid hard-coded German defaults)
                language_rows = conn.execute(
                    """
                    SELECT
                        LOWER(COALESCE(NULLIF(s.language, ''), 'unknown')) as lang,
                        COUNT(*) as sessions,
                        AVG(s.avg_viewers) as avg_viewers
                    FROM twitch_stream_sessions s
                    WHERE s.started_at >= ? AND LOWER(s.streamer_login) = ? AND s.ended_at IS NOT NULL
                    GROUP BY lang
                    ORDER BY sessions DESC
                """,
                    [since_date, streamer.lower()],
                ).fetchall()

                language_session_total = int(sum(r[1] or 0 for r in language_rows))
                primary_lang_row = language_rows[0] if language_rows else None
                primary_lang_code = (
                    (primary_lang_row[0] or "unknown") if primary_lang_row else "unknown"
                )
                language_confidence = (
                    round(((primary_lang_row[1] or 0) / max(1, language_session_total)) * 100, 1)
                    if primary_lang_row
                    else 0.0
                )

                def _lang_label(code: str) -> str:
                    c = (code or "unknown").lower()
                    if c.startswith("de"):
                        return "German"
                    if c.startswith("en"):
                        return "English"
                    if c.startswith("fr"):
                        return "French"
                    if c.startswith("es"):
                        return "Spanish"
                    if c.startswith("pt"):
                        return "Portuguese"
                    if c.startswith("tr"):
                        return "Turkish"
                    if c.startswith("pl"):
                        return "Polish"
                    if c.startswith("ru"):
                        return "Russian"
                    if c.startswith("it"):
                        return "Italian"
                    if c == "unknown":
                        return "Unbekannt"
                    return c

                primary_language_label = _lang_label(primary_lang_code)

                # Analyze stream times to estimate audience timezone/region
                time_stats = conn.execute(
                    """
                    SELECT
                        CAST(strftime('%H', s.started_at) AS INTEGER) as hour,
                        AVG(s.avg_viewers) as avg_viewers,
                        COUNT(*) as stream_count
                    FROM twitch_stream_sessions s
                    WHERE s.started_at >= ? AND LOWER(s.streamer_login) = ? AND s.ended_at IS NOT NULL
                    GROUP BY hour
                """,
                    [since_date, streamer.lower()],
                ).fetchall()

                def _hour_weight(row) -> float:
                    avg_v = float(row[1] or 0.0)
                    stream_count = float(row[2] or 0.0)
                    return max(1.0, avg_v) * max(1.0, stream_count)

                peak_hours = (
                    [r[0] for r in sorted(time_stats, key=_hour_weight, reverse=True)[:3]]
                    if time_stats
                    else [20, 21, 22]
                )

                # Region estimation combines language hint + schedule
                region_scores = {"DACH": 0.0, "Rest EU": 0.0, "NA": 0.0, "Other": 0.0}
                lang_hint = primary_lang_code.lower()
                dach_langs = {"de", "de-de", "de-at", "de-ch", "ger", "german"}
                eu_langs = {
                    "fr",
                    "fr-fr",
                    "es",
                    "es-es",
                    "it",
                    "pl",
                    "ru",
                    "nl",
                    "sv",
                    "da",
                    "fi",
                    "tr",
                }

                if lang_hint in dach_langs:
                    region_scores["DACH"] += 3.5
                    region_scores["Rest EU"] += 2.0
                elif lang_hint in eu_langs:
                    region_scores["Rest EU"] += 3.0
                    region_scores["Other"] += 0.8
                elif lang_hint.startswith("en"):
                    region_scores["NA"] += 2.5
                    region_scores["Rest EU"] += 2.0
                elif lang_hint.startswith("pt") or lang_hint.startswith("es"):
                    region_scores["Other"] += 2.5  # LATAM/BR bucket -> Other bucket
                    region_scores["Rest EU"] += 1.0
                else:
                    region_scores["Other"] += 2.0

                for hour, avg_viewers, stream_count in time_stats:
                    score = _hour_weight((hour, avg_viewers, stream_count))
                    if 17 <= hour <= 23:
                        region_scores["Rest EU"] += score
                        if lang_hint in dach_langs:
                            region_scores["DACH"] += score * 0.7
                    if 0 <= hour <= 5:
                        region_scores["NA"] += score
                    if 6 <= hour <= 12:
                        region_scores["Other"] += score
                    if 12 < hour < 17:
                        region_scores["Rest EU"] += score * 0.5

                total_region_score = sum(region_scores.values()) or 1.0
                regions = [
                    {"region": name, "percentage": round(score / total_region_score * 100, 1)}
                    for name, score in region_scores.items()
                ]

                # Chat activity analysis for engagement type (weight by samples/duration to avoid short-session bias)
                chat_stats = conn.execute(
                    """
                    WITH weighted AS (
                        SELECT
                            COALESCE(NULLIF(s.samples, 0), NULLIF(s.duration_seconds, 0), 1) AS weight,
                            s.unique_chatters,
                            s.returning_chatters,
                            s.avg_viewers
                        FROM twitch_stream_sessions s
                        WHERE s.started_at >= ? AND LOWER(s.streamer_login) = ? AND s.ended_at IS NOT NULL
                    )
                    SELECT
                        SUM(CASE WHEN avg_viewers > 0 THEN (unique_chatters * weight * 1.0) / avg_viewers ELSE 0 END) AS weighted_chat_rate_sum,
                        SUM(weight) AS total_weight,
                        SUM(unique_chatters) AS total_chatters,
                        SUM(returning_chatters) AS total_returning
                    FROM weighted
                """,
                    [since_date, streamer.lower()],
                ).fetchone()

                total_weight = float(chat_stats[1]) if chat_stats and chat_stats[1] else 0
                weighted_chat_rate_sum = float(chat_stats[0]) if chat_stats and chat_stats[0] else 0

                chat_rate_raw = weighted_chat_rate_sum / total_weight if total_weight > 0 else 0
                chat_rate = min(1.0, chat_rate_raw)

                # Distinct viewer cohorts with fallback when is_first_time_global is missing
                viewer_rows = conn.execute(
                    """
                    WITH per_user AS (
                        SELECT
                            COALESCE(NULLIF(sc.chatter_login, ''), sc.chatter_id) AS user_id,
                            NULLIF(sc.chatter_login, '') AS chatter_login,
                            COUNT(DISTINCT sc.session_id) AS session_count,
                            MAX(CASE WHEN sc.messages > 0 THEN 1 ELSE 0 END) AS active_flag,
                            MAX(CASE WHEN sc.messages = 0 AND sc.seen_via_chatters_api IS TRUE THEN 1 ELSE 0 END) AS lurker_flag,
                            MAX(CASE WHEN sc.is_first_time_global IS TRUE THEN 1 ELSE 0 END) AS first_time_flag,
                            MAX(CASE WHEN sc.is_first_time_global IS NOT NULL THEN 1 ELSE 0 END) AS has_first_flag
                        FROM twitch_session_chatters sc
                        JOIN twitch_stream_sessions s ON s.id = sc.session_id
                        WHERE s.started_at >= ? AND LOWER(s.streamer_login) = ? AND s.ended_at IS NOT NULL
                          AND COALESCE(NULLIF(sc.chatter_login, ''), sc.chatter_id) IS NOT NULL
                        GROUP BY user_id, chatter_login
                    ),
                    rollup AS (
                        SELECT LOWER(streamer_login) AS streamer_login, LOWER(chatter_login) AS chatter_login
                        FROM twitch_chatter_rollup
                        WHERE LOWER(streamer_login) = ?
                    )
                    SELECT
                        pu.user_id,
                        pu.chatter_login,
                        pu.session_count,
                        pu.active_flag,
                        pu.lurker_flag,
                        pu.first_time_flag,
                        pu.has_first_flag,
                        CASE WHEN r.chatter_login IS NOT NULL THEN 1 ELSE 0 END AS seen_before
                    FROM per_user pu
                    LEFT JOIN rollup r ON r.chatter_login = LOWER(pu.chatter_login)
                """,
                    [since_date, streamer.lower(), streamer.lower()],
                ).fetchall()

                viewer_entries = []
                has_first_flag_data = False
                for row in viewer_rows:
                    entry = {
                        "user_id": row[0],
                        "chatter_login": row[1],
                        "session_count": int(row[2] or 0),
                        "active": bool(row[3]),
                        "lurker": bool(row[4]),
                        "first_flag": bool(row[5]),
                        "has_first_flag": bool(row[6]),
                        "seen_before": bool(row[7]),
                    }
                    has_first_flag_data = has_first_flag_data or entry["has_first_flag"]
                    viewer_entries.append(entry)

                total_viewers = len(viewer_entries)
                loyalty_returning = sum(1 for v in viewer_entries if v["session_count"] >= 2)

                first_time_viewers = 0
                returning_viewers = 0
                dedicated = 0
                casual = 0
                new_viewers = 0

                for v in viewer_entries:
                    if has_first_flag_data:
                        is_first = v["first_flag"]
                    else:
                        # Fallback: treat as returning if we have historical rollup for this login
                        is_first = not v["seen_before"] if v["chatter_login"] else True

                    if is_first:
                        first_time_viewers += 1
                        if v["active"]:
                            casual += 1
                        else:
                            new_viewers += 1
                    else:
                        returning_viewers += 1
                        if v["active"]:
                            dedicated += 1

                regular = max(0, returning_viewers - dedicated)

                def _pct(part: int, whole: int) -> float:
                    return round((part / whole) * 100, 1) if whole > 0 else 0.0

                viewer_type = [
                    {"label": "Dedicated Fans", "percentage": _pct(dedicated, total_viewers)},
                    {"label": "Regular Viewers", "percentage": _pct(regular, total_viewers)},
                    {"label": "Casual Viewers", "percentage": _pct(casual, total_viewers)},
                    {"label": "New Visitors", "percentage": _pct(new_viewers, total_viewers)},
                ]

                loyalty_score = _pct(loyalty_returning, total_viewers)

                # Activity pattern based on stream schedule
                schedule_stats = conn.execute(
                    """
                    SELECT
                        CAST(strftime('%w', s.started_at) AS INTEGER) as weekday,
                        COUNT(*) as count
                    FROM twitch_stream_sessions s
                    WHERE s.started_at >= ? AND LOWER(s.streamer_login) = ? AND s.ended_at IS NOT NULL
                    GROUP BY weekday
                """,
                    [since_date, streamer.lower()],
                ).fetchall()

                weekday_counts = {r[0]: r[1] for r in schedule_stats}
                weekend_streams = weekday_counts.get(0, 0) + weekday_counts.get(6, 0)
                weekday_streams = sum(weekday_counts.get(i, 0) for i in range(1, 6))

                if weekend_streams > weekday_streams:
                    activity_pattern = "weekend-heavy"
                elif weekday_streams > weekend_streams * 2:
                    activity_pattern = "weekday-focused"
                else:
                    activity_pattern = "balanced"

                schedule_session_total = int(sum(weekday_counts.values()))
                session_samples = max(language_session_total, schedule_session_total)
                if session_samples >= 25:
                    confidence = "high"
                elif session_samples >= 12:
                    confidence = "medium"
                elif session_samples >= 6:
                    confidence = "low"
                else:
                    confidence = "very_low"

                return web.json_response(
                    {
                        "estimatedRegions": regions,
                        "viewerTypes": viewer_type,
                        "activityPattern": activity_pattern,
                        "primaryLanguage": primary_language_label,
                        "languageConfidence": language_confidence,
                        "peakActivityHours": peak_hours,
                        "interactiveRate": round(chat_rate * 100, 1),
                        "loyaltyScore": loyalty_score,
                        "dataQuality": {
                            "confidence": confidence,
                            "sessions": session_samples,
                            "method": "heuristic_from_language_and_schedule",
                        },
                    }
                )
        except Exception as exc:
            log.exception("Error in audience demographics API")
            return web.json_response({"error": str(exc)}, status=500)

