"""
Analytics API v2 - Raids Mixin.

Raid analytics: per-source-channel performance, retention curves, follow attribution.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from aiohttp import web

from ..storage import pg as storage

log = logging.getLogger("TwitchStreams.AnalyticsV2")


class _AnalyticsRaidsMixin:
    """Mixin providing raid analytics endpoints."""

    async def _api_v2_raid_analytics(self, request: web.Request) -> web.Response:
        """Raid analytics: per-source performance, retention curves, follow attribution."""
        self._require_v2_auth(request)

        streamer = request.query.get("streamer", "").strip().lower()
        days = min(max(int(request.query.get("days", "30")), 7), 365)

        if not streamer:
            return web.json_response({"error": "Streamer required"}, status=400)

        cutoff = (datetime.now(UTC) - timedelta(days=days)).isoformat()

        try:
            with storage.get_conn() as c:
                # --- 1. Per-source-channel performance ---
                # Joins twitch_raid_retention + twitch_raid_history to get
                # per-source stats: avg viewers sent, new chatters, retention at 30m, etc.
                source_rows = c.execute(
                    """
                    SELECT
                        rh.from_broadcaster_login,
                        COUNT(*) AS raids_received,
                        AVG(rh.viewer_count) AS avg_viewers_sent,
                        AVG(rr.new_chatters) AS avg_new_chatters,
                        AVG(CASE WHEN rr.viewer_count_sent > 0
                            THEN CAST(rr.chatters_at_plus30m AS REAL) / rr.viewer_count_sent
                            ELSE NULL END) AS avg_retention_30m,
                        AVG(CASE WHEN rr.viewer_count_sent > 0
                            THEN CAST(rr.known_from_raider AS REAL) / rr.viewer_count_sent
                            ELSE NULL END) AS known_audience_overlap
                    FROM twitch_raid_retention rr
                    JOIN twitch_raid_history rh ON rh.id = rr.raid_id
                    JOIN twitch_stream_sessions ss ON ss.id = rr.target_session_id
                    WHERE LOWER(ss.streamer_login) = ?
                      AND ss.started_at >= ?
                    GROUP BY rh.from_broadcaster_login
                    ORDER BY raids_received DESC
                    LIMIT 20
                    """,
                    [streamer, cutoff],
                ).fetchall()

                # --- 2. Follow attribution (precise, no heuristic time window) ---
                # A follow is attributed to a raid ONLY if:
                #   - follower appeared in the session AFTER the raid timestamp
                #   - follower was NOT known before this session (first_seen_at >= session start)
                follow_rows = c.execute(
                    """
                    SELECT
                        fe.follower_login,
                        CASE
                            WHEN rh.executed_at IS NOT NULL
                             AND sc.first_message_at >= rh.executed_at
                             AND cr_before.chatter_login IS NULL
                                THEN 'raid'
                            ELSE 'organic'
                        END AS follow_source,
                        rh.from_broadcaster_login AS raid_source
                    FROM twitch_follow_events fe
                    JOIN twitch_stream_sessions ss
                        ON LOWER(ss.streamer_login) = LOWER(fe.streamer_login)
                       AND fe.followed_at BETWEEN ss.started_at AND COALESCE(ss.ended_at, datetime('now'))
                    LEFT JOIN twitch_session_chatters sc
                        ON sc.session_id = ss.id
                       AND LOWER(sc.chatter_login) = LOWER(fe.follower_login)
                    LEFT JOIN twitch_raid_retention rr ON rr.target_session_id = ss.id
                    LEFT JOIN twitch_raid_history rh ON rh.id = rr.raid_id
                    LEFT JOIN twitch_chatter_rollup cr_before
                        ON LOWER(cr_before.chatter_login) = LOWER(fe.follower_login)
                       AND LOWER(cr_before.streamer_login) = LOWER(fe.streamer_login)
                       AND cr_before.first_seen_at < ss.started_at
                    WHERE LOWER(fe.streamer_login) = ?
                      AND fe.followed_at >= ?
                    """,
                    [streamer, cutoff],
                ).fetchall()

                # --- 3. Raid retention curves ---
                curve_rows = c.execute(
                    """
                    SELECT
                        rr.raid_id,
                        rh.from_broadcaster_login,
                        rr.viewer_count_sent,
                        rr.new_chatters,
                        rr.chatters_at_plus5m,
                        rr.chatters_at_plus15m,
                        rr.chatters_at_plus30m,
                        ss.started_at AS session_start
                    FROM twitch_raid_retention rr
                    JOIN twitch_raid_history rh ON rh.id = rr.raid_id
                    JOIN twitch_stream_sessions ss ON ss.id = rr.target_session_id
                    WHERE LOWER(ss.streamer_login) = ?
                      AND ss.started_at >= ?
                    ORDER BY ss.started_at DESC
                    LIMIT 50
                    """,
                    [streamer, cutoff],
                ).fetchall()

                # --- Build response ---

                # Per-source summary
                per_source = []
                for r in source_rows:
                    # Count follows attributed to this source
                    src_login = (r["from_broadcaster_login"] or "").lower()
                    follows_attributed = sum(
                        1 for f in follow_rows
                        if f["follow_source"] == "raid"
                        and (f["raid_source"] or "").lower() == src_login
                    )
                    avg_viewers = float(r["avg_viewers_sent"] or 0)
                    per_source.append({
                        "from_channel": r["from_broadcaster_login"] or "unknown",
                        "raids_received": int(r["raids_received"] or 0),
                        "avg_viewers_sent": round(avg_viewers, 1),
                        "avg_new_chatters": round(float(r["avg_new_chatters"] or 0), 1),
                        "avg_retention_30m": round(float(r["avg_retention_30m"] or 0), 3) if r["avg_retention_30m"] is not None else None,
                        "follows_attributed": follows_attributed,
                        "conversion_rate": round(follows_attributed / avg_viewers, 3) if avg_viewers > 0 else None,
                        "known_audience_overlap": round(float(r["known_audience_overlap"] or 0), 3) if r["known_audience_overlap"] is not None else None,
                    })

                # Follow attribution summary
                raid_follows = sum(1 for f in follow_rows if f["follow_source"] == "raid")
                organic_follows = sum(1 for f in follow_rows if f["follow_source"] == "organic")
                total_follows = len(follow_rows)

                follow_attribution = {
                    "total_follows": total_follows,
                    "raid_follows": raid_follows,
                    "organic_follows": organic_follows,
                    "raid_conversion_rate": round(raid_follows / total_follows, 3) if total_follows > 0 else None,
                } if total_follows > 0 else None

                # Retention curves per raid
                retention_curves = []
                for r in curve_rows:
                    sent = int(r["viewer_count_sent"] or 0)
                    if sent <= 0:
                        continue
                    retention_curves.append({
                        "raid_id": int(r["raid_id"]),
                        "from": r["from_broadcaster_login"] or "unknown",
                        "viewers_sent": sent,
                        "new_chatters": int(r["new_chatters"] or 0),
                        "retention_curve": {
                            "plus5m": round(int(r["chatters_at_plus5m"] or 0) / sent, 3),
                            "plus15m": round(int(r["chatters_at_plus15m"] or 0) / sent, 3),
                            "plus30m": round(int(r["chatters_at_plus30m"] or 0) / sent, 3),
                        },
                    })

                return web.json_response({
                    "per_source": per_source,
                    "follow_attribution": follow_attribution,
                    "retention_curves": retention_curves,
                    "window_days": days,
                })

        except Exception as exc:
            log.exception("Error in raid analytics API")
            return web.json_response({"error": str(exc)}, status=500)
