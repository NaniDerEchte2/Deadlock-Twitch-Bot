"""
Analytics API v2 – KI Analyse Mixin.

Admin-only endpoint: Collects all analytics data and feeds it to Claude Opus
for a deep 10-point improvement analysis.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime, timedelta

from aiohttp import web

from ..storage import pg as storage

log = logging.getLogger("TwitchStreams.AnalyticsV2.AI")

_anthropic_client = None
_DOW_NAMES = ["So", "Mo", "Di", "Mi", "Do", "Fr", "Sa"]


def _get_async_client():
    """Lazy-initialize AsyncAnthropic client with API key from keyring or env."""
    global _anthropic_client
    if _anthropic_client is not None:
        return _anthropic_client

    try:
        import anthropic as _lib
    except ImportError as exc:
        raise RuntimeError(
            "anthropic package not installed. Run: pip install anthropic"
        ) from exc

    api_key = ""
    try:
        import keyring
        api_key = keyring.get_password("DeadlockBot", "ANTHROPIC_API_KEY") or ""
    except Exception:
        pass

    if not api_key:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")

    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY nicht gefunden. Setze via keyring "
            "(service=DeadlockBot, key=ANTHROPIC_API_KEY) oder als Umgebungsvariable."
        )

    _anthropic_client = _lib.AsyncAnthropic(api_key=api_key, timeout=120.0)
    return _anthropic_client


class _AnalyticsAIMixin:
    """Admin-only mixin: deep stream analytics via Claude Opus."""

    # ------------------------------------------------------------------
    #  GET /twitch/api/v2/ai/analysis
    #  Parameter: streamer (required), days (optional, default 30)
    #  Auth: admin or localhost only
    # ------------------------------------------------------------------

    async def _api_v2_ai_analysis(self, request: web.Request) -> web.Response:
        """Deep stream analytics analysis via Claude Opus (admin only)."""
        err = self._require_v2_admin_api(request)
        if err is not None:
            return err

        streamer = request.query.get("streamer", "").strip().lower() or None
        if not streamer:
            return web.json_response({"error": "streamer parameter required"}, status=400)

        try:
            days = min(max(int(request.query.get("days", "30")), 7), 365)
        except (ValueError, TypeError):
            days = 30

        since = (datetime.now(UTC) - timedelta(days=days)).isoformat()

        # Step 1: collect analytics context from DB
        try:
            ctx = self._collect_ai_context(streamer, days, since)
        except Exception as exc:
            log.exception("Error collecting AI context for %s", streamer)
            return web.json_response(
                {"error": f"Datensammlung fehlgeschlagen: {exc}"}, status=500
            )

        # Step 2: call Claude Opus
        try:
            points = await self._call_claude_opus(streamer, days, ctx)
        except RuntimeError as exc:
            return web.json_response({"error": str(exc)}, status=503)
        except Exception as exc:
            log.exception("Error calling Claude Opus for %s", streamer)
            return web.json_response(
                {"error": f"KI-Analyse fehlgeschlagen: {exc}"}, status=500
            )

        return web.json_response({
            "streamer": streamer,
            "days": days,
            "generatedAt": datetime.now(UTC).isoformat(),
            "points": points,
            "dataSnapshot": ctx.get("summary", {}),
        })

    def _collect_ai_context(self, streamer: str, days: int, since: str) -> dict:
        """Collect comprehensive analytics data for Opus context."""
        with storage.get_conn() as conn:
            # 1. Overview KPIs
            ov = conn.execute(
                """
                SELECT
                    COUNT(*) AS stream_count,
                    ROUND(SUM(duration_seconds) / 3600.0, 1) AS total_hours,
                    ROUND(AVG(avg_viewers), 1) AS avg_viewers,
                    MAX(peak_viewers) AS peak_viewers,
                    COALESCE(SUM(
                        CASE WHEN follower_delta > 0 THEN follower_delta ELSE 0 END
                    ), 0) AS followers_gained,
                    ROUND(AVG(retention_10m) * 100, 1) AS avg_retention_10m,
                    ROUND(AVG(dropoff_pct) * 100, 1) AS avg_dropoff_pct,
                    ROUND(AVG(COALESCE(unique_chatters, 0)), 0) AS avg_chatters
                FROM twitch_stream_sessions
                WHERE LOWER(streamer_login) = %s
                  AND started_at >= %s
                  AND ended_at IS NOT NULL
                """,
                (streamer, since),
            ).fetchone()

            # 2. Recent sessions (last 20, newest first)
            sessions_rows = conn.execute(
                """
                SELECT
                    started_at::date,
                    stream_title,
                    ROUND(duration_seconds / 3600.0, 2) AS hours,
                    ROUND(avg_viewers, 1),
                    peak_viewers,
                    ROUND(retention_10m * 100, 1),
                    ROUND(dropoff_pct * 100, 1),
                    COALESCE(unique_chatters, 0),
                    COALESCE(follower_delta, 0)
                FROM twitch_stream_sessions
                WHERE LOWER(streamer_login) = %s
                  AND started_at >= %s
                  AND ended_at IS NOT NULL
                ORDER BY started_at DESC
                LIMIT 20
                """,
                (streamer, since),
            ).fetchall()

            # 3. Weekday performance (sorted by avg viewers)
            weekday_rows = conn.execute(
                """
                SELECT
                    EXTRACT(DOW FROM started_at)::int AS dow,
                    COUNT(*) AS streams,
                    ROUND(AVG(avg_viewers), 1),
                    ROUND(AVG(peak_viewers), 1)
                FROM twitch_stream_sessions
                WHERE LOWER(streamer_login) = %s
                  AND started_at >= %s
                  AND ended_at IS NOT NULL
                GROUP BY dow
                ORDER BY AVG(avg_viewers) DESC
                """,
                (streamer, since),
            ).fetchall()

            # 4. Best 5 sessions by avg viewers
            best_rows = conn.execute(
                """
                SELECT
                    COALESCE(stream_title, ''), avg_viewers, peak_viewers,
                    ROUND(retention_10m * 100, 1), started_at::date
                FROM twitch_stream_sessions
                WHERE LOWER(streamer_login) = %s
                  AND started_at >= %s
                  AND ended_at IS NOT NULL
                ORDER BY avg_viewers DESC NULLS LAST
                LIMIT 5
                """,
                (streamer, since),
            ).fetchall()

            # 5. Worst 5 sessions by avg viewers
            worst_rows = conn.execute(
                """
                SELECT
                    COALESCE(stream_title, ''), avg_viewers, peak_viewers,
                    ROUND(retention_10m * 100, 1), started_at::date
                FROM twitch_stream_sessions
                WHERE LOWER(streamer_login) = %s
                  AND started_at >= %s
                  AND ended_at IS NOT NULL
                ORDER BY avg_viewers ASC NULLS LAST
                LIMIT 5
                """,
                (streamer, since),
            ).fetchall()

            # 6. Game/category breakdown from exp_sessions (best-effort)
            game_rows = []
            try:
                game_rows = conn.execute(
                    """
                    SELECT
                        COALESCE(game_name, 'Unbekannt') AS game,
                        COUNT(*) AS sessions,
                        ROUND(AVG(avg_viewers), 1),
                        MAX(peak_viewers),
                        ROUND(AVG(duration_min), 1)
                    FROM exp_sessions
                    WHERE LOWER(streamer) = %s
                      AND started_at >= %s
                      AND ended_at IS NOT NULL
                    GROUP BY game_name
                    ORDER BY AVG(avg_viewers) DESC
                    LIMIT 10
                    """,
                    (streamer, since),
                ).fetchall()
            except Exception:
                pass  # exp_sessions might not be populated yet

            # 7. Weekly follower trend
            trend_rows = conn.execute(
                """
                SELECT
                    DATE_TRUNC('week', started_at)::date AS week_start,
                    COUNT(*) AS streams,
                    SUM(CASE WHEN follower_delta > 0 THEN follower_delta ELSE 0 END)
                FROM twitch_stream_sessions
                WHERE LOWER(streamer_login) = %s
                  AND started_at >= %s
                  AND ended_at IS NOT NULL
                GROUP BY week_start
                ORDER BY week_start
                """,
                (streamer, since),
            ).fetchall()

        def _s(v, default=0):
            return v if v is not None else default

        return {
            "summary": {
                "streamCount": int(_s(ov[0])),
                "totalHours": float(_s(ov[1])),
                "avgViewers": float(_s(ov[2])),
                "peakViewers": int(_s(ov[3])),
                "followersGained": int(_s(ov[4])),
                "avgRetention10m": float(_s(ov[5])),
                "avgDropoffPct": float(_s(ov[6])),
                "avgChatters": int(_s(ov[7])),
            },
            "recentSessions": [
                {
                    "date": str(r[0]),
                    "title": (str(r[1]) if r[1] else "")[:60],
                    "hours": float(_s(r[2])),
                    "avgViewers": float(_s(r[3])),
                    "peakViewers": int(_s(r[4])),
                    "retention10m": float(_s(r[5])),
                    "dropoffPct": float(_s(r[6])),
                    "chatters": int(_s(r[7])),
                    "followerDelta": int(_s(r[8])),
                }
                for r in sessions_rows
            ],
            "weekdayPerformance": [
                {
                    "day": _DOW_NAMES[int(w[0])] if 0 <= int(w[0]) <= 6 else "?",
                    "streams": int(w[1]),
                    "avgViewers": float(_s(w[2])),
                    "avgPeak": float(_s(w[3])),
                }
                for w in weekday_rows
            ],
            "bestSessions": [
                {
                    "title": (str(s[0]) if s[0] else "")[:60],
                    "avgViewers": float(_s(s[1])),
                    "peakViewers": int(_s(s[2])),
                    "retention10m": float(_s(s[3])),
                    "date": str(s[4]),
                }
                for s in best_rows
            ],
            "worstSessions": [
                {
                    "title": (str(s[0]) if s[0] else "")[:60],
                    "avgViewers": float(_s(s[1])),
                    "peakViewers": int(_s(s[2])),
                    "retention10m": float(_s(s[3])),
                    "date": str(s[4]),
                }
                for s in worst_rows
            ],
            "gamePerformance": [
                {
                    "game": str(g[0]),
                    "sessions": int(g[1]),
                    "avgViewers": float(_s(g[2])),
                    "peakViewers": int(_s(g[3])),
                    "avgDurationMin": float(_s(g[4])),
                }
                for g in game_rows
            ],
            "weeklyTrend": [
                {
                    "week": str(t[0]),
                    "streams": int(t[1]),
                    "followersGained": int(_s(t[2])),
                }
                for t in trend_rows
            ],
        }

    async def _call_claude_opus(
        self, streamer: str, days: int, ctx: dict
    ) -> list[dict]:
        """Send analytics context to Claude Opus, return structured 10-point analysis."""
        s = ctx["summary"]

        game_section = ctx.get("gamePerformance", [])
        if not game_section:
            game_section = [{"note": "Keine Kategorie-Daten vorhanden (exp_sessions leer)"}]

        prompt = f"""Du bist ein Experte für Twitch-Streaming-Analytik und Wachstumsstrategie.

Analysiere die Streaming-Daten des Kanals **{streamer}** (letzte {days} Tage) und erstelle einen TIEFEN, DATEN-BASIERTEN 10-Punkte-Verbesserungsplan.

REGELN:
- Referenziere IMMER konkrete Zahlen aus den Daten
- Keine generischen Ratschläge
- Erkläre das WARUM hinter jedem Pattern
- Priorisiere nach maximalem Impact (#1 = wichtigster Hebel)
- Zeige sowohl Chancen als auch Risiken auf

=== KPI ÜBERSICHT ===
Streams: {s['streamCount']} | Gesamtzeit: {s['totalHours']}h
Ø Viewer: {s['avgViewers']} | Peak: {s['peakViewers']}
Follower gewonnen: +{s['followersGained']}
Ø 10-Min-Retention: {s['avgRetention10m']}% | Ø Dropoff: {s['avgDropoffPct']}%
Ø Aktive Chatter: {s['avgChatters']}

=== TOP 5 STREAMS (Ø Viewer) ===
{json.dumps(ctx.get('bestSessions', []), ensure_ascii=False)}

=== SCHWÄCHSTE 5 STREAMS ===
{json.dumps(ctx.get('worstSessions', []), ensure_ascii=False)}

=== LETZTE 20 SESSIONS ===
{json.dumps(ctx.get('recentSessions', []), ensure_ascii=False)}

=== WOCHENTAG-PERFORMANCE ===
{json.dumps(ctx.get('weekdayPerformance', []), ensure_ascii=False)}

=== KATEGORIEN-PERFORMANCE ===
{json.dumps(game_section, ensure_ascii=False)}

=== WÖCHENTLICHER FOLLOWER-TREND ===
{json.dumps(ctx.get('weeklyTrend', []), ensure_ascii=False)}

---
Antworte NUR als JSON Array mit exakt 10 Objekten. Kein Markdown, kein Text außerhalb des JSON.

[
  {{
    "number": 1,
    "priority": "kritisch",
    "title": "Titel (max 8 Wörter)",
    "analysis": "Tiefenanalyse 3-5 Sätze mit konkreten Zahlen aus den Daten.",
    "action": "Konkrete Handlungsempfehlung: Was, wann, wie oft, wie messen.",
    "expectedImpact": "Realistischer erwarteter Effekt basierend auf den Daten."
  }}
]

Gültige priority-Werte: "kritisch", "hoch", "mittel"
Punkte 1-3: kritisch | Punkte 4-7: hoch | Punkte 8-10: mittel"""

        client = _get_async_client()
        msg = await client.messages.create(
            model="claude-opus-4-6",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )

        raw = (msg.content[0].text or "").strip()

        # Strip markdown code fences if present
        if raw.startswith("```"):
            lines = raw.splitlines()
            lines = [ln for ln in lines if not ln.strip().startswith("```")]
            raw = "\n".join(lines).strip()

        try:
            points = json.loads(raw)
            if isinstance(points, list):
                return points
        except json.JSONDecodeError:
            log.warning("Claude returned non-JSON response (first 300 chars): %s", raw[:300])
            return [
                {
                    "number": 1,
                    "priority": "hoch",
                    "title": "Rohe Analyse",
                    "analysis": raw,
                    "action": "",
                    "expectedImpact": "",
                }
            ]

        return []
