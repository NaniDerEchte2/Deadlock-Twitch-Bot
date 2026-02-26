"""Routes mixin for DashboardV2Server — core routes and route registration."""

from __future__ import annotations

import html
import json
from typing import Any

from aiohttp import web

from .. import storage
from ..core.constants import log
from .live import DashboardLiveMixin, _REQUIRED_SCOPES, _CRITICAL_SCOPES, _SCOPE_COLUMN_LABELS

TWITCH_DASHBOARDS_LOGIN_URL = "/twitch/auth/login?next=%2Ftwitch%2Fdashboards"
TWITCH_DASHBOARD_V2_LOGIN_URL = "/twitch/auth/login?next=%2Ftwitch%2Fdashboard-v2"
TWITCH_DASHBOARDS_DISCORD_LOGIN_URL = "/twitch/auth/discord/login?next=%2Ftwitch%2Fdashboards"


class _DashboardRoutesMixin:
    """Core dashboard routes and route table registration."""

    # ------------------------------------------------------------------ #
    # Core routes                                                          #
    # ------------------------------------------------------------------ #

    async def index(self, request: web.Request) -> web.StreamResponse:
        """Entrypoint with local-first admin behavior.

        Local requests should land directly in the legacy stats/admin UI.
        Public/proxied requests keep the dashboard selection page.
        """
        if self._is_local_request(request) or self._is_discord_admin_request(request):
            destination = "/twitch/admin"
            fallback = "/twitch/admin"
        else:
            destination = "/twitch/dashboards"
            fallback = "/twitch/dashboards"
        if request.query_string:
            destination = f"{destination}?{request.query_string}"
        safe_destination = self._safe_internal_redirect(destination, fallback=fallback)
        raise web.HTTPFound(safe_destination)

    async def public_home(self, request: web.Request) -> web.StreamResponse:
        """Public homepage for OAuth verification and app information."""
        dashboard_url = (
            "/twitch/dashboards"
            if self._check_v2_auth(request)
            else TWITCH_DASHBOARDS_DISCORD_LOGIN_URL
            if self._should_use_discord_admin_login(request)
            else TWITCH_DASHBOARDS_LOGIN_URL
        )
        dashboard_label = (
            "Dashboard oeffnen"
            if self._check_v2_auth(request)
            else "Mit Discord anmelden"
            if self._should_use_discord_admin_login(request)
            else "Mit Twitch anmelden"
        )
        safe_dashboard_url = html.escape(
            self._safe_internal_redirect(dashboard_url, fallback="/twitch/dashboards"),
            quote=True,
        )
        safe_dashboard_label = html.escape(dashboard_label, quote=True)

        page = (
            "<!doctype html><html lang='de'><head><meta charset='utf-8'>"
            "<meta name='viewport' content='width=device-width,initial-scale=1'>"
            "<title>Deutsche Deadlock Community</title>"
            "<style>"
            ":root{color-scheme:light;}"
            "body{margin:0;background:#f8fafc;color:#0f172a;font-family:Segoe UI,Arial,sans-serif;line-height:1.55;}"
            ".wrap{max-width:980px;margin:0 auto;padding:30px 18px 44px;}"
            ".top{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;}"
            "h1{margin:0;font-size:1.85rem;}"
            ".tag{display:inline-block;margin-top:10px;padding:5px 10px;border-radius:999px;background:#dbeafe;color:#1e3a8a;font-weight:600;font-size:.85rem;}"
            ".panel{margin-top:18px;background:#ffffff;border:1px solid #dbe2ea;border-radius:14px;padding:18px;}"
            ".muted{color:#334155;}"
            ".actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:14px;}"
            ".btn{display:inline-block;padding:10px 14px;border-radius:10px;text-decoration:none;font-weight:600;}"
            ".btn-primary{background:#2563eb;color:#fff;}"
            ".btn-secondary{border:1px solid #cbd5e1;color:#0f172a;background:#fff;}"
            "footer{margin-top:22px;padding-top:12px;border-top:1px solid #e2e8f0;color:#475569;font-size:.92rem;}"
            "a{color:#1d4ed8;}"
            "</style></head><body><main class='wrap'>"
            "<div class='top'>"
            "<h1>Deutsche Deadlock Community</h1>"
            "<a href='/privacy'>Datenschutzerklaerung</a>"
            "</div>"
            "<div class='tag'>Offizielle App-Startseite</div>"
            "<section class='panel'>"
            "<h2 style='margin-top:0;'>Wozu dient diese App?</h2>"
            "<p class='muted'>"
            "Diese App wird von der <strong>Deutsche Deadlock Community</strong> betrieben und unterstuetzt "
            "verifizierte Community-Streamer bei Twitch-Funktionen: Analytics-Dashboard, Raid-Autorisierung "
            "und Clip-Management inklusive Social-Media-Veroeffentlichung."
            "</p>"
            "<p class='muted'>"
            "Die App ist ein Community-Tool fuer Streamer-Partner. Allgemeine Informationen (inklusive "
            "Datenschutz und Nutzungsbedingungen) sind ohne Anmeldung aufrufbar."
            "</p>"
            "<div class='actions'>"
            f"<a class='btn btn-primary' href='{safe_dashboard_url}'>{safe_dashboard_label}</a>"
            "<a class='btn btn-secondary' href='/terms'>Nutzungsbedingungen</a>"
            "<a class='btn btn-secondary' href='/privacy'>Datenschutzerklaerung</a>"
            "</div>"
            "</section>"
            "<footer>"
            "App-Name im OAuth-Zustimmungsbildschirm: <strong>Deutsche Deadlock Community</strong>"
            "</footer>"
            "</main></body></html>"
        )
        return web.Response(text=page, content_type="text/html", charset="utf-8")

    async def admin(self, request: web.Request) -> web.StreamResponse:
        """Legacy partner admin surface (streamer management)."""
        return await DashboardLiveMixin.index(self, request)

    async def stats_entry(self, request: web.Request) -> web.StreamResponse:
        """Canonical public entrypoint that links old + beta analytics dashboards."""
        if not self._check_v2_auth(request):
            login_url = (
                TWITCH_DASHBOARDS_DISCORD_LOGIN_URL
                if self._should_use_discord_admin_login(request)
                else TWITCH_DASHBOARDS_LOGIN_URL
            )
            raise web.HTTPFound(login_url)

        legacy_url = self._resolve_legacy_stats_url()
        beta_url = "/twitch/dashboard-v2"
        logout_url = (
            "/twitch/auth/discord/logout"
            if self._is_discord_admin_request(request)
            else "/twitch/auth/logout"
        )

        # Look up scopes for the logged-in user
        session = self._get_dashboard_auth_session(request)
        twitch_login = (session or {}).get("twitch_login", "")
        missing_scopes: list[str] = []
        missing_critical: list[str] = []
        if twitch_login:
            try:
                with storage.get_conn() as conn:
                    row = conn.execute(
                        "SELECT scopes FROM twitch_raid_auth WHERE LOWER(twitch_login) = LOWER(?)",
                        [twitch_login],
                    ).fetchone()
                if row:
                    token_scopes = set((row[0] or "").split())
                    missing_scopes = [s for s in _REQUIRED_SCOPES if s not in token_scopes]
                    missing_critical = [s for s in missing_scopes if s in _CRITICAL_SCOPES]
                else:
                    missing_scopes = list(_REQUIRED_SCOPES)
                    missing_critical = [s for s in _REQUIRED_SCOPES if s in _CRITICAL_SCOPES]
            except Exception:
                log.exception("stats_entry: failed to load scopes for %s", twitch_login)

        # Build scope status HTML block
        if twitch_login and missing_scopes:
            scope_items = "".join(
                f"<li style='margin-bottom:4px;'>"
                f"<span style='color:{'#f87171' if s in _CRITICAL_SCOPES else '#fbbf24'};margin-right:6px;'>"
                f"{'⚠' if s in _CRITICAL_SCOPES else '○'}</span>"
                f"<code style='font-size:12px;background:#1f2937;padding:1px 5px;border-radius:4px;'>{html.escape(s)}</code>"
                f"<span style='color:#94a3b8;font-size:12px;margin-left:6px;'>{html.escape(_SCOPE_COLUMN_LABELS.get(s, ''))}</span>"
                f"</li>"
                for s in missing_scopes
            )
            critical_note = (
                f"<p style='color:#f87171;font-size:13px;margin-top:8px;'>"
                f"⚠ {len(missing_critical)} kritische Scope(s) fehlen — einige Features sind deaktiviert.</p>"
                if missing_critical else ""
            )
            scope_panel = (
                "<div style='background:#111827;border:1px solid #7f1d1d;border-radius:12px;"
                "padding:18px;margin-bottom:20px;'>"
                "<h3 style='margin:0 0 10px;color:#fca5a5;font-size:15px;'>Fehlende OAuth-Scopes</h3>"
                f"<p style='color:#94a3b8;font-size:13px;margin:0 0 10px;'>"
                f"Für <strong style='color:#e2e8f0;'>{html.escape(twitch_login)}</strong> fehlen "
                f"{len(missing_scopes)} von {len(_REQUIRED_SCOPES)} Scopes. "
                f"Bitte neu authentifizieren.</p>"
                f"<ul style='list-style:none;margin:0;padding:0;'>{scope_items}</ul>"
                f"{critical_note}"
                "</div>"
            )
        elif twitch_login:
            scope_panel = (
                "<div style='background:#111827;border:1px solid #14532d;border-radius:12px;"
                "padding:14px 18px;margin-bottom:20px;display:flex;align-items:center;gap:10px;'>"
                "<span style='color:#4ade80;font-size:18px;'>✓</span>"
                "<span style='color:#86efac;font-size:14px;'>Alle OAuth-Scopes vorhanden</span>"
                "</div>"
            )
        else:
            scope_panel = ""

        page_html = (
            "<!doctype html><html lang='de'><head><meta charset='utf-8'>"
            "<meta name='viewport' content='width=device-width,initial-scale=1'>"
            "<title>Twitch Stats Dashboard</title>"
            "<style>"
            "body{font-family:Segoe UI,Arial,sans-serif;background:#0f172a;color:#e2e8f0;margin:0;}"
            ".wrap{max-width:980px;margin:0 auto;padding:32px 18px;}"
            ".cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:16px;}"
            ".card{background:#111827;border:1px solid #1f2937;border-radius:12px;padding:18px;}"
            ".btn{display:inline-block;margin-top:10px;padding:10px 14px;border-radius:8px;text-decoration:none;"
            "background:#2563eb;color:#fff;font-weight:600;}"
            ".muted{color:#94a3b8;font-size:14px;}"
            ".top{display:flex;justify-content:space-between;align-items:center;margin-bottom:18px;gap:10px;}"
            "a.logout{color:#93c5fd;text-decoration:none;font-size:14px;}"
            "</style></head><body><div class='wrap'>"
            "<div class='top'><h1 style='margin:0;'>Twitch Dashboard Zugang</h1>"
            f"<a class='logout' href='{logout_url}'>Logout</a></div>"
            "<p class='muted'>Beta ist jetzt für verifizierte Streamer-Partner freigeschaltet.</p>"
            f"{scope_panel}"
            "<div class='cards'>"
            "<div class='card'><h2 style='margin-top:0;'>Stats Dashboard (Alt)</h2>"
            "<p class='muted'>Bestehendes Dashboard für die bisherigen Stats-Ansichten.</p>"
            f"<a class='btn' href='{legacy_url}'>Altes Dashboard öffnen</a></div>"
            "<div class='card'><h2 style='margin-top:0;'>Analyse Dashboard (Beta)</h2>"
            "<p class='muted'>Neues v2 Analytics Dashboard mit erweiterten Insights.</p>"
            f"<a class='btn' href='{beta_url}'>Beta Dashboard öffnen</a></div>"
            "<div class='card'><h2 style='margin-top:0;'>📱 Social Media Publisher</h2>"
            "<p class='muted'>Verwalte Twitch-Clips und veröffentliche auf TikTok, YouTube & Instagram</p>"
            "<a class='btn' href='/social-media'>Social Media Dashboard öffnen</a></div>"
            "</div></div></body></html>"
        )
        return web.Response(text=page_html, content_type="text/html")

    async def auth_logout(self, request: web.Request) -> web.StreamResponse:
        """Logout and clear dashboard session cookie."""
        session_id = (request.cookies.get(self._session_cookie_name) or "").strip()
        if session_id:
            session = self._auth_sessions.pop(session_id, None)
            twitch_login = (session or {}).get("twitch_login", "unknown") if session else "unknown"
            log.info(
                "AUDIT dashboard logout: twitch=%s peer=%s",
                self._sanitize_log_value(twitch_login),
                self._sanitize_log_value(self._peer_host(request)),
            )

        response = web.HTTPFound(TWITCH_DASHBOARD_V2_LOGIN_URL)
        self._clear_session_cookie(response, request)
        raise response

    async def discord_link(self, request: web.Request) -> web.StreamResponse:
        """Persist Discord profile metadata from the stats dashboard."""
        self._require_token(request)
        if not callable(self._discord_profile):
            location = self._redirect_location(
                request, err="Discord-Link ist aktuell nicht verfügbar"
            )
            safe_location = self._safe_internal_redirect(location, fallback="/twitch/stats")
            raise web.HTTPFound(location=safe_location)

        data = await request.post()
        login = (data.get("login") or "").strip()
        discord_user_id = (data.get("discord_user_id") or "").strip()
        discord_display_name = (data.get("discord_display_name") or "").strip()
        member_raw = (data.get("member_flag") or "").strip().lower()
        mark_member = member_raw in {"1", "true", "on", "yes"}

        try:
            message = await self._discord_profile(
                login,
                discord_user_id=discord_user_id or None,
                discord_display_name=discord_display_name or None,
                mark_member=mark_member,
            )
            location = self._redirect_location(request, ok=message)
        except ValueError as exc:
            location = self._redirect_location(request, err=str(exc))
        except Exception:
            log.exception("dashboard discord_link failed")
            location = self._redirect_location(
                request, err="Discord-Daten konnten nicht gespeichert werden"
            )
        safe_location = self._safe_internal_redirect(location, fallback="/twitch/stats")
        raise web.HTTPFound(location=safe_location)

    async def market_research(self, request: web.Request) -> web.StreamResponse:
        """Serve the internal Market Research dashboard."""
        self._require_token(request)

        page_html = """
        <!DOCTYPE html>
        <html lang="de">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Deadlock Market Research (Internal)</title>
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
            <style>
                body { font-family: 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }
                .container { max-width: 1400px; margin: 0 auto; }
                h1 { color: #f8fafc; border-bottom: 1px solid #334155; padding-bottom: 10px; }
                .card { background: #1e293b; border-radius: 8px; padding: 20px; margin-bottom: 20px; border: 1px solid #334155; }
                .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
                table { width: 100%; border-collapse: collapse; margin-top: 10px; }
                th, td { text-align: left; padding: 12px; border-bottom: 1px solid #334155; }
                th { color: #94a3b8; font-weight: 600; text-transform: uppercase; font-size: 0.85rem; }
                tr:hover { background: #334155; }
                .badge { padding: 4px 8px; border-radius: 4px; font-size: 0.8rem; font-weight: 600; }
                .badge-live { background: #ef4444; color: white; }
                .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 20px; }
                .stat-box { background: #0f172a; padding: 15px; border-radius: 6px; text-align: center; border: 1px solid #334155; }
                .stat-val { font-size: 2rem; font-weight: bold; color: #38bdf8; }
                .stat-label { color: #94a3b8; font-size: 0.9rem; }
                .progress-bar { background: #334155; height: 8px; border-radius: 4px; overflow: hidden; margin-top: 5px; }
                .progress-fill { height: 100%; background: #38bdf8; }
                .sentiment-pos { color: #4ade80; }
                .sentiment-neg { color: #f87171; }
                .question-item { border-left: 4px solid #38bdf8; padding: 10px; margin-bottom: 10px; background: #0f172a; border-radius: 0 4px 4px 0; }
                .question-meta { font-size: 0.8rem; color: #94a3b8; margin-top: 5px; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Deadlock DACH Market Research 🕵️‍♂️</h1>

                <div class="stat-grid" id="kpi">
                    <!-- Loaded via JS -->
                </div>

                <div class="card">
                    <h2>📈 Market Volume (24h)</h2>
                    <div style="height: 300px; position: relative;">
                        <canvas id="marketChart"></canvas>
                    </div>
                </div>

                <div class="grid-2">
                    <div class="card">
                        <h2>🔥 Meta Snapshot (Top Mentions 1h)</h2>
                        <table id="meta-table">
                            <thead><tr><th>Term</th><th>Mentions</th><th>Trend</th></tr></thead>
                            <tbody></tbody>
                        </table>
                    </div>
                    <div class="card">
                        <h2>🌡️ Sentiment Analysis</h2>
                        <div id="sentiment-chart" style="padding: 20px; text-align: center;"></div>
                    </div>
                </div>

                <div class="grid-2">
                    <div class="card">
                        <h2>🕸️ Viewer Overlap (Shared Chatters)</h2>
                        <table id="overlap-table">
                            <thead><tr><th>Streamer A</th><th>Streamer B</th><th>Shared Users</th></tr></thead>
                            <tbody></tbody>
                        </table>
                    </div>
                    <div class="card">
                        <h2>❓ Question Radar (Latest)</h2>
                        <div id="questions" style="max-height: 400px; overflow-y: auto; padding-right: 10px;">
                            <!-- Questions go here -->
                        </div>
                    </div>
                </div>

                <div class="card">
                    <h2>Live Monitored Channels</h2>
                    <table id="channels">
                        <thead>
                            <tr>
                                <th>Streamer</th>
                                <th>Viewers</th>
                                <th>Chat Activity</th>
                                <th>Lurker %</th>
                                <th>Msg/Min</th>
                                <th>Top Topic</th>
                            </tr>
                        </thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>

            <script>
                let marketChart = null;

                async function loadData() {
                    const res = await fetch('/twitch/api/market_data');
                    const data = await res.json();

                    // KPIs
                    document.getElementById('kpi').innerHTML = `
                        <div class="stat-box">
                            <div class="stat-val">${data.total_monitored}</div>
                            <div class="stat-label">Active Monitored Channels</div>
                        </div>
                        <div class="stat-box">
                            <div class="stat-val">${data.total_viewers.toLocaleString()}</div>
                            <div class="stat-label">Total Deadlock Viewers (DACH)</div>
                        </div>
                        <div class="stat-box">
                            <div class="stat-val">${data.avg_chat_health.toFixed(1)}%</div>
                            <div class="stat-label">Avg Chat Engagement</div>
                        </div>
                        <div class="stat-box">
                            <div class="stat-val">${data.total_messages.toLocaleString()}</div>
                            <div class="stat-label">Messages Analyzed (1h)</div>
                        </div>
                        <div class="stat-box">
                            <div class="stat-val">${data.avg_lurker_ratio.toFixed(1)}%</div>
                            <div class="stat-label">Avg Lurker Ratio</div>
                        </div>
                    `;

                    // Market Chart
                    const ctx = document.getElementById('marketChart').getContext('2d');
                    const chartLabels = data.market_history.map(h => {
                        const d = new Date(h.ts + 'Z');
                        return d.getHours().toString().padStart(2, '0') + ':' + d.getMinutes().toString().padStart(2, '0');
                    });

                    const chartData = {
                        labels: chartLabels,
                        datasets: [
                            {
                                label: 'Total Viewers',
                                data: data.market_history.map(h => h.total_viewers),
                                borderColor: '#38bdf8',
                                backgroundColor: 'rgba(56, 189, 248, 0.1)',
                                fill: true,
                                tension: 0.4
                            },
                            {
                                label: 'Streamer Count',
                                data: data.market_history.map(h => h.streamer_count * 10), // Scale for visibility
                                borderColor: '#f472b6',
                                borderDash: [5, 5],
                                tension: 0.1,
                                yAxisID: 'y1'
                            }
                        ]
                    };

                    if (marketChart) {
                        marketChart.data = chartData;
                        marketChart.update();
                    } else {
                        marketChart = new Chart(ctx, {
                            type: 'line',
                            data: chartData,
                            options: {
                                responsive: true,
                                maintainAspectRatio: false,
                                scales: {
                                    y: { beginAtZero: true, grid: { color: '#334155' } },
                                    y1: { position: 'right', beginAtZero: true, grid: { display: false } },
                                    x: { grid: { display: false } }
                                },
                                plugins: { legend: { labels: { color: '#e2e8f0' } } }
                            }
                        });
                    }

                    // Questions
                    document.getElementById('questions').innerHTML = data.questions.map(q => `
                        <div class="question-item">
                            <div>${q.content}</div>
                            <div class="question-meta">in @${q.streamer} • ${q.ts.split('T')[1].substring(0, 5)} Uhr</div>
                        </div>
                    `).join('');

                    // Meta Snapshot
                    document.getElementById('meta-table').querySelector('tbody').innerHTML = data.meta_snapshot.map(m => `
                        <tr>
                            <td><strong>${m.term}</strong></td>
                            <td>${m.count}</td>
                            <td><div class="progress-bar"><div class="progress-fill" style="width: ${Math.min(100, m.count * 2)}%"></div></div></td>
                        </tr>
                    `).join('');

                    // Sentiment
                    const sent = data.sentiment;
                    document.getElementById('sentiment-chart').innerHTML = `
                        <div style="display: flex; justify-content: space-around; font-size: 1.2rem;">
                            <div class="sentiment-pos">Positiv: ${sent.positive} (${sent.pos_pct}%)</div>
                            <div style="color: #94a3b8;">Neutral: ${sent.neutral} (${sent.neu_pct}%)</div>
                            <div class="sentiment-neg">Negativ: ${sent.negative} (${sent.neg_pct}%)</div>
                        </div>
                        <div style="display: flex; height: 20px; margin-top: 15px; border-radius: 10px; overflow: hidden;">
                            <div style="width: ${sent.pos_pct}%; background: #4ade80;"></div>
                            <div style="width: ${sent.neu_pct}%; background: #94a3b8;"></div>
                            <div style="width: ${sent.neg_pct}%; background: #f87171;"></div>
                        </div>
                    `;

                    // Overlap
                    document.getElementById('overlap-table').querySelector('tbody').innerHTML = data.overlap.map(o => `
                        <tr>
                            <td>${o.a}</td>
                            <td>${o.b}</td>
                            <td>${o.shared}</td>
                        </tr>
                    `).join('');

                    // Channels Table
                    const tbody = document.querySelector('#channels tbody');
                    tbody.innerHTML = data.channels.map(c => `
                        <tr>
                            <td>
                                <strong>${c.login}</strong>
                                ${c.is_live ? '<span class="badge badge-live">LIVE</span>' : ''}
                            </td>
                            <td>${c.viewers}</td>
                            <td>${c.chat_health.toFixed(1)}%</td>
                            <td>${c.lurker_ratio.toFixed(1)}%</td>
                            <td>${c.msg_per_min.toFixed(1)}</td>
                            <td>${c.top_topic || '-'}</td>
                        </tr>
                    `).join('');
                }
                loadData();
                setInterval(loadData, 30000);
            </script>
        </body>
        </html>
        """
        return web.Response(text=page_html, content_type="text/html")

    async def api_market_data(self, request: web.Request) -> web.Response:
        """API providing aggregated data for market research including Meta & Sentiment."""
        # Simple auth check (internal/admin only)
        if not self._check_admin_token(
            request.headers.get("X-Admin-Token") or request.query.get("token")
        ) and not self._is_local_request(request):
            return web.json_response({"error": "Unauthorized"}, status=401)

        try:
            with storage.get_conn() as conn:
                def _to_iso(val: Any) -> Any:
                    """Convert datetime-like objects to ISO strings for JSON serialization."""
                    return val.isoformat() if hasattr(val, "isoformat") else val

                def _json_default(obj: Any) -> str:
                    """Fallback serializer for json.dumps to handle datetime objects safely."""
                    return obj.isoformat() if hasattr(obj, "isoformat") else str(obj)

                # 1. Active Monitored Channels
                rows = conn.execute("""
                    SELECT s.twitch_login, l.last_viewer_count
                    FROM twitch_streamers s
                    LEFT JOIN twitch_live_state l ON s.twitch_user_id = l.twitch_user_id
                    WHERE s.is_monitored_only = 1
                """).fetchall()

                channels = []
                total_viewers = 0

                for r in rows:
                    login = r[0]
                    viewers = r[1] or 0
                    total_viewers += viewers

                    # Recent chat stats
                    chat_stats = conn.execute(
                        """
                        SELECT COUNT(*), COUNT(DISTINCT chatter_login)
                        FROM twitch_chat_messages
                        WHERE streamer_login = ?
                          AND message_ts >= datetime('now', '-1 hour')
                    """,
                        [login],
                    ).fetchone()

                    msgs = chat_stats[0] or 0
                    active_chatters = chat_stats[1] or 0

                    # Lurker stats
                    session_id_row = conn.execute(
                        "SELECT active_session_id FROM twitch_live_state WHERE streamer_login = ?",
                        (login,),
                    ).fetchone()

                    lurkers = 0
                    total_connected = active_chatters
                    if session_id_row and session_id_row[0]:
                        lurker_stats = conn.execute(
                            """
                            SELECT COUNT(*), SUM(CASE WHEN messages = 0 THEN 1 ELSE 0 END)
                            FROM twitch_session_chatters WHERE session_id = ?
                        """,
                            (session_id_row[0],),
                        ).fetchone()
                        if lurker_stats:
                            total_connected = lurker_stats[0] or active_chatters
                            lurkers = lurker_stats[1] or 0

                    channels.append(
                        {
                            "login": login,
                            "viewers": viewers,
                            "is_live": viewers > 0,
                            "chat_health": min(100, (active_chatters / max(1, viewers)) * 100)
                            if viewers > 0
                            else 0,
                            "lurker_ratio": (lurkers / max(1, total_connected)) * 100,
                            "msg_per_min": msgs / 60.0,
                            "top_topic": "n/a",
                        }
                    )

                channels.sort(key=lambda x: x["viewers"], reverse=True)
                avg_health = sum(c["chat_health"] for c in channels) / max(1, len(channels))
                avg_lurker = sum(c["lurker_ratio"] for c in channels) / max(1, len(channels))

                # --- 2. Market History (24h) ---
                history_rows = conn.execute("""
                    SELECT ts_utc, SUM(viewer_count) as total_viewers, COUNT(DISTINCT streamer) as streamer_count
                    FROM twitch_stats_category
                    WHERE ts_utc >= datetime('now', '-24 hours')
                    GROUP BY ts_utc
                    ORDER BY ts_utc ASC
                """).fetchall()
                market_history = [
                    {"ts": _to_iso(r[0]), "total_viewers": r[1], "streamer_count": r[2]}
                    for r in history_rows
                ]

                # --- 3. Question Radar ---
                question_rows = conn.execute(
                    """
                    SELECT content, streamer_login, message_ts
                    FROM twitch_chat_messages
                    WHERE message_ts >= datetime('now', '-6 hours')
                      AND content LIKE ?
                      AND length(content) > 10
                    ORDER BY message_ts DESC
                    LIMIT 20
                """,
                    ("%?%",),
                ).fetchall()
                questions = [
                    {"content": r[0], "streamer": r[1], "ts": _to_iso(r[2])}
                    for r in question_rows
                ]

                # --- 4. Meta Snapshot & Sentiment (1h) ---
                deadlock_terms = [
                    "abrams",
                    "bebop",
                    "dynamo",
                    "grey talon",
                    "haze",
                    "infernus",
                    "ivy",
                    "kelvin",
                    "lady geist",
                    "mcginnis",
                    "mo & krill",
                    "paradox",
                    "pocket",
                    "seven",
                    "vindicta",
                    "viscous",
                    "warden",
                    "wraith",
                    "yamato",
                    "lash",
                    "shiv",
                    "urn",
                    "midboss",
                    "soul",
                    "flex slot",
                    "build",
                    "op",
                    "nerf",
                    "buff",
                    "patch",
                ]
                recent_msgs = conn.execute(
                    "SELECT content FROM twitch_chat_messages WHERE message_ts >= datetime('now', '-1 hour')"
                ).fetchall()

                term_counts = {t: 0 for t in deadlock_terms}
                sentiment = {"positive": 0, "negative": 0, "neutral": 0}
                pos_words = {
                    "pog",
                    "gg",
                    "nice",
                    "cool",
                    "krass",
                    "lol",
                    "win",
                    "stark",
                }
                neg_words = {
                    "rip",
                    "bad",
                    "lose",
                    "troll",
                    "cringe",
                    "throw",
                    "sucks",
                    "lag",
                }

                for row in recent_msgs:
                    content = (row[0] or "").lower()
                    for t in deadlock_terms:
                        if t in content:
                            term_counts[t] += 1
                    is_pos = any(w in content for w in pos_words)
                    is_neg = any(w in content for w in neg_words)
                    if is_pos and not is_neg:
                        sentiment["positive"] += 1
                    elif is_neg and not is_pos:
                        sentiment["negative"] += 1
                    else:
                        sentiment["neutral"] += 1

                meta_snapshot = sorted(
                    [{"term": k, "count": v} for k, v in term_counts.items() if v > 0],
                    key=lambda x: x["count"],
                    reverse=True,
                )[:10]
                total_sent = sum(sentiment.values()) or 1
                sent_data = {
                    "positive": sentiment["positive"],
                    "negative": sentiment["negative"],
                    "neutral": sentiment["neutral"],
                    "pos_pct": round(sentiment["positive"] / total_sent * 100, 1),
                    "neg_pct": round(sentiment["negative"] / total_sent * 100, 1),
                    "neu_pct": round(sentiment["neutral"] / total_sent * 100, 1),
                }

                # --- 5. Overlap (Top 5 Pairs) ---
                top_logins = [c["login"] for c in channels[:5]]
                overlap = []
                if len(top_logins) >= 2:
                    login_slots = (top_logins + ["!unused!"] * 5)[:5]
                    rows_overlap = conn.execute(
                        """
                        SELECT c1.streamer_login, c2.streamer_login, COUNT(DISTINCT c1.chatter_login)
                        FROM twitch_chat_messages c1
                        JOIN twitch_chat_messages c2 ON c1.chatter_login = c2.chatter_login AND c1.streamer_login < c2.streamer_login
                        WHERE c1.message_ts >= datetime('now', '-6 hours') AND c2.message_ts >= datetime('now', '-6 hours')
                          AND c1.streamer_login IN (?, ?, ?, ?, ?)
                          AND c2.streamer_login IN (?, ?, ?, ?, ?)
                        GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 5
                    """,
                        login_slots + login_slots,
                    ).fetchall()
                    overlap = [{"a": ro[0], "b": ro[1], "shared": ro[2]} for ro in rows_overlap]

                payload = {
                    "total_monitored": len(channels),
                    "total_viewers": total_viewers,
                    "avg_chat_health": avg_health,
                    "avg_lurker_ratio": avg_lurker,
                    "total_messages": len(recent_msgs),
                    "market_history": market_history,
                    "questions": questions,
                    "channels": channels,
                    "meta_snapshot": meta_snapshot,
                    "sentiment": sent_data,
                    "overlap": overlap,
                }

                return web.json_response(
                    payload, dumps=lambda data: json.dumps(data, default=_json_default)
                )
        except Exception as e:
            log.exception("Market API Error")
            return web.json_response({"error": str(e)}, status=500)

    async def reload_cog(self, request: web.Request) -> web.Response:
        """Optional reload endpoint for admin tooling compatibility."""
        token = (await request.post()).get("token", "")
        if not self._check_admin_token(token):
            log.warning(
                "AUDIT dashboard reload_cog: unauthorized attempt from peer=%s",
                self._sanitize_log_value(self._peer_host(request)),
            )
            return web.Response(text="Unauthorized", status=401)

        log.info(
            "AUDIT dashboard reload_cog: triggered by peer=%s",
            self._sanitize_log_value(self._peer_host(request)),
        )
        if self._reload_cb:
            msg = await self._reload_cb()
            return web.Response(text=msg)
        return web.Response(text="Kein Reload-Handler definiert", status=501)

    # ------------------------------------------------------------------ #
    # Route registration                                                   #
    # ------------------------------------------------------------------ #

    def _register_social_media_routes(self, app: web.Application) -> None:
        """Register Social Media Clip Publisher routes."""
        try:
            from ..social_media import ClipManager, create_social_media_app

            # Create clip manager (no Twitch API dependency yet)
            clip_manager = ClipManager()

            # Create social media dashboard with auth checker
            social_app = create_social_media_app(
                clip_manager=clip_manager,
                auth_checker=self._check_v2_auth,
                auth_session_getter=self._get_dashboard_auth_session,
            )

            # Mount social media routes
            for route in social_app.router.routes():
                app.router.add_route(
                    route.method,
                    route.resource.canonical,
                    route.handler,
                )

            log.info("Social Media Dashboard routes registered successfully")
        except Exception:
            log.exception("Failed to register Social Media Dashboard routes")

    def attach(self, app: web.Application) -> None:
        app.add_routes(
            [
                web.get("/", self.public_home),
                web.get("/twitch", self.index),
                web.get("/twitch/", self.index),
                web.get("/twitch/admin", self.admin),
                web.get("/twitch/live", self.admin),
                web.get("/twitch/add_any", self.add_any),
                web.get("/twitch/add_url", self.add_url),
                web.get("/twitch/add_login/{login}", self.add_login),
                web.post("/twitch/add_streamer", self.add_streamer),
                web.post("/twitch/remove", self.remove),
                web.post("/twitch/verify", self.verify),
                web.post("/twitch/archive", self.archive),
                web.post("/twitch/discord_flag", self.discord_flag),
                web.get("/twitch/stats", self.stats),
                web.get("/twitch/partners", self.partner_stats),
                web.get("/twitch/dashboards", self.stats_entry),
                web.get("/twitch/raid/auth", self.raid_auth_start),
                web.get("/twitch/raid/go", self.raid_auth_go),
                web.get("/twitch/raid/requirements", self.raid_requirements),
                web.get("/twitch/raid/history", self.raid_history),
                web.get("/twitch/raid/analytics", self.raid_analytics),
                web.get("/twitch/auth/login", self.auth_login),
                web.get("/twitch/auth/callback", self.auth_callback),
                web.get("/twitch/auth/logout", self.auth_logout),
                web.get("/twitch/auth/discord/login", self.discord_auth_login),
                web.get("/twitch/auth/discord/callback", self.discord_auth_callback),
                web.get("/twitch/auth/discord/logout", self.discord_auth_logout),
                web.get("/twitch/raid/callback", self.raid_oauth_callback),
                web.post("/twitch/discord_link", self.discord_link),
                web.post("/twitch/reload", self.reload_cog),
                web.get("/twitch/market", self.market_research),
                web.get("/twitch/api/market_data", self.api_market_data),
            ]
        )
        self._register_v2_routes(app.router)
        self._register_social_media_routes(app)
