"""
Analytics API v2 - Roadmap Mixin.

Public GET endpoint + admin-only CREATE / UPDATE / DELETE.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from aiohttp import web

from ..storage import pg as storage

log = logging.getLogger("TwitchStreams.AnalyticsV2")

_VALID_STATUSES = {"planned", "in_progress", "done"}


def _ensure_roadmap_table() -> None:
    with storage.get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS twitch_roadmap_items (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                title       TEXT    NOT NULL,
                description TEXT,
                status      TEXT    NOT NULL DEFAULT 'planned',
                priority    INTEGER NOT NULL DEFAULT 0,
                created_at  TEXT    DEFAULT (datetime('now')),
                updated_at  TEXT    DEFAULT (datetime('now'))
            )
            """
        )
        conn.commit()


def _row_to_dict(row: Any) -> dict:
    if hasattr(row, "keys"):
        return dict(row)
    return {
        "id": row[0],
        "title": row[1],
        "description": row[2],
        "status": row[3],
        "priority": row[4],
        "created_at": row[5],
        "updated_at": row[6],
    }


class _AnalyticsRoadmapMixin:
    """Mixin providing roadmap CRUD endpoints."""

    def _init_roadmap(self) -> None:
        try:
            _ensure_roadmap_table()
        except Exception:
            log.exception("Failed to ensure roadmap table")

    # ------------------------------------------------------------------
    # GET /twitch/api/v2/roadmap  (public)
    # ------------------------------------------------------------------

    async def _api_v2_roadmap_get(self, request: web.Request) -> web.Response:
        try:
            _ensure_roadmap_table()
            with storage.get_conn() as conn:
                rows = conn.execute(
                    "SELECT id, title, description, status, priority, created_at, updated_at "
                    "FROM twitch_roadmap_items ORDER BY priority DESC, id ASC"
                ).fetchall()
        except Exception:
            log.exception("roadmap_get failed")
            return web.Response(
                status=500,
                content_type="application/json",
                text=json.dumps({"error": "DB-Fehler"}),
            )

        grouped: dict[str, list[dict]] = {"planned": [], "in_progress": [], "done": []}
        for row in rows:
            item = _row_to_dict(row)
            status = item.get("status", "planned")
            if status in grouped:
                grouped[status].append(item)
            else:
                grouped["planned"].append(item)

        return web.Response(
            content_type="application/json",
            text=json.dumps(grouped),
        )

    # ------------------------------------------------------------------
    # POST /twitch/api/v2/roadmap  (admin only)
    # ------------------------------------------------------------------

    async def _api_v2_roadmap_create(self, request: web.Request) -> web.Response:
        if not self._check_v2_admin_auth(request):
            return web.Response(
                status=403,
                content_type="application/json",
                text=json.dumps({"error": "Nur Admins können Roadmap-Einträge erstellen"}),
            )

        try:
            body = await request.json()
        except Exception:
            return web.Response(
                status=400,
                content_type="application/json",
                text=json.dumps({"error": "Ungültiges JSON"}),
            )

        title = str(body.get("title") or "").strip()
        if not title:
            return web.Response(
                status=400,
                content_type="application/json",
                text=json.dumps({"error": "Titel fehlt"}),
            )

        description = str(body.get("description") or "").strip() or None
        status = str(body.get("status") or "planned").strip().lower()
        if status not in _VALID_STATUSES:
            status = "planned"
        priority = int(body.get("priority") or 0)

        try:
            _ensure_roadmap_table()
            with storage.get_conn() as conn:
                cur = conn.execute(
                    "INSERT INTO twitch_roadmap_items (title, description, status, priority) "
                    "VALUES (?, ?, ?, ?)",
                    (title, description, status, priority),
                )
                row_id = cur.lastrowid
                conn.commit()
                row = conn.execute(
                    "SELECT id, title, description, status, priority, created_at, updated_at "
                    "FROM twitch_roadmap_items WHERE id = ?",
                    (row_id,),
                ).fetchone()
        except Exception:
            log.exception("roadmap_create failed")
            return web.Response(
                status=500,
                content_type="application/json",
                text=json.dumps({"error": "DB-Fehler beim Erstellen"}),
            )

        return web.Response(
            status=201,
            content_type="application/json",
            text=json.dumps(_row_to_dict(row)),
        )

    # ------------------------------------------------------------------
    # PATCH /twitch/api/v2/roadmap/{id}  (admin only)
    # ------------------------------------------------------------------

    async def _api_v2_roadmap_update(self, request: web.Request) -> web.Response:
        if not self._check_v2_admin_auth(request):
            return web.Response(
                status=403,
                content_type="application/json",
                text=json.dumps({"error": "Nur Admins können Roadmap-Einträge ändern"}),
            )

        item_id = request.match_info.get("id", "")
        if not item_id.isdigit():
            return web.Response(
                status=400,
                content_type="application/json",
                text=json.dumps({"error": "Ungültige ID"}),
            )

        try:
            body = await request.json()
        except Exception:
            return web.Response(
                status=400,
                content_type="application/json",
                text=json.dumps({"error": "Ungültiges JSON"}),
            )

        updates: list[str] = []
        params: list[Any] = []

        if "title" in body:
            title = str(body["title"] or "").strip()
            if title:
                updates.append("title = ?")
                params.append(title)

        if "description" in body:
            updates.append("description = ?")
            params.append(str(body["description"] or "").strip() or None)

        if "status" in body:
            status = str(body["status"] or "").strip().lower()
            if status in _VALID_STATUSES:
                updates.append("status = ?")
                params.append(status)

        if "priority" in body:
            updates.append("priority = ?")
            params.append(int(body["priority"] or 0))

        if not updates:
            return web.Response(
                status=400,
                content_type="application/json",
                text=json.dumps({"error": "Keine änderbaren Felder angegeben"}),
            )

        updates.append("updated_at = ?")
        params.append(datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"))
        params.append(int(item_id))

        try:
            _ensure_roadmap_table()
            with storage.get_conn() as conn:
                conn.execute(
                    f"UPDATE twitch_roadmap_items SET {', '.join(updates)} WHERE id = ?",
                    params,
                )
                conn.commit()
                row = conn.execute(
                    "SELECT id, title, description, status, priority, created_at, updated_at "
                    "FROM twitch_roadmap_items WHERE id = ?",
                    (int(item_id),),
                ).fetchone()
        except Exception:
            log.exception("roadmap_update failed")
            return web.Response(
                status=500,
                content_type="application/json",
                text=json.dumps({"error": "DB-Fehler beim Aktualisieren"}),
            )

        if row is None:
            return web.Response(
                status=404,
                content_type="application/json",
                text=json.dumps({"error": "Eintrag nicht gefunden"}),
            )

        return web.Response(
            content_type="application/json",
            text=json.dumps(_row_to_dict(row)),
        )

    # ------------------------------------------------------------------
    # DELETE /twitch/api/v2/roadmap/{id}  (admin only)
    # ------------------------------------------------------------------

    async def _api_v2_roadmap_delete(self, request: web.Request) -> web.Response:
        if not self._check_v2_admin_auth(request):
            return web.Response(
                status=403,
                content_type="application/json",
                text=json.dumps({"error": "Nur Admins können Roadmap-Einträge löschen"}),
            )

        item_id = request.match_info.get("id", "")
        if not item_id.isdigit():
            return web.Response(
                status=400,
                content_type="application/json",
                text=json.dumps({"error": "Ungültige ID"}),
            )

        try:
            _ensure_roadmap_table()
            with storage.get_conn() as conn:
                conn.execute(
                    "DELETE FROM twitch_roadmap_items WHERE id = ?",
                    (int(item_id),),
                )
                conn.commit()
        except Exception:
            log.exception("roadmap_delete failed")
            return web.Response(
                status=500,
                content_type="application/json",
                text=json.dumps({"error": "DB-Fehler beim Löschen"}),
            )

        return web.Response(
            status=204,
            content_type="application/json",
            text="",
        )
