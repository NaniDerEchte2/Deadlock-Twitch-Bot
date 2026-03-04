"""Standalone dashboard service app that forwards bot operations via internal API."""

from __future__ import annotations

import asyncio
import os
from typing import Any

from aiohttp import web

from ..core.constants import (
    TWITCH_DASHBOARD_HOST,
    TWITCH_DASHBOARD_NOAUTH,
    TWITCH_DASHBOARD_PORT,
    TWITCH_INTERNAL_API_HOST,
    TWITCH_INTERNAL_API_PORT,
    log,
)
from ..dashboard.server_v2 import build_v2_app
from .client import BotApiClient


def _parse_env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _require_noauth_opt_in_if_enabled(*, enabled: bool) -> None:
    if not enabled:
        return
    if _parse_env_bool("TWITCH_ALLOW_DASHBOARD_NOAUTH", False):
        return
    raise RuntimeError(
        "Refusing to start dashboard with no-auth enabled. "
        "Set TWITCH_ALLOW_DASHBOARD_NOAUTH=1 only for controlled local debugging."
    )


def _parse_env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _parse_env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _default_internal_api_base_url() -> str:
    explicit = (os.getenv("TWITCH_INTERNAL_API_BASE_URL") or "").strip()
    if explicit:
        return explicit
    host = (os.getenv("TWITCH_INTERNAL_API_HOST") or TWITCH_INTERNAL_API_HOST or "127.0.0.1").strip()
    port = _parse_env_int("TWITCH_INTERNAL_API_PORT", int(TWITCH_INTERNAL_API_PORT or 8766))
    return f"http://{host}:{port}"


def build_dashboard_service_app(
    *,
    internal_api_base_url: str | None = None,
    internal_api_token: str | None = None,
    internal_api_allow_non_loopback: bool | None = None,
    internal_api_timeout_seconds: float | None = None,
    dashboard_token: str | None = None,
    partner_token: str | None = None,
    noauth: bool | None = None,
    oauth_client_id: str | None = None,
    oauth_client_secret: str | None = None,
    oauth_redirect_uri: str | None = None,
    session_ttl_seconds: int | None = None,
    legacy_stats_url: str | None = None,
) -> web.Application:
    """Build the standalone dashboard app and wire callbacks through `BotApiClient`."""

    resolved_noauth = (
        bool(noauth)
        if noauth is not None
        else _parse_env_bool("TWITCH_DASHBOARD_NOAUTH", bool(TWITCH_DASHBOARD_NOAUTH))
    )
    _require_noauth_opt_in_if_enabled(enabled=resolved_noauth)

    resolved_internal_base = (internal_api_base_url or _default_internal_api_base_url()).strip()
    resolved_internal_token = (
        internal_api_token
        if internal_api_token is not None
        else (os.getenv("TWITCH_INTERNAL_API_TOKEN") or "").strip()
    )
    timeout_seconds = (
        float(internal_api_timeout_seconds)
        if internal_api_timeout_seconds is not None
        else _parse_env_float("TWITCH_INTERNAL_API_TIMEOUT_SEC", 10.0)
    )
    allow_non_loopback = (
        bool(internal_api_allow_non_loopback)
        if internal_api_allow_non_loopback is not None
        else _parse_env_bool("TWITCH_INTERNAL_API_ALLOW_NON_LOOPBACK", False)
    )
    client = BotApiClient(
        base_url=resolved_internal_base,
        token=resolved_internal_token,
        allow_non_loopback=allow_non_loopback,
        timeout_seconds=timeout_seconds,
    )

    async def _add_cb(login: str, require_link: bool) -> str:
        return await client.add_streamer(login, require_link=require_link)

    async def _remove_cb(login: str) -> str:
        return await client.remove_streamer(login)

    async def _list_cb() -> list[dict[str, Any]]:
        return await client.get_streamers()

    async def _stats_cb(**kwargs: Any) -> dict[str, Any]:
        return await client.get_stats(
            hour_from=kwargs.get("hour_from"),
            hour_to=kwargs.get("hour_to"),
            streamer=kwargs.get("streamer"),
        )

    async def _verify_cb(login: str, mode: str) -> str:
        return await client.verify_streamer(login, mode=mode)

    async def _archive_cb(login: str, mode: str) -> str:
        return await client.archive_streamer(login, mode=mode)

    async def _discord_flag_cb(login: str, is_on_discord: bool) -> str:
        return await client.set_discord_flag(login, is_on_discord=is_on_discord)

    async def _discord_profile_cb(
        login: str,
        discord_user_id: str | None,
        discord_display_name: str | None,
        mark_member: bool,
    ) -> str:
        return await client.save_discord_profile(
            login,
            discord_user_id=discord_user_id,
            discord_display_name=discord_display_name,
            mark_member=mark_member,
        )

    async def _raid_auth_url_cb(login: str) -> str:
        return await client.get_raid_auth_url(login)

    async def _raid_go_url_cb(state: str) -> str | None:
        return await client.get_raid_go_url(state)

    async def _raid_requirements_cb(login: str) -> str:
        return await client.send_raid_requirements(login)

    async def _raid_oauth_callback_cb(*, code: str, state: str, error: str) -> dict[str, Any]:
        return await client.process_raid_oauth_callback(code=code, state=state, error=error)

    resolved_dashboard_token = (
        dashboard_token
        if dashboard_token is not None
        else (os.getenv("TWITCH_DASHBOARD_TOKEN") or "").strip() or None
    )
    resolved_partner_token = (
        partner_token
        if partner_token is not None
        else (os.getenv("TWITCH_PARTNER_TOKEN") or "").strip() or None
    )
    resolved_oauth_client_id = (
        oauth_client_id if oauth_client_id is not None else (os.getenv("TWITCH_CLIENT_ID") or "").strip() or None
    )
    resolved_oauth_client_secret = (
        oauth_client_secret
        if oauth_client_secret is not None
        else (os.getenv("TWITCH_CLIENT_SECRET") or "").strip() or None
    )
    resolved_oauth_redirect_uri = (
        oauth_redirect_uri
        if oauth_redirect_uri is not None
        else (os.getenv("TWITCH_DASHBOARD_AUTH_REDIRECT_URI") or "").strip()
        or "https://twitch.earlysalty.com/twitch/auth/callback"
    )
    resolved_session_ttl = (
        int(session_ttl_seconds)
        if session_ttl_seconds is not None
        else max(6 * 3600, _parse_env_int("TWITCH_DASHBOARD_SESSION_TTL_SEC", 6 * 3600))
    )
    resolved_legacy_stats_url = (
        legacy_stats_url
        if legacy_stats_url is not None
        else (os.getenv("TWITCH_LEGACY_STATS_URL") or "").strip() or None
    )

    app = build_v2_app(
        noauth=resolved_noauth,
        token=resolved_dashboard_token,
        partner_token=resolved_partner_token,
        oauth_client_id=resolved_oauth_client_id,
        oauth_client_secret=resolved_oauth_client_secret,
        oauth_redirect_uri=resolved_oauth_redirect_uri,
        session_ttl_seconds=resolved_session_ttl,
        legacy_stats_url=resolved_legacy_stats_url,
        add_cb=_add_cb,
        remove_cb=_remove_cb,
        list_cb=_list_cb,
        stats_cb=_stats_cb,
        verify_cb=_verify_cb,
        archive_cb=_archive_cb,
        discord_flag_cb=_discord_flag_cb,
        discord_profile_cb=_discord_profile_cb,
        raid_history_cb=None,
        raid_bot=None,
        raid_auth_url_cb=_raid_auth_url_cb,
        raid_go_url_cb=_raid_go_url_cb,
        raid_requirements_cb=_raid_requirements_cb,
        raid_oauth_callback_cb=_raid_oauth_callback_cb,
        reload_cb=None,
        eventsub_webhook_handler=None,
    )

    async def _close_client(_: web.Application) -> None:
        await client.close()

    app["bot_api_client"] = client
    app.on_cleanup.append(_close_client)
    return app


async def run_dashboard_service(
    *,
    host: str | None = None,
    port: int | None = None,
    app: web.Application | None = None,
) -> None:
    """Run standalone dashboard service until cancelled."""

    resolved_host = (host or os.getenv("TWITCH_DASHBOARD_HOST") or TWITCH_DASHBOARD_HOST or "127.0.0.1").strip()
    resolved_port = int(
        port
        if port is not None
        else _parse_env_int("TWITCH_DASHBOARD_PORT", int(TWITCH_DASHBOARD_PORT or 8765))
    )
    dashboard_app = app or build_dashboard_service_app()
    runner = web.AppRunner(dashboard_app)
    await runner.setup()
    site = web.TCPSite(runner, host=resolved_host, port=resolved_port)
    await site.start()

    log.info(
        "Standalone dashboard service running on http://%s:%s/twitch",
        resolved_host,
        resolved_port,
    )
    try:
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()


__all__ = ["build_dashboard_service_app", "run_dashboard_service"]
