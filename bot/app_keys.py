"""Shared aiohttp application keys for split runtime state."""

from __future__ import annotations

from typing import Any

from aiohttp import web

BOT_API_CLIENT_KEY = web.AppKey("bot_api_client", Any)
ANALYTICS_DB_FINGERPRINT_KEY = web.AppKey("analytics_db_fingerprint", Any)
ANALYTICS_DB_FINGERPRINT_DETAILS_KEY = web.AppKey("analytics_db_fingerprint_details", Any)
INTERNAL_API_ANALYTICS_DB_FINGERPRINT_KEY = web.AppKey(
    "internal_api_analytics_db_fingerprint",
    Any,
)
ANALYTICS_DB_FINGERPRINT_MISMATCH_KEY = web.AppKey(
    "analytics_db_fingerprint_mismatch",
    Any,
)
ANALYTICS_DB_FINGERPRINT_ERROR_KEY = web.AppKey("analytics_db_fingerprint_error", Any)


__all__ = [
    "ANALYTICS_DB_FINGERPRINT_DETAILS_KEY",
    "ANALYTICS_DB_FINGERPRINT_ERROR_KEY",
    "ANALYTICS_DB_FINGERPRINT_KEY",
    "ANALYTICS_DB_FINGERPRINT_MISMATCH_KEY",
    "BOT_API_CLIENT_KEY",
    "INTERNAL_API_ANALYTICS_DB_FINGERPRINT_KEY",
]
