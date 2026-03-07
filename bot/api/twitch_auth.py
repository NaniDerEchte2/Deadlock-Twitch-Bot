"""Shared helpers for Twitch OAuth credential handling."""

from __future__ import annotations

import json


class TwitchClientConfigError(RuntimeError):
    """Raised when Twitch client credentials are missing or rejected."""


def normalize_twitch_credential(value: str | None) -> str:
    """Normalize environment-provided Twitch credentials."""
    return str(value or "").strip()


def is_invalid_client_response(status: int, body: str | None) -> bool:
    """Return True when Twitch rejects the client credentials themselves."""
    if int(status) != 400:
        return False

    text = str(body or "")
    lowered = text.lower()
    if "invalid client" in lowered:
        return True

    try:
        payload = json.loads(text)
    except Exception:
        return False

    if not isinstance(payload, dict):
        return False

    for key in ("message", "error"):
        if "invalid client" in str(payload.get(key, "")).lower():
            return True

    return False
