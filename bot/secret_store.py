"""Small helpers for loading secrets from keyring or environment."""

from __future__ import annotations

import os

KEYRING_SERVICE_NAME = "DeadlockBot"


def read_keyring_secret(key: str) -> str:
    """Return a trimmed secret from Windows Credential Manager when available."""
    secret_key = str(key or "").strip()
    if not secret_key:
        return ""

    try:
        import keyring  # type: ignore
    except Exception:
        return ""

    try:
        value = keyring.get_password(KEYRING_SERVICE_NAME, secret_key)
        if not value:
            value = keyring.get_password(f"{secret_key}@{KEYRING_SERVICE_NAME}", secret_key)
    except Exception:
        return ""

    return str(value or "").strip()


def load_secret_value(*keys: str) -> str:
    """Return the first non-empty secret, preferring keyring over environment."""
    for raw_key in keys:
        value = read_keyring_secret(raw_key)
        if value:
            return value

    for raw_key in keys:
        key = str(raw_key or "").strip()
        if not key:
            continue
        value = str(os.getenv(key) or "").strip()
        if value:
            return value

    return ""
