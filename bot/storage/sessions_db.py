"""Encrypted SQLite session storage for dashboard web sessions.

Sessions are persisted in data/sessions.sqlite3 (separate from PostgreSQL analytics).
The payload is encrypted with Fernet (AES-128-CBC + HMAC-SHA256) using a key stored
in the Windows Credential Manager (service: DeadlockBot, key: SESSIONS_ENCRYPTION_KEY).

If the key is missing it is auto-generated and saved to the keyring on first run.
Without the key the file is useless to an attacker even if they steal it.
"""

from __future__ import annotations

import contextlib
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import TYPE_CHECKING

from ..core.constants import log

if TYPE_CHECKING:
    from cryptography.fernet import Fernet as _FernetT

_KEYRING_SERVICE = "DeadlockBot"
_KEYRING_KEY_NAME = "SESSIONS_ENCRYPTION_KEY"
_DB_PATH_ENV = "SESSIONS_DB_PATH"

# Module-level Fernet singleton – initialised lazily
_fernet: _FernetT | None = None


# ---------------------------------------------------------------------------
# Key management
# ---------------------------------------------------------------------------

def _load_or_create_key() -> bytes:
    """Return the Fernet key from keyring, creating and storing it on first use."""
    try:
        import keyring  # type: ignore
        val = keyring.get_password(_KEYRING_SERVICE, _KEYRING_KEY_NAME)
        if val:
            return val.encode()
    except Exception as exc:
        log.debug("Keyring read for sessions key failed: %s", exc)

    from cryptography.fernet import Fernet
    key = Fernet.generate_key()
    log.info("Sessions: generated new encryption key")
    try:
        import keyring  # type: ignore
        keyring.set_password(_KEYRING_SERVICE, _KEYRING_KEY_NAME, key.decode())
        log.info("Sessions: stored encryption key in Windows Credential Manager")
    except Exception as exc:
        log.warning(
            "Sessions: could not store encryption key in keyring (%s). "
            "Sessions will not survive restarts until the key is persisted.",
            exc,
        )
    return key


def _get_fernet() -> _FernetT:
    global _fernet
    if _fernet is None:
        from cryptography.fernet import Fernet
        _fernet = Fernet(_load_or_create_key())
    return _fernet


# ---------------------------------------------------------------------------
# Encryption helpers
# ---------------------------------------------------------------------------

def _encrypt(payload: dict) -> bytes:
    return _get_fernet().encrypt(json.dumps(payload, default=str).encode())


def _decrypt(data: bytes | memoryview) -> dict:
    raw = bytes(data) if isinstance(data, memoryview) else data
    return json.loads(_get_fernet().decrypt(raw).decode())


# ---------------------------------------------------------------------------
# DB path
# ---------------------------------------------------------------------------

def _db_path() -> Path:
    env = os.getenv(_DB_PATH_ENV, "").strip()
    if env:
        return Path(env)
    return Path(__file__).parent.parent.parent / "data" / "sessions.sqlite3"


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def _ensure_table(conn: sqlite3.Connection) -> None:
    if getattr(_ensure_table, "_done", False):
        return
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS dashboard_sessions (
            session_id   TEXT PRIMARY KEY,
            session_type TEXT NOT NULL DEFAULT 'twitch',
            payload_enc  BLOB NOT NULL,
            created_at   REAL NOT NULL,
            expires_at   REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_ds_expires
            ON dashboard_sessions(expires_at);
        """
    )
    conn.commit()
    _ensure_table._done = True


@contextlib.contextmanager
def _conn():
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        _ensure_table(conn)
        yield conn
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Public API  (mirrors what auth_mixin / routes_mixin call)
# ---------------------------------------------------------------------------

def upsert_session(
    session_id: str,
    session_type: str,
    payload: dict,
    created_at: float,
    expires_at: float,
) -> None:
    """Insert or refresh a session (payload is Fernet-encrypted)."""
    enc = _encrypt(payload)
    with _conn() as conn:
        conn.execute(
            """
            INSERT INTO dashboard_sessions
                (session_id, session_type, payload_enc, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                payload_enc = excluded.payload_enc,
                expires_at  = excluded.expires_at
            """,
            (session_id, session_type, enc, created_at, expires_at),
        )


def delete_session(session_id: str) -> None:
    """Remove a session (logout / invalidation)."""
    path = _db_path()
    if not path.exists():
        return
    with _conn() as conn:
        conn.execute(
            "DELETE FROM dashboard_sessions WHERE session_id = ?", (session_id,)
        )


def load_valid_sessions(
    session_type: str, min_expires_at: float
) -> list[tuple[str, dict]]:
    """Return all non-expired sessions as (session_id, payload) tuples."""
    path = _db_path()
    if not path.exists():
        return []

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        _ensure_table(conn)
        rows = conn.execute(
            """
            SELECT session_id, payload_enc
            FROM   dashboard_sessions
            WHERE  session_type = ? AND expires_at > ?
            """,
            (session_type, min_expires_at),
        ).fetchall()
    finally:
        conn.close()

    result: list[tuple[str, dict]] = []
    fernet = _get_fernet()
    for row in rows:
        try:
            payload = json.loads(fernet.decrypt(bytes(row["payload_enc"])).decode())
            result.append((row["session_id"], payload))
        except Exception as exc:
            log.debug("Sessions: could not decrypt row %s: %s", row["session_id"], exc)
    return result


def delete_expired_sessions(now: float) -> None:
    """Purge all sessions that have already expired."""
    path = _db_path()
    if not path.exists():
        return
    with _conn() as conn:
        conn.execute(
            "DELETE FROM dashboard_sessions WHERE expires_at <= ?", (now,)
        )
