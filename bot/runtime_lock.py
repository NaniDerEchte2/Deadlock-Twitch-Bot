"""Single-instance PID/file locks for split Twitch services."""

from __future__ import annotations

import json
import os
import re
import sys
import threading
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

if os.name == "nt":  # pragma: no cover - exercised on Windows runtime
    import msvcrt
else:  # pragma: no cover - exercised on POSIX runtime
    import fcntl

_ACTIVE_LOCKS: set[Path] = set()
_ACTIVE_LOCKS_GUARD = threading.Lock()


class RuntimeInstanceLockError(RuntimeError):
    """Raised when another runtime instance already holds the service lock."""


def _sanitize_service_name(service_name: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(service_name or "").strip())
    return normalized.strip(".-") or "runtime"


def _default_lock_dir() -> Path:
    explicit = (os.getenv("TWITCH_RUNTIME_PID_LOCK_DIR") or "").strip()
    if explicit:
        return Path(explicit).expanduser()
    return Path(__file__).resolve().parents[1] / "data" / "runtime" / "locks"


def _lock_metadata(
    *,
    service_name: str,
    port: int,
) -> dict[str, Any]:
    return {
        "pid": os.getpid(),
        "service": service_name,
        "port": int(port),
        "started_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "argv": list(sys.argv),
    }


def _read_metadata(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
    except OSError:
        return {}
    if not raw:
        return {}
    text = raw.decode("utf-8", errors="replace").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        payload = {}
    if isinstance(payload, dict):
        return payload
    return {}


def _write_metadata(path: Path, payload: dict[str, Any]) -> None:
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    try:
        path.write_bytes(encoded)
    except OSError:
        return


def _ensure_lock_file_exists(handle) -> None:
    handle.seek(0, os.SEEK_END)
    if handle.tell() > 0:
        handle.seek(0)
        return
    handle.write(b"\n")
    handle.flush()
    handle.seek(0)


def _acquire_file_lock(handle) -> None:
    handle.seek(0)
    if os.name == "nt":  # pragma: no branch
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        return
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _release_file_lock(handle) -> None:
    handle.seek(0)
    if os.name == "nt":  # pragma: no branch
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return
    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class RuntimePidLock(AbstractContextManager["RuntimePidLock"]):
    """Keep a per-service lock file held for the current process lifetime."""

    def __init__(
        self,
        service_name: str,
        *,
        port: int,
        lock_dir: str | os.PathLike[str] | None = None,
    ) -> None:
        self.service_name = _sanitize_service_name(service_name)
        self.port = int(port)
        self.lock_dir = Path(lock_dir).expanduser() if lock_dir is not None else _default_lock_dir()
        self.path = self.lock_dir / f"{self.service_name}-{self.port}.pidlock"
        self.lock_path = self.lock_dir / f"{self.service_name}-{self.port}.lock"
        self._handle = None

    def __enter__(self) -> RuntimePidLock:
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.release()
        return False

    def acquire(self) -> None:
        if self._handle is not None:
            return

        self.lock_dir.mkdir(parents=True, exist_ok=True)
        with _ACTIVE_LOCKS_GUARD:
            if self.lock_path in _ACTIVE_LOCKS:
                raise RuntimeInstanceLockError(
                    f"{self.service_name} already holds runtime lock {self.lock_path} in pid {os.getpid()}."
                )

        try:
            handle = self.lock_path.open("r+b")
        except FileNotFoundError:
            handle = self.lock_path.open("w+b")

        try:
            _ensure_lock_file_exists(handle)
            try:
                _acquire_file_lock(handle)
            except OSError as exc:
                owner = _read_metadata(self.path)
                owner_pid = owner.get("pid")
                owner_started_at = owner.get("started_at")
                details = [
                    f"{self.service_name} runtime lock already held",
                    f"port={self.port}",
                    f"path={self.lock_path}",
                ]
                if owner_pid:
                    details.append(f"owner_pid={owner_pid}")
                if owner_started_at:
                    details.append(f"owner_started_at={owner_started_at}")
                raise RuntimeInstanceLockError(", ".join(details)) from exc

            payload = _lock_metadata(service_name=self.service_name, port=self.port)
            _write_metadata(self.path, payload)
            self._handle = handle
            with _ACTIVE_LOCKS_GUARD:
                _ACTIVE_LOCKS.add(self.lock_path)
        except Exception:
            handle.close()
            raise

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        self._handle = None
        with _ACTIVE_LOCKS_GUARD:
            _ACTIVE_LOCKS.discard(self.lock_path)
        try:
            _release_file_lock(handle)
        finally:
            handle.close()


def runtime_pid_lock(
    service_name: str,
    *,
    port: int,
    lock_dir: str | os.PathLike[str] | None = None,
) -> RuntimePidLock:
    """Return a context manager that enforces one runtime instance per service/port."""

    return RuntimePidLock(service_name, port=port, lock_dir=lock_dir)


__all__ = [
    "RuntimeInstanceLockError",
    "RuntimePidLock",
    "runtime_pid_lock",
]
