"""Runtime role/port hardening helpers for split Twitch services."""

from __future__ import annotations

import os

ROLE_MASTER = "master"
ROLE_TWITCH_WORKER = "twitch_worker"
ROLE_DASHBOARD = "dashboard"

DASHBOARD_SERVICE_PORT = 8765
MASTER_API_RESERVED_PORT = 8766
INTERNAL_API_PORT = 8776

_ALLOWED_ROLES = {
    ROLE_MASTER,
    ROLE_TWITCH_WORKER,
    ROLE_DASHBOARD,
}

_ROLE_ALIASES = {
    "bot": ROLE_TWITCH_WORKER,
    "worker": ROLE_TWITCH_WORKER,
    "twitch-worker": ROLE_TWITCH_WORKER,
}


def _parse_env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def split_runtime_enforced() -> bool:
    if (os.getenv("TWITCH_RUNTIME_ENFORCE") or "").strip():
        return _parse_env_bool("TWITCH_RUNTIME_ENFORCE", True)
    return _parse_env_bool("TWITCH_SPLIT_RUNTIME_ENFORCE", True)


def resolve_runtime_role(value: str | None = None) -> str:
    raw = value
    if raw is None:
        raw = (
            os.getenv("TWITCH_RUNTIME_ROLE")
            or os.getenv("TWITCH_SPLIT_RUNTIME_ROLE")
            or ""
        )
    normalized = str(raw or "").strip().lower().replace("-", "_")
    return _ROLE_ALIASES.get(normalized, normalized)


def _role_error_message(*, service_name: str, expected_role: str, got_role: str) -> str:
    if not got_role:
        return (
            f"Runtime hardening violation for {service_name}: runtime role is missing. "
            f"Set TWITCH_RUNTIME_ROLE={expected_role} "
            f"(or TWITCH_SPLIT_RUNTIME_ROLE={expected_role})."
        )
    if got_role not in _ALLOWED_ROLES:
        return (
            f"Runtime hardening violation for {service_name}: unsupported runtime role '{got_role}'. "
            "Allowed roles: master, twitch_worker, dashboard."
        )
    return (
        f"Runtime hardening violation for {service_name}: expected role '{expected_role}', "
        f"got '{got_role}'."
    )


def _port_error_message(*, service_name: str, expected_port: int, got_port: int) -> str:
    if got_port == MASTER_API_RESERVED_PORT:
        return (
            f"Runtime hardening violation for {service_name}: port {MASTER_API_RESERVED_PORT} "
            "is reserved for the master API service."
        )
    return (
        f"Runtime hardening violation for {service_name}: expected port {expected_port}, "
        f"got {got_port}."
    )


def _enforce_service_runtime(
    *,
    service_name: str,
    expected_role: str,
    expected_port: int,
    role: str | None,
    port: int,
) -> str:
    resolved_role = resolve_runtime_role(role)
    if not split_runtime_enforced():
        return resolved_role

    if resolved_role != expected_role:
        raise RuntimeError(
            _role_error_message(
                service_name=service_name,
                expected_role=expected_role,
                got_role=resolved_role,
            )
        )

    if int(port) != int(expected_port):
        raise RuntimeError(
            _port_error_message(
                service_name=service_name,
                expected_port=int(expected_port),
                got_port=int(port),
            )
        )

    return resolved_role


def enforce_dashboard_service_runtime(*, role: str | None = None, port: int) -> str:
    return _enforce_service_runtime(
        service_name="dashboard_service",
        expected_role=ROLE_DASHBOARD,
        expected_port=DASHBOARD_SERVICE_PORT,
        role=role,
        port=port,
    )


def enforce_internal_api_runtime(*, role: str | None = None, port: int) -> str:
    return _enforce_service_runtime(
        service_name="internal_api",
        expected_role=ROLE_TWITCH_WORKER,
        expected_port=INTERNAL_API_PORT,
        role=role,
        port=port,
    )


__all__ = [
    "DASHBOARD_SERVICE_PORT",
    "INTERNAL_API_PORT",
    "MASTER_API_RESERVED_PORT",
    "ROLE_DASHBOARD",
    "ROLE_MASTER",
    "ROLE_TWITCH_WORKER",
    "enforce_dashboard_service_runtime",
    "enforce_internal_api_runtime",
    "resolve_runtime_role",
    "split_runtime_enforced",
]
