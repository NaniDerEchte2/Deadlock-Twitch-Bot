"""Internal API app for bot/dashboard split mode."""

from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import time as time_module
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from ipaddress import ip_address
from typing import Any
from urllib.parse import unquote, urlsplit
from uuid import UUID

from aiohttp import web

from ..core.constants import log

INTERNAL_API_BASE_PATH = "/internal/twitch/v1"
INTERNAL_TOKEN_HEADER = "X-Internal-Token"
IDEMPOTENCY_KEY_HEADER = "Idempotency-Key"
_LOGIN_RE = re.compile(r"^[a-z0-9_]{3,25}$")


@dataclass(slots=True)
class _IdempotencyInFlight:
    fingerprint: str
    future: asyncio.Future[tuple[int, Any]]
    created_at: float


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, set):
        return list(value)
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


class InternalApiServer:
    """Expose selected Twitch dashboard operations via an authenticated local API."""

    def __init__(
        self,
        *,
        token: str | None,
        base_path: str = INTERNAL_API_BASE_PATH,
        add_cb: Callable[[str, bool], Awaitable[str]] | None = None,
        remove_cb: Callable[[str], Awaitable[str]] | None = None,
        list_cb: Callable[[], Awaitable[list[dict[str, Any]]]] | None = None,
        stats_cb: Callable[..., Awaitable[dict[str, Any]]] | None = None,
        verify_cb: Callable[[str, str], Awaitable[str]] | None = None,
        archive_cb: Callable[[str, str], Awaitable[str]] | None = None,
        discord_flag_cb: Callable[[str, bool], Awaitable[str]] | None = None,
        discord_profile_cb: Callable[..., Awaitable[str]] | None = None,
        streamer_analytics_cb: Callable[[str, int], Awaitable[dict[str, Any]]] | None = None,
        comparison_cb: Callable[[int], Awaitable[dict[str, Any]]] | None = None,
        session_cb: Callable[[int], Awaitable[dict[str, Any]]] | None = None,
        raid_auth_url_cb: Callable[[str], Awaitable[str]] | None = None,
        raid_go_url_cb: Callable[[str], Awaitable[str | None]] | None = None,
        raid_requirements_cb: Callable[[str], Awaitable[str]] | None = None,
        raid_oauth_callback_cb: Callable[..., Awaitable[dict[str, Any]]] | None = None,
    ) -> None:
        self._token = (token or "").strip()
        base = (base_path or INTERNAL_API_BASE_PATH).strip()
        if not base:
            base = INTERNAL_API_BASE_PATH
        if not base.startswith("/"):
            base = f"/{base}"
        self._base_path = base.rstrip("/")

        self._add = add_cb if callable(add_cb) else self._empty_add
        self._remove = remove_cb if callable(remove_cb) else self._empty_remove
        self._list = list_cb if callable(list_cb) else self._empty_list
        self._stats = stats_cb if callable(stats_cb) else self._empty_stats
        self._verify = verify_cb if callable(verify_cb) else self._empty_verify
        self._archive = archive_cb if callable(archive_cb) else self._empty_archive
        self._discord_flag = (
            discord_flag_cb if callable(discord_flag_cb) else self._empty_discord_flag
        )
        self._discord_profile = (
            discord_profile_cb if callable(discord_profile_cb) else self._empty_discord_profile
        )
        self._streamer_analytics = (
            streamer_analytics_cb
            if callable(streamer_analytics_cb)
            else self._empty_streamer_analytics
        )
        self._comparison = comparison_cb if callable(comparison_cb) else self._empty_comparison
        self._session = session_cb if callable(session_cb) else self._empty_session
        self._raid_auth_url = (
            raid_auth_url_cb if callable(raid_auth_url_cb) else self._empty_raid_auth_url
        )
        self._raid_go_url = raid_go_url_cb if callable(raid_go_url_cb) else self._empty_raid_go_url
        self._raid_requirements = (
            raid_requirements_cb
            if callable(raid_requirements_cb)
            else self._empty_raid_requirements
        )
        self._raid_oauth_callback = (
            raid_oauth_callback_cb
            if callable(raid_oauth_callback_cb)
            else self._empty_raid_oauth_callback
        )
        self._idempotency_cache: dict[str, dict[str, Any]] = {}
        self._idempotency_inflight: dict[str, _IdempotencyInFlight] = {}
        self._idempotency_ttl_seconds = 15 * 60
        self._idempotency_max_entries = 2000
        self._allowed_guild_ids = self._parse_allowlist_ids(
            os.getenv("TWITCH_INTERNAL_API_ALLOWED_GUILD_IDS"),
            env_name="TWITCH_INTERNAL_API_ALLOWED_GUILD_IDS",
        )
        self._allowed_channel_ids = self._parse_allowlist_ids(
            os.getenv("TWITCH_INTERNAL_API_ALLOWED_CHANNEL_IDS"),
            env_name="TWITCH_INTERNAL_API_ALLOWED_CHANNEL_IDS",
        )
        self._allowed_role_ids = self._parse_allowlist_ids(
            os.getenv("TWITCH_INTERNAL_API_ALLOWED_ROLE_IDS"),
            env_name="TWITCH_INTERNAL_API_ALLOWED_ROLE_IDS",
        )

    async def _empty_add(self, _: str, __: bool) -> str:
        return "Add operation unavailable"

    async def _empty_remove(self, _: str) -> str:
        return "Remove operation unavailable"

    async def _empty_list(self) -> list[dict[str, Any]]:
        return []

    async def _empty_stats(self, **_: Any) -> dict[str, Any]:
        return {}

    async def _empty_verify(self, _: str, __: str) -> str:
        return "Verify operation unavailable"

    async def _empty_archive(self, _: str, __: str) -> str:
        return "Archive operation unavailable"

    async def _empty_discord_flag(self, _: str, __: bool) -> str:
        return "Discord flag operation unavailable"

    async def _empty_discord_profile(
        self,
        _: str,
        __: str | None,
        ___: str | None,
        ____: bool,
    ) -> str:
        return "Discord profile operation unavailable"

    async def _empty_streamer_analytics(self, _: str, __: int) -> dict[str, Any]:
        return {}

    async def _empty_comparison(self, _: int) -> dict[str, Any]:
        return {}

    async def _empty_session(self, _: int) -> dict[str, Any]:
        return {}

    async def _empty_raid_auth_url(self, _: str) -> str:
        return ""

    async def _empty_raid_go_url(self, _: str) -> str | None:
        return None

    async def _empty_raid_requirements(self, _: str) -> str:
        return "Raid requirements operation unavailable"

    async def _empty_raid_oauth_callback(
        self,
        *,
        code: str,
        state: str,
        error: str,
    ) -> dict[str, Any]:
        del code, state, error
        return {
            "status": 503,
            "title": "Raid-Bot nicht verfügbar",
            "body_html": "<p>Raid OAuth callback operation unavailable.</p>",
        }

    @property
    def base_path(self) -> str:
        return self._base_path

    def _is_authorized(self, request: web.Request) -> bool:
        presented = str(request.headers.get(INTERNAL_TOKEN_HEADER) or "").strip()
        if not self._token or not presented:
            return False
        try:
            return secrets.compare_digest(presented, self._token)
        except Exception:
            return False

    @staticmethod
    def _host_without_port(raw: str | None) -> str:
        value = str(raw or "").strip()
        if not value:
            return ""
        host = value.split(",", 1)[0].strip()
        if not host:
            return ""
        if host.startswith("["):
            end = host.find("]")
            if end != -1:
                host = host[1:end]
            return host.lower().rstrip(".")

        normalized = host.lower().rstrip(".")
        if not normalized:
            return ""

        # Keep raw IPv6 literals intact (e.g. "::1", "0:0:0:0:0:0:0:1").
        try:
            ip_address(normalized)
            return normalized
        except ValueError:
            pass

        # host:port for DNS names / IPv4.
        if normalized.count(":") == 1:
            host_part, port_part = normalized.rsplit(":", 1)
            if host_part and port_part.isdigit():
                return host_part
        return normalized

    @classmethod
    def _is_loopback_host(cls, raw: str | None) -> bool:
        host = cls._host_without_port(raw)
        if not host:
            return False
        if host == "localhost":
            return True
        try:
            return ip_address(host).is_loopback
        except ValueError:
            return False

    @classmethod
    def _is_loopback_origin(cls, raw_origin: str | None) -> bool:
        origin = str(raw_origin or "").strip()
        if not origin:
            return True
        try:
            parsed = urlsplit(origin)
        except Exception:
            return False
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return False
        if parsed.username or parsed.password:
            return False
        return cls._is_loopback_host(parsed.hostname)

    @staticmethod
    def _peer_host(request: web.Request) -> str:
        remote = str(getattr(request, "remote", "") or "").strip()
        if remote:
            return remote
        transport = getattr(request, "transport", None)
        if transport is None:
            return ""
        peer = transport.get_extra_info("peername")
        if isinstance(peer, tuple) and peer:
            return str(peer[0]).strip()
        if isinstance(peer, str):
            return peer.strip()
        return ""

    def _is_loopback_request(self, request: web.Request) -> bool:
        host_header = request.headers.get("Host") or request.host or ""
        if not self._is_loopback_host(host_header):
            return False

        if not self._is_loopback_origin(request.headers.get("Origin")):
            return False

        peer = self._peer_host(request)
        if not self._is_loopback_host(peer):
            return False

        return True

    @staticmethod
    def _canonical_json(value: Any) -> str:
        try:
            return json.dumps(
                value if value is not None else {},
                default=_json_default,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        except Exception:
            return "{}"

    @classmethod
    def _request_fingerprint(
        cls,
        *,
        request: web.Request,
        payload: dict[str, Any] | None,
    ) -> str:
        return "|".join(
            [
                str(request.method or "").upper().strip(),
                str(request.path_qs or request.path or "").strip(),
                cls._canonical_json(payload if isinstance(payload, dict) else {}),
            ]
        )

    def _cleanup_idempotency_cache(self) -> None:
        now = time_module.time()
        expired = [
            key
            for key, entry in self._idempotency_cache.items()
            if now - float(entry.get("created_at", 0.0)) > self._idempotency_ttl_seconds
        ]
        for key in expired:
            self._idempotency_cache.pop(key, None)

        overflow = len(self._idempotency_cache) - self._idempotency_max_entries
        if overflow > 0:
            oldest = sorted(
                self._idempotency_cache.items(),
                key=lambda kv: float(kv[1].get("created_at", 0.0)),
            )
            for key, _ in oldest[:overflow]:
                self._idempotency_cache.pop(key, None)

    def _cleanup_idempotency_inflight(self) -> None:
        now = time_module.time()
        expired: list[str] = []
        for key, entry in self._idempotency_inflight.items():
            if entry.future.done():
                expired.append(key)
                continue
            if now - float(entry.created_at) > self._idempotency_ttl_seconds:
                entry.future.set_result(
                    (
                        503,
                        {
                            "error": "upstream_unavailable",
                            "message": "idempotent request timed out",
                        },
                    )
                )
                expired.append(key)
        for key in expired:
            self._idempotency_inflight.pop(key, None)

    def _prepare_idempotency(
        self,
        *,
        request: web.Request,
        payload: dict[str, Any] | None,
    ) -> tuple[
        str,
        str,
        web.Response | None,
        asyncio.Future[tuple[int, Any]] | None,
        bool,
    ]:
        key = str(request.headers.get(IDEMPOTENCY_KEY_HEADER) or "").strip()
        if not key:
            return "", "", None, None, False
        if len(key) > 128:
            return (
                "",
                "",
                self._json_error("bad_request", 400, "invalid idempotency key"),
                None,
                False,
            )

        self._cleanup_idempotency_cache()
        self._cleanup_idempotency_inflight()
        fingerprint = self._request_fingerprint(request=request, payload=payload)
        entry = self._idempotency_cache.get(key)
        if entry:
            if str(entry.get("fingerprint") or "") != fingerprint:
                return (
                    "",
                    "",
                    self._json_error(
                        "idempotency_conflict",
                        409,
                        "idempotency key already used with a different request",
                    ),
                    None,
                    False,
                )

            replay_payload = entry.get("payload")
            status = int(entry.get("status", 200) or 200)
            response = self._json_response(replay_payload, status=status)
            response.headers["X-Idempotency-Replayed"] = "1"
            return "", "", response, None, False

        inflight = self._idempotency_inflight.get(key)
        if inflight is not None:
            if inflight.fingerprint != fingerprint:
                return (
                    "",
                    "",
                    self._json_error(
                        "idempotency_conflict",
                        409,
                        "idempotency key already used with a different request",
                    ),
                    None,
                    False,
                )
            return "", "", None, inflight.future, False

        future: asyncio.Future[tuple[int, Any]] = asyncio.get_running_loop().create_future()
        self._idempotency_inflight[key] = _IdempotencyInFlight(
            fingerprint=fingerprint,
            future=future,
            created_at=time_module.time(),
        )
        return key, fingerprint, None, None, True

    async def _wait_idempotency_result(
        self,
        *,
        future: asyncio.Future[tuple[int, Any]],
    ) -> web.Response:
        try:
            status, payload = await asyncio.wait_for(asyncio.shield(future), timeout=30.0)
        except asyncio.TimeoutError:
            return self._json_error(
                "upstream_unavailable",
                503,
                "idempotent request timed out",
            )
        except Exception:
            return self._json_error(
                "internal_error",
                500,
                "failed to resolve idempotent request",
            )
        response = self._json_response(payload, status=int(status or 200))
        response.headers["X-Idempotency-Replayed"] = "1"
        return response

    @staticmethod
    def _response_payload(response: web.Response) -> Any:
        try:
            raw = response.text if response.text is not None else ""
            if raw:
                return json.loads(raw)
        except Exception:
            pass
        return {}

    def _store_idempotency_result(
        self,
        *,
        key: str,
        fingerprint: str,
        status: int,
        payload: Any,
    ) -> None:
        if not key:
            return
        status_code = int(status or 200)
        if status_code >= 500:
            return
        self._cleanup_idempotency_cache()
        self._idempotency_cache[key] = {
            "fingerprint": str(fingerprint),
            "status": status_code,
            "payload": payload,
            "created_at": time_module.time(),
        }

    def _complete_idempotency_owner(
        self,
        *,
        key: str,
        fingerprint: str,
        response: web.Response,
        cacheable: bool,
    ) -> None:
        if not key:
            return
        status = int(getattr(response, "status", 500) or 500)
        payload = self._response_payload(response)
        if cacheable and status < 500:
            self._store_idempotency_result(
                key=key,
                fingerprint=fingerprint,
                status=status,
                payload=payload,
            )

        inflight = self._idempotency_inflight.get(key)
        if inflight is not None and inflight.fingerprint == fingerprint:
            if not inflight.future.done():
                inflight.future.set_result((status, payload))
            self._idempotency_inflight.pop(key, None)

    def _release_idempotency_owner(
        self,
        *,
        key: str,
        fingerprint: str,
        response: web.Response | None,
        cacheable: bool,
    ) -> None:
        if not key:
            return
        fallback_response = response
        if fallback_response is None:
            fallback_response = self._json_error(
                "internal_error",
                500,
                "idempotent request failed",
            )
        self._complete_idempotency_owner(
            key=key,
            fingerprint=fingerprint,
            response=fallback_response,
            cacheable=cacheable,
        )

    @staticmethod
    def _json_dumps(payload: Any) -> str:
        return json.dumps(payload, default=_json_default, ensure_ascii=False)

    def _json_response(self, payload: Any, *, status: int = 200) -> web.Response:
        return web.json_response(payload, status=status, dumps=self._json_dumps)

    def _json_error(self, error: str, status: int, message: str) -> web.Response:
        return self._json_response(
            {
                "error": error,
                "message": message,
            },
            status=status,
        )

    def _safe_bad_request(
        self,
        *,
        context: str,
        exc: Exception,
        message: str,
        code: str = "bad_request",
    ) -> web.Response:
        log.warning("internal api %s bad request: %s", context, exc)
        return self._json_error(code, 400, message)

    def _safe_exception_error(
        self,
        *,
        context: str,
        exc: Exception,
        error: str,
        status: int,
        message: str,
    ) -> web.Response:
        log.warning("internal api %s failed: %s", context, exc)
        return self._json_error(error, status, message)

    @staticmethod
    def _parse_allowlist_ids(raw: str | None, *, env_name: str) -> set[int] | None:
        # Env var unset => allowlist not configured (keep existing fail-open behavior).
        if raw is None:
            return None

        value = str(raw).strip()
        allowed: set[int] = set()
        for token in value.replace(";", ",").split(","):
            item = token.strip()
            if not item:
                continue
            if not item.isdigit():
                log.warning("Ignoring invalid %s entry: %r", env_name, item)
                continue
            parsed = int(item)
            if parsed > 0:
                allowed.add(parsed)
        if not allowed:
            log.warning(
                "%s configured but no valid positive IDs parsed; enabling fail-closed deny-all.",
                env_name,
            )
        return allowed

    @staticmethod
    def _coerce_optional_positive_int(value: Any, *, key: str) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            raise ValueError(f"{key} must be a positive integer")
        if isinstance(value, int):
            parsed = value
        elif isinstance(value, str):
            item = value.strip()
            if not item:
                return None
            if not item.isdigit():
                raise ValueError(f"{key} must be a positive integer")
            parsed = int(item)
        else:
            raise ValueError(f"{key} must be a positive integer")
        if parsed <= 0:
            raise ValueError(f"{key} must be a positive integer")
        return parsed

    def _enforce_scope_allowlist(
        self,
        *,
        payload: dict[str, Any],
        key: str,
        allowed: set[int] | None,
    ) -> None:
        if allowed is None:
            return
        value = self._coerce_optional_positive_int(payload.get(key), key=key)
        if value is None or value not in allowed:
            raise PermissionError(f"{key} is not allowed")

    def _enforce_discord_action_scope(self, payload: dict[str, Any]) -> None:
        self._enforce_scope_allowlist(
            payload=payload,
            key="guild_id",
            allowed=self._allowed_guild_ids,
        )
        self._enforce_scope_allowlist(
            payload=payload,
            key="channel_id",
            allowed=self._allowed_channel_ids,
        )
        self._enforce_scope_allowlist(
            payload=payload,
            key="role_id",
            allowed=self._allowed_role_ids,
        )

    @staticmethod
    def _parse_optional_int(value: str | None, *, minimum: int | None = None) -> int | None:
        raw = (value or "").strip()
        if not raw:
            return None
        try:
            parsed = int(raw)
        except ValueError:
            raise ValueError("invalid integer parameter")
        if minimum is not None and parsed < minimum:
            raise ValueError("integer parameter below minimum")
        return parsed

    @staticmethod
    def _normalize_login(raw: str) -> str | None:
        value = unquote(str(raw or "")).strip()
        if not value:
            return None
        if value.startswith("@"):
            value = value[1:].strip()
        if "://" in value or "twitch.tv" in value or "/" in value:
            candidate = value if "://" in value else f"https://{value}"
            try:
                parts = urlsplit(candidate)
            except Exception:
                return None
            segs = [seg for seg in (parts.path or "").split("/") if seg]
            if not segs:
                return None
            value = segs[0]
        value = value.lower().strip()
        if not _LOGIN_RE.fullmatch(value):
            return None
        return value

    @classmethod
    def _normalize_raid_auth_target(cls, raw: str) -> str | None:
        value = unquote(str(raw or "")).strip()
        if not value:
            return None

        lowered = value.lower()
        if lowered.startswith("discord:"):
            discord_id = lowered.split(":", 1)[1].strip()
            if discord_id.isdigit():
                return f"discord:{discord_id}"
            return None

        return cls._normalize_login(value)

    @staticmethod
    def _parse_bool(value: Any, *, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        lowered = str(value).strip().lower()
        if not lowered:
            return default
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
        return default

    async def _json_body(self, request: web.Request) -> dict[str, Any]:
        if not request.can_read_body:
            return {}
        try:
            body = await request.json()
        except Exception:
            raise ValueError("invalid json body")
        if body is None:
            return {}
        if not isinstance(body, dict):
            raise ValueError("json body must be an object")
        return body

    async def healthz(self, request: web.Request) -> web.Response:
        del request
        return self._json_response(
            {
                "ok": True,
                "service": "twitch-internal-api",
            }
        )

    async def streamers(self, request: web.Request) -> web.Response:
        del request
        try:
            items = await self._list()
            if not isinstance(items, list):
                items = list(items) if items else []
            return self._json_response(items)
        except Exception:
            log.exception("internal api streamers listing failed")
            return self._json_error("internal_error", 500, "failed to list streamers")

    async def streamer_add(self, request: web.Request) -> web.Response:
        owner_key = ""
        owner_fingerprint = ""
        owner_response: web.Response | None = None
        owner_cacheable = False
        try:
            body = await self._json_body(request)
            (
                idempotency_key,
                idempotency_fingerprint,
                replay,
                wait_future,
                is_owner,
            ) = self._prepare_idempotency(
                request=request,
                payload=body,
            )
            if replay is not None:
                return replay
            if wait_future is not None:
                return await self._wait_idempotency_result(future=wait_future)
            if is_owner:
                owner_key = idempotency_key
                owner_fingerprint = idempotency_fingerprint
            login = self._normalize_login(
                str(body.get("login") or body.get("streamer") or body.get("twitch_login") or "")
            )
            if not login:
                owner_response = self._json_error("bad_request", 400, "invalid or missing login")
                return owner_response
            require_link = self._parse_bool(body.get("require_link"), default=False)
            message = await self._add(login, require_link)
            response_payload = {
                "ok": True,
                "login": login,
                "message": str(message or "added"),
            }
            owner_cacheable = True
            owner_response = self._json_response(response_payload, status=201)
            return owner_response
        except ValueError as exc:
            owner_response = self._safe_bad_request(
                context="add streamer",
                exc=exc,
                message="invalid request body",
            )
            return owner_response
        except Exception:
            log.exception("internal api add streamer failed")
            owner_response = self._json_error("internal_error", 500, "failed to add streamer")
            return owner_response
        finally:
            self._release_idempotency_owner(
                key=owner_key,
                fingerprint=owner_fingerprint,
                response=owner_response,
                cacheable=owner_cacheable,
            )

    async def streamer_remove(self, request: web.Request) -> web.Response:
        (
            idempotency_key,
            idempotency_fingerprint,
            replay,
            wait_future,
            is_owner,
        ) = self._prepare_idempotency(
            request=request,
            payload=None,
        )
        owner_key = idempotency_key if is_owner else ""
        owner_fingerprint = idempotency_fingerprint if is_owner else ""
        owner_response: web.Response | None = None
        owner_cacheable = False
        if replay is not None:
            return replay
        if wait_future is not None:
            return await self._wait_idempotency_result(future=wait_future)
        try:
            raw_login = request.match_info.get("login", "")
            login = self._normalize_login(raw_login)
            if not login:
                owner_response = self._json_error("bad_request", 400, "invalid login")
                return owner_response
            message = await self._remove(login)
            response_payload = {"ok": True, "login": login, "message": str(message or "removed")}
            owner_cacheable = True
            owner_response = self._json_response(response_payload)
            return owner_response
        except Exception:
            log.exception("internal api remove streamer failed")
            owner_response = self._json_error("internal_error", 500, "failed to remove streamer")
            return owner_response
        finally:
            self._release_idempotency_owner(
                key=owner_key,
                fingerprint=owner_fingerprint,
                response=owner_response,
                cacheable=owner_cacheable,
            )

    async def streamer_verify(self, request: web.Request) -> web.Response:
        raw_login = request.match_info.get("login", "")
        login = self._normalize_login(raw_login)
        if not login:
            return self._json_error("bad_request", 400, "invalid login")
        owner_key = ""
        owner_fingerprint = ""
        owner_response: web.Response | None = None
        owner_cacheable = False
        try:
            body = await self._json_body(request)
            (
                idempotency_key,
                idempotency_fingerprint,
                replay,
                wait_future,
                is_owner,
            ) = self._prepare_idempotency(
                request=request,
                payload=body,
            )
            if replay is not None:
                return replay
            if wait_future is not None:
                return await self._wait_idempotency_result(future=wait_future)
            if is_owner:
                owner_key = idempotency_key
                owner_fingerprint = idempotency_fingerprint
            mode = str(body.get("mode") or "permanent").strip().lower()
            if not mode:
                mode = "permanent"
            message = await self._verify(login, mode)
            response_payload = {"ok": True, "login": login, "message": str(message or "verified")}
            owner_cacheable = True
            owner_response = self._json_response(response_payload)
            return owner_response
        except ValueError as exc:
            owner_response = self._safe_bad_request(
                context="verify streamer",
                exc=exc,
                message="invalid request body",
            )
            return owner_response
        except Exception:
            log.exception("internal api verify streamer failed")
            owner_response = self._json_error("internal_error", 500, "failed to verify streamer")
            return owner_response
        finally:
            self._release_idempotency_owner(
                key=owner_key,
                fingerprint=owner_fingerprint,
                response=owner_response,
                cacheable=owner_cacheable,
            )

    async def streamer_archive(self, request: web.Request) -> web.Response:
        raw_login = request.match_info.get("login", "")
        login = self._normalize_login(raw_login)
        if not login:
            return self._json_error("bad_request", 400, "invalid login")
        owner_key = ""
        owner_fingerprint = ""
        owner_response: web.Response | None = None
        owner_cacheable = False
        try:
            body = await self._json_body(request)
            (
                idempotency_key,
                idempotency_fingerprint,
                replay,
                wait_future,
                is_owner,
            ) = self._prepare_idempotency(
                request=request,
                payload=body,
            )
            if replay is not None:
                return replay
            if wait_future is not None:
                return await self._wait_idempotency_result(future=wait_future)
            if is_owner:
                owner_key = idempotency_key
                owner_fingerprint = idempotency_fingerprint
            mode = str(body.get("mode") or "toggle").strip().lower()
            if not mode:
                mode = "toggle"
            message = await self._archive(login, mode)
            response_payload = {"ok": True, "login": login, "message": str(message or "updated")}
            owner_cacheable = True
            owner_response = self._json_response(response_payload)
            return owner_response
        except ValueError as exc:
            owner_response = self._safe_bad_request(
                context="archive streamer",
                exc=exc,
                message="invalid request body",
            )
            return owner_response
        except Exception:
            log.exception("internal api archive streamer failed")
            owner_response = self._json_error("internal_error", 500, "failed to update archive state")
            return owner_response
        finally:
            self._release_idempotency_owner(
                key=owner_key,
                fingerprint=owner_fingerprint,
                response=owner_response,
                cacheable=owner_cacheable,
            )

    async def streamer_discord_flag(self, request: web.Request) -> web.Response:
        raw_login = request.match_info.get("login", "")
        login = self._normalize_login(raw_login)
        if not login:
            return self._json_error("bad_request", 400, "invalid login")
        owner_key = ""
        owner_fingerprint = ""
        owner_response: web.Response | None = None
        owner_cacheable = False
        try:
            body = await self._json_body(request)
            (
                idempotency_key,
                idempotency_fingerprint,
                replay,
                wait_future,
                is_owner,
            ) = self._prepare_idempotency(
                request=request,
                payload=body,
            )
            if replay is not None:
                return replay
            if wait_future is not None:
                return await self._wait_idempotency_result(future=wait_future)
            if is_owner:
                owner_key = idempotency_key
                owner_fingerprint = idempotency_fingerprint
            self._enforce_discord_action_scope(body)
            if "is_on_discord" not in body and "enabled" not in body and "value" not in body:
                owner_response = self._json_error("bad_request", 400, "is_on_discord is required")
                return owner_response
            enabled = self._parse_bool(
                body.get("is_on_discord", body.get("enabled", body.get("value"))),
                default=False,
            )
            message = await self._discord_flag(login, enabled)
            response_payload = {"ok": True, "login": login, "message": str(message or "updated")}
            owner_cacheable = True
            owner_response = self._json_response(response_payload)
            return owner_response
        except ValueError as exc:
            owner_response = self._safe_bad_request(
                context="discord flag",
                exc=exc,
                message="invalid request body",
            )
            return owner_response
        except PermissionError as exc:
            owner_response = self._safe_exception_error(
                context="discord flag scope",
                exc=exc,
                error="forbidden",
                status=403,
                message="action outside configured scope",
            )
            return owner_response
        except Exception:
            log.exception("internal api discord flag failed")
            owner_response = self._json_error("internal_error", 500, "failed to update discord flag")
            return owner_response
        finally:
            self._release_idempotency_owner(
                key=owner_key,
                fingerprint=owner_fingerprint,
                response=owner_response,
                cacheable=owner_cacheable,
            )

    async def streamer_discord_profile(self, request: web.Request) -> web.Response:
        raw_login = request.match_info.get("login", "")
        login = self._normalize_login(raw_login)
        if not login:
            return self._json_error("bad_request", 400, "invalid login")
        owner_key = ""
        owner_fingerprint = ""
        owner_response: web.Response | None = None
        owner_cacheable = False
        try:
            body = await self._json_body(request)
            (
                idempotency_key,
                idempotency_fingerprint,
                replay,
                wait_future,
                is_owner,
            ) = self._prepare_idempotency(
                request=request,
                payload=body,
            )
            if replay is not None:
                return replay
            if wait_future is not None:
                return await self._wait_idempotency_result(future=wait_future)
            if is_owner:
                owner_key = idempotency_key
                owner_fingerprint = idempotency_fingerprint
            self._enforce_discord_action_scope(body)
            discord_user_id = body.get("discord_user_id")
            if discord_user_id is not None:
                discord_user_id = str(discord_user_id).strip() or None
            discord_display_name = body.get("discord_display_name")
            if discord_display_name is not None:
                discord_display_name = str(discord_display_name).strip() or None
            mark_member = self._parse_bool(
                body.get("mark_member", body.get("member_flag")),
                default=True,
            )
            message = await self._discord_profile(
                login,
                discord_user_id=discord_user_id,
                discord_display_name=discord_display_name,
                mark_member=mark_member,
            )
            response_payload = {"ok": True, "login": login, "message": str(message or "updated")}
            owner_cacheable = True
            owner_response = self._json_response(response_payload)
            return owner_response
        except ValueError as exc:
            owner_response = self._safe_bad_request(
                context="discord profile",
                exc=exc,
                message="invalid request body",
            )
            return owner_response
        except PermissionError as exc:
            owner_response = self._safe_exception_error(
                context="discord profile scope",
                exc=exc,
                error="forbidden",
                status=403,
                message="action outside configured scope",
            )
            return owner_response
        except Exception:
            log.exception("internal api discord profile failed")
            owner_response = self._json_error("internal_error", 500, "failed to update discord profile")
            return owner_response
        finally:
            self._release_idempotency_owner(
                key=owner_key,
                fingerprint=owner_fingerprint,
                response=owner_response,
                cacheable=owner_cacheable,
            )

    async def stats(self, request: web.Request) -> web.Response:
        try:
            hour_from = self._parse_optional_int(request.query.get("hour_from"), minimum=0)
            hour_to = self._parse_optional_int(request.query.get("hour_to"), minimum=0)
            streamer_raw = str(request.query.get("streamer") or "").strip()
            streamer = None
            if streamer_raw:
                streamer = self._normalize_login(streamer_raw)
                if streamer is None:
                    return self._json_error("bad_request", 400, "invalid streamer login")
            payload = await self._stats(hour_from=hour_from, hour_to=hour_to, streamer=streamer)
            return self._json_response(payload if isinstance(payload, dict) else {})
        except ValueError as exc:
            return self._safe_bad_request(
                context="stats query",
                exc=exc,
                message="invalid query parameters",
            )
        except Exception:
            log.exception("internal api stats failed")
            return self._json_error("internal_error", 500, "failed to fetch stats")

    async def streamer_analytics(self, request: web.Request) -> web.Response:
        raw_login = request.match_info.get("login", "")
        login = self._normalize_login(raw_login)
        if not login:
            return self._json_error("bad_request", 400, "invalid login")
        try:
            days = self._parse_optional_int(request.query.get("days"), minimum=1) or 30
            payload = await self._streamer_analytics(login, int(days))
            return self._json_response(payload if isinstance(payload, dict) else {})
        except ValueError as exc:
            return self._safe_bad_request(
                context="streamer analytics query",
                exc=exc,
                message="invalid query parameters",
            )
        except Exception:
            log.exception("internal api streamer analytics failed")
            return self._json_error("internal_error", 500, "failed to fetch streamer analytics")

    async def analytics_comparison(self, request: web.Request) -> web.Response:
        try:
            days = self._parse_optional_int(request.query.get("days"), minimum=1) or 30
            payload = await self._comparison(int(days))
            return self._json_response(payload if isinstance(payload, dict) else {})
        except ValueError as exc:
            return self._safe_bad_request(
                context="comparison analytics query",
                exc=exc,
                message="invalid query parameters",
            )
        except Exception:
            log.exception("internal api comparison analytics failed")
            return self._json_error("internal_error", 500, "failed to fetch comparison analytics")

    async def session_detail(self, request: web.Request) -> web.Response:
        raw_session_id = request.match_info.get("session_id", "")
        try:
            session_id = int(str(raw_session_id).strip())
        except ValueError:
            return self._json_error("bad_request", 400, "invalid session id")
        try:
            payload = await self._session(session_id)
            if isinstance(payload, dict) and not payload:
                return self._json_error("not_found", 404, "session not found")
            return self._json_response(payload if isinstance(payload, dict) else {})
        except ValueError as exc:
            return self._safe_bad_request(
                context="session detail",
                exc=exc,
                message="invalid request parameters",
            )
        except Exception:
            log.exception("internal api session detail failed")
            return self._json_error("internal_error", 500, "failed to fetch session detail")

    async def raid_auth_url(self, request: web.Request) -> web.Response:
        login = self._normalize_raid_auth_target(request.query.get("login", ""))
        if not login:
            return self._json_error("bad_request", 400, "invalid or missing login")
        try:
            auth_url = str(await self._raid_auth_url(login)).strip()
            if not auth_url:
                return self._json_error("upstream_unavailable", 503, "raid bot not initialized")
            return self._json_response({"ok": True, "auth_url": auth_url, "login": login})
        except ValueError as exc:
            return self._safe_bad_request(
                context="raid auth url",
                exc=exc,
                message="invalid request parameters",
            )
        except LookupError as exc:
            return self._safe_exception_error(
                context="raid auth url not found",
                exc=exc,
                error="not_found",
                status=404,
                message="resource not found",
            )
        except PermissionError as exc:
            return self._safe_exception_error(
                context="raid auth url forbidden",
                exc=exc,
                error="forbidden",
                status=403,
                message="forbidden",
            )
        except RuntimeError as exc:
            return self._safe_exception_error(
                context="raid auth url runtime",
                exc=exc,
                error="upstream_unavailable",
                status=503,
                message="upstream unavailable",
            )
        except Exception:
            log.exception("internal api raid auth url failed")
            return self._json_error("internal_error", 500, "failed to generate raid auth url")

    async def raid_go_url(self, request: web.Request) -> web.Response:
        state = str(request.query.get("state") or "").strip()
        if not state:
            return self._json_error("bad_request", 400, "missing state parameter")
        try:
            auth_url = await self._raid_go_url(state)
            auth_url_str = str(auth_url or "").strip()
            if not auth_url_str:
                return self._json_error("not_found", 404, "state not found or expired")
            return self._json_response({"ok": True, "auth_url": auth_url_str})
        except ValueError as exc:
            return self._safe_bad_request(
                context="raid go url",
                exc=exc,
                message="invalid request parameters",
            )
        except RuntimeError as exc:
            return self._safe_exception_error(
                context="raid go url runtime",
                exc=exc,
                error="upstream_unavailable",
                status=503,
                message="upstream unavailable",
            )
        except Exception:
            log.exception("internal api raid go url failed")
            return self._json_error("internal_error", 500, "failed to resolve raid auth url")

    async def raid_requirements(self, request: web.Request) -> web.Response:
        owner_key = ""
        owner_fingerprint = ""
        owner_response: web.Response | None = None
        owner_cacheable = False
        try:
            body = await self._json_body(request)
            (
                idempotency_key,
                idempotency_fingerprint,
                replay,
                wait_future,
                is_owner,
            ) = self._prepare_idempotency(
                request=request,
                payload=body,
            )
            if replay is not None:
                return replay
            if wait_future is not None:
                return await self._wait_idempotency_result(future=wait_future)
            if is_owner:
                owner_key = idempotency_key
                owner_fingerprint = idempotency_fingerprint
            self._enforce_discord_action_scope(body)
            login = self._normalize_login(
                str(body.get("login") or body.get("streamer") or body.get("twitch_login") or "")
            )
            if not login:
                owner_response = self._json_error("bad_request", 400, "invalid or missing login")
                return owner_response
            message = await self._raid_requirements(login)
            response_payload = {"ok": True, "login": login, "message": str(message or "sent")}
            owner_cacheable = True
            owner_response = self._json_response(response_payload)
            return owner_response
        except ValueError as exc:
            owner_response = self._safe_bad_request(
                context="raid requirements",
                exc=exc,
                message="invalid request body",
            )
            return owner_response
        except PermissionError as exc:
            owner_response = self._safe_exception_error(
                context="raid requirements forbidden",
                exc=exc,
                error="forbidden",
                status=403,
                message="action outside configured scope",
            )
            return owner_response
        except LookupError as exc:
            owner_response = self._safe_exception_error(
                context="raid requirements not found",
                exc=exc,
                error="not_found",
                status=404,
                message="resource not found",
            )
            return owner_response
        except RuntimeError as exc:
            owner_response = self._safe_exception_error(
                context="raid requirements runtime",
                exc=exc,
                error="upstream_unavailable",
                status=503,
                message="upstream unavailable",
            )
            return owner_response
        except Exception:
            log.exception("internal api raid requirements failed")
            owner_response = self._json_error("internal_error", 500, "failed to send raid requirements")
            return owner_response
        finally:
            self._release_idempotency_owner(
                key=owner_key,
                fingerprint=owner_fingerprint,
                response=owner_response,
                cacheable=owner_cacheable,
            )

    async def raid_oauth_callback(self, request: web.Request) -> web.Response:
        owner_key = ""
        owner_fingerprint = ""
        owner_response: web.Response | None = None
        owner_cacheable = False
        try:
            body = await self._json_body(request)
            (
                idempotency_key,
                idempotency_fingerprint,
                replay,
                wait_future,
                is_owner,
            ) = self._prepare_idempotency(
                request=request,
                payload=body,
            )
            if replay is not None:
                return replay
            if wait_future is not None:
                return await self._wait_idempotency_result(future=wait_future)
            if is_owner:
                owner_key = idempotency_key
                owner_fingerprint = idempotency_fingerprint
            self._enforce_discord_action_scope(body)
            result = await self._raid_oauth_callback(
                code=str(body.get("code") or ""),
                state=str(body.get("state") or ""),
                error=str(body.get("error") or ""),
            )
            if not isinstance(result, dict):
                result = {
                    "status": 500,
                    "title": "Autorisierung fehlgeschlagen",
                    "body_html": "<p>Ungültige Antwort vom Raid OAuth Callback.</p>",
                }
            status = result.get("status", 200)
            try:
                status_code = int(status)
            except (TypeError, ValueError):
                status_code = 200
            status_code = max(200, min(status_code, 599))
            result["status"] = status_code
            owner_cacheable = True
            owner_response = self._json_response(result)
            return owner_response
        except ValueError as exc:
            owner_response = self._safe_bad_request(
                context="raid oauth callback",
                exc=exc,
                message="invalid request body",
            )
            return owner_response
        except PermissionError as exc:
            owner_response = self._safe_exception_error(
                context="raid oauth callback forbidden",
                exc=exc,
                error="forbidden",
                status=403,
                message="action outside configured scope",
            )
            return owner_response
        except Exception:
            log.exception("internal api raid oauth callback failed")
            owner_response = self._json_error("internal_error", 500, "failed to process raid oauth callback")
            return owner_response
        finally:
            self._release_idempotency_owner(
                key=owner_key,
                fingerprint=owner_fingerprint,
                response=owner_response,
                cacheable=owner_cacheable,
            )

    def attach(self, app: web.Application) -> None:
        base = self._base_path
        app.add_routes(
            [
                web.get(f"{base}/healthz", self.healthz),
                web.get(f"{base}/streamers", self.streamers),
                web.post(f"{base}/streamers", self.streamer_add),
                web.delete(f"{base}/streamers/{{login}}", self.streamer_remove),
                web.post(f"{base}/streamers/{{login}}/verify", self.streamer_verify),
                web.post(f"{base}/streamers/{{login}}/archive", self.streamer_archive),
                web.post(f"{base}/streamers/{{login}}/discord-flag", self.streamer_discord_flag),
                web.post(
                    f"{base}/streamers/{{login}}/discord-profile",
                    self.streamer_discord_profile,
                ),
                web.get(f"{base}/stats", self.stats),
                web.get(
                    f"{base}/analytics/streamer/{{login}}",
                    self.streamer_analytics,
                ),
                web.get(f"{base}/analytics/comparison", self.analytics_comparison),
                web.get(f"{base}/sessions/{{session_id}}", self.session_detail),
                web.get(f"{base}/raid/auth-url", self.raid_auth_url),
                web.get(f"{base}/raid/go-url", self.raid_go_url),
                web.post(f"{base}/raid/requirements", self.raid_requirements),
                web.post(f"{base}/raid/oauth-callback", self.raid_oauth_callback),
            ]
        )


def build_internal_api_app(
    *,
    token: str | None,
    base_path: str = INTERNAL_API_BASE_PATH,
    add_cb: Callable[[str, bool], Awaitable[str]] | None = None,
    remove_cb: Callable[[str], Awaitable[str]] | None = None,
    list_cb: Callable[[], Awaitable[list[dict[str, Any]]]] | None = None,
    stats_cb: Callable[..., Awaitable[dict[str, Any]]] | None = None,
    verify_cb: Callable[[str, str], Awaitable[str]] | None = None,
    archive_cb: Callable[[str, str], Awaitable[str]] | None = None,
    discord_flag_cb: Callable[[str, bool], Awaitable[str]] | None = None,
    discord_profile_cb: Callable[..., Awaitable[str]] | None = None,
    streamer_analytics_cb: Callable[[str, int], Awaitable[dict[str, Any]]] | None = None,
    comparison_cb: Callable[[int], Awaitable[dict[str, Any]]] | None = None,
    session_cb: Callable[[int], Awaitable[dict[str, Any]]] | None = None,
    raid_auth_url_cb: Callable[[str], Awaitable[str]] | None = None,
    raid_go_url_cb: Callable[[str], Awaitable[str | None]] | None = None,
    raid_requirements_cb: Callable[[str], Awaitable[str]] | None = None,
    raid_oauth_callback_cb: Callable[..., Awaitable[dict[str, Any]]] | None = None,
) -> web.Application:
    server = InternalApiServer(
        token=token,
        base_path=base_path,
        add_cb=add_cb,
        remove_cb=remove_cb,
        list_cb=list_cb,
        stats_cb=stats_cb,
        verify_cb=verify_cb,
        archive_cb=archive_cb,
        discord_flag_cb=discord_flag_cb,
        discord_profile_cb=discord_profile_cb,
        streamer_analytics_cb=streamer_analytics_cb,
        comparison_cb=comparison_cb,
        session_cb=session_cb,
        raid_auth_url_cb=raid_auth_url_cb,
        raid_go_url_cb=raid_go_url_cb,
        raid_requirements_cb=raid_requirements_cb,
        raid_oauth_callback_cb=raid_oauth_callback_cb,
    )

    @web.middleware
    async def _loopback_middleware(request: web.Request, handler: Any) -> web.StreamResponse:
        if not server._is_loopback_request(request):
            return server._json_error(
                error="forbidden",
                status=403,
                message="internal API accepts loopback traffic only",
            )
        return await handler(request)

    @web.middleware
    async def _auth_middleware(request: web.Request, handler: Any) -> web.StreamResponse:
        if not server._is_authorized(request):
            return server._json_error(
                error="unauthorized",
                status=401,
                message="missing or invalid internal token",
            )
        return await handler(request)

    app = web.Application(middlewares=[_loopback_middleware, _auth_middleware])
    server.attach(app)
    return app


__all__ = [
    "INTERNAL_API_BASE_PATH",
    "IDEMPOTENCY_KEY_HEADER",
    "INTERNAL_TOKEN_HEADER",
    "InternalApiServer",
    "build_internal_api_app",
]
