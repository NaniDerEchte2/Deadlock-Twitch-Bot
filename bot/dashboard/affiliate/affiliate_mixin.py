"""Affiliate system mixin for DashboardV2Server - OAuth, signup, Stripe Connect, claims, commissions."""

from __future__ import annotations

import contextlib
import re
import secrets
import time
import zlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import aiohttp
from aiohttp import web

from ... import storage
from ...core.constants import log
from ...storage import sessions_db

TWITCH_OAUTH_AUTHORIZE_URL = "https://id.twitch.tv/oauth2/authorize"
TWITCH_OAUTH_TOKEN_URL = "https://id.twitch.tv/oauth2/token"  # noqa: S105
TWITCH_HELIX_USERS_URL = "https://api.twitch.tv/helix/users"
STRIPE_CONNECT_AUTHORIZE_URL = "https://connect.stripe.com/oauth/authorize"
STRIPE_CONNECT_TOKEN_URL = "https://connect.stripe.com/oauth/token"  # noqa: S105

_LOGIN_RE = re.compile(r"^[A-Za-z0-9_]{3,25}$")
_AFFILIATE_SESSION_TTL = 7 * 24 * 3600  # 7 days
_AFFILIATE_COOKIE = "twitch_affiliate_session"
_COMMISSION_RATE = 0.30
_MAX_PENDING_COMMISSION_CENTS = 5000
_AFFILIATE_COMMISSION_LOCK_NAMESPACE = 1_103_151_689


class _DashboardAffiliateMixin:
    """Affiliate portal: Twitch OAuth, signup, Stripe Connect, claims, commissions."""

    # ------------------------------------------------------------------ #
    # Table setup                                                          #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _affiliate_ensure_tables(conn: Any) -> None:
        schema_path = Path(__file__).resolve().parents[2] / "migrations" / "affiliate_schema.sql"
        sql = schema_path.read_text(encoding="utf-8")
        conn.executescript(sql)
        conn.commit()

    # ------------------------------------------------------------------ #
    # Session helpers                                                      #
    # ------------------------------------------------------------------ #

    def _get_affiliate_session(self, request: web.Request) -> dict[str, Any] | None:
        if not hasattr(self, "_affiliate_sessions_loaded"):
            self._affiliate_sessions_loaded = True
            try:
                for sid, data in sessions_db.load_valid_sessions("affiliate", time.time()):
                    if not hasattr(self, "_affiliate_sessions"):
                        self._affiliate_sessions = {}
                    self._affiliate_sessions[sid] = data
            except Exception as exc:
                log.debug("Could not load affiliate sessions from DB: %s", exc)

        sessions = getattr(self, "_affiliate_sessions", {})
        session_id = (request.cookies.get(_AFFILIATE_COOKIE) or "").strip()
        if not session_id:
            return None
        session = sessions.get(session_id)
        if not session:
            return None
        now = time.time()
        if float(session.get("expires_at", 0.0)) <= now:
            sessions.pop(session_id, None)
            try:
                sessions_db.delete_session(session_id)
            except Exception:
                pass
            return None
        return session

    def _create_affiliate_session(
        self, *, twitch_login: str, twitch_user_id: str, display_name: str, email: str = "",
    ) -> str:
        if not hasattr(self, "_affiliate_sessions"):
            self._affiliate_sessions = {}
        session_id = secrets.token_urlsafe(32)
        now = time.time()
        session_data = {
            "twitch_login": twitch_login,
            "twitch_user_id": twitch_user_id,
            "display_name": display_name or twitch_login,
            "email": email,
            "created_at": now,
            "expires_at": now + _AFFILIATE_SESSION_TTL,
        }
        self._affiliate_sessions[session_id] = session_data
        try:
            sessions_db.upsert_session(
                session_id, "affiliate", session_data, now, now + _AFFILIATE_SESSION_TTL
            )
        except Exception as exc:
            log.debug("Could not persist affiliate session to DB: %s", exc)
        return session_id

    def _set_affiliate_cookie(
        self, response: web.StreamResponse, request: web.Request, session_id: str
    ) -> None:
        response.set_cookie(
            _AFFILIATE_COOKIE,
            session_id,
            max_age=_AFFILIATE_SESSION_TTL,
            httponly=True,
            secure=self._is_secure_request(request),
            samesite="Lax",
            path="/",
        )

    @staticmethod
    def _affiliate_import_stripe() -> tuple[Any | None, str | None]:
        try:
            import stripe
        except Exception as exc:
            return None, str(exc)
        return stripe, None

    @staticmethod
    def _affiliate_commission_lock_key(affiliate_login: str) -> tuple[int, int]:
        normalized = str(affiliate_login or "").strip().lower().encode("utf-8")
        lock_key = zlib.crc32(normalized)
        if lock_key >= 2**31:
            lock_key -= 2**32
        return _AFFILIATE_COMMISSION_LOCK_NAMESPACE, lock_key

    @contextlib.contextmanager
    def _affiliate_commission_lock(self, conn: Any, affiliate_login: str):
        transaction_factory = getattr(conn, "transaction", None)
        if not affiliate_login or not callable(transaction_factory):
            yield
            return

        namespace, lock_key = self._affiliate_commission_lock_key(affiliate_login)
        conn.execute("SELECT pg_advisory_lock(?, ?)", (namespace, lock_key))
        try:
            yield
        finally:
            conn.execute("SELECT pg_advisory_unlock(?, ?)", (namespace, lock_key))

    @contextlib.contextmanager
    def _affiliate_db_transaction(self, conn: Any):
        transaction_factory = getattr(conn, "transaction", None)
        if callable(transaction_factory):
            with transaction_factory():
                yield
            return

        raw_conn = getattr(conn, "_conn", conn)
        began_transaction = raw_conn.__class__.__module__.startswith("sqlite3")
        if began_transaction:
            conn.execute("BEGIN IMMEDIATE")

        try:
            yield
        except Exception:
            if began_transaction:
                rollback = getattr(conn, "rollback", None)
                if callable(rollback):
                    rollback()
            raise
        else:
            if began_transaction:
                commit = getattr(conn, "commit", None)
                if callable(commit):
                    commit()

    def _affiliate_replay_pending_commissions(
        self,
        conn: Any,
        *,
        stripe: Any | None,
        stripe_account_id: str,
        affiliate_login: str,
        error_message: str | None = None,
        commit: bool = True,
    ) -> None:
        pending_result = conn.execute(
            """SELECT id, stripe_event_id, commission_cents, currency
               FROM affiliate_commissions
               WHERE affiliate_twitch_login = ?
                 AND status IN ('pending', 'failed')
                 AND stripe_transfer_id IS NULL
                 AND transferred_at IS NULL
               ORDER BY created_at ASC, id ASC""",
            (affiliate_login,),
        )
        pending_rows = pending_result.fetchall() if pending_result else []

        for row in pending_rows:
            self._affiliate_transfer_commission(
                conn,
                stripe=stripe,
                stripe_account_id=stripe_account_id,
                commission_id=int(row["id"]),
                stripe_event_id=str(row["stripe_event_id"] or ""),
                commission_cents=int(row["commission_cents"] or 0),
                currency=str(row["currency"] or "eur"),
                error_message=error_message,
                commit=commit,
            )

    def _affiliate_transfer_commission(
        self,
        conn: Any,
        *,
        stripe: Any | None,
        stripe_account_id: str,
        commission_id: int,
        stripe_event_id: str,
        commission_cents: int,
        currency: str,
        error_message: str | None = None,
        commit: bool = True,
    ) -> str:
        idempotency_key = f"affiliate-transfer:{int(commission_id)}"

        if error_message:
            conn.execute(
                """UPDATE affiliate_commissions
                   SET status = 'pending', stripe_transfer_id = NULL, transferred_at = NULL,
                       error_message = ?
                   WHERE id = ?""",
                (str(error_message)[:500], commission_id),
            )
            if commit:
                conn.commit()
            return "pending"

        try:
            transfer = stripe.Transfer.create(
                amount=commission_cents,
                currency=currency,
                destination=stripe_account_id,
                transfer_group=stripe_event_id,
                idempotency_key=idempotency_key,
            )
        except Exception as exc:
            log.warning("Affiliate Stripe transfer failed for commission %s: %s", commission_id, exc)
            conn.execute(
                """UPDATE affiliate_commissions
                   SET status = 'pending', stripe_transfer_id = NULL, transferred_at = NULL,
                       error_message = ?
                   WHERE id = ?""",
                (str(exc)[:500], commission_id),
            )
            if commit:
                conn.commit()
            return "pending"

        conn.execute(
            """UPDATE affiliate_commissions
               SET status = 'transferred', stripe_transfer_id = ?, transferred_at = ?,
                   error_message = NULL
               WHERE id = ?""",
            (transfer.id, datetime.now(UTC).isoformat(), commission_id),
        )
        if commit:
            conn.commit()
        return "transferred"

    # ------------------------------------------------------------------ #
    # Twitch OAuth (affiliate-specific)                                    #
    # ------------------------------------------------------------------ #

    def _affiliate_build_redirect_uri(self) -> str:
        configured = self._load_secret_value(
            "TWITCH_AFFILIATE_AUTH_REDIRECT_URI",
        )
        if configured:
            return configured
        public_url = getattr(self, "_public_url", "") or ""
        if not public_url:
            public_url = "https://twitch.earlysalty.com"
        return f"{public_url.rstrip('/')}/twitch/auth/affiliate/callback"

    def _affiliate_build_stripe_connect_redirect_uri(self) -> str:
        public_url = getattr(self, "_public_url", "") or ""
        if not public_url:
            public_url = "https://twitch.earlysalty.com"
        return f"{public_url.rstrip('/')}/twitch/affiliate/connect/stripe/callback"

    def _affiliate_stripe_connect_client_id(self) -> str:
        configured = str(getattr(self, "_stripe_connect_client_id", "") or "").strip()
        if configured:
            return configured
        return str(self._load_secret_value("STRIPE_CONNECT_CLIENT_ID") or "").strip()

    async def _affiliate_auth_login(self, request: web.Request) -> web.StreamResponse:
        if not self._is_oauth_configured():
            return web.Response(text="OAuth ist nicht konfiguriert.", status=503)

        existing = self._get_affiliate_session(request)
        if existing:
            raise web.HTTPFound("/twitch/affiliate/portal")

        if not hasattr(self, "_affiliate_oauth_states"):
            self._affiliate_oauth_states = {}

        redirect_uri = self._affiliate_build_redirect_uri()
        state = secrets.token_urlsafe(24)
        self._affiliate_oauth_states[state] = {
            "created_at": time.time(),
            "redirect_uri": redirect_uri,
        }
        params = urlencode({
            "client_id": self._oauth_client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": "user:read:email",
            "state": state,
        })
        raise web.HTTPFound(f"{TWITCH_OAUTH_AUTHORIZE_URL}?{params}")

    async def _affiliate_auth_callback(self, request: web.Request) -> web.StreamResponse:
        if not self._is_oauth_configured():
            return web.Response(text="OAuth ist nicht konfiguriert.", status=503)

        error = (request.query.get("error") or "").strip()
        if error:
            return web.Response(text=f"OAuth-Fehler: {error}", status=401)

        state = (request.query.get("state") or "").strip()
        code = (request.query.get("code") or "").strip()
        if not state or not code:
            return web.Response(text="Fehlender OAuth state/code.", status=400)

        states = getattr(self, "_affiliate_oauth_states", {})
        state_data = states.pop(state, None)
        if not state_data:
            return web.Response(text="OAuth state ungueltig oder abgelaufen.", status=400)
        if time.time() - float(state_data.get("created_at", 0)) > 600:
            return web.Response(text="OAuth state abgelaufen.", status=400)

        redirect_uri = str(state_data.get("redirect_uri") or "")
        user = await self._affiliate_exchange_code(code, redirect_uri)
        if not user:
            return web.Response(text="OAuth-Austausch fehlgeschlagen.", status=401)

        twitch_login = user["twitch_login"]
        twitch_user_id = user["twitch_user_id"]
        display_name = user.get("display_name", twitch_login)
        email = user.get("email", "")

        session_id = self._create_affiliate_session(
            twitch_login=twitch_login,
            twitch_user_id=twitch_user_id,
            display_name=display_name,
            email=email,
        )

        now = datetime.now(UTC).isoformat()
        with storage.get_conn() as conn:
            self._affiliate_ensure_tables(conn)
            result = conn.execute(
                "SELECT twitch_login FROM affiliate_accounts WHERE twitch_login = ?",
                (twitch_login,),
            )
            row = result.fetchone() if result else None
            if not row:
                try:
                    conn.execute(
                        """INSERT INTO affiliate_accounts
                           (twitch_login, twitch_user_id, display_name, email, full_name,
                            address_line1, address_city, address_zip, address_country,
                            created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'DE', ?, ?)""",
                        (
                            twitch_login,
                            twitch_user_id,
                            display_name,
                            email,
                            "",
                            "",
                            "",
                            "",
                            now,
                            now,
                        ),
                    )
                    conn.commit()
                except Exception as _dup_exc:
                    if "unique" not in str(_dup_exc).lower() and "duplicate" not in str(_dup_exc).lower():
                        raise

        destination = "/twitch/affiliate/portal"

        response = web.HTTPFound(destination)
        self._set_affiliate_cookie(response, request, session_id)
        raise response

    async def _affiliate_exchange_code(
        self, code: str, redirect_uri: str
    ) -> dict[str, str] | None:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                TWITCH_OAUTH_TOKEN_URL,
                data={
                    "client_id": self._oauth_client_id,
                    "client_secret": self._oauth_client_secret,
                    "code": code,
                    "grant_type": "authorization_code",
                    "redirect_uri": redirect_uri,
                },
            ) as token_resp:
                if token_resp.status != 200:
                    log.warning("Affiliate OAuth exchange failed: %s", token_resp.status)
                    return None
                token_data = await token_resp.json()

            access_token = str(token_data.get("access_token") or "").strip()
            if not access_token:
                return None

            async with session.get(
                TWITCH_HELIX_USERS_URL,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Client-Id": str(self._oauth_client_id),
                },
            ) as user_resp:
                if user_resp.status != 200:
                    return None
                user_data = await user_resp.json()

        users = user_data.get("data") if isinstance(user_data, dict) else None
        if not isinstance(users, list) or not users:
            return None
        u = users[0] or {}
        return {
            "twitch_login": str(u.get("login") or "").strip().lower(),
            "twitch_user_id": str(u.get("id") or "").strip(),
            "display_name": str(u.get("display_name") or u.get("login") or "").strip(),
            "email": str(u.get("email") or "").strip(),
        }

    # ------------------------------------------------------------------ #
    # Signup routes                                                        #
    # ------------------------------------------------------------------ #

    async def _affiliate_signup_page(self, request: web.Request) -> web.StreamResponse:
        session = self._get_affiliate_session(request)
        if not session:
            raise web.HTTPFound("/twitch/auth/affiliate/login")
        raise web.HTTPFound("/twitch/affiliate/portal")

    async def _affiliate_signup_complete(self, request: web.Request) -> web.StreamResponse:
        session = self._get_affiliate_session(request)
        if not session:
            raise web.HTTPFound("/twitch/auth/affiliate/login")
        raise web.HTTPFound("/twitch/affiliate/portal")

    # ------------------------------------------------------------------ #
    # Stripe Connect                                                       #
    # ------------------------------------------------------------------ #

    async def _affiliate_connect_stripe(self, request: web.Request) -> web.StreamResponse:
        session = self._get_affiliate_session(request)
        if not session:
            raise web.HTTPFound("/twitch/auth/affiliate/login")

        stripe_connect_client_id = self._affiliate_stripe_connect_client_id()
        if not stripe_connect_client_id:
            return web.Response(text="Stripe Connect ist nicht konfiguriert.", status=503)

        if not hasattr(self, "_affiliate_connect_states"):
            self._affiliate_connect_states = {}

        state = secrets.token_urlsafe(24)
        redirect_uri = self._affiliate_build_stripe_connect_redirect_uri()
        self._affiliate_connect_states[state] = {
            "created_at": time.time(),
            "redirect_uri": redirect_uri,
            "twitch_login": session.get("twitch_login", ""),
        }

        params = urlencode({
            "response_type": "code",
            "client_id": stripe_connect_client_id,
            "redirect_uri": redirect_uri,
            "scope": "read_write",
            "state": state,
        })
        raise web.HTTPFound(f"{STRIPE_CONNECT_AUTHORIZE_URL}?{params}")

    async def _affiliate_connect_stripe_callback(self, request: web.Request) -> web.StreamResponse:
        session = self._get_affiliate_session(request)
        if not session:
            raise web.HTTPFound("/twitch/auth/affiliate/login")

        state = (request.query.get("state") or "").strip()
        code = (request.query.get("code") or "").strip()
        if not state or not code:
            return web.Response(text="Fehlender state/code.", status=400)

        states = getattr(self, "_affiliate_connect_states", {})
        state_data = states.pop(state, None)
        if not state_data:
            return web.Response(text="State ungueltig oder abgelaufen.", status=400)
        if time.time() - float(state_data.get("created_at", 0)) > 600:
            return web.Response(text="State abgelaufen.", status=400)
        session_login = str(session.get("twitch_login") or "").strip().lower()
        state_login = str(state_data.get("twitch_login") or "").strip().lower()
        if not state_login or session_login != state_login:
            return web.Response(
                text="Affiliate-Session passt nicht zum Stripe Connect state.",
                status=403,
            )
        redirect_uri = str(state_data.get("redirect_uri") or "").strip()

        stripe_secret_key = self._load_secret_value(
            "STRIPE_SECRET_KEY", "TWITCH_BILLING_STRIPE_SECRET_KEY"
        )
        if not stripe_secret_key:
            return web.Response(text="Stripe ist nicht konfiguriert.", status=503)

        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as http_session:
            async with http_session.post(
                STRIPE_CONNECT_TOKEN_URL,
                data={
                    "client_secret": stripe_secret_key,
                    "code": code,
                    "grant_type": "authorization_code",
                    "redirect_uri": redirect_uri,
                },
            ) as resp:
                if resp.status != 200:
                    log.warning("Stripe Connect token exchange failed: %s", resp.status)
                    return web.Response(text="Stripe Connect fehlgeschlagen.", status=502)
                resp_data = await resp.json()

        stripe_user_id = str(resp_data.get("stripe_user_id") or "").strip()
        if not stripe_user_id:
            return web.Response(text="Keine Stripe Account ID erhalten.", status=502)

        twitch_login = session_login
        now = datetime.now(UTC).isoformat()

        with storage.get_conn() as conn:
            self._affiliate_ensure_tables(conn)
            with self._affiliate_commission_lock(conn, twitch_login):
                conn.execute(
                    """UPDATE affiliate_accounts
                       SET stripe_account_id = ?, stripe_connected_at = ?,
                           stripe_connect_status = 'connected', updated_at = ?
                       WHERE twitch_login = ?""",
                    (stripe_user_id, now, now, twitch_login),
                )
                conn.commit()

                stripe, stripe_import_error = self._affiliate_import_stripe()
                if stripe is not None:
                    stripe.api_key = stripe_secret_key

                self._affiliate_replay_pending_commissions(
                    conn,
                    stripe=stripe,
                    stripe_account_id=stripe_user_id,
                    affiliate_login=twitch_login,
                    error_message=stripe_import_error or None,
                )

        raise web.HTTPFound("/twitch/affiliate/portal")

    # ------------------------------------------------------------------ #
    # Claim route                                                          #
    # ------------------------------------------------------------------ #

    async def _affiliate_claim(self, request: web.Request) -> web.StreamResponse:
        session = self._get_affiliate_session(request)
        if not session:
            return web.json_response({"error": "unauthorized"}, status=401)

        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "invalid_json"}, status=400)

        streamer_login = str(body.get("streamer_login") or "").strip().lower()
        if not _LOGIN_RE.match(streamer_login):
            return web.json_response({"error": "invalid_login"}, status=400)

        twitch_login = session.get("twitch_login", "")
        now = datetime.now(UTC).isoformat()

        with storage.get_conn() as conn:
            self._affiliate_ensure_tables(conn)

            # Check if streamer is already a registered partner
            row = conn.execute(
                "SELECT twitch_login FROM twitch_streamers_partner_state WHERE LOWER(twitch_login) = LOWER(?) AND is_partner_active = 1",
                (streamer_login,),
            ).fetchone()
            if row:
                return web.json_response({"error": "streamer_already_registered"}, status=409)

            try:
                conn.execute(
                    """INSERT INTO affiliate_streamer_claims
                       (affiliate_twitch_login, claimed_streamer_login, claimed_at)
                       VALUES (?, ?, ?)""",
                    (twitch_login, streamer_login, now),
                )
                conn.commit()
            except Exception as _dup_exc:
                if "unique" in str(_dup_exc).lower() or "duplicate" in str(_dup_exc).lower():
                    return web.json_response({"error": "already_claimed"}, status=409)
                raise

        return web.json_response({"ok": True, "claimed": streamer_login})

    # ------------------------------------------------------------------ #
    # API data routes                                                      #
    # ------------------------------------------------------------------ #

    async def _affiliate_api_me(self, request: web.Request) -> web.StreamResponse:
        session = self._get_affiliate_session(request)
        if not session:
            return web.json_response({"error": "unauthorized"}, status=401)

        twitch_login = session.get("twitch_login", "")

        with storage.get_conn() as conn:
            self._affiliate_ensure_tables(conn)
            row = conn.execute(
                "SELECT * FROM affiliate_accounts WHERE twitch_login = ?",
                (twitch_login,),
            ).fetchone()

        if not row:
            return web.json_response({"error": "not_found"}, status=404)

        # Mask stripe_account_id
        stripe_id = str(row["stripe_account_id"] or "")
        masked = f"{stripe_id[:8]}...{stripe_id[-4:]}" if len(stripe_id) > 12 else stripe_id

        return web.json_response({
            "twitch_login": row["twitch_login"],
            "display_name": row["display_name"],
            "email": row["email"],
            "full_name": row["full_name"],
            "stripe_connect_status": row["stripe_connect_status"],
            "stripe_account_id": masked,
            "is_active": bool(row["is_active"]),
            "created_at": row["created_at"],
        })

    async def _affiliate_api_claims(self, request: web.Request) -> web.StreamResponse:
        session = self._get_affiliate_session(request)
        if not session:
            return web.json_response({"error": "unauthorized"}, status=401)

        twitch_login = session.get("twitch_login", "")

        with storage.get_conn() as conn:
            self._affiliate_ensure_tables(conn)
            rows = conn.execute(
                """SELECT c.claimed_streamer_login, c.claimed_at,
                          COUNT(co.id) AS commission_count,
                          COALESCE(SUM(co.commission_cents), 0) AS total_commission_cents
                   FROM affiliate_streamer_claims c
                   LEFT JOIN affiliate_commissions co
                       ON co.affiliate_twitch_login = c.affiliate_twitch_login
                       AND co.streamer_login = c.claimed_streamer_login
                   WHERE c.affiliate_twitch_login = ?
                   GROUP BY c.claimed_streamer_login, c.claimed_at""",
                (twitch_login,),
            ).fetchall()

        claims = [
            {
                "streamer_login": r["claimed_streamer_login"],
                "claimed_at": r["claimed_at"],
                "commission_count": r["commission_count"],
                "total_commission_cents": r["total_commission_cents"],
            }
            for r in rows
        ]
        return web.json_response({"claims": claims})

    async def _affiliate_api_commissions(self, request: web.Request) -> web.StreamResponse:
        session = self._get_affiliate_session(request)
        if not session:
            return web.json_response({"error": "unauthorized"}, status=401)

        twitch_login = session.get("twitch_login", "")
        try:
            page = max(1, int(request.query.get("page", "1")))
            page_size = min(100, max(1, int(request.query.get("page_size", "25"))))
        except (ValueError, TypeError):
            page, page_size = 1, 25
        offset = (page - 1) * page_size

        with storage.get_conn() as conn:
            self._affiliate_ensure_tables(conn)
            total_row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM affiliate_commissions WHERE affiliate_twitch_login = ?",
                (twitch_login,),
            ).fetchone()
            total = total_row["cnt"] if total_row else 0

            rows = conn.execute(
                """SELECT id, streamer_login, brutto_cents, commission_cents, currency,
                          status, period_start, period_end, created_at, transferred_at
                   FROM affiliate_commissions
                   WHERE affiliate_twitch_login = ?
                   ORDER BY created_at DESC
                   LIMIT ? OFFSET ?""",
                (twitch_login, page_size, offset),
            ).fetchall()

        commissions = [
            {
                "id": r["id"],
                "streamer_login": r["streamer_login"],
                "brutto_cents": r["brutto_cents"],
                "commission_cents": r["commission_cents"],
                "currency": r["currency"],
                "status": r["status"],
                "period_start": r["period_start"],
                "period_end": r["period_end"],
                "created_at": r["created_at"],
                "transferred_at": r["transferred_at"],
            }
            for r in rows
        ]
        return web.json_response({
            "commissions": commissions,
            "page": page,
            "page_size": page_size,
            "total": total,
        })

    # ------------------------------------------------------------------ #
    # Commission processing (called from webhook, not a route)             #
    # ------------------------------------------------------------------ #

    def _affiliate_process_commission(
        self,
        conn: Any,
        *,
        stripe: Any,
        stripe_event_id: str,
        stripe_customer_id: str,
        amount_paid_cents: int,
        currency: str,
        invoice_id: str,
        period_start: str,
        period_end: str,
    ) -> str:
        self._affiliate_ensure_tables(conn)

        # Look up streamer from billing subscription
        row = conn.execute(
            "SELECT twitch_login FROM twitch_billing_subscriptions WHERE stripe_customer_id = ?",
            (stripe_customer_id,),
        ).fetchone()
        if not row:
            return "no_streamer"
        streamer_login = str(row["twitch_login"] or "").strip().lower()

        # Look up affiliate claim
        claim = conn.execute(
            "SELECT affiliate_twitch_login FROM affiliate_streamer_claims WHERE claimed_streamer_login = ?",
            (streamer_login,),
        ).fetchone()
        if not claim:
            return "no_affiliate"
        affiliate_login = str(claim["affiliate_twitch_login"] or "")

        if amount_paid_cents <= 0:
            return "skipped"
        commission_cents = int(amount_paid_cents * _COMMISSION_RATE)
        if commission_cents <= 0:
            return "skipped"
        now = datetime.now(UTC).isoformat()

        initial_status = "pending"

        with self._affiliate_commission_lock(conn, affiliate_login):
            try:
                with self._affiliate_db_transaction(conn):
                    acct_result = conn.execute(
                        "SELECT stripe_account_id FROM affiliate_accounts WHERE twitch_login = ?",
                        (affiliate_login,),
                    )
                    acct = acct_result.fetchone() if acct_result else None
                    stripe_account_id = str(
                        (acct["stripe_account_id"] if acct else None) or ""
                    ).strip()

                    if not stripe_account_id:
                        pending_total_result = conn.execute(
                            """SELECT COALESCE(SUM(commission_cents), 0) AS pending_total
                               FROM affiliate_commissions
                               WHERE affiliate_twitch_login = ? AND status = 'pending'""",
                            (affiliate_login,),
                        )
                        pending_total_row = pending_total_result.fetchone() if pending_total_result else None
                        current_pending_total = int(
                            (pending_total_row["pending_total"] if pending_total_row else 0) or 0
                        )
                        if current_pending_total + commission_cents > _MAX_PENDING_COMMISSION_CENTS:
                            initial_status = "skipped"

                    conn.execute(
                        """INSERT INTO affiliate_commissions
                           (affiliate_twitch_login, streamer_login, stripe_event_id, stripe_invoice_id,
                            stripe_customer_id, brutto_cents, commission_cents, currency,
                            status, period_start, period_end, created_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (affiliate_login, streamer_login, stripe_event_id, invoice_id,
                         stripe_customer_id, amount_paid_cents, commission_cents, currency,
                         initial_status, period_start, period_end, now),
                    )
            except Exception as _dup_exc:
                if "unique" in str(_dup_exc).lower() or "duplicate" in str(_dup_exc).lower():
                    return "duplicate"
                raise

            if not stripe_account_id:
                return initial_status

            self._affiliate_replay_pending_commissions(
                conn,
                stripe=stripe,
                stripe_account_id=stripe_account_id,
                affiliate_login=affiliate_login,
            )

            status_result = conn.execute(
                "SELECT status FROM affiliate_commissions WHERE stripe_event_id = ?",
                (stripe_event_id,),
            )
            status_row = status_result.fetchone() if status_result else None
            return str((status_row["status"] if status_row else None) or "pending")

    # ------------------------------------------------------------------ #
    # Route registration                                                   #
    # ------------------------------------------------------------------ #

    def _affiliate_register_routes(self, app: web.Application) -> None:
        with storage.get_conn() as conn:
            self._affiliate_ensure_tables(conn)

        app.router.add_get(
            "/twitch/auth/affiliate/login", self._affiliate_auth_login
        )
        app.router.add_get(
            "/twitch/auth/affiliate/callback", self._affiliate_auth_callback
        )
        app.router.add_get(
            "/twitch/affiliate/signup", self._affiliate_signup_page
        )
        app.router.add_post(
            "/twitch/affiliate/signup/complete", self._affiliate_signup_complete
        )
        app.router.add_get(
            "/twitch/affiliate/connect/stripe", self._affiliate_connect_stripe
        )
        app.router.add_get(
            "/twitch/affiliate/connect/stripe/callback",
            self._affiliate_connect_stripe_callback,
        )
        app.router.add_post(
            "/twitch/affiliate/claim", self._affiliate_claim
        )
        app.router.add_get(
            "/twitch/api/affiliate/me", self._affiliate_api_me
        )
        app.router.add_get(
            "/twitch/api/affiliate/claims", self._affiliate_api_claims
        )
        app.router.add_get(
            "/twitch/api/affiliate/commissions", self._affiliate_api_commissions
        )
