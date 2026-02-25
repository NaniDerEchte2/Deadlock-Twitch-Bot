"""_EmbedsMixin – Discord embeds and UI components for live announcements."""
from __future__ import annotations

import asyncio
import secrets
from datetime import UTC, datetime
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import discord

from .. import storage
from ..core.constants import (
    TWITCH_BRAND_COLOR_HEX,
    TWITCH_BUTTON_LABEL,
    TWITCH_DISCORD_REF_CODE,
    TWITCH_TARGET_GAME_NAME,
    TWITCH_VOD_BUTTON_LABEL,
    log,
)


class _EmbedsMixin:

    def _build_live_embed(self, login: str, stream: dict) -> discord.Embed:
        """Erzeuge ein Discord-Embed für das Go-Live-Posting mit Stream-Vorschau."""

        display_name = stream.get("user_name") or login
        game = stream.get("game_name") or TWITCH_TARGET_GAME_NAME
        title = stream.get("title") or "Live!"
        viewer_count = int(stream.get("viewer_count") or 0)

        timestamp = datetime.now(tz=UTC)
        started_at_raw = stream.get("started_at")
        if isinstance(started_at_raw, str) and started_at_raw:
            try:
                timestamp = datetime.fromisoformat(started_at_raw.replace("Z", "+00:00"))
            except ValueError as exc:
                log.debug("Ungültiger started_at-Wert '%s': %s", started_at_raw, exc)

        embed = discord.Embed(
            title=f"{display_name} ist LIVE in {game}!",
            description=title,
            colour=discord.Color(TWITCH_BRAND_COLOR_HEX),
            timestamp=timestamp,
        )

        embed.add_field(name="Viewer", value=str(viewer_count), inline=True)
        embed.add_field(name="Kategorie", value=game, inline=True)

        thumbnail_url = (stream.get("thumbnail_url") or "").strip()
        if thumbnail_url:
            thumbnail_url = thumbnail_url.replace("{width}", "1280").replace("{height}", "720")
            cache_bust = int(datetime.now(tz=UTC).timestamp())
            embed.set_image(url=f"{thumbnail_url}?rand={cache_bust}")

        embed.set_footer(text="Auf Twitch ansehen fuer mehr Deadlock-Action!")
        embed.set_author(name=f"LIVE: {display_name}")

        return embed

    def _build_offline_embed(
        self,
        *,
        login: str,
        display_name: str,
        last_title: str | None,
        last_game: str | None,
        preview_image_url: str | None,
    ) -> discord.Embed:
        """Offline-Overlay: gleicher Stil wie live, aber klar als VOD markiert."""

        game = last_game or TWITCH_TARGET_GAME_NAME or "Twitch"
        description = last_title or "Letzten Stream als VOD ansehen."

        embed = discord.Embed(
            title=f"{display_name} ist OFFLINE",
            description=description,
            colour=discord.Color(TWITCH_BRAND_COLOR_HEX),
            timestamp=datetime.now(tz=UTC),
        )

        embed.add_field(name="Status", value="OFFLINE", inline=True)
        embed.add_field(name="Kategorie", value=game, inline=True)
        embed.add_field(name="Hinweis", value="VOD ueber den Button abrufen.", inline=False)

        if preview_image_url:
            embed.set_image(url=preview_image_url)

        embed.set_footer(text="Letzten Stream auf Twitch ansehen.")
        embed.set_author(name=f"OFFLINE: {display_name}")

        return embed

    def _build_offline_link_view(
        self, referral_url: str, *, label: str | None = None
    ) -> discord.ui.View:
        """Offline-Ansicht: einfacher Link-Button ohne Tracking."""
        view = discord.ui.View(timeout=None)
        view.add_item(
            discord.ui.Button(
                label=label or TWITCH_BUTTON_LABEL,
                style=discord.ButtonStyle.link,
                url=referral_url,
            )
        )
        return view

    async def cog_load(self) -> None:
        await super().cog_load()
        spawner = getattr(self, "_spawn_bg_task", None)
        if callable(spawner):
            spawner(self._register_persistent_live_views(), "twitch.register_live_views")
        else:
            asyncio.create_task(
                self._register_persistent_live_views(),
                name="twitch.register_live_views",
            )

    def _build_live_view(
        self,
        streamer_login: str,
        referral_url: str,
        tracking_token: str,
    ) -> _TwitchLiveAnnouncementView | None:
        """Create a persistent view that tracks button clicks before redirecting."""
        if not tracking_token:
            return None
        return _TwitchLiveAnnouncementView(
            cog=self,
            streamer_login=streamer_login,
            referral_url=referral_url,
            tracking_token=tracking_token,
        )

    @staticmethod
    def _generate_tracking_token() -> str:
        return secrets.token_hex(8)

    def _build_referral_url(self, login: str) -> str:
        """Append the configured referral parameter to the Twitch URL."""
        normalized_login = (login or "").strip()
        base_url = (
            f"https://www.twitch.tv/{normalized_login}"
            if normalized_login
            else "https://www.twitch.tv/"
        )
        ref_code = (TWITCH_DISCORD_REF_CODE or "").strip()
        if not ref_code:
            return base_url
        parsed = urlparse(base_url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query["ref"] = ref_code
        encoded = urlencode(query)
        return urlunparse(parsed._replace(query=encoded))

    async def _register_persistent_live_views(self) -> None:
        """Re-register live announcement views after a restart."""
        if not self._notify_channel_id:
            return
        try:
            await self.bot.wait_until_ready()
        except Exception:
            log.exception("wait_until_ready für Twitch-Views fehlgeschlagen")
            return

        try:
            with storage.get_conn() as c:
                rows = c.execute(
                    "SELECT streamer_login, last_discord_message_id, last_tracking_token "
                    "FROM twitch_live_state "
                    "WHERE last_discord_message_id IS NOT NULL AND last_tracking_token IS NOT NULL"
                ).fetchall()
        except Exception:
            log.exception("Konnte persistente Twitch-Views nicht registrieren")
            return

        for row in rows:
            login = (row["streamer_login"] or "").strip()
            token = (row["last_tracking_token"] or "").strip()
            message_id_raw = row["last_discord_message_id"]
            if not login or not token or not message_id_raw:
                continue
            try:
                message_id = int(message_id_raw)
            except (TypeError, ValueError):
                continue
            referral_url = self._build_referral_url(login)
            view = self._build_live_view(login, referral_url, token)
            if view is None:
                continue
            view.bind_to_message(channel_id=self._notify_channel_id, message_id=message_id)
            self._register_live_view(tracking_token=token, view=view, message_id=message_id)

    def _get_live_view_registry(self) -> dict[str, _TwitchLiveAnnouncementView]:
        registry = getattr(self, "_live_view_registry", None)
        if registry is None:
            registry = {}
            self._live_view_registry = registry
        return registry

    def _register_live_view(
        self,
        *,
        tracking_token: str,
        view: _TwitchLiveAnnouncementView,
        message_id: int,
    ) -> None:
        if not tracking_token:
            return
        registry = self._get_live_view_registry()
        registry[tracking_token] = view
        try:
            self.bot.add_view(view, message_id=message_id)
        except Exception:
            log.exception("Konnte View für Twitch-Posting %s nicht registrieren", message_id)

    def _drop_live_view(self, tracking_token: str | None) -> None:
        if not tracking_token:
            return
        registry = self._get_live_view_registry()
        view = registry.pop(tracking_token, None)
        if view is None:
            return

        # discord.py hat kein natives remove_view am Bot-Objekt.
        # view.stop() reicht aus, um die Interaktionen zu beenden.
        view.stop()
        log.debug("Live-View gestoppt und aus Registry entfernt: %s", tracking_token)

    def _log_link_click(
        self,
        *,
        interaction: discord.Interaction,
        view: _TwitchLiveAnnouncementView,
    ) -> None:
        clicked_at = datetime.now(tz=UTC).isoformat(timespec="seconds")
        user = interaction.user
        user_id = str(getattr(user, "id", "") or "") or None
        username = str(user) if user else None
        guild_id = str(interaction.guild_id) if interaction.guild_id else None
        channel_source = interaction.channel_id or view.channel_id
        channel_id = str(channel_source) if channel_source else None
        if interaction.message and interaction.message.id:
            message_id = str(interaction.message.id)
        elif view.message_id:
            message_id = str(view.message_id)
        else:
            message_id = None
        ref_code = (TWITCH_DISCORD_REF_CODE or "").strip() or None

        try:
            with storage.get_conn() as c:
                c.execute(
                    """
                    INSERT INTO twitch_link_clicks (
                        clicked_at,
                        streamer_login,
                        tracking_token,
                        discord_user_id,
                        discord_username,
                        guild_id,
                        channel_id,
                        message_id,
                        ref_code,
                        source_hint
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        clicked_at,
                        view.streamer_login.lower(),
                        view.tracking_token,
                        user_id,
                        username,
                        guild_id,
                        channel_id,
                        message_id,
                        ref_code,
                        "live_button",
                    ),
                )
        except Exception:
            log.exception("Konnte Twitch-Link-Klick nicht speichern")

    async def _handle_tracked_button_click(
        self,
        interaction: discord.Interaction,
        view: _TwitchLiveAnnouncementView,
    ) -> None:
        try:
            self._log_link_click(interaction=interaction, view=view)
        except Exception:
            log.exception("Konnte Klick nicht loggen")

        content = f"Hier ist dein Twitch-Link für **{view.streamer_login}**."
        response_view = _TwitchReferralLinkView(view.referral_url)
        try:
            if interaction.response.is_done():
                await interaction.followup.send(content, view=response_view, ephemeral=True)
            else:
                await interaction.response.send_message(content, view=response_view, ephemeral=True)
        except Exception:
            log.exception("Antwort mit Referral-Link fehlgeschlagen")


class _TwitchReferralLinkView(discord.ui.View):
    """Ephemeral view with a direct Twitch hyperlink."""

    def __init__(self, referral_url: str):
        super().__init__(timeout=60)
        self.add_item(
            discord.ui.Button(
                label=TWITCH_BUTTON_LABEL,
                style=discord.ButtonStyle.link,
                url=referral_url,
            )
        )


class _TrackedTwitchButton(discord.ui.Button):
    def __init__(self, parent: _TwitchLiveAnnouncementView, *, custom_id: str):
        super().__init__(
            label=TWITCH_BUTTON_LABEL,
            style=discord.ButtonStyle.primary,
            custom_id=custom_id,
        )
        self._view_ref = parent  # Renamed from _parent to avoid discord.py conflict

    async def callback(self, interaction: discord.Interaction) -> None:  # type: ignore[override]
        await self._view_ref.handle_click(interaction)


class _TwitchLiveAnnouncementView(discord.ui.View):
    """Persistent live announcement view that tracks clicks before redirecting."""

    def __init__(
        self,
        *,
        cog: _EmbedsMixin,
        streamer_login: str,
        referral_url: str,
        tracking_token: str,
    ):
        super().__init__(timeout=None)
        self.cog = cog
        self.streamer_login = streamer_login
        self.referral_url = referral_url
        self.tracking_token = tracking_token
        self.message_id: int | None = None
        self.channel_id: int | None = None

        custom_id = self._build_custom_id(streamer_login, tracking_token)
        self.add_item(_TrackedTwitchButton(self, custom_id=custom_id))

    @staticmethod
    def _build_custom_id(streamer_login: str, tracking_token: str) -> str:
        login_part = "".join(ch for ch in streamer_login.lower() if ch.isalnum())[:24] or "stream"
        token_part = (tracking_token or "")[:32] or secrets.token_hex(4)
        return f"twitch-live:{login_part}:{token_part}"

    def bind_to_message(self, *, channel_id: int | None, message_id: int | None) -> None:
        self.channel_id = channel_id
        self.message_id = message_id

    async def handle_click(self, interaction: discord.Interaction) -> None:
        await self.cog._handle_tracked_button_click(interaction, self)
