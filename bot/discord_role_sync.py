from __future__ import annotations

import asyncio
import logging
import os

import discord

log = logging.getLogger("TwitchStreams.DiscordRoleSync")

_DEFAULT_STREAMER_ROLE_ID = 1313624729466441769


def _parse_env_int(name: str, default: int = 0) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _mask_log_identifier(value: object, *, visible_prefix: int = 3, visible_suffix: int = 2) -> str:
    text = str(value or "").strip()
    if not text:
        return "<empty>"
    if len(text) <= visible_prefix + visible_suffix:
        return "***"
    return f"{text[:visible_prefix]}...{text[-visible_suffix:]}"


def streamer_role_id() -> int:
    return _parse_env_int("STREAMER_ROLE_ID", _DEFAULT_STREAMER_ROLE_ID)


def streamer_guild_id() -> int:
    return _parse_env_int("STREAMER_GUILD_ID", 0)


def fallback_main_guild_id() -> int:
    return _parse_env_int("MAIN_GUILD_ID", 0)


def get_discord_role_sync_mode() -> str:
    raw = (os.getenv("TWITCH_DISCORD_ROLE_SYNC_MODE") or "local").strip().lower()
    if raw in {"", "local"}:
        return "local"
    if raw == "external":
        return "external"
    log.warning(
        "Invalid TWITCH_DISCORD_ROLE_SYNC_MODE=%r; falling back to 'local'.",
        raw,
    )
    return "local"


def is_local_discord_role_sync_enabled() -> bool:
    return get_discord_role_sync_mode() == "local"


def normalize_discord_user_id(raw: str | None) -> str | None:
    candidate = str(raw or "").strip()
    if candidate and candidate.isdigit():
        return candidate
    return None


def iter_role_guild_candidates(discord_bot: discord.Client | None) -> list[discord.Guild]:
    if discord_bot is None:
        return []

    candidates: list[discord.Guild] = []
    seen: set[int] = set()
    for guild_id in (streamer_guild_id(), fallback_main_guild_id()):
        if guild_id and guild_id not in seen:
            seen.add(guild_id)
            guild = discord_bot.get_guild(guild_id)
            if guild is not None:
                candidates.append(guild)

    if not candidates:
        candidates.extend(getattr(discord_bot, "guilds", []))
    return candidates


async def sync_streamer_role(
    discord_bot: discord.Client | None,
    discord_user_id: str | None,
    *,
    should_have_role: bool,
    reason: str,
    logger: logging.Logger | None = None,
) -> bool:
    active_logger = logger or log

    if not is_local_discord_role_sync_enabled():
        active_logger.debug(
            "Skipping streamer role sync for Discord user %s because TWITCH_DISCORD_ROLE_SYNC_MODE=external.",
            _mask_log_identifier(discord_user_id),
        )
        return False

    role_id = streamer_role_id()
    if discord_bot is None or role_id <= 0:
        return False

    normalized_id = normalize_discord_user_id(discord_user_id)
    if not normalized_id:
        return False

    changed = False
    user_id_int = int(normalized_id)
    for guild in iter_role_guild_candidates(discord_bot):
        role = guild.get_role(role_id)
        if role is None:
            continue

        member = guild.get_member(user_id_int)
        if member is None:
            try:
                member = await guild.fetch_member(user_id_int)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                member = None

        if member is None:
            continue

        try:
            has_role = role in member.roles
            if should_have_role and not has_role:
                await member.add_roles(role, reason=reason)
                changed = True
                active_logger.info(
                    "Streamer role granted to %s in guild %s",
                    normalized_id,
                    guild.id,
                )
            elif (not should_have_role) and has_role:
                await member.remove_roles(role, reason=reason)
                changed = True
                active_logger.info(
                    "Streamer role removed from %s in guild %s",
                    normalized_id,
                    guild.id,
                )
        except discord.Forbidden:
            active_logger.warning("Missing permission to sync streamer role in guild %s", guild.id)
        except discord.HTTPException:
            active_logger.warning(
                "Discord API error while syncing streamer role in guild %s",
                guild.id,
            )
    return changed


def schedule_streamer_role_sync(
    discord_bot: discord.Client | None,
    discord_user_id: str | None,
    *,
    should_have_role: bool,
    reason: str,
    task_name: str = "twitch.streamer_role_sync",
    logger: logging.Logger | None = None,
) -> bool:
    normalized_id = normalize_discord_user_id(discord_user_id)
    if not normalized_id:
        return False

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return False

    loop.create_task(
        sync_streamer_role(
            discord_bot,
            normalized_id,
            should_have_role=should_have_role,
            reason=reason,
            logger=logger,
        ),
        name=task_name,
    )
    return True


__all__ = [
    "fallback_main_guild_id",
    "get_discord_role_sync_mode",
    "is_local_discord_role_sync_enabled",
    "iter_role_guild_candidates",
    "normalize_discord_user_id",
    "schedule_streamer_role_sync",
    "streamer_guild_id",
    "streamer_role_id",
    "sync_streamer_role",
]
