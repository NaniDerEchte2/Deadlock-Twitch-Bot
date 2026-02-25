"""Compatibility package for legacy imports after the repo split.

- Keeps ``import twitch_cog`` working by delegating to the new modular code in ``bot``.
- Provides submodules such as ``twitch_cog.storage_pg`` that mirror the old layout but
  internally re-export the modern PostgreSQL wrapper from ``bot.storage.pg``.
"""

from __future__ import annotations

# Public entrypoints expected by the master bot
from bot import setup, teardown  # noqa: F401

# Convenience re-export for callers that previously did
# ``from twitch_cog import storage`` or similar.
from bot import storage  # type: ignore  # noqa: F401

__all__ = ["setup", "teardown", "storage"]
