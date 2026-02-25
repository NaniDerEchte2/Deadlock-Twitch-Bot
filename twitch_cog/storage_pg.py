"""Legacy shim: old code imported twitch_cog.storage_pg.

We now forward everything to the new implementation in bot.storage.pg, which
provides sqlite-compatible helpers (execute, executemany, changes(), etc.) on
top of psycopg. This keeps existing calls working without code changes.
"""

from __future__ import annotations

# Re-export the full surface
from bot.storage.pg import *  # noqa: F401,F403
