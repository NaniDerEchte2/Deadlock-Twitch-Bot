"""
Raid Bot Manager für automatische Twitch Raids zwischen Partnern.

Verwaltet:
- OAuth User Access Tokens für Streamer
- Automatische Raids beim Offline-Gehen
- Partner-Auswahl (niedrigste Viewer, optional niedrigste Follower)
- Raid-Metadaten und History
"""

from .auth import RaidAuthManager      # noqa: F401
from .executor import RaidExecutor     # noqa: F401
from .bot import RaidBot               # noqa: F401

__all__ = ["RaidAuthManager", "RaidExecutor", "RaidBot"]
