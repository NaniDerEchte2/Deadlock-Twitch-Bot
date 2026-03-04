"""Compatibility shim: run the standalone dashboard service module."""

from __future__ import annotations

from bot.dashboard_service.__main__ import main


if __name__ == "__main__":
    main()
