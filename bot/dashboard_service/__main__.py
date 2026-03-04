"""CLI entrypoint for the standalone dashboard split service."""

from __future__ import annotations

import asyncio

from .app import run_dashboard_service


def main() -> None:
    asyncio.run(run_dashboard_service())


if __name__ == "__main__":
    main()
