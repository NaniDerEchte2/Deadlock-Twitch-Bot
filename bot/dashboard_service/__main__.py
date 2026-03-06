"""CLI entrypoint for the standalone dashboard split service."""

from __future__ import annotations

import asyncio

from ..core.constants import log
from .app import run_dashboard_service


def main() -> None:
    try:
        asyncio.run(run_dashboard_service())
    except KeyboardInterrupt:
        log.info("Dashboard service interrupted by signal")
    except RuntimeError as exc:
        message = str(exc).strip() or "dashboard service startup failed"
        log.error("%s", message)
        raise SystemExit(2) from None
    except asyncio.CancelledError:
        log.info("Dashboard service cancelled")
        raise SystemExit(0) from None


if __name__ == "__main__":
    main()
