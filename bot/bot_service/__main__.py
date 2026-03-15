"""CLI entrypoint for the standalone Twitch worker service."""

from __future__ import annotations

import asyncio

from ..core.constants import log
from ..logging_setup import ensure_twitch_logger_file_handler
from .app import run_bot_service


def main() -> None:
    ensure_twitch_logger_file_handler()
    try:
        asyncio.run(run_bot_service())
    except KeyboardInterrupt:
        log.info("Twitch worker service interrupted by signal")
    except RuntimeError as exc:
        message = str(exc).strip() or "twitch worker service startup failed"
        log.error("%s", message)
        raise SystemExit(2) from None
    except asyncio.CancelledError:
        log.info("Twitch worker service cancelled")
        raise SystemExit(0) from None


if __name__ == "__main__":
    main()
