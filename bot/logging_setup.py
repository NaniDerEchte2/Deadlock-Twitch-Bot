from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_LOGS_DIR = _PROJECT_ROOT / "logs"
_TWITCH_LOGGER_NAME = "TwitchStreams"
_TWITCH_LOG_FILENAME = "twitch_bot.log"
_DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def project_root() -> Path:
    return _PROJECT_ROOT


def logs_dir() -> Path:
    _LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return _LOGS_DIR


def log_path(filename: str) -> Path:
    normalized = Path(str(filename or "")).name
    if not normalized:
        raise ValueError("filename is required")
    return logs_dir() / normalized


def _same_file_handler_target(handler: logging.Handler, expected_path: Path) -> bool:
    if not isinstance(handler, logging.handlers.RotatingFileHandler):
        return False
    base_filename = getattr(handler, "baseFilename", "")
    if not base_filename:
        return False
    try:
        return Path(base_filename).resolve() == expected_path.resolve()
    except OSError:
        return str(base_filename) == str(expected_path)


def _handler_file_name(handler: logging.Handler) -> str:
    base_filename = getattr(handler, "baseFilename", "")
    if not base_filename:
        return ""
    return Path(str(base_filename)).name


def ensure_twitch_logger_file_handler(*, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(_TWITCH_LOGGER_NAME)
    if logger.level == logging.NOTSET or logger.level > level:
        logger.setLevel(level)

    file_path = log_path(_TWITCH_LOG_FILENAME)
    formatter = logging.Formatter(_DEFAULT_LOG_FORMAT)
    stale_handlers: list[logging.Handler] = []

    for handler in logger.handlers:
        if _handler_file_name(handler) != _TWITCH_LOG_FILENAME:
            continue
        if not _same_file_handler_target(handler, file_path):
            stale_handlers.append(handler)
            continue
        handler.setFormatter(formatter)
        if handler.level > level:
            handler.setLevel(level)
        return logger

    for handler in stale_handlers:
        logger.removeHandler(handler)
        handler.close()

    handler = logging.handlers.RotatingFileHandler(
        file_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    handler.setFormatter(formatter)
    handler.setLevel(level)
    logger.addHandler(handler)
    return logger
