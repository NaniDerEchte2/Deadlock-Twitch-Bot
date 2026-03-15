from __future__ import annotations

import logging
import logging.handlers
import os
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_LOGS_DIR = _PROJECT_ROOT / "logs"
_TWITCH_LOGGER_NAME = "TwitchStreams"
_DEFAULT_TWITCH_LOG_FILENAME = "twitch_bot.log"
_DASHBOARD_TWITCH_LOG_FILENAME = "twitch_dashboard.log"
_MANAGED_TWITCH_LOG_FILENAMES = frozenset(
    {
        _DEFAULT_TWITCH_LOG_FILENAME,
        _DASHBOARD_TWITCH_LOG_FILENAME,
    }
)
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


def current_twitch_log_filename() -> str:
    explicit_value = Path(str(os.getenv("TWITCH_LOG_FILENAME") or "")).name
    if explicit_value:
        return explicit_value

    split_runtime_role = str(os.getenv("TWITCH_SPLIT_RUNTIME_ROLE") or "").strip().lower()
    if split_runtime_role == "dashboard":
        return _DASHBOARD_TWITCH_LOG_FILENAME
    return _DEFAULT_TWITCH_LOG_FILENAME


def ensure_twitch_logger_file_handler(*, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(_TWITCH_LOGGER_NAME)
    if logger.level == logging.NOTSET or logger.level > level:
        logger.setLevel(level)

    target_filename = current_twitch_log_filename()
    file_path = log_path(target_filename)
    formatter = logging.Formatter(_DEFAULT_LOG_FORMAT)
    stale_handlers: list[logging.Handler] = []

    for handler in logger.handlers:
        handler_filename = _handler_file_name(handler)
        if handler_filename != target_filename:
            if handler_filename in _MANAGED_TWITCH_LOG_FILENAMES:
                stale_handlers.append(handler)
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
