import logging
import logging.handlers
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bot import logging_setup


class LoggingSetupTests(unittest.TestCase):
    def test_log_path_uses_project_logs_dir_and_strips_nested_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(logging_setup, "_LOGS_DIR", Path(tmpdir)):
                resolved = logging_setup.log_path("nested/twitch_autobans.log")

        self.assertEqual(resolved, Path(tmpdir) / "twitch_autobans.log")

    def test_ensure_twitch_logger_file_handler_is_idempotent(self) -> None:
        logger_name = "TwitchStreams.TestLoggingSetup"
        logger = logging.getLogger(logger_name)
        logger.handlers.clear()
        logger.setLevel(logging.NOTSET)

        with tempfile.TemporaryDirectory() as tmpdir:
            legacy_dir = Path(tmpdir) / "legacy"
            legacy_dir.mkdir()
            legacy_handler = logging.handlers.RotatingFileHandler(
                legacy_dir / "twitch_bot.log",
                maxBytes=1024,
                backupCount=1,
                encoding="utf-8",
            )
            logger.addHandler(legacy_handler)

            with patch.object(logging_setup, "_LOGS_DIR", Path(tmpdir)):
                with patch.object(logging_setup, "_TWITCH_LOGGER_NAME", logger_name):
                    logging_setup.ensure_twitch_logger_file_handler()
                    logging_setup.ensure_twitch_logger_file_handler()

                    handlers = [
                        handler
                        for handler in logger.handlers
                        if isinstance(handler, logging.handlers.RotatingFileHandler)
                    ]

                    self.assertEqual(len(handlers), 1)
                    self.assertEqual(
                        Path(handlers[0].baseFilename),
                        Path(tmpdir) / "twitch_bot.log",
                    )
                    self.assertEqual(logger.level, logging.INFO)

            for handler in list(logger.handlers):
                logger.removeHandler(handler)
                handler.close()
        logger.setLevel(logging.NOTSET)


if __name__ == "__main__":
    unittest.main()
