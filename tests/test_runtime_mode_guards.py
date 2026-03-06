import unittest
from unittest.mock import patch

from bot.runtime_mode import (
    DASHBOARD_SERVICE_PORT,
    INTERNAL_API_PORT,
    MASTER_API_RESERVED_PORT,
    enforce_dashboard_service_runtime,
    enforce_internal_api_runtime,
)


class RuntimeModeGuardsTests(unittest.TestCase):
    def test_dashboard_runtime_guard_accepts_expected_role_and_port(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "TWITCH_RUNTIME_ROLE": "dashboard",
                "TWITCH_RUNTIME_ENFORCE": "1",
            },
            clear=True,
        ):
            role = enforce_dashboard_service_runtime(port=DASHBOARD_SERVICE_PORT)

        self.assertEqual(role, "dashboard")

    def test_internal_runtime_guard_accepts_legacy_bot_alias(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "TWITCH_SPLIT_RUNTIME_ROLE": "bot",
                "TWITCH_SPLIT_RUNTIME_ENFORCE": "1",
            },
            clear=True,
        ):
            role = enforce_internal_api_runtime(port=INTERNAL_API_PORT)

        self.assertEqual(role, "twitch_worker")

    def test_dashboard_runtime_guard_rejects_missing_role(self) -> None:
        with patch.dict("os.environ", {"TWITCH_RUNTIME_ENFORCE": "1"}, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                enforce_dashboard_service_runtime(port=DASHBOARD_SERVICE_PORT)

        self.assertIn("runtime role is missing", str(ctx.exception).lower())

    def test_internal_runtime_guard_rejects_reserved_master_port(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "TWITCH_RUNTIME_ROLE": "twitch_worker",
                "TWITCH_RUNTIME_ENFORCE": "1",
            },
            clear=True,
        ):
            with self.assertRaises(RuntimeError) as ctx:
                enforce_internal_api_runtime(port=MASTER_API_RESERVED_PORT)

        self.assertIn("reserved for the master api service", str(ctx.exception).lower())

    def test_runtime_guard_can_be_disabled(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "TWITCH_RUNTIME_ROLE": "master",
                "TWITCH_RUNTIME_ENFORCE": "0",
            },
            clear=True,
        ):
            role = enforce_dashboard_service_runtime(port=9999)

        self.assertEqual(role, "master")


if __name__ == "__main__":
    unittest.main()
