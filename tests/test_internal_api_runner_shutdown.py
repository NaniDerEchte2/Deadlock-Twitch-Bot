import asyncio
import unittest
from unittest.mock import patch

from bot.internal_api.runner import InternalApiRunner


class _CancelledCleanupRunner:
    async def cleanup(self) -> None:
        raise asyncio.CancelledError()


class InternalApiRunnerShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def test_stop_handles_cancelled_error_without_raising(self) -> None:
        runner = InternalApiRunner(host="127.0.0.1", port=8776, token="secret")
        runner._runner = _CancelledCleanupRunner()  # type: ignore[assignment]
        runner._app = object()  # type: ignore[assignment]

        await runner.stop()

        self.assertFalse(runner.is_running)

    async def test_start_fails_cleanly_on_runtime_guard_violation(self) -> None:
        runner = InternalApiRunner(host="127.0.0.1", port=8766, token="secret")
        with patch.dict(
            "os.environ",
            {
                "TWITCH_RUNTIME_ROLE": "twitch_worker",
                "TWITCH_RUNTIME_ENFORCE": "1",
            },
            clear=True,
        ):
            await runner.start()

        self.assertFalse(runner.is_running)


if __name__ == "__main__":
    unittest.main()
