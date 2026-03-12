import contextlib
import unittest
from unittest.mock import patch

from bot.core.constants import TWITCH_LOG_EVERY_N_TICKS
from bot.monitoring.monitoring import TwitchMonitoringMixin


class _PartnerStateConnection:
    def execute(self, sql: str, params=(), *args, **kwargs):
        return self

    def fetchall(self):
        return []


class _ApiStub:
    def is_auth_blocked(self) -> bool:
        return False

    async def get_streams_by_logins(self, logins, language=None):
        return []

    async def get_streams_by_category(self, category_id, language=None, limit=100):
        return []


class _MonitoringHarness(TwitchMonitoringMixin):
    def __init__(self, log_every_n: int = TWITCH_LOG_EVERY_N_TICKS) -> None:
        self.api = _ApiStub()
        self._category_id = "509658"
        self._language_filters = []
        self._category_sample_limit = 50
        self._tick_count = 0
        self._log_every_n = log_every_n
        self.log_stats_calls = 0
        self.snapshot_reasons: list[str] = []
        self.reconciliation_triggers: list[str] = []

    async def _process_postings(self, tracked, streams_by_login) -> None:
        return None

    def _maybe_schedule_partner_raid_score_reconciliation(self, *, trigger: str) -> bool:
        self.reconciliation_triggers.append(trigger)
        return True

    async def _record_eventsub_capacity_snapshot(self, reason: str) -> None:
        self.snapshot_reasons.append(reason)

    async def _log_stats(self, streams_by_login: dict[str, dict], category_streams: list[dict]) -> None:
        self.log_stats_calls += 1

    async def _run_partner_recruit(self, category_streams: list[dict]) -> None:
        return None


class MonitoringStatsLoggingIntervalTests(unittest.IsolatedAsyncioTestCase):
    async def test_default_stats_logging_runs_every_poll_tick(self) -> None:
        harness = _MonitoringHarness()
        conn = _PartnerStateConnection()

        with patch(
            "bot.monitoring.monitoring.storage.get_conn",
            side_effect=lambda: contextlib.nullcontext(conn),
        ):
            await harness._tick()
            await harness._tick()

        self.assertEqual(harness.log_stats_calls, 2)
        self.assertEqual(harness._tick_count, 2)
        self.assertEqual(harness.snapshot_reasons, ["poll_tick", "poll_tick"])
        self.assertEqual(
            harness.reconciliation_triggers,
            ["poll_tick_reconciliation", "poll_tick_reconciliation"],
        )

    async def test_custom_tick_multiple_still_delays_stats_logging(self) -> None:
        harness = _MonitoringHarness(log_every_n=3)
        conn = _PartnerStateConnection()

        with patch(
            "bot.monitoring.monitoring.storage.get_conn",
            side_effect=lambda: contextlib.nullcontext(conn),
        ):
            await harness._tick()
            await harness._tick()
            self.assertEqual(harness.log_stats_calls, 0)
            await harness._tick()

        self.assertEqual(harness.log_stats_calls, 1)
        self.assertEqual(harness._tick_count, 3)


if __name__ == "__main__":
    unittest.main()
