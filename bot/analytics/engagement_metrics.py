"""Shared engagement KPI calculations for Twitch analytics endpoints."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

QUALITY_METHOD = Literal["real_samples", "low_coverage", "no_data"]

CHATTERS_COVERAGE_THRESHOLD = 0.2
PASSIVE_VIEWER_MIN_SAMPLES = 1


@dataclass(slots=True)
class EngagementInputs:
    """Raw counters needed to compute engagement KPIs."""

    total_messages: int
    active_chatters: int
    tracked_chat_accounts: int
    chatters_api_seen: int
    viewer_minutes: float
    viewer_minutes_has_real_samples: bool
    avg_viewers: float
    session_count: int
    sessions_with_chat: int


@dataclass(slots=True)
class EngagementOutputs:
    """Computed engagement KPIs and quality metadata."""

    chat_penetration_pct: float | None
    chat_penetration_reliable: bool
    messages_per_100_viewer_minutes: float | None
    viewer_minutes: float
    legacy_interaction_active_per_avg_viewer: float | None
    active_ratio: float
    passive_viewer_samples: int
    chatters_coverage: float
    method: QUALITY_METHOD
    chat_session_coverage: float


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def calculate_engagement(inputs: EngagementInputs) -> EngagementOutputs:
    tracked_accounts = max(0, int(inputs.tracked_chat_accounts))
    active_chatters = max(0, int(inputs.active_chatters))
    chatters_api_seen = max(0, int(inputs.chatters_api_seen))
    total_messages = max(0, int(inputs.total_messages))
    viewer_minutes = max(0.0, float(inputs.viewer_minutes or 0.0))
    avg_viewers = max(0.0, float(inputs.avg_viewers or 0.0))
    session_count = max(0, int(inputs.session_count))
    sessions_with_chat = max(0, int(inputs.sessions_with_chat))

    passive_viewer_samples = max(0, tracked_accounts - active_chatters)
    chatters_coverage = _safe_ratio(chatters_api_seen, tracked_accounts)
    active_ratio = _safe_ratio(active_chatters, tracked_accounts)
    chat_penetration_pct = round(active_ratio * 100.0, 1) if tracked_accounts > 0 else None

    messages_per_100_viewer_minutes = (
        round((total_messages / viewer_minutes) * 100.0, 2) if viewer_minutes > 0 else None
    )
    legacy_interaction_active_per_avg_viewer = (
        round((active_chatters / avg_viewers) * 100.0, 1) if avg_viewers > 0 else None
    )

    chat_penetration_reliable = (
        passive_viewer_samples >= PASSIVE_VIEWER_MIN_SAMPLES
        and chatters_coverage >= CHATTERS_COVERAGE_THRESHOLD
    )

    has_any_data = (
        tracked_accounts > 0 or active_chatters > 0 or total_messages > 0 or viewer_minutes > 0
    )
    if not has_any_data:
        method: QUALITY_METHOD = "no_data"
    elif chat_penetration_reliable and inputs.viewer_minutes_has_real_samples:
        method = "real_samples"
    else:
        method = "low_coverage"

    chat_session_coverage = _safe_ratio(sessions_with_chat, session_count)

    return EngagementOutputs(
        chat_penetration_pct=chat_penetration_pct,
        chat_penetration_reliable=chat_penetration_reliable,
        messages_per_100_viewer_minutes=messages_per_100_viewer_minutes,
        viewer_minutes=round(viewer_minutes, 2),
        legacy_interaction_active_per_avg_viewer=legacy_interaction_active_per_avg_viewer,
        active_ratio=round(active_ratio, 3),
        passive_viewer_samples=passive_viewer_samples,
        chatters_coverage=round(chatters_coverage, 3),
        method=method,
        chat_session_coverage=round(chat_session_coverage, 3),
    )
