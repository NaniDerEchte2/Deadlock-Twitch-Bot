"""Standalone dashboard service utilities for split deployments."""

from .app import build_dashboard_service_app, run_dashboard_service
from .client import BotApiClient, BotApiClientError

__all__ = [
    "BotApiClient",
    "BotApiClientError",
    "build_dashboard_service_app",
    "run_dashboard_service",
]

