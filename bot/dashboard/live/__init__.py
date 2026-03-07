"""Live dashboard feature package."""

from .._compat import export_name_map

_EXPORT_MODULES = {
    "DashboardLiveAnnouncementMixin": ".live_announcement_mixin",
    "DashboardLiveMixin": ".live",
    "_BILLING_PLANS": ".live",
    "_CHAT_ACTION_MODES": ".live",
    "_CHAT_ANNOUNCEMENT_COLORS": ".live",
    "_CRITICAL_SCOPES": ".live",
    "_DASHBOARD_OWNER_DISCORD_ID": ".live",
    "_REQUIRED_SCOPES": ".live",
    "_SCOPE_COLUMN_LABELS": ".live",
    "_storage": ".live",
}

export_name_map(globals(), _EXPORT_MODULES)
