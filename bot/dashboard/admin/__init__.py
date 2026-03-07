"""Admin-only dashboard feature package."""

from .._compat import export_name_map

export_name_map(
    globals(),
    {
        "_DashboardLegalMixin": ".legal_mixin",
        "DashboardAdminAnnouncementMixin": ".announcement_mode_mixin",
    },
)
