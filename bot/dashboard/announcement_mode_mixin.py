"""Compatibility shim for legacy dashboard announcement mode imports."""

from ._compat import export_lazy

export_lazy(globals(), ".admin.announcement_mode_mixin", public=["DashboardAdminAnnouncementMixin"])
