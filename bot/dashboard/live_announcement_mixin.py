"""Compatibility shim for legacy live announcement imports."""

from ._compat import export_lazy

export_lazy(globals(), ".live.live_announcement_mixin", public=["DashboardLiveAnnouncementMixin"])
