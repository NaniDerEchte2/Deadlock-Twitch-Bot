"""Compatibility shim for legacy dashboard stats imports."""

from ._compat import export_lazy

export_lazy(globals(), ".core.stats", public=["DashboardStatsMixin"])
