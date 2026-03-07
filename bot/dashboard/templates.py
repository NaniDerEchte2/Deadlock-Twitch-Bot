"""Compatibility shim for legacy dashboard template imports."""

from ._compat import export_lazy

export_lazy(globals(), ".core.templates", public=["DashboardTemplateMixin"])
