"""Compatibility shim for legacy dashboard legal imports."""

from ._compat import export_lazy

export_lazy(globals(), ".admin.legal_mixin", public=["_DashboardLegalMixin"])
