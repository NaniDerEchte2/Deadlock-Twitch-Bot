"""Compatibility shim for legacy dashboard auth imports."""

from ._compat import export_lazy

export_lazy(globals(), ".auth.auth_mixin", public=["_DashboardAuthMixin"])
