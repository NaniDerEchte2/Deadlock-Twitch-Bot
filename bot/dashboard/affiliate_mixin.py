"""Compatibility shim for legacy dashboard affiliate imports."""

from ._compat import export_lazy

export_lazy(globals(), ".affiliate.affiliate_mixin", public=["_DashboardAffiliateMixin"])
