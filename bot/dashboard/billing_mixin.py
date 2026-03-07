"""Compatibility shim for legacy dashboard billing imports."""

from ._compat import export_lazy

export_lazy(globals(), ".billing.billing_mixin", public=["_DashboardBillingMixin"])
