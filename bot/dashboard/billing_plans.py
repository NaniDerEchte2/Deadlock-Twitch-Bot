"""Compatibility shim for legacy dashboard billing plan imports."""

from ._compat import export_lazy

export_lazy(globals(), ".billing.billing_plans")
