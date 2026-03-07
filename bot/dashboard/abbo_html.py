"""Compatibility shim for legacy dashboard abo HTML imports."""

from ._compat import export_lazy

export_lazy(globals(), ".core.abbo_html", public=["render_abbo_page"])
