"""Shared dashboard infrastructure helpers."""

from .._compat import export_name_map

export_name_map(
    globals(),
    {
        "DashboardStatsMixin": ".stats",
        "DashboardTemplateMixin": ".templates",
        "render_abbo_page": ".abbo_html",
    },
)
