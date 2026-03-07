"""Billing plan definitions, catalog builder and ID-mapping helpers."""

from __future__ import annotations

import json
from typing import Any

BILLING_STRIPE_QUICKSTART_URL = "https://docs.stripe.com/billing/quickstart"

BILLING_CYCLE_DISCOUNTS: dict[int, int] = {1: 0, 6: 10, 12: 20}

BILLING_PLANS: tuple[dict[str, Any], ...] = (
    {
        "id": "raid_free",
        "name": "Raid Free",
        "badge": "free",
        "description": "Starte kostenlos mit automatischen Raids in die Community.",
        "monthly_net_cents": 0,
        "recommended": False,
        "features": [
            "Auto-Raid Grundfunktion bleibt aktiv",
            "Keine monatlichen Kosten für Basis-Raids",
            "Upgrade auf Raid Boost jederzeit moeglich",
        ],
    },
    {
        "id": "raid_boost",
        "name": "Raid Boost",
        "badge": "raids",
        "description": "Dein Kanal wird bevorzugt als Raid-Ziel vorgeschlagen \u2014 mehr eingehende Zuschauer.",
        "monthly_net_cents": 799,
        "recommended": False,
        "features": [
            "Bevorzugte Platzierung im Raid-Netzwerk",
            "Sichtbarkeit auch bei deiner Inaktivit\u00e4t",
            "Kein Setup n\u00f6tig \u2014 l\u00e4uft automatisch",
        ],
    },
    {
        "id": "analysis_dashboard",
        "name": "Analyse Dashboard",
        "badge": "analytics",
        "description": "Vollst\u00e4ndiges Analytics-Dashboard mit Stream-Statistiken, Viewer-Kurven und Wachstumsvergleichen.",
        "monthly_net_cents": 1699,
        "recommended": True,
        "features": [
            "Viewer-Verlauf & Peak-Analyse pro Stream",
            "Zeitraumvergleiche und Wachstumstrends",
            "Follower- und Retention-\u00dcbersichten",
        ],
    },
    {
        "id": "bundle_analysis_raid_boost",
        "name": "Bundle: Analyse + Raid Boost",
        "badge": "bundle",
        "description": "Analyse Dashboard + Raid Boost im Paket \u2014 g\u00fcnstiger als einzeln.",
        "monthly_net_cents": 2299,
        "recommended": False,
        "features": [
            "Alle Analytics-Features inklusive",
            "Bevorzugte Raid-Platzierung aktiv",
            "Spare gegen\u00fcber Einzelbuchung",
        ],
    },
)


def normalize_billing_cycle(raw_cycle: int | str | None) -> int:
    try:
        cycle = int(raw_cycle or 1)
    except (TypeError, ValueError):
        cycle = 1
    if cycle not in BILLING_CYCLE_DISCOUNTS:
        return 1
    return cycle


def billing_cycle_label(months: int) -> str:
    if months == 1:
        return "30 Tage"
    return f"{months} Monate"


def format_eur_cents(cents: int) -> str:
    euros, remainder = divmod(max(int(cents), 0), 100)
    return f"{euros},{remainder:02d} EUR"


def build_billing_catalog(cycle_months: int | str | None) -> dict[str, Any]:
    cycle = normalize_billing_cycle(cycle_months)
    cycle_discount = int(BILLING_CYCLE_DISCOUNTS.get(cycle, 0))
    cycle_label = billing_cycle_label(cycle)
    plans: list[dict[str, Any]] = []
    for blueprint in BILLING_PLANS:
        monthly_net_cents = int(blueprint["monthly_net_cents"])
        subtotal_net_cents = monthly_net_cents * cycle
        discount_percent = cycle_discount if cycle > 1 and subtotal_net_cents > 0 else 0
        discount_cents = (
            (subtotal_net_cents * discount_percent + 50) // 100
            if discount_percent > 0
            else 0
        )
        total_net_cents = subtotal_net_cents - discount_cents
        effective_monthly_net_cents = (
            (total_net_cents + cycle // 2) // cycle if cycle > 0 else total_net_cents
        )
        plans.append(
            {
                "id": blueprint["id"],
                "name": blueprint["name"],
                "badge": blueprint["badge"],
                "description": blueprint["description"],
                "recommended": bool(blueprint.get("recommended")),
                "monthly_net_cents": monthly_net_cents,
                "features": list(blueprint.get("features", [])),
                "price": {
                    "cycle_months": cycle,
                    "cycle_label": cycle_label,
                    "subtotal_net_cents": subtotal_net_cents,
                    "discount_percent": discount_percent,
                    "discount_cents": discount_cents,
                    "total_net_cents": total_net_cents,
                    "effective_monthly_net_cents": effective_monthly_net_cents,
                    "subtotal_net_label": format_eur_cents(subtotal_net_cents),
                    "total_net_label": format_eur_cents(total_net_cents),
                    "effective_monthly_net_label": format_eur_cents(effective_monthly_net_cents),
                },
            }
        )
    return {
        "currency": "EUR",
        "tax_mode": "net_only",
        "gross_available": False,
        "cycle_months": cycle,
        "cycle_label": cycle_label,
        "discount_percent": cycle_discount if cycle > 1 else 0,
        "plans": plans,
        "payment": {
            "provider": "stripe",
            "integration_state": "planned",
            "checkout_enabled": False,
            "checkout_preview_enabled": True,
            "catalog_path": "/twitch/api/billing/catalog",
            "checkout_preview_path": "/twitch/api/billing/checkout-preview",
            "checkout_session_path": "/twitch/api/billing/checkout-session",
            "readiness_path": "/twitch/api/billing/readiness",
            "webhook_path": "/twitch/api/billing/stripe/webhook",
            "quickstart_url": BILLING_STRIPE_QUICKSTART_URL,
            "supported_methods_planned": [
                "card",
                "sepa_debit",
                "paypal_via_wallet_if_enabled",
            ],
        },
    }


def billing_value_preview(raw_value: str | None, *, secret: bool) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    if not secret:
        return value
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def billing_parse_cycle_key(raw_cycle: Any) -> int | None:
    try:
        cycle = int(raw_cycle)
    except (TypeError, ValueError):
        return None
    if cycle not in BILLING_CYCLE_DISCOUNTS:
        return None
    return cycle


def billing_parse_price_id_mapping(raw_mapping: Any) -> dict[str, dict[int, str]]:
    payload: Any = raw_mapping
    if isinstance(raw_mapping, str):
        raw = raw_mapping.strip()
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
        except Exception:
            return {}
    if not isinstance(payload, dict):
        return {}
    normalized: dict[str, dict[int, str]] = {}
    for raw_plan_id, raw_cycle_map in payload.items():
        plan_id = str(raw_plan_id or "").strip()
        if not plan_id or not isinstance(raw_cycle_map, dict):
            continue
        cycle_map: dict[int, str] = {}
        for raw_cycle, raw_price_id in raw_cycle_map.items():
            cycle = billing_parse_cycle_key(raw_cycle)
            if cycle is None:
                continue
            price_id = str(raw_price_id or "").strip()
            if price_id:
                cycle_map[cycle] = price_id
        if cycle_map:
            normalized[plan_id] = cycle_map
    return normalized


def billing_parse_product_id_mapping(raw_mapping: Any) -> dict[str, str]:
    payload: Any = raw_mapping
    if isinstance(raw_mapping, str):
        raw = raw_mapping.strip()
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
        except Exception:
            return {}
    if not isinstance(payload, dict):
        return {}
    normalized: dict[str, str] = {}
    for raw_plan_id, raw_product_id in payload.items():
        plan_id = str(raw_plan_id or "").strip()
        product_id = str(raw_product_id or "").strip()
        if plan_id and product_id:
            normalized[plan_id] = product_id
    return normalized


def billing_dump_price_id_mapping(mapping: dict[str, dict[int, str]]) -> str:
    payload: dict[str, dict[str, str]] = {}
    for plan_id in sorted(mapping.keys()):
        cycle_map = mapping.get(plan_id) or {}
        normalized_cycle_map: dict[str, str] = {}
        for cycle in sorted(cycle_map.keys()):
            price_id = str(cycle_map.get(cycle) or "").strip()
            if price_id:
                normalized_cycle_map[str(cycle)] = price_id
        if normalized_cycle_map:
            payload[plan_id] = normalized_cycle_map
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def billing_dump_product_id_mapping(mapping: dict[str, str]) -> str:
    payload: dict[str, str] = {}
    for plan_id in sorted(mapping.keys()):
        product_id = str(mapping.get(plan_id) or "").strip()
        if product_id:
            payload[plan_id] = product_id
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def billing_is_paid_plan(plan: dict[str, Any]) -> bool:
    return int(plan.get("monthly_net_cents") or 0) > 0


# ---------------------------------------------------------------------------
# Back-compat aliases — old underscore names still importable during migration
# ---------------------------------------------------------------------------
_BILLING_CYCLE_DISCOUNTS = BILLING_CYCLE_DISCOUNTS
_BILLING_PLANS = BILLING_PLANS
_build_billing_catalog = build_billing_catalog
_billing_cycle_label = billing_cycle_label
_normalize_billing_cycle = normalize_billing_cycle
_format_eur_cents = format_eur_cents
_billing_value_preview = billing_value_preview
_billing_parse_cycle_key = billing_parse_cycle_key
_billing_parse_price_id_mapping = billing_parse_price_id_mapping
_billing_parse_product_id_mapping = billing_parse_product_id_mapping
_billing_dump_price_id_mapping = billing_dump_price_id_mapping
_billing_dump_product_id_mapping = billing_dump_product_id_mapping
_billing_is_paid_plan = billing_is_paid_plan
