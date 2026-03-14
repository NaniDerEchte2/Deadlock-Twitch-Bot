import unittest

from bot.dashboard.billing_plans import (
    _billing_dump_price_id_mapping,
    _billing_dump_product_id_mapping,
    _billing_is_paid_plan_id,
    _billing_parse_price_id_mapping,
    _billing_parse_product_id_mapping,
    _build_billing_catalog,
)


class BillingHelperTests(unittest.TestCase):
    def test_price_mapping_parser_normalizes_and_filters_invalid_entries(self) -> None:
        raw = (
            '{"analysis_dashboard":{"1":"price_month","6":"price_half","x":"bad"},'
            '"raid_boost":{"12":"price_year","1":""}}'
        )
        parsed = _billing_parse_price_id_mapping(raw)
        self.assertEqual(parsed["analysis_dashboard"][1], "price_month")
        self.assertEqual(parsed["analysis_dashboard"][6], "price_half")
        self.assertNotIn("x", parsed["analysis_dashboard"])
        self.assertEqual(parsed["raid_boost"][12], "price_year")
        self.assertNotIn(1, parsed["raid_boost"])

    def test_price_mapping_dump_roundtrip(self) -> None:
        mapping = {
            "analysis_dashboard": {1: "price_a", 6: "price_b"},
            "raid_boost": {12: "price_c"},
        }
        dumped = _billing_dump_price_id_mapping(mapping)
        parsed = _billing_parse_price_id_mapping(dumped)
        self.assertEqual(parsed, mapping)

    def test_product_mapping_dump_roundtrip(self) -> None:
        mapping = {
            "analysis_dashboard": "prod_123",
            "raid_boost": "prod_456",
        }
        dumped = _billing_dump_product_id_mapping(mapping)
        parsed = _billing_parse_product_id_mapping(dumped)
        self.assertEqual(parsed, mapping)

    def test_catalog_cycle_discount_math(self) -> None:
        catalog = _build_billing_catalog(6)
        analysis_plan = next(
            plan for plan in catalog["plans"] if str(plan.get("id")) == "analysis_dashboard"
        )
        price = dict(analysis_plan.get("price") or {})
        self.assertEqual(price.get("subtotal_net_cents"), 10194)
        self.assertEqual(price.get("discount_percent"), 10)
        self.assertEqual(price.get("discount_cents"), 1019)
        self.assertEqual(price.get("total_net_cents"), 9175)

    def test_paid_plan_id_helper_distinguishes_paid_from_free(self) -> None:
        self.assertFalse(_billing_is_paid_plan_id("raid_free"))
        self.assertTrue(_billing_is_paid_plan_id("raid_boost"))
        self.assertTrue(_billing_is_paid_plan_id("analysis_dashboard"))
        self.assertTrue(_billing_is_paid_plan_id("bundle_analysis_raid_boost"))


if __name__ == "__main__":
    unittest.main()
