"""Unit tests for the flattening + mapping + sign-normalization layer.

DB persistence (upsert, COGS lookup, formula end-to-end) is exercised by the
live validation script `scripts/validate_amazon_etl.py` against Railway,
since it requires Postgres-specific features (JSONB, ON CONFLICT constraint
references) that aren't trivial to substitute in unit tests.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from app.etl.amazon_etl import FlatLineItem, flatten_events_payload
from app.etl.pnl_mapping import (
    ADJUSTMENT_TYPE,
    PnlCategory,
    REFUND_CONTEXT_OVERRIDES,
    SERVICE_FEE_TYPE,
    SHIPMENT_CHARGE_TYPE,
    SHIPMENT_ITEM_FEE_TYPE,
    lookup_adjustment,
    lookup_shipment_charge,
    normalize_amount,
)


# -----------------------------------------------------------------------------
# Sign normalization
# -----------------------------------------------------------------------------


def test_sign_normalization_sales_passes_through():
    # Positive Sales (Principal) stays positive
    assert normalize_amount(PnlCategory.SALES, 99.95) == 99.95
    # Negative Sales (Promotion discount) stays negative and net-reduces Sales
    assert normalize_amount(PnlCategory.SALES, -5.99) == -5.99


def test_sign_normalization_reimbursements_passes_through():
    assert normalize_amount(PnlCategory.REIMBURSEMENTS, 12.50) == 12.50


def test_sign_normalization_cost_categories_invert():
    # Amazon returns most fees as negative (outflow); we store as positive cost.
    assert normalize_amount(PnlCategory.SELLING_FEES, -15.00) == 15.00
    assert normalize_amount(PnlCategory.OPERATIONAL_FEES, -25.00) == 25.00
    assert normalize_amount(PnlCategory.REFUNDS, -50.00) == 50.00
    # A positive raw on a cost category (fee reversal / credit back to seller)
    # becomes a negative cost so the formula's subtraction adds the money back.
    assert normalize_amount(PnlCategory.SELLING_FEES, 7.50) == -7.50


def test_sign_normalization_none_returns_zero():
    assert normalize_amount(PnlCategory.SALES, None) == 0.0


# -----------------------------------------------------------------------------
# Mapping tables: each PNL_MAPPING.md category has expected SP-API entries
# -----------------------------------------------------------------------------


def test_all_seven_categories_have_entries():
    """Every category from PNL_MAPPING.md must be reachable through the
    mapping tables. AD_SPEND is reached via EVENT_LIST_DEFAULT_CATEGORY, not
    via the per-line-item tables, so we cover it implicitly in the
    ProductAdsPaymentEvent test below.
    """
    categories_seen = (
        set(SHIPMENT_CHARGE_TYPE.values())
        | set(SHIPMENT_ITEM_FEE_TYPE.values())
        | set(SERVICE_FEE_TYPE.values())
        | set(ADJUSTMENT_TYPE.values())
    )
    # COGS comes from product_cogs, not from financial events
    expected = {
        PnlCategory.SALES,
        PnlCategory.SELLING_FEES,
        PnlCategory.OPERATIONAL_FEES,
        PnlCategory.REFUNDS,
        PnlCategory.REIMBURSEMENTS,
    }
    assert expected.issubset(categories_seen)


def test_pnl_mapping_md_confirmed_items_are_mapped():
    """The line items confirmed by the data specialist in PNL_MAPPING.md
    must each resolve to the documented category."""
    # Sales additions
    assert ADJUSTMENT_TYPE["Liquidations"] is PnlCategory.SALES
    assert ADJUSTMENT_TYPE["LiquidationAdjustment"] is PnlCategory.SALES
    assert ADJUSTMENT_TYPE["ProductCharges"] is PnlCategory.SALES
    # Selling Fees additions
    assert SHIPMENT_ITEM_FEE_TYPE["POAServiceFee"] is PnlCategory.SELLING_FEES
    assert SHIPMENT_ITEM_FEE_TYPE["POAPerUnitFulfillmentFee"] is PnlCategory.SELLING_FEES
    assert SHIPMENT_ITEM_FEE_TYPE["DigitalServicesFee"] is PnlCategory.SELLING_FEES
    assert SHIPMENT_ITEM_FEE_TYPE["DigitalServicesFeeFBA"] is PnlCategory.SELLING_FEES
    assert SERVICE_FEE_TYPE["DigitalServicesFeeAdjustment"] is PnlCategory.SELLING_FEES
    # Operational Fees additions
    assert SERVICE_FEE_TYPE["RetrochargeReversal"] is PnlCategory.OPERATIONAL_FEES
    assert SERVICE_FEE_TYPE["FeeAdjustment"] is PnlCategory.OPERATIONAL_FEES
    assert SERVICE_FEE_TYPE["OtherTransaction"] is PnlCategory.OPERATIONAL_FEES
    # Op Fees from the Elena/Sellerise crosswalk (2026-07-01 diff)
    assert SERVICE_FEE_TYPE["FBALongTermStorageFee"] is PnlCategory.OPERATIONAL_FEES
    assert SERVICE_FEE_TYPE["FBAInboundConvenienceFee"] is PnlCategory.OPERATIONAL_FEES
    assert SERVICE_FEE_TYPE["FBARemovalFee"] is PnlCategory.OPERATIONAL_FEES
    assert SERVICE_FEE_TYPE["FBADisposalFee"] is PnlCategory.OPERATIONAL_FEES
    assert SERVICE_FEE_TYPE["PaidServicesFee"] is PnlCategory.OPERATIONAL_FEES
    assert SERVICE_FEE_TYPE["CouponPerformanceFee"] is PnlCategory.OPERATIONAL_FEES
    assert SERVICE_FEE_TYPE["CouponParticipationFee"] is PnlCategory.OPERATIONAL_FEES
    # Reversal reimbursement lives in Op Fees per Elena's Sellerise formula
    assert ADJUSTMENT_TYPE["ReversalReimbursement"] is PnlCategory.OPERATIONAL_FEES
    # Refunds addition
    assert ADJUSTMENT_TYPE["Goodwill"] is PnlCategory.REFUNDS
    # Reimbursements addition
    assert ADJUSTMENT_TYPE["RemovalOrderLost"] is PnlCategory.REIMBURSEMENTS


def test_restocking_fee_is_operational_even_inside_refund():
    """PNL_MAPPING.md puts Restocking fee under Operational Fees regardless
    of context, so a RefundEvent containing a RestockingFee must not roll
    it into Refunds."""
    assert REFUND_CONTEXT_OVERRIDES["RestockingFee"] is PnlCategory.OPERATIONAL_FEES


# -----------------------------------------------------------------------------
# Flattening
# -----------------------------------------------------------------------------


def _amt(value: float, code: str = "USD") -> dict:
    return {"CurrencyAmount": value, "CurrencyCode": code}


SHIPMENT_FIXTURE = {
    "AmazonOrderId": "111-1111111-1111111",
    "PostedDate": "2025-01-15T12:00:00Z",
    "MarketplaceName": "Amazon.com",
    "ShipmentItemList": [
        {
            "SellerSKU": "MBDBOX1",
            "OrderItemId": "item-1",
            "QuantityShipped": 2,
            "ItemChargeList": [
                {"ChargeType": "Principal", "ChargeAmount": _amt(99.95)},
                {"ChargeType": "Tax", "ChargeAmount": _amt(8.50)},
                {"ChargeType": "Shipping", "ChargeAmount": _amt(5.99)},
            ],
            "ItemFeeList": [
                {"FeeType": "Commission", "FeeAmount": _amt(-15.00)},
                {"FeeType": "FBAPerUnitFulfillmentFee", "FeeAmount": _amt(-5.50)},
            ],
            "PromotionList": [
                {"PromotionType": "Shipping", "PromotionAmount": _amt(-5.99)},
            ],
        }
    ],
}

REFUND_FIXTURE = {
    "AmazonOrderId": "111-2222222-2222222",
    "PostedDate": "2025-01-16T10:00:00Z",
    "MarketplaceName": "Amazon.com",
    "ShipmentItemList": [
        {
            "SellerSKU": "MBDBOX1",
            "OrderItemId": "item-2",
            "QuantityShipped": 1,
            "ItemChargeList": [
                {"ChargeType": "Principal", "ChargeAmount": _amt(-50.00)},
                {"ChargeType": "Tax", "ChargeAmount": _amt(-4.25)},
            ],
            "ItemFeeList": [
                {"FeeType": "Commission", "FeeAmount": _amt(7.50)},
                {"FeeType": "RefundCommission", "FeeAmount": _amt(-2.50)},
            ],
        }
    ],
}

REFUND_WITH_RESTOCKING_FIXTURE = {
    "AmazonOrderId": "111-3333333-3333333",
    "PostedDate": "2025-01-17T10:00:00Z",
    "MarketplaceName": "Amazon.com",
    "ShipmentItemList": [
        {
            "SellerSKU": "MBDBOX1",
            "ItemChargeList": [
                {"ChargeType": "Principal", "ChargeAmount": _amt(-50.00)},
                {"ChargeType": "RestockingFee", "ChargeAmount": _amt(10.00)},
            ],
            "ItemFeeList": [],
        }
    ],
}

SERVICE_FEE_FIXTURE = {
    "PostedDate": "2025-01-18T00:00:00Z",
    "FeeReason": "FBAStorageFee",
    "FeeList": [
        {"FeeType": "FBAStorageFee", "FeeAmount": _amt(-25.00)},
    ],
    "MarketplaceName": "Amazon.com",
    "ASIN": "B0CX1XSLV7",
    "SellerSKU": "MBDBOX1",
}

ADJUSTMENT_REIMBURSEMENT_FIXTURE = {
    "PostedDate": "2025-01-19T00:00:00Z",
    "AdjustmentType": "ReversalReimbursement",
    "AdjustmentAmount": _amt(12.50),
    "AdjustmentItemList": [
        {
            "SellerSKU": "MBDBOX1",
            "ASIN": "B0CX1XSLV7",
            "Quantity": "1",
            "TotalAmount": _amt(12.50),
        }
    ],
}

ADJUSTMENT_LIQUIDATION_FIXTURE = {
    "PostedDate": "2025-01-20T00:00:00Z",
    "AdjustmentType": "Liquidations",
    "AdjustmentAmount": _amt(75.00),
}

PRODUCT_ADS_PAYMENT_FIXTURE = {
    "postedDate": "2025-01-21T00:00:00Z",
    "transactionType": "charge",
    "invoiceId": "ad-invoice-1",
    "transactionValue": _amt(-50.00),
}

UNKNOWN_EVENT_FIXTURE = {
    "PostedDate": "2025-01-22T00:00:00Z",
    "TotalAmount": _amt(-3.00),
}


def _build_payload(**lists) -> dict:
    return {k: v for k, v in lists.items() if v is not None}


def _items_by_category(items):
    out: dict[str, list] = {}
    for item in items:
        key = item.category.value if item.category else "_unmapped"
        out.setdefault(key, []).append(item)
    return out


def test_flatten_shipment_event_charges_fees_and_promotions():
    items = list(
        flatten_events_payload(
            _build_payload(ShipmentEventList=[SHIPMENT_FIXTURE]), region="NA"
        )
    )
    by_cat = _items_by_category(items)
    # 3 charges + 1 promotion -> Sales; 2 fees -> Selling Fees
    sales_types = sorted(i.fee_type for i in by_cat["sales"])
    assert sales_types == ["Principal", "Promotion:Shipping", "Shipping", "Tax"]
    selling_types = sorted(i.fee_type for i in by_cat["selling_fees"])
    assert selling_types == ["Commission", "FBAPerUnitFulfillmentFee"]
    # Fees stored as positive cost
    commission = next(i for i in by_cat["selling_fees"] if i.fee_type == "Commission")
    assert commission.raw_amount == Decimal("-15.00")
    assert commission.fee_amount == Decimal("15.00")
    # MarketplaceName resolves to US marketplace_id
    assert all(i.marketplace_id == "ATVPDKIKX0DER" for i in items)
    # SKU and quantity preserved
    assert all(i.sku == "MBDBOX1" and i.quantity == 2 for i in items)


def test_flatten_refund_event_routes_everything_to_refunds():
    items = list(
        flatten_events_payload(
            _build_payload(RefundEventList=[REFUND_FIXTURE]), region="NA"
        )
    )
    by_cat = _items_by_category(items)
    # All four line items -> Refunds
    assert set(by_cat.keys()) == {"refunds"}
    types = sorted(i.fee_type for i in by_cat["refunds"])
    assert types == ["Commission", "Principal", "RefundCommission", "Tax"]
    # Sign normalization: Principal -50 -> stored cost +50
    principal = next(i for i in by_cat["refunds"] if i.fee_type == "Principal")
    assert principal.raw_amount == Decimal("-50.00")
    assert principal.fee_amount == Decimal("50.00")
    # Commission +7.50 (refunded back to seller) -> stored cost -7.50 (reduces refund cost)
    commission = next(i for i in by_cat["refunds"] if i.fee_type == "Commission")
    assert commission.raw_amount == Decimal("7.50")
    assert commission.fee_amount == Decimal("-7.50")


def test_restocking_fee_inside_refund_lands_in_operational_fees():
    items = list(
        flatten_events_payload(
            _build_payload(RefundEventList=[REFUND_WITH_RESTOCKING_FIXTURE]),
            region="NA",
        )
    )
    by_cat = _items_by_category(items)
    assert {i.fee_type for i in by_cat.get("operational_fees", [])} == {"RestockingFee"}
    assert {i.fee_type for i in by_cat.get("refunds", [])} == {"Principal"}


def test_flatten_service_fee_event_maps_storage_to_operational():
    items = list(
        flatten_events_payload(
            _build_payload(ServiceFeeEventList=[SERVICE_FEE_FIXTURE]),
            region="NA",
        )
    )
    assert len(items) == 1
    assert items[0].category is PnlCategory.OPERATIONAL_FEES
    assert items[0].fee_type == "FBAStorageFee"
    assert items[0].raw_amount == Decimal("-25.00")
    assert items[0].fee_amount == Decimal("25.00")


def test_service_fee_types_from_elena_crosswalk_all_map_to_op_fees():
    """After the 2026-07-01 line-item diff against Elena's Sellerise sheet,
    the following SP-API fee_type strings all belong to Op Fees. Prior to
    this fix they were landing in unmapped_line_items and dropping ~$5.6k
    of Jan 2026 US Op Fees on the floor."""
    for fee_type in (
        "FBALongTermStorageFee",     # Elena: Storage renewal billing
        "FBAInboundConvenienceFee",  # Elena: FBA inbound placement service fee
        "FBARemovalFee",             # Elena: Removal complete
        "FBADisposalFee",            # Elena: Disposal complete
        "PaidServicesFee",           # Elena: Premium services fee
        "CouponPerformanceFee",      # Elena: Amazon fees (aggregate)
        "CouponParticipationFee",    # Elena: Amazon fees (aggregate)
        "CustomerReturnHRRUnitFee",  # Not in Elena's list, still Op Fee
    ):
        fixture = {
            "PostedDate": "2026-01-15T10:00:00Z",
            "FeeList": [
                {"FeeType": fee_type, "FeeAmount": _amt(-10.00)},
            ],
        }
        items = list(
            flatten_events_payload(
                _build_payload(ServiceFeeEventList=[fixture]),
                region="NA",
            )
        )
        assert len(items) == 1, f"{fee_type} produced no line item"
        assert items[0].category is PnlCategory.OPERATIONAL_FEES, (
            f"{fee_type} mapped to {items[0].category} not OPERATIONAL_FEES"
        )
        assert items[0].fee_type == fee_type
        assert items[0].fee_amount == Decimal("10.00")  # cost cat inverts sign


def test_flatten_adjustment_reversal_reimbursement_is_op_fee_offset():
    """Elena's Sellerise formula puts ReversalReimbursement inside Op Fees
    as a negative-expense offset (Amazon reversing a prior fee). Raw is
    positive (cash coming back); fee_amount is negative because
    normalize_amount inverts for cost categories — subtracting a negative
    fee from the P&L formula reduces total Op Fees, matching Elena."""
    items = list(
        flatten_events_payload(
            _build_payload(AdjustmentEventList=[ADJUSTMENT_REIMBURSEMENT_FIXTURE]),
            region="NA",
        )
    )
    assert len(items) == 1
    assert items[0].category is PnlCategory.OPERATIONAL_FEES
    assert items[0].raw_amount == Decimal("12.50")
    assert items[0].fee_amount == Decimal("-12.50")  # cost cat inverts sign
    assert items[0].sku == "MBDBOX1"
    assert items[0].quantity == 1


def test_flatten_adjustment_liquidation_maps_to_sales():
    items = list(
        flatten_events_payload(
            _build_payload(AdjustmentEventList=[ADJUSTMENT_LIQUIDATION_FIXTURE]),
            region="NA",
        )
    )
    assert len(items) == 1
    assert items[0].category is PnlCategory.SALES
    assert items[0].raw_amount == Decimal("75.00")


def test_flatten_product_ads_payment_maps_to_ad_spend():
    items = list(
        flatten_events_payload(
            _build_payload(ProductAdsPaymentEventList=[PRODUCT_ADS_PAYMENT_FIXTURE]),
            region="NA",
        )
    )
    assert len(items) == 1
    assert items[0].category is PnlCategory.AD_SPEND
    # AD_SPEND is a cost category; -50 raw becomes +50 stored
    assert items[0].fee_amount == Decimal("50.00")


def test_unknown_event_type_is_captured_as_unmapped_not_dropped():
    items = list(
        flatten_events_payload(
            _build_payload(MysteryFutureEventList=[UNKNOWN_EVENT_FIXTURE]),
            region="NA",
        )
    )
    assert len(items) == 1
    assert items[0].category is None  # unmapped
    assert items[0].event_type == "MysteryFutureEvent"
    assert items[0].fee_type == "MysteryFutureEventList"
    assert items[0].raw_amount == Decimal("-3.00")


def test_unknown_charge_type_inside_known_event_logs_unmapped():
    """A new ChargeType inside a known ShipmentEvent must surface in
    unmapped_line_items rather than be silently dropped."""
    fixture = {
        "AmazonOrderId": "111-X",
        "PostedDate": "2025-01-23T00:00:00Z",
        "MarketplaceName": "Amazon.com",
        "ShipmentItemList": [
            {
                "SellerSKU": "SKU-X",
                "QuantityShipped": 1,
                "ItemChargeList": [
                    {"ChargeType": "Principal", "ChargeAmount": _amt(20.00)},
                    {"ChargeType": "BrandNewMysteryCharge", "ChargeAmount": _amt(-1.00)},
                ],
            }
        ],
    }
    items = list(
        flatten_events_payload(
            _build_payload(ShipmentEventList=[fixture]), region="NA"
        )
    )
    by_cat = _items_by_category(items)
    assert {i.fee_type for i in by_cat.get("sales", [])} == {"Principal"}
    assert by_cat.get("_unmapped"), "unmapped charge must be captured"
    assert by_cat["_unmapped"][0].fee_type == "BrandNewMysteryCharge"


def test_unknown_marketplace_falls_back_to_region_primary():
    fixture = {
        "PostedDate": "2025-01-22T00:00:00Z",
        "AdjustmentType": "Subscription",  # not in ADJUSTMENT_TYPE -> unmapped
        "AdjustmentAmount": _amt(-39.99),
    }
    items = list(
        flatten_events_payload(
            _build_payload(AdjustmentEventList=[fixture]), region="NA"
        )
    )
    assert items[0].marketplace_id == "ATVPDKIKX0DER"


def test_region_resolution_picks_correct_primary():
    fixture = {
        "PostedDate": "2025-01-22T00:00:00Z",
        "AdjustmentType": "Goodwill",
        "AdjustmentAmount": _amt(-5.00),
    }
    for region, expected in [
        ("NA", "ATVPDKIKX0DER"),
        ("EU", "A1F83G8C2ARO7P"),
        ("FE", "A39IBJ37TRP1C6"),
    ]:
        items = list(
            flatten_events_payload(
                _build_payload(AdjustmentEventList=[fixture]), region=region
            )
        )
        assert items[0].marketplace_id == expected


def test_all_seven_categories_appear_in_a_combined_payload():
    # Elena's Sellerise formula now puts ReversalReimbursement in Op Fees, so
    # we need a different AdjustmentType to exercise the REIMBURSEMENTS bucket
    # here — MissingFromInbound stays in Reimbursements.
    missing_from_inbound_fixture = {
        "PostedDate": "2025-01-19T00:00:00Z",
        "AdjustmentType": "MissingFromInbound",
        "AdjustmentAmount": _amt(50.00),
    }
    payload = _build_payload(
        ShipmentEventList=[SHIPMENT_FIXTURE],  # SALES + SELLING_FEES
        RefundEventList=[REFUND_FIXTURE],  # REFUNDS
        ServiceFeeEventList=[SERVICE_FEE_FIXTURE],  # OPERATIONAL_FEES
        AdjustmentEventList=[
            missing_from_inbound_fixture,  # REIMBURSEMENTS
            ADJUSTMENT_REIMBURSEMENT_FIXTURE,  # OPERATIONAL_FEES (per Elena)
            ADJUSTMENT_LIQUIDATION_FIXTURE,  # SALES (via liquidations)
        ],
        ProductAdsPaymentEventList=[PRODUCT_ADS_PAYMENT_FIXTURE],  # AD_SPEND
    )
    items = list(flatten_events_payload(payload, region="NA"))
    seen = {i.category.value for i in items if i.category}
    # COGS is not a financial-event category; it comes from product_cogs lookup.
    assert seen == {
        "sales",
        "selling_fees",
        "operational_fees",
        "refunds",
        "reimbursements",
        "ad_spend",
    }


def test_posted_date_is_parsed_with_tzaware_utc():
    items = list(
        flatten_events_payload(
            _build_payload(ShipmentEventList=[SHIPMENT_FIXTURE]), region="NA"
        )
    )
    assert items[0].posted_date == datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)


def test_event_type_marker_is_refundevent_for_refund_context():
    items = list(
        flatten_events_payload(
            _build_payload(RefundEventList=[REFUND_FIXTURE]), region="NA"
        )
    )
    assert {i.event_type for i in items} == {"RefundEvent"}


def test_shipment_event_marketplace_resolution_uses_marketplace_name():
    """A NA-region shipment with MarketplaceName=Amazon.ca must be attributed
    to CA, not the NA primary (US)."""
    fixture = {
        "AmazonOrderId": "111-CA",
        "PostedDate": "2025-01-15T12:00:00Z",
        "MarketplaceName": "Amazon.ca",
        "ShipmentItemList": [
            {
                "SellerSKU": "MBDBOX1",
                "QuantityShipped": 1,
                "ItemChargeList": [{"ChargeType": "Principal", "ChargeAmount": _amt(50.00, "CAD")}],
            }
        ],
    }
    items = list(
        flatten_events_payload(
            _build_payload(ShipmentEventList=[fixture]), region="NA"
        )
    )
    assert items[0].marketplace_id == "A2EUQ1WTGCTBG2"
    assert items[0].currency == "CAD"


def test_servicefee_without_posted_date_or_marketplace_uses_fallbacks():
    """This seller's ServiceFeeEvents arrive with only AmazonOrderId + FeeList
    (no PostedDate, no MarketplaceName). The flattener must:
      1. fall back to the supplied default_posted so the row aggregates
      2. infer marketplace from FeeList[*].FeeAmount.CurrencyCode
    Without these fallbacks, every storage/subscription/disposal fee is
    invisible to the daily aggregation and idempotency breaks.
    """
    fixture = {
        "AmazonOrderId": "702-8761411-1609006",
        "FeeList": [
            {
                "FeeType": "FBADisposalFee",
                "FeeAmount": {"CurrencyAmount": -5.02, "CurrencyCode": "CAD"},
            }
        ],
    }
    window_start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    items = list(
        flatten_events_payload(
            _build_payload(ServiceFeeEventList=[fixture]),
            region="NA",
            default_posted=window_start,
        )
    )
    assert len(items) == 1
    assert items[0].posted_date == window_start
    # CAD currency -> CA marketplace, not US (which would be the region primary)
    assert items[0].marketplace_id == "A2EUQ1WTGCTBG2"


def test_adjustment_event_without_posted_date_uses_fallback_and_currency():
    fixture = {
        "AdjustmentType": "REVERSAL_REIMBURSEMENT",
        "AdjustmentAmount": {"CurrencyAmount": 12.50, "CurrencyCode": "USD"},
    }
    window_start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    items = list(
        flatten_events_payload(
            _build_payload(AdjustmentEventList=[fixture]),
            region="NA",
            default_posted=window_start,
        )
    )
    assert items[0].posted_date == window_start
    assert items[0].marketplace_id == "ATVPDKIKX0DER"
    assert items[0].category is PnlCategory.OPERATIONAL_FEES


def test_explicit_posted_date_overrides_fallback():
    fixture = {
        "PostedDate": "2026-01-15T10:00:00Z",
        "FeeList": [
            {
                "FeeType": "FBAStorageFee",
                "FeeAmount": {"CurrencyAmount": -3.00, "CurrencyCode": "USD"},
            }
        ],
    }
    window_start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    items = list(
        flatten_events_payload(
            _build_payload(ServiceFeeEventList=[fixture]),
            region="NA",
            default_posted=window_start,
        )
    )
    assert items[0].posted_date == datetime(2026, 1, 15, 10, 0, 0, tzinfo=timezone.utc)


def test_screaming_snake_case_adjustment_types_resolve_to_camelcase_categories():
    """SP-API uses both CamelCase ('ReversalReimbursement') and
    SCREAMING_SNAKE_CASE ('REVERSAL_REIMBURSEMENT') for the same identifier
    in different endpoints. Both forms must resolve to the same category."""
    fixtures = [
        ("REVERSAL_REIMBURSEMENT", PnlCategory.OPERATIONAL_FEES),
        ("COMPENSATED_CLAWBACK", PnlCategory.OPERATIONAL_FEES),
        ("WAREHOUSE_LOST", PnlCategory.REIMBURSEMENTS),
        ("WAREHOUSE_DAMAGE", PnlCategory.REIMBURSEMENTS),
        ("FREE_REPLACEMENT_REFUND_ITEMS", PnlCategory.OPERATIONAL_FEES),
    ]
    for adj_type, expected in fixtures:
        event = {
            "PostedDate": "2025-01-22T00:00:00Z",
            "AdjustmentType": adj_type,
            "AdjustmentAmount": _amt(10.00),
        }
        items = list(
            flatten_events_payload(
                _build_payload(AdjustmentEventList=[event]), region="NA"
            )
        )
        assert items[0].category is expected, f"{adj_type} should map to {expected.value}"


def test_shipping_charge_synonym_maps_to_sales():
    """ShipmentEvent.ItemChargeList uses 'ShippingCharge' as a synonym for
    'Shipping' in some payloads. Both must land in Sales."""
    fixture = {
        "AmazonOrderId": "111-Z",
        "PostedDate": "2025-01-22T00:00:00Z",
        "MarketplaceName": "Amazon.com",
        "ShipmentItemList": [
            {
                "SellerSKU": "SKU-Z",
                "QuantityShipped": 1,
                "ItemChargeList": [
                    {"ChargeType": "ShippingCharge", "ChargeAmount": _amt(4.99)},
                    {"ChargeType": "SalesTaxCollectionFee", "ChargeAmount": _amt(0.0)},
                ],
            }
        ],
    }
    items = list(
        flatten_events_payload(
            _build_payload(ShipmentEventList=[fixture]), region="NA"
        )
    )
    by_cat = _items_by_category(items)
    assert {i.fee_type for i in by_cat.get("sales", [])} == {"ShippingCharge"}
    assert {i.fee_type for i in by_cat.get("selling_fees", [])} == {"SalesTaxCollectionFee"}


@pytest.mark.parametrize(
    "category,raw,expected",
    [
        (PnlCategory.SALES, 100.00, 100.00),
        (PnlCategory.SALES, -10.00, -10.00),
        (PnlCategory.REIMBURSEMENTS, 25.00, 25.00),
        (PnlCategory.SELLING_FEES, -15.00, 15.00),
        (PnlCategory.SELLING_FEES, 5.00, -5.00),
        (PnlCategory.OPERATIONAL_FEES, -30.00, 30.00),
        (PnlCategory.REFUNDS, -50.00, 50.00),
        (PnlCategory.AD_SPEND, -100.00, 100.00),
    ],
)
def test_sign_normalization_table(category, raw, expected):
    assert normalize_amount(category, raw) == expected
