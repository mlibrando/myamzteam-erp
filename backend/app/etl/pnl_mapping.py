"""Canonical mapping: SP-API raw line-item identifier -> P&L category.

This is the **machine-readable** version of PNL_MAPPING.md. The repo-root
PNL_MAPPING.md uses Elena's human-readable labels (e.g. "Fba per unit
fulfillment fee"); the SP-API uses CamelCase identifiers (e.g.
"FBAPerUnitFulfillmentFee"). Each entry here corresponds to a bullet in
PNL_MAPPING.md. When PNL_MAPPING.md changes, this file must change too.

Any SP-API identifier not present in one of these dicts is treated as
unmapped: it gets logged to `unmapped_line_items` and is NOT silently
allocated to a default bucket.
"""

from __future__ import annotations

from enum import Enum


class PnlCategory(str, Enum):
    SALES = "sales"
    COGS = "cogs"
    AD_SPEND = "ad_spend"
    SELLING_FEES = "selling_fees"
    OPERATIONAL_FEES = "operational_fees"
    REFUNDS = "refunds"
    REIMBURSEMENTS = "reimbursements"


# -----------------------------------------------------------------------------
# ShipmentEvent and RefundEvent share the same ChargeType / FeeType vocabulary.
# In a ShipmentEvent the amounts are the seller's gross receipts and Amazon's
# fees deducted; in a RefundEvent the same fields reappear as refund
# components. The ETL switches mapping table based on the parent event type:
#   - ShipmentEvent.ItemChargeList   -> SHIPMENT_CHARGE_TYPE
#   - ShipmentEvent.ItemFeeList      -> SHIPMENT_ITEM_FEE_TYPE
#   - ShipmentEvent.PromotionList    -> SHIPMENT_PROMOTION (rolled into Sales)
#   - RefundEvent.ItemChargeList     -> all Refunds
#   - RefundEvent.ItemFeeList        -> all Refunds
#   - RefundEvent.PromotionList      -> all Refunds
# Refund-event line items are uniformly Refunds regardless of the underlying
# ChargeType/FeeType because that's how Elena's spreadsheet aggregates them
# (PNL_MAPPING.md "Refunds" section: "Refund - Product sales", "Refund - Tax",
# "Refund commission", ...).
# -----------------------------------------------------------------------------

# ShipmentEvent.ShipmentItemList[].ItemChargeList[].ChargeType
SHIPMENT_CHARGE_TYPE: dict[str, PnlCategory] = {
    "Principal": PnlCategory.SALES,
    "Tax": PnlCategory.SALES,
    "Shipping": PnlCategory.SALES,
    "ShippingCharge": PnlCategory.SALES,  # synonym used in some shipment payloads
    "ShippingTax": PnlCategory.SALES,
    "GiftWrap": PnlCategory.SALES,
    "GiftWrapTax": PnlCategory.SALES,
    "ReturnShipping": PnlCategory.SALES,
    "Goodwill": PnlCategory.REFUNDS,
    "RestockingFee": PnlCategory.OPERATIONAL_FEES,
    "ExportCharge": PnlCategory.SALES,
    # Reported-only tax field carrying $0; map to Selling Fees so it stays
    # accounted for rather than landing in unmapped.
    "SalesTaxCollectionFee": PnlCategory.SELLING_FEES,
    "SalesTax": PnlCategory.SALES,
}

# ShipmentEvent.ShipmentItemList[].ItemFeeList[].FeeType
SHIPMENT_ITEM_FEE_TYPE: dict[str, PnlCategory] = {
    "FBAPerUnitFulfillmentFee": PnlCategory.SELLING_FEES,
    "FBAPerOrderFulfillmentFee": PnlCategory.SELLING_FEES,
    "FBAWeightBasedFee": PnlCategory.SELLING_FEES,
    "Commission": PnlCategory.SELLING_FEES,
    "ShippingChargeback": PnlCategory.SELLING_FEES,
    "GiftwrapChargeback": PnlCategory.SELLING_FEES,
    "FixedClosingFee": PnlCategory.SELLING_FEES,
    "VariableClosingFee": PnlCategory.SELLING_FEES,
    "PerOrderFee": PnlCategory.SELLING_FEES,
    "PerItemFee": PnlCategory.SELLING_FEES,
    "RenewedProgramFee": PnlCategory.SELLING_FEES,
    "DigitalServicesFee": PnlCategory.SELLING_FEES,
    "DigitalServicesFeeFBA": PnlCategory.SELLING_FEES,
    "POAServiceFee": PnlCategory.SELLING_FEES,
    "POAPerUnitFulfillmentFee": PnlCategory.SELLING_FEES,
    "RefundCommission": PnlCategory.REFUNDS,
    # SP-API also emits SalesTaxCollectionFee inside ItemFeeList; treat as Selling Fees
    # (it's a $0 reporting line in practice but should not be unmapped).
    "SalesTaxCollectionFee": PnlCategory.SELLING_FEES,
}

# ServiceFeeEvent.FeeList[].FeeType and ServiceFeeEvent.FeeReason
# (Many ServiceFeeEvents have a single-element FeeList where FeeType==FeeReason;
# this table covers both lookups.)
#
# SP-API and Sellerise use different vocabulary for the same Amazon-side fee.
# Elena's spreadsheet uses Sellerise labels; we consolidate both into
# PnlCategory buckets so the aggregate matches her formula. Confirmed by
# line-item diff against Elena's Jan 2026 US RAW_AMZ_US sheet (2026-07-01):
#   SP-API label                    | Elena's Sellerise label
#   FBALongTermStorageFee           -> Storage renewal billing
#   FBAInboundConvenienceFee        -> FBA inbound placement service fee
#   FBARemovalFee                   -> Removal complete
#   FBADisposalFee                  -> Disposal complete
#   PaidServicesFee                 -> Premium services fee
#   CouponPerformanceFee +          -> Amazon fees (Elena aggregates the two
#     CouponParticipationFee           coupon fees under one label)
#   CustomerReturnHRRUnitFee        -> (not in Elena's list, tiny amount)
SERVICE_FEE_TYPE: dict[str, PnlCategory] = {
    # PNL_MAPPING.md Operational Fees
    "FBAStorageFee": PnlCategory.OPERATIONAL_FEES,
    "FBALongTermStorageFee": PnlCategory.OPERATIONAL_FEES,
    "StorageRenewalBilling": PnlCategory.OPERATIONAL_FEES,
    "FBAInboundTransportationFee": PnlCategory.OPERATIONAL_FEES,
    "FBAInboundTransportationFeeAdjustment": PnlCategory.OPERATIONAL_FEES,
    "FBAInboundPlacementServiceFee": PnlCategory.OPERATIONAL_FEES,
    "FBAInboundConvenienceFee": PnlCategory.OPERATIONAL_FEES,
    "FBARemovalFee": PnlCategory.OPERATIONAL_FEES,
    "FBADisposalFee": PnlCategory.OPERATIONAL_FEES,
    "Subscription": PnlCategory.OPERATIONAL_FEES,
    "PremiumServiceFee": PnlCategory.OPERATIONAL_FEES,
    "PaidServicesFee": PnlCategory.OPERATIONAL_FEES,
    "CouponPerformanceFee": PnlCategory.OPERATIONAL_FEES,
    "CouponParticipationFee": PnlCategory.OPERATIONAL_FEES,
    "CustomerReturnHRRUnitFee": PnlCategory.OPERATIONAL_FEES,
    "DisposalComplete": PnlCategory.OPERATIONAL_FEES,
    "RemovalComplete": PnlCategory.OPERATIONAL_FEES,
    "FreeReplacementRefundItems": PnlCategory.OPERATIONAL_FEES,
    "MissingFromInboundClawback": PnlCategory.OPERATIONAL_FEES,
    "CompensatedClawback": PnlCategory.OPERATIONAL_FEES,
    "FeeAdjustment": PnlCategory.OPERATIONAL_FEES,
    "OtherTransaction": PnlCategory.OPERATIONAL_FEES,
    "Retrocharge": PnlCategory.OPERATIONAL_FEES,
    "RetrochargeReversal": PnlCategory.OPERATIONAL_FEES,
    "RestockingFee": PnlCategory.OPERATIONAL_FEES,
    # PNL_MAPPING.md Selling Fees that arrive as ServiceFees (UK/CA VAT and POA)
    "DigitalServicesFee": PnlCategory.SELLING_FEES,
    "DigitalServicesFeeFBA": PnlCategory.SELLING_FEES,
    "DigitalServicesFeeAdjustment": PnlCategory.SELLING_FEES,
    "POAServiceFee": PnlCategory.SELLING_FEES,
    "POAPerUnitFulfillmentFee": PnlCategory.SELLING_FEES,
}

# AdjustmentEvent.AdjustmentType
#
# Elena's Sellerise column layout puts "Reversal reimbursement" inside the
# Op Fees formula, not Reimbursements — a reversal of an earlier
# Amazon-side fee (e.g. Amazon reimburses a mistakenly-charged fee) is
# treated as a negative expense that partially offsets Op Fees. Confirmed
# 2026-07-01 against Elena's Jan 2026 US RAW_AMZ_US sheet. Semantically
# reasonable — the money doesn't come from customer refunds or lost
# inventory; it comes from Amazon reversing an operational fee.
ADJUSTMENT_TYPE: dict[str, PnlCategory] = {
    # PNL_MAPPING.md Reimbursements from AMZ
    "ReversalReimbursement": PnlCategory.OPERATIONAL_FEES,
    "MissingFromInbound": PnlCategory.REIMBURSEMENTS,
    "WarehouseDamage": PnlCategory.REIMBURSEMENTS,
    "WarehouseLost": PnlCategory.REIMBURSEMENTS,
    "RemovalOrderLost": PnlCategory.REIMBURSEMENTS,
    # SP-API also emits these FBA inventory reimbursement subtypes
    "FBAInventoryReimbursement-Customer-Return": PnlCategory.REIMBURSEMENTS,
    "FBAInventoryReimbursement-Damaged-Warehouse": PnlCategory.REIMBURSEMENTS,
    "FBAInventoryReimbursement-Lost-Warehouse": PnlCategory.REIMBURSEMENTS,
    "FBAInventoryReimbursement-Lost-Inbound": PnlCategory.REIMBURSEMENTS,
    "FBAInventoryReimbursement-Missing-Inbound": PnlCategory.REIMBURSEMENTS,
    "FBAInventoryReimbursement-Removal-Lost": PnlCategory.REIMBURSEMENTS,
    "FBAInventoryReimbursement-General-Adjustment": PnlCategory.REIMBURSEMENTS,
    # PNL_MAPPING.md Refunds
    "Goodwill": PnlCategory.REFUNDS,
    # PNL_MAPPING.md Sales (FBA liquidation revenue, not an operational expense)
    "Liquidations": PnlCategory.SALES,
    "LiquidationAdjustment": PnlCategory.SALES,
    "ProductCharges": PnlCategory.SALES,
    # PNL_MAPPING.md Operational Fees
    "Retrocharge": PnlCategory.OPERATIONAL_FEES,
    "RetrochargeReversal": PnlCategory.OPERATIONAL_FEES,
    "PostageBilling": PnlCategory.OPERATIONAL_FEES,
    "FeeAdjustment": PnlCategory.OPERATIONAL_FEES,
    "OtherTransaction": PnlCategory.OPERATIONAL_FEES,
    # Also reachable as adjustment types (mirror of ServiceFeeEvent reasons)
    "CompensatedClawback": PnlCategory.OPERATIONAL_FEES,
    "MissingFromInboundClawback": PnlCategory.OPERATIONAL_FEES,
    "FreeReplacementRefundItems": PnlCategory.OPERATIONAL_FEES,
    "DisposalComplete": PnlCategory.OPERATIONAL_FEES,
    "RemovalComplete": PnlCategory.OPERATIONAL_FEES,
}


# SP-API emits the same identifier in CamelCase in some endpoints and in
# SCREAMING_SNAKE_CASE in others (e.g. "ReversalReimbursement" vs
# "REVERSAL_REIMBURSEMENT"). Lookups normalize on both sides so either form
# resolves to the same category.
def _normalize_key(key: str) -> str:
    return key.replace("_", "").replace("-", "").lower()


def _build_normalized_lookup(table: dict[str, PnlCategory]) -> dict[str, PnlCategory]:
    out: dict[str, PnlCategory] = {}
    for raw_key, value in table.items():
        out[_normalize_key(raw_key)] = value
    return out


# Inside a RefundEvent (or sibling: GuaranteeClaimEvent, ChargebackEvent),
# all line items default to REFUNDS regardless of their underlying
# ChargeType/FeeType -- "Refund - Product sales", "Refund - Tax",
# "Refund commission" etc. per PNL_MAPPING.md are all bookings in the
# Refunds category. EXCEPT for these specific identifiers, which retain
# their non-refund category even when they appear embedded in a refund
# event (e.g. RestockingFee is an Operational Fee regardless of context).
REFUND_CONTEXT_OVERRIDES: dict[str, PnlCategory] = {
    "RestockingFee": PnlCategory.OPERATIONAL_FEES,
    "Goodwill": PnlCategory.REFUNDS,  # already refund, kept here for clarity
}


SHIPMENT_CHARGE_TYPE_NORMALIZED = _build_normalized_lookup(SHIPMENT_CHARGE_TYPE)
SHIPMENT_ITEM_FEE_TYPE_NORMALIZED = _build_normalized_lookup(SHIPMENT_ITEM_FEE_TYPE)
SERVICE_FEE_TYPE_NORMALIZED = _build_normalized_lookup(SERVICE_FEE_TYPE)
ADJUSTMENT_TYPE_NORMALIZED = _build_normalized_lookup(ADJUSTMENT_TYPE)
REFUND_CONTEXT_OVERRIDES_NORMALIZED = _build_normalized_lookup(REFUND_CONTEXT_OVERRIDES)


def lookup_shipment_charge(key: str) -> PnlCategory | None:
    return SHIPMENT_CHARGE_TYPE_NORMALIZED.get(_normalize_key(key))


def lookup_shipment_item_fee(key: str) -> PnlCategory | None:
    return SHIPMENT_ITEM_FEE_TYPE_NORMALIZED.get(_normalize_key(key))


def lookup_service_fee(key: str) -> PnlCategory | None:
    return SERVICE_FEE_TYPE_NORMALIZED.get(_normalize_key(key))


def lookup_adjustment(key: str) -> PnlCategory | None:
    return ADJUSTMENT_TYPE_NORMALIZED.get(_normalize_key(key))


def lookup_refund_context_override(key: str) -> PnlCategory | None:
    return REFUND_CONTEXT_OVERRIDES_NORMALIZED.get(_normalize_key(key))

# Per-event-list fallback when an event has no explicit line items but does
# carry a top-level amount (rare).
#
# ProductAdsPaymentEventList is mapped to AD_SPEND *only for traceability* —
# the daily_pnl ad_spend total comes from the ad_spend table (populated by
# amazon_ads_etl), NOT from these SP-API events. pnl_calculator's
# `_AGG_CATEGORIES` deliberately excludes AD_SPEND, so financial_events rows
# with category=='ad_spend' are stored but never rolled into daily_pnl,
# preventing a double-count against the authoritative Ads Reports API total.
# The empirical gap (Jan 2026 US: SP-API ProductAdsPaymentEventList charges
# were ~$24k higher via by-group vs by-date and both differ from the Ads API
# total) is expected — Ads API is authoritative for spend.
#
# If you ever need to add another entry here that IS meant to flow into
# daily_pnl, add its PnlCategory to `_AGG_CATEGORIES` in pnl_calculator
# too, or the total will be silently dropped.
EVENT_LIST_DEFAULT_CATEGORY: dict[str, PnlCategory] = {
    "ProductAdsPaymentEventList": PnlCategory.AD_SPEND,
}

# Cost categories: stored as positive in fee_amount (representing outflow);
# raw negative API values are negated, raw positive values (fee reversals) are
# stored as negative cost (so subtracting them from profit adds them back).
COST_CATEGORIES: frozenset[PnlCategory] = frozenset(
    {
        PnlCategory.COGS,
        PnlCategory.AD_SPEND,
        PnlCategory.SELLING_FEES,
        PnlCategory.OPERATIONAL_FEES,
        PnlCategory.REFUNDS,
    }
)

# Inflow categories: store raw signs as-is. Promotions inside Sales arrive
# negative and net against positive Principal — that's correct.
INFLOW_CATEGORIES: frozenset[PnlCategory] = frozenset(
    {PnlCategory.SALES, PnlCategory.REIMBURSEMENTS}
)


def normalize_amount(category: PnlCategory, raw_amount: float | None) -> float:
    """Convert a raw SP-API amount to the sign convention used in fee_amount.

    Sales / Reimbursements: stored as-is (positive = inflow, negative net reduction).
    Cost categories: stored as positive outflow; SP-API returns most fees as
        negative numbers, so we negate. A positive raw value (fee reversal /
        credit back) becomes a negative stored outflow, which when subtracted
        from profit adds money back -- the correct economic direction.
    """
    if raw_amount is None:
        return 0.0
    if category in COST_CATEGORIES:
        return -raw_amount
    return raw_amount
