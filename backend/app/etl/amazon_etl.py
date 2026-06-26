"""Amazon SP-API ETL: pull Finances API events -> flatten -> map -> persist.

Entry point: `run_amazon_etl(session, start_date, end_date, marketplace_ids)`.
Pulls financial events via the by-date list endpoint for each region required
by the requested marketplaces, flattens each event into per-line-item rows,
maps each line item to a P&L category per PNL_MAPPING.md, and persists to:
  - financial_events (mapped rows, with category and signed fee_amount)
  - unmapped_line_items (raw items the mapping didn't recognize)
  - raw_api_log (one row per API response page, for debug traceability)

The run is idempotent over the (start_date, end_date, marketplace_ids) tuple:
re-running deletes prior rows in that window for the same marketplaces and
re-inserts. Use `AmazonEtlSummary` for a structured report of what happened.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import and_, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.amazon_sp import (
    AmazonSPConnector,
    CURRENCY_TO_MARKETPLACE,
    MARKETPLACE_CHANNEL,
    MARKETPLACE_CURRENCY,
    MARKETPLACE_NAME_TO_ID,
    MARKETPLACE_REGION,
    REGION_PRIMARY_MARKETPLACE,
    Region,
)
from app.etl.pnl_mapping import (
    EVENT_LIST_DEFAULT_CATEGORY,
    PnlCategory,
    lookup_adjustment,
    lookup_refund_context_override,
    lookup_service_fee,
    lookup_shipment_charge,
    lookup_shipment_item_fee,
    normalize_amount,
)
from app.models import FinancialEvent, RawApiLog, UnmappedLineItem

logger = logging.getLogger(__name__)

ALL_MARKETPLACES: tuple[str, ...] = tuple(MARKETPLACE_REGION.keys())

FINANCES_ENDPOINT_PATH = "/finances/v0/financialEvents"


@dataclass
class FlatLineItem:
    """One row in financial_events derived from a single fee/charge in an event."""

    event_type: str
    posted_date: datetime | None
    marketplace_id: str | None
    order_id: str | None
    asin: str | None
    sku: str | None
    fee_type: str
    raw_amount: Decimal
    currency: str | None
    quantity: int | None
    category: PnlCategory | None
    raw_payload: dict[str, Any]

    @property
    def fee_amount(self) -> Decimal:
        if self.category is None:
            return Decimal("0")
        return Decimal(str(normalize_amount(self.category, float(self.raw_amount))))


@dataclass
class AmazonEtlSummary:
    start_date: date
    end_date: date
    marketplace_ids: list[str] = field(default_factory=list)
    events_pulled: int = 0
    line_items_total: int = 0
    line_items_mapped: int = 0
    line_items_unmapped: int = 0
    by_category: dict[str, int] = field(default_factory=dict)
    by_marketplace: dict[str, int] = field(default_factory=dict)
    unmapped_samples: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "marketplace_ids": list(self.marketplace_ids),
            "events_pulled": self.events_pulled,
            "line_items_total": self.line_items_total,
            "line_items_mapped": self.line_items_mapped,
            "line_items_unmapped": self.line_items_unmapped,
            "by_category": dict(self.by_category),
            "by_marketplace": dict(self.by_marketplace),
            "unmapped_samples": list(self.unmapped_samples),
        }


# -----------------------------------------------------------------------------
# Flattening: event payload -> per-line-item rows
# -----------------------------------------------------------------------------


def _parse_amount(amount_obj: dict[str, Any] | None) -> tuple[Decimal, str | None]:
    """SP-API amount objects are { CurrencyAmount: float, CurrencyCode: str }."""
    if not amount_obj:
        return Decimal("0"), None
    raw = amount_obj.get("CurrencyAmount")
    currency = amount_obj.get("CurrencyCode")
    if raw is None:
        return Decimal("0"), currency
    return Decimal(str(raw)), currency


def _parse_posted_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _resolve_marketplace(
    event: dict[str, Any], region: Region, *, currency_hint: str | None = None
) -> str | None:
    """Resolution priority:
    1. Explicit MarketplaceName on the event
    2. Currency code from a per-event amount (ServiceFee + Adjustment events
       lack MarketplaceName but carry CurrencyCode in their amount objects)
    3. Region's primary marketplace as last resort
    """
    name = event.get("MarketplaceName")
    if name and name in MARKETPLACE_NAME_TO_ID:
        return MARKETPLACE_NAME_TO_ID[name]
    if currency_hint:
        inferred = CURRENCY_TO_MARKETPLACE.get(currency_hint)
        if inferred and MARKETPLACE_REGION.get(inferred) == region:
            return inferred
    return REGION_PRIMARY_MARKETPLACE.get(region)


def _first_currency_from_event(event: dict[str, Any], *keys: str) -> str | None:
    """Pull a CurrencyCode out of the first matching nested amount object.

    Used to infer marketplace for events without MarketplaceName.
    """
    for key in keys:
        amount_obj = event.get(key)
        if isinstance(amount_obj, dict):
            code = amount_obj.get("CurrencyCode")
            if code:
                return code
    # Try FeeList / AdjustmentItemList nested currency
    fee_list = event.get("FeeList") or []
    for fee in fee_list:
        amt = fee.get("FeeAmount") or {}
        if isinstance(amt, dict) and amt.get("CurrencyCode"):
            return amt["CurrencyCode"]
    items = event.get("AdjustmentItemList") or []
    for item in items:
        amt = item.get("TotalAmount") or {}
        if isinstance(amt, dict) and amt.get("CurrencyCode"):
            return amt["CurrencyCode"]
    return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _flatten_shipment_event(
    event: dict[str, Any],
    *,
    region: Region,
    is_refund_context: bool,
) -> Iterator[FlatLineItem]:
    """ShipmentEvent and its refund-shaped siblings (RefundEvent,
    GuaranteeClaimEvent, ChargebackEvent) share the same item structure:
    ShipmentItemList[*].(ItemChargeList | ItemFeeList | PromotionList)."""
    event_type = (
        "RefundEvent" if is_refund_context else event.get("EventType") or "ShipmentEvent"
    )
    if not is_refund_context:
        # ShipmentEvent doesn't include its own type marker; the upstream
        # list name is what we got it from.
        event_type = "ShipmentEvent"
    posted = _parse_posted_date(event.get("PostedDate"))
    marketplace = _resolve_marketplace(event, region)
    order_id = event.get("AmazonOrderId") or event.get("SellerOrderId")

    item_lists = event.get("ShipmentItemList") or event.get("ShipmentItemAdjustmentList") or []
    if not item_lists:
        # Some events arrive with adjustment-list aliases.
        item_lists = event.get("ShipmentItemList") or []

    for item in item_lists:
        sku = item.get("SellerSKU")
        quantity = _to_int(item.get("QuantityShipped"))

        # Charges (Principal, Tax, Shipping, ...)
        for charge in item.get("ItemChargeList") or []:
            charge_type = charge.get("ChargeType") or "Unknown"
            amount, currency = _parse_amount(charge.get("ChargeAmount"))
            category = _map_shipment_charge(charge_type, is_refund_context)
            yield FlatLineItem(
                event_type=event_type,
                posted_date=posted,
                marketplace_id=marketplace,
                order_id=order_id,
                asin=None,
                sku=sku,
                fee_type=charge_type,
                raw_amount=amount,
                currency=currency,
                quantity=quantity,
                category=category,
                raw_payload=item,
            )

        # Adjustment to charges (refund-side often)
        for charge in item.get("ItemChargeAdjustmentList") or []:
            charge_type = charge.get("ChargeType") or "Unknown"
            amount, currency = _parse_amount(charge.get("ChargeAmount"))
            category = _map_shipment_charge(charge_type, is_refund_context)
            yield FlatLineItem(
                event_type=event_type,
                posted_date=posted,
                marketplace_id=marketplace,
                order_id=order_id,
                asin=None,
                sku=sku,
                fee_type=charge_type,
                raw_amount=amount,
                currency=currency,
                quantity=quantity,
                category=category,
                raw_payload=item,
            )

        # Fees (Commission, FBA fulfillment, ...)
        for fee in item.get("ItemFeeList") or []:
            fee_type = fee.get("FeeType") or "Unknown"
            amount, currency = _parse_amount(fee.get("FeeAmount"))
            category = _map_shipment_fee(fee_type, is_refund_context)
            yield FlatLineItem(
                event_type=event_type,
                posted_date=posted,
                marketplace_id=marketplace,
                order_id=order_id,
                asin=None,
                sku=sku,
                fee_type=fee_type,
                raw_amount=amount,
                currency=currency,
                quantity=quantity,
                category=category,
                raw_payload=item,
            )

        # Adjustment to fees
        for fee in item.get("ItemFeeAdjustmentList") or []:
            fee_type = fee.get("FeeType") or "Unknown"
            amount, currency = _parse_amount(fee.get("FeeAmount"))
            category = _map_shipment_fee(fee_type, is_refund_context)
            yield FlatLineItem(
                event_type=event_type,
                posted_date=posted,
                marketplace_id=marketplace,
                order_id=order_id,
                asin=None,
                sku=sku,
                fee_type=fee_type,
                raw_amount=amount,
                currency=currency,
                quantity=quantity,
                category=category,
                raw_payload=item,
            )

        # Promotions roll up into the Sales (or Refunds) category as price reductions.
        for promo in item.get("PromotionList") or []:
            promo_type = promo.get("PromotionType") or "Promotion"
            amount, currency = _parse_amount(promo.get("PromotionAmount"))
            category = (
                PnlCategory.REFUNDS if is_refund_context else PnlCategory.SALES
            )
            yield FlatLineItem(
                event_type=event_type,
                posted_date=posted,
                marketplace_id=marketplace,
                order_id=order_id,
                asin=None,
                sku=sku,
                fee_type=f"Promotion:{promo_type}",
                raw_amount=amount,
                currency=currency,
                quantity=quantity,
                category=category,
                raw_payload=item,
            )

        for promo in item.get("PromotionAdjustmentList") or []:
            promo_type = promo.get("PromotionType") or "Promotion"
            amount, currency = _parse_amount(promo.get("PromotionAmount"))
            category = (
                PnlCategory.REFUNDS if is_refund_context else PnlCategory.SALES
            )
            yield FlatLineItem(
                event_type=event_type,
                posted_date=posted,
                marketplace_id=marketplace,
                order_id=order_id,
                asin=None,
                sku=sku,
                fee_type=f"Promotion:{promo_type}",
                raw_amount=amount,
                currency=currency,
                quantity=quantity,
                category=category,
                raw_payload=item,
            )


def _map_shipment_charge(charge_type: str, is_refund_context: bool) -> PnlCategory | None:
    if is_refund_context:
        override = lookup_refund_context_override(charge_type)
        if override is not None:
            return override
        # Standard refund components map to REFUNDS regardless of underlying ChargeType
        mapped = lookup_shipment_charge(charge_type)
        if mapped is not None:
            return PnlCategory.REFUNDS if mapped == PnlCategory.SALES else mapped
        return None
    return lookup_shipment_charge(charge_type)


def _map_shipment_fee(fee_type: str, is_refund_context: bool) -> PnlCategory | None:
    if is_refund_context:
        override = lookup_refund_context_override(fee_type)
        if override is not None:
            return override
        if lookup_shipment_item_fee(fee_type) is not None:
            return PnlCategory.REFUNDS
        if fee_type == "RefundCommission":
            return PnlCategory.REFUNDS
        return None
    return lookup_shipment_item_fee(fee_type)


def _flatten_service_fee_event(
    event: dict[str, Any], *, region: Region, default_posted: datetime | None = None
) -> Iterator[FlatLineItem]:
    # SP-API frequently returns ServiceFeeEvents without a top-level PostedDate
    # (the event is implicitly within the requested PostedAfter window). Fall
    # back to the ETL window start so the event is still attributable to a day
    # and survives the date-windowed purge/upsert flow.
    posted = _parse_posted_date(event.get("PostedDate")) or default_posted
    currency_hint = _first_currency_from_event(event, "FeeAmount")
    marketplace = _resolve_marketplace(event, region, currency_hint=currency_hint)
    asin = event.get("ASIN")
    sku = event.get("SellerSKU")
    fee_reason = event.get("FeeReason")

    fee_list = event.get("FeeList") or []
    if fee_list:
        for fee in fee_list:
            fee_type = fee.get("FeeType") or fee_reason or "Unknown"
            amount, currency = _parse_amount(fee.get("FeeAmount"))
            category = lookup_service_fee(fee_type) or (
                lookup_service_fee(fee_reason) if fee_reason else None
            )
            yield FlatLineItem(
                event_type="ServiceFeeEvent",
                posted_date=posted,
                marketplace_id=marketplace,
                order_id=event.get("AmazonOrderId"),
                asin=asin,
                sku=sku,
                fee_type=fee_type,
                raw_amount=amount,
                currency=currency,
                quantity=None,
                category=category,
                raw_payload=event,
            )
    elif fee_reason:
        # Some ServiceFeeEvents carry only a top-level reason without a FeeList.
        category = lookup_service_fee(fee_reason)
        yield FlatLineItem(
            event_type="ServiceFeeEvent",
            posted_date=posted,
            marketplace_id=marketplace,
            order_id=event.get("AmazonOrderId"),
            asin=asin,
            sku=sku,
            fee_type=fee_reason,
            raw_amount=Decimal("0"),
            currency=None,
            quantity=None,
            category=category,
            raw_payload=event,
        )


def _flatten_adjustment_event(
    event: dict[str, Any], *, region: Region, default_posted: datetime | None = None
) -> Iterator[FlatLineItem]:
    posted = _parse_posted_date(event.get("PostedDate")) or default_posted
    currency_hint = _first_currency_from_event(event, "AdjustmentAmount")
    marketplace = _resolve_marketplace(event, region, currency_hint=currency_hint)
    adj_type = event.get("AdjustmentType") or "Unknown"
    category = lookup_adjustment(adj_type)

    items = event.get("AdjustmentItemList") or []
    if items:
        for item in items:
            amount, currency = _parse_amount(item.get("TotalAmount"))
            yield FlatLineItem(
                event_type="AdjustmentEvent",
                posted_date=posted,
                marketplace_id=marketplace,
                order_id=None,
                asin=item.get("ASIN"),
                sku=item.get("SellerSKU"),
                fee_type=adj_type,
                raw_amount=amount,
                currency=currency,
                quantity=_to_int(item.get("Quantity")),
                category=category,
                raw_payload=item,
            )
    else:
        amount, currency = _parse_amount(event.get("AdjustmentAmount"))
        yield FlatLineItem(
            event_type="AdjustmentEvent",
            posted_date=posted,
            marketplace_id=marketplace,
            order_id=None,
            asin=None,
            sku=None,
            fee_type=adj_type,
            raw_amount=amount,
            currency=currency,
            quantity=None,
            category=category,
            raw_payload=event,
        )


def _flatten_product_ads_payment_event(
    event: dict[str, Any], *, region: Region, default_posted: datetime | None = None
) -> Iterator[FlatLineItem]:
    # ProductAdsPaymentEvent uses lowercase field names.
    posted = _parse_posted_date(event.get("postedDate")) or default_posted
    amount, currency = _parse_amount(event.get("transactionValue"))
    yield FlatLineItem(
        event_type="ProductAdsPaymentEvent",
        posted_date=posted,
        # No MarketplaceName on this event; attribute to region primary.
        marketplace_id=REGION_PRIMARY_MARKETPLACE.get(region),
        order_id=event.get("invoiceId"),
        asin=None,
        sku=None,
        fee_type=event.get("transactionType") or "ProductAdsPayment",
        raw_amount=amount,
        currency=currency,
        quantity=None,
        category=PnlCategory.AD_SPEND,
        raw_payload=event,
    )


def _flatten_retrocharge_event(
    event: dict[str, Any], *, region: Region, default_posted: datetime | None = None
) -> Iterator[FlatLineItem]:
    posted = _parse_posted_date(event.get("PostedDate")) or default_posted
    marketplace = _resolve_marketplace(event, region)
    order_id = event.get("AmazonOrderId")
    # RetrochargeEvent has BaseTax, ShippingTax (sometimes more) as top-level amounts.
    for field_name in ("BaseTax", "ShippingTax"):
        amount_obj = event.get(field_name)
        if amount_obj is None:
            continue
        amount, currency = _parse_amount(amount_obj)
        yield FlatLineItem(
            event_type="RetrochargeEvent",
            posted_date=posted,
            marketplace_id=marketplace,
            order_id=order_id,
            asin=None,
            sku=None,
            fee_type=f"Retrocharge:{field_name}",
            raw_amount=amount,
            currency=currency,
            quantity=None,
            category=PnlCategory.OPERATIONAL_FEES,
            raw_payload=event,
        )


def _flatten_unknown_event(
    list_name: str, event: dict[str, Any], *, region: Region, default_posted: datetime | None = None
) -> Iterator[FlatLineItem]:
    """Fallback for event types we don't model: emit an unmapped row with
    whatever top-level amount we can scrape, so it lands in unmapped_line_items
    and stays visible."""
    posted = _parse_posted_date(event.get("PostedDate") or event.get("postedDate")) or default_posted
    marketplace = _resolve_marketplace(event, region)
    fallback_category = EVENT_LIST_DEFAULT_CATEGORY.get(list_name)
    # Try common amount fields
    for field_name in ("TotalAmount", "TransactionAmount", "Amount", "FeeAmount"):
        amount_obj = event.get(field_name)
        if amount_obj is None:
            continue
        amount, currency = _parse_amount(amount_obj)
        yield FlatLineItem(
            event_type=list_name.replace("List", ""),
            posted_date=posted,
            marketplace_id=marketplace,
            order_id=event.get("AmazonOrderId"),
            asin=None,
            sku=None,
            fee_type=list_name,
            raw_amount=amount,
            currency=currency,
            quantity=None,
            category=fallback_category,
            raw_payload=event,
        )
        return
    # No amount found -- still emit a record (zero amount) so the event type
    # gets surfaced as unmapped instead of silently dropped.
    yield FlatLineItem(
        event_type=list_name.replace("List", ""),
        posted_date=posted,
        marketplace_id=marketplace,
        order_id=None,
        asin=None,
        sku=None,
        fee_type=list_name,
        raw_amount=Decimal("0"),
        currency=None,
        quantity=None,
        category=fallback_category,
        raw_payload=event,
    )


_SHIPMENT_SHAPED_REFUND_LISTS = {
    "RefundEventList",
    "GuaranteeClaimEventList",
    "ChargebackEventList",
}


def flatten_events_payload(
    payload: dict[str, list[dict[str, Any]]],
    *,
    region: Region,
    default_posted: datetime | None = None,
) -> Iterator[FlatLineItem]:
    """Walk the merged FinancialEvents dict and yield FlatLineItems.

    `default_posted` is the fallback posted_date for events that don't carry
    their own PostedDate (notably ServiceFeeEvents in this seller's data,
    which arrive with only AmazonOrderId + FeeList). Pass the ETL window's
    start so these orphans aggregate into the first day of the window and
    survive the idempotent purge-and-reinsert flow.
    """
    for list_name, events in payload.items():
        if not isinstance(events, list):
            continue
        for event in events:
            if list_name == "ShipmentEventList":
                yield from _flatten_shipment_event(event, region=region, is_refund_context=False)
            elif list_name in _SHIPMENT_SHAPED_REFUND_LISTS:
                yield from _flatten_shipment_event(event, region=region, is_refund_context=True)
            elif list_name == "ServiceFeeEventList":
                yield from _flatten_service_fee_event(event, region=region, default_posted=default_posted)
            elif list_name == "AdjustmentEventList":
                yield from _flatten_adjustment_event(event, region=region, default_posted=default_posted)
            elif list_name == "ProductAdsPaymentEventList":
                yield from _flatten_product_ads_payment_event(event, region=region, default_posted=default_posted)
            elif list_name == "RetrochargeEventList":
                yield from _flatten_retrocharge_event(event, region=region, default_posted=default_posted)
            else:
                yield from _flatten_unknown_event(list_name, event, region=region, default_posted=default_posted)


# -----------------------------------------------------------------------------
# Persistence
# -----------------------------------------------------------------------------


def _date_window(start_date: date, end_date: date) -> tuple[datetime, datetime]:
    """Convert (start_date, end_date) (inclusive) to a UTC datetime window
    [start_date 00:00, end_date+1 00:00). SP-API also requires PostedBefore
    >= 2 minutes ago; the caller is expected to keep `end_date` strictly
    in the past."""
    start = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
    end = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=timezone.utc)
    return start, end


async def _purge_existing_rows(
    session: AsyncSession,
    *,
    window_start: datetime,
    window_end: datetime,
    marketplace_ids: Iterable[str],
) -> None:
    """Delete prior financial_events and unmapped_line_items rows for the
    given marketplaces and posted-date window. Re-running the ETL for the
    same window therefore yields exactly one set of rows -- no duplicates."""
    mp_list = list(marketplace_ids)
    await session.execute(
        delete(FinancialEvent).where(
            and_(
                FinancialEvent.marketplace_id.in_(mp_list),
                FinancialEvent.posted_date >= window_start,
                FinancialEvent.posted_date < window_end,
            )
        )
    )
    await session.execute(
        delete(UnmappedLineItem).where(
            and_(
                UnmappedLineItem.marketplace_id.in_(mp_list),
                UnmappedLineItem.posted_date >= window_start,
                UnmappedLineItem.posted_date < window_end,
            )
        )
    )


def _persist_line_item(
    session: AsyncSession,
    item: FlatLineItem,
    *,
    endpoint: str,
    summary: AmazonEtlSummary,
) -> None:
    summary.line_items_total += 1
    if item.marketplace_id:
        summary.by_marketplace[item.marketplace_id] = (
            summary.by_marketplace.get(item.marketplace_id, 0) + 1
        )
    if item.category is None:
        summary.line_items_unmapped += 1
        if len(summary.unmapped_samples) < 25:
            summary.unmapped_samples.append(
                {
                    "event_type": item.event_type,
                    "fee_type": item.fee_type,
                    "amount": float(item.raw_amount),
                    "currency": item.currency,
                    "marketplace_id": item.marketplace_id,
                    "posted_date": item.posted_date.isoformat() if item.posted_date else None,
                }
            )
        session.add(
            UnmappedLineItem(
                posted_date=item.posted_date,
                marketplace_id=item.marketplace_id,
                event_type=item.event_type,
                line_item_name=item.fee_type,
                amount=item.raw_amount,
                currency=item.currency,
                source_endpoint=endpoint,
                raw_payload=item.raw_payload,
            )
        )
        return

    summary.line_items_mapped += 1
    summary.by_category[item.category.value] = (
        summary.by_category.get(item.category.value, 0) + 1
    )
    session.add(
        FinancialEvent(
            event_type=item.event_type,
            posted_date=item.posted_date,
            marketplace_id=item.marketplace_id,
            order_id=item.order_id,
            asin=item.asin,
            sku=item.sku,
            fee_type=item.fee_type,
            category=item.category.value,
            fee_amount=item.fee_amount,
            raw_amount=item.raw_amount,
            quantity=item.quantity,
            currency=item.currency,
            raw_payload=item.raw_payload,
        )
    )


def _count_events(payload: dict[str, list[dict[str, Any]]]) -> int:
    return sum(len(v) for v in payload.values() if isinstance(v, list))


def _log_raw_response(
    session: AsyncSession,
    *,
    endpoint: str,
    region: Region,
    payload: dict[str, list[dict[str, Any]]],
    window_start: datetime,
    window_end: datetime,
) -> None:
    session.add(
        RawApiLog(
            source="amazon_sp",
            endpoint=endpoint,
            request_params={
                "region": region,
                "PostedAfter": window_start.isoformat(),
                "PostedBefore": window_end.isoformat(),
            },
            response_status=200,
            response_body={"FinancialEvents": payload},
        )
    )


# -----------------------------------------------------------------------------
# Public entry point
# -----------------------------------------------------------------------------


async def run_amazon_etl(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
    marketplace_ids: Iterable[str] | None = None,
    connector_factory: Any = None,
) -> AmazonEtlSummary:
    """Pull Amazon SP-API financial events for the window and persist.

    `start_date` / `end_date` are inclusive UTC dates. `marketplace_ids`
    defaults to all five. `connector_factory(region)` is injectable for
    tests; defaults to `AmazonSPConnector(region=region)`.
    """
    marketplaces = tuple(marketplace_ids) if marketplace_ids else ALL_MARKETPLACES
    unknown = [m for m in marketplaces if m not in MARKETPLACE_REGION]
    if unknown:
        raise ValueError(f"unknown marketplace_ids: {unknown}")

    summary = AmazonEtlSummary(
        start_date=start_date, end_date=end_date, marketplace_ids=list(marketplaces)
    )
    window_start, window_end = _date_window(start_date, end_date)

    await _purge_existing_rows(
        session,
        window_start=window_start,
        window_end=window_end,
        marketplace_ids=marketplaces,
    )

    # Group requested marketplaces by region so we pull each region once.
    regions_needed: dict[Region, list[str]] = {}
    for mp in marketplaces:
        region = MARKETPLACE_REGION[mp]
        regions_needed.setdefault(region, []).append(mp)

    factory = connector_factory or (lambda region: AmazonSPConnector(region=region))

    for region, region_marketplaces in regions_needed.items():
        logger.info(
            "amazon_etl region=%s marketplaces=%s window=%s/%s",
            region,
            region_marketplaces,
            window_start.isoformat(),
            window_end.isoformat(),
        )
        async with factory(region) as conn:
            payload = await conn.get_financial_events_by_date(
                posted_after=window_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                posted_before=window_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            )

        _log_raw_response(
            session,
            endpoint=FINANCES_ENDPOINT_PATH,
            region=region,
            payload=payload,
            window_start=window_start,
            window_end=window_end,
        )
        summary.events_pulled += _count_events(payload)

        region_marketplaces_set = set(region_marketplaces)
        for item in flatten_events_payload(payload, region=region, default_posted=window_start):
            # Drop items that don't belong to one of the requested marketplaces
            # in this region (e.g. a CA shipment when only US was requested).
            if item.marketplace_id and item.marketplace_id not in region_marketplaces_set:
                continue
            _persist_line_item(
                session,
                item,
                endpoint=FINANCES_ENDPOINT_PATH,
                summary=summary,
            )

    await session.flush()
    return summary


__all__ = [
    "ALL_MARKETPLACES",
    "AmazonEtlSummary",
    "FlatLineItem",
    "MARKETPLACE_CHANNEL",
    "MARKETPLACE_CURRENCY",
    "flatten_events_payload",
    "run_amazon_etl",
]
