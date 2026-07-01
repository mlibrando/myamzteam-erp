"""Drill into ServiceFeeEventList (and every fee-bearing event type) at the
line_item level: for Jan 2026 US, list every distinct fee name found in
by-group vs by-date with count + USD sum, then diff.

This tells us which specific fee categories (storage, placement, taxes
remitted, disposal, etc.) surface only in by-group — i.e., which of
Elena's gaps by-group can close.

Line item name key by event type:
- ServiceFeeEventList: use `FeeReason` (or fallback to `FeeDescription`)
- ShipmentEventList: fee breakdown inside ShipmentFeeList[].FeeType and
  ShipmentItemList[].ItemFeeList[].FeeType (FBA + referral fees)
- AdjustmentEventList: `AdjustmentType`
- ProductAdsPaymentEventList: `transactionType` (Charge/Refund)
- RefundEventList: fee breakdown similar to ShipmentEventList

Run from backend/:
    .venv/bin/python -m scripts.diff_bygroup_line_items
"""

from __future__ import annotations

import asyncio
import logging
import sys
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

from app.connectors.amazon_sp import AmazonSPConnector

logging.basicConfig(level=logging.WARNING)

WINDOW_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
WINDOW_END = datetime(2026, 2, 1, tzinfo=timezone.utc)


def _iso_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_dt(x):
    if not x:
        return None
    try:
        return datetime.fromisoformat(x.replace("Z", "+00:00"))
    except ValueError:
        return None


def _in_window(event: dict) -> bool:
    pd = _parse_dt(event.get("PostedDate"))
    if pd is None:
        return True
    return WINDOW_START <= pd < WINDOW_END


def _sum_amount(amount_node) -> Decimal:
    if not isinstance(amount_node, dict):
        return Decimal("0")
    if amount_node.get("CurrencyCode") != "USD":
        return Decimal("0")
    try:
        return Decimal(str(amount_node.get("CurrencyAmount", 0)))
    except Exception:
        return Decimal("0")


def _line_items_from_service_fee(events: list[dict]) -> dict[str, tuple[int, Decimal]]:
    """ServiceFeeEventList: each event has FeeList[].FeeType + FeeAmount."""
    agg: dict[str, tuple[int, Decimal]] = defaultdict(lambda: (0, Decimal("0")))
    for e in events:
        if not _in_window(e):
            continue
        reason = e.get("FeeReason") or e.get("FeeDescription") or "(unknown reason)"
        for fee in (e.get("FeeList") or []):
            fee_type = fee.get("FeeType") or "(no FeeType)"
            key = f"{reason} :: {fee_type}"
            amt = _sum_amount(fee.get("FeeAmount"))
            c, s = agg[key]
            agg[key] = (c + 1, s + amt)
    return dict(agg)


def _line_items_from_shipment(events: list[dict]) -> dict[str, tuple[int, Decimal]]:
    """ShipmentEventList: fees live in ShipmentFeeList[] and
    ShipmentItemList[].ItemFeeList[]."""
    agg: dict[str, tuple[int, Decimal]] = defaultdict(lambda: (0, Decimal("0")))
    for e in events:
        if not _in_window(e):
            continue
        for fee in (e.get("ShipmentFeeList") or []):
            ft = fee.get("FeeType") or "(no FeeType)"
            amt = _sum_amount(fee.get("FeeAmount"))
            c, s = agg[f"shipment :: {ft}"]
            agg[f"shipment :: {ft}"] = (c + 1, s + amt)
        for item in (e.get("ShipmentItemList") or []):
            for fee in (item.get("ItemFeeList") or []):
                ft = fee.get("FeeType") or "(no FeeType)"
                amt = _sum_amount(fee.get("FeeAmount"))
                c, s = agg[f"item :: {ft}"]
                agg[f"item :: {ft}"] = (c + 1, s + amt)
            for chg in (item.get("ItemChargeList") or []):
                ct = chg.get("ChargeType") or "(no ChargeType)"
                amt = _sum_amount(chg.get("ChargeAmount"))
                c, s = agg[f"item_charge :: {ct}"]
                agg[f"item_charge :: {ct}"] = (c + 1, s + amt)
    return dict(agg)


def _line_items_from_adjustment(events: list[dict]) -> dict[str, tuple[int, Decimal]]:
    agg: dict[str, tuple[int, Decimal]] = defaultdict(lambda: (0, Decimal("0")))
    for e in events:
        if not _in_window(e):
            continue
        atype = e.get("AdjustmentType") or "(unknown)"
        amt = _sum_amount(e.get("AdjustmentAmount"))
        c, s = agg[atype]
        agg[atype] = (c + 1, s + amt)
    return dict(agg)


def _line_items_from_product_ads(events: list[dict]) -> dict[str, tuple[int, Decimal]]:
    agg: dict[str, tuple[int, Decimal]] = defaultdict(lambda: (0, Decimal("0")))
    for e in events:
        if not _in_window(e):
            continue
        ttype = e.get("transactionType") or "(unknown)"
        amt = _sum_amount(e.get("transactionValue"))
        c, s = agg[ttype]
        agg[ttype] = (c + 1, s + amt)
    return dict(agg)


def _print_diff(title: str,
                by_date: dict[str, tuple[int, Decimal]],
                by_group: dict[str, tuple[int, Decimal]]) -> None:
    print(f"\n== {title} ==")
    all_keys = sorted(set(by_date) | set(by_group))
    print(f"  {'line item':60}  {'d cnt':>5}  {'d USD':>12}  {'g cnt':>5}  {'g USD':>12}  {'delta USD':>12}")
    print("  " + "-" * 118)
    total_d = Decimal("0")
    total_g = Decimal("0")
    for k in all_keys:
        dc, ds = by_date.get(k, (0, Decimal("0")))
        gc, gs = by_group.get(k, (0, Decimal("0")))
        delta = gs - ds
        flag = "  *" if abs(delta) > Decimal("50") else ""
        print(f"  {k[:60]:60}  {dc:>5}  {float(ds):>12,.2f}  {gc:>5}  {float(gs):>12,.2f}  {float(delta):>12,.2f}{flag}")
        total_d += ds
        total_g += gs
    print("  " + "-" * 118)
    print(f"  {'TOTAL':60}  {'':>5}  {float(total_d):>12,.2f}  {'':>5}  {float(total_g):>12,.2f}  {float(total_g - total_d):>12,.2f}")


async def main() -> int:
    async with AmazonSPConnector(region="NA") as conn:
        # by-date
        by_date = await conn.get_financial_events_by_date(
            posted_after=_iso_z(WINDOW_START),
            posted_before=_iso_z(WINDOW_END),
        )

        # by-group (all Closed groups overlapping Jan 2026)
        list_start = datetime(2025, 12, 1, tzinfo=timezone.utc)
        groups = await conn.get_financial_event_groups(start_date=_iso_z(list_start))
        merged: dict[str, list[dict]] = defaultdict(list)
        for g in groups:
            if g.get("ProcessingStatus") != "Closed":
                continue
            gs = _parse_dt(g.get("FinancialEventGroupStart"))
            ge = _parse_dt(g.get("FinancialEventGroupEnd"))
            if gs is None or ge is None or ge < WINDOW_START or gs >= WINDOW_END:
                continue
            events = await conn.get_financial_events(g["FinancialEventGroupId"])
            for k, v in events.items():
                merged[k].extend(v)

    # Drill each fee-carrying type
    _print_diff(
        "ServiceFeeEventList",
        _line_items_from_service_fee(by_date.get("ServiceFeeEventList", [])),
        _line_items_from_service_fee(merged.get("ServiceFeeEventList", [])),
    )
    _print_diff(
        "ShipmentEventList",
        _line_items_from_shipment(by_date.get("ShipmentEventList", [])),
        _line_items_from_shipment(merged.get("ShipmentEventList", [])),
    )
    _print_diff(
        "AdjustmentEventList",
        _line_items_from_adjustment(by_date.get("AdjustmentEventList", [])),
        _line_items_from_adjustment(merged.get("AdjustmentEventList", [])),
    )
    _print_diff(
        "ProductAdsPaymentEventList",
        _line_items_from_product_ads(by_date.get("ProductAdsPaymentEventList", [])),
        _line_items_from_product_ads(merged.get("ProductAdsPaymentEventList", [])),
    )

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
