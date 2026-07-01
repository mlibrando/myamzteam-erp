"""Raw line-item breakdown for Jan 2026 US.

Print one row per (source, category, event_type, fee_type) with:
- count of events
- signed raw_amount sum   (what SP-API returned; fees are negative)
- signed fee_amount sum   (normalized to P&L convention; positive
                           = magnitude of expense for cost categories)

Grouped by category for direct comparison against Elena's RAW_AMZ_US
sheet. The table intentionally avoids interpreting anything — the user
diffs each row against Elena's values manually.

Also print a per-category subtotal + Elena's category value so we can
see the gap at each level without ambiguity about sign conventions.

Run from backend/:
    .venv/bin/python -m scripts.report_line_items_vs_elena
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import date
from decimal import Decimal

from sqlalchemy import and_, func, select

from app.database import AsyncSessionLocal
from app.etl.timezone_utils import local_date_expr
from app.models import FinancialEvent

logging.basicConfig(level=logging.WARNING)

US_MARKETPLACE_ID = "ATVPDKIKX0DER"
START = date(2026, 1, 1)
END = date(2026, 1, 31)

# Elena's Jan 2026 US category totals (signed convention — negative =
# expense / reduction). These are for context ONLY; the primary comparison
# is line-item vs line-item, since categories are moving targets.
ELENA = {
    "sales": Decimal("175191.94"),
    "selling_fees": Decimal("-51401"),      # signed: expense
    "operational_fees": Decimal("-14912.08"),  # signed: expense
    "refunds": Decimal("-10924"),           # signed: expense (approx from prior context)
    "reimbursements": Decimal("3224"),      # signed: inflow (approx from prior context)
}


def _fmt_signed(v: Decimal) -> str:
    return f"{float(v):>14,.2f}"


async def main() -> int:
    date_col = local_date_expr(FinancialEvent.posted_date)
    async with AsyncSessionLocal() as session:
        stmt = (
            select(
                FinancialEvent.category,
                FinancialEvent.source,
                FinancialEvent.event_type,
                FinancialEvent.fee_type,
                func.count().label("cnt"),
                func.coalesce(func.sum(FinancialEvent.raw_amount), 0).label("raw_sum"),
                func.coalesce(func.sum(FinancialEvent.fee_amount), 0).label("fee_sum"),
            )
            .where(
                and_(
                    FinancialEvent.marketplace_id == US_MARKETPLACE_ID,
                    date_col >= START,
                    date_col <= END,
                    FinancialEvent.category.is_not(None),
                )
            )
            .group_by(
                FinancialEvent.category,
                FinancialEvent.source,
                FinancialEvent.event_type,
                FinancialEvent.fee_type,
            )
            .order_by(
                FinancialEvent.category,
                FinancialEvent.source,
                func.abs(func.sum(FinancialEvent.raw_amount)).desc(),
            )
        )
        result = (await session.execute(stmt)).all()

    print(
        f"\nJan 2026 US line-item breakdown ({START} .. {END} PT-local dates, "
        f"daily_pnl-compatible bucketing)\n"
    )
    print("Sign convention:")
    print("  raw_amount = as returned by SP-API. Fees are NEGATIVE (Amazon deducted).")
    print("  fee_amount = normalized to P&L. For cost categories, fee_amount = -raw_amount")
    print("               so magnitude-of-expense sums are POSITIVE. daily_pnl stores fee_amount.")
    print("  Elena signed = negative for expenses, positive for inflows.\n")
    print(
        f"{'category':>18}  {'source':>28}  {'event_type':22}  "
        f"{'fee_type':40}  {'cnt':>6}  {'raw_signed':>14}  {'fee_signed':>14}"
    )
    print("-" * 148)

    current_category = None
    cat_raw = Decimal("0")
    cat_fee = Decimal("0")

    def _flush_category(cat: str | None) -> None:
        if cat is None:
            return
        elena_val = ELENA.get(cat)
        print("-" * 148)
        print(
            f"{'TOTAL ' + cat:>18}  {'':>28}  {'':22}  {'':40}  {'':>6}  "
            f"{_fmt_signed(cat_raw)}  {_fmt_signed(cat_fee)}"
        )
        if elena_val is not None:
            # Convert stored fee_signed to Elena's convention:
            # cost categories: our fee_signed is +magnitude; Elena's is -magnitude
            # inflow categories: our fee_signed and Elena's are both positive
            # We report the raw_signed (which is already sign-preserving) for
            # the comparison; that removes normalization ambiguity.
            gap_raw = cat_raw - elena_val
            print(
                f"{'  vs Elena':>18}  {'':>28}  {'':22}  {'':40}  {'':>6}  "
                f"{_fmt_signed(elena_val)}  gap_raw={_fmt_signed(gap_raw)}"
            )
        print("=" * 148)

    for cat, src, et, ft, cnt, raw, fee in result:
        if cat != current_category:
            _flush_category(current_category)
            current_category = cat
            cat_raw = Decimal("0")
            cat_fee = Decimal("0")
        raw = Decimal(str(raw))
        fee = Decimal(str(fee))
        print(
            f"{cat:>18}  {src:>28}  {et:22}  {ft:40}  {cnt:>6}  "
            f"{_fmt_signed(raw)}  {_fmt_signed(fee)}"
        )
        cat_raw += raw
        cat_fee += fee
    _flush_category(current_category)

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
