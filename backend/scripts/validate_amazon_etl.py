"""End-to-end live validation: pull January 2025 US data through the ETL and
print daily_pnl rows and unmapped line items so we can compare against
Elena's manual P&L.

Also exercises:
  - upsert (runs the ETL twice; the second run should produce the SAME row
    counts, not duplicate them)
  - COGS lookup against the populated product_cogs table (warns on missing SKUs)
  - the P&L formula end-to-end against real data

Run from backend/ with .env loaded:
    .venv/bin/python -m scripts.validate_amazon_etl
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import and_, func, select

from app.database import AsyncSessionLocal
from app.etl.amazon_etl import run_amazon_etl
from app.etl.pnl_calculator import calculate_daily_pnl
from app.etl.timezone_utils import date_range_utc, local_date_expr
from app.models import DailyPnL, FinancialEvent, UnmappedLineItem

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

US_MARKETPLACE_ID = "ATVPDKIKX0DER"
US_CHANNEL = "amazon_us"

# PT-local Jan 2026. Elena's manual P&L cuts on Pacific Time (Amazon Seller
# Central default), so validation windows are PT-anchored. daily_pnl.date is
# also PT-local via pnl_calculator's PT bucketing, so START/END are the
# right filters for the "did January match" question.
START = date(2026, 1, 1)
END = date(2026, 1, 31)

# The ETL still fetches by UTC PostedAfter/PostedBefore. Widen by one day
# on each side so we capture events posted during the ~8-hour PT-vs-UTC
# offset at the month boundaries (they'll be bucketed to the correct PT
# date by pnl_calculator).
_PT_UTC_START, _PT_UTC_END = date_range_utc(START, END + timedelta(days=1))
ETL_START = _PT_UTC_START.date()  # date used by run_amazon_etl (UTC-inclusive)
ETL_END = _PT_UTC_END.date()      # date used by run_amazon_etl (UTC-inclusive)


def fmt(amount: Decimal | None) -> str:
    if amount is None:
        return "       -  "
    return f"{float(amount):>10,.2f}"


async def _print_existing_rows(session) -> None:
    stmt = (
        select(DailyPnL)
        .where(
            and_(
                DailyPnL.channel == US_CHANNEL,
                DailyPnL.date >= START,
                DailyPnL.date <= END,
            )
        )
        .order_by(DailyPnL.date)
    )
    result = await session.execute(stmt)
    rows = result.scalars().all()
    print(f"  daily_pnl rows for {US_CHANNEL} {START}..{END}: {len(rows)}")


async def main() -> int:
    print(f"[1/4] Initial state")
    async with AsyncSessionLocal() as session:
        await _print_existing_rows(session)

    print(f"[2/4] First ETL run ({START} -> {END}, US only)")
    async with AsyncSessionLocal() as session:
        etl_summary = await run_amazon_etl(
            session,
            start_date=ETL_START,
            end_date=ETL_END,
            marketplace_ids=[US_MARKETPLACE_ID],
        )
        pnl_summary = await calculate_daily_pnl(
            session,
            start_date=START,
            end_date=END,
            marketplace_ids=[US_MARKETPLACE_ID],
        )
        await session.commit()
    print(f"  events_pulled:     {etl_summary.events_pulled}")
    print(f"  line_items_total:  {etl_summary.line_items_total}")
    print(f"  line_items_mapped: {etl_summary.line_items_mapped}")
    print(f"  line_items_unmapped: {etl_summary.line_items_unmapped}")
    print(f"  by_category: {etl_summary.by_category}")
    print(f"  pnl rows_written: {pnl_summary.rows_written}")
    print(f"  skus_without_cogs: {len(pnl_summary.skus_without_cogs)} unique SKUs")
    if pnl_summary.skus_without_cogs[:5]:
        print(f"    sample: {pnl_summary.skus_without_cogs[:5]}")

    print(f"[3/4] Idempotency check: re-run same window")
    async with AsyncSessionLocal() as session:
        etl_summary_2 = await run_amazon_etl(
            session,
            start_date=ETL_START,
            end_date=ETL_END,
            marketplace_ids=[US_MARKETPLACE_ID],
        )
        pnl_summary_2 = await calculate_daily_pnl(
            session,
            start_date=START,
            end_date=END,
            marketplace_ids=[US_MARKETPLACE_ID],
        )
        await session.commit()
        # Verify financial_events row count is the same after re-run.
        # PT-local dates so this matches Elena's window definition.
        fe_local_date = local_date_expr(FinancialEvent.posted_date)
        count_stmt = select(func.count(FinancialEvent.id)).where(
            and_(
                FinancialEvent.marketplace_id == US_MARKETPLACE_ID,
                fe_local_date >= START,
                fe_local_date <= END,
            )
        )
        fe_count = (await session.execute(count_stmt)).scalar_one()
        pnl_count_stmt = select(func.count(DailyPnL.id)).where(
            and_(
                DailyPnL.channel == US_CHANNEL,
                DailyPnL.date >= START,
                DailyPnL.date <= END,
            )
        )
        pnl_count = (await session.execute(pnl_count_stmt)).scalar_one()
    print(f"  re-run line_items_mapped: {etl_summary_2.line_items_mapped} (was {etl_summary.line_items_mapped})")
    print(f"  re-run pnl rows_written: {pnl_summary_2.rows_written}")
    print(f"  financial_events row count in window: {fe_count}")
    print(f"  daily_pnl row count in window: {pnl_count}")
    assert etl_summary_2.line_items_mapped == etl_summary.line_items_mapped, "idempotency failed"
    assert pnl_count == pnl_summary.rows_written, "daily_pnl row count diverged"

    print(f"[4/4] Daily P&L for {US_CHANNEL} {START}..{END}")
    async with AsyncSessionLocal() as session:
        stmt = (
            select(DailyPnL)
            .where(
                and_(
                    DailyPnL.channel == US_CHANNEL,
                    DailyPnL.date >= START,
                    DailyPnL.date <= END,
                )
            )
            .order_by(DailyPnL.date)
        )
        rows = (await session.execute(stmt)).scalars().all()

        print(
            f"  {'date':10}  {'sales':>10}  {'cogs':>10}  {'sell_fees':>10}  "
            f"{'op_fees':>10}  {'refunds':>10}  {'reimburs':>10}  "
            f"{'gp_no_reimb':>11}  {'gp_w_reimb':>11}  {'margin%':>8}"
        )
        totals = dict(sales=Decimal("0"), cogs=Decimal("0"), selling_fees=Decimal("0"),
                      operational_fees=Decimal("0"), refunds=Decimal("0"),
                      reimbursements=Decimal("0"), gp_no_reimb=Decimal("0"),
                      gp_with_reimb=Decimal("0"))
        for r in rows:
            print(
                f"  {r.date}  {fmt(r.sales)}  {fmt(r.cogs)}  {fmt(r.selling_fees)}  "
                f"{fmt(r.operational_fees)}  {fmt(r.refunds)}  {fmt(r.reimbursements)}  "
                f"{fmt(r.gross_profit_no_reimb)}  {fmt(r.gross_profit_with_reimb)}  "
                f"{float(r.margin_pct):>7.2f}%"
            )
            for k, val in (("sales", r.sales), ("cogs", r.cogs),
                           ("selling_fees", r.selling_fees), ("operational_fees", r.operational_fees),
                           ("refunds", r.refunds), ("reimbursements", r.reimbursements),
                           ("gp_no_reimb", r.gross_profit_no_reimb),
                           ("gp_with_reimb", r.gross_profit_with_reimb)):
                totals[k] += val
        print(
            f"  {'TOTAL':10}  {fmt(totals['sales'])}  {fmt(totals['cogs'])}  "
            f"{fmt(totals['selling_fees'])}  {fmt(totals['operational_fees'])}  "
            f"{fmt(totals['refunds'])}  {fmt(totals['reimbursements'])}  "
            f"{fmt(totals['gp_no_reimb'])}  {fmt(totals['gp_with_reimb'])}"
        )

        # Unmapped items
        unm_stmt = (
            select(
                UnmappedLineItem.event_type,
                UnmappedLineItem.line_item_name,
                func.count().label("count"),
                func.coalesce(func.sum(UnmappedLineItem.amount), 0).label("total"),
            )
            .where(
                and_(
                    UnmappedLineItem.marketplace_id == US_MARKETPLACE_ID,
                    local_date_expr(UnmappedLineItem.posted_date) >= START,
                    local_date_expr(UnmappedLineItem.posted_date) <= END,
                )
            )
            .group_by(UnmappedLineItem.event_type, UnmappedLineItem.line_item_name)
            .order_by(func.count().desc())
        )
        unm = (await session.execute(unm_stmt)).all()
        print(f"\n  Unmapped line items in window: {len(unm)} distinct (event_type, line_item)")
        if unm:
            print(f"  {'event_type':30}  {'line_item_name':40}  {'count':>6}  {'sum':>12}")
            for et, name, cnt, total in unm[:30]:
                print(f"  {et:30}  {name:40}  {cnt:>6}  {float(total):>12,.2f}")
        else:
            print("  (none -- mapping covered every line item)")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
