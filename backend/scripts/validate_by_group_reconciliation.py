"""Live validation for the by-group reconciliation ETL against Jan 2026 US.

Sequence:
1. Print daily_pnl totals BEFORE reconciliation (current by-date state).
2. Run reconciliation for PT-local Jan 2026.
3. Re-run calculate_daily_pnl for the same PT window.
4. Print daily_pnl totals AFTER reconciliation.
5. Show the delta per category — the interesting cells are Op Fees and
   Ad Spend (SP-API-side ProductAdsPayment events).

Elena's Jan 2026 US actuals were: Sales $175,191.94, Selling Fees
~$51,401, Op Fees ~$5,722. After the PT-cutoff commit, Sales moved to
$170,241, Selling Fees $40,423, Op Fees $929 — a $4,793 Op Fees gap.
By-group reconciliation should close ~$4,421 of that (empirically
observed via the diff scripts).

Run from backend/:
    .venv/bin/python -m scripts.validate_by_group_reconciliation
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import date
from decimal import Decimal

from sqlalchemy import and_, select

from app.database import AsyncSessionLocal
from app.etl.amazon_etl import run_amazon_by_group_reconciliation
from app.etl.pnl_calculator import calculate_daily_pnl
from app.models import DailyPnL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

US_MARKETPLACE_ID = "ATVPDKIKX0DER"
US_CHANNEL = "amazon_us"
START = date(2026, 1, 1)
END = date(2026, 1, 31)


async def _totals(session) -> dict[str, Decimal]:
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
    keys = ("sales", "cogs", "ad_spend", "selling_fees", "operational_fees",
            "refunds", "reimbursements", "gross_profit_no_reimb")
    totals = {k: Decimal("0") for k in keys}
    for row in rows:
        for k in keys:
            totals[k] += getattr(row, k)
    return totals


def _fmt(v: Decimal) -> str:
    return f"{float(v):>12,.2f}"


async def main() -> int:
    print("[1/4] daily_pnl totals BEFORE reconciliation (by-date only)")
    async with AsyncSessionLocal() as session:
        before = await _totals(session)
    for k, v in before.items():
        print(f"  {k:>22}  {_fmt(v)}")

    print(f"\n[2/4] Running by-group reconciliation for PT {START}..{END}")
    async with AsyncSessionLocal() as session:
        recon = await run_amazon_by_group_reconciliation(
            session,
            window_start_pt=START,
            window_end_pt=END,
            marketplace_ids=[US_MARKETPLACE_ID],
        )
        await session.commit()
    print(f"  groups_listed:            {recon.groups_listed}")
    print(f"  groups_closed_in_window:  {recon.groups_closed_in_window}")
    print(f"  groups_processed:         {recon.groups_processed}")
    print(f"  events_pulled:            {recon.events_pulled}")
    print(f"  line_items_mapped:        {recon.line_items_mapped}")
    print(f"  line_items_unmapped:      {recon.line_items_unmapped}")
    print(f"  by_category:              {recon.by_category}")
    print(f"  rows_deleted_by_date:     {recon.rows_deleted_by_date}")
    print(f"  rows_inserted_by_group:   {recon.rows_inserted_by_group}")

    print(f"\n[3/4] Re-running pnl_calculator for the reconciled window")
    async with AsyncSessionLocal() as session:
        pnl_calc = await calculate_daily_pnl(
            session,
            start_date=START,
            end_date=END,
            marketplace_ids=[US_MARKETPLACE_ID],
        )
        await session.commit()
    print(f"  daily_pnl rows_written:   {pnl_calc.rows_written}")

    print(f"\n[4/4] daily_pnl totals AFTER reconciliation")
    async with AsyncSessionLocal() as session:
        after = await _totals(session)

    print(
        f"  {'category':>22}  {'before':>12}  {'after':>12}  "
        f"{'delta':>12}"
    )
    for k in before:
        b, a = before[k], after[k]
        print(f"  {k:>22}  {_fmt(b)}  {_fmt(a)}  {_fmt(a - b)}")

    # Elena's Jan 2026 US targets, from her RAW_AMZ_US sheet (2026-07-01
    # snapshot). Values stored as MAGNITUDE (positive) matching daily_pnl
    # storage convention — daily_pnl stores expenses as positive
    # magnitudes; Elena's signed values are the negative of these.
    print("\n[Elena target vs after — daily_pnl magnitude convention]")
    elena = {
        "sales": Decimal("175191.94"),
        "selling_fees": Decimal("51401"),        # Elena signed: -51401
        "operational_fees": Decimal("14912.08"),  # Elena signed: -14912.08
        # Reimbursements: Elena's Sellerise formula excludes Reversal +
        # Warehouse-Lost, so her target after those exclusions is ~$1,442.
        "reimbursements": Decimal("1442"),
    }
    for k, target in elena.items():
        gap = after[k] - target
        pct = float(gap / target * 100) if target != 0 else 0.0
        print(f"  {k:>22}  after={_fmt(after[k])}  elena={_fmt(target)}  "
              f"gap={_fmt(gap)}  ({pct:+.1f}%)")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
