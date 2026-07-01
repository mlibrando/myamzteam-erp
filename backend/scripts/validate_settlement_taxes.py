"""Live validation for settlement report ingestion — Taxes remitted only.

Sequence:
1. Print daily_pnl BEFORE settlement ingestion (already has PT + by-group).
2. Run settlement ingestion for PT-Jan 2026 US.
3. Re-run pnl_calculator.
4. Print daily_pnl AFTER + gap vs Elena's Jan 2026 Selling Fees ($51,401).

Run from backend/:
    .venv/bin/python -m scripts.validate_settlement_taxes
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import date
from decimal import Decimal

from sqlalchemy import and_, select

from app.database import AsyncSessionLocal
from app.etl.amazon_settlement_etl import run_amazon_settlement_ingestion
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
    print("[1/4] daily_pnl totals BEFORE settlement ingestion")
    async with AsyncSessionLocal() as session:
        before = await _totals(session)
    for k, v in before.items():
        print(f"  {k:>22}  {_fmt(v)}")

    print(f"\n[2/4] Running settlement ingestion for PT {START}..{END}")
    async with AsyncSessionLocal() as session:
        recon = await run_amazon_settlement_ingestion(
            session,
            window_start_pt=START,
            window_end_pt=END,
            marketplace_ids=[US_MARKETPLACE_ID],
        )
        await session.commit()
    print(f"  reports_listed:           {recon.reports_listed}")
    print(f"  reports_processed:        {recon.reports_processed}")
    print(f"  rows_scanned:             {recon.rows_scanned}")
    print(f"  tax_rows_matched:         {recon.tax_rows_matched}")
    print(f"  rows_deleted (settlement): {recon.rows_deleted}")
    print(f"  rows_inserted:            {recon.rows_inserted}")
    print(f"  by_amount_description:    {recon.by_amount_description}")

    print(f"\n[3/4] Re-running pnl_calculator for PT {START}..{END}")
    async with AsyncSessionLocal() as session:
        pnl_calc = await calculate_daily_pnl(
            session,
            start_date=START,
            end_date=END,
            marketplace_ids=[US_MARKETPLACE_ID],
        )
        await session.commit()
    print(f"  daily_pnl rows_written:   {pnl_calc.rows_written}")

    print(f"\n[4/4] daily_pnl totals AFTER settlement ingestion")
    async with AsyncSessionLocal() as session:
        after = await _totals(session)
    print(f"  {'category':>22}  {'before':>12}  {'after':>12}  {'delta':>12}")
    for k in before:
        b, a = before[k], after[k]
        print(f"  {k:>22}  {_fmt(b)}  {_fmt(a)}  {_fmt(a - b)}")

    print("\n[Elena target vs after]")
    elena = {
        "sales": Decimal("175191.94"),
        "selling_fees": Decimal("51401"),
        "operational_fees": Decimal("5722"),
    }
    for k, target in elena.items():
        gap = after[k] - target
        pct = float(gap / target * 100) if target != 0 else 0.0
        print(f"  {k:>22}  after={_fmt(after[k])}  elena={_fmt(target)}  "
              f"gap={_fmt(gap)}  ({pct:+.1f}%)")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
