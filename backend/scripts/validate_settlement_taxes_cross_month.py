"""Run settlement Taxes ingestion for multiple months, report residual
vs Elena's confirmed Sellerise-export values, and print the drift pattern.

If a target month is more than ~90 days back, Amazon's Reports API
createdSince limit blocks listing its settlement reports and we get
an empty ingestion (documented, not a bug).

Run from backend/:
    .venv/bin/python -m scripts.validate_settlement_taxes_cross_month
"""

from __future__ import annotations

import asyncio
import calendar
import logging
import sys
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from sqlalchemy import and_, func, select

from app.database import AsyncSessionLocal
from app.etl.amazon_settlement_etl import run_amazon_settlement_ingestion
from app.etl.pnl_calculator import calculate_daily_pnl
from app.etl.timezone_utils import local_date_expr
from app.models import DailyPnL, FinancialEvent

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

US_MARKETPLACE_ID = "ATVPDKIKX0DER"
US_CHANNEL = "amazon_us"


@dataclass
class MonthTarget:
    year: int
    month: int
    elena_magnitude: Decimal  # positive magnitude (Elena's raw signed value is -this)


TARGETS: list[MonthTarget] = [
    MonthTarget(2026, 3, Decimal("6676.90")),
    MonthTarget(2026, 4, Decimal("6061.17")),
    MonthTarget(2026, 6, Decimal("4591.77")),
]


async def _sum_settlement(session, month_start: date, month_end: date) -> tuple[int, Decimal]:
    """Sum fee_amount (magnitude) for source='settlement' rows in the
    PT-local month window."""
    date_col = local_date_expr(FinancialEvent.posted_date)
    stmt = (
        select(func.count(), func.coalesce(func.sum(FinancialEvent.fee_amount), 0))
        .where(
            and_(
                FinancialEvent.marketplace_id == US_MARKETPLACE_ID,
                FinancialEvent.source == "settlement",
                date_col >= month_start,
                date_col <= month_end,
            )
        )
    )
    cnt, total = (await session.execute(stmt)).one()
    return int(cnt), Decimal(str(total))


async def run_month(target: MonthTarget) -> dict:
    year, month = target.year, target.month
    start = date(year, month, 1)
    end = date(year, month, calendar.monthrange(year, month)[1])
    result: dict = {"year": year, "month": month, "elena": target.elena_magnitude}

    async with AsyncSessionLocal() as session:
        recon = await run_amazon_settlement_ingestion(
            session,
            window_start_pt=start,
            window_end_pt=end,
            marketplace_ids=[US_MARKETPLACE_ID],
        )
        await session.commit()
    result.update({
        "reports_listed": recon.reports_listed,
        "reports_processed": recon.reports_processed,
        "reports_skipped_as_duplicate": recon.reports_skipped_as_duplicate,
        "rows_scanned": recon.rows_scanned,
        "rows_inserted": recon.rows_inserted,
        "by_amount_description": recon.by_amount_description,
    })

    async with AsyncSessionLocal() as session:
        await calculate_daily_pnl(
            session,
            start_date=start,
            end_date=end,
            marketplace_ids=[US_MARKETPLACE_ID],
        )
        await session.commit()

    async with AsyncSessionLocal() as session:
        cnt, magnitude = await _sum_settlement(session, start, end)
    result["settlement_rows_in_db"] = cnt
    result["magnitude"] = magnitude
    return result


async def main() -> int:
    print(f"\nCross-month settlement Taxes validation for US "
          f"(magnitude convention; Elena's signed = -magnitude)\n")
    print(f"  {'month':>10}  {'reports':>8}  {'proc':>5}  {'skip':>5}  "
          f"{'rows':>6}  {'our_mag':>10}  {'elena_mag':>10}  "
          f"{'delta':>10}  {'pct':>8}")
    print("  " + "-" * 90)

    all_results = []
    for target in TARGETS:
        r = await run_month(target)
        all_results.append(r)
        our_mag = r["magnitude"]
        delta = our_mag - target.elena_magnitude
        pct = float(delta / target.elena_magnitude * 100) if target.elena_magnitude else 0.0
        print(f"  {target.year}-{target.month:02d}     "
              f"{r['reports_listed']:>8}  "
              f"{r['reports_processed']:>5}  "
              f"{r['reports_skipped_as_duplicate']:>5}  "
              f"{r['rows_inserted']:>6}  "
              f"{float(our_mag):>10,.2f}  "
              f"{float(target.elena_magnitude):>10,.2f}  "
              f"{float(delta):>+10,.2f}  {pct:>+7.2f}%")

    # Prior May 2026 result (from the previous run, still in DB)
    print("\n  (May 2026 US from prior run: 987 rows, magnitude 5,980.20, "
          "elena 5,587.53, delta +392.67, +7.03%)")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
