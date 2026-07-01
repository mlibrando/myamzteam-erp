"""End-to-end live validation for the Amazon Ads ETL.

Pulls the last 7 days of SP/SB/SD spend for NA marketplaces, persists to
ad_spend, recomputes daily_pnl, and prints summary tables so the totals can
be compared against the Amazon Advertising console.

Also exercises:
  - profile discovery (lists profileIds returned)
  - parallel report execution per marketplace (SP+SB+SD in flight together)
  - idempotency (runs the ETL twice, checks row count is stable)
  - daily_pnl ad_spend_* columns getting populated by pnl_calculator

Run from backend/ with .env loaded:
    .venv/bin/python -m scripts.validate_amazon_ads_etl
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import date, timedelta

from sqlalchemy import and_, func, select

from app.database import AsyncSessionLocal
from app.etl.amazon_ads_etl import run_amazon_ads_etl
from app.etl.pnl_calculator import calculate_daily_pnl
from app.models import AdSpend, DailyPnL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

NA_MARKETPLACES = ["ATVPDKIKX0DER", "A2EUQ1WTGCTBG2", "A1AM78C64UM0Y8"]
SHORT_CODES = {"ATVPDKIKX0DER": "us", "A2EUQ1WTGCTBG2": "ca", "A1AM78C64UM0Y8": "mx"}
CHANNELS = {"ATVPDKIKX0DER": "amazon_us", "A2EUQ1WTGCTBG2": "amazon_ca", "A1AM78C64UM0Y8": "amazon_mx"}


async def _ad_spend_totals(session, *, start: date, end: date) -> dict:
    stmt = (
        select(AdSpend.marketplace, AdSpend.platform,
               func.count(), func.coalesce(func.sum(AdSpend.spend), 0))
        .where(and_(AdSpend.date >= start, AdSpend.date <= end))
        .group_by(AdSpend.marketplace, AdSpend.platform)
        .order_by(AdSpend.marketplace, AdSpend.platform)
    )
    return list((await session.execute(stmt)).all())


async def _daily_pnl_ad_columns(session, *, start: date, end: date) -> list:
    stmt = (
        select(DailyPnL.date, DailyPnL.channel,
               DailyPnL.ad_spend, DailyPnL.ad_spend_sp,
               DailyPnL.ad_spend_sb, DailyPnL.ad_spend_sd)
        .where(and_(DailyPnL.date >= start, DailyPnL.date <= end,
                    DailyPnL.channel.in_(list(CHANNELS.values()))))
        .order_by(DailyPnL.date, DailyPnL.channel)
    )
    return list((await session.execute(stmt)).all())


async def main() -> int:
    today = date.today()
    end = today - timedelta(days=1)
    start = end - timedelta(days=6)

    print(f"[1/4] Window: {start} -> {end} ({(end-start).days + 1} days), NA marketplaces")

    print("[2/4] First ETL run")
    s1 = await run_amazon_ads_etl(
        AsyncSessionLocal, start_date=start, end_date=end, marketplace_ids=NA_MARKETPLACES
    )
    async with AsyncSessionLocal() as session:
        p1 = await calculate_daily_pnl(
            session, start_date=start, end_date=end, marketplace_ids=NA_MARKETPLACES
        )
        await session.commit()
    print(f"   profiles_resolved: {s1.profiles_resolved}")
    print(f"   reports_created: {s1.reports_created}  completed: {s1.reports_completed}  "
          f"failed: {len(s1.reports_failed)}")
    print(f"   rows_inserted: {s1.rows_inserted}  by_platform: {s1.rows_by_platform}")
    print(f"   spend_by_platform: {s1.spend_by_platform}")
    print(f"   skipped_marketplaces: {s1.skipped_marketplaces}")
    print(f"   pnl rows_written: {p1.rows_written}")
    if s1.reports_failed:
        print("   FAILURES:")
        for f in s1.reports_failed:
            print(f"     {f}")

    print("[3/4] Idempotency: re-run same window")
    s2 = await run_amazon_ads_etl(
        AsyncSessionLocal, start_date=start, end_date=end, marketplace_ids=NA_MARKETPLACES
    )
    async with AsyncSessionLocal() as session:
        await calculate_daily_pnl(
            session, start_date=start, end_date=end, marketplace_ids=NA_MARKETPLACES
        )
        await session.commit()
        # Verify ad_spend row count is stable (purged + reinserted, not appended)
        count_stmt = select(func.count(AdSpend.id)).where(
            and_(AdSpend.date >= start, AdSpend.date <= end,
                 AdSpend.marketplace.in_([SHORT_CODES[m] for m in NA_MARKETPLACES]))
        )
        ad_count = (await session.execute(count_stmt)).scalar_one()
    print(f"   re-run rows_inserted: {s2.rows_inserted} (first run: {s1.rows_inserted})")
    print(f"   ad_spend row count in DB after re-run: {ad_count}")
    # Idempotency check: DB row count must equal the LAST run's inserted count
    # (proves purge-then-insert cycle, no accumulation across runs). Amazon
    # legitimately returns slightly different row counts across runs as
    # attribution windows update, and transient failures (e.g. throttled
    # reports) may succeed on retry -- so we don't require s1 == s2.
    assert ad_count == s2.rows_inserted, (
        f"ad_spend rows ({ad_count}) don't match latest run's inserts "
        f"({s2.rows_inserted}) -- purge+reinsert broken"
    )

    print(f"\n[4/4] ad_spend totals by (marketplace, platform):")
    async with AsyncSessionLocal() as session:
        totals = await _ad_spend_totals(session, start=start, end=end)
        print(f"   {'mkt':5}  {'platform':12}  {'rows':>6}  {'spend':>10}")
        for mkt, plat, cnt, spend in totals:
            print(f"   {mkt:5}  {plat:12}  {cnt:>6}  {float(spend):>10,.2f}")

        print(f"\n   daily_pnl ad_spend* columns:")
        rows = await _daily_pnl_ad_columns(session, start=start, end=end)
        print(f"   {'date':10}  {'channel':10}  {'total':>10}  {'sp':>10}  {'sb':>10}  {'sd':>10}")
        for d, ch, total, sp, sb, sd in rows:
            print(f"   {d}  {ch:10}  {float(total):>10,.2f}  {float(sp):>10,.2f}  {float(sb):>10,.2f}  {float(sd):>10,.2f}")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
