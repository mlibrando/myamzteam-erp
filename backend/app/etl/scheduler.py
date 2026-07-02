"""APScheduler jobs for automated daily and monthly ETL runs.

Daily jobs (all UTC, pull *yesterday*):
  daily_sp_etl    – 06:00       Amazon SP-API financial events (by-date)
  daily_ads_etl   – 06:30       Amazon Ads SP/SB/SD campaign spend
  daily_pnl       – 07:00       P&L aggregation across all sources

Monthly jobs (all UTC, operate on PT-anchored calendar months):
  monthly_by_group_rec           – day 10 10:00  by-group reconciliation for prior month
  monthly_settlement_taxes       – day 10 11:00  settlement Taxes ingestion for prior month
  monthly_catchup_settlement     – day 15 10:00  catch-up settlement Taxes for month before prior
  monthly_pnl_recalc             – day 15 12:00  P&L recalc for the full reconciled window

Day-10 gives Amazon's last biweekly settlement cycle of the prior month time to
close and publish (~1-2 days after cycle end).  Day-15 re-runs settlement Taxes
for the month before that, picking up the late-arriving refund reversals and
tax adjustments that cause ±5-8% drift in the day-10 snapshot.

All jobs are idempotent and error-isolated: an exception in one job is logged
and swallowed so it never kills the scheduler process.

Manual overrides are always available via the /api/etl/* endpoints.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta, timezone
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.database import AsyncSessionLocal
from app.etl.amazon_ads_etl import (
    ALL_MARKETPLACES as ADS_ALL_MARKETPLACES,
    run_amazon_ads_etl,
)
from app.etl.amazon_etl import (
    ALL_MARKETPLACES,
    run_amazon_by_group_reconciliation,
    run_amazon_etl,
)
from app.etl.amazon_settlement_etl import run_amazon_settlement_ingestion
from app.etl.pnl_calculator import calculate_daily_pnl

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _yesterday_utc() -> date:
    return date.today() - timedelta(days=1)


def _prior_month() -> tuple[date, date]:
    """Return (first, last) of the calendar month before today's month."""
    today = date.today()
    last = today.replace(day=1) - timedelta(days=1)
    return last.replace(day=1), last


def _month_before_prior() -> tuple[date, date]:
    """Return (first, last) of two calendar months ago."""
    first_of_prior, _ = _prior_month()
    last = first_of_prior - timedelta(days=1)
    return last.replace(day=1), last


# ---------------------------------------------------------------------------
# Daily jobs
# ---------------------------------------------------------------------------

async def job_daily_sp_etl() -> None:
    """Pull Amazon SP-API financial events for yesterday (UTC)."""
    target = _yesterday_utc()
    logger.info("scheduler.sp_etl start date=%s", target)
    try:
        async with AsyncSessionLocal() as session:
            summary = await run_amazon_etl(
                session,
                start_date=target,
                end_date=target,
                marketplace_ids=list(ALL_MARKETPLACES),
            )
            await session.commit()
        logger.info("scheduler.sp_etl done date=%s events=%s", target, summary.to_dict())
    except Exception:
        logger.exception("scheduler.sp_etl failed date=%s", target)


async def job_daily_ads_etl() -> None:
    """Pull Amazon Ads SP/SB/SD spend reports for yesterday (UTC)."""
    target = _yesterday_utc()
    logger.info("scheduler.ads_etl start date=%s", target)
    try:
        summary = await run_amazon_ads_etl(
            AsyncSessionLocal,
            start_date=target,
            end_date=target,
            marketplace_ids=list(ADS_ALL_MARKETPLACES),
        )
        logger.info("scheduler.ads_etl done date=%s summary=%s", target, summary.to_dict())
    except Exception:
        logger.exception("scheduler.ads_etl failed date=%s", target)


async def job_daily_pnl() -> None:
    """Aggregate all sources into daily_pnl for yesterday (UTC)."""
    target = _yesterday_utc()
    logger.info("scheduler.pnl start date=%s", target)
    try:
        async with AsyncSessionLocal() as session:
            result = await calculate_daily_pnl(
                session,
                start_date=target,
                end_date=target,
                marketplace_ids=list(ALL_MARKETPLACES),
            )
            await session.commit()
        logger.info(
            "scheduler.pnl done date=%s rows_written=%s skus_without_cogs=%s",
            target, result.rows_written, result.skus_without_cogs,
        )
    except Exception:
        logger.exception("scheduler.pnl failed date=%s", target)


# ---------------------------------------------------------------------------
# Monthly jobs
# ---------------------------------------------------------------------------

async def job_monthly_by_group_reconciliation() -> None:
    """Day 10: replace by-date events with by-group data for the prior month.

    Closes the ~$4.4k Op Fees gap (storage / long-term-storage /
    inbound-convenience / removal / inbound-transportation / subscription)
    that the daily by-date ETL misses because those fees only appear in
    Closed settlement groups.
    """
    start, end = _prior_month()
    logger.info("scheduler.monthly_by_group start=%s end=%s", start, end)
    try:
        async with AsyncSessionLocal() as session:
            summary = await run_amazon_by_group_reconciliation(
                session,
                window_start_pt=start,
                window_end_pt=end,
                marketplace_ids=list(ALL_MARKETPLACES),
            )
            await session.commit()
        logger.info(
            "scheduler.monthly_by_group done start=%s end=%s summary=%s",
            start, end, summary.to_dict(),
        )
    except Exception:
        logger.exception("scheduler.monthly_by_group failed start=%s end=%s", start, end)


async def job_monthly_settlement_taxes() -> None:
    """Day 10: ingest settlement Taxes-remitted-to-Amazon for the prior month.

    Closes the ~$11k Selling Fees gap that neither by-date nor by-group
    surfaces — Taxes-remitted only appear in settlement flat-file reports.
    """
    start, end = _prior_month()
    logger.info("scheduler.monthly_settlement start=%s end=%s", start, end)
    try:
        async with AsyncSessionLocal() as session:
            summary = await run_amazon_settlement_ingestion(
                session,
                window_start_pt=start,
                window_end_pt=end,
                marketplace_ids=list(ALL_MARKETPLACES),
            )
            await session.commit()
        logger.info(
            "scheduler.monthly_settlement done start=%s end=%s summary=%s",
            start, end, summary.to_dict(),
        )
    except Exception:
        logger.exception("scheduler.monthly_settlement failed start=%s end=%s", start, end)


async def job_monthly_catchup_settlement_taxes() -> None:
    """Day 15: re-run settlement Taxes for the month before the prior month.

    Late-arriving refund reversals and post-hoc tax adjustments trickle in
    for 2-3 weeks after a month closes, reducing the ±5-8% snapshot drift
    measured on day 10.  This catch-up run picks up that tail.
    """
    start, end = _month_before_prior()
    logger.info("scheduler.catchup_settlement start=%s end=%s", start, end)
    try:
        async with AsyncSessionLocal() as session:
            summary = await run_amazon_settlement_ingestion(
                session,
                window_start_pt=start,
                window_end_pt=end,
                marketplace_ids=list(ALL_MARKETPLACES),
            )
            await session.commit()
        logger.info(
            "scheduler.catchup_settlement done start=%s end=%s summary=%s",
            start, end, summary.to_dict(),
        )
    except Exception:
        logger.exception("scheduler.catchup_settlement failed start=%s end=%s", start, end)


async def job_monthly_pnl_recalc() -> None:
    """Day 15: re-aggregate P&L for the full reconciled two-month window.

    Runs after both the day-10 by-group/settlement pass (prior month) and
    the day-15 catch-up settlement pass (month before prior) have completed,
    so daily_pnl reflects the latest reconciled numbers for both months.
    """
    start, _ = _month_before_prior()
    _, end = _prior_month()
    logger.info("scheduler.monthly_pnl_recalc start=%s end=%s", start, end)
    try:
        async with AsyncSessionLocal() as session:
            result = await calculate_daily_pnl(
                session,
                start_date=start,
                end_date=end,
                marketplace_ids=list(ALL_MARKETPLACES),
            )
            await session.commit()
        logger.info(
            "scheduler.monthly_pnl_recalc done start=%s end=%s rows_written=%s",
            start, end, result.rows_written,
        )
    except Exception:
        logger.exception("scheduler.monthly_pnl_recalc failed start=%s end=%s", start, end)


# ---------------------------------------------------------------------------
# Scheduler lifecycle
# ---------------------------------------------------------------------------

def build_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone=timezone.utc)

    # --- Daily ---
    scheduler.add_job(
        job_daily_sp_etl,
        trigger=CronTrigger(hour=6, minute=0, timezone=timezone.utc),
        id="daily_sp_etl",
        name="Daily Amazon SP-API ETL",
        replace_existing=True,
        misfire_grace_time=3600,
        max_instances=1,
    )
    scheduler.add_job(
        job_daily_ads_etl,
        trigger=CronTrigger(hour=6, minute=30, timezone=timezone.utc),
        id="daily_ads_etl",
        name="Daily Amazon Ads ETL",
        replace_existing=True,
        misfire_grace_time=3600,
        max_instances=1,
    )
    scheduler.add_job(
        job_daily_pnl,
        trigger=CronTrigger(hour=7, minute=0, timezone=timezone.utc),
        id="daily_pnl",
        name="Daily P&L aggregation",
        replace_existing=True,
        misfire_grace_time=3600,
        max_instances=1,
    )

    # --- Monthly ---
    scheduler.add_job(
        job_monthly_by_group_reconciliation,
        trigger=CronTrigger(day=10, hour=10, minute=0, timezone=timezone.utc),
        id="monthly_by_group_rec",
        name="Monthly by-group reconciliation",
        replace_existing=True,
        misfire_grace_time=7200,
        max_instances=1,
    )
    scheduler.add_job(
        job_monthly_settlement_taxes,
        trigger=CronTrigger(day=10, hour=11, minute=0, timezone=timezone.utc),
        id="monthly_settlement_taxes",
        name="Monthly settlement Taxes ingestion",
        replace_existing=True,
        misfire_grace_time=7200,
        max_instances=1,
    )
    scheduler.add_job(
        job_monthly_catchup_settlement_taxes,
        trigger=CronTrigger(day=15, hour=10, minute=0, timezone=timezone.utc),
        id="monthly_catchup_settlement",
        name="Monthly catch-up settlement Taxes",
        replace_existing=True,
        misfire_grace_time=7200,
        max_instances=1,
    )
    scheduler.add_job(
        job_monthly_pnl_recalc,
        trigger=CronTrigger(day=15, hour=12, minute=0, timezone=timezone.utc),
        id="monthly_pnl_recalc",
        name="Monthly P&L recalculation",
        replace_existing=True,
        misfire_grace_time=7200,
        max_instances=1,
    )

    return scheduler


def start_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if _scheduler is not None:
        return _scheduler
    _scheduler = build_scheduler()
    _scheduler.start()
    logger.info(
        "scheduler started: %s",
        [j.name for j in _scheduler.get_jobs()],
    )
    return _scheduler


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None
        logger.info("scheduler stopped")


def get_scheduler_status() -> dict[str, Any]:
    """Return current job status for the /api/scheduler/status endpoint."""
    if _scheduler is None:
        return {"running": False, "jobs": []}
    jobs = []
    for job in _scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run_utc": job.next_run_time.isoformat() if job.next_run_time else None,
        })
    return {"running": _scheduler.running, "jobs": jobs}
