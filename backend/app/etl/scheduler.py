"""APScheduler jobs for automated daily ETL runs.

Jobs (all UTC):
  daily_sp_etl    – 06:00  Amazon SP-API financial events (by-date)
  daily_ads_etl   – 06:30  Amazon Ads SP/SB/SD campaign spend reports
  daily_pnl       – 07:00  P&L aggregation across all sources

All jobs pull *yesterday* (UTC) so the full previous day's data is
available when Amazon's APIs have settled.  Jobs are idempotent: re-running
for the same date safely overwrites existing rows.

Settlement Taxes ingestion and by-group reconciliation are *not* scheduled
here; they are monthly operations run manually (see CLAUDE.md).
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
from app.etl.amazon_etl import ALL_MARKETPLACES, run_amazon_etl
from app.etl.pnl_calculator import calculate_daily_pnl

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


def _yesterday_utc() -> date:
    return date.today() - timedelta(days=1)


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
        logger.info(
            "scheduler.sp_etl done date=%s events=%s",
            target,
            summary.to_dict(),
        )
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
        logger.info(
            "scheduler.ads_etl done date=%s summary=%s",
            target,
            summary.to_dict(),
        )
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
            target,
            result.rows_written,
            result.skus_without_cogs,
        )
    except Exception:
        logger.exception("scheduler.pnl failed date=%s", target)


def build_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone=timezone.utc)
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
