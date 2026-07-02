"""Tests for the APScheduler integration.

Coverage:
- build_scheduler() creates all 7 jobs with correct IDs and UTC cron times.
- _prior_month() / _month_before_prior() return correct PT-anchored windows.
- start/stop lifecycle and idempotency.
- get_scheduler_status() reflects running state.
- All job functions swallow ETL errors (error-isolation guarantee).
- Happy-path jobs commit their sessions.

No live DB connections or external API calls — all ETL functions are
patched via unittest.mock.  Lifecycle tests are async so AsyncIOScheduler
can bind to the running event loop.
"""

from __future__ import annotations

import calendar
from datetime import date, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.etl.scheduler import (
    _month_before_prior,
    _prior_month,
    build_scheduler,
    get_scheduler_status,
    job_daily_ads_etl,
    job_daily_pnl,
    job_daily_sp_etl,
    job_monthly_by_group_reconciliation,
    job_monthly_catchup_settlement_taxes,
    job_monthly_pnl_recalc,
    job_monthly_settlement_taxes,
    start_scheduler,
    stop_scheduler,
)

DAILY_JOB_IDS = {"daily_sp_etl", "daily_ads_etl", "daily_pnl"}
MONTHLY_JOB_IDS = {
    "monthly_by_group_rec",
    "monthly_settlement_taxes",
    "monthly_catchup_settlement",
    "monthly_pnl_recalc",
}
ALL_JOB_IDS = DAILY_JOB_IDS | MONTHLY_JOB_IDS


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def test_prior_month_first_and_last_day() -> None:
    # Pin today to 2026-07-10.
    with patch("app.etl.scheduler.date") as mock_date:
        mock_date.today.return_value = date(2026, 7, 10)
        mock_date.side_effect = lambda *a, **k: date(*a, **k)
        start, end = _prior_month()
    assert start == date(2026, 6, 1)
    assert end == date(2026, 6, 30)


def test_prior_month_january_rolls_back_to_december() -> None:
    with patch("app.etl.scheduler.date") as mock_date:
        mock_date.today.return_value = date(2026, 1, 15)
        mock_date.side_effect = lambda *a, **k: date(*a, **k)
        start, end = _prior_month()
    assert start == date(2025, 12, 1)
    assert end == date(2025, 12, 31)


def test_month_before_prior() -> None:
    with patch("app.etl.scheduler.date") as mock_date:
        mock_date.today.return_value = date(2026, 7, 15)
        mock_date.side_effect = lambda *a, **k: date(*a, **k)
        start, end = _month_before_prior()
    assert start == date(2026, 5, 1)
    assert end == date(2026, 5, 31)


def test_monthly_pnl_recalc_window_spans_two_months() -> None:
    """job_monthly_pnl_recalc covers month[-2] through month[-1]."""
    with patch("app.etl.scheduler.date") as mock_date:
        mock_date.today.return_value = date(2026, 7, 15)
        mock_date.side_effect = lambda *a, **k: date(*a, **k)
        prior_start, prior_end = _prior_month()
        mbp_start, _ = _month_before_prior()
    # Window: May 1 → Jun 30
    assert mbp_start == date(2026, 5, 1)
    assert prior_end == date(2026, 6, 30)


# ---------------------------------------------------------------------------
# Scheduler construction (no event loop needed — just inspect, don't start)
# ---------------------------------------------------------------------------

def test_build_scheduler_creates_all_jobs() -> None:
    sched = build_scheduler()
    assert {j.id for j in sched.get_jobs()} == ALL_JOB_IDS


def test_build_scheduler_daily_cron_times() -> None:
    sched = build_scheduler()

    def cron(job_id: str) -> tuple[int, int]:
        job = next(j for j in sched.get_jobs() if j.id == job_id)
        fields = {f.name: f for f in job.trigger.fields}
        return int(str(fields["hour"])), int(str(fields["minute"]))

    assert cron("daily_sp_etl") == (6, 0)
    assert cron("daily_ads_etl") == (6, 30)
    assert cron("daily_pnl") == (7, 0)


def test_build_scheduler_monthly_cron_times() -> None:
    sched = build_scheduler()

    def cron(job_id: str) -> tuple[int, int, int]:
        job = next(j for j in sched.get_jobs() if j.id == job_id)
        fields = {f.name: f for f in job.trigger.fields}
        return (
            int(str(fields["day"])),
            int(str(fields["hour"])),
            int(str(fields["minute"])),
        )

    assert cron("monthly_by_group_rec") == (10, 10, 0)
    assert cron("monthly_settlement_taxes") == (10, 11, 0)
    assert cron("monthly_catchup_settlement") == (15, 10, 0)
    assert cron("monthly_pnl_recalc") == (15, 12, 0)


def test_all_jobs_use_utc_timezone() -> None:
    sched = build_scheduler()
    for job in sched.get_jobs():
        assert job.trigger.timezone == timezone.utc, f"{job.id} not UTC"


# ---------------------------------------------------------------------------
# Lifecycle (async — AsyncIOScheduler needs a running event loop)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_stop_scheduler_lifecycle() -> None:
    try:
        sched = start_scheduler()
        assert sched.running
        status = get_scheduler_status()
        assert status["running"] is True
        assert len(status["jobs"]) == len(ALL_JOB_IDS)
    finally:
        stop_scheduler()
    assert get_scheduler_status() == {"running": False, "jobs": []}


@pytest.mark.asyncio
async def test_start_scheduler_is_idempotent() -> None:
    try:
        assert start_scheduler() is start_scheduler()
    finally:
        stop_scheduler()


def test_get_scheduler_status_when_not_running() -> None:
    stop_scheduler()
    assert get_scheduler_status() == {"running": False, "jobs": []}


# ---------------------------------------------------------------------------
# Error isolation — every job must swallow exceptions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sp_etl_job_swallows_errors() -> None:
    with patch("app.etl.scheduler.run_amazon_etl", new_callable=AsyncMock,
               side_effect=RuntimeError("API down")):
        await job_daily_sp_etl()


@pytest.mark.asyncio
async def test_ads_etl_job_swallows_errors() -> None:
    with patch("app.etl.scheduler.run_amazon_ads_etl", new_callable=AsyncMock,
               side_effect=RuntimeError("timeout")):
        await job_daily_ads_etl()


@pytest.mark.asyncio
async def test_pnl_job_swallows_errors() -> None:
    with patch("app.etl.scheduler.calculate_daily_pnl", new_callable=AsyncMock,
               side_effect=RuntimeError("DB error")):
        await job_daily_pnl()


@pytest.mark.asyncio
async def test_by_group_job_swallows_errors() -> None:
    with patch("app.etl.scheduler.run_amazon_by_group_reconciliation",
               new_callable=AsyncMock, side_effect=RuntimeError("SP-API 503")):
        await job_monthly_by_group_reconciliation()


@pytest.mark.asyncio
async def test_settlement_taxes_job_swallows_errors() -> None:
    with patch("app.etl.scheduler.run_amazon_settlement_ingestion",
               new_callable=AsyncMock, side_effect=RuntimeError("report missing")):
        await job_monthly_settlement_taxes()


@pytest.mark.asyncio
async def test_catchup_settlement_job_swallows_errors() -> None:
    with patch("app.etl.scheduler.run_amazon_settlement_ingestion",
               new_callable=AsyncMock, side_effect=RuntimeError("90-day limit")):
        await job_monthly_catchup_settlement_taxes()


@pytest.mark.asyncio
async def test_monthly_pnl_recalc_job_swallows_errors() -> None:
    with patch("app.etl.scheduler.calculate_daily_pnl", new_callable=AsyncMock,
               side_effect=RuntimeError("DB error")):
        await job_monthly_pnl_recalc()


# ---------------------------------------------------------------------------
# Happy-path commit verification
# ---------------------------------------------------------------------------

def _mock_session() -> AsyncMock:
    s = AsyncMock()
    s.__aenter__ = AsyncMock(return_value=s)
    s.__aexit__ = AsyncMock(return_value=False)
    return s


@pytest.mark.asyncio
async def test_sp_etl_job_commits_on_success() -> None:
    session = _mock_session()
    summary = MagicMock()
    summary.to_dict.return_value = {}
    with (
        patch("app.etl.scheduler.AsyncSessionLocal", return_value=session),
        patch("app.etl.scheduler.run_amazon_etl", new_callable=AsyncMock,
              return_value=summary),
    ):
        await job_daily_sp_etl()
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_by_group_job_commits_on_success() -> None:
    session = _mock_session()
    summary = MagicMock()
    summary.to_dict.return_value = {}
    with (
        patch("app.etl.scheduler.AsyncSessionLocal", return_value=session),
        patch("app.etl.scheduler.run_amazon_by_group_reconciliation",
              new_callable=AsyncMock, return_value=summary),
    ):
        await job_monthly_by_group_reconciliation()
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_settlement_taxes_job_commits_on_success() -> None:
    session = _mock_session()
    summary = MagicMock()
    summary.to_dict.return_value = {}
    with (
        patch("app.etl.scheduler.AsyncSessionLocal", return_value=session),
        patch("app.etl.scheduler.run_amazon_settlement_ingestion",
              new_callable=AsyncMock, return_value=summary),
    ):
        await job_monthly_settlement_taxes()
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_monthly_pnl_recalc_commits_on_success() -> None:
    session = _mock_session()
    result = MagicMock()
    result.rows_written = 62
    result.skus_without_cogs = []
    with (
        patch("app.etl.scheduler.AsyncSessionLocal", return_value=session),
        patch("app.etl.scheduler.calculate_daily_pnl", new_callable=AsyncMock,
              return_value=result),
    ):
        await job_monthly_pnl_recalc()
    session.commit.assert_awaited_once()
