"""Tests for the APScheduler integration.

We verify:
- build_scheduler() creates three jobs with the correct IDs and UTC cron times.
- start_scheduler() / stop_scheduler() lifecycle works without errors.
- get_scheduler_status() reflects running state and job names.
- The scheduled job functions handle ETL errors without propagating them (so a
  single nightly failure doesn't kill the whole scheduler process).

No live DB connections or external API calls are made; all ETL functions are
patched via unittest.mock.  Lifecycle tests are async so APScheduler's
AsyncIOScheduler has a running event loop to bind to.
"""

from __future__ import annotations

from datetime import timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.etl.scheduler import (
    build_scheduler,
    get_scheduler_status,
    job_daily_ads_etl,
    job_daily_pnl,
    job_daily_sp_etl,
    start_scheduler,
    stop_scheduler,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _job_cron(scheduler, job_id: str) -> tuple[int, int]:
    job = next(j for j in scheduler.get_jobs() if j.id == job_id)
    fields = {f.name: f for f in job.trigger.fields}
    return int(str(fields["hour"])), int(str(fields["minute"]))


# ---------------------------------------------------------------------------
# Scheduler construction (no event loop needed — just inspect, don't start)
# ---------------------------------------------------------------------------

def test_build_scheduler_creates_three_jobs() -> None:
    sched = build_scheduler()
    assert set(j.id for j in sched.get_jobs()) == {
        "daily_sp_etl", "daily_ads_etl", "daily_pnl"
    }


def test_build_scheduler_cron_times_utc() -> None:
    sched = build_scheduler()
    assert _job_cron(sched, "daily_sp_etl") == (6, 0)
    assert _job_cron(sched, "daily_ads_etl") == (6, 30)
    assert _job_cron(sched, "daily_pnl") == (7, 0)
    for job in sched.get_jobs():
        assert job.trigger.timezone == timezone.utc


# ---------------------------------------------------------------------------
# Lifecycle (async so AsyncIOScheduler can bind to the running event loop)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_stop_scheduler_lifecycle() -> None:
    try:
        sched = start_scheduler()
        assert sched.running
        status = get_scheduler_status()
        assert status["running"] is True
        assert len(status["jobs"]) == 3
    finally:
        stop_scheduler()

    status_after = get_scheduler_status()
    assert status_after["running"] is False


@pytest.mark.asyncio
async def test_start_scheduler_is_idempotent() -> None:
    """Calling start_scheduler() twice returns the same instance."""
    try:
        s1 = start_scheduler()
        s2 = start_scheduler()
        assert s1 is s2
    finally:
        stop_scheduler()


def test_get_scheduler_status_when_not_running() -> None:
    stop_scheduler()  # ensure clean state (module-level singleton may be set)
    status = get_scheduler_status()
    assert status == {"running": False, "jobs": []}


# ---------------------------------------------------------------------------
# Job error isolation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sp_etl_job_swallows_etl_errors() -> None:
    with patch(
        "app.etl.scheduler.run_amazon_etl",
        new_callable=AsyncMock,
        side_effect=RuntimeError("API down"),
    ):
        await job_daily_sp_etl()


@pytest.mark.asyncio
async def test_ads_etl_job_swallows_etl_errors() -> None:
    with patch(
        "app.etl.scheduler.run_amazon_ads_etl",
        new_callable=AsyncMock,
        side_effect=RuntimeError("timeout"),
    ):
        await job_daily_ads_etl()


@pytest.mark.asyncio
async def test_pnl_job_swallows_calc_errors() -> None:
    with patch(
        "app.etl.scheduler.calculate_daily_pnl",
        new_callable=AsyncMock,
        side_effect=RuntimeError("DB error"),
    ):
        await job_daily_pnl()


# ---------------------------------------------------------------------------
# Successful job execution (happy path, mock out DB + ETL)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sp_etl_job_commits_on_success() -> None:
    mock_summary = MagicMock()
    mock_summary.to_dict.return_value = {}

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch("app.etl.scheduler.AsyncSessionLocal", return_value=mock_session),
        patch(
            "app.etl.scheduler.run_amazon_etl",
            new_callable=AsyncMock,
            return_value=mock_summary,
        ),
    ):
        await job_daily_sp_etl()

    mock_session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_pnl_job_commits_on_success() -> None:
    mock_result = MagicMock()
    mock_result.rows_written = 5
    mock_result.skus_without_cogs = []

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch("app.etl.scheduler.AsyncSessionLocal", return_value=mock_session),
        patch(
            "app.etl.scheduler.calculate_daily_pnl",
            new_callable=AsyncMock,
            return_value=mock_result,
        ),
    ):
        await job_daily_pnl()

    mock_session.commit.assert_awaited_once()
