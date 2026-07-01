"""Timezone helpers for monthly P&L cutoffs.

Amazon Seller Central defaults reporting cutoffs to Pacific Time. Elena's
manual P&L follows the same convention. Our storage layer keeps everything
in UTC (SP-API returns UTC PostedDate; we store TIMESTAMPTZ), but bucketing
and monthly windows must honor `settings.MONTHLY_CUTOFF_TIMEZONE`.

Two directions:
- `month_window_utc(year, month)` → the (start, end) UTC datetimes that
  correspond to (start of month, start of next month) in the cutoff tz.
  Use this when calling SP-API with PostedAfter/PostedBefore.
- `local_date_expr()` → a SQL expression that converts `posted_date` (UTC
  timestamptz) into a bare DATE in the cutoff tz. Use this in
  pnl_calculator's `group by ...day` clauses.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import Date, cast, literal
from sqlalchemy.sql import ColumnElement, func

from app.config import settings


def cutoff_zone() -> ZoneInfo:
    return ZoneInfo(settings.MONTHLY_CUTOFF_TIMEZONE)


def month_window_utc(year: int, month: int) -> tuple[datetime, datetime]:
    """(start_utc, end_utc) covering local-time `year-month`.

    start = year-month-01 00:00:00 in cutoff tz, converted to UTC.
    end   = start of the following month in cutoff tz, converted to UTC.
    Both are timezone-aware UTC datetimes suitable for passing to
    `AmazonSPConnector.get_financial_events_by_date`.
    """
    tz = cutoff_zone()
    start_local = datetime(year, month, 1, tzinfo=tz)
    if month == 12:
        end_local = datetime(year + 1, 1, 1, tzinfo=tz)
    else:
        end_local = datetime(year, month + 1, 1, tzinfo=tz)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def date_range_utc(start_local: date, end_local: date) -> tuple[datetime, datetime]:
    """Convert a local-date range (inclusive start, exclusive end) to UTC.

    Handy for validation scripts that speak in local-date terms:
        start_utc, end_utc = date_range_utc(date(2026, 1, 1), date(2026, 2, 1))
    """
    tz = cutoff_zone()
    start = datetime.combine(start_local, datetime.min.time(), tzinfo=tz)
    end = datetime.combine(end_local, datetime.min.time(), tzinfo=tz)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


def local_date_expr(posted_date_col: ColumnElement) -> ColumnElement:
    """SQL expression: (posted_date AT TIME ZONE cutoff_tz)::date.

    posted_date is TIMESTAMPTZ so `AT TIME ZONE X` yields the wall-clock
    timestamp in zone X (as TIMESTAMP WITHOUT TIME ZONE), from which we
    take the DATE component. This is the correct primitive for grouping
    events into the seller-facing daily bucket.
    """
    # func.timezone(tz, col) is portable across PG for TIMESTAMPTZ input.
    return cast(func.timezone(literal(settings.MONTHLY_CUTOFF_TIMEZONE), posted_date_col), Date)


__all__ = [
    "cutoff_zone",
    "month_window_utc",
    "date_range_utc",
    "local_date_expr",
]
