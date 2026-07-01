"""Timezone helpers cover the PT month-window and local-date conversions
that anchor monthly P&L cutoffs to Amazon Seller Central's default. The
tests exercise both PST (winter) and PDT (summer) so the DST edges don't
regress silently later."""

from __future__ import annotations

from datetime import date, datetime, timezone

from app.etl.timezone_utils import date_range_utc, month_window_utc


def test_month_window_utc_pst_january() -> None:
    # January is fully PST (UTC-8). Jan 1 00:00 PT = Jan 1 08:00 UTC.
    start, end = month_window_utc(2026, 1)
    assert start == datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 2, 1, 8, 0, tzinfo=timezone.utc)


def test_month_window_utc_pdt_july() -> None:
    # July is PDT (UTC-7). Jul 1 00:00 PT = Jul 1 07:00 UTC.
    start, end = month_window_utc(2026, 7)
    assert start == datetime(2026, 7, 1, 7, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 8, 1, 7, 0, tzinfo=timezone.utc)


def test_month_window_utc_december_rolls_to_next_year() -> None:
    start, end = month_window_utc(2026, 12)
    assert start == datetime(2026, 12, 1, 8, 0, tzinfo=timezone.utc)
    assert end == datetime(2027, 1, 1, 8, 0, tzinfo=timezone.utc)


def test_month_window_utc_dst_spring_forward_march() -> None:
    # March 2026: DST begins Sunday Mar 8 at 02:00 PST -> 03:00 PDT.
    # Start of month is still PST (UTC-8); the month window's end is April 1,
    # which is PDT (UTC-7). So the window is asymmetric — that's real and
    # the callers relying on this API must accept UTC offsets that differ
    # at the start and end of the month.
    start, end = month_window_utc(2026, 3)
    assert start == datetime(2026, 3, 1, 8, 0, tzinfo=timezone.utc)  # PST
    assert end == datetime(2026, 4, 1, 7, 0, tzinfo=timezone.utc)    # PDT


def test_date_range_utc_matches_month_window() -> None:
    # date_range_utc is the lower-level primitive; month_window_utc is a
    # thin wrapper over it. Sanity-check they agree for a random month.
    start_a, end_a = month_window_utc(2026, 6)
    start_b, end_b = date_range_utc(date(2026, 6, 1), date(2026, 7, 1))
    assert start_a == start_b
    assert end_a == end_b


def test_date_range_utc_arbitrary_window() -> None:
    # Non-month-aligned window used by validation scripts.
    start, end = date_range_utc(date(2026, 1, 15), date(2026, 1, 20))
    assert start == datetime(2026, 1, 15, 8, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 1, 20, 8, 0, tzinfo=timezone.utc)
