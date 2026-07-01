"""Head-to-head: pull Jan 2026 US financial events via by-date and via
by-group, then diff event types + amount totals per type.

Goal: understand whether the by-group endpoint surfaces fee types (Taxes
remitted to Amazon, storage renewal, placement, disposal, removal) that
the by-date endpoint drops — which would explain the ~$10,978 Selling
Fees / ~$4,793 Op Fees gap against Elena's manual Jan 2026 US P&L.

Method:
- by-date: PostedAfter=2026-01-01T00:00:00Z, PostedBefore=2026-02-01T00:00:00Z
- by-group: iterate every Closed group whose window overlaps Jan 2026 US
  and merge. Post-filter to events with PostedDate in Jan 2026 to keep
  the comparison apples-to-apples (a group can span Jan+Feb).

For each event type, sum the "amount" values found in the event
structure — walk any nested dicts with CurrencyCode/CurrencyAmount pairs.

Run from backend/:
    .venv/bin/python -m scripts.diff_bygroup_vs_bydate
"""

from __future__ import annotations

import asyncio
import logging
import sys
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

from app.connectors.amazon_sp import AmazonSPConnector

logging.basicConfig(level=logging.WARNING)

WINDOW_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
WINDOW_END = datetime(2026, 2, 1, tzinfo=timezone.utc)


def _iso_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_dt(x: str | None) -> datetime | None:
    if not x:
        return None
    try:
        return datetime.fromisoformat(x.replace("Z", "+00:00"))
    except ValueError:
        return None


def _in_window(event: dict) -> bool:
    """Filter events by PostedDate ∈ Jan 2026 UTC. Events without PostedDate
    (some ServiceFeeEvents) are kept — they'll be attributed to the group's
    window on the by-group side and to the by-date query window on the
    by-date side, so counting them everywhere is intentional (matches ETL
    behavior)."""
    pd = _parse_dt(event.get("PostedDate"))
    if pd is None:
        return True
    return WINDOW_START <= pd < WINDOW_END


def _walk_amounts(node, out: list[Decimal]) -> None:
    """Collect every {CurrencyCode: 'USD', CurrencyAmount: X} pair found in
    a nested event structure. We could restrict to USD but the by-date
    query for the US marketplace returns USD anyway."""
    if isinstance(node, dict):
        if "CurrencyAmount" in node and node.get("CurrencyCode") == "USD":
            try:
                out.append(Decimal(str(node["CurrencyAmount"])))
            except Exception:
                pass
        for v in node.values():
            _walk_amounts(v, out)
    elif isinstance(node, list):
        for v in node:
            _walk_amounts(v, out)


def _summarize(events_by_type: dict[str, list[dict]]) -> dict[str, tuple[int, Decimal]]:
    out: dict[str, tuple[int, Decimal]] = {}
    for event_type, events in events_by_type.items():
        filtered = [e for e in events if _in_window(e)]
        total = Decimal("0")
        for e in filtered:
            buf: list[Decimal] = []
            _walk_amounts(e, buf)
            total += sum(buf, Decimal("0"))
        if filtered or events:
            out[event_type] = (len(filtered), total)
    return out


async def main() -> int:
    print(f"Window: {WINDOW_START.date()} .. {WINDOW_END.date()} UTC (Jan 2026)")

    async with AmazonSPConnector(region="NA") as conn:
        # ------- BY-DATE -------
        # PostedBefore must be at least 2 minutes in the past — Feb 1 is fine.
        print(f"\n[by-date] GET /finances/v0/financialEvents PostedAfter={WINDOW_START.date()} PostedBefore={WINDOW_END.date()}")
        by_date = await conn.get_financial_events_by_date(
            posted_after=_iso_z(WINDOW_START),
            posted_before=_iso_z(WINDOW_END),
        )
        by_date_summary = _summarize(by_date)
        print(f"  event types returned: {len(by_date_summary)}")

        # ------- BY-GROUP -------
        # List all groups whose start could produce events in Jan 2026. Groups
        # started as far back as mid-Dec 2025 can hold Jan events.
        list_start = datetime(2025, 12, 1, tzinfo=timezone.utc)
        print(f"\n[by-group] listing financialEventGroups started after {list_start.date()}")
        groups = await conn.get_financial_event_groups(start_date=_iso_z(list_start))
        closed_jan = []
        for g in groups:
            if g.get("ProcessingStatus") != "Closed":
                continue
            gs = _parse_dt(g.get("FinancialEventGroupStart"))
            ge = _parse_dt(g.get("FinancialEventGroupEnd"))
            if gs is None or ge is None:
                continue
            if ge < WINDOW_START or gs >= WINDOW_END:
                continue
            closed_jan.append(g)
        print(f"  {len(closed_jan)} Closed groups overlap Jan 2026")

        merged_by_group: dict[str, list[dict]] = defaultdict(list)
        for idx, g in enumerate(closed_jan, 1):
            gid = g["FinancialEventGroupId"]
            print(
                f"  [{idx}/{len(closed_jan)}] {gid[:32]}... "
                f"{g.get('FinancialEventGroupStart')} -> {g.get('FinancialEventGroupEnd')}"
            )
            events = await conn.get_financial_events(gid)
            for k, v in events.items():
                merged_by_group[k].extend(v)

        by_group_summary = _summarize(dict(merged_by_group))
        print(f"  event types returned across all Jan-overlapping groups: {len(by_group_summary)}")

    # ------- DIFF -------
    all_types = sorted(set(by_date_summary) | set(by_group_summary))
    print(f"\n{'event_type':45}  {'by-date cnt':>12}  {'by-date USD':>15}  "
          f"{'by-group cnt':>12}  {'by-group USD':>15}  {'delta USD':>15}")
    print("-" * 130)

    date_total = Decimal("0")
    group_total = Decimal("0")
    for et in all_types:
        dc, dt = by_date_summary.get(et, (0, Decimal("0")))
        gc, gt = by_group_summary.get(et, (0, Decimal("0")))
        delta = gt - dt
        print(f"{et:45}  {dc:>12}  {float(dt):>15,.2f}  "
              f"{gc:>12}  {float(gt):>15,.2f}  {float(delta):>15,.2f}")
        date_total += dt
        group_total += gt

    print("-" * 130)
    print(f"{'TOTAL':45}  {'':>12}  {float(date_total):>15,.2f}  "
          f"{'':>12}  {float(group_total):>15,.2f}  {float(group_total - date_total):>15,.2f}")

    only_in_group = sorted(set(by_group_summary) - set(by_date_summary))
    only_in_date = sorted(set(by_date_summary) - set(by_group_summary))
    print(f"\nEvent types present ONLY in by-group ({len(only_in_group)}):")
    for et in only_in_group:
        print(f"  {et}: count={by_group_summary[et][0]}, sum=${float(by_group_summary[et][1]):,.2f}")
    print(f"\nEvent types present ONLY in by-date ({len(only_in_date)}):")
    for et in only_in_date:
        print(f"  {et}: count={by_date_summary[et][0]}, sum=${float(by_date_summary[et][1]):,.2f}")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
