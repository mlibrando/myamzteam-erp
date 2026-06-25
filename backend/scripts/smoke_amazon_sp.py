"""Smoke test: hits real SP-API with NA refresh token.

Verifies:
1. LWA token exchange
2. /sales/v1/orderMetrics for US marketplace, last 7 days
3. /finances/v0/financialEventGroups returns >=1 group
4. /finances/v0/financialEvents (date-range list) paginates the last 7 days

The by-group endpoint /finances/v0/financialEvents/{eventGroupId} requires the
"Finance and Accounting" SP-API data role and only serves Closed groups; the
list endpoint with PostedAfter/PostedBefore works with the default role and is
what the daily ETL uses.

Run from backend/ with the .env loaded:
    .venv/bin/python -m scripts.smoke_amazon_sp
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone

from app.connectors.amazon_sp import AmazonSPConnector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

US_MARKETPLACE_ID = "ATVPDKIKX0DER"


def _iso_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _iso_offset(dt: datetime) -> str:
    # Sales API requires an explicit offset (Z is not accepted).
    return dt.strftime("%Y-%m-%dT%H:%M:%S-00:00")


async def main() -> int:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    end = now.replace(hour=0, minute=0, second=0)
    start = end - timedelta(days=7)

    async with AmazonSPConnector(region="NA") as conn:
        print(f"[1/4] LWA token exchange (region=NA)")
        token = await conn._get_access_token()
        print(f"      access_token: {token[:12]}... (len={len(token)})")

        print(f"[2/4] get_order_metrics US {start.date()} -> {end.date()}")
        metrics = await conn.get_order_metrics(
            marketplace_id=US_MARKETPLACE_ID,
            start_date=_iso_offset(start),
            end_date=_iso_offset(end),
            granularity="Day",
        )
        payload = metrics.get("payload", [])
        print(f"      buckets returned: {len(payload)}")
        if payload:
            total = sum(float((b.get("totalSales") or {}).get("amount", 0)) for b in payload)
            print(f"      total sales (7d): {total:.2f}")
            print(f"      first bucket: {payload[0]}")

        print(f"[3/4] get_financial_event_groups (last 30d)")
        groups_start = now - timedelta(days=30)
        groups = await conn.get_financial_event_groups(start_date=_iso_z(groups_start))
        print(f"      groups returned: {len(groups)}")
        if not groups:
            print("      no groups returned — cannot test event pagination")
            return 0
        statuses = {g.get("ProcessingStatus") for g in groups}
        print(f"      statuses observed: {sorted(s for s in statuses if s)}")
        print(f"      first group: id={groups[0].get('FinancialEventGroupId')} status={groups[0].get('ProcessingStatus')}")

        # SP-API requires PostedBefore to be at least 2 minutes in the past.
        events_end = now - timedelta(minutes=5)
        events_start = events_end - timedelta(days=7)
        print(f"[4/4] get_financial_events_by_date {events_start.date()} -> {events_end.date()}")
        events = await conn.get_financial_events_by_date(
            posted_after=_iso_z(events_start),
            posted_before=_iso_z(events_end),
        )
        if not events:
            print("      no events returned in window")
        for key, value in events.items():
            print(f"      {key}: {len(value)} events")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
