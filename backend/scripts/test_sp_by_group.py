"""One-shot: test SP-API by-group financial events endpoint.

Correct URL path (SP-API model):
    GET /finances/v0/financialEventGroups/{eventGroupId}/financialEvents

Prior incorrect path (returned 401/403 "Unauthorized" for malformed paths,
not 404) — misled us into thinking this required an additional data role:
    GET /finances/v0/financialEvents/{eventGroupId}   # WRONG

Run from backend/:
    .venv/bin/python -m scripts.test_sp_by_group
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from collections import Counter
from datetime import datetime, timezone

from app.connectors.amazon_sp import AmazonSPConnector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def _iso_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _count_by_type(events: dict[str, list]) -> Counter:
    return Counter({k: len(v) for k, v in events.items() if v})


async def main() -> int:
    window_start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    window_end = datetime(2026, 2, 1, tzinfo=timezone.utc)

    async with AmazonSPConnector(region="NA") as conn:
        print("[1/3] LWA token exchange (region=NA)")
        token = await conn._get_access_token()
        print(f"      access_token: {token[:12]}... (len={len(token)})")

        list_start = datetime(2025, 12, 1, tzinfo=timezone.utc)
        print(f"[2/3] list financial event groups started after {list_start.date()}")
        groups = await conn.get_financial_event_groups(start_date=_iso_z(list_start))
        closed = [g for g in groups if g.get("ProcessingStatus") == "Closed"]
        print(f"      groups returned: {len(groups)}  closed: {len(closed)}")

        # Filter to Closed groups overlapping Jan 2026
        def _parse(x):
            if not x:
                return None
            return datetime.fromisoformat(x.replace("Z", "+00:00"))

        jan_closed = []
        for g in closed:
            gs = _parse(g.get("FinancialEventGroupStart"))
            ge = _parse(g.get("FinancialEventGroupEnd"))
            if gs is None or ge is None:
                continue
            if ge < window_start or gs >= window_end:
                continue
            jan_closed.append(g)

        print(f"      Closed groups overlapping Jan 2026: {len(jan_closed)}")
        if not jan_closed:
            print("no Closed Jan 2026 groups to test against")
            return 0

        # Pick the first one and probe with the corrected path.
        target = jan_closed[0]
        gid = target["FinancialEventGroupId"]
        print(
            f"[3/3] probe corrected path against group {gid}\n"
            f"      window: {target.get('FinancialEventGroupStart')} -> "
            f"{target.get('FinancialEventGroupEnd')}"
        )

        # Bypass the connector wrapper — call raw HTTP so we can see the exact
        # status + body regardless of whether the connector raises.
        headers = await conn._auth_headers()
        url = (
            f"{conn.base_url}/finances/v0/financialEventGroups/{gid}/financialEvents"
        )
        print(f"      URL: {url}")
        resp = await conn._client.get(url, params={"MaxResultsPerPage": 100}, headers=headers)
        print(f"      status={resp.status_code}")
        if resp.status_code != 200:
            print(f"      body: {resp.text}")
            return 1

        payload = resp.json().get("payload", {})
        events = payload.get("FinancialEvents", {})
        next_token = payload.get("NextToken")
        counts = _count_by_type(events)
        print(f"      page1 event types (with count):")
        for k, v in sorted(counts.items(), key=lambda kv: -kv[1]):
            print(f"        {k}: {v}")
        print(f"      NextToken present on page 1: {bool(next_token)}")

        # Show a sample of one event from each populated list so we can eyeball
        # the shape — is it identical to what by-date returns?
        print(f"      sample events (1 per populated type):")
        for k, v in events.items():
            if v:
                print(f"        {k}[0]: {json.dumps(v[0], default=str)[:400]}")

        # Now paginate to exhaustion and get the full picture.
        print(f"      paginating to exhaustion...")
        full_events = await conn.get_financial_events(gid)
        full_counts = _count_by_type(full_events)
        print(f"      full event type counts (all pages):")
        for k, v in sorted(full_counts.items(), key=lambda kv: -kv[1]):
            print(f"        {k}: {v}")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
