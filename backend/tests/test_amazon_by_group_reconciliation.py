"""Tests for the by-group reconciliation ETL.

Focus areas:
- Group filtering: Open groups skipped, out-of-window groups skipped,
  Closed groups whose windows overlap are processed.
- URL correctness: fetches use `/financialEventGroups/{id}/financialEvents`
  (not the misleading old path that returned 403).
- Delete-and-insert pattern: DELETE only touches (source='financial_events_by_date'),
  never rows from other sources. Reruns purge by-group rows too so
  idempotency holds.
- Summary counts: groups_listed / groups_closed_in_window /
  groups_processed / events_pulled reflect what actually happened.

DB persistence is mocked via a MockSession that records add() + execute()
calls; the live delete/insert is exercised by the validation script,
matching the existing test pattern (see test_amazon_etl.py docstring)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

import httpx
import respx

from app.connectors.amazon_sp import (
    LWA_TOKEN_URL,
    REGION_BASE_URLS,
    AmazonSPConnector,
)
from app.etl.amazon_etl import (
    SOURCE_BY_DATE,
    SOURCE_BY_GROUP,
    run_amazon_by_group_reconciliation,
)
from app.models import FinancialEvent


CREDS = dict(
    client_id="amzn1.application-oa2-client.test",
    client_secret="test-secret",
    refresh_token="Atzr|test-refresh-token",
)


def _token_route(mock: respx.Router, access_token: str = "Atza|test-access") -> respx.Route:
    return mock.post(LWA_TOKEN_URL).mock(
        return_value=httpx.Response(
            200,
            json={"access_token": access_token, "token_type": "bearer", "expires_in": 3600},
        )
    )


@dataclass
class _DeleteResult:
    rowcount: int


@dataclass
class MockSession:
    """Just enough AsyncSession surface for run_amazon_by_group_reconciliation.

    Captures every add() and execute() call so tests can assert on them.
    delete() statements report a configurable rowcount; add() calls collect
    the model instances so we can inspect .source, .fee_type, etc.
    """

    added_events: list[FinancialEvent] = field(default_factory=list)
    added_other: list[Any] = field(default_factory=list)
    delete_calls: list[tuple[str, int]] = field(default_factory=list)
    delete_rowcount_default: int = 0
    flushes: int = 0

    async def execute(self, statement: Any) -> _DeleteResult:
        # We only need to distinguish DELETE-from-FinancialEvent-with-source
        # from any other execute() (e.g. RawApiLog inserts don't go through
        # execute in this ETL). Report the label + rowcount so tests can
        # assert the DELETE(source=by_date) was issued.
        label = _describe_delete(statement)
        self.delete_calls.append((label, self.delete_rowcount_default))
        return _DeleteResult(rowcount=self.delete_rowcount_default)

    def add(self, instance: Any) -> None:
        if isinstance(instance, FinancialEvent):
            self.added_events.append(instance)
        else:
            self.added_other.append(instance)

    async def flush(self) -> None:
        self.flushes += 1


def _describe_delete(stmt: Any) -> str:
    compiled = str(stmt.compile(compile_kwargs={"literal_binds": False}))
    if "financial_events" in compiled and "source" in compiled:
        return "delete_financial_events_by_source"
    return f"other:{compiled[:60]}"


def _shipment_event(order_id: str, principal: float = 100.0, sku: str = "SKU-X") -> dict:
    return {
        "AmazonOrderId": order_id,
        "MarketplaceName": "Amazon.com",
        "PostedDate": "2026-01-15T18:00:00Z",
        "ShipmentItemList": [
            {
                "SellerSKU": sku,
                "OrderItemId": f"oitem-{order_id}",
                "QuantityShipped": 1,
                "ItemChargeList": [
                    {
                        "ChargeType": "Principal",
                        "ChargeAmount": {"CurrencyCode": "USD", "CurrencyAmount": principal},
                    }
                ],
                "ItemFeeList": [
                    {
                        "FeeType": "Commission",
                        "FeeAmount": {"CurrencyCode": "USD", "CurrencyAmount": -15.0},
                    }
                ],
            }
        ],
    }


async def test_reconciliation_skips_open_groups_and_out_of_window_closed() -> None:
    """Only Closed groups whose (start,end) overlap the target UTC window
    get their events fetched. Open groups and non-overlapping Closed groups
    are skipped."""
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=False) as mock:
        _token_route(mock)
        # 1 Open group (skipped), 1 Closed pre-window (skipped),
        # 1 Closed in-window (fetched), 1 Closed post-window (skipped).
        groups_route = mock.get(f"{base}/finances/v0/financialEventGroups").mock(
            return_value=httpx.Response(
                200,
                json={
                    "payload": {
                        "FinancialEventGroupList": [
                            {
                                "FinancialEventGroupId": "g-open",
                                "ProcessingStatus": "Open",
                                "FinancialEventGroupStart": "2026-01-10T00:00:00Z",
                                "FinancialEventGroupEnd": None,
                            },
                            {
                                "FinancialEventGroupId": "g-pre",
                                "ProcessingStatus": "Closed",
                                "FinancialEventGroupStart": "2025-11-01T00:00:00Z",
                                "FinancialEventGroupEnd": "2025-11-14T00:00:00Z",
                            },
                            {
                                "FinancialEventGroupId": "g-in",
                                "ProcessingStatus": "Closed",
                                "FinancialEventGroupStart": "2026-01-10T00:00:00Z",
                                "FinancialEventGroupEnd": "2026-01-24T00:00:00Z",
                            },
                            {
                                "FinancialEventGroupId": "g-post",
                                "ProcessingStatus": "Closed",
                                "FinancialEventGroupStart": "2026-03-01T00:00:00Z",
                                "FinancialEventGroupEnd": "2026-03-14T00:00:00Z",
                            },
                        ],
                    }
                },
            )
        )
        # Only g-in should be fetched, and via the corrected URL.
        in_route = mock.get(
            f"{base}/finances/v0/financialEventGroups/g-in/financialEvents"
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "payload": {
                        "FinancialEvents": {
                            "ShipmentEventList": [_shipment_event("111-A")],
                        }
                    }
                },
            )
        )

        session = MockSession()
        summary = await run_amazon_by_group_reconciliation(
            session,  # type: ignore[arg-type]
            window_start_pt=date(2026, 1, 1),
            window_end_pt=date(2026, 1, 31),
            marketplace_ids=["ATVPDKIKX0DER"],
            connector_factory=lambda region: AmazonSPConnector(region=region, **CREDS),
        )

        assert groups_route.call_count == 1
        assert in_route.call_count == 1
        assert summary.groups_listed == 4
        assert summary.groups_closed_in_window == 1
        assert summary.groups_processed == 1
        assert summary.events_pulled == 1  # one ShipmentEvent
        # ShipmentEvent produces 2 line items (Principal charge + Commission fee)
        assert summary.line_items_total == 2
        assert summary.line_items_mapped == 2
        assert summary.by_category["sales"] == 1
        assert summary.by_category["selling_fees"] == 1


async def test_reconciliation_writes_source_by_group_and_deletes_by_date() -> None:
    """Every FinancialEvent added by the reconciliation carries
    source='financial_events_by_group'. The reconciliation issues a DELETE
    with source='financial_events_by_date' before inserting (and another
    with source='financial_events_by_group' for idempotency)."""
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=False) as mock:
        _token_route(mock)
        mock.get(f"{base}/finances/v0/financialEventGroups").mock(
            return_value=httpx.Response(
                200,
                json={
                    "payload": {
                        "FinancialEventGroupList": [
                            {
                                "FinancialEventGroupId": "g-in",
                                "ProcessingStatus": "Closed",
                                "FinancialEventGroupStart": "2026-01-10T00:00:00Z",
                                "FinancialEventGroupEnd": "2026-01-24T00:00:00Z",
                            }
                        ]
                    }
                },
            )
        )
        mock.get(
            f"{base}/finances/v0/financialEventGroups/g-in/financialEvents"
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "payload": {
                        "FinancialEvents": {
                            "ShipmentEventList": [_shipment_event("111-A", principal=250.0)]
                        }
                    }
                },
            )
        )
        # Report 5 by-date rows deleted so we can verify the summary picks
        # up the rowcount.
        session = MockSession(delete_rowcount_default=5)
        summary = await run_amazon_by_group_reconciliation(
            session,  # type: ignore[arg-type]
            window_start_pt=date(2026, 1, 1),
            window_end_pt=date(2026, 1, 31),
            marketplace_ids=["ATVPDKIKX0DER"],
            connector_factory=lambda region: AmazonSPConnector(region=region, **CREDS),
        )

        # Two DELETE statements: by-date (purge original) + by-group (idempotency).
        by_source_deletes = [c for c in session.delete_calls if c[0] == "delete_financial_events_by_source"]
        assert len(by_source_deletes) == 2
        assert summary.rows_deleted_by_date == 5
        # Every added FinancialEvent carries source=by_group.
        assert len(session.added_events) == 2  # Principal + Commission
        for ev in session.added_events:
            assert ev.source == SOURCE_BY_GROUP


async def test_reconciliation_pages_group_events_via_corrected_url() -> None:
    """Multi-page group event responses paginate through the corrected path;
    the misleading old path (/financialEvents/{id}) is never called."""
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=False) as mock:
        _token_route(mock)
        mock.get(f"{base}/finances/v0/financialEventGroups").mock(
            return_value=httpx.Response(
                200,
                json={
                    "payload": {
                        "FinancialEventGroupList": [
                            {
                                "FinancialEventGroupId": "g-in",
                                "ProcessingStatus": "Closed",
                                "FinancialEventGroupStart": "2026-01-10T00:00:00Z",
                                "FinancialEventGroupEnd": "2026-01-24T00:00:00Z",
                            }
                        ]
                    }
                },
            )
        )
        wrong_url = mock.get(f"{base}/finances/v0/financialEvents/g-in").mock(
            return_value=httpx.Response(200, json={"payload": {}})
        )
        events_route = mock.get(
            f"{base}/finances/v0/financialEventGroups/g-in/financialEvents"
        ).mock(
            side_effect=[
                httpx.Response(
                    200,
                    json={
                        "payload": {
                            "FinancialEvents": {"ShipmentEventList": [_shipment_event("111-A")]},
                            "NextToken": "page-2",
                        }
                    },
                ),
                httpx.Response(
                    200,
                    json={
                        "payload": {
                            "FinancialEvents": {"ShipmentEventList": [_shipment_event("111-B")]},
                        }
                    },
                ),
            ]
        )

        session = MockSession()
        summary = await run_amazon_by_group_reconciliation(
            session,  # type: ignore[arg-type]
            window_start_pt=date(2026, 1, 1),
            window_end_pt=date(2026, 1, 31),
            marketplace_ids=["ATVPDKIKX0DER"],
            connector_factory=lambda region: AmazonSPConnector(region=region, **CREDS),
        )

        assert events_route.call_count == 2
        assert wrong_url.call_count == 0  # the misleading old path is never hit
        assert summary.events_pulled == 2
