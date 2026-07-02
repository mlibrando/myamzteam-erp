"""Tests for the settlement report ingestion (Taxes remitted only).

Focus:
- TSV parser handles the raw settlement flat-file format (with headers
  and empty rows). Both plain and GZIP-compressed bodies work.
- _is_taxes_remitted matches the expected amount-description strings
  (MarketplaceFacilitatorTax, MarketplaceFacilitatorVAT, TaxWithheld)
  case- and separator-insensitively; other descriptions are excluded.
- run_amazon_settlement_ingestion:
  * Lists reports, downloads documents, parses rows.
  * Only Taxes-remitted rows land in financial_events; everything else
    is scanned but skipped.
  * Rows outside the widened UTC window are excluded.
  * DELETE issued for source='settlement' only — never for by-date/by-group.
  * Every added row carries source='settlement', category='selling_fees'.
"""

from __future__ import annotations

import gzip
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import httpx
import respx

from app.connectors.amazon_sp import (
    LWA_TOKEN_URL,
    REGION_BASE_URLS,
    AmazonSPConnector,
)
from app.etl.amazon_etl import SOURCE_SETTLEMENT
from app.etl.amazon_settlement_etl import (
    SETTLEMENT_REPORT_TYPE,
    _is_taxes_remitted,
    parse_settlement_report,
    run_amazon_settlement_ingestion,
)
from app.etl.pnl_mapping import PnlCategory
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


# -----------------------------------------------------------------------------
# _is_taxes_remitted
# -----------------------------------------------------------------------------


def test_is_taxes_remitted_matches_marketplace_facilitator() -> None:
    assert _is_taxes_remitted("MarketplaceFacilitatorTax-Principal") is True
    assert _is_taxes_remitted("MarketplaceFacilitatorTax-Shipping") is True
    assert _is_taxes_remitted("marketplacefacilitatorvat-principal") is True
    assert _is_taxes_remitted("Marketplace_Facilitator_Tax - Other") is True


def test_is_taxes_remitted_matches_tax_withheld() -> None:
    assert _is_taxes_remitted("TaxWithheld") is True
    assert _is_taxes_remitted("tax_withheld") is True


def test_is_taxes_remitted_rejects_other_descriptions() -> None:
    assert _is_taxes_remitted("Principal") is False
    assert _is_taxes_remitted("Commission") is False
    assert _is_taxes_remitted("FBAPerUnitFulfillmentFee") is False
    # Tax on the seller's side (they collected it) is NOT the same as tax
    # Amazon remitted on their behalf. Bare "Tax" is a shipment charge
    # already booked to Sales; must not be re-booked here.
    assert _is_taxes_remitted("Tax") is False


# -----------------------------------------------------------------------------
# TSV parser
# -----------------------------------------------------------------------------


_SAMPLE_TSV = (
    "settlement-id\tsettlement-start-date\tsettlement-end-date\tdeposit-date\t"
    "total-amount\tcurrency\ttransaction-type\torder-id\tmerchant-order-id\t"
    "adjustment-id\tshipment-id\tmarketplace-name\tamount-type\t"
    "amount-description\tamount\tsku\tposted-date-time\n"
    "12345\t2026-01-01T00:00:00Z\t2026-01-14T00:00:00Z\t2026-01-15T00:00:00Z\t"
    "9999.99\tUSD\t\t\t\t\t\tamazon.com\t\t\t\t\t\n"  # header/summary row
    "12345\t\t\t\t\tUSD\tOrder\t111-2222-33\t\t\t\tamazon.com\tItemPrice\t"
    "Principal\t100.00\tSKU-A\t2026-01-05 12:34:56 UTC\n"
    "12345\t\t\t\t\tUSD\tOrder\t111-2222-33\t\t\t\tamazon.com\tItemFees\t"
    "MarketplaceFacilitatorTax-Principal\t-8.50\tSKU-A\t2026-01-05 12:34:56 UTC\n"
    "12345\t\t\t\t\tUSD\tOrder\t111-2222-44\t\t\t\tamazon.com\tItemFees\t"
    "Commission\t-15.00\tSKU-B\t2026-01-06 10:00:00 UTC\n"
    "12345\t\t\t\t\tUSD\tOrder\t111-2222-44\t\t\t\tamazon.com\tItemFees\t"
    "MarketplaceFacilitatorTax-Shipping\t-1.20\tSKU-B\t2026-01-06 10:00:00 UTC\n"
)


def test_parse_settlement_report_plain_utf8() -> None:
    rows = parse_settlement_report(_SAMPLE_TSV.encode("utf-8"))
    # The summary row + 4 detail rows.
    assert len(rows) == 5
    # rows[0] is the summary/header row (total-amount populated, no order-id).
    # rows[1] is Principal; rows[2] is MarketplaceFacilitatorTax-Principal.
    assert rows[2]["order-id"] == "111-2222-33"
    assert rows[2]["amount-description"] == "MarketplaceFacilitatorTax-Principal"
    assert rows[2]["amount"] == "-8.50"


def test_parse_settlement_report_gzip() -> None:
    compressed = gzip.compress(_SAMPLE_TSV.encode("utf-8"))
    rows = parse_settlement_report(compressed, compression="GZIP")
    assert len(rows) == 5


# -----------------------------------------------------------------------------
# run_amazon_settlement_ingestion
# -----------------------------------------------------------------------------


@dataclass
class _DeleteResult:
    rowcount: int


@dataclass
class MockSession:
    added_events: list[FinancialEvent] = field(default_factory=list)
    added_other: list[Any] = field(default_factory=list)
    delete_calls: list[str] = field(default_factory=list)
    delete_rowcount_default: int = 0
    flushes: int = 0

    async def execute(self, statement: Any) -> _DeleteResult:
        compiled = str(statement.compile(compile_kwargs={"literal_binds": True}))
        label = (
            "delete_financial_events_settlement"
            if "financial_events" in compiled and "'settlement'" in compiled
            else f"other:{compiled[:80]}"
        )
        self.delete_calls.append(label)
        return _DeleteResult(rowcount=self.delete_rowcount_default)

    def add(self, instance: Any) -> None:
        if isinstance(instance, FinancialEvent):
            self.added_events.append(instance)
        else:
            self.added_other.append(instance)

    async def flush(self) -> None:
        self.flushes += 1


async def test_settlement_ingestion_only_inserts_taxes_remitted() -> None:
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=False) as mock:
        _token_route(mock)
        # One report matching the target window.
        mock.get(f"{base}/reports/2021-06-30/reports").mock(
            return_value=httpx.Response(
                200,
                json={
                    "reports": [
                        {
                            "reportId": "rpt-1",
                            "reportType": SETTLEMENT_REPORT_TYPE,
                            "reportDocumentId": "doc-1",
                            "dataStartTime": "2026-01-01T00:00:00Z",
                            "dataEndTime": "2026-01-14T00:00:00Z",
                            "processingStatus": "DONE",
                        }
                    ]
                },
            )
        )
        mock.get(f"{base}/reports/2021-06-30/documents/doc-1").mock(
            return_value=httpx.Response(
                200,
                json={"reportDocumentId": "doc-1", "url": "https://s3.example/doc-1"},
            )
        )
        mock.get("https://s3.example/doc-1").mock(
            return_value=httpx.Response(200, content=_SAMPLE_TSV.encode("utf-8"))
        )

        session = MockSession(delete_rowcount_default=0)
        summary = await run_amazon_settlement_ingestion(
            session,  # type: ignore[arg-type]
            window_start_pt=date(2026, 1, 1),
            window_end_pt=date(2026, 1, 31),
            marketplace_ids=["ATVPDKIKX0DER"],
            connector_factory=lambda region: AmazonSPConnector(region=region, **CREDS),
        )

        assert summary.reports_listed == 1
        assert summary.reports_processed == 1
        # 5 rows scanned, 2 matched (MarketplaceFacilitatorTax-Principal + -Shipping)
        assert summary.rows_scanned == 5
        assert summary.tax_rows_matched == 2
        assert summary.rows_inserted == 2

        # Every inserted row carries source=settlement and category=selling_fees.
        assert len(session.added_events) == 2
        for ev in session.added_events:
            assert ev.source == SOURCE_SETTLEMENT
            assert ev.category == PnlCategory.SELLING_FEES.value

        # Amounts: -8.50 (Tax-Principal) + -1.20 (Tax-Shipping) = -9.70
        # normalize_amount inverts for SELLING_FEES so fee_amount = +8.50 and +1.20
        fee_totals = sorted(float(e.fee_amount) for e in session.added_events)
        assert fee_totals == [1.20, 8.50]

        # DELETE was issued for source='settlement' before insert
        assert any(c == "delete_financial_events_settlement" for c in session.delete_calls)


async def test_settlement_ingestion_excludes_rows_outside_window() -> None:
    """Tax rows whose posted-date-time falls outside the widened UTC window
    are skipped even if the report itself is DONE and covers the window."""
    base = REGION_BASE_URLS["NA"]

    # Row posted 2025-12-15 (before window start of PT-Jan 2026) should be skipped
    early_tsv = (
        "settlement-id\ttransaction-type\tamount-type\tamount-description\t"
        "amount\tposted-date-time\tcurrency\tmarketplace-name\torder-id\tsku\n"
        "1\tOrder\tItemFees\tMarketplaceFacilitatorTax-Principal\t-5.00\t"
        "2025-12-15 08:00:00 UTC\tUSD\tamazon.com\t111-A\tSKU-A\n"
        "1\tOrder\tItemFees\tMarketplaceFacilitatorTax-Principal\t-3.00\t"
        "2026-01-10 08:00:00 UTC\tUSD\tamazon.com\t111-B\tSKU-B\n"
    )

    with respx.mock(assert_all_called=False) as mock:
        _token_route(mock)
        mock.get(f"{base}/reports/2021-06-30/reports").mock(
            return_value=httpx.Response(
                200,
                json={
                    "reports": [
                        {
                            "reportId": "rpt-1",
                            "reportType": SETTLEMENT_REPORT_TYPE,
                            "reportDocumentId": "doc-1",
                            "dataStartTime": "2025-12-01T00:00:00Z",
                            "dataEndTime": "2026-01-14T00:00:00Z",
                            "processingStatus": "DONE",
                        }
                    ]
                },
            )
        )
        mock.get(f"{base}/reports/2021-06-30/documents/doc-1").mock(
            return_value=httpx.Response(
                200,
                json={"reportDocumentId": "doc-1", "url": "https://s3.example/doc-1"},
            )
        )
        mock.get("https://s3.example/doc-1").mock(
            return_value=httpx.Response(200, content=early_tsv.encode("utf-8"))
        )

        session = MockSession()
        summary = await run_amazon_settlement_ingestion(
            session,  # type: ignore[arg-type]
            window_start_pt=date(2026, 1, 1),
            window_end_pt=date(2026, 1, 31),
            marketplace_ids=["ATVPDKIKX0DER"],
            connector_factory=lambda region: AmazonSPConnector(region=region, **CREDS),
        )

        # Only the Jan 10 row should be ingested (the Dec 15 row is outside
        # the widened UTC window Jan 1 08:00 -> Feb 1 08:00 UTC).
        assert summary.rows_inserted == 1
        assert len(session.added_events) == 1
        assert session.added_events[0].posted_date == datetime(
            2026, 1, 10, 8, 0, 0, tzinfo=timezone.utc
        )


async def test_settlement_ingestion_dedupes_reports_by_settlement_id() -> None:
    """Amazon returns multiple report versions for the same settlement
    (different reportId + reportDocumentId, same settlement-id inside).
    Only the first report seen per settlement-id should be processed;
    subsequent duplicates must be skipped, not double-inserted."""
    base = REGION_BASE_URLS["NA"]

    # Same settlement-id "SETTLE-A" produced by two "revision" reports
    # (rpt-1 + rpt-2) plus a distinct settlement "SETTLE-B" in rpt-3.
    def _tsv(settlement_id: str, amount: str) -> str:
        return (
            "settlement-id\ttransaction-type\tamount-type\tamount-description\t"
            "amount\tposted-date-time\tcurrency\tmarketplace-name\torder-id\tsku\n"
            f"{settlement_id}\tOrder\tItemFees\tMarketplaceFacilitatorTax-Principal\t"
            f"{amount}\t2026-01-10 08:00:00 UTC\tUSD\tamazon.com\t111-A\tSKU-A\n"
        )

    with respx.mock(assert_all_called=False) as mock:
        _token_route(mock)
        mock.get(f"{base}/reports/2021-06-30/reports").mock(
            return_value=httpx.Response(
                200,
                json={"reports": [
                    {"reportId": "rpt-1", "reportType": SETTLEMENT_REPORT_TYPE,
                     "reportDocumentId": "doc-1", "dataStartTime": "2026-01-01T00:00:00Z",
                     "dataEndTime": "2026-01-14T00:00:00Z", "processingStatus": "DONE"},
                    {"reportId": "rpt-2", "reportType": SETTLEMENT_REPORT_TYPE,
                     "reportDocumentId": "doc-2", "dataStartTime": "2026-01-01T00:00:00Z",
                     "dataEndTime": "2026-01-14T00:00:00Z", "processingStatus": "DONE"},
                    {"reportId": "rpt-3", "reportType": SETTLEMENT_REPORT_TYPE,
                     "reportDocumentId": "doc-3", "dataStartTime": "2026-01-15T00:00:00Z",
                     "dataEndTime": "2026-01-28T00:00:00Z", "processingStatus": "DONE"},
                ]},
            )
        )
        for doc_id in ("doc-1", "doc-2", "doc-3"):
            mock.get(f"{base}/reports/2021-06-30/documents/{doc_id}").mock(
                return_value=httpx.Response(
                    200,
                    json={"reportDocumentId": doc_id, "url": f"https://s3.example/{doc_id}"},
                )
            )
        # rpt-1 and rpt-2 both carry settlement-id "SETTLE-A" with the same
        # $10 tax row; rpt-3 carries settlement-id "SETTLE-B" with $7.
        mock.get("https://s3.example/doc-1").mock(
            return_value=httpx.Response(200, content=_tsv("SETTLE-A", "-10.00").encode("utf-8"))
        )
        mock.get("https://s3.example/doc-2").mock(
            return_value=httpx.Response(200, content=_tsv("SETTLE-A", "-10.00").encode("utf-8"))
        )
        mock.get("https://s3.example/doc-3").mock(
            return_value=httpx.Response(200, content=_tsv("SETTLE-B", "-7.00").encode("utf-8"))
        )

        session = MockSession()
        summary = await run_amazon_settlement_ingestion(
            session,  # type: ignore[arg-type]
            window_start_pt=date(2026, 1, 1),
            window_end_pt=date(2026, 1, 31),
            marketplace_ids=["ATVPDKIKX0DER"],
            connector_factory=lambda region: AmazonSPConnector(region=region, **CREDS),
        )

        assert summary.reports_listed == 3
        assert summary.reports_processed == 2  # doc-1 + doc-3 (doc-2 is a duplicate)
        assert summary.reports_skipped_as_duplicate == 1
        # Two rows inserted total (one per settlement), not three
        assert summary.rows_inserted == 2
        assert len(session.added_events) == 2
        # Amounts: SETTLE-A -$10 + SETTLE-B -$7 = -$17 raw; fee_amount magnitude $17
        fee_totals = sorted(float(e.fee_amount) for e in session.added_events)
        assert fee_totals == [7.00, 10.00]
