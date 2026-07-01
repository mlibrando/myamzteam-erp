"""Amazon SP-API settlement report ingestion — Taxes remitted to Amazon only.

Scope (intentionally narrow):
- Only Taxes-remitted-to-Amazon line items are ingested. Every other row
  in the settlement report is already covered by financial_events via
  the daily by-date ETL and the monthly by-group reconciliation.
- Taxes remitted rows land in financial_events with
  source='settlement', category='selling_fees'.

Why this matters — Elena's manual P&L books "Taxes (tax remitted to
Amazon)" as Selling Fees. It represents sales tax that Amazon collects
from the buyer under Marketplace Facilitator laws and remits directly
to the taxing jurisdiction; the money never hits the seller's account.
Because SP-API's financialEvents endpoint doesn't surface it as its own
line item (the tax appears bundled inside ShipmentEvent.ItemChargeList
under ChargeType='Tax' which we map to Sales, mirroring Elena's
"gross-of-tax" Sales bookkeeping), our Selling Fees total was ~$10,978
short vs Elena's Jan 2026 US number.

The settlement report exposes each tax remittance as a discrete row
with amount-description containing 'MarketplaceFacilitator' or 'Tax'
and a non-null amount. This ETL parses only those rows.

Flow:
1. Call SP-API Reports API for reportType
   GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2 with the target
   marketplace + PT-local date window (widened to UTC).
2. For each DONE report, fetch its reportDocument (a pre-signed S3 URL),
   download, decompress if GZIP, parse the tab-separated body.
3. Filter to (posted-date-time in the target UTC window) AND
   (amount-description looks like a Taxes-remitted line).
4. DELETE prior source='settlement' rows in the window + INSERT the new
   ones — same delete-and-insert pattern the by-group reconciliation
   uses so re-runs stay idempotent.

Settlement rows are the highest-priority source and are never touched
by the by-date or by-group ETLs — they only DELETE their own source.
"""

from __future__ import annotations

import csv
import gzip
import io
import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import and_, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.amazon_sp import (
    AmazonSPConnector,
    MARKETPLACE_CHANNEL,
    MARKETPLACE_CURRENCY,
    MARKETPLACE_REGION,
    Region,
)
from app.etl.amazon_etl import SOURCE_SETTLEMENT
from app.etl.pnl_mapping import PnlCategory, normalize_amount
from app.etl.timezone_utils import date_range_utc
from app.models import FinancialEvent, RawApiLog

logger = logging.getLogger(__name__)

SETTLEMENT_REPORT_TYPE = "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2"

# Amount descriptions in the settlement report that represent tax Amazon
# collected and remitted directly to the taxing jurisdiction (not the
# seller). All match forms are matched case-insensitively as prefixes
# so future Amazon-added variants (e.g. 'MarketplaceFacilitatorTax-Other')
# are captured automatically.
TAXES_REMITTED_PREFIXES: tuple[str, ...] = (
    "marketplacefacilitatortax",
    "marketplacefacilitatorvat",
    # Some regions emit these as top-level 'TaxWithheld' rows.
    "taxwithheld",
)

# Column names in the settlement flat file. Documented at
# https://developer-docs.amazon.com/sp-api/docs/report-type-values-settlement
_COL_SETTLEMENT_ID = "settlement-id"
_COL_TRANSACTION_TYPE = "transaction-type"
_COL_AMOUNT_TYPE = "amount-type"
_COL_AMOUNT_DESCRIPTION = "amount-description"
_COL_AMOUNT = "amount"
_COL_POSTED_DATE_TIME = "posted-date-time"
_COL_ORDER_ID = "order-id"
_COL_SKU = "sku"
_COL_MARKETPLACE_NAME = "marketplace-name"
_COL_CURRENCY = "currency"


@dataclass
class SettlementEtlSummary:
    marketplace_ids: list[str] = field(default_factory=list)
    window_start_pt: date | None = None
    window_end_pt: date | None = None
    reports_listed: int = 0
    reports_processed: int = 0
    reports_skipped_as_duplicate: int = 0
    rows_scanned: int = 0
    tax_rows_matched: int = 0
    rows_deleted: int = 0
    rows_inserted: int = 0
    by_marketplace: dict[str, int] = field(default_factory=dict)
    by_amount_description: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "marketplace_ids": list(self.marketplace_ids),
            "window_start_pt": self.window_start_pt.isoformat() if self.window_start_pt else None,
            "window_end_pt": self.window_end_pt.isoformat() if self.window_end_pt else None,
            "reports_listed": self.reports_listed,
            "reports_processed": self.reports_processed,
            "reports_skipped_as_duplicate": self.reports_skipped_as_duplicate,
            "rows_scanned": self.rows_scanned,
            "tax_rows_matched": self.tax_rows_matched,
            "rows_deleted": self.rows_deleted,
            "rows_inserted": self.rows_inserted,
            "by_marketplace": dict(self.by_marketplace),
            "by_amount_description": dict(self.by_amount_description),
        }


def _is_taxes_remitted(amount_description: str) -> bool:
    key = amount_description.replace("-", "").replace("_", "").replace(" ", "").lower()
    return any(key.startswith(prefix) for prefix in TAXES_REMITTED_PREFIXES)


def _parse_report_datetime(raw: str) -> datetime | None:
    """Settlement reports use `2026-01-15 12:34:56 UTC` or the ISO variant."""
    if not raw:
        return None
    raw = raw.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S UTC", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S+00:00"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _decompress(payload: bytes, algorithm: str | None) -> bytes:
    if algorithm and algorithm.upper() == "GZIP":
        return gzip.decompress(payload)
    return payload


def parse_settlement_report(raw_bytes: bytes, *, compression: str | None = None) -> list[dict[str, str]]:
    """Decompress + TSV-parse the settlement flat file.

    Returns each detail row as a dict keyed by column name. The header row
    (first non-empty line) provides the keys; subsequent lines are dicts
    of column->value. Empty rows are skipped."""
    body = _decompress(raw_bytes, compression)
    text = body.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    return [row for row in reader if any(v.strip() for v in row.values() if v is not None)]


def _first_settlement_id(rows: list[dict[str, str]]) -> str | None:
    """Return the first non-empty settlement-id value found in the rows.

    Every row in a settlement report shares the same settlement-id (they
    all belong to one disbursement cycle), so peeking at the first
    populated one is sufficient to identify the report."""
    for row in rows:
        sid = (row.get(_COL_SETTLEMENT_ID) or "").strip()
        if sid:
            return sid
    return None


async def _purge_settlement_rows(
    session: AsyncSession,
    *,
    window_start: datetime,
    window_end: datetime,
    marketplace_ids: Iterable[str],
) -> int:
    """Delete prior source='settlement' rows in the widened UTC window so
    re-running the ETL is idempotent. by-date + by-group rows are never
    touched by this DELETE (source filter)."""
    mp_list = list(marketplace_ids)
    result = await session.execute(
        delete(FinancialEvent).where(
            and_(
                FinancialEvent.marketplace_id.in_(mp_list),
                FinancialEvent.posted_date >= window_start,
                FinancialEvent.posted_date < window_end,
                FinancialEvent.source == SOURCE_SETTLEMENT,
            )
        )
    )
    return result.rowcount or 0


def _tax_row_to_flat(
    row: dict[str, str],
    *,
    marketplace_id: str,
    default_posted: datetime,
) -> FinancialEvent | None:
    """Build a FinancialEvent row for a Taxes-remitted settlement line.

    Returns None if the row is malformed (missing amount)."""
    raw_amount_str = (row.get(_COL_AMOUNT) or "").strip()
    if not raw_amount_str:
        return None
    try:
        raw_amount = Decimal(raw_amount_str)
    except Exception:
        return None
    if raw_amount == 0:
        return None
    posted_dt = _parse_report_datetime(row.get(_COL_POSTED_DATE_TIME) or "")
    posted_dt = posted_dt or default_posted
    amount_description = (row.get(_COL_AMOUNT_DESCRIPTION) or "").strip() or "TaxesRemittedToAmazon"
    order_id = (row.get(_COL_ORDER_ID) or "").strip() or None
    sku = (row.get(_COL_SKU) or "").strip() or None
    currency = (row.get(_COL_CURRENCY) or "").strip() or None
    # Elena's mapping: Taxes remitted -> Selling Fees. Sign convention:
    # settlement 'amount' is negative when Amazon deducts from disbursement
    # (which is the case for Taxes remitted — Amazon collected it, keeps
    # it, remits to state). normalize_amount inverts negatives so it
    # stores as a positive fee_amount (outflow), which the P&L formula
    # subtracts.
    fee_amount = normalize_amount(PnlCategory.SELLING_FEES, float(raw_amount))
    return FinancialEvent(
        event_type="SettlementTaxRemitted",
        posted_date=posted_dt,
        marketplace_id=marketplace_id,
        order_id=order_id,
        asin=None,
        sku=sku,
        fee_type=amount_description,
        category=PnlCategory.SELLING_FEES.value,
        fee_amount=Decimal(str(fee_amount)),
        raw_amount=raw_amount,
        quantity=None,
        currency=currency,
        source=SOURCE_SETTLEMENT,
        raw_payload=row,
    )


async def run_amazon_settlement_ingestion(
    session: AsyncSession,
    *,
    window_start_pt: date,
    window_end_pt: date,
    marketplace_ids: Iterable[str] | None = None,
    connector_factory: Any = None,
) -> SettlementEtlSummary:
    """Ingest Taxes-remitted-to-Amazon lines from settlement reports.

    Idempotent per (marketplace, PT-local window). Re-running purges
    prior source='settlement' rows in the window and re-inserts. Never
    touches source='financial_events_by_date' or 'financial_events_by_group'."""
    marketplaces = tuple(marketplace_ids) if marketplace_ids else tuple(MARKETPLACE_REGION.keys())
    unknown = [m for m in marketplaces if m not in MARKETPLACE_REGION]
    if unknown:
        raise ValueError(f"unknown marketplace_ids: {unknown}")

    summary = SettlementEtlSummary(
        marketplace_ids=list(marketplaces),
        window_start_pt=window_start_pt,
        window_end_pt=window_end_pt,
    )

    # UTC window for both listing settlement reports and filtering
    # posted-date-time on tax rows. Include one day past window_end_pt so
    # PT-window_end_pt evening events (posted before Feb 1 08:00 UTC) are
    # captured.
    utc_start, utc_end = date_range_utc(window_start_pt, window_end_pt + timedelta(days=1))
    # Settlement reports are created shortly after the settlement group's
    # end (delivery lag ~1-2 days). A report covering the last few days
    # of the target window may not exist until ~2 weeks later, so we
    # widen createdUntil by 15 days. For createdSince: Amazon's Reports
    # API rejects any value more than 90 days old (RequestedFromDate
    # >90 days -> 400 InvalidInput), so cap the earliest we ask for at
    # 89 days back from now. If the target window is entirely outside
    # the 90-day-back cutoff, list_reports will return nothing and the
    # ETL is a no-op — that's expected. Callers should run this ETL
    # monthly (or more often) so it always reaches back to CURRENT
    # settlement reports that are inside the window.
    now = datetime.now(timezone.utc)
    earliest_allowed = now - timedelta(days=89)
    desired_created_since = utc_start - timedelta(days=30)
    created_since_dt = max(desired_created_since, earliest_allowed)
    created_since = created_since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    created_until = (utc_end + timedelta(days=15)).strftime("%Y-%m-%dT%H:%M:%SZ")

    deleted = await _purge_settlement_rows(
        session,
        window_start=utc_start,
        window_end=utc_end,
        marketplace_ids=marketplaces,
    )
    summary.rows_deleted = deleted

    regions_needed: dict[Region, list[str]] = {}
    for mp in marketplaces:
        region = MARKETPLACE_REGION[mp]
        regions_needed.setdefault(region, []).append(mp)

    factory = connector_factory or (lambda region: AmazonSPConnector(region=region))

    for region, region_marketplaces in regions_needed.items():
        region_marketplaces_set = set(region_marketplaces)
        # Amazon may return multiple report VERSIONS (different reportId,
        # different reportDocumentId) for the same underlying SETTLEMENT
        # (same settlement-id) — presumably initial + revised copies of
        # the same biweekly disbursement. Track processed settlement-ids
        # so we don't insert the same disbursement's rows twice when
        # Amazon does emit revisions.
        seen_settlement_ids: set[str] = set()
        # And separately: within a single run (across all reports we do
        # process), track individual row identities so a row that appears
        # in overlapping reports — e.g. a boundary transaction that sits
        # in both settlement A's tail and settlement B's head — lands
        # exactly once. Key on the tuple that uniquely identifies an
        # Amazon financial event: (settlement-id, transaction-type,
        # amount-type, amount-description, amount, posted-date-time,
        # order-id). Empirically closes the last few $ of drift versus
        # Elena's aggregated Sellerise "Taxes" line for May 2026 US.
        seen_row_keys: set[tuple[str, ...]] = set()
        async with factory(region) as conn:
            logger.info(
                "settlement_ingestion region=%s marketplaces=%s window=%s..%s "
                "createdSince=%s createdUntil=%s",
                region,
                region_marketplaces,
                window_start_pt,
                window_end_pt,
                created_since,
                created_until,
            )
            reports = await conn.list_reports(
                report_types=[SETTLEMENT_REPORT_TYPE],
                marketplace_ids=list(region_marketplaces),
                created_since=created_since,
                created_until=created_until,
                processing_statuses=["DONE"],
            )
            summary.reports_listed += len(reports)

            for report in reports:
                doc_id = report.get("reportDocumentId")
                if not doc_id:
                    continue
                logger.info(
                    "settlement_ingestion report_id=%s data_start=%s data_end=%s",
                    report.get("reportId"),
                    report.get("dataStartTime"),
                    report.get("dataEndTime"),
                )
                document = await conn.get_report_document(doc_id)
                raw_bytes = await conn.download_report_document(document)
                rows = parse_settlement_report(
                    raw_bytes,
                    compression=document.get("compressionAlgorithm"),
                )
                # Peek at the first non-empty settlement-id in the file
                # to check whether we've already ingested this settlement.
                report_settlement_id = _first_settlement_id(rows)
                if report_settlement_id and report_settlement_id in seen_settlement_ids:
                    logger.info(
                        "settlement_ingestion skipping duplicate settlement-id=%s "
                        "(reportId=%s already covered by an earlier report version)",
                        report_settlement_id,
                        report.get("reportId"),
                    )
                    summary.reports_skipped_as_duplicate += 1
                    continue
                if report_settlement_id:
                    seen_settlement_ids.add(report_settlement_id)
                summary.reports_processed += 1
                summary.rows_scanned += len(rows)

                # Log the raw report metadata (not the full body — that can
                # be MBs) so we have a trail of which reports fed this ETL.
                session.add(
                    RawApiLog(
                        source="amazon_sp",
                        endpoint=f"/reports/2021-06-30/documents/{doc_id}",
                        request_params={
                            "region": region,
                            "reportType": SETTLEMENT_REPORT_TYPE,
                            "reportId": report.get("reportId"),
                            "dataStartTime": report.get("dataStartTime"),
                            "dataEndTime": report.get("dataEndTime"),
                        },
                        response_status=200,
                        response_body={
                            "reportDocumentId": doc_id,
                            "row_count": len(rows),
                            "compression": document.get("compressionAlgorithm"),
                        },
                    )
                )

                # Region primary marketplace for rows without an explicit
                # marketplace-name column (some formats include it, some don't).
                fallback_marketplace = region_marketplaces[0] if region_marketplaces else None
                default_posted = _parse_report_datetime(
                    report.get("dataEndTime") or ""
                ) or utc_start

                for row in rows:
                    amount_description = (row.get(_COL_AMOUNT_DESCRIPTION) or "").strip()
                    if not _is_taxes_remitted(amount_description):
                        continue
                    posted_dt = _parse_report_datetime(
                        row.get(_COL_POSTED_DATE_TIME) or ""
                    )
                    row_datetime = posted_dt or default_posted
                    if row_datetime < utc_start or row_datetime >= utc_end:
                        continue
                    marketplace_id = fallback_marketplace
                    if marketplace_id not in region_marketplaces_set:
                        continue

                    # Row-level dedup — a small number of tax rows repeat
                    # across overlapping Amazon reports even after the
                    # settlement-id dedup above. Skip a row identity we
                    # already ingested this run.
                    row_key = (
                        (row.get(_COL_SETTLEMENT_ID) or "").strip(),
                        (row.get(_COL_TRANSACTION_TYPE) or "").strip(),
                        (row.get(_COL_AMOUNT_TYPE) or "").strip(),
                        amount_description,
                        (row.get(_COL_AMOUNT) or "").strip(),
                        (row.get(_COL_POSTED_DATE_TIME) or "").strip(),
                        (row.get(_COL_ORDER_ID) or "").strip(),
                    )
                    if row_key in seen_row_keys:
                        continue
                    seen_row_keys.add(row_key)

                    flat = _tax_row_to_flat(
                        row,
                        marketplace_id=marketplace_id,
                        default_posted=default_posted,
                    )
                    if flat is None:
                        continue
                    summary.tax_rows_matched += 1
                    summary.rows_inserted += 1
                    summary.by_marketplace[marketplace_id] = (
                        summary.by_marketplace.get(marketplace_id, 0) + 1
                    )
                    summary.by_amount_description[amount_description] = (
                        summary.by_amount_description.get(amount_description, 0) + 1
                    )
                    session.add(flat)

    await session.flush()
    return summary


__all__ = [
    "SETTLEMENT_REPORT_TYPE",
    "SettlementEtlSummary",
    "TAXES_REMITTED_PREFIXES",
    "parse_settlement_report",
    "run_amazon_settlement_ingestion",
]
