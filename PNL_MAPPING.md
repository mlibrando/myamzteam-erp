# P&L Category Mapping (from Elena's Mapping Tab)

This is the definitive mapping of raw Amazon line items to P&L categories.
Source: Profit_and_Loss spreadsheet > Mapping tab, with all previously-unmapped
items confirmed by the client's data specialist (2026-06).

## Sales
- Product sales
- Tax
- Promotion
- Shipping charge
- Shipping tax
- Gift wrap
- Gift wrap tax
- Shipping
- Product charges
- Liquidations
- Liquidation adjustments

## COGS (Cost of Goods)
- Cost of Goods (from internal per-unit COGS table, NOT from Amazon API)
- Multiplied by quantity sold per SKU
- Requires a COGS lookup table: SKU -> unit cost

## Ad Spend
- Sponsored brands
- Sponsored display
- Sponsored videos
- Sponsored television
- Sponsored products

## Selling Fees
- Taxes (tax remitted to Amazon - this is a top-level line in raw data, rolled into Selling Fees)
- FBA fees (total)
  - Fba per unit fulfillment fee
  - Fba fees (pending)
- Referral fees (total)
  - Commission
  - Shipping chargeback
  - Giftwrap chargeback
  - Referral fee (pending)
- POA fees
  - POA service fee
  - POA per unit fulfillment fee
- Digital services fees (UK/CA VAT-related)
  - Digital services fee
  - Digital services fee FBA
  - Digital services fee adjustment

## Operational Fees
- Storage fees
- Amazon fees
- Premium services fee
- Subscription fee
- Restocking fee
- Retrocharge
- Retrocharge reversal
- Inbound transportation fee
- Inbound transportation fee - adjustment
- Fba inbound placement service fee
- Storage renewal billing
- Compensated clawback
- Disposal complete
- Removal complete
- Missing from inbound clawback
- Free replacement refund items
- Fee adjustment
- Other-transaction
- **Reversal reimbursement** (Elena's Sellerise-derived formula treats this as a negative-expense offset inside Op Fees, not as a separate Reimbursements line. Confirmed 2026-07-01 against her Jan 2026 US sheet — moving it here matched her Op Fees total exactly.)

### SP-API vocabulary crosswalk (confirmed 2026-07-01 against Elena's RAW_AMZ_US)

Elena's column labels come from Sellerise; SP-API returns different `fee_type` strings for the same underlying Amazon-side fee. Both names map to Operational Fees:

| Elena's label | SP-API `fee_type` | Notes |
|---|---|---|
| Storage fees | `FBAStorageFee` | Monthly inventory storage |
| Storage renewal billing | `FBALongTermStorageFee` (+ `StorageRenewalBilling` as alternate) | Long-term storage surcharge |
| FBA inbound placement service fee | `FBAInboundConvenienceFee` (+ `FBAInboundPlacementServiceFee` as alternate) | Inbound processing at Amazon warehouse |
| Removal complete | `FBARemovalFee` (+ `RemovalComplete` as adjustment-side alternate) | Per-unit fee for removing inventory |
| Disposal complete | `FBADisposalFee` (+ `DisposalComplete` as adjustment-side alternate) | Per-unit fee for disposing inventory |
| Premium services fee | `PaidServicesFee` (+ `PremiumServiceFee` as alternate) | Amazon Global Selling / Vendor Central paid services |
| Amazon fees | `CouponPerformanceFee` + `CouponParticipationFee` | Elena aggregates the two coupon-related fees into a single "Amazon fees" line |
| Inbound transportation fee | `FBAInboundTransportationFee` (`FeeReason=INITIAL_FEE`) | Amazon-arranged inbound freight |
| Inbound transportation fee - adjustment | `FBAInboundTransportationFee` (`FeeReason=SELLER_CAUSED_OMC`) | Post-hoc correction; same SP-API `fee_type`, different `FeeReason` — currently summed together with the initial fee in our aggregation |
| Reversal reimbursement | `AdjustmentEvent.AdjustmentType=REVERSAL_REIMBURSEMENT` | See note above; moved to Op Fees per Elena's formula |

## Refunds
- Refund - Product sales
- Refund - Tax
- Tax withheld
- Refund commission
- Refund - Promotion
- Refund - Shipping charge
- Refund - Shipping chargeback
- Refund - Shipping tax
- Goodwill

## Reimbursements from AMZ (separate positive line item)
- Missing from inbound
- Warehouse damage
- Warehouse lost
- Removal order lost

(**Reversal reimbursement is NOT in this list** — Elena's Sellerise formula categorizes it under Op Fees as a negative-expense offset. See "Operational Fees" above.)

## P&L Formula
Gross Profit (without reimbursements) = Sales - COGS - Ad Spend - Selling Fees - Operational Fees - Refunds
Gross Profit (with reimbursements) = Gross Profit (without) + Reimbursements from AMZ

Both versions are tracked. The dashboard should show both.

## Reporting Timezone (Pacific Time)

Every date in `daily_pnl.date` is a **Pacific Time (America/Los_Angeles)** local date. Elena's manual P&L uses Amazon Seller Central's default reporting cutoff, which is PT year-round (PST in winter, PDT in summer), regardless of marketplace region. A US shipment posted at 2026-01-01T04:00:00Z (Amazon SP-API returns UTC) actually posted at 2025-12-31 20:00 PT, so it belongs to December in Elena's sheet — not January. Our ETL preserves that convention.

**Storage layer stays UTC.** `financial_events.posted_date` and `ad_spend.date` are stored as UTC timestamps. The conversion happens at aggregation time in `pnl_calculator._aggregate_categories` and `_aggregate_net_units` via `(posted_date AT TIME ZONE 'America/Los_Angeles')::date`. The controlling setting is `MONTHLY_CUTOFF_TIMEZONE` in `app/config.py`.

**API windows.** SP-API is queried with UTC `PostedAfter`/`PostedBefore`. To fetch "PT-local January 2026" the caller widens the UTC window to include the PT/UTC boundary hours (e.g. Jan 1 08:00 UTC → Feb 1 08:00 UTC becomes Jan 1 00:00 UTC → Feb 2 00:00 UTC via the ETL's inclusive-date interface — PT bucketing then attributes each event to the correct local day). Use `app.etl.timezone_utils.month_window_utc(year, month)` or `date_range_utc(start_local, end_local)` for the conversion.

## Ad Spend Source of Truth

`daily_pnl.ad_spend` (and its per-product columns `ad_spend_sp` / `ad_spend_sb` / `ad_spend_sd` / `ad_spend_sv`) comes exclusively from the **Amazon Advertising Reports API** via the Ads ETL. It does NOT include the `ProductAdsPaymentEventList` line items that appear in SP-API financial events, even though those events are also ad spend from Amazon's side.

Reason: the Ads Reports API returns campaign-level daily spend with attribution and product-type tagging; SP-API's `ProductAdsPaymentEventList` returns aggregate charge/refund events without campaign detail. Elena's spreadsheet aligns with the Ads-Reports view.

For traceability, SP-API `ProductAdsPaymentEventList` events are still stored in `financial_events` with `category='ad_spend'`, but `pnl_calculator` deliberately excludes the `ad_spend` category from `daily_pnl` aggregation (`_AGG_CATEGORIES` in `pnl_calculator.py` lists only Sales / Selling Fees / Op Fees / Refunds / Reimbursements). This prevents double-counting.

If you ever wire a new event type to `daily_pnl` via `EVENT_LIST_DEFAULT_CATEGORY`, you must also add its PnlCategory to `_AGG_CATEGORIES`, or the total will be silently dropped.

## Currency Conversion Rates (used for USD normalization)
- 1 GBP = 1.32196 USD
- 1 CAD = 0.70508 USD
- 1 AUD = 0.69554 USD
- USD = 1.0 (no conversion needed)

Note: These rates should be configurable, not hardcoded. Store in a config table.

## Marketplaces
- AMZ US: USD (data from Sellerise, replacing with SP-API)
- AMZ CA: CAD (data from Sellerise, replacing with SP-API)
- AMZ UK: GBP (data from Sellerise, replacing with SP-API)
- AMZ AU: AUD (data from Amazon Payments directly)
- Retail US: USD (from Shopify)
- Retail UK: GBP (from Shopify)
- Retail AU: AUD (from Shopify)

## COGS Reference (per SKU per marketplace, from COGS_Magical_Butter.xlsx)

COGS varies by marketplace due to different product prices and shipping costs.
Same ASIN may have different SKUs across marketplaces.
Discontinued products (no COGS) should be excluded.

### US (62 active SKUs, sample of highest volume)
| Product | ASIN | SKU | Price | COGS |
|---------|------|-----|-------|------|
| MB2E | B014GNGTBK | 850251005008 | $189.00 | $48.97 |
| Gummy Maker | B0DDL84DNV | GMAKER-3 | $159.95 | $30.76 |
| MB2E, Decarbox | B0CX1XSLV7 | MBDBOX1 | $199.95 | $61.33 |
| 2mL Silicone Molds | B088TWCGKL | 83-EFO4-1SGB | $22.95 | $6.04 |
| DecarBox | B0892T1RF4 | RD-QAW6-M2FO | $49.95 | $12.36 |
| Filter Press | B09TLGP2XP | JU-L51X-ZVP2 | $48.50 | $7.30 |
(Full list: 62 products in COGS_Magical_Butter.xlsx > US tab)

### CA (12 active SKUs)
| Product | ASIN | SKU | Price (CAD) | COGS (CAD) |
|---------|------|-----|-------------|------------|
| MB2E, Decarbox | B0CX1XSLV7 | MBDBOX1 | $228.74 | $82.80 |
| MB2E | B014GNGTBK | 850251005008 | $216.22 | $66.11 |
| Gummy Maker | B0DDL84DNV | GMAKER-3 | $182.98 | $41.53 |
| 2mL Silicone Molds | B088TWCGKL | 83-EFO4-1SGB | $26.25 | $8.15 |
(Full list: 12 products in COGS_Magical_Butter.xlsx > CA tab)

### UK (14 active SKUs)
| Product | ASIN | SKU | Price (GBP) | COGS (GBP) |
|---------|------|-----|-------------|------------|
| MB2E, Decarbox | B09NP5KWQ6 | ABDB | £189.95 | £78.53 |
| Gummy Maker | B0DDL84DNV | GMAKER-3 | £130.95 | £30.94 |
| Mb2e full bundle | B0CX1WMVQV | MBUKB1 | £195.93 | £96.06 |
(Full list: 14 products in COGS_Magical_Butter.xlsx > UK tab)

### AU (13 active SKUs)
| Product | ASIN | SKU | Price (AUD) | COGS (AUD) |
|---------|------|-----|-------------|------------|
| MB2E, Decarbox | B084Q3HHTH | 1-420-240V-AU | A$284.99 | A$69.03 |
| Gummy Maker | B0DDL84DNV | GMAKER-3 | A$203.99 | A$30.99 |
| Mb2e full bundle | B0CX1WMVQV | MBUKB1 | A$199.94 | A$86.71 |
(Full list: 13 products in COGS_Magical_Butter.xlsx > AU tab)

The `product_cogs` table must be seeded from this spreadsheet. Include marketplace as a column since the same product has different COGS in different markets. The COGS spreadsheet should also be included in the repo as a reference file.

## Marketplace-Specific Line Item Notes

Not every line item appears in every marketplace. Notable cases:

- **Digital services fee / Digital services fee FBA / Digital services fee adjustment**: UK and CA only (VAT-related). Won't appear in US/AU.
- **POA service fee / POA per unit fulfillment fee**: US-observed; small amounts.
- **Other-transaction**: UK catch-all from Amazon's side. Treat as Operational Fees.
- **Goodwill**: US and CA. Customer-service refund credits, mapped to Refunds.
- **Liquidations / Liquidation adjustments**: FBA liquidation proceeds — these are revenue (Amazon paying us for liquidated inventory), so they belong in Sales, not Operational Fees.
- **Removal order lost**: Compensation for inventory lost during a removal order — a Reimbursement, not an Operational Fee.
- **Product charges**: Positive line item, reflects product-level charges/adjustments paid to the seller — categorized as Sales.

## Previously-Resolved Discrepancies

The earlier validation pass against Elena's US-January spreadsheet showed two
category-level gaps:

- **Operational Fees overstated by ~$1,525** — caused by Liquidations / Liquidation adjustments being booked as Operational Fees instead of Sales, and Removal order lost being booked as Operational Fees instead of Reimbursements. Both are corrected in the mapping above.
- **Reimbursements understated by ~$1,782** — partially explained by the missing Removal order lost line and by spreadsheet inconsistencies around Reversal reimbursement. The Removal order lost fix is in this mapping; the Reversal reimbursement question is a spreadsheet hygiene issue on Elena's side, not a mapping issue.

PR 3 validation should re-run the US-January comparison and confirm both gaps close.

## ETL Implementation Notes

### Validation Strategy (PR 3)
When comparing ETL output against Elena's manual P&L for the same date range:
1. Start with a single month (e.g. January US) where you have known actuals
2. Compare each of the six categories independently (Sales, COGS, Ad Spend, Selling Fees, Operational Fees, Refunds, Reimbursements), not just the final Gross Profit
3. Any category-level discrepancy points directly to a line item allocation issue — pull the offending line item from the catch-all log
4. Use the discrepancies to pin down any remaining edge cases
5. Once US matches, repeat for CA, UK, AU — each marketplace has different
   line items (Digital services fee only in UK/CA, AU has its own quirks)

### Catch-All for Unmapped Items (REQUIRED)
PNL_MAPPING.md is the **definitive source** for raw-line-item → P&L-category
mapping. The ETL must consult only the mappings listed above. Any line item
returned by the API that does not match a known mapping must be:

1. Logged to a dedicated `unmapped_line_items` table with: date, marketplace,
   line item name, amount, source endpoint, and the raw event payload
2. NOT silently dropped — silent drops cause totals to drift without explanation,
   and silent allocation to a default bucket hides emerging problems
3. Surfaced in the dashboard or an admin view so unmapped items can be reviewed
   and added to this mapping over time
4. New Amazon fee types appear without warning (Amazon regularly introduces new
   fee categories). The catch-all ensures these are visible immediately rather
   than discovered months later when someone notices the P&L doesn't add up

When a new line item is added to PNL_MAPPING.md, the catch-all backfill should
re-process any historical unmapped entries that match the new name so the
daily_pnl table stays consistent.
