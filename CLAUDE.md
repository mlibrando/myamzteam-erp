# CLAUDE.md - MYAMZTEAM ERP Command Center

## What is this project?

An automated daily P&L dashboard for MagicalButter, replacing a manual Google Sheets process. The system pulls financial data from Amazon SP-API, Amazon Advertising API, Shopify, and Meta Ads into PostgreSQL, then serves it via a Next.js dashboard with a Claude-powered financial analyst agent.

## Architecture

```
Amazon SP-API (Sales + Finances) ──┐
Amazon Ads API (SP/SB/SD reports) ─┤
Shopify Admin API (orders) ────────┤──> FastAPI ETL ──> PostgreSQL (Railway)
Meta Marketing API (ad spend) ─────┘         │
                                             │
                              ┌──────────────┤
                              v              v
                        Next.js Dashboard   Claude Agent
                        (Vercel)            (tool use, queries DB)
```

## Tech Stack

- Backend: Python 3.12, FastAPI, SQLAlchemy 2.0, Alembic, APScheduler
- Database: PostgreSQL 16 on Railway
- Frontend: Next.js 14, TypeScript, TanStack Table, Tailwind CSS
- Agent: Anthropic Claude API with tool use
- Hosting: Railway (backend + DB), Vercel (frontend)

## Project Structure

- `backend/` - FastAPI application, connectors, ETL pipelines, agent
- `backend/app/connectors/` - API connectors (one per data source)
- `backend/app/etl/` - ETL pipelines and P&L calculation logic
- `backend/app/models/` - SQLAlchemy models
- `backend/app/api/routes/` - API endpoints
- `backend/app/agent/` - Claude agent tools and handler
- `frontend/` - Next.js dashboard application

## Key Design Decisions

- **P&L formula**: Gross Profit = Sales - COGS - Ad Spend - Selling Fees - Operational Fees - Refunds. Track both "with reimbursements" and "without reimbursements" versions. See PNL_MAPPING.md for the definitive category-to-line-item mapping.
- **Fees split into two categories**: "Selling Fees" (FBA fulfillment + referral/commission) and "Operational Fees" (storage, inbound transport, placement, subscription, disposal, etc.). These are NOT combined.
- **COGS is NOT from Amazon API**. It comes from a `product_cogs` table with known per-unit costs per SKU. COGS = unit_cost * quantity_sold. This table is seeded manually and updated when costs change.
- **Multi-currency**: UK (GBP), CA (CAD), AU (AUD) data must be converted to USD using rates from a `currency_rates` table. Store both original currency amounts and USD-converted amounts.
- **Reporting timezone is Pacific Time.** `daily_pnl.date` is a PT-local date (Amazon Seller Central's default; matches Elena's manual P&L). Storage stays UTC — the conversion happens at aggregation time in `pnl_calculator` via `AT TIME ZONE 'America/Los_Angeles'`. Controlled by `MONTHLY_CUTOFF_TIMEZONE` in `config.py`. Validation scripts must convert PT month bounds to UTC before calling the SP-API — helpers in [backend/app/etl/timezone_utils.py](backend/app/etl/timezone_utils.py). See PNL_MAPPING.md → "Reporting Timezone".
- Every connector stores raw API responses in a `raw_payload` JSONB column alongside normalized data. This is intentional for debugging discrepancies against Elena's manual P&L.
- The daily_pnl table is the single source of truth for the dashboard. ETL pipelines write to source-specific tables first, then a P&L calculator aggregates into daily_pnl.
- All ETL jobs are idempotent. Re-running for the same date overwrites existing records (upsert on date + channel composite key), never duplicates.
- Rate limits are critical. Amazon SP-API throttles aggressively. Every connector must implement exponential backoff and respect the rate limits documented in PROJECT_PLAN.md.
- **Reimbursements from AMZ** (reversal reimbursement, missing from inbound, warehouse damage, warehouse lost) are a separate positive line item. They are NOT netted into refunds or fees.
- **Catch-all for unmapped line items**: Any raw API line item that doesn't match a known category mapping must be logged (not silently dropped). Amazon introduces new fee types without warning. See PNL_MAPPING.md for the full list of known unmapped items and validation strategy.

## Amazon SP-API Auth Flow

1. Exchange refresh_token + client_id + client_secret for access_token via POST to `https://api.amazon.com/auth/o2/token`
2. Access token expires in 3600s. Cache it, refresh only on expiry or 401.
3. Pass access_token in `x-amz-access-token` header on all SP-API calls.

### Regional auth

The SP-API is split into three regions. Each region has its own base URL and its own refresh token, but all three share the same LWA `client_id` + `client_secret`. Token caching is per-region — each refresh token mints a region-specific access token that is only valid against that region's base URL.

| Region | Marketplaces | Refresh token env var       | Base URL                                  |
|--------|--------------|-----------------------------|-------------------------------------------|
| NA     | US, CA, MX   | `AMAZON_SP_REFRESH_TOKEN_NA` | `https://sellingpartnerapi-na.amazon.com` |
| EU     | UK           | `AMAZON_SP_REFRESH_TOKEN_EU` | `https://sellingpartnerapi-eu.amazon.com` |
| FE     | AU           | `AMAZON_SP_REFRESH_TOKEN_FE` | `https://sellingpartnerapi-fe.amazon.com` |

The connector ([backend/app/connectors/amazon_sp.py](backend/app/connectors/amazon_sp.py)) takes a `region` parameter (`"NA"` | `"EU"` | `"FE"`) and selects the matching refresh token + base URL. Use `AmazonSPConnector.for_marketplace(marketplace_id)` to construct the right region from a marketplace ID. Run one connector instance per region in ETL — they don't share tokens.

### Finances endpoints: by-date vs by-group

The daily ETL uses `/finances/v0/financialEvents?PostedAfter=...&PostedBefore=...` (by-date). This endpoint works with the default SP-API app scope and returns the same FinancialEvents structure (ShipmentEventList, RefundEventList, ServiceFeeEventList, AdjustmentEventList, ...) merged across all groups that overlap the window. It's the correct primitive for daily P&L: you query yesterday's posting window directly with no dependency on settlement-group lifecycle.

**By-group URL correction (2026-07-01).** The endpoint path is `/finances/v0/financialEventGroups/{eventGroupId}/financialEvents` — NOT `/finances/v0/financialEvents/{eventGroupId}`. The wrong-path variant returns `403 Unauthorized: Access to requested resource is denied` (not 404), which historically misled us into thinking by-group required an additional data role. It does not — LWA scope was fine all along. Verified with the corrected path against 11 Closed groups covering Jan 2026 via [backend/scripts/test_sp_by_group.py](backend/scripts/test_sp_by_group.py): every call returned 200. The connector's `get_financial_events(event_group_id)` now uses the corrected path. The Finance-and-Accounting role addition Elena performed for the 2026-07-01 token was not necessary for this endpoint (it may still matter for other SP-API paths, but not by-group).

**What by-group covers that by-date does not.** Head-to-head against Elena's Jan 2026 US window (see [backend/scripts/diff_bygroup_line_items.py](backend/scripts/diff_bygroup_line_items.py)):

| Category | Overlap | by-group extra volume vs by-date |
|---|---|---:|
| Sales (`ShipmentEventList` Principal/Tax/Shipping/GiftWrap) | identical | $0 |
| Refunds (`RefundEventList`) | identical | $0 |
| Reimbursements + Adjustments (`AdjustmentEventList`) | identical | $0 |
| Selling Fees inside shipment items (Commission, FBAPerUnitFulfillmentFee, ...) | identical | $0 |
| **Op Fees inside `ServiceFeeEventList`** (FBAStorageFee, FBALongTermStorageFee, FBAInboundConvenienceFee, FBARemovalFee, FBAInboundTransportationFee, Subscription) | by-group is ~2× the by-date volume | **~-$4,421** |
| `ProductAdsPaymentEventList` | by-group ~2× the by-date volume | ~-$24,382 (informational only — see PNL_MAPPING.md "Ad Spend Source of Truth") |

The ~$4,421 extra Op Fees in by-group aligns almost exactly with Elena's $4,793 Op Fees gap — by-group closes it. But **no `TaxesRemittedToAmazon` line appears in either endpoint**, so the $10,978 Selling Fees gap is NOT closed by by-group. That money surfaces only in the settlement report (`Reports API` + `reportType=GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2`).

**Practical constraint.** by-group only serves `ProcessingStatus=Closed` groups, so daily ETL stays on by-date. by-group is the month-end reconciliation pass, not a daily-ETL replacement — see PROJECT_PLAN.md → PR 4.5.

## Amazon Advertising API Auth Flow

The Ads API is a separate LWA app from SP-API — different refresh token, different base URL, different header style. Do not reuse the SP-API connector.

1. Exchange refresh_token + client_id + client_secret for access_token via POST to `https://api.amazon.com/auth/o2/token` (same endpoint, separate token).
2. Pass `Authorization: Bearer {access_token}` (NOT `x-amz-access-token`).
3. Pass `Amazon-Advertising-API-ClientId: {client_id}` on every request.
4. Discover profiles once: `GET /v2/profiles` returns one profile per (advertising-account × country). Map by `countryCode` → marketplaceId.
5. Pass `Amazon-Advertising-API-Scope: {profileId}` on every subsequent request — different profileId for each marketplace.

### Regional Ads endpoints

| Region | Marketplaces | Refresh token env var          | Base URL                          |
|--------|--------------|---------------------------------|-----------------------------------|
| NA     | US, CA, MX   | `AMAZON_ADS_REFRESH_TOKEN_NA`   | `https://advertising-api.amazon.com` |
| EU     | UK           | `AMAZON_ADS_REFRESH_TOKEN_EU` (optional) | `https://advertising-api-eu.amazon.com` |
| FE     | AU           | `AMAZON_ADS_REFRESH_TOKEN_FE` (optional) | `https://advertising-api-fe.amazon.com` |

**Single-token cross-region support.** A single LWA authorization can span all three regions when the seller login has access to marketplaces in each. In that case only `AMAZON_ADS_REFRESH_TOKEN_NA` needs to be set; the EU/FE connectors fall back to the NA token automatically. Verified 2026-07 for the MagicalButter account — the same access token authenticated all three regional `/v2/profiles` endpoints with the expected US, UK, and AU profiles returned respectively.

Notable observation from that verification: one LWA authorization can span **multiple distinct seller-account IDs** (NA=A348DWHA5R9ZQL, EU=AEXHDCLX193WF, FE=A1DT6S189BMA8X for MagicalButter) because they're linked to the same Amazon login. If Ads auth ever breaks in the future, "seller separated their accounts" is a candidate root cause worth checking — in that case set the regional token env vars explicitly so each region has its own OAuth flow.

The connector ([backend/app/connectors/amazon_ads.py](backend/app/connectors/amazon_ads.py)) is per-region. Use `conn.for_profile(profile_id)` to spawn a profile-scoped clone for per-marketplace requests; the clone shares the parent's HTTP client and token cache. Each region instance still caches its own access token even when the refresh token is shared.

### Async report flow (Reports API v3)

All campaign-level spend data comes from async reports — no real-time GET endpoints.

1. `POST /reporting/reports` with `configuration.reportTypeId` (`spCampaigns` / `sbCampaigns` / `sdCampaigns`), `timeUnit=DAILY`, `groupBy=["campaign"]`, columns list, format `GZIP_JSON`. Returns `reportId`.
2. `GET /reporting/reports/{reportId}` — poll every 30s. States: `PENDING → PROCESSING → COMPLETED` (or `FAILED`). Max poll time 10 minutes.
3. When `COMPLETED`, response contains a signed S3 `url`. Download it (no auth headers), decompress gzip if present, parse JSON.
4. SP uses `sales7d`/`purchases7d` (7-day attribution); SB and SD use `sales`/`purchases`.

### Sponsored Television / Sponsored Videos (future work)

PNL_MAPPING.md lists Sponsored Videos and Sponsored Television under Ad Spend. They require different `reportTypeId` values (`stCampaigns`, `svCampaigns` or equivalent) and possibly different column sets. The `ad_spend_sv` column already exists in `daily_pnl`. To add: extend `REPORT_TYPE_ID_BY_AD_PRODUCT`, `METRICS_BY_AD_PRODUCT`, and `AD_PRODUCT_TO_PLATFORM` in [backend/app/etl/amazon_ads_etl.py](backend/app/etl/amazon_ads_etl.py) and `AD_SPEND_PLATFORM_TO_COLUMN` in [backend/app/etl/pnl_calculator.py](backend/app/etl/pnl_calculator.py).

## Marketplace IDs

- US: ATVPDKIKX0DER (primary, highest volume) - USD - NA endpoint
- CA: A2EUQ1WTGCTBG2 - CAD - NA endpoint
- MX: A1AM78C64UM0Y8 - MXN - NA endpoint
- UK: A1F83G8C2ARO7P - GBP - EU endpoint (sellingpartnerapi-eu.amazon.com)
- AU: A39IBJ37TRP1C6 - AUD - FE endpoint (sellingpartnerapi-fe.amazon.com)

Note: UK and AU use different regional base URLs and may require separate refresh tokens.

## Important Rate Limits

- Sales API: 0.5 req/s
- Finances API: 0.5 req/s
- Orders API: 0.0167 req/s (very slow, use sparingly)
- Reports API: 0.0167 req/s
- FBA Inventory: 2 req/s
- All async reports: poll every 30 seconds, not faster

## Commands

```bash
# Backend
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000

# Run migrations
cd backend
alembic upgrade head

# Create new migration
alembic revision --autogenerate -m "description"

# Frontend
cd frontend
npm install
npm run dev

# Manual ETL trigger (once endpoints exist)
curl -X POST http://localhost:8000/api/etl/amazon/run
curl -X POST http://localhost:8000/api/etl/amazon-ads/run
```

## Environment Variables

All secrets are in .env (never committed). See .env.example for the full list. Key ones:
- AMAZON_SP_CLIENT_ID, AMAZON_SP_CLIENT_SECRET, AMAZON_SP_REFRESH_TOKEN
- AMAZON_ADS_CLIENT_ID, AMAZON_ADS_CLIENT_SECRET, AMAZON_ADS_REFRESH_TOKEN_NA, AMAZON_ADS_REFRESH_TOKEN_EU, AMAZON_ADS_REFRESH_TOKEN_FE
- SHOPIFY_STORE_URL, SHOPIFY_ACCESS_TOKEN
- META_ADS_ACCESS_TOKEN, META_ADS_ACCOUNT_ID
- DATABASE_URL (provided by Railway automatically)
- ANTHROPIC_API_KEY

## PR Workflow

Each feature is delivered as a separate PR. See PROJECT_PLAN.md for the full 10-PR delivery plan. PRs build on each other sequentially. Current PR context will be noted in branch names prefixed with `feat/`.

## Coding Conventions

- Python: type hints everywhere, Pydantic for all request/response models, async where beneficial (especially API calls)
- Use `httpx` (async) over `requests` for all external API calls
- All monetary values are DECIMAL(12,2), never float
- All dates are UTC. Convert to user timezone only in the frontend.
- Error handling: never let a single API failure crash the entire ETL run. Log the error, skip that data source, continue with others.
- Tests: pytest, mock all external API calls, test ETL transformation logic with fixture data
