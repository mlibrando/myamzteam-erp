# MYAMZTEAM ERP Command Center - Phase 1: Automated Daily P&L

## Project Overview

Automated daily P&L dashboard replacing Elena's manual Google Sheets process. Pulls data from Amazon SP-API, Amazon Advertising API, Shopify Admin API, and Meta Ads API into a unified PostgreSQL database, served via a Next.js dashboard with an embedded Claude-powered financial analyst agent.

## Tech Stack

- **Backend**: Python 3.12 + FastAPI + SQLAlchemy + Alembic
- **Database**: PostgreSQL 16 (hosted on Railway)
- **ETL/Scheduler**: APScheduler (in-process) or Railway cron
- **Frontend**: Next.js 14 + TypeScript + TanStack Table + Tailwind CSS
- **Agent**: Claude API with tool use (queries P&L data via defined tools)
- **Hosting**: Railway (backend + database), Vercel (frontend)
- **CI/CD**: GitHub Actions, PR-based deploys

## Repository Structure

```
myamzteam-erp/
├── backend/
│   ├── app/
│   │   ├── main.py                  # FastAPI app entry
│   │   ├── config.py                # Settings via pydantic-settings
│   │   ├── database.py              # SQLAlchemy engine + session
│   │   ├── models/
│   │   │   ├── base.py              # Declarative base
│   │   │   ├── daily_pnl.py         # Daily P&L aggregated table
│   │   │   ├── financial_events.py  # Raw Amazon financial events
│   │   │   ├── ad_spend.py          # Ad spend (Amazon, Meta)
│   │   │   ├── shopify_sales.py     # Shopify order/sales data
│   │   │   ├── product_cogs.py      # SKU-level cost of goods lookup
│   │   │   ├── currency_rates.py    # Configurable FX rates
│   │   │   └── raw_api_log.py       # Raw API response archive (jsonb)
│   │   ├── connectors/
│   │   │   ├── base.py              # Base connector with auth refresh
│   │   │   ├── amazon_sp.py         # SP-API connector (Sales, Finances, Reports)
│   │   │   ├── amazon_ads.py        # Advertising API connector (SP/SB/SD reports)
│   │   │   ├── shopify.py           # Shopify Admin API connector
│   │   │   └── meta_ads.py          # Meta Marketing API connector
│   │   ├── etl/
│   │   │   ├── scheduler.py         # APScheduler job definitions
│   │   │   ├── amazon_etl.py        # Amazon data pull + normalize
│   │   │   ├── amazon_ads_etl.py    # Ads data pull + normalize
│   │   │   ├── shopify_etl.py       # Shopify data pull + normalize
│   │   │   ├── meta_ads_etl.py      # Meta data pull + normalize
│   │   │   └── pnl_calculator.py    # Aggregate all sources into daily P&L
│   │   ├── api/
│   │   │   ├── routes/
│   │   │   │   ├── pnl.py           # GET /api/pnl (daily, range, by channel)
│   │   │   │   ├── health.py        # GET /api/health
│   │   │   │   └── agent.py         # POST /api/agent/query
│   │   │   └── deps.py              # Shared dependencies (db session, auth)
│   │   └── agent/
│   │       ├── tools.py             # Claude tool definitions for P&L queries
│   │       └── handler.py           # Agent orchestration
│   ├── alembic/
│   │   ├── alembic.ini
│   │   ├── env.py
│   │   └── versions/                # Migration files
│   ├── tests/
│   ├── requirements.txt
│   ├── Dockerfile
│   ├── railway.toml
│   └── .env.example
├── frontend/
│   ├── src/
│   │   ├── app/
│   │   │   ├── page.tsx             # Dashboard home (daily P&L table)
│   │   │   ├── layout.tsx
│   │   │   └── agent/
│   │   │       └── page.tsx         # Agent chat interface
│   │   ├── components/
│   │   │   ├── pnl-table.tsx        # Main P&L data table
│   │   │   ├── summary-cards.tsx    # Revenue, costs, profit cards
│   │   │   ├── date-picker.tsx      # Date range selector
│   │   │   ├── channel-filter.tsx   # Filter by marketplace/channel
│   │   │   └── chat-panel.tsx       # Agent chat panel
│   │   ├── lib/
│   │   │   ├── api.ts               # Backend API client
│   │   │   └── types.ts             # TypeScript interfaces
│   │   └── hooks/
│   │       └── use-pnl-data.ts      # Data fetching hook
│   ├── package.json
│   ├── tailwind.config.ts
│   ├── tsconfig.json
│   └── next.config.js
├── CLAUDE.md                         # Claude Code project context
├── PNL_MAPPING.md                    # P&L category mapping reference
├── .github/
│   └── workflows/
│       └── ci.yml
├── .gitignore
├── .env.example
└── README.md
```

## Database Schema (Core Tables)

### daily_pnl (aggregated output - what the dashboard reads)

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| date | DATE | P&L date |
| channel | VARCHAR | amazon_us, amazon_ca, amazon_uk, amazon_au, shopify_us, shopify_uk, shopify_au |
| currency | VARCHAR(3) | Original currency (USD, CAD, GBP, AUD) |
| sales | DECIMAL(12,2) | Total sales (product sales + tax + shipping + gift wrap + promotions) |
| cogs | DECIMAL(12,2) | Cost of goods sold (from product_cogs lookup * units sold) |
| ad_spend | DECIMAL(12,2) | Total ad spend (SP + SB + SD + SV + ST) |
| ad_spend_sp | DECIMAL(12,2) | Sponsored Products |
| ad_spend_sb | DECIMAL(12,2) | Sponsored Brands |
| ad_spend_sd | DECIMAL(12,2) | Sponsored Display |
| ad_spend_sv | DECIMAL(12,2) | Sponsored Videos |
| selling_fees | DECIMAL(12,2) | FBA fees + referral/commission fees |
| operational_fees | DECIMAL(12,2) | Storage, inbound transport, placement, subscription, disposal, etc. |
| refunds | DECIMAL(12,2) | Total refunds (product + tax + shipping + commission adjustments) |
| reimbursements | DECIMAL(12,2) | Reimbursements from AMZ (reversal, missing inbound, warehouse damage/lost) |
| gross_profit_no_reimb | DECIMAL(12,2) | Sales - COGS - Ad Spend - Selling Fees - Operational Fees - Refunds |
| gross_profit_with_reimb | DECIMAL(12,2) | gross_profit_no_reimb + reimbursements |
| margin_pct | DECIMAL(5,2) | gross_profit_no_reimb / sales * 100 |
| sales_usd | DECIMAL(12,2) | Sales converted to USD |
| gross_profit_usd | DECIMAL(12,2) | Gross profit converted to USD |
| fx_rate | DECIMAL(10,6) | Exchange rate used for USD conversion |
| created_at | TIMESTAMP | Record creation |
| updated_at | TIMESTAMP | Last update |

### product_cogs (SKU-level cost of goods, per marketplace)

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| marketplace | VARCHAR | us, ca, uk, au |
| sku | VARCHAR | Seller SKU (varies per marketplace) |
| asin | VARCHAR | Product ASIN |
| product_name | VARCHAR | Product display name |
| unit_cost | DECIMAL(12,2) | Cost per unit in marketplace currency |
| product_price | DECIMAL(12,2) | Normal selling price |
| currency | VARCHAR(3) | Currency of the unit cost (USD, CAD, GBP, AUD) |
| status | VARCHAR | active, discontinued |
| effective_date | DATE | When this cost became effective |
| created_at | TIMESTAMP | Record creation |

### currency_rates (configurable FX rates)

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| from_currency | VARCHAR(3) | Source currency (GBP, CAD, AUD) |
| to_currency | VARCHAR(3) | Target currency (USD) |
| rate | DECIMAL(10,6) | Conversion rate |
| effective_date | DATE | When this rate became effective |
| created_at | TIMESTAMP | Record creation |

### financial_events (raw Amazon fee data)

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| event_group_id | VARCHAR | Amazon financial event group ID |
| event_type | VARCHAR | ShipmentEvent, RefundEvent, etc. |
| posted_date | TIMESTAMP | When Amazon posted the event |
| marketplace_id | VARCHAR | ATVPDKIKX0DER, A2EUQ1WTGCTBG2, etc. |
| order_id | VARCHAR | Amazon order ID |
| asin | VARCHAR | Product ASIN |
| sku | VARCHAR | Seller SKU |
| fee_type | VARCHAR | FBAPerUnitFulfillmentFee, Commission, etc. |
| fee_amount | DECIMAL(12,2) | Fee amount |
| currency | VARCHAR(3) | USD, CAD, MXN |
| raw_payload | JSONB | Full API response for this event |
| created_at | TIMESTAMP | Record creation |

### ad_spend (Amazon + Meta ads)

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| date | DATE | Spend date |
| platform | VARCHAR | amazon_sp, amazon_sb, amazon_sd, meta |
| campaign_id | VARCHAR | Campaign identifier |
| campaign_name | VARCHAR | Campaign name |
| marketplace | VARCHAR | US, CA, MX, etc. |
| spend | DECIMAL(12,2) | Ad spend amount |
| sales_attributed | DECIMAL(12,2) | Attributed sales (14d for Amazon) |
| impressions | INTEGER | Ad impressions |
| clicks | INTEGER | Ad clicks |
| acos | DECIMAL(5,2) | ACoS percentage |
| currency | VARCHAR(3) | Currency code |
| raw_payload | JSONB | Full report row |
| created_at | TIMESTAMP | Record creation |

### shopify_sales

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| date | DATE | Order date |
| order_id | VARCHAR | Shopify order ID |
| total_price | DECIMAL(12,2) | Order total |
| subtotal | DECIMAL(12,2) | Before tax/shipping |
| total_tax | DECIMAL(12,2) | Tax collected |
| total_discounts | DECIMAL(12,2) | Discounts applied |
| currency | VARCHAR(3) | Currency |
| raw_payload | JSONB | Full order object |
| created_at | TIMESTAMP | Record creation |

### raw_api_log (debug/audit trail)

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| source | VARCHAR | amazon_sp, amazon_ads, shopify, meta |
| endpoint | VARCHAR | API endpoint called |
| request_params | JSONB | Request parameters |
| response_status | INTEGER | HTTP status code |
| response_body | JSONB | Truncated response (first 10KB) |
| pulled_at | TIMESTAMP | When the pull happened |

## PR Delivery Plan

### PR 1: Project Scaffolding + Database Setup
**Branch**: `feat/project-scaffold`

- Initialize repo with backend/ and frontend/ structure
- FastAPI app skeleton with health endpoint
- SQLAlchemy models + Alembic setup
- Initial migration with all core tables
- Dockerfile + railway.toml for Railway deployment
- .env.example with all required env vars
- CLAUDE.md with project context
- Deploy to Railway, run migrations, verify DB is live

**Validation**: `GET /api/health` returns 200 with DB connection status

---

### PR 2: Amazon SP-API Connector + Auth
**Branch**: `feat/amazon-sp-connector`

- LWA token exchange with auto-refresh (cache access token, refresh on expiry)
- Base connector class with retry logic, rate limiting, error handling
- Sales API integration (`/sales/v1/orderMetrics`) for daily revenue
- Finances API integration (`/finances/v0/financialEventGroups` + drill-down)
- Unit tests with mocked API responses

**Validation**: Run connector manually, verify revenue + fee data prints correctly for US marketplace

---

### PR 3: Amazon SP-API ETL + Data Storage
**Branch**: `feat/amazon-sp-etl`

- ETL pipeline: pull Sales API data, normalize, store in daily_pnl
- ETL pipeline: pull Finances API data, normalize, store in financial_events
- P&L calculator: aggregate financial_events into daily_pnl fee columns
- Raw API response logging to raw_api_log
- Support for all three marketplaces (US, CA, MX)
- Manual trigger endpoint: `POST /api/etl/amazon/run`

**Validation**: Trigger ETL, query daily_pnl table, compare output against Elena's manual P&L for same date range

---

### PR 4: Amazon Advertising API Connector + ETL
**Branch**: `feat/amazon-ads`

- Ads API OAuth setup (separate from SP-API auth)
- Profile discovery (`GET /v2/profiles`) + storage of profileIds per marketplace
- Async report flow: create SP/SB/SD campaign reports, poll, download gzipped JSON
- Parse and normalize ad spend data into ad_spend table
- Aggregate daily ad spend into daily_pnl (ad_spend_sp, ad_spend_sb, ad_spend_sd columns)
- Manual trigger endpoint: `POST /api/etl/amazon-ads/run`

**Validation**: Trigger ETL, verify ad spend totals match what Elena sees in Amazon Ads console

---

### PR 4.5: SP-API Reconciliation (by-group + settlement Taxes) + PT cutoffs
**Branch**: `feat/sp-reconciliation`

Closes the last remaining P&L gaps against Elena's manual sheet without changing daily-ETL semantics. Rationale in CLAUDE.md → "Finances endpoints: by-date vs by-group". Three deliverables:

1. **PT timezone cutoffs (partially landed with the by-group URL fix)**. `MONTHLY_CUTOFF_TIMEZONE` (default `America/Los_Angeles`) added. `pnl_calculator` buckets `daily_pnl.date` in the cutoff tz so it matches Elena's Seller Central defaults. Validation scripts convert PT month bounds to UTC before hitting the API. Documented in PNL_MAPPING.md.
2. **Monthly by-group reconciliation ETL**. Runs after settlement windows close (weekly-ish). Lists all `ProcessingStatus=Closed` groups whose window overlaps the target period, pulls each via the corrected `/finances/v0/financialEventGroups/{gid}/financialEvents` URL, and re-writes `financial_events` rows for the covered PT-local date range. Adds `source` column (`financial_events_by_date` vs `financial_events_by_group`) so daily and reconciliation rows can coexist during the fetch and the reconciliation pass wins on conflict. Re-runs `pnl_calculator` for affected days. Closes ~$4,421 Op Fees gap (empirically measured Jan 2026 US: FBAStorageFee, FBALongTermStorageFee, FBAInboundConvenienceFee, FBARemovalFee, FBAInboundTransportationFee, Subscription).
3. **Settlement report ingestion — Taxes remitted only**. `Reports API` `reportType=GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2`. Parses only the `transaction-type=other-transaction` + `amount-description=TaxesRemittedToAmazon` line (or the equivalent Elena-mapped identifier). Inserts into `financial_events` with `source='settlement'`, `category=SELLING_FEES`. Everything else in the settlement report is ignored (by-group covers it). Closes the ~$10,978 Selling Fees gap.

Daily ETL is unchanged — by-date remains the correct primitive for daily P&L. by-group/settlement only refill the month-end totals with a delay of one settlement cycle.

**Validation**: after by-group + settlement + PT bucketing land, Jan 2026 US Sales/Selling Fees/Op Fees/Refunds/Reimbursements each match Elena within <1% (target: <$500 residual per category).

---

### PR 5: Scheduler + Automated Daily Runs
**Branch**: `feat/scheduler`

Daily jobs (all UTC, pull yesterday's data):
- Job: Amazon SP-API pull (revenue + fees) — 06:00 UTC
- Job: Amazon Ads pull (SP/SB/SD spend) — 06:30 UTC
- Job: P&L recalculation — 07:00 UTC (after all pulls complete)

Monthly jobs (all UTC, PT-anchored calendar months):
- Day 10 10:00 — by-group reconciliation for the just-closed prior month (closes ~$4.4k Op Fees gap)
- Day 10 11:00 — settlement Taxes ingestion for the just-closed prior month (closes ~$11k Selling Fees gap)
- Day 15 10:00 — catch-up settlement Taxes re-run for the month before the prior month (late-arriving adjustments)
- Day 15 12:00 — full P&L recalculation covering both reconciled months

- Error handling: each job catches and logs its own exceptions; one failure never kills the scheduler process
- Idempotency: re-running for the same date/month overwrites, never duplicates
- `ETL_SCHEDULE_ENABLED=False` config flag suppresses scheduler startup (CI, ad-hoc scripts)
- `GET /api/scheduler/status` endpoint returns running state and next fire times
- Manual overrides always available via `/api/etl/*` endpoints

**Validation**: Let scheduler run for 2-3 days, verify daily_pnl populates automatically; verify monthly jobs fire on day 10/15

---

### PR 6: P&L API Endpoints
**Branch**: `feat/pnl-api`

- `GET /api/pnl/daily?start_date=&end_date=&channel=` - daily P&L with filters
- `GET /api/pnl/summary?start_date=&end_date=` - aggregated summary across channels
- `GET /api/pnl/compare?date=&compare_to=` - day-over-day or period comparison
- Response schemas with Pydantic models
- Pagination for large date ranges

**Validation**: Hit endpoints via curl/Postman, verify response shape and data accuracy

---

### PR 7: Frontend Dashboard - P&L Table
**Branch**: `feat/frontend-dashboard`

- Next.js project setup with TypeScript + Tailwind
- API client connecting to Railway backend
- Main P&L table (TanStack Table) with date range + channel filters
- Summary cards: total revenue, total expenses, net profit, margin %
- Date picker component (day, week, month views)
- Channel filter (All, Amazon US, Amazon CA, Amazon MX, Shopify)
- Deploy to Vercel

**Validation**: Dashboard loads, table populates with real data from API

---

### PR 8: Shopify + Meta Ads Connectors
**Branch**: `feat/shopify-meta`

- Shopify Admin API connector (OAuth callback setup, order data pull)
- Shopify ETL: pull orders, normalize into shopify_sales + daily_pnl
- Meta Marketing API connector (access token, campaign insights)
- Meta ETL: pull ad spend by campaign, normalize into ad_spend + daily_pnl
- Scheduler jobs for both (daily)
- Manual trigger endpoints for both

**Validation**: All four data sources flowing into daily_pnl, totals match manual P&L

---

### PR 9: Claude Agent Integration
**Branch**: `feat/agent`

- Claude API setup with tool use
- Tool definitions:
  - `get_daily_pnl(date_range, channel)` - query daily P&L data
  - `get_expense_breakdown(date_range, category)` - detailed fee breakdown
  - `compare_periods(period_a, period_b)` - period comparison
  - `get_top_ad_campaigns(date_range, metric)` - top campaigns by spend/ROAS
- Agent handler: receives natural language query, routes to tools, formats response
- `POST /api/agent/query` endpoint
- Frontend chat panel component wired to agent endpoint

**Validation**: Ask agent "What was our Amazon US margin yesterday?" and get accurate answer

---

### PR 10: Polish + Production Hardening
**Branch**: `feat/production-polish`

- Environment variable validation on startup
- Graceful error handling across all connectors (partial failures don't block entire ETL)
- API rate limit middleware
- CORS configuration for Vercel frontend
- Logging standardization (structured JSON logs)
- README with setup instructions, architecture diagram
- Walkthrough with Mark

**Validation**: Full end-to-end daily P&L matches Elena's manual output for a 7-day period

## Environment Variables

```
# Amazon SP-API (one LWA app, three regional refresh tokens)
AMAZON_SP_CLIENT_ID=
AMAZON_SP_CLIENT_SECRET=
AMAZON_SP_REFRESH_TOKEN_NA=    # US, CA, MX -> sellingpartnerapi-na.amazon.com
AMAZON_SP_REFRESH_TOKEN_EU=    # UK         -> sellingpartnerapi-eu.amazon.com
AMAZON_SP_REFRESH_TOKEN_FE=    # AU         -> sellingpartnerapi-fe.amazon.com

# Amazon Advertising API (separate LWA app from SP-API; three regional endpoints)
# NA is required; EU and FE are OPTIONAL and fall back to the NA token when
# unset (works when the Amazon login has cross-region seller access).
AMAZON_ADS_CLIENT_ID=
AMAZON_ADS_CLIENT_SECRET=
AMAZON_ADS_REFRESH_TOKEN_NA=  # US, CA, MX (+ BR)  -> advertising-api.amazon.com
AMAZON_ADS_REFRESH_TOKEN_EU=  # optional; UK (+ DE/FR/IT/ES/NL/IE/SE/PL/TR/AE)
AMAZON_ADS_REFRESH_TOKEN_FE=  # optional; AU

# Shopify
SHOPIFY_STORE_URL=
SHOPIFY_API_KEY=
SHOPIFY_API_SECRET=
SHOPIFY_ACCESS_TOKEN=

# Meta Ads
META_ADS_ACCESS_TOKEN=
META_ADS_ACCOUNT_ID=

# Database
DATABASE_URL=postgresql://user:pass@host:port/dbname

# Claude API
ANTHROPIC_API_KEY=

# App
APP_ENV=development
LOG_LEVEL=INFO
ETL_SCHEDULE_ENABLED=true
```

## Amazon Marketplace IDs (Reference)

| Marketplace | ID | Currency | Region Endpoint |
|-------------|-----|----------|-----------------|
| Amazon.com (US) | ATVPDKIKX0DER | USD | sellingpartnerapi-na.amazon.com |
| Amazon.ca (CA) | A2EUQ1WTGCTBG2 | CAD | sellingpartnerapi-na.amazon.com |
| Amazon.com.mx (MX) | A1AM78C64UM0Y8 | MXN | sellingpartnerapi-na.amazon.com |
| Amazon.co.uk (UK) | A1F83G8C2ARO7P | GBP | sellingpartnerapi-eu.amazon.com |
| Amazon.com.au (AU) | A39IBJ37TRP1C6 | AUD | sellingpartnerapi-fe.amazon.com |

Note: UK and AU use different regional endpoints. Auth tokens may need separate refresh tokens per region. Verify with the credentials provided.

## Key API Endpoints (Amazon SP-API)

| Purpose | Endpoint | Method | Frequency |
|---------|----------|--------|-----------|
| Revenue (aggregate) | /sales/v1/orderMetrics | GET | Daily |
| Revenue (order-level) | /orders/v0/orders | GET | Daily |
| Revenue (bulk/ASIN) | /reports/2021-06-30/reports (GET_SALES_AND_TRAFFIC_REPORT) | POST | Weekly |
| Fee groups | /finances/v0/financialEventGroups | GET | Monthly |
| Fee details | /finances/v0/financialEvents/{groupId} | GET | Monthly |
| FBA Inventory | /fba/inventory/v1/summaries | GET | Daily |
| Auth | https://api.amazon.com/auth/o2/token | POST | Per session |

## Key API Endpoints (Amazon Advertising)

Base URLs are regional: `advertising-api.amazon.com` (NA), `advertising-api-eu.amazon.com` (EU), `advertising-api-fe.amazon.com` (FE). Each region uses its own refresh token but shares the same LWA client_id + secret.

| Purpose | Endpoint | Method | Frequency |
|---------|----------|--------|-----------|
| Profiles | /v2/profiles | GET | Once per region (cached) |
| SP Campaign Report | /reporting/reports (`reportTypeId=spCampaigns`) | POST | Daily |
| SB Campaign Report | /reporting/reports (`reportTypeId=sbCampaigns`) | POST | Daily |
| SD Campaign Report | /reporting/reports (`reportTypeId=sdCampaigns`) | POST | Daily |
| Report Status | /reporting/reports/{reportId} | GET | Poll every 30s, max 10 min |
| Report Download | (signed S3 URL from status response) | GET | Per completed report |

### Future marketplaces available with existing auth

The 2026-07 multi-region auth verification (see CLAUDE.md § Amazon Advertising API Auth Flow) surfaced additional marketplaces the current LWA authorization already covers, out of scope for Phase 1 but zero-auth-work to add:

- **NA** additionally covers Brazil (BR / A2Q3Y263D00KWC / BRL)
- **EU** additionally covers Germany, France, Italy, Spain, Netherlands, Ireland, Sweden, Poland, Turkey, UAE (DE, FR, IT, ES, NL, IE, SE, PL, TR, AE)

Adding any of these to the ETL is purely a config change — extend `MARKETPLACE_REGION`, `MARKETPLACE_CHANNEL`, `MARKETPLACE_CURRENCY`, and `COUNTRY_TO_MARKETPLACE` in [backend/app/connectors/amazon_sp.py](backend/app/connectors/amazon_sp.py) and [backend/app/connectors/amazon_ads.py](backend/app/connectors/amazon_ads.py). Note: SP-API access may still need to be authorized per new marketplace even if Ads is already in scope.

### Future ad products (deferred from PR 4)

PNL_MAPPING.md lists Sponsored Television (`Sponsored television`) and Sponsored Videos (`Sponsored videos`) under Ad Spend. The `daily_pnl.ad_spend_sv` column already exists. Adding either requires:
- Add the `adProduct` + `reportTypeId` to `REPORT_TYPE_ID_BY_AD_PRODUCT` and the per-product metric list to `METRICS_BY_AD_PRODUCT` in [backend/app/etl/amazon_ads_etl.py](backend/app/etl/amazon_ads_etl.py)
- Add the platform key (e.g. `amazon_sv` / `amazon_st`) to `AD_PRODUCT_TO_PLATFORM` and `AD_SPEND_PLATFORM_TO_COLUMN`
- Confirm SV/ST use the same `groupBy=["campaign"]` + `timeUnit=DAILY` shape; if not, generalize the report helper

## Rate Limits to Respect

- Sales API: 0.5 req/s
- Orders API: 0.0167 req/s (1 per minute)
- Finances API: 0.5 req/s
- Reports API: 0.0167 req/s
- FBA Inventory: 2 req/s
- LWA Token: no explicit limit, but cache and refresh only on expiry
- Ads API reports: async, no per-request limit but respect polling intervals (30s)

## Success Criteria

1. Daily P&L populates automatically by 7 AM UTC without manual intervention
2. All P&L line items match Elena's manual calculations within 1% tolerance
3. Formula: Gross Profit = Sales - COGS - Ad Spend - Selling Fees - Operational Fees - Refunds (both with and without reimbursements)
4. Dashboard displays daily/weekly/monthly P&L with channel breakdown
5. Multi-currency support: all non-USD values converted using configurable FX rates
6. COGS calculated from product_cogs lookup table * units sold (not from Amazon API)
7. Agent answers natural language questions about P&L data accurately
8. System handles API failures gracefully (retries, partial data, alerts)

## Critical Reference Files

- PNL_MAPPING.md - definitive mapping of raw line items to P&L categories
- PROJECT_PLAN.md - this file, overall architecture and PR plan
- CLAUDE.md - Claude Code project context
