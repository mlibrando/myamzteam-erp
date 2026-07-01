"""Aggregate financial_events + ad_spend into daily_pnl rows.

Reads from:
  - financial_events (populated by amazon_etl) for sales, fees, refunds,
    reimbursements
  - product_cogs * net units shipped, for COGS
  - ad_spend (populated by amazon_ads_etl) for ad_spend / ad_spend_sp /
    ad_spend_sb / ad_spend_sd / ad_spend_sv
  - currency_rates for USD normalization

Upserts daily_pnl on (date, channel). Each call is idempotent over the
window: the calculator reads all four sources fresh and overwrites the
existing row.

If only one of {financial_events, ad_spend} has data for a given (date,
marketplace), that day still gets a row -- whichever source is missing
contributes zeros. This means running the Ads ETL before the SP-API ETL
will still produce a daily_pnl row (with sales=0 etc.), and the next
SP-API run will fill in the missing fields.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable

from sqlalchemy import and_, cast, func, select, Date
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.amazon_sp import (
    MARKETPLACE_CHANNEL,
    MARKETPLACE_CURRENCY,
    MARKETPLACE_REGION,
)
from app.etl.pnl_mapping import PnlCategory
from app.models import AdSpend, CurrencyRate, DailyPnL, FinancialEvent, ProductCogs


# ad_spend.platform values for the four Amazon ad products (SV = Sponsored
# Videos, a future-work product that doesn't have an ETL yet but the column
# exists in daily_pnl).
AD_SPEND_PLATFORM_TO_COLUMN: dict[str, str] = {
    "amazon_sp": "ad_spend_sp",
    "amazon_sb": "ad_spend_sb",
    "amazon_sd": "ad_spend_sd",
    "amazon_sv": "ad_spend_sv",
}

# ad_spend.marketplace is the lowercase short code (us, ca, mx, uk, au).
# Convert to the canonical Amazon marketplace_id used everywhere else.
SHORT_CODE_TO_MARKETPLACE: dict[str, str] = {
    "us": "ATVPDKIKX0DER",
    "ca": "A2EUQ1WTGCTBG2",
    "mx": "A1AM78C64UM0Y8",
    "uk": "A1F83G8C2ARO7P",
    "au": "A39IBJ37TRP1C6",
}

logger = logging.getLogger(__name__)


# Map marketplace_id -> the marketplace-key used in product_cogs.marketplace.
# product_cogs uses the lowercase short code (us, ca, mx, uk, au).
MARKETPLACE_COGS_KEY: dict[str, str] = {
    "ATVPDKIKX0DER": "us",
    "A2EUQ1WTGCTBG2": "ca",
    "A1AM78C64UM0Y8": "mx",
    "A1F83G8C2ARO7P": "uk",
    "A39IBJ37TRP1C6": "au",
}


@dataclass
class PnlCalcSummary:
    rows_written: int = 0
    skus_without_cogs: list[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.skus_without_cogs is None:
            self.skus_without_cogs = []


_DEC_ZERO = Decimal("0")
_DEC_HUNDRED = Decimal("100")
_AGG_CATEGORIES: tuple[PnlCategory, ...] = (
    PnlCategory.SALES,
    PnlCategory.SELLING_FEES,
    PnlCategory.OPERATIONAL_FEES,
    PnlCategory.REFUNDS,
    PnlCategory.REIMBURSEMENTS,
)


def _round_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


async def _aggregate_categories(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
    marketplace_ids: Iterable[str],
) -> dict[tuple[date, str], dict[str, Decimal]]:
    """Sum fee_amount per (date, marketplace_id, category) across the window."""
    date_col = cast(FinancialEvent.posted_date, Date).label("posted_day")
    stmt = (
        select(
            date_col,
            FinancialEvent.marketplace_id,
            FinancialEvent.category,
            func.coalesce(func.sum(FinancialEvent.fee_amount), 0),
        )
        .where(
            and_(
                FinancialEvent.marketplace_id.in_(list(marketplace_ids)),
                cast(FinancialEvent.posted_date, Date) >= start_date,
                cast(FinancialEvent.posted_date, Date) <= end_date,
                FinancialEvent.category.is_not(None),
            )
        )
        .group_by(date_col, FinancialEvent.marketplace_id, FinancialEvent.category)
    )
    result = await session.execute(stmt)
    totals: dict[tuple[date, str], dict[str, Decimal]] = defaultdict(
        lambda: {c.value: _DEC_ZERO for c in _AGG_CATEGORIES}
    )
    for posted_day, marketplace_id, category, total in result.all():
        bucket = totals[(posted_day, marketplace_id)]
        bucket[category] = Decimal(str(total))
    return totals


async def _aggregate_net_units(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
    marketplace_ids: Iterable[str],
) -> dict[tuple[date, str, str], int]:
    """Net units sold per (date, marketplace, sku).

    Counts quantity ONCE per shipment item via the Principal-charge row,
    then subtracts the Principal-charge quantity from refund events.
    Other fee_type rows for the same shipment item duplicate quantity, so
    we filter to Principal-only.
    """
    date_col = cast(FinancialEvent.posted_date, Date).label("posted_day")
    stmt = (
        select(
            date_col,
            FinancialEvent.marketplace_id,
            FinancialEvent.sku,
            FinancialEvent.event_type,
            func.coalesce(func.sum(FinancialEvent.quantity), 0),
        )
        .where(
            and_(
                FinancialEvent.marketplace_id.in_(list(marketplace_ids)),
                cast(FinancialEvent.posted_date, Date) >= start_date,
                cast(FinancialEvent.posted_date, Date) <= end_date,
                FinancialEvent.fee_type == "Principal",
                FinancialEvent.sku.is_not(None),
                FinancialEvent.event_type.in_(["ShipmentEvent", "RefundEvent"]),
            )
        )
        .group_by(date_col, FinancialEvent.marketplace_id, FinancialEvent.sku, FinancialEvent.event_type)
    )
    result = await session.execute(stmt)
    net: dict[tuple[date, str, str], int] = defaultdict(int)
    for posted_day, marketplace_id, sku, event_type, qty in result.all():
        delta = int(qty) if event_type == "ShipmentEvent" else -int(qty)
        net[(posted_day, marketplace_id, sku)] += delta
    return net


async def _load_active_cogs(
    session: AsyncSession,
    *,
    cogs_keys: Iterable[str],
) -> dict[tuple[str, str], Decimal]:
    """Most-recent active unit_cost per (marketplace_key, sku).

    For an MVP this returns the latest effective_date row for each SKU; a
    point-in-time-aware variant can land later once we have multi-version COGS.
    """
    stmt = select(
        ProductCogs.marketplace,
        ProductCogs.sku,
        ProductCogs.unit_cost,
        ProductCogs.effective_date,
        ProductCogs.status,
    ).where(ProductCogs.marketplace.in_(list(cogs_keys)))
    result = await session.execute(stmt)
    latest: dict[tuple[str, str], tuple[date, Decimal]] = {}
    for marketplace_key, sku, unit_cost, effective_date, status in result.all():
        if status != "active":
            continue
        existing = latest.get((marketplace_key, sku))
        if existing is None or effective_date > existing[0]:
            latest[(marketplace_key, sku)] = (effective_date, Decimal(str(unit_cost)))
    return {key: cost for key, (_, cost) in latest.items()}


async def _aggregate_ad_spend(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
    marketplace_ids: Iterable[str],
) -> dict[tuple[date, str], dict[str, Decimal]]:
    """Sum spend per (date, marketplace, platform) for the window.

    Returns: {(day, marketplace_id): {platform_column: spend, ...}}
    where platform_column is the daily_pnl column name (ad_spend_sp, ...).
    Unknown platforms are silently ignored (e.g. 'meta' from PR 8 lands in
    daily_pnl via a different path).
    """
    short_codes = [
        code for code, mp in SHORT_CODE_TO_MARKETPLACE.items() if mp in marketplace_ids
    ]
    if not short_codes:
        return {}
    stmt = (
        select(
            AdSpend.date,
            AdSpend.marketplace,
            AdSpend.platform,
            func.coalesce(func.sum(AdSpend.spend), 0),
        )
        .where(
            and_(
                AdSpend.marketplace.in_(short_codes),
                AdSpend.platform.in_(list(AD_SPEND_PLATFORM_TO_COLUMN.keys())),
                AdSpend.date >= start_date,
                AdSpend.date <= end_date,
            )
        )
        .group_by(AdSpend.date, AdSpend.marketplace, AdSpend.platform)
    )
    result = await session.execute(stmt)
    out: dict[tuple[date, str], dict[str, Decimal]] = defaultdict(
        lambda: {col: _DEC_ZERO for col in AD_SPEND_PLATFORM_TO_COLUMN.values()}
    )
    for day, short_code, platform, total in result.all():
        marketplace_id = SHORT_CODE_TO_MARKETPLACE.get(short_code)
        if marketplace_id is None:
            continue
        column = AD_SPEND_PLATFORM_TO_COLUMN[platform]
        out[(day, marketplace_id)][column] = Decimal(str(total))
    return out


async def _load_fx_rates(
    session: AsyncSession,
    *,
    currencies: Iterable[str],
) -> dict[str, Decimal]:
    """Most-recent {from_currency}->USD rate per currency. USD is always 1."""
    non_usd = [c for c in currencies if c and c != "USD"]
    rates: dict[str, Decimal] = {"USD": Decimal("1")}
    if not non_usd:
        return rates
    stmt = select(
        CurrencyRate.from_currency,
        CurrencyRate.rate,
        CurrencyRate.effective_date,
    ).where(
        and_(
            CurrencyRate.from_currency.in_(non_usd),
            CurrencyRate.to_currency == "USD",
        )
    )
    result = await session.execute(stmt)
    latest: dict[str, tuple[date, Decimal]] = {}
    for from_currency, rate, effective_date in result.all():
        existing = latest.get(from_currency)
        if existing is None or effective_date > existing[0]:
            latest[from_currency] = (effective_date, Decimal(str(rate)))
    for currency, (_, rate) in latest.items():
        rates[currency] = rate
    return rates


async def calculate_daily_pnl(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
    marketplace_ids: Iterable[str] | None = None,
) -> PnlCalcSummary:
    marketplaces = list(marketplace_ids) if marketplace_ids else list(MARKETPLACE_REGION.keys())
    summary = PnlCalcSummary()

    category_totals = await _aggregate_categories(
        session, start_date=start_date, end_date=end_date, marketplace_ids=marketplaces
    )
    net_units = await _aggregate_net_units(
        session, start_date=start_date, end_date=end_date, marketplace_ids=marketplaces
    )
    ad_spend_totals = await _aggregate_ad_spend(
        session, start_date=start_date, end_date=end_date, marketplace_ids=marketplaces
    )

    cogs_keys = {MARKETPLACE_COGS_KEY[m] for m in marketplaces}
    cogs_table = await _load_active_cogs(session, cogs_keys=cogs_keys)

    currencies = {MARKETPLACE_CURRENCY[m] for m in marketplaces}
    fx_rates = await _load_fx_rates(session, currencies=currencies)

    # Group net_units by (date, marketplace) for COGS aggregation.
    cogs_per_dm: dict[tuple[date, str], Decimal] = defaultdict(lambda: _DEC_ZERO)
    missing_cogs: set[str] = set()
    for (day, marketplace, sku), units in net_units.items():
        if units == 0:
            continue
        cogs_key = MARKETPLACE_COGS_KEY[marketplace]
        unit_cost = cogs_table.get((cogs_key, sku))
        if unit_cost is None:
            if sku not in missing_cogs:
                missing_cogs.add(sku)
                logger.warning(
                    "pnl_calculator missing_cogs sku=%s marketplace=%s", sku, marketplace
                )
            continue
        cogs_per_dm[(day, marketplace)] += unit_cost * Decimal(units)
    summary.skus_without_cogs = sorted(missing_cogs)

    # Build the set of (day, marketplace) keys to write -- union of all
    # source-data keys. A day with only ad spend (or only COGS) still gets
    # a row so the spend isn't lost; SP-API fields stay at 0 until the
    # next financial-events ETL run.
    days_marketplaces: set[tuple[date, str]] = (
        set(category_totals.keys())
        | set(cogs_per_dm.keys())
        | set(ad_spend_totals.keys())
    )

    for day, marketplace_id in sorted(days_marketplaces):
        cats = category_totals.get((day, marketplace_id), {})
        sales = Decimal(str(cats.get(PnlCategory.SALES.value, _DEC_ZERO)))
        selling_fees = Decimal(str(cats.get(PnlCategory.SELLING_FEES.value, _DEC_ZERO)))
        operational_fees = Decimal(str(cats.get(PnlCategory.OPERATIONAL_FEES.value, _DEC_ZERO)))
        refunds = Decimal(str(cats.get(PnlCategory.REFUNDS.value, _DEC_ZERO)))
        reimbursements = Decimal(str(cats.get(PnlCategory.REIMBURSEMENTS.value, _DEC_ZERO)))
        cogs = cogs_per_dm.get((day, marketplace_id), _DEC_ZERO)

        ad_spend_cols = ad_spend_totals.get((day, marketplace_id), {})
        ad_spend_sp = ad_spend_cols.get("ad_spend_sp", _DEC_ZERO)
        ad_spend_sb = ad_spend_cols.get("ad_spend_sb", _DEC_ZERO)
        ad_spend_sd = ad_spend_cols.get("ad_spend_sd", _DEC_ZERO)
        ad_spend_sv = ad_spend_cols.get("ad_spend_sv", _DEC_ZERO)
        ad_spend = ad_spend_sp + ad_spend_sb + ad_spend_sd + ad_spend_sv

        gross_profit_no_reimb = (
            sales - cogs - ad_spend - selling_fees - operational_fees - refunds
        )
        gross_profit_with_reimb = gross_profit_no_reimb + reimbursements
        margin_pct = (
            (gross_profit_no_reimb / sales * _DEC_HUNDRED)
            if sales != _DEC_ZERO
            else _DEC_ZERO
        )

        currency = MARKETPLACE_CURRENCY[marketplace_id]
        fx_rate = fx_rates.get(currency, Decimal("1"))
        sales_usd = sales * fx_rate
        gross_profit_usd = gross_profit_no_reimb * fx_rate

        values = dict(
            date=day,
            channel=MARKETPLACE_CHANNEL[marketplace_id],
            currency=currency,
            sales=_round_money(sales),
            cogs=_round_money(cogs),
            ad_spend=_round_money(ad_spend),
            ad_spend_sp=_round_money(ad_spend_sp),
            ad_spend_sb=_round_money(ad_spend_sb),
            ad_spend_sd=_round_money(ad_spend_sd),
            ad_spend_sv=_round_money(ad_spend_sv),
            selling_fees=_round_money(selling_fees),
            operational_fees=_round_money(operational_fees),
            refunds=_round_money(refunds),
            reimbursements=_round_money(reimbursements),
            gross_profit_no_reimb=_round_money(gross_profit_no_reimb),
            gross_profit_with_reimb=_round_money(gross_profit_with_reimb),
            margin_pct=margin_pct.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
            sales_usd=_round_money(sales_usd),
            gross_profit_usd=_round_money(gross_profit_usd),
            fx_rate=fx_rate,
        )

        stmt = insert(DailyPnL).values(**values)
        update_cols = {
            c: stmt.excluded[c]
            for c in values
            if c not in ("date", "channel")
        }
        # updated_at: bump explicitly since ON CONFLICT bypasses ORM hooks.
        update_cols["updated_at"] = func.now()
        stmt = stmt.on_conflict_do_update(
            constraint="uq_daily_pnl_date_channel",
            set_=update_cols,
        )
        await session.execute(stmt)
        summary.rows_written += 1

    await session.flush()
    return summary


__all__ = ["PnlCalcSummary", "calculate_daily_pnl"]
