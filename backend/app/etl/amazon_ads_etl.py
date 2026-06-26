"""Amazon Advertising ETL: per marketplace, run SP+SB+SD reports in parallel,
parse the gzipped JSON, persist to ad_spend, and refresh daily_pnl ad_spend*
columns via the existing pnl_calculator.

Idempotency: deletes existing ad_spend rows for each (date_range, marketplace,
platform) tuple before inserting, so re-running for the same window replaces
the prior data exactly.

Daily_pnl: this ETL does NOT directly write daily_pnl. After loading ad_spend
rows it calls `calculate_daily_pnl`, which reads the ad_spend table as part
of its aggregation and upserts ad_spend / ad_spend_sp / ad_spend_sb /
ad_spend_sd / gross_profit_* / margin_pct accordingly.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import and_, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.amazon_ads import (
    AdsProfile,
    AmazonAdsConnector,
    MARKETPLACE_REGION,
    REPORT_POLL_INTERVAL_SECONDS,
    REPORT_POLL_MAX_SECONDS,
    Region,
)
from app.models import AdSpend

logger = logging.getLogger(__name__)

ALL_MARKETPLACES: tuple[str, ...] = tuple(MARKETPLACE_REGION.keys())

MARKETPLACE_CHANNEL: dict[str, str] = {
    "ATVPDKIKX0DER": "amazon_us",
    "A2EUQ1WTGCTBG2": "amazon_ca",
    "A1AM78C64UM0Y8": "amazon_mx",
    "A1F83G8C2ARO7P": "amazon_uk",
    "A39IBJ37TRP1C6": "amazon_au",
}
MARKETPLACE_SHORT_CODE: dict[str, str] = {
    "ATVPDKIKX0DER": "us",
    "A2EUQ1WTGCTBG2": "ca",
    "A1AM78C64UM0Y8": "mx",
    "A1F83G8C2ARO7P": "uk",
    "A39IBJ37TRP1C6": "au",
}
MARKETPLACE_CURRENCY: dict[str, str] = {
    "ATVPDKIKX0DER": "USD",
    "A2EUQ1WTGCTBG2": "CAD",
    "A1AM78C64UM0Y8": "MXN",
    "A1F83G8C2ARO7P": "GBP",
    "A39IBJ37TRP1C6": "AUD",
}

AD_PRODUCT_TO_PLATFORM: dict[str, str] = {
    "SPONSORED_PRODUCTS": "amazon_sp",
    "SPONSORED_BRANDS": "amazon_sb",
    "SPONSORED_DISPLAY": "amazon_sd",
}

# All three ad products report `cost`, `impressions`, `clicks`. They differ on
# how `sales` is reported: SP exposes `sales1d/7d/14d` (we use 7d to match the
# default Amazon Ads console attribution window), SB+SD expose `sales`.
SALES_FIELD_BY_AD_PRODUCT: dict[str, str] = {
    "SPONSORED_PRODUCTS": "sales7d",
    "SPONSORED_BRANDS": "sales",
    "SPONSORED_DISPLAY": "sales",
}
PURCHASES_FIELD_BY_AD_PRODUCT: dict[str, str] = {
    "SPONSORED_PRODUCTS": "purchases7d",
    "SPONSORED_BRANDS": "purchases",
    "SPONSORED_DISPLAY": "purchases",
}


@dataclass
class AmazonAdsEtlSummary:
    start_date: date
    end_date: date
    marketplace_ids: list[str] = field(default_factory=list)
    profiles_resolved: list[dict[str, str]] = field(default_factory=list)
    reports_created: int = 0
    reports_completed: int = 0
    reports_failed: list[str] = field(default_factory=list)
    rows_inserted: int = 0
    rows_by_platform: dict[str, int] = field(default_factory=dict)
    spend_by_platform: dict[str, float] = field(default_factory=dict)
    skipped_marketplaces: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "marketplace_ids": list(self.marketplace_ids),
            "profiles_resolved": list(self.profiles_resolved),
            "reports_created": self.reports_created,
            "reports_completed": self.reports_completed,
            "reports_failed": list(self.reports_failed),
            "rows_inserted": self.rows_inserted,
            "rows_by_platform": dict(self.rows_by_platform),
            "spend_by_platform": {k: round(v, 2) for k, v in self.spend_by_platform.items()},
            "skipped_marketplaces": list(self.skipped_marketplaces),
        }


def _to_decimal(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_report_date(value: str | None) -> date | None:
    if not value:
        return None
    # Ads reports use YYYY-MM-DD; tolerate ISO-format leftovers.
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return None


def _build_ad_spend_row(
    row: dict[str, Any],
    *,
    ad_product: str,
    marketplace_id: str,
    currency: str,
) -> AdSpend | None:
    spend_date = _parse_report_date(row.get("date"))
    if spend_date is None:
        return None
    platform = AD_PRODUCT_TO_PLATFORM[ad_product]
    spend = _to_decimal(row.get("cost"))
    sales = _to_decimal(row.get(SALES_FIELD_BY_AD_PRODUCT[ad_product]))
    impressions = _to_int(row.get("impressions"))
    clicks = _to_int(row.get("clicks"))
    # ACoS = cost / sales * 100; capped at 999.99 to fit DECIMAL(5,2).
    acos: Decimal | None
    if sales > Decimal("0"):
        raw_acos = spend / sales * Decimal("100")
        capped = max(Decimal("-999.99"), min(Decimal("999.99"), raw_acos))
        acos = capped.quantize(Decimal("0.01"))
    else:
        acos = None
    return AdSpend(
        date=spend_date,
        platform=platform,
        campaign_id=str(row.get("campaignId")) if row.get("campaignId") is not None else None,
        campaign_name=row.get("campaignName"),
        marketplace=MARKETPLACE_SHORT_CODE.get(marketplace_id),
        spend=spend,
        sales_attributed=sales if sales > 0 else None,
        impressions=impressions,
        clicks=clicks,
        acos=acos,
        currency=currency,
        raw_payload=row,
    )


async def _purge_existing(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
    marketplace_id: str,
    platforms: Iterable[str],
) -> None:
    short = MARKETPLACE_SHORT_CODE[marketplace_id]
    await session.execute(
        delete(AdSpend).where(
            and_(
                AdSpend.marketplace == short,
                AdSpend.platform.in_(list(platforms)),
                AdSpend.date >= start_date,
                AdSpend.date <= end_date,
            )
        )
    )


async def _run_ad_product(
    conn: AmazonAdsConnector,
    *,
    ad_product: str,
    start_date: date,
    end_date: date,
    poll_interval: float,
    max_seconds: float,
) -> list[dict[str, Any]]:
    return await conn.run_campaign_report(
        ad_product=ad_product,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        poll_interval=poll_interval,
        max_seconds=max_seconds,
    )


async def _process_marketplace(
    session: AsyncSession,
    *,
    profile_scoped_conn: AmazonAdsConnector,
    profile: AdsProfile,
    marketplace_id: str,
    start_date: date,
    end_date: date,
    summary: AmazonAdsEtlSummary,
    ad_products: tuple[str, ...],
    poll_interval: float,
    max_seconds: float,
) -> None:
    platforms = [AD_PRODUCT_TO_PLATFORM[p] for p in ad_products]
    await _purge_existing(
        session,
        start_date=start_date,
        end_date=end_date,
        marketplace_id=marketplace_id,
        platforms=platforms,
    )
    summary.reports_created += len(ad_products)
    # Fire all three reports in parallel.
    tasks = [
        _run_ad_product(
            profile_scoped_conn,
            ad_product=ad_product,
            start_date=start_date,
            end_date=end_date,
            poll_interval=poll_interval,
            max_seconds=max_seconds,
        )
        for ad_product in ad_products
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    currency = profile.currency_code or MARKETPLACE_CURRENCY[marketplace_id]

    for ad_product, result in zip(ad_products, results, strict=True):
        platform = AD_PRODUCT_TO_PLATFORM[ad_product]
        if isinstance(result, Exception):
            summary.reports_failed.append(
                f"{marketplace_id}:{ad_product}:{type(result).__name__}:{result}"
            )
            logger.warning(
                "amazon_ads_etl report_failed marketplace=%s ad_product=%s error=%s",
                marketplace_id, ad_product, result,
            )
            continue
        summary.reports_completed += 1
        rows = result or []
        platform_rows = 0
        platform_spend = Decimal("0")
        for row in rows:
            obj = _build_ad_spend_row(
                row,
                ad_product=ad_product,
                marketplace_id=marketplace_id,
                currency=currency,
            )
            if obj is None:
                continue
            session.add(obj)
            platform_rows += 1
            platform_spend += obj.spend
        summary.rows_inserted += platform_rows
        summary.rows_by_platform[platform] = (
            summary.rows_by_platform.get(platform, 0) + platform_rows
        )
        summary.spend_by_platform[platform] = (
            summary.spend_by_platform.get(platform, 0.0) + float(platform_spend)
        )


async def run_amazon_ads_etl(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
    marketplace_ids: Iterable[str] | None = None,
    ad_products: tuple[str, ...] = (
        "SPONSORED_PRODUCTS",
        "SPONSORED_BRANDS",
        "SPONSORED_DISPLAY",
    ),
    connector_factory: Any = None,
    poll_interval: float = REPORT_POLL_INTERVAL_SECONDS,
    max_seconds: float = REPORT_POLL_MAX_SECONDS,
) -> AmazonAdsEtlSummary:
    marketplaces = tuple(marketplace_ids) if marketplace_ids else ALL_MARKETPLACES
    unknown = [m for m in marketplaces if m not in MARKETPLACE_REGION]
    if unknown:
        raise ValueError(f"unknown marketplace_ids: {unknown}")

    summary = AmazonAdsEtlSummary(
        start_date=start_date, end_date=end_date, marketplace_ids=list(marketplaces)
    )

    # Group requested marketplaces by region so we list profiles once per region.
    regions_needed: dict[Region, list[str]] = {}
    for mp in marketplaces:
        regions_needed.setdefault(MARKETPLACE_REGION[mp], []).append(mp)

    factory = connector_factory or (lambda region: AmazonAdsConnector(region=region))

    for region, region_marketplaces in regions_needed.items():
        async with factory(region) as conn:
            profiles = await conn.list_profiles()
            mp_to_profile: dict[str, AdsProfile] = {}
            for p in profiles:
                if p.marketplace_id and p.marketplace_id in region_marketplaces:
                    mp_to_profile[p.marketplace_id] = p

            for mp in region_marketplaces:
                profile = mp_to_profile.get(mp)
                if profile is None:
                    summary.skipped_marketplaces.append(mp)
                    logger.warning(
                        "amazon_ads_etl no_profile marketplace=%s region=%s", mp, region
                    )
                    continue
                summary.profiles_resolved.append(
                    {"marketplace_id": mp, "profile_id": profile.profile_id}
                )
                scoped = conn.for_profile(profile.profile_id)
                await _process_marketplace(
                    session,
                    profile_scoped_conn=scoped,
                    profile=profile,
                    marketplace_id=mp,
                    start_date=start_date,
                    end_date=end_date,
                    summary=summary,
                    ad_products=ad_products,
                    poll_interval=poll_interval,
                    max_seconds=max_seconds,
                )

    await session.flush()
    return summary


__all__ = [
    "ALL_MARKETPLACES",
    "AD_PRODUCT_TO_PLATFORM",
    "AmazonAdsEtlSummary",
    "MARKETPLACE_CHANNEL",
    "run_amazon_ads_etl",
]
