"""Amazon Advertising API connector.

Wholly separate from SP-API:
  - Different LWA refresh token (`AMAZON_ADS_REFRESH_TOKEN_{NA,EU,FE}`)
  - Different base URL (`advertising-api*.amazon.com`)
  - `Authorization: Bearer …` header (vs SP-API's `x-amz-access-token`)
  - Per-request `Amazon-Advertising-API-ClientId` and
    `Amazon-Advertising-API-Scope` (profileId) headers
  - Async report flow: POST /reporting/reports -> poll -> download gzip
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any, Literal

import httpx

from app.config import settings
from app.connectors.base import (
    AuthError,
    BaseConnector,
    ConnectorError,
    RateLimit,
    RateLimiter,
)

logger = logging.getLogger(__name__)

Region = Literal["NA", "EU", "FE"]

LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"

REGION_BASE_URLS: dict[Region, str] = {
    "NA": "https://advertising-api.amazon.com",
    "EU": "https://advertising-api-eu.amazon.com",
    "FE": "https://advertising-api-fe.amazon.com",
}

# Same marketplace -> region mapping as SP-API. Sponsored Brands and Sponsored
# Display use the same regional split (NA/EU/FE) as Sponsored Products.
MARKETPLACE_REGION: dict[str, Region] = {
    "ATVPDKIKX0DER": "NA",  # US
    "A2EUQ1WTGCTBG2": "NA",  # CA
    "A1AM78C64UM0Y8": "NA",  # MX
    "A1F83G8C2ARO7P": "EU",  # UK
    "A39IBJ37TRP1C6": "FE",  # AU
}

# Profile.countryCode -> marketplaceId. /v2/profiles doesn't return
# marketplaceId directly; we map by countryCode.
COUNTRY_TO_MARKETPLACE: dict[str, str] = {
    "US": "ATVPDKIKX0DER",
    "CA": "A2EUQ1WTGCTBG2",
    "MX": "A1AM78C64UM0Y8",
    "UK": "A1F83G8C2ARO7P",
    "GB": "A1F83G8C2ARO7P",
    "AU": "A39IBJ37TRP1C6",
}

# Ads API publishes rough rate limits per endpoint. These are conservative
# defaults; the API responds with 429 + Retry-After when exceeded and the
# base connector handles that.
REPORT_CREATE_RATE = RateLimit(requests_per_second=2.0)
REPORT_POLL_RATE = RateLimit(requests_per_second=2.0)
PROFILE_RATE = RateLimit(requests_per_second=2.0)

TOKEN_REFRESH_SKEW_SECONDS = 60

REPORT_POLL_INTERVAL_SECONDS = 30
REPORT_POLL_MAX_SECONDS = 600

# Report API v3 (current). Each ad product has its own reportTypeId.
REPORT_TYPE_ID_BY_AD_PRODUCT: dict[str, str] = {
    "SPONSORED_PRODUCTS": "spCampaigns",
    "SPONSORED_BRANDS": "sbCampaigns",
    "SPONSORED_DISPLAY": "sdCampaigns",
}

# Standard campaign-level metrics for daily P&L use.
SP_METRICS: tuple[str, ...] = (
    "campaignId",
    "campaignName",
    "date",
    "cost",
    "sales1d",
    "sales7d",
    "sales14d",
    "impressions",
    "clicks",
    "purchases7d",
)
SB_METRICS: tuple[str, ...] = (
    "campaignId",
    "campaignName",
    "date",
    "cost",
    "sales",
    "impressions",
    "clicks",
    "purchases",
)
SD_METRICS: tuple[str, ...] = (
    "campaignId",
    "campaignName",
    "date",
    "cost",
    "sales",
    "impressions",
    "clicks",
    "purchases",
)
METRICS_BY_AD_PRODUCT: dict[str, tuple[str, ...]] = {
    "SPONSORED_PRODUCTS": SP_METRICS,
    "SPONSORED_BRANDS": SB_METRICS,
    "SPONSORED_DISPLAY": SD_METRICS,
}


# Regex for the Ads v3 duplicate-report message.
# Example body: {"code":"425","detail":"The Request is a duplicate of : <uuid>"}
_DUPLICATE_REPORT_ID_RE = re.compile(
    r"duplicate of\s*:\s*([0-9a-fA-F-]{8,})", re.IGNORECASE
)


def _extract_duplicate_report_id(body_text: str) -> str | None:
    """Parse the existing reportId out of a 425 response. Tolerates JSON
    decode failures (returns None) and minor wording variations."""
    # Try JSON first
    try:
        body = json.loads(body_text)
    except (TypeError, ValueError):
        body = None
    if isinstance(body, dict):
        # Some accounts surface the existing ID under a typed field.
        for key in ("duplicateReportId", "existingReportId", "reportId"):
            value = body.get(key)
            if value:
                return str(value)
        detail = body.get("detail") or body.get("message") or ""
        match = _DUPLICATE_REPORT_ID_RE.search(detail)
        if match:
            return match.group(1)
    # Fallback: raw text search in case body isn't JSON.
    match = _DUPLICATE_REPORT_ID_RE.search(body_text or "")
    if match:
        return match.group(1)
    return None


@dataclass
class _CachedToken:
    access_token: str
    expires_at: float  # monotonic seconds

    def is_expired(self) -> bool:
        return time.monotonic() >= self.expires_at - TOKEN_REFRESH_SKEW_SECONDS


@dataclass
class AdsProfile:
    profile_id: str
    country_code: str
    currency_code: str
    marketplace_id: str | None
    timezone: str | None
    account_name: str | None


class ReportFailed(ConnectorError):
    """Raised when an async report ends in FAILED state."""


class ReportPollTimeout(ConnectorError):
    """Raised when an async report doesn't complete within REPORT_POLL_MAX_SECONDS."""


class AmazonAdsConnector(BaseConnector):
    """One instance per region. Holds a region-bound token cache and
    optionally a scope (profileId). Use `for_profile(profile)` to derive
    a scoped instance for per-marketplace requests, or pass `profile_id`
    explicitly to each method."""

    def __init__(
        self,
        region: Region = "NA",
        *,
        client_id: str | None = None,
        client_secret: str | None = None,
        refresh_token: str | None = None,
        profile_id: str | None = None,
        client: httpx.AsyncClient | None = None,
        rate_limiter: RateLimiter | None = None,
        timeout: float = 60.0,
        token_cache: "_CachedToken | None" = None,
        token_lock: asyncio.Lock | None = None,
    ) -> None:
        super().__init__(client=client, rate_limiter=rate_limiter, timeout=timeout)
        if region not in REGION_BASE_URLS:
            raise ValueError(f"unknown Ads region: {region}")
        self.region: Region = region
        self.base_url = REGION_BASE_URLS[region]
        self._client_id = client_id or settings.AMAZON_ADS_CLIENT_ID
        self._client_secret = client_secret or settings.AMAZON_ADS_CLIENT_SECRET
        self._refresh_token = refresh_token or self._refresh_token_for(region)
        if not (self._client_id and self._client_secret and self._refresh_token):
            raise AuthError(
                f"Amazon Ads credentials not configured for region {region}"
            )
        self._profile_id = profile_id
        # Token state is shared across cloned instances (for_profile) so that
        # multiple profile-scoped connectors don't re-exchange tokens.
        self._token: _CachedToken | None = token_cache
        self._token_lock = token_lock or asyncio.Lock()

    @staticmethod
    def _refresh_token_for(region: Region) -> str:
        return {
            "NA": settings.AMAZON_ADS_REFRESH_TOKEN_NA,
            "EU": settings.AMAZON_ADS_REFRESH_TOKEN_EU,
            "FE": settings.AMAZON_ADS_REFRESH_TOKEN_FE,
        }[region]

    def for_profile(self, profile_id: str) -> "AmazonAdsConnector":
        """Return a new connector instance scoped to a profile, sharing the
        same underlying HTTP client and token cache."""
        clone = AmazonAdsConnector(
            region=self.region,
            client_id=self._client_id,
            client_secret=self._client_secret,
            refresh_token=self._refresh_token,
            profile_id=profile_id,
            client=self._client,
            rate_limiter=self._rate_limiter,
            token_cache=self._token,
            token_lock=self._token_lock,
        )
        # The clone borrowed the client; it must NOT close it on exit.
        clone._owns_client = False
        return clone

    async def _auth_headers(self, *, force_refresh: bool = False) -> dict[str, str]:
        token = await self._get_access_token(force_refresh=force_refresh)
        headers = {
            "Authorization": f"Bearer {token}",
            "Amazon-Advertising-API-ClientId": self._client_id,
        }
        if self._profile_id is not None:
            headers["Amazon-Advertising-API-Scope"] = str(self._profile_id)
        return headers

    async def _get_access_token(self, *, force_refresh: bool = False) -> str:
        async with self._token_lock:
            if not force_refresh and self._token is not None and not self._token.is_expired():
                return self._token.access_token
            self._token = await self._exchange_refresh_token()
            return self._token.access_token

    async def _exchange_refresh_token(self) -> _CachedToken:
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self._refresh_token,
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }
        logger.info("amazon_ads lwa_token_exchange region=%s", self.region)
        response = await self._client.post(LWA_TOKEN_URL, data=payload)
        if response.status_code != 200:
            raise AuthError(
                f"Ads LWA token exchange failed region={self.region} "
                f"status={response.status_code} body={response.text}"
            )
        body = response.json()
        access_token = body.get("access_token")
        expires_in = int(body.get("expires_in", 3600))
        if not access_token:
            raise AuthError(f"LWA response missing access_token: {body}")
        return _CachedToken(
            access_token=access_token,
            expires_at=time.monotonic() + expires_in,
        )

    # ---- Profiles -------------------------------------------------------------

    async def list_profiles(self) -> list[AdsProfile]:
        """GET /v2/profiles. Returns one profile per advertising account in
        the region, with countryCode that maps to a marketplaceId."""
        response = await self.request(
            "GET",
            "/v2/profiles",
            endpoint_key=f"profiles:{self.region}",
            rate_limit=PROFILE_RATE,
        )
        profiles = []
        for entry in response.json():
            country_code = entry.get("countryCode") or ""
            marketplace_id = COUNTRY_TO_MARKETPLACE.get(country_code)
            profiles.append(
                AdsProfile(
                    profile_id=str(entry.get("profileId")),
                    country_code=country_code,
                    currency_code=entry.get("currencyCode") or "",
                    marketplace_id=marketplace_id,
                    timezone=entry.get("timezone"),
                    account_name=(entry.get("accountInfo") or {}).get("name"),
                )
            )
        return profiles

    # ---- Reports --------------------------------------------------------------

    async def create_campaign_report(
        self,
        *,
        ad_product: str,
        start_date: str,
        end_date: str,
        name: str | None = None,
    ) -> str:
        """POST /reporting/reports. Returns reportId."""
        if ad_product not in REPORT_TYPE_ID_BY_AD_PRODUCT:
            raise ValueError(f"unknown ad_product: {ad_product}")
        report_type_id = REPORT_TYPE_ID_BY_AD_PRODUCT[ad_product]
        metrics = METRICS_BY_AD_PRODUCT[ad_product]
        # Include a short uuid in the default name so Amazon's create-report
        # dedup (which keys on name + configuration) never reuses a stale
        # report from a prior ETL invocation. Reports occasionally stay
        # PENDING indefinitely on Amazon's side; if we let dedup point us
        # back to one of those, the whole window's worth of reports hangs.
        # The 425 handler below still catches true within-invocation
        # duplicates (e.g. retried POSTs from network flakes).
        default_name = (
            f"{ad_product} campaigns {start_date} {end_date} "
            f"{uuid.uuid4().hex[:8]}"
        )
        body = {
            "name": name or default_name,
            "startDate": start_date,
            "endDate": end_date,
            "configuration": {
                "adProduct": ad_product,
                "groupBy": ["campaign"],
                "columns": list(metrics),
                "reportTypeId": report_type_id,
                "timeUnit": "DAILY",
                "format": "GZIP_JSON",
            },
        }
        response = await self.request(
            "POST",
            "/reporting/reports",
            endpoint_key=f"reports:{self.region}",
            rate_limit=REPORT_CREATE_RATE,
            json=body,
            headers={"Content-Type": "application/vnd.createasyncreportrequest.v3+json"},
            allowed_statuses={425},
        )
        if response.status_code == 425:
            # Amazon Ads v3 returns 425 when an identical report request
            # (same name + configuration) was submitted within the dedup
            # window. The existing reportId is embedded in the response
            # `detail` field. Reuse it rather than failing -- callers
            # expect idempotency over the (ad_product, window) tuple.
            existing_id = _extract_duplicate_report_id(response.text)
            if not existing_id:
                raise ConnectorError(
                    f"create_campaign_report got 425 but could not parse "
                    f"duplicate reportId from body: {response.text}"
                )
            logger.info(
                "amazon_ads report_deduped ad_product=%s existing_report_id=%s region=%s",
                ad_product,
                existing_id,
                self.region,
            )
            return existing_id

        data = response.json()
        report_id = data.get("reportId")
        if not report_id:
            raise ConnectorError(f"create_campaign_report missing reportId: {data}")
        logger.info(
            "amazon_ads report_created ad_product=%s report_id=%s region=%s",
            ad_product,
            report_id,
            self.region,
        )
        return report_id

    async def get_report_status(self, report_id: str) -> dict[str, Any]:
        response = await self.request(
            "GET",
            f"/reporting/reports/{report_id}",
            endpoint_key=f"reports:{self.region}",
            rate_limit=REPORT_POLL_RATE,
        )
        return response.json()

    async def wait_for_report(
        self,
        report_id: str,
        *,
        poll_interval: float = REPORT_POLL_INTERVAL_SECONDS,
        max_seconds: float = REPORT_POLL_MAX_SECONDS,
    ) -> dict[str, Any]:
        """Poll until report status is COMPLETED (returning status payload)
        or raise ReportFailed / ReportPollTimeout."""
        deadline = time.monotonic() + max_seconds
        while True:
            status = await self.get_report_status(report_id)
            state = (status.get("status") or "").upper()
            logger.info(
                "amazon_ads report_status report_id=%s status=%s region=%s",
                report_id,
                state,
                self.region,
            )
            if state == "COMPLETED":
                return status
            if state == "FAILED":
                raise ReportFailed(
                    f"report {report_id} FAILED: {status.get('failureReason')}"
                )
            if time.monotonic() >= deadline:
                raise ReportPollTimeout(
                    f"report {report_id} did not complete within {max_seconds}s "
                    f"(last status={state})"
                )
            await asyncio.sleep(poll_interval)

    async def download_report(self, url: str) -> list[dict[str, Any]]:
        """Download the gzipped JSON file at `url` and return parsed rows."""
        # The download URL is a signed S3 URL; do NOT inject Ads auth headers.
        response = await self._client.get(url)
        if response.status_code != 200:
            raise ConnectorError(
                f"report download failed status={response.status_code} url={url[:80]}..."
            )
        raw = response.content
        if not raw:
            return []
        # The response may already be decompressed by the server (some S3
        # signed URLs serve decompressed bodies depending on Accept-Encoding).
        # Detect gzip magic and decompress only if present.
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)

    async def run_campaign_report(
        self,
        *,
        ad_product: str,
        start_date: str,
        end_date: str,
        poll_interval: float = REPORT_POLL_INTERVAL_SECONDS,
        max_seconds: float = REPORT_POLL_MAX_SECONDS,
    ) -> list[dict[str, Any]]:
        """End-to-end: create -> poll -> download. Used by the ETL."""
        report_id = await self.create_campaign_report(
            ad_product=ad_product, start_date=start_date, end_date=end_date
        )
        status = await self.wait_for_report(
            report_id, poll_interval=poll_interval, max_seconds=max_seconds
        )
        url = status.get("url")
        if not url:
            raise ConnectorError(f"completed report {report_id} missing url: {status}")
        rows = await self.download_report(url)
        logger.info(
            "amazon_ads report_downloaded report_id=%s ad_product=%s rows=%d region=%s",
            report_id,
            ad_product,
            len(rows),
            self.region,
        )
        return rows


__all__ = [
    "AdsProfile",
    "AmazonAdsConnector",
    "COUNTRY_TO_MARKETPLACE",
    "MARKETPLACE_REGION",
    "REGION_BASE_URLS",
    "REPORT_POLL_INTERVAL_SECONDS",
    "REPORT_POLL_MAX_SECONDS",
    "REPORT_TYPE_ID_BY_AD_PRODUCT",
    "ReportFailed",
    "ReportPollTimeout",
]
