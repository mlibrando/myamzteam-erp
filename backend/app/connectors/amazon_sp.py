from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Literal

import httpx

from app.config import settings
from app.connectors.base import (
    AuthError,
    BaseConnector,
    RateLimit,
    RateLimiter,
)

logger = logging.getLogger(__name__)

Region = Literal["NA", "EU", "FE"]

LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"

REGION_BASE_URLS: dict[Region, str] = {
    "NA": "https://sellingpartnerapi-na.amazon.com",
    "EU": "https://sellingpartnerapi-eu.amazon.com",
    "FE": "https://sellingpartnerapi-fe.amazon.com",
}

MARKETPLACE_REGION: dict[str, Region] = {
    "ATVPDKIKX0DER": "NA",  # US
    "A2EUQ1WTGCTBG2": "NA",  # CA
    "A1AM78C64UM0Y8": "NA",  # MX
    "A1F83G8C2ARO7P": "EU",  # UK
    "A39IBJ37TRP1C6": "FE",  # AU
}

MARKETPLACE_CHANNEL: dict[str, str] = {
    "ATVPDKIKX0DER": "amazon_us",
    "A2EUQ1WTGCTBG2": "amazon_ca",
    "A1AM78C64UM0Y8": "amazon_mx",
    "A1F83G8C2ARO7P": "amazon_uk",
    "A39IBJ37TRP1C6": "amazon_au",
}

MARKETPLACE_CURRENCY: dict[str, str] = {
    "ATVPDKIKX0DER": "USD",
    "A2EUQ1WTGCTBG2": "CAD",
    "A1AM78C64UM0Y8": "MXN",
    "A1F83G8C2ARO7P": "GBP",
    "A39IBJ37TRP1C6": "AUD",
}

# Inverse of MARKETPLACE_CURRENCY. SP-API ServiceFee + Adjustment events
# often arrive without a MarketplaceName but always with a CurrencyCode, so
# currency is a reliable second-best attribution signal.
CURRENCY_TO_MARKETPLACE: dict[str, str] = {v: k for k, v in MARKETPLACE_CURRENCY.items()}

# SP-API embeds MarketplaceName (not Id) in most events. This maps the
# display-name strings back to canonical marketplace IDs so the ETL can
# attribute region-shared events to the right marketplace.
MARKETPLACE_NAME_TO_ID: dict[str, str] = {
    "Amazon.com": "ATVPDKIKX0DER",
    "Amazon.ca": "A2EUQ1WTGCTBG2",
    "Amazon.com.mx": "A1AM78C64UM0Y8",
    "Amazon.co.uk": "A1F83G8C2ARO7P",
    "Amazon.com.au": "A39IBJ37TRP1C6",
}

# For events with no MarketplaceName (region-level subscription / storage),
# attribute to the region's primary marketplace.
REGION_PRIMARY_MARKETPLACE: dict[Region, str] = {
    "NA": "ATVPDKIKX0DER",
    "EU": "A1F83G8C2ARO7P",
    "FE": "A39IBJ37TRP1C6",
}

SALES_RATE = RateLimit(requests_per_second=0.5)
FINANCES_RATE = RateLimit(requests_per_second=0.5)
REPORTS_RATE = RateLimit(requests_per_second=0.0167)

# Refresh the LWA token 60s before its stated expiry to absorb clock skew
# and avoid mid-flight 401s on long-running ETL jobs.
TOKEN_REFRESH_SKEW_SECONDS = 60


@dataclass
class _CachedToken:
    access_token: str
    expires_at: float  # monotonic seconds

    def is_expired(self) -> bool:
        return time.monotonic() >= self.expires_at - TOKEN_REFRESH_SKEW_SECONDS


class AmazonSPConnector(BaseConnector):
    """Amazon Selling Partner API connector.

    One instance is bound to a single SP-API region (NA / EU / FE). Each region
    uses its own refresh token and base URL but the same LWA client credentials.
    Construct separate instances per region for cross-region ETL runs.
    """

    def __init__(
        self,
        region: Region = "NA",
        *,
        client_id: str | None = None,
        client_secret: str | None = None,
        refresh_token: str | None = None,
        client: httpx.AsyncClient | None = None,
        rate_limiter: RateLimiter | None = None,
        timeout: float = 30.0,
    ) -> None:
        super().__init__(client=client, rate_limiter=rate_limiter, timeout=timeout)
        if region not in REGION_BASE_URLS:
            raise ValueError(f"unknown SP-API region: {region}")
        self.region: Region = region
        self.base_url = REGION_BASE_URLS[region]
        self._client_id = client_id or settings.AMAZON_SP_CLIENT_ID
        self._client_secret = client_secret or settings.AMAZON_SP_CLIENT_SECRET
        self._refresh_token = refresh_token or self._refresh_token_for(region)
        if not (self._client_id and self._client_secret and self._refresh_token):
            raise AuthError(f"Amazon SP credentials not configured for region {region}")
        self._token: _CachedToken | None = None
        self._token_lock = asyncio.Lock()

    @staticmethod
    def _refresh_token_for(region: Region) -> str:
        return {
            "NA": settings.AMAZON_SP_REFRESH_TOKEN_NA,
            "EU": settings.AMAZON_SP_REFRESH_TOKEN_EU,
            "FE": settings.AMAZON_SP_REFRESH_TOKEN_FE,
        }[region]

    @classmethod
    def for_marketplace(cls, marketplace_id: str, **kwargs: Any) -> "AmazonSPConnector":
        try:
            region = MARKETPLACE_REGION[marketplace_id]
        except KeyError as exc:
            raise ValueError(f"unknown marketplace_id: {marketplace_id}") from exc
        return cls(region=region, **kwargs)

    async def _auth_headers(self, *, force_refresh: bool = False) -> dict[str, str]:
        token = await self._get_access_token(force_refresh=force_refresh)
        return {"x-amz-access-token": token}

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
        logger.info("amazon_sp lwa_token_exchange region=%s", self.region)
        response = await self._client.post(LWA_TOKEN_URL, data=payload)
        if response.status_code != 200:
            raise AuthError(
                f"LWA token exchange failed region={self.region} "
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

    # ---- Sales API ------------------------------------------------------------

    async def get_order_metrics(
        self,
        marketplace_id: str,
        start_date: str,
        end_date: str,
        *,
        granularity: str = "Day",
    ) -> dict[str, Any]:
        """GET /sales/v1/orderMetrics.

        start_date / end_date are ISO-8601 strings (e.g. "2026-06-17T00:00:00-00:00").
        The Sales API requires a timezone offset on both ends.
        """
        params = {
            "marketplaceIds": marketplace_id,
            "interval": f"{start_date}--{end_date}",
            "granularity": granularity,
        }
        response = await self.request(
            "GET",
            "/sales/v1/orderMetrics",
            endpoint_key=f"sales:{self.region}",
            rate_limit=SALES_RATE,
            params=params,
        )
        return response.json()

    # ---- Finances API ---------------------------------------------------------

    async def get_financial_event_groups(
        self,
        start_date: str,
        end_date: str | None = None,
        *,
        max_results_per_page: int = 100,
    ) -> list[dict[str, Any]]:
        """GET /finances/v0/financialEventGroups, paginated to exhaustion.

        Returns the merged FinancialEventGroupList across all pages. Preserves the
        full upstream group structure so the PR 3 ETL can map any new fields.
        """
        params: dict[str, Any] = {
            "FinancialEventGroupStartedAfter": start_date,
            "MaxResultsPerPage": max_results_per_page,
        }
        if end_date is not None:
            params["FinancialEventGroupStartedBefore"] = end_date

        groups: list[dict[str, Any]] = []
        next_token: str | None = None
        while True:
            call_params = dict(params)
            if next_token is not None:
                call_params = {"NextToken": next_token}
            response = await self.request(
                "GET",
                "/finances/v0/financialEventGroups",
                endpoint_key=f"finances:{self.region}",
                rate_limit=FINANCES_RATE,
                params=call_params,
            )
            body = response.json()
            payload = body.get("payload", {})
            groups.extend(payload.get("FinancialEventGroupList", []))
            next_token = payload.get("NextToken")
            if not next_token:
                break
        return groups

    async def get_financial_events(
        self,
        event_group_id: str,
        *,
        max_results_per_page: int = 100,
        posted_after: str | None = None,
        posted_before: str | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """GET /finances/v0/financialEvents/{eventGroupId}, paginated to exhaustion.

        Requires the SP-API "Finance and Accounting" data role on the LWA app and
        only serves groups whose ProcessingStatus is Closed (Open groups 403).
        For day-by-day P&L use `get_financial_events_by_date` instead.
        """
        path = f"/finances/v0/financialEvents/{event_group_id}"
        base_params: dict[str, Any] = {"MaxResultsPerPage": max_results_per_page}
        if posted_after is not None:
            base_params["PostedAfter"] = posted_after
        if posted_before is not None:
            base_params["PostedBefore"] = posted_before
        return await self._paginate_financial_events(path, base_params)

    async def get_financial_events_by_date(
        self,
        posted_after: str,
        posted_before: str | None = None,
        *,
        max_results_per_page: int = 100,
    ) -> dict[str, list[dict[str, Any]]]:
        """GET /finances/v0/financialEvents, paginated to exhaustion.

        Date-range list of all financial events posted in the window, regardless
        of which event group they belong to. This is the daily-ETL driver — for
        a daily P&L you query yesterday's window and aggregate, no group-status
        coordination needed. Returns the merged FinancialEvents object keyed by
        event-type list name (ShipmentEventList, RefundEventList,
        ServiceFeeEventList, ...). Full per-event structure is preserved so PR 3
        can map fee breakdowns into the Selling Fees / Operational Fees /
        Refunds / Reimbursements categories defined in PNL_MAPPING.md.
        """
        base_params: dict[str, Any] = {
            "PostedAfter": posted_after,
            "MaxResultsPerPage": max_results_per_page,
        }
        if posted_before is not None:
            base_params["PostedBefore"] = posted_before
        return await self._paginate_financial_events(
            "/finances/v0/financialEvents", base_params
        )

    async def _paginate_financial_events(
        self,
        path: str,
        base_params: dict[str, Any],
    ) -> dict[str, list[dict[str, Any]]]:
        merged: dict[str, list[dict[str, Any]]] = {}
        next_token: str | None = None
        while True:
            call_params = dict(base_params)
            if next_token is not None:
                # SP-API rejects extra params when NextToken is supplied; the
                # next page must be requested with NextToken alone.
                call_params = {"NextToken": next_token}
            response = await self.request(
                "GET",
                path,
                endpoint_key=f"finances:{self.region}",
                rate_limit=FINANCES_RATE,
                params=call_params,
            )
            body = response.json()
            payload = body.get("payload", {})
            events = payload.get("FinancialEvents", {})
            for key, value in events.items():
                if isinstance(value, list):
                    merged.setdefault(key, []).extend(value)
            next_token = payload.get("NextToken")
            if not next_token:
                break
        return merged
