from __future__ import annotations

import time

import httpx
import pytest
import respx

from app.connectors.amazon_sp import (
    LWA_TOKEN_URL,
    REGION_BASE_URLS,
    AmazonSPConnector,
    _CachedToken,
)
from app.connectors.base import AuthError, RateLimitError


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


async def test_token_exchange_caches_and_injects_header() -> None:
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=True) as mock:
        token_route = _token_route(mock)
        metrics_route = mock.get(f"{base}/sales/v1/orderMetrics").mock(
            return_value=httpx.Response(200, json={"payload": []})
        )

        async with AmazonSPConnector(region="NA", **CREDS) as conn:
            await conn.get_order_metrics(
                marketplace_id="ATVPDKIKX0DER",
                start_date="2026-06-17T00:00:00-00:00",
                end_date="2026-06-18T00:00:00-00:00",
            )
            # Second call should reuse cached token, not re-exchange.
            await conn.get_order_metrics(
                marketplace_id="ATVPDKIKX0DER",
                start_date="2026-06-18T00:00:00-00:00",
                end_date="2026-06-19T00:00:00-00:00",
            )

        assert token_route.call_count == 1
        assert metrics_route.call_count == 2
        assert metrics_route.calls[0].request.headers["x-amz-access-token"] == "Atza|test-access"


async def test_401_triggers_token_refresh_then_succeeds() -> None:
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=True) as mock:
        token_route = mock.post(LWA_TOKEN_URL).mock(
            side_effect=[
                httpx.Response(200, json={"access_token": "stale", "expires_in": 3600}),
                httpx.Response(200, json={"access_token": "fresh", "expires_in": 3600}),
            ]
        )
        metrics_route = mock.get(f"{base}/sales/v1/orderMetrics").mock(
            side_effect=[
                httpx.Response(401, json={"errors": [{"code": "Unauthorized"}]}),
                httpx.Response(200, json={"payload": []}),
            ]
        )

        async with AmazonSPConnector(region="NA", **CREDS) as conn:
            await conn.get_order_metrics(
                marketplace_id="ATVPDKIKX0DER",
                start_date="2026-06-17T00:00:00-00:00",
                end_date="2026-06-18T00:00:00-00:00",
            )

        assert token_route.call_count == 2
        assert metrics_route.call_count == 2
        assert metrics_route.calls[1].request.headers["x-amz-access-token"] == "fresh"


async def test_429_retries_then_succeeds() -> None:
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=True) as mock:
        _token_route(mock)
        metrics_route = mock.get(f"{base}/sales/v1/orderMetrics").mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "0"}),
                httpx.Response(200, json={"payload": []}),
            ]
        )

        async with AmazonSPConnector(region="NA", **CREDS) as conn:
            await conn.get_order_metrics(
                marketplace_id="ATVPDKIKX0DER",
                start_date="2026-06-17T00:00:00-00:00",
                end_date="2026-06-18T00:00:00-00:00",
            )

        assert metrics_route.call_count == 2


async def test_429_exhausts_retries_raises() -> None:
    base = REGION_BASE_URLS["NA"]
    with respx.mock() as mock:
        _token_route(mock)
        mock.get(f"{base}/sales/v1/orderMetrics").mock(
            return_value=httpx.Response(429, headers={"Retry-After": "0"})
        )

        async with AmazonSPConnector(region="NA", **CREDS) as conn:
            conn.max_retries = 1
            conn.backoff_base = 0.0
            with pytest.raises(RateLimitError):
                await conn.get_order_metrics(
                    marketplace_id="ATVPDKIKX0DER",
                    start_date="2026-06-17T00:00:00-00:00",
                    end_date="2026-06-18T00:00:00-00:00",
                )


async def test_financial_event_groups_paginate() -> None:
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=True) as mock:
        _token_route(mock)
        page1 = {
            "payload": {
                "FinancialEventGroupList": [{"FinancialEventGroupId": "group-1"}],
                "NextToken": "tok-2",
            }
        }
        page2 = {
            "payload": {
                "FinancialEventGroupList": [
                    {"FinancialEventGroupId": "group-2"},
                    {"FinancialEventGroupId": "group-3"},
                ]
            }
        }
        groups_route = mock.get(f"{base}/finances/v0/financialEventGroups").mock(
            side_effect=[httpx.Response(200, json=page1), httpx.Response(200, json=page2)]
        )

        async with AmazonSPConnector(region="NA", **CREDS) as conn:
            groups = await conn.get_financial_event_groups(start_date="2026-05-01T00:00:00Z")

        assert [g["FinancialEventGroupId"] for g in groups] == ["group-1", "group-2", "group-3"]
        assert groups_route.call_count == 2
        # Second page must be requested with NextToken only.
        assert groups_route.calls[1].request.url.params["NextToken"] == "tok-2"


async def test_financial_events_paginate_and_merge_lists() -> None:
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=True) as mock:
        _token_route(mock)
        page1 = {
            "payload": {
                "FinancialEvents": {
                    "ShipmentEventList": [{"AmazonOrderId": "111-1"}],
                    "RefundEventList": [{"AmazonOrderId": "111-2"}],
                },
                "NextToken": "next",
            }
        }
        page2 = {
            "payload": {
                "FinancialEvents": {
                    "ShipmentEventList": [{"AmazonOrderId": "111-3"}],
                    "ServiceFeeEventList": [{"FeeReason": "FBAStorageFee"}],
                }
            }
        }
        events_route = mock.get(
            f"{base}/finances/v0/financialEventGroups/group-1/financialEvents"
        ).mock(
            side_effect=[httpx.Response(200, json=page1), httpx.Response(200, json=page2)]
        )

        async with AmazonSPConnector(region="NA", **CREDS) as conn:
            events = await conn.get_financial_events("group-1")

        assert events_route.call_count == 2
        assert [e["AmazonOrderId"] for e in events["ShipmentEventList"]] == ["111-1", "111-3"]
        assert events["RefundEventList"][0]["AmazonOrderId"] == "111-2"
        assert events["ServiceFeeEventList"][0]["FeeReason"] == "FBAStorageFee"


async def test_financial_events_by_date_paginate() -> None:
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=True) as mock:
        _token_route(mock)
        page1 = {
            "payload": {
                "FinancialEvents": {
                    "ShipmentEventList": [{"AmazonOrderId": "111-A"}],
                    "ServiceFeeEventList": [{"FeeReason": "FBAStorageFee"}],
                },
                "NextToken": "next-page",
            }
        }
        page2 = {
            "payload": {
                "FinancialEvents": {
                    "ShipmentEventList": [{"AmazonOrderId": "111-B"}],
                    "RefundEventList": [{"AmazonOrderId": "111-C"}],
                }
            }
        }
        events_route = mock.get(f"{base}/finances/v0/financialEvents").mock(
            side_effect=[httpx.Response(200, json=page1), httpx.Response(200, json=page2)]
        )

        async with AmazonSPConnector(region="NA", **CREDS) as conn:
            events = await conn.get_financial_events_by_date(
                posted_after="2026-06-01T00:00:00Z",
                posted_before="2026-06-08T00:00:00Z",
            )

        assert events_route.call_count == 2
        first_params = events_route.calls[0].request.url.params
        assert first_params["PostedAfter"] == "2026-06-01T00:00:00Z"
        assert first_params["PostedBefore"] == "2026-06-08T00:00:00Z"
        # Second page must use NextToken alone, not the date params.
        second_params = events_route.calls[1].request.url.params
        assert second_params["NextToken"] == "next-page"
        assert "PostedAfter" not in second_params
        assert [e["AmazonOrderId"] for e in events["ShipmentEventList"]] == ["111-A", "111-B"]
        assert events["ServiceFeeEventList"][0]["FeeReason"] == "FBAStorageFee"
        assert events["RefundEventList"][0]["AmazonOrderId"] == "111-C"


async def test_token_expiry_triggers_refresh() -> None:
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=True) as mock:
        token_route = mock.post(LWA_TOKEN_URL).mock(
            side_effect=[
                httpx.Response(200, json={"access_token": "first", "expires_in": 3600}),
                httpx.Response(200, json={"access_token": "second", "expires_in": 3600}),
            ]
        )
        metrics_route = mock.get(f"{base}/sales/v1/orderMetrics").mock(
            return_value=httpx.Response(200, json={"payload": []})
        )

        async with AmazonSPConnector(region="NA", **CREDS) as conn:
            await conn.get_order_metrics(
                marketplace_id="ATVPDKIKX0DER",
                start_date="2026-06-17T00:00:00-00:00",
                end_date="2026-06-18T00:00:00-00:00",
            )
            # Force the cached token to look expired.
            assert conn._token is not None
            conn._token = _CachedToken(access_token="first", expires_at=time.monotonic() - 1)
            await conn.get_order_metrics(
                marketplace_id="ATVPDKIKX0DER",
                start_date="2026-06-18T00:00:00-00:00",
                end_date="2026-06-19T00:00:00-00:00",
            )

        assert token_route.call_count == 2
        assert metrics_route.calls[1].request.headers["x-amz-access-token"] == "second"


async def test_region_selects_base_url_and_token() -> None:
    eu_base = REGION_BASE_URLS["EU"]
    with respx.mock(assert_all_called=True) as mock:
        token_route = _token_route(mock)
        events_route = mock.get(f"{eu_base}/finances/v0/financialEventGroups").mock(
            return_value=httpx.Response(200, json={"payload": {"FinancialEventGroupList": []}})
        )

        async with AmazonSPConnector(
            region="EU",
            client_id="cid",
            client_secret="csecret",
            refresh_token="Atzr|eu-token",
        ) as conn:
            await conn.get_financial_event_groups(start_date="2026-05-01T00:00:00Z")

        assert events_route.call_count == 1
        assert events_route.calls[0].request.url.host == "sellingpartnerapi-eu.amazon.com"
        # Verify the EU refresh token was sent to LWA, not NA's.
        assert b"Atzr%7Ceu-token" in token_route.calls[0].request.content


async def test_missing_credentials_raises() -> None:
    with pytest.raises(AuthError):
        AmazonSPConnector(
            region="NA",
            client_id="",
            client_secret="",
            refresh_token="",
        )


async def test_lwa_failure_raises_auth_error() -> None:
    with respx.mock() as mock:
        mock.post(LWA_TOKEN_URL).mock(
            return_value=httpx.Response(400, json={"error": "invalid_grant"})
        )
        async with AmazonSPConnector(region="NA", **CREDS) as conn:
            with pytest.raises(AuthError):
                await conn._get_access_token()
