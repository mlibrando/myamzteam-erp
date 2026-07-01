"""Unit tests for the Amazon Advertising connector + ETL parsing layer.

Tests cover OAuth, profile discovery, async report polling, gzip vs plain
download decoding, per-ad-product row parsing, and the ETL summary shape.

DB-level idempotency for `run_amazon_ads_etl` is exercised by the live
validation script (`scripts/validate_amazon_ads_etl.py`) since it relies on
Postgres-specific DELETE + INSERT semantics not easily mocked in-process.
"""

from __future__ import annotations

import gzip
import json
from datetime import date
from decimal import Decimal

import httpx
import pytest
import respx

from app.connectors.amazon_ads import (
    AmazonAdsConnector,
    AdsProfile,
    LWA_TOKEN_URL,
    REGION_BASE_URLS,
    REPORT_TYPE_ID_BY_AD_PRODUCT,
    ReportFailed,
    ReportPollTimeout,
)
from app.connectors.base import AuthError
from app.etl.amazon_ads_etl import (
    AD_PRODUCT_TO_PLATFORM,
    AmazonAdsEtlSummary,
    _build_ad_spend_row,
)


CREDS = dict(
    client_id="amzn1.application-oa2-client.ads-test",
    client_secret="ads-test-secret",
    refresh_token="Atzr|ads-test-refresh",
)


def _token_route(mock: respx.Router, access_token: str = "Atza|ads-test-access") -> respx.Route:
    return mock.post(LWA_TOKEN_URL).mock(
        return_value=httpx.Response(
            200,
            json={"access_token": access_token, "token_type": "bearer", "expires_in": 3600},
        )
    )


# -----------------------------------------------------------------------------
# OAuth + headers
# -----------------------------------------------------------------------------


async def test_oauth_bearer_token_and_clientid_headers_injected():
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=True) as mock:
        token_route = _token_route(mock)
        profiles_route = mock.get(f"{base}/v2/profiles").mock(
            return_value=httpx.Response(200, json=[])
        )

        async with AmazonAdsConnector(region="NA", **CREDS) as conn:
            await conn.list_profiles()

        assert token_route.call_count == 1
        headers = profiles_route.calls[0].request.headers
        assert headers["Authorization"] == "Bearer Atza|ads-test-access"
        assert headers["Amazon-Advertising-API-ClientId"] == CREDS["client_id"]
        # No scope until for_profile() is called
        assert "Amazon-Advertising-API-Scope" not in headers


async def test_for_profile_clone_adds_scope_header_and_shares_token_cache():
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=True) as mock:
        token_route = _token_route(mock)
        report_route = mock.post(f"{base}/reporting/reports").mock(
            return_value=httpx.Response(200, json={"reportId": "rep-1"})
        )

        async with AmazonAdsConnector(region="NA", **CREDS) as conn:
            scoped = conn.for_profile("profile-1234")
            await scoped.create_campaign_report(
                ad_product="SPONSORED_PRODUCTS",
                start_date="2026-06-01",
                end_date="2026-06-07",
            )

        # Token exchanged exactly once even though the scoped clone makes
        # the first request -- token cache is shared.
        assert token_route.call_count == 1
        headers = report_route.calls[0].request.headers
        assert headers["Amazon-Advertising-API-Scope"] == "profile-1234"
        assert headers["Authorization"] == "Bearer Atza|ads-test-access"


async def test_401_triggers_token_refresh_then_succeeds():
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=True) as mock:
        token_route = mock.post(LWA_TOKEN_URL).mock(
            side_effect=[
                httpx.Response(200, json={"access_token": "stale", "expires_in": 3600}),
                httpx.Response(200, json={"access_token": "fresh", "expires_in": 3600}),
            ]
        )
        profiles_route = mock.get(f"{base}/v2/profiles").mock(
            side_effect=[
                httpx.Response(401, json={"message": "Unauthorized"}),
                httpx.Response(200, json=[]),
            ]
        )

        async with AmazonAdsConnector(region="NA", **CREDS) as conn:
            await conn.list_profiles()

        assert token_route.call_count == 2
        assert profiles_route.call_count == 2
        assert profiles_route.calls[1].request.headers["Authorization"] == "Bearer fresh"


async def test_missing_credentials_raises():
    with pytest.raises(AuthError):
        AmazonAdsConnector(
            region="NA", client_id="", client_secret="", refresh_token=""
        )


# -----------------------------------------------------------------------------
# Profiles
# -----------------------------------------------------------------------------


async def test_list_profiles_maps_country_codes_to_marketplaces():
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=True) as mock:
        _token_route(mock)
        mock.get(f"{base}/v2/profiles").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "profileId": 1111,
                        "countryCode": "US",
                        "currencyCode": "USD",
                        "timezone": "America/Los_Angeles",
                        "accountInfo": {"name": "MagicalButter US"},
                    },
                    {
                        "profileId": 2222,
                        "countryCode": "CA",
                        "currencyCode": "CAD",
                        "timezone": "America/Toronto",
                        "accountInfo": {"name": "MagicalButter CA"},
                    },
                    {
                        "profileId": 3333,
                        "countryCode": "ZZ",  # unknown country
                        "currencyCode": "USD",
                    },
                ],
            )
        )

        async with AmazonAdsConnector(region="NA", **CREDS) as conn:
            profiles = await conn.list_profiles()

        by_country = {p.country_code: p for p in profiles}
        assert by_country["US"].profile_id == "1111"
        assert by_country["US"].marketplace_id == "ATVPDKIKX0DER"
        assert by_country["CA"].profile_id == "2222"
        assert by_country["CA"].marketplace_id == "A2EUQ1WTGCTBG2"
        # Unknown country code -> marketplace_id None, still listed
        assert by_country["ZZ"].marketplace_id is None


# -----------------------------------------------------------------------------
# Async report flow
# -----------------------------------------------------------------------------


async def test_create_campaign_report_returns_report_id_and_uses_right_typeid():
    base = REGION_BASE_URLS["NA"]
    captured: dict = {}

    def _handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"reportId": "rep-abc"})

    with respx.mock(assert_all_called=True) as mock:
        _token_route(mock)
        mock.post(f"{base}/reporting/reports").mock(side_effect=_handler)

        async with AmazonAdsConnector(
            region="NA", profile_id="profile-X", **CREDS
        ) as conn:
            report_id = await conn.create_campaign_report(
                ad_product="SPONSORED_BRANDS",
                start_date="2026-06-01",
                end_date="2026-06-07",
            )

        assert report_id == "rep-abc"
        cfg = captured["body"]["configuration"]
        assert cfg["adProduct"] == "SPONSORED_BRANDS"
        assert cfg["reportTypeId"] == REPORT_TYPE_ID_BY_AD_PRODUCT["SPONSORED_BRANDS"]
        assert cfg["groupBy"] == ["campaign"]
        assert cfg["timeUnit"] == "DAILY"
        assert cfg["format"] == "GZIP_JSON"


async def test_create_report_handles_425_duplicate_by_reusing_id():
    """Amazon Ads returns 425 with {'code':'425','detail':'duplicate of : <uuid>'}
    when an identical create request was made in the last ~24h. The connector
    must extract that existing reportId and return it instead of failing."""
    base = REGION_BASE_URLS["NA"]
    existing_id = "7307043c-3eab-4f59-b981-609f42b98849"
    with respx.mock(assert_all_called=True) as mock:
        _token_route(mock)
        mock.post(f"{base}/reporting/reports").mock(
            return_value=httpx.Response(
                425,
                json={
                    "code": "425",
                    "detail": f"The Request is a duplicate of : {existing_id}",
                },
            )
        )
        async with AmazonAdsConnector(
            region="NA", profile_id="prof", **CREDS
        ) as conn:
            report_id = await conn.create_campaign_report(
                ad_product="SPONSORED_PRODUCTS",
                start_date="2026-06-23",
                end_date="2026-06-29",
            )
        assert report_id == existing_id


async def test_create_report_handles_425_with_typed_duplicate_id_field():
    """Some Ads responses surface the existing ID under a typed field rather
    than embedded in the detail string. Support both shapes."""
    base = REGION_BASE_URLS["NA"]
    existing_id = "abc12345-dead-beef-cafe-000000000001"
    with respx.mock(assert_all_called=True) as mock:
        _token_route(mock)
        mock.post(f"{base}/reporting/reports").mock(
            return_value=httpx.Response(
                425, json={"code": "425", "duplicateReportId": existing_id}
            )
        )
        async with AmazonAdsConnector(
            region="NA", profile_id="prof", **CREDS
        ) as conn:
            report_id = await conn.create_campaign_report(
                ad_product="SPONSORED_BRANDS",
                start_date="2026-06-23",
                end_date="2026-06-29",
            )
        assert report_id == existing_id


async def test_create_report_425_without_parseable_id_raises():
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=True) as mock:
        _token_route(mock)
        mock.post(f"{base}/reporting/reports").mock(
            return_value=httpx.Response(425, text="garbage no uuid here")
        )
        async with AmazonAdsConnector(
            region="NA", profile_id="prof", **CREDS
        ) as conn:
            with pytest.raises(Exception, match="duplicate"):
                await conn.create_campaign_report(
                    ad_product="SPONSORED_PRODUCTS",
                    start_date="2026-06-23",
                    end_date="2026-06-29",
                )


def test_extract_duplicate_report_id_handles_variants():
    """Parser coverage for the body shapes we've seen in the wild."""
    from app.connectors.amazon_ads import _extract_duplicate_report_id

    # Standard v3 shape
    assert _extract_duplicate_report_id(
        '{"code":"425","detail":"The Request is a duplicate of : 7307043c-3eab-4f59-b981-609f42b98849"}'
    ) == "7307043c-3eab-4f59-b981-609f42b98849"
    # Typed field variant
    assert _extract_duplicate_report_id(
        '{"duplicateReportId":"abc12345-dead-beef-cafe-000000000001"}'
    ) == "abc12345-dead-beef-cafe-000000000001"
    # Non-JSON fallback
    assert _extract_duplicate_report_id(
        "Plain text: duplicate of : abc12345-dead-beef-cafe-000000000002"
    ) == "abc12345-dead-beef-cafe-000000000002"
    # Unparseable returns None
    assert _extract_duplicate_report_id("nope") is None
    assert _extract_duplicate_report_id("") is None


async def test_wait_for_report_polls_until_completed():
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=True) as mock:
        _token_route(mock)
        status_route = mock.get(f"{base}/reporting/reports/rep-1").mock(
            side_effect=[
                httpx.Response(200, json={"status": "PENDING"}),
                httpx.Response(200, json={"status": "PROCESSING"}),
                httpx.Response(
                    200,
                    json={
                        "status": "COMPLETED",
                        "url": "https://s3.example/report-rep-1.json.gz",
                    },
                ),
            ]
        )

        async with AmazonAdsConnector(region="NA", **CREDS) as conn:
            status = await conn.wait_for_report(
                "rep-1", poll_interval=0.0, max_seconds=5
            )

        assert status_route.call_count == 3
        assert status["url"].endswith("rep-1.json.gz")


async def test_wait_for_report_raises_on_failed_status():
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=True) as mock:
        _token_route(mock)
        mock.get(f"{base}/reporting/reports/rep-x").mock(
            return_value=httpx.Response(
                200, json={"status": "FAILED", "failureReason": "out of memory"}
            )
        )
        async with AmazonAdsConnector(region="NA", **CREDS) as conn:
            with pytest.raises(ReportFailed, match="out of memory"):
                await conn.wait_for_report("rep-x", poll_interval=0.0, max_seconds=5)


async def test_wait_for_report_times_out():
    base = REGION_BASE_URLS["NA"]
    with respx.mock(assert_all_called=True) as mock:
        _token_route(mock)
        mock.get(f"{base}/reporting/reports/rep-stuck").mock(
            return_value=httpx.Response(200, json={"status": "PROCESSING"})
        )
        async with AmazonAdsConnector(region="NA", **CREDS) as conn:
            with pytest.raises(ReportPollTimeout):
                # max_seconds=0 forces the deadline check to fire after the
                # first poll.
                await conn.wait_for_report("rep-stuck", poll_interval=0.0, max_seconds=0)


async def test_download_report_handles_gzip_payload():
    payload_rows = [
        {"campaignId": "c1", "date": "2026-06-01", "cost": "12.34"},
        {"campaignId": "c2", "date": "2026-06-01", "cost": "56.78"},
    ]
    gzipped = gzip.compress(json.dumps(payload_rows).encode("utf-8"))
    with respx.mock(assert_all_called=True) as mock:
        # No token route: download_report uses the raw HTTP client without
        # auth headers (the S3 URL carries its own signed auth).
        mock.get("https://s3.example/report.json.gz").mock(
            return_value=httpx.Response(200, content=gzipped)
        )
        async with AmazonAdsConnector(region="NA", **CREDS) as conn:
            rows = await conn.download_report("https://s3.example/report.json.gz")
        assert rows == payload_rows


async def test_download_report_handles_plain_json_when_server_decompressed():
    payload_rows = [{"campaignId": "c1", "date": "2026-06-01", "cost": "1.00"}]
    with respx.mock(assert_all_called=True) as mock:
        mock.get("https://s3.example/report.json").mock(
            return_value=httpx.Response(
                200, content=json.dumps(payload_rows).encode("utf-8")
            )
        )
        async with AmazonAdsConnector(region="NA", **CREDS) as conn:
            rows = await conn.download_report("https://s3.example/report.json")
        assert rows == payload_rows


async def test_run_campaign_report_end_to_end():
    base = REGION_BASE_URLS["NA"]
    payload_rows = [
        {
            "campaignId": "cmp-1",
            "campaignName": "Brand defense",
            "date": "2026-06-15",
            "cost": "42.50",
            "sales7d": "180.25",
            "impressions": 10000,
            "clicks": 250,
            "purchases7d": 6,
        }
    ]
    gzipped = gzip.compress(json.dumps(payload_rows).encode("utf-8"))
    with respx.mock(assert_all_called=True) as mock:
        _token_route(mock)
        mock.post(f"{base}/reporting/reports").mock(
            return_value=httpx.Response(200, json={"reportId": "rep-e2e"})
        )
        mock.get(f"{base}/reporting/reports/rep-e2e").mock(
            return_value=httpx.Response(
                200,
                json={
                    "status": "COMPLETED",
                    "url": "https://s3.example/rep-e2e.json.gz",
                },
            )
        )
        mock.get("https://s3.example/rep-e2e.json.gz").mock(
            return_value=httpx.Response(200, content=gzipped)
        )

        async with AmazonAdsConnector(
            region="NA", profile_id="prof", **CREDS
        ) as conn:
            rows = await conn.run_campaign_report(
                ad_product="SPONSORED_PRODUCTS",
                start_date="2026-06-15",
                end_date="2026-06-15",
                poll_interval=0.0,
                max_seconds=5,
            )

        assert rows == payload_rows


# -----------------------------------------------------------------------------
# ETL: row parsing
# -----------------------------------------------------------------------------


def test_build_ad_spend_row_sp_uses_sales7d():
    row = {
        "campaignId": "1",
        "campaignName": "Brand Defense",
        "date": "2026-06-15",
        "cost": "10.50",
        "sales1d": "0",
        "sales7d": "84.00",
        "sales14d": "120.00",
        "impressions": 500,
        "clicks": 12,
        "purchases7d": 2,
    }
    ad = _build_ad_spend_row(
        row, ad_product="SPONSORED_PRODUCTS", marketplace_id="ATVPDKIKX0DER", currency="USD"
    )
    assert ad.date == date(2026, 6, 15)
    assert ad.platform == "amazon_sp"
    assert ad.marketplace == "us"
    assert ad.currency == "USD"
    assert ad.spend == Decimal("10.50")
    assert ad.sales_attributed == Decimal("84.00")  # sales7d, not 1d or 14d
    assert ad.impressions == 500
    assert ad.clicks == 12
    # ACoS = 10.50 / 84.00 * 100 = 12.50
    assert ad.acos == Decimal("12.50")
    assert ad.campaign_id == "1"


def test_build_ad_spend_row_sb_uses_sales_field():
    row = {
        "campaignId": "2",
        "campaignName": "SB campaign",
        "date": "2026-06-15",
        "cost": "5.00",
        "sales": "0",
        "impressions": 100,
        "clicks": 3,
        "purchases": 0,
    }
    ad = _build_ad_spend_row(
        row, ad_product="SPONSORED_BRANDS", marketplace_id="ATVPDKIKX0DER", currency="USD"
    )
    assert ad.platform == "amazon_sb"
    assert ad.spend == Decimal("5.00")
    # No sales -> sales_attributed None, ACoS None
    assert ad.sales_attributed is None
    assert ad.acos is None


def test_build_ad_spend_row_sd():
    row = {
        "campaignId": "3",
        "campaignName": "SD campaign",
        "date": "2026-06-15",
        "cost": "20.00",
        "sales": "50.00",
        "impressions": 1000,
        "clicks": 25,
        "purchases": 1,
    }
    ad = _build_ad_spend_row(
        row, ad_product="SPONSORED_DISPLAY", marketplace_id="ATVPDKIKX0DER", currency="USD"
    )
    assert ad.platform == "amazon_sd"
    assert ad.sales_attributed == Decimal("50.00")
    # ACoS = 20.00 / 50.00 * 100 = 40.00
    assert ad.acos == Decimal("40.00")


def test_build_ad_spend_row_caps_extreme_acos_to_fit_column():
    """ACoS = cost / sales * 100. When sales is tiny, ACoS can exceed
    DECIMAL(5,2) range -- the builder must cap it to fit the schema."""
    row = {
        "campaignId": "c",
        "date": "2026-06-15",
        "cost": "100.00",
        "sales": "0.01",  # forces ACoS to 1,000,000.00
        "impressions": 1,
        "clicks": 1,
    }
    ad = _build_ad_spend_row(
        row, ad_product="SPONSORED_BRANDS", marketplace_id="ATVPDKIKX0DER", currency="USD"
    )
    assert ad.acos == Decimal("999.99")


def test_build_ad_spend_row_skips_when_date_missing():
    row = {"campaignId": "1", "cost": "5.00"}
    ad = _build_ad_spend_row(
        row, ad_product="SPONSORED_PRODUCTS", marketplace_id="ATVPDKIKX0DER", currency="USD"
    )
    assert ad is None


def test_marketplace_short_code_correct_for_all_marketplaces():
    """Each marketplace_id must map to the correct ad_spend.marketplace value
    so pnl_calculator's reverse lookup works."""
    expected = {
        "ATVPDKIKX0DER": "us",
        "A2EUQ1WTGCTBG2": "ca",
        "A1AM78C64UM0Y8": "mx",
        "A1F83G8C2ARO7P": "uk",
        "A39IBJ37TRP1C6": "au",
    }
    for marketplace_id, short_code in expected.items():
        ad = _build_ad_spend_row(
            {"date": "2026-01-01", "cost": "1", "sales": "1"},
            ad_product="SPONSORED_BRANDS",
            marketplace_id=marketplace_id,
            currency="USD",
        )
        assert ad.marketplace == short_code


def test_ad_product_to_platform_covers_all_three_types():
    """The ETL writes ad_spend.platform from this mapping; pnl_calculator
    reads it back. They must agree on the three SP-API ad products."""
    assert AD_PRODUCT_TO_PLATFORM["SPONSORED_PRODUCTS"] == "amazon_sp"
    assert AD_PRODUCT_TO_PLATFORM["SPONSORED_BRANDS"] == "amazon_sb"
    assert AD_PRODUCT_TO_PLATFORM["SPONSORED_DISPLAY"] == "amazon_sd"


def test_summary_to_dict_is_serializable():
    summary = AmazonAdsEtlSummary(
        start_date=date(2026, 6, 1),
        end_date=date(2026, 6, 7),
        marketplace_ids=["ATVPDKIKX0DER"],
        reports_created=3,
        reports_completed=3,
        rows_inserted=42,
        rows_by_platform={"amazon_sp": 30, "amazon_sb": 5, "amazon_sd": 7},
        spend_by_platform={"amazon_sp": 100.0, "amazon_sb": 25.5, "amazon_sd": 60.25},
    )
    out = summary.to_dict()
    assert out["start_date"] == "2026-06-01"
    assert out["rows_inserted"] == 42
    assert out["spend_by_platform"]["amazon_sp"] == 100.0


# -----------------------------------------------------------------------------
# pnl_calculator's ad_spend aggregation
# -----------------------------------------------------------------------------


def test_pnl_calc_platform_to_column_mapping():
    """pnl_calculator reads ad_spend.platform values and routes each into
    a specific daily_pnl column. Verify the four-platform mapping."""
    from app.etl.pnl_calculator import AD_SPEND_PLATFORM_TO_COLUMN

    assert AD_SPEND_PLATFORM_TO_COLUMN == {
        "amazon_sp": "ad_spend_sp",
        "amazon_sb": "ad_spend_sb",
        "amazon_sd": "ad_spend_sd",
        "amazon_sv": "ad_spend_sv",
    }
