"""One-off diagnostic: can the NA Ads refresh token also authorize EU and FE
region calls?

If yes, one LWA authorization covers all three Ads regions and we can collapse
the three env vars into one. If no, we need separate OAuth flows per region.

Run from backend/ with .env loaded:
    .venv/bin/python -m scripts.test_ads_multi_region
"""

from __future__ import annotations

import asyncio
import json
import sys

import httpx

from app.config import settings

LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
REGION_PROFILES_URL = {
    "NA": "https://advertising-api.amazon.com/v2/profiles",
    "EU": "https://advertising-api-eu.amazon.com/v2/profiles",
    "FE": "https://advertising-api-fe.amazon.com/v2/profiles",
}
EXPECTED_COUNTRY_BY_REGION = {
    "NA": ("US", "ATVPDKIKX0DER"),
    "EU": ("GB", "A1F83G8C2ARO7P"),  # UK profiles report countryCode="GB"
    "FE": ("AU", "A39IBJ37TRP1C6"),
}


async def _exchange_refresh_token(client: httpx.AsyncClient) -> str:
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": settings.AMAZON_ADS_REFRESH_TOKEN_NA,
        "client_id": settings.AMAZON_ADS_CLIENT_ID,
        "client_secret": settings.AMAZON_ADS_CLIENT_SECRET,
    }
    r = await client.post(LWA_TOKEN_URL, data=payload)
    if r.status_code != 200:
        print(f"LWA token exchange FAILED status={r.status_code} body={r.text}")
        sys.exit(2)
    return r.json()["access_token"]


async def _probe_region(
    client: httpx.AsyncClient, region: str, url: str, token: str
) -> dict:
    headers = {
        "Authorization": f"Bearer {token}",
        "Amazon-Advertising-API-ClientId": settings.AMAZON_ADS_CLIENT_ID,
    }
    r = await client.get(url, headers=headers)
    result = {
        "region": region,
        "url": url,
        "status": r.status_code,
        "body": r.text,
    }
    try:
        result["json"] = r.json()
    except ValueError:
        result["json"] = None
    return result


def _print_probe(result: dict) -> None:
    region = result["region"]
    print("=" * 72)
    print(f"[{region}] GET {result['url']}")
    print(f"    status: {result['status']}")
    body = result["body"]
    # Pretty-print JSON if available; truncate long bodies for readability.
    payload = result.get("json")
    if payload is not None:
        formatted = json.dumps(payload, indent=2)
        if len(formatted) > 4000:
            formatted = formatted[:4000] + "\n    ... (truncated)"
        print(f"    body:\n{formatted}")
    else:
        if len(body) > 2000:
            body = body[:2000] + " ... (truncated)"
        print(f"    body: {body}")

    if isinstance(payload, list):
        countries = sorted({(p.get("countryCode") or "?") for p in payload})
        currencies = sorted({(p.get("currencyCode") or "?") for p in payload})
        print(f"    profile count: {len(payload)}")
        print(f"    countryCodes: {countries}")
        print(f"    currencyCodes: {currencies}")
        expected_cc, expected_mp = EXPECTED_COUNTRY_BY_REGION[region]
        matching = [p for p in payload if p.get("countryCode") == expected_cc]
        if matching:
            print(
                f"    ✓ expected {expected_cc} ({expected_mp}) profile present: "
                f"profileId={matching[0].get('profileId')} "
                f"acct={(matching[0].get('accountInfo') or {}).get('name')}"
            )
        else:
            print(f"    ✗ no {expected_cc} profile in the response")


def _summarize(probes: dict[str, dict]) -> tuple[str, str]:
    """Return (outcome_letter, human_summary)."""
    na_ok = probes["NA"]["status"] == 200 and probes["NA"].get("json")
    eu_ok = probes["EU"]["status"] == 200 and probes["EU"].get("json")
    fe_ok = probes["FE"]["status"] == 200 and probes["FE"].get("json")

    if not na_ok:
        return "sanity_fail", (
            f"NA sanity check failed (status={probes['NA']['status']}). "
            "The NA token itself isn't working; can't judge cross-region compatibility."
        )
    if eu_ok and fe_ok:
        # Also verify the expected country profile is actually returned.
        def has(region: str) -> bool:
            payload = probes[region].get("json") or []
            expected_cc = EXPECTED_COUNTRY_BY_REGION[region][0]
            return any(p.get("countryCode") == expected_cc for p in payload)

        if has("EU") and has("FE"):
            return "a", (
                "NA token works AS-IS against EU and FE endpoints, and each "
                "returned the expected country profile. Single-token setup is viable."
            )
        return "c-partial-profiles", (
            "EU and FE both returned 200, but at least one is missing the expected "
            "country profile. Auth works cross-region but the underlying Ads account "
            "may not have access to UK/AU marketplaces."
        )
    if probes["EU"]["status"] == 401 and probes["FE"]["status"] == 401:
        return "b", "Both EU and FE returned 401. NA token is region-locked."
    return "c", (
        f"Mixed results. EU status={probes['EU']['status']}, "
        f"FE status={probes['FE']['status']}."
    )


async def main() -> int:
    if not (
        settings.AMAZON_ADS_CLIENT_ID
        and settings.AMAZON_ADS_CLIENT_SECRET
        and settings.AMAZON_ADS_REFRESH_TOKEN_NA
    ):
        print("ERROR: AMAZON_ADS_CLIENT_ID / _SECRET / _REFRESH_TOKEN_NA not set in .env")
        return 1

    async with httpx.AsyncClient(timeout=30.0) as client:
        print(f"[1/2] LWA token exchange with NA refresh token")
        token = await _exchange_refresh_token(client)
        print(f"    access_token: {token[:12]}... (len={len(token)})")

        print(f"[2/2] Probing profile endpoints with the SAME access token")
        probes = {}
        # Sequential so the output is easy to read.
        for region, url in REGION_PROFILES_URL.items():
            probes[region] = await _probe_region(client, region, url, token)
            _print_probe(probes[region])

    print()
    print("=" * 72)
    outcome, summary = _summarize(probes)
    print(f"OUTCOME: {outcome}")
    print(f"SUMMARY: {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
