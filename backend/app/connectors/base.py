from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class ConnectorError(Exception):
    """Base exception for connector failures."""


class AuthError(ConnectorError):
    """Raised when authentication fails and cannot be recovered."""


class RateLimitError(ConnectorError):
    """Raised when a request is throttled after exhausting retries."""


@dataclass(frozen=True)
class RateLimit:
    """Per-endpoint rate limit expressed in requests-per-second."""

    requests_per_second: float

    @property
    def min_interval(self) -> float:
        return 1.0 / self.requests_per_second if self.requests_per_second > 0 else 0.0


class RateLimiter:
    """Token-bucket-style limiter keyed by endpoint name.

    Amazon SP-API publishes per-endpoint quotas (Sales 0.5 r/s, Reports 0.0167 r/s, ...).
    A single shared limiter for the connector would serialize unrelated calls, so we
    keep one bucket per endpoint key.
    """

    def __init__(self) -> None:
        self._next_allowed: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()

    async def _lock_for(self, key: str) -> asyncio.Lock:
        async with self._global_lock:
            lock = self._locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[key] = lock
            return lock

    async def acquire(self, key: str, limit: RateLimit) -> None:
        if limit.min_interval == 0.0:
            return
        lock = await self._lock_for(key)
        async with lock:
            now = time.monotonic()
            next_allowed = self._next_allowed.get(key, 0.0)
            wait = next_allowed - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = time.monotonic()
            self._next_allowed[key] = now + limit.min_interval


class BaseConnector:
    """Async HTTP connector with retries, per-endpoint rate limiting, and auth recovery.

    Subclasses set `base_url`, provide `_auth_headers()` to inject auth, and call
    `request()` for every outbound call. Pass an `endpoint_key` + `RateLimit` per
    call so the limiter throttles each Amazon endpoint independently.
    """

    base_url: str = ""
    max_retries: int = 3
    backoff_base: float = 1.0
    backoff_cap: float = 30.0

    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        rate_limiter: RateLimiter | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=timeout)
        self._rate_limiter = rate_limiter or RateLimiter()

    async def __aenter__(self) -> "BaseConnector":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def _auth_headers(self, *, force_refresh: bool = False) -> dict[str, str]:
        """Override to inject Authorization / x-amz-access-token headers."""
        return {}

    async def request(
        self,
        method: str,
        path: str,
        *,
        endpoint_key: str,
        rate_limit: RateLimit,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        headers: dict[str, str] | None = None,
        auth: bool = True,
        allowed_statuses: set[int] | None = None,
    ) -> httpx.Response:
        """`allowed_statuses` opts the caller into seeing specific 4xx codes
        as successful responses (no raise_for_status). Useful for endpoints
        whose 4xx replies carry actionable data — e.g. Amazon Ads returns
        425 with the existing reportId in the body for duplicate-create
        requests, and the caller wants to extract that ID rather than error."""
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        attempt = 0
        forced_refresh = False

        while True:
            await self._rate_limiter.acquire(endpoint_key, rate_limit)

            merged_headers: dict[str, str] = {}
            if auth:
                merged_headers.update(await self._auth_headers(force_refresh=forced_refresh))
            if headers:
                merged_headers.update(headers)

            start = time.monotonic()
            try:
                response = await self._client.request(
                    method,
                    url,
                    params=params,
                    json=json,
                    headers=merged_headers,
                )
            except httpx.HTTPError as exc:
                duration_ms = int((time.monotonic() - start) * 1000)
                logger.warning(
                    "connector_request_error endpoint=%s method=%s url=%s duration_ms=%d error=%s",
                    endpoint_key,
                    method,
                    url,
                    duration_ms,
                    exc,
                )
                if attempt >= self.max_retries:
                    raise ConnectorError(f"{method} {url} failed: {exc}") from exc
                await self._sleep_backoff(attempt)
                attempt += 1
                continue

            duration_ms = int((time.monotonic() - start) * 1000)
            logger.info(
                "connector_request endpoint=%s method=%s url=%s status=%d duration_ms=%d",
                endpoint_key,
                method,
                url,
                response.status_code,
                duration_ms,
            )

            if response.status_code == 401 and auth and not forced_refresh:
                forced_refresh = True
                continue

            if response.status_code == 429 or response.status_code >= 500:
                if attempt >= self.max_retries:
                    if response.status_code == 429:
                        raise RateLimitError(
                            f"{method} {url} throttled after {attempt + 1} attempts"
                        )
                    response.raise_for_status()
                retry_after = self._parse_retry_after(response)
                await self._sleep_backoff(attempt, retry_after=retry_after)
                attempt += 1
                continue

            if response.status_code == 401:
                raise AuthError(f"{method} {url} unauthorized after refresh: {response.text}")

            if allowed_statuses and response.status_code in allowed_statuses:
                return response
            response.raise_for_status()
            return response

    async def _sleep_backoff(self, attempt: int, *, retry_after: float | None = None) -> None:
        if retry_after is not None:
            delay = retry_after
        else:
            delay = min(self.backoff_base * (2**attempt), self.backoff_cap)
            delay += random.uniform(0, delay * 0.1)
        await asyncio.sleep(delay)

    @staticmethod
    def _parse_retry_after(response: httpx.Response) -> float | None:
        value = response.headers.get("Retry-After")
        if value is None:
            return None
        try:
            return float(value)
        except ValueError:
            return None
