"""Async client for the Mobility Database catalog API."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from enum import Enum
from http import HTTPStatus
from types import TracebackType
from typing import Any, Self

import aiohttp

from .const import (
    DEFAULT_TIMEOUT_SECONDS,
    PROD_BASE_URL,
    TOKEN_EXPIRY_SKEW_SECONDS,
    TOKEN_PATH,
)
from .exceptions import (
    MobilityDatabaseApiError,
    MobilityDatabaseAuthenticationError,
    MobilityDatabaseConnectionError,
    MobilityDatabaseError,
    MobilityDatabaseNotFoundError,
    MobilityDatabaseRateLimitError,
)
from .models import (
    AccessToken,
    BoundingFilterMethod,
    DataType,
    EntityType,
    Feed,
    FeedStatus,
    GbfsFeed,
    GtfsDataset,
    GtfsFeed,
    GtfsFeedAvailability,
    GtfsRtFeed,
    License,
    LicenseWithRules,
    LocationSearchResults,
    LocationType,
    MatchingLicense,
    Metadata,
    SearchResults,
    SortOrder,
)


def _encode_value(value: Any) -> str:
    """Encode a single query parameter value as a string."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return ",".join(_encode_value(item) for item in value)
    return str(value)


def encode_params(params: dict[str, Any]) -> dict[str, str]:
    """Encode query parameters, dropping None values.

    Booleans become "true"/"false", enums use their value, datetimes use ISO
    format, and lists/tuples are comma-joined (the API's convention for
    multi-value filters).
    """
    return {
        key: _encode_value(value) for key, value in params.items() if value is not None
    }


class MobilityDatabaseClient:
    """Async client for the Mobility Database catalog API.

    If ``session`` is not provided, the client lazily creates its own
    ``aiohttp.ClientSession`` and closes it in :meth:`close`. An injected
    session is never closed by the client.
    """

    def __init__(
        self,
        refresh_token: str,
        session: aiohttp.ClientSession | None = None,
        *,
        base_url: str = PROD_BASE_URL,
        request_timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        """Initialize the client. Performs no I/O."""
        self._refresh_token = refresh_token
        self._session = session
        self._owns_session = session is None
        self._base_url = base_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=request_timeout)
        self._access_token: str | None = None
        self._token_expiration: datetime | None = None
        self._token_lock = asyncio.Lock()

    async def __aenter__(self) -> Self:
        """Enter the async context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Exit the async context manager, closing owned resources."""
        await self.close()

    async def close(self) -> None:
        """Close the underlying session if this client owns it. Idempotent."""
        if (
            self._owns_session
            and self._session is not None
            and not self._session.closed
        ):
            await self._session.close()
            self._session = None

    def _get_session(self) -> aiohttp.ClientSession:
        """Return the session, lazily creating an owned one if needed."""
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    def _token_needs_refresh(self) -> bool:
        """Return True if there is no token or it is at/near expiration."""
        if self._access_token is None or self._token_expiration is None:
            return True
        skew = timedelta(seconds=TOKEN_EXPIRY_SKEW_SECONDS)
        return datetime.now(UTC) >= self._token_expiration - skew

    async def _async_ensure_token(
        self, *, force: bool = False, stale_token: str | None = None
    ) -> str:
        """Return a valid access token, fetching or refreshing as needed.

        Guarded by a lock so concurrent requests trigger exactly one token
        request. NOTE: the live API returns HTTP 500 for an invalid refresh
        token, so ANY non-200 here is treated as an authentication failure.

        ``stale_token`` dedupes concurrent forced refreshes: when several
        requests all hit a 401 on the same token, each calls this with
        ``force=True`` and the token *it* used. Only the first to acquire the
        lock actually refetches; the rest see that ``self._access_token`` no
        longer matches their ``stale_token`` (a sibling already refreshed it)
        and return the new token without another token request.
        """
        async with self._token_lock:
            if not force and not self._token_needs_refresh():
                assert self._access_token is not None  # guarded above
                return self._access_token
            if (
                force
                and stale_token is not None
                and self._access_token is not None
                and self._access_token != stale_token
            ):
                return self._access_token
            session = self._get_session()
            try:
                async with session.post(
                    f"{self._base_url}{TOKEN_PATH}",
                    json={"refresh_token": self._refresh_token},
                    timeout=self._timeout,
                ) as resp:
                    if resp.status != HTTPStatus.OK:
                        body = await resp.text()
                        raise MobilityDatabaseAuthenticationError(
                            f"Unable to obtain access token ({resp.status}): {body}"
                        )
                    data = await resp.json()
            except (TimeoutError, aiohttp.ClientError, ValueError) as err:
                raise MobilityDatabaseConnectionError(
                    f"Error requesting access token: {err}"
                ) from err
            token = AccessToken.from_dict(data)
            expiration = token.expiration_datetime_utc
            if expiration.tzinfo is None:
                expiration = expiration.replace(tzinfo=UTC)
            self._access_token = token.access_token
            self._token_expiration = expiration
            return self._access_token

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        retry_on_auth_fail: bool = True,
    ) -> Any:
        """Perform an authenticated request and return the decoded JSON body."""
        token = await self._async_ensure_token()
        session = self._get_session()
        try:
            async with session.request(
                method,
                f"{self._base_url}{path}",
                params=encode_params(params) if params else None,
                json=json_body,
                headers={"Authorization": f"Bearer {token}"},
                timeout=self._timeout,
            ) as resp:
                if resp.status == HTTPStatus.UNAUTHORIZED:
                    if retry_on_auth_fail:
                        await self._async_ensure_token(force=True, stale_token=token)
                        return await self._request(
                            method,
                            path,
                            params=params,
                            json_body=json_body,
                            retry_on_auth_fail=False,
                        )
                    raise MobilityDatabaseAuthenticationError(
                        "Request unauthorized after token refresh"
                    )
                if resp.status == HTTPStatus.NOT_FOUND:
                    raise MobilityDatabaseNotFoundError(f"Not found: {path}")
                if resp.status == HTTPStatus.TOO_MANY_REQUESTS:
                    raise MobilityDatabaseRateLimitError("API rate limit exceeded")
                if resp.status >= HTTPStatus.BAD_REQUEST:
                    raise MobilityDatabaseApiError(resp.status, await resp.text())
                try:
                    return await resp.json()
                except (aiohttp.ContentTypeError, ValueError) as err:
                    raise MobilityDatabaseConnectionError(
                        "API returned invalid JSON"
                    ) from err
        except MobilityDatabaseError:
            raise
        except (TimeoutError, aiohttp.ClientError) as err:
            raise MobilityDatabaseConnectionError(
                f"Error communicating with API: {err}"
            ) from err

    async def get_metadata(self) -> Metadata:
        """Get metadata about the API."""
        data = await self._request("GET", "/v1/metadata")
        return Metadata.from_dict(data)

    async def get_feeds(
        self,
        *,
        limit: int | None = None,
        offset: int | None = None,
        status: FeedStatus | None = None,
        provider: str | None = None,
        producer_url: str | None = None,
        is_official: bool | None = None,
    ) -> list[Feed]:
        """Get feeds of all data types."""
        data = await self._request(
            "GET",
            "/v1/feeds",
            params={
                "limit": limit,
                "offset": offset,
                "status": status,
                "provider": provider,
                "producer_url": producer_url,
                "is_official": is_official,
            },
        )
        return [Feed.from_dict(item) for item in data]

    async def get_feed(self, feed_id: str) -> Feed:
        """Get a single feed by ID."""
        data = await self._request("GET", f"/v1/feeds/{feed_id}")
        return Feed.from_dict(data)

    async def get_gtfs_feeds(
        self,
        *,
        limit: int | None = None,
        offset: int | None = None,
        provider: str | None = None,
        producer_url: str | None = None,
        country_code: str | None = None,
        subdivision_name: str | None = None,
        municipality: str | None = None,
        dataset_latitudes: tuple[float, float] | None = None,
        dataset_longitudes: tuple[float, float] | None = None,
        bounding_filter_method: BoundingFilterMethod | None = None,
        is_official: bool | None = None,
    ) -> list[GtfsFeed]:
        """Get GTFS schedule feeds."""
        data = await self._request(
            "GET",
            "/v1/gtfs_feeds",
            params={
                "limit": limit,
                "offset": offset,
                "provider": provider,
                "producer_url": producer_url,
                "country_code": country_code,
                "subdivision_name": subdivision_name,
                "municipality": municipality,
                "dataset_latitudes": dataset_latitudes,
                "dataset_longitudes": dataset_longitudes,
                "bounding_filter_method": bounding_filter_method,
                "is_official": is_official,
            },
        )
        return [GtfsFeed.from_dict(item) for item in data]

    async def get_gtfs_feed(self, feed_id: str) -> GtfsFeed:
        """Get a single GTFS feed by ID."""
        data = await self._request("GET", f"/v1/gtfs_feeds/{feed_id}")
        return GtfsFeed.from_dict(data)

    async def get_gtfs_rt_feeds(
        self,
        *,
        limit: int | None = None,
        offset: int | None = None,
        provider: str | None = None,
        producer_url: str | None = None,
        entity_types: list[EntityType] | None = None,
        country_code: str | None = None,
        subdivision_name: str | None = None,
        municipality: str | None = None,
        is_official: bool | None = None,
    ) -> list[GtfsRtFeed]:
        """Get GTFS Realtime feeds."""
        data = await self._request(
            "GET",
            "/v1/gtfs_rt_feeds",
            params={
                "limit": limit,
                "offset": offset,
                "provider": provider,
                "producer_url": producer_url,
                "entity_types": entity_types,
                "country_code": country_code,
                "subdivision_name": subdivision_name,
                "municipality": municipality,
                "is_official": is_official,
            },
        )
        return [GtfsRtFeed.from_dict(item) for item in data]

    async def get_gtfs_rt_feed(self, feed_id: str) -> GtfsRtFeed:
        """Get a single GTFS Realtime feed by ID."""
        data = await self._request("GET", f"/v1/gtfs_rt_feeds/{feed_id}")
        return GtfsRtFeed.from_dict(data)

    async def get_gbfs_feeds(
        self,
        *,
        limit: int | None = None,
        offset: int | None = None,
        provider: str | None = None,
        producer_url: str | None = None,
        country_code: str | None = None,
        subdivision_name: str | None = None,
        municipality: str | None = None,
        system_id: str | None = None,
        version: str | None = None,
    ) -> list[GbfsFeed]:
        """Get GBFS feeds."""
        data = await self._request(
            "GET",
            "/v1/gbfs_feeds",
            params={
                "limit": limit,
                "offset": offset,
                "provider": provider,
                "producer_url": producer_url,
                "country_code": country_code,
                "subdivision_name": subdivision_name,
                "municipality": municipality,
                "system_id": system_id,
                "version": version,
            },
        )
        return [GbfsFeed.from_dict(item) for item in data]

    async def get_gbfs_feed(self, feed_id: str) -> GbfsFeed:
        """Get a single GBFS feed by ID."""
        data = await self._request("GET", f"/v1/gbfs_feeds/{feed_id}")
        return GbfsFeed.from_dict(data)

    async def get_gtfs_feed_datasets(
        self,
        feed_id: str,
        *,
        latest: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
        downloaded_after: datetime | None = None,
        downloaded_before: datetime | None = None,
    ) -> list[GtfsDataset]:
        """Get datasets for a GTFS feed, sorted newest to oldest."""
        data = await self._request(
            "GET",
            f"/v1/gtfs_feeds/{feed_id}/datasets",
            params={
                "latest": latest,
                "limit": limit,
                "offset": offset,
                "downloaded_after": downloaded_after,
                "downloaded_before": downloaded_before,
            },
        )
        return [GtfsDataset.from_dict(item) for item in data]

    async def get_gtfs_feed_gtfs_rt_feeds(self, feed_id: str) -> list[GtfsRtFeed]:
        """Get GTFS Realtime feeds related to a GTFS feed."""
        data = await self._request("GET", f"/v1/gtfs_feeds/{feed_id}/gtfs_rt_feeds")
        return [GtfsRtFeed.from_dict(item) for item in data]

    async def get_gtfs_feed_availability(
        self,
        feed_id: str,
        *,
        checked_after: datetime | None = None,
        checked_before: datetime | None = None,
        limit: int | None = None,
        offset: int | None = None,
        sort: SortOrder | None = None,
    ) -> GtfsFeedAvailability:
        """Get historical availability checks for a GTFS feed."""
        data = await self._request(
            "GET",
            f"/v1/gtfs_feeds/{feed_id}/availability",
            params={
                "from": checked_after,
                "to": checked_before,
                "limit": limit,
                "offset": offset,
                "sort": sort,
            },
        )
        return GtfsFeedAvailability.from_dict(data)

    async def get_dataset_gtfs(self, dataset_id: str) -> GtfsDataset:
        """Get a single GTFS dataset by ID."""
        data = await self._request("GET", f"/v1/datasets/gtfs/{dataset_id}")
        return GtfsDataset.from_dict(data)

    async def search_feeds(
        self,
        *,
        search_query: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        statuses: list[FeedStatus] | None = None,
        feed_id: str | None = None,
        data_types: list[DataType] | None = None,
        is_official: bool | None = None,
        version: str | None = None,
        features: list[str] | None = None,
        license_ids: list[str] | None = None,
        license_is_spdx: bool | None = None,
        license_tags: list[str] | None = None,
    ) -> SearchResults:
        """Full-text search for feeds by name, location, and provider."""
        data = await self._request(
            "GET",
            "/v1/search",
            params={
                "search_query": search_query,
                "limit": limit,
                "offset": offset,
                "status": statuses,
                "feed_id": feed_id,
                "data_type": data_types,
                "is_official": is_official,
                "version": version,
                "feature": features,
                "license_ids": license_ids,
                "license_is_spdx": license_is_spdx,
                "license_tags": license_tags,
            },
        )
        return SearchResults.from_dict(data)

    async def get_locations(
        self,
        *,
        search_query: str | None = None,
        country_code: str | None = None,
        subdivision_code: str | None = None,
        location_type: LocationType | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> LocationSearchResults:
        """Search locations (countries, subdivisions, municipalities)."""
        data = await self._request(
            "GET",
            "/v1/locations",
            params={
                "search_query": search_query,
                "country_code": country_code,
                "subdivision_code": subdivision_code,
                "location_type": location_type,
                "limit": limit,
                "offset": offset,
            },
        )
        return LocationSearchResults.from_dict(data)

    async def get_licenses(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> list[License]:
        """Get all licenses in the database."""
        data = await self._request(
            "GET", "/v1/licenses", params={"limit": limit, "offset": offset}
        )
        return [License.from_dict(item) for item in data]

    async def get_license(self, license_id: str) -> LicenseWithRules:
        """Get a single license, including its rules."""
        data = await self._request("GET", f"/v1/licenses/{license_id}")
        return LicenseWithRules.from_dict(data)

    async def get_matching_licenses(self, license_url: str) -> list[MatchingLicense]:
        """Match a license URL against the license database."""
        data = await self._request(
            "POST", "/v1/licenses:match", json_body={"license_url": license_url}
        )
        return [MatchingLicense.from_dict(item) for item in data]
