"""Async client for the Mobility Database catalog API."""

from __future__ import annotations

import asyncio
import ipaddress
import math
import re
from datetime import UTC, datetime, timedelta
from enum import Enum
from http import HTTPStatus
from types import TracebackType
from typing import Any, Self
from urllib.parse import quote

import aiohttp
from mashumaro import DataClassDictMixin
from yarl import URL

from ._http import URL_ERRORS, require_session, require_text
from ._json import loads as json_loads
from ._payload import mismatch
from ._transport import Answer, exchange
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


def _require_catalog_session(session: aiohttp.ClientSession) -> None:
    """Raise ValueError for an injected session the client can't use.

    The shared policy (:func:`._http.require_session`: no credential
    defaults, sendable headers), and no ``base_url``: aiohttp before 3.14
    then refuses the absolute URLs the client requests.
    """
    require_session(session)
    if getattr(session, "_base_url", None) is not None:
        raise ValueError("the session has a base_url; pass base_url= instead")


def _require_base_url(base_url: str) -> str:
    """Return ``base_url`` without a trailing slash; ValueError if unusable.

    An http(s) URL with a host and nothing a request path can't follow
    (credentials would be sent as Basic auth beside the Bearer token; a
    query or fragment would swallow every path). RFC 6750 section 5.3: a
    bearer token travels over TLS only, so plain http is for loopback.
    """
    try:
        parsed = URL(base_url)
        host = parsed.host
    except URL_ERRORS:
        parsed = None
    if parsed is None:  # outside the except, quoting nothing: it may hold a password
        raise ValueError("base_url is no URL")
    if parsed.scheme not in ("http", "https") or not host:
        raise ValueError("base_url must be an http(s) URL with a host")
    if parsed.user is not None or parsed.raw_password is not None:
        raise ValueError("base_url must not hold credentials")
    if (
        parsed.raw_query_string
        or parsed.raw_fragment
        or "?" in base_url
        or "#" in base_url
    ):
        raise ValueError("base_url must not hold a query or fragment")
    if parsed.scheme == "http" and not _is_loopback(host):
        raise ValueError("base_url must be https: bearer tokens travel over TLS only")
    return base_url.rstrip("/")


def _timeout_seconds(request_timeout: object) -> float:
    """Return ``request_timeout`` as float seconds; ValueError if unusable.

    A positive number a deadline can be scheduled with: asyncio can't
    schedule an infinite one (nor an int past float's range, which compares
    below infinity), so every request would fail with OverflowError as the
    API's fault.
    """
    seconds = math.nan
    if isinstance(request_timeout, bool) or not isinstance(
        request_timeout, int | float
    ):
        raise TypeError("request_timeout must be a number of seconds")
    try:
        seconds = float(request_timeout)
    except OverflowError:
        seconds = math.inf
    if not 0 < seconds < math.inf:
        raise ValueError("request_timeout must be a finite positive number of seconds")
    return seconds


def _is_loopback(host: str) -> bool:
    """Whether a host names this machine (where plain http stays local)."""
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _encode_value(value: Any) -> str:
    """Encode one query parameter value: a scalar, or a list or tuple of them.

    A list is the API's comma-separated multi-value filter, so an item that
    holds a comma itself would be read as two: the caller's ValueError.
    """
    if not isinstance(value, list | tuple):
        return _encode_scalar(value)
    items = [_encode_scalar(item) for item in value]
    if any("," in item for item in items):
        raise ValueError("a query list item holds a comma, which separates items")
    return ",".join(items)


def _encode_scalar(value: Any) -> str:
    """Encode one scalar, from an allowlist of the types the methods take.

    Anything else is the caller's TypeError (``str()`` of bytes or an object
    filtered on its repr), and a float that's no number the API can read
    (NaN, infinity) a ValueError -- raised before any request is made.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Enum):
        return _encode_scalar(value.value)
    if isinstance(value, datetime):
        return _encode_datetime(value)
    # The base types' own spellings: a subclass's (numpy 2's np.float64
    # repr is "np.float64(2.5)") would be sent as the value.
    if isinstance(value, int):
        return int.__repr__(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("a query parameter is no finite number")
        return float.__repr__(value)
    if isinstance(value, str):
        return require_text(str.__str__(value), "a query parameter")
    raise TypeError(f"a query parameter can't be a {type(value).__name__}")


def _encode_datetime(value: datetime) -> str:
    """Encode an aware datetime as RFC 3339, in UTC; ValueError otherwise.

    RFC 3339 section 5.6 requires an offset: a naive time names no instant,
    and the API would have to guess the zone. Sent in UTC: the same instant,
    and an offset RFC 3339 can write (hours and minutes; Python allows
    seconds, and isoformat writes "+00:19:32"). Errors are raised after the
    except block, in the library's words: a tzinfo's own (a broken one) or
    an OverflowError (no UTC time datetime can hold) aren't passed on.
    """
    try:
        aware = value.utcoffset() is not None
        encoded = value.astimezone(UTC).isoformat() if aware else None
    except (ValueError, TypeError):
        failure = "a query datetime's tzinfo gives no usable offset"
    except OverflowError:
        failure = "a query datetime has no UTC time datetime can hold"
    else:
        if encoded is not None:
            return encoded
        failure = "a query datetime must be timezone-aware"
    raise ValueError(failure)


def _quote_segment(segment: str) -> str:
    """Percent-encode one path segment (an id) for safe URL interpolation.

    Every id interpolated into a request path (feed_id, license_id,
    dataset_id, ...) goes through this. ``safe=""`` quotes every character
    outside the unreserved set -- including "/" -- so an id containing "/",
    "?", "#", "%", spaces, or non-ASCII text always becomes exactly one path
    segment instead of injecting extra segments, a query string, or a
    fragment. Additive-safe: ids already restricted to unreserved characters
    (the overwhelming common case) are returned unchanged.

    Raises ``ValueError`` for ``""``, ``"."`` and ``".."``: no encoding
    makes them one segment. Empty, the path names the collection instead;
    the dot segments are removed when the URL is resolved (RFC 3986
    section 5.2.4), and percent-encoding them changes nothing, since
    ``%2E`` is the same unreserved ``.`` (section 2.3). ``TypeError`` for
    an id that isn't a ``str`` (``b".."`` would slip past that check).

    aiohttp's URL handling may send some sub-delimiters ``quote`` encoded
    (``;``, ``,``, ``@``, ``:``, ``=``...) as the characters themselves.
    That is still one segment, which the API decodes to the same id.
    """
    if not isinstance(segment, str):
        raise TypeError(f"an id must be a str, not {type(segment).__name__}")
    if segment in ("", ".", ".."):
        raise ValueError(f"{segment!r} cannot be sent as an id: it is no path segment")
    return quote(require_text(segment, "an id"), safe="")


# What decoding a type-checked JSON value into a model can still raise:
# mashumaro's MissingField (a LookupError) and InvalidFieldValue (a
# ValueError: an unknown enum value, an unparseable datetime).
_DECODE_ERRORS = (LookupError, ValueError)
# RFC 6750 section 2.1's b64token: all a Bearer credential may hold. Anything
# else (a CR or LF above all) can't go in an Authorization header.
_B64TOKEN = re.compile(r"[A-Za-z0-9\-._~+/]+=*")


def _decode(answer: Answer) -> Any:
    """Return an answer's JSON, or raise MobilityDatabaseConnectionError.

    The media type must be ``application/json`` itself (``problem+json`` and
    the like aren't the documented answer), and the body JSON text by
    :func:`._json.loads`: strict UTF-8, unique names, no ``NaN``.
    """
    if answer.media_type != "application/json":
        # The media type stays out of the message: it is server text.
        raise MobilityDatabaseConnectionError(
            "API answered other than application/json"
        )
    if answer.body is None:
        raise MobilityDatabaseConnectionError("API answer's body couldn't be read")
    try:
        return json_loads(answer.body)
    except ValueError:
        pass
    # Raised outside the except: a JSONDecodeError holds the whole body (.doc),
    # and from None would only hide it, still attached as the context.
    raise MobilityDatabaseConnectionError("API returned invalid JSON")


def _text(answer: Answer) -> str:
    """Return an error answer's body as text, for the exception's ``body``."""
    return (answer.body or b"").decode("utf-8", "replace")


def _one[ModelT: DataClassDictMixin](model: type[ModelT], data: Any) -> ModelT:
    """Decode one API object, or raise MobilityDatabaseConnectionError.

    The body of a 200 is the server's data, so anything it holds that is not
    the documented object is an invalid payload, never a bare decoding error
    -- nor wrong data: every value's JSON type is checked against the model
    first (see :mod:`._payload`), since mashumaro would convert it.
    """
    if (problem := mismatch(model, data)) is not None:
        # The problem names a path of model fields and indices: no value.
        raise MobilityDatabaseConnectionError(
            f"API returned an invalid {model.__name__}: {problem}"
        )
    try:
        return model.from_dict(data)
    except _DECODE_ERRORS as err:
        # mashumaro's own text quotes the value (an echoed token, say): name
        # the field only, and raise outside the except, attaching nothing.
        field = getattr(err, "field_name", None) or "a field"
    raise MobilityDatabaseConnectionError(
        f"API returned an invalid {model.__name__}: {field} can't be read"
    )


def _many[ModelT: DataClassDictMixin](model: type[ModelT], data: Any) -> list[ModelT]:
    """Decode an API array of objects, or raise MobilityDatabaseConnectionError."""
    if not isinstance(data, list):
        raise MobilityDatabaseConnectionError(
            f"API returned {type(data).__name__}, not an array of {model.__name__}"
        )
    return [_one(model, item) for item in data]


def encode_params(params: dict[str, Any]) -> dict[str, str]:
    """Encode query parameters, dropping None values and empty containers.

    Booleans become "true"/"false", enums use their value, datetimes use ISO
    format, and lists/tuples are comma-joined (the API's convention for
    multi-value filters). An empty list or tuple means "no filter" and is
    dropped just like None, rather than encoding to an empty-string param.
    """
    return {
        key: _encode_value(value)
        for key, value in params.items()
        if value is not None and not (isinstance(value, list | tuple) and not value)
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
        """Initialize the client. Performs no I/O.

        Raises ``ValueError`` for an empty refresh token, or an injected
        session whose defaults would change what the client sends: an
        ``auth`` (it would ride to the token endpoint, and clash with the
        Bearer header), a ``base_url`` (aiohttp then refuses absolute URLs
        before 3.14), or a default header HTTP can't carry.
        """
        if not isinstance(refresh_token, str):
            raise TypeError("refresh_token must be a str")
        if not refresh_token:
            raise ValueError("refresh_token is empty")
        seconds = _timeout_seconds(request_timeout)
        if session is not None:
            _require_catalog_session(session)
        self._refresh_token = refresh_token
        self._session = session
        self._owns_session = session is None
        self._base_url = _require_base_url(base_url)
        self._timeout = aiohttp.ClientTimeout(total=seconds)
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
        # now + skew, never expiration - skew: an expiration in year 1 would
        # overflow the subtraction.
        return datetime.now(UTC) + skew >= self._token_expiration

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
            answer = await exchange(
                self._get_session(),
                "POST",
                f"{self._base_url}{TOKEN_PATH}",
                what="Token request",
                client_timeout=self._timeout,
                json={"refresh_token": self._refresh_token},
            )
            if answer.status != HTTPStatus.OK:
                raise MobilityDatabaseAuthenticationError(
                    f"Unable to obtain access token ({answer.status})",
                    status=answer.status,
                    body=_text(answer),
                )
            data = _decode(answer)
            token = _one(AccessToken, data)
            if token.token_type is not None and token.token_type.lower() != "bearer":
                # RFC 6749 section 7.1: a client MUST NOT use an access token
                # whose type it doesn't understand; this one sends Bearer.
                # (The type itself stays out of the message: server text.)
                raise MobilityDatabaseConnectionError(
                    "API returned a token that is no Bearer token"
                )
            if not _B64TOKEN.fullmatch(token.access_token):
                # It goes in an Authorization header: a CR, LF or space
                # there would fail every request, or forge another header.
                raise MobilityDatabaseConnectionError(
                    "API returned an access token that is no Bearer credential"
                )
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
        """Perform an authenticated request and return the decoded JSON body.

        On a 401 the token is refreshed and the request retried once (the
        401's connection already released: :func:`._transport.exchange`
        reads every answer whole).
        """
        # Encoded first: a bad value is the caller's error, before any request.
        encoded = encode_params(params) if params else None
        token = await self._async_ensure_token()
        answer = await exchange(
            self._get_session(),
            method,
            f"{self._base_url}{path}",
            what="API request",
            client_timeout=self._timeout,
            params=encoded,
            json=json_body,
            headers={"Authorization": f"Bearer {token}"},
        )
        if answer.status == HTTPStatus.UNAUTHORIZED:
            if not retry_on_auth_fail:
                raise MobilityDatabaseAuthenticationError(
                    "Request unauthorized after token refresh",
                    status=answer.status,
                    body=_text(answer),
                )
            await self._async_ensure_token(force=True, stale_token=token)
            return await self._request(
                method,
                path,
                params=params,
                json_body=json_body,
                retry_on_auth_fail=False,
            )
        if answer.status == HTTPStatus.NOT_FOUND:
            raise MobilityDatabaseNotFoundError("Not found")
        if answer.status == HTTPStatus.TOO_MANY_REQUESTS:
            raise MobilityDatabaseRateLimitError("API rate limit exceeded")
        # Only a 200 or 203 carries the answer (RFC 9110 section 15.3): a 204
        # has none, and a 3xx -- never followed -- is not the resource asked
        # for.
        if answer.status not in (
            HTTPStatus.OK,
            HTTPStatus.NON_AUTHORITATIVE_INFORMATION,
        ):
            raise MobilityDatabaseApiError(answer.status, _text(answer))
        return _decode(answer)

    async def get_metadata(self) -> Metadata:
        """Get metadata about the API."""
        data = await self._request("GET", "/v1/metadata")
        return _one(Metadata, data)

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
        return _many(Feed, data)

    async def get_feed(self, feed_id: str) -> Feed:
        """Get a single feed by ID."""
        data = await self._request("GET", f"/v1/feeds/{_quote_segment(feed_id)}")
        return _one(Feed, data)

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
        return _many(GtfsFeed, data)

    async def get_gtfs_feed(self, feed_id: str) -> GtfsFeed:
        """Get a single GTFS feed by ID."""
        data = await self._request("GET", f"/v1/gtfs_feeds/{_quote_segment(feed_id)}")
        return _one(GtfsFeed, data)

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
        return _many(GtfsRtFeed, data)

    async def get_gtfs_rt_feed(self, feed_id: str) -> GtfsRtFeed:
        """Get a single GTFS Realtime feed by ID."""
        data = await self._request(
            "GET", f"/v1/gtfs_rt_feeds/{_quote_segment(feed_id)}"
        )
        return _one(GtfsRtFeed, data)

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
        return _many(GbfsFeed, data)

    async def get_gbfs_feed(self, feed_id: str) -> GbfsFeed:
        """Get a single GBFS feed by ID."""
        data = await self._request("GET", f"/v1/gbfs_feeds/{_quote_segment(feed_id)}")
        return _one(GbfsFeed, data)

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
            f"/v1/gtfs_feeds/{_quote_segment(feed_id)}/datasets",
            params={
                "latest": latest,
                "limit": limit,
                "offset": offset,
                "downloaded_after": downloaded_after,
                "downloaded_before": downloaded_before,
            },
        )
        return _many(GtfsDataset, data)

    async def get_gtfs_feed_gtfs_rt_feeds(self, feed_id: str) -> list[GtfsRtFeed]:
        """Get GTFS Realtime feeds related to a GTFS feed."""
        data = await self._request(
            "GET", f"/v1/gtfs_feeds/{_quote_segment(feed_id)}/gtfs_rt_feeds"
        )
        return _many(GtfsRtFeed, data)

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
            f"/v1/gtfs_feeds/{_quote_segment(feed_id)}/availability",
            params={
                "from": checked_after,
                "to": checked_before,
                "limit": limit,
                "offset": offset,
                "sort": sort,
            },
        )
        return _one(GtfsFeedAvailability, data)

    async def get_dataset_gtfs(self, dataset_id: str) -> GtfsDataset:
        """Get a single GTFS dataset by ID."""
        data = await self._request(
            "GET", f"/v1/datasets/gtfs/{_quote_segment(dataset_id)}"
        )
        return _one(GtfsDataset, data)

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
        return _one(SearchResults, data)

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
        return _one(LocationSearchResults, data)

    async def get_licenses(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> list[License]:
        """Get all licenses in the database."""
        data = await self._request(
            "GET", "/v1/licenses", params={"limit": limit, "offset": offset}
        )
        return _many(License, data)

    async def get_license(self, license_id: str) -> LicenseWithRules:
        """Get a single license, including its rules."""
        data = await self._request("GET", f"/v1/licenses/{_quote_segment(license_id)}")
        return _one(LicenseWithRules, data)

    async def get_matching_licenses(self, license_url: str) -> list[MatchingLicense]:
        """Match a license URL against the license database."""
        if not isinstance(license_url, str):
            raise TypeError(
                f"license_url must be a str, not {type(license_url).__name__}"
            )
        data = await self._request(
            "POST",
            "/v1/licenses:match",
            json_body={"license_url": require_text(license_url, "license_url")},
        )
        return _many(MatchingLicense, data)
