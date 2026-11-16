"""Async client for the Mobility Database catalog API."""

from __future__ import annotations

import asyncio
import ipaddress
import json
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

from ._payload import mismatch
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


def _require_text(text: str, what: str) -> str:
    """Return ``text``, or raise ValueError when it isn't UTF-8 text.

    A lone surrogate has no bytes: yarl drops it from a query, so a filter
    would silently become another one -- or none.
    """
    try:
        text.encode("utf-8")
    except UnicodeEncodeError as err:
        raise ValueError(f"{what} holds a lone surrogate, not text") from err
    return text


# RFC 9110 section 5.1 field names; section 5.5 bars control characters but
# HTAB from values (aiohttp refuses the same ones, mid-request).
_FIELD_NAME = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+")
_FIELD_VALUE_FORBIDDEN = re.compile(r"[\x00-\x08\x0a-\x1f\x7f]")


def _require_catalog_session(session: aiohttp.ClientSession) -> None:
    """Raise ValueError for an injected session's default the client can't use."""
    if getattr(session, "auth", None) is not None:
        raise ValueError("the session's auth would go to the catalog API")
    if any(name.lower() == "authorization" for name in session.headers):
        # It would ride on the token request (data requests replace it).
        raise ValueError(
            "the session's Authorization header would go to the catalog API"
        )
    if getattr(session, "_base_url", None) is not None:
        raise ValueError("the session has a base_url; pass base_url= instead")
    for name, value in session.headers.items():
        if not _FIELD_NAME.fullmatch(name) or _FIELD_VALUE_FORBIDDEN.search(value):
            raise ValueError(f"the session's header {name!r} can't be sent")
        _require_text(value, f"the session's header {name!r}")


def _redact(text: str, *secrets: str | None) -> str:
    """Hide each secret in an error page's text, JSON-escaped form included.

    Pages that echo a request (debug pages, WAFs, gateways) would otherwise
    put a token into the exception's text, and so into logs.
    """
    for secret in secrets:
        if secret:
            for form in {secret, secret.replace("/", "\\/")}:
                text = text.replace(form, "[token]")
    return text


def _describe(err: BaseException) -> str:
    """Name a request error without its text, which may echo a token.

    A response error's message can quote a malformed header the server
    echoed from the request; a connection failure's names only the host.
    """
    if isinstance(err, aiohttp.ClientConnectorError):
        return str(err)
    if isinstance(err, aiohttp.ClientResponseError):
        return f"{type(err).__name__} ({err.status})"
    return type(err).__name__


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
    except (ValueError, IndexError) as err:
        raise ValueError(f"base_url {base_url!r} is no URL") from err
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


def _is_loopback(host: str) -> bool:
    """Whether a host names this machine (where plain http stays local)."""
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _encode_value(value: Any) -> str:
    """Encode a single query parameter value as a string (UTF-8 text only)."""
    return _require_text(_encode_scalar(value), "a query parameter")


def _encode_scalar(value: Any) -> str:
    """Encode a single query parameter value as a string."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list | tuple):
        return ",".join(_encode_value(item) for item in value)
    return str(value)


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
    return quote(segment, safe="")


# What decoding a type-checked JSON value into a model can still raise:
# mashumaro's MissingField (a LookupError) and InvalidFieldValue (a
# ValueError: an unknown enum value, an unparseable datetime).
_DECODE_ERRORS = (LookupError, ValueError)
# RFC 6750 section 2.1's b64token: all a Bearer credential may hold. Anything
# else (a CR or LF above all) can't go in an Authorization header.
_B64TOKEN = re.compile(r"[A-Za-z0-9\-._~+/]+=*")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Build a JSON object, refusing a name that appears twice."""
    obj = dict(pairs)
    if len(obj) != len(pairs):
        raise ValueError("a JSON object repeats a name")
    return obj


def _reject_constant(name: str) -> None:
    """Refuse ``NaN``/``Infinity``: Python's json accepts them, JSON does not."""
    raise ValueError(f"{name} is not valid JSON")


async def _json_body(resp: aiohttp.ClientResponse) -> Any:
    """Return a response's JSON, or raise MobilityDatabaseConnectionError.

    The content type must be ``application/json`` itself: aiohttp's own check
    also takes ``application/problem+json`` and the like, which aren't the
    documented answer. The body is parsed from its bytes, never by the
    response's ``charset``: RFC 8259 section 8.1 has JSON exchanged between
    systems be UTF-8, and its section 11 gives a charset parameter no effect
    on a compliant recipient -- read by it, a UTF-8 "Montr\u00e9al" sent as
    ``charset=iso-8859-1`` would be mojibake. Decoded as strict UTF-8 (a BOM
    ignored, as RFC 8259 allows): ``json.loads`` on the bytes would also
    take UTF-16/32 and let encoded surrogates through. A name repeated in an
    object (RFC 8259 section 4: unpredictable) and ``NaN``/``Infinity``
    (no JSON) are refused. Any of these is ValueError; nesting deeper than
    the interpreter's stack, RecursionError.
    """
    try:
        content_type = resp.content_type
    except ValueError:
        # aiohttp before 3.14 parses the header with email's parser, which
        # raises (UnicodeEncodeError) for an undecodable byte beside text --
        # in a charset, say, which has no effect anyway. Read the media type
        # itself, as later versions do.
        header = resp.headers.get("Content-Type", "")
        content_type = header.split(";", 1)[0].strip(" \t").lower()
    if content_type != "application/json":
        raise MobilityDatabaseConnectionError(
            f"API returned {content_type!r}, not application/json"
        )
    raw = await resp.read()
    try:
        return json.loads(
            raw.decode("utf-8-sig"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (ValueError, RecursionError) as err:
        raise MobilityDatabaseConnectionError("API returned invalid JSON") from err


async def _body_text(resp: aiohttp.ClientResponse, secrets: tuple[str, ...]) -> str:
    """Return an error response's body as text, whatever bytes it holds.

    ``resp.text()`` raises for bytes its charset (UTF-8 when none is given)
    can't decode -- a Latin-1 gateway page -- or for a charset that isn't a
    text encoding (LookupError: ``rot13``) or won't take the "replace"
    handler (UnicodeError: ``idna``, ``undefined``). The body only explains
    an error, so undecodable bytes are replaced, as UTF-8 when the charset
    can't be used.

    Each secret is hidden in the bytes before decoding, then in the text: a
    page echoing the request under another charset (``utf-16`` over ASCII)
    would otherwise decode into text the token is recoverable from.
    """
    try:
        raw = await resp.read()
    except aiohttp.ClientPayloadError:
        return ""  # unreadable (a Content-Encoding its bytes don't follow)
    for secret in secrets:
        if secret:
            raw = raw.replace(secret.encode("utf-8"), b"[token]")
    try:
        text = raw.decode(resp.charset or "utf-8", "replace")
    except (LookupError, ValueError, TypeError):
        # The charset is the server's, after RFC 2231 decoding: it can hold a
        # NUL (ValueError), and aiohttp 3.9 gives a tuple for charset*=
        # (TypeError). UnicodeError is a ValueError.
        text = raw.decode("utf-8", "replace")
    return _redact(text, *secrets)


def _one[ModelT: DataClassDictMixin](model: type[ModelT], data: Any) -> ModelT:
    """Decode one API object, or raise MobilityDatabaseConnectionError.

    The body of a 200 is the server's data, so anything it holds that is not
    the documented object is an invalid payload, never a bare decoding error
    -- nor wrong data: every value's JSON type is checked against the model
    first (see :mod:`._payload`), since mashumaro would convert it.
    """
    if (problem := mismatch(model, data)) is not None:
        raise MobilityDatabaseConnectionError(
            f"API returned an invalid {model.__name__}: {problem}"
        )
    try:
        return model.from_dict(data)
    except _DECODE_ERRORS as err:
        raise MobilityDatabaseConnectionError(
            f"API returned an invalid {model.__name__}: {err}"
        ) from err


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
        if not refresh_token:
            raise ValueError("refresh_token is empty")
        if session is not None:
            _require_catalog_session(session)
        self._refresh_token = refresh_token
        self._session = session
        self._owns_session = session is None
        self._base_url = _require_base_url(base_url)
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
            session = self._get_session()
            try:
                async with session.post(
                    f"{self._base_url}{TOKEN_PATH}",
                    json={"refresh_token": self._refresh_token},
                    timeout=self._timeout,
                    # RFC 6749 section 10.4: the refresh token goes to the
                    # authorization server only; a redirect would carry it
                    # elsewhere. Section 5.1 has the endpoint answer 200, so a
                    # 3xx is a non-200 like any other: an auth failure.
                    allow_redirects=False,
                    raise_for_status=False,  # whatever the session's default
                ) as resp:
                    if resp.status != HTTPStatus.OK:
                        # A page echoing the request must not put the refresh
                        # token into the exception's text (and so logs).
                        body = await _body_text(resp, (self._refresh_token,))
                        raise MobilityDatabaseAuthenticationError(
                            f"Unable to obtain access token ({resp.status}): {body}"
                        )
                    data = await _json_body(resp)
            except (TimeoutError, aiohttp.ClientError) as err:
                # Not from err: aiohttp's text can echo a malformed response
                # header -- the request's token with it -- and a traceback
                # prints the cause.
                raise MobilityDatabaseConnectionError(
                    f"Error requesting access token: {_describe(err)}"
                ) from None
            token = _one(AccessToken, data)
            if token.token_type is not None and token.token_type.lower() != "bearer":
                # RFC 6749 section 7.1: a client MUST NOT use an access token
                # whose type it doesn't understand; this one sends Bearer.
                raise MobilityDatabaseConnectionError(
                    f"API returned a {token.token_type!r} token, not a Bearer one"
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

        On a 401 the token is refreshed and the request retried once -- after
        this response is released, so a pool of one connection doesn't wait
        on the connection the refresh needs.
        """
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
                # A redirect is answered as the 3xx it is (not the resource
                # asked for: an API error), never followed: another origin's
                # body is no catalog data, and a same-origin Location with
                # credentials makes aiohttp raise a bare ValueError beside
                # the Bearer header (maintainer's choice).
                allow_redirects=False,
                raise_for_status=False,  # the status mapping below is ours
            ) as resp:
                if resp.status != HTTPStatus.UNAUTHORIZED:
                    return await self._answer(resp, path, (token, self._refresh_token))
                if not retry_on_auth_fail:
                    raise MobilityDatabaseAuthenticationError(
                        "Request unauthorized after token refresh"
                    )
        except MobilityDatabaseError:
            raise
        except (TimeoutError, aiohttp.ClientError) as err:
            # Not from err: see _async_ensure_token.
            raise MobilityDatabaseConnectionError(
                f"Error communicating with API: {_describe(err)}"
            ) from None
        await self._async_ensure_token(force=True, stale_token=token)
        return await self._request(
            method, path, params=params, json_body=json_body, retry_on_auth_fail=False
        )

    @staticmethod
    async def _answer(
        resp: aiohttp.ClientResponse, path: str, secrets: tuple[str, ...]
    ) -> Any:
        """Map a non-401 response to its JSON body or the matching error."""
        if resp.status == HTTPStatus.NOT_FOUND:
            raise MobilityDatabaseNotFoundError(f"Not found: {path}")
        if resp.status == HTTPStatus.TOO_MANY_REQUESTS:
            raise MobilityDatabaseRateLimitError("API rate limit exceeded")
        # Only a 200 or 203 carries the answer (RFC 9110 section 15.3): a 204
        # has none, and a 300 or a 3xx left unfollowed is not the resource
        # asked for.
        if resp.status not in (HTTPStatus.OK, HTTPStatus.NON_AUTHORITATIVE_INFORMATION):
            body = await _body_text(resp, secrets)
            raise MobilityDatabaseApiError(resp.status, body)
        return await _json_body(resp)

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
        data = await self._request(
            "POST",
            "/v1/licenses:match",
            json_body={"license_url": _require_text(license_url, "license_url")},
        )
        return _many(MatchingLicense, data)
