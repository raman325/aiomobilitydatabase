"""A 200 body that isn't the documented payload is an invalid response.

The README's error table: ``MobilityDatabaseConnectionError`` is a "network
failure, timeout, or invalid response body" -- never a bare ``ValueError``,
mashumaro error or ``RecursionError`` from decoding it.
"""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
import json
import math
import re
import types
import typing
from datetime import UTC, datetime, timedelta, timezone
from enum import Enum
from typing import Any

import aiohttp
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.client import MobilityDatabaseClient
from aiomobilitydatabase.exceptions import (
    MobilityDatabaseApiError,
    MobilityDatabaseAuthenticationError,
    MobilityDatabaseConnectionError,
    MobilityDatabaseError,
    MobilityDatabaseNotFoundError,
)
from aiomobilitydatabase.models import (
    Feed,
    GbfsFeed,
    GtfsDataset,
    GtfsFeed,
    GtfsFeedAvailability,
    GtfsRtFeed,
    License,
    LicenseWithRules,
    LocationSearchResults,
    MatchingLicense,
    Metadata,
    SearchResults,
)

from tests.fixtures import (
    AVAILABILITY_RESPONSE,
    GBFS_FEED,
    GTFS_DATASET,
    GTFS_FEED,
    GTFS_RT_FEED,
    LICENSE,
    LICENSE_WITH_RULES,
    LOCATION_SEARCH_RESPONSE,
    MATCHING_LICENSE,
    METADATA,
    SEARCH_RESPONSE,
    TOKEN_RESPONSE,
)
from tests.mock_server import MockApi

# method name -> (HTTP method, path, positional args, model, returns a list)
_ENDPOINTS: dict[str, tuple[str, str, tuple[str, ...], type, bool]] = {
    "get_metadata": ("GET", "/v1/metadata", (), Metadata, False),
    "get_feeds": ("GET", "/v1/feeds", (), Feed, True),
    "get_feed": ("GET", "/v1/feeds/mdb-1", ("mdb-1",), Feed, False),
    "get_gtfs_feeds": ("GET", "/v1/gtfs_feeds", (), GtfsFeed, True),
    "get_gtfs_feed": ("GET", "/v1/gtfs_feeds/mdb-1", ("mdb-1",), GtfsFeed, False),
    "get_gtfs_rt_feeds": ("GET", "/v1/gtfs_rt_feeds", (), GtfsRtFeed, True),
    "get_gtfs_rt_feed": (
        "GET",
        "/v1/gtfs_rt_feeds/mdb-1",
        ("mdb-1",),
        GtfsRtFeed,
        False,
    ),
    "get_gbfs_feeds": ("GET", "/v1/gbfs_feeds", (), GbfsFeed, True),
    "get_gbfs_feed": ("GET", "/v1/gbfs_feeds/gbfs-1", ("gbfs-1",), GbfsFeed, False),
    "get_gtfs_feed_datasets": (
        "GET",
        "/v1/gtfs_feeds/mdb-1/datasets",
        ("mdb-1",),
        GtfsDataset,
        True,
    ),
    "get_gtfs_feed_gtfs_rt_feeds": (
        "GET",
        "/v1/gtfs_feeds/mdb-1/gtfs_rt_feeds",
        ("mdb-1",),
        GtfsRtFeed,
        True,
    ),
    "get_gtfs_feed_availability": (
        "GET",
        "/v1/gtfs_feeds/mdb-1/availability",
        ("mdb-1",),
        GtfsFeedAvailability,
        False,
    ),
    "get_dataset_gtfs": (
        "GET",
        "/v1/datasets/gtfs/ds-1",
        ("ds-1",),
        GtfsDataset,
        False,
    ),
    "search_feeds": ("GET", "/v1/search", (), SearchResults, False),
    "get_locations": ("GET", "/v1/locations", (), LocationSearchResults, False),
    "get_licenses": ("GET", "/v1/licenses", (), License, True),
    "get_license": ("GET", "/v1/licenses/MIT", ("MIT",), LicenseWithRules, False),
    "get_matching_licenses": (
        "POST",
        "/v1/licenses:match",
        ("https://example.com/license",),
        MatchingLicense,
        True,
    ),
}

_JSON = st.recursive(
    st.none()
    | st.booleans()
    | st.integers()
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text(max_size=8),
    lambda inner: (
        st.lists(inner, max_size=3)
        | st.dictionaries(st.text(max_size=8), inner, max_size=3)
    ),
    max_leaves=12,
)
# Payloads near the documented shape reach the decoder's field checks, not
# just its "not an object" door: a real feed with one key replaced.
_NEAR_FEED = st.builds(
    lambda key, value: {**GTFS_FEED, key: value},
    st.sampled_from(sorted(GTFS_FEED)),
    _JSON,
)
_BODIES = _JSON | _NEAR_FEED | st.lists(_NEAR_FEED, max_size=2)


def _call(method_name: str, body: bytes) -> Any:
    """Answer ``method_name``'s request with ``body`` (a 200) and return the
    result, or the MobilityDatabaseConnectionError it raised."""
    http_method, path, args, _model, _many = _ENDPOINTS[method_name]

    async def scenario() -> Any:
        api = MockApi()
        await api.start()
        try:
            api.post("/v1/tokens", payload=TOKEN_RESPONSE)
            register = api.post if http_method == "POST" else api.get
            register(path, body=body, content_type="application/json")
            async with MobilityDatabaseClient("t", base_url=api.url()) as client:
                try:
                    return await getattr(client, method_name)(*args)
                except MobilityDatabaseConnectionError as err:
                    return err
        finally:
            await api.stop()

    return asyncio.run(scenario())


def _check(method_name: str, result: Any) -> None:
    _http_method, _path, _args, model, many = _ENDPOINTS[method_name]
    if isinstance(result, MobilityDatabaseConnectionError):
        return
    if many:
        assert isinstance(result, list)
        assert all(isinstance(item, model) for item in result)
    else:
        assert isinstance(result, model)


@given(method_name=st.sampled_from(sorted(_ENDPOINTS)), body=_BODIES)
@settings(max_examples=300, deadline=None)
def test_any_json_body_decodes_or_is_invalid(method_name: str, body: Any) -> None:
    _check(method_name, _call(method_name, json.dumps(body).encode()))


@pytest.mark.parametrize("method_name", sorted(_ENDPOINTS))
@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b"[" * 100_000 + b"]" * 100_000, id="nested-arrays"),
        pytest.param(b"7", id="number"),
        pytest.param(b'"text"', id="string"),
        pytest.param(b"null", id="null"),
        pytest.param(b"[1, 2]", id="array-of-numbers"),
        pytest.param(b'{"a": "b"}' * 2, id="two-objects"),
    ],
)
def test_a_malformed_body_is_a_connection_error(method_name: str, body: bytes) -> None:
    result = _call(method_name, body)
    assert isinstance(result, MobilityDatabaseConnectionError)


def _token_result(body: bytes) -> Any:
    """Answer the token request with ``body``; then serve real metadata."""

    async def scenario() -> Any:
        api = MockApi()
        await api.start()
        try:
            api.post("/v1/tokens", body=body, content_type="application/json")
            api.get("/v1/metadata", payload=METADATA)
            async with MobilityDatabaseClient("t", base_url=api.url()) as client:
                try:
                    return await client.get_metadata()
                except MobilityDatabaseConnectionError as err:
                    return err
        finally:
            await api.stop()

    return asyncio.run(scenario())


_TOKEN_BODIES = _JSON | st.builds(
    lambda key, value: {**TOKEN_RESPONSE, key: value},
    st.sampled_from(sorted(TOKEN_RESPONSE)),
    _JSON,
)


@given(body=_TOKEN_BODIES)
@settings(max_examples=150, deadline=None)
def test_any_token_body_decodes_or_is_invalid(body: Any) -> None:
    """A drawn body is occasionally a token (``"20260101"`` is an ISO 8601
    date): then the request goes through. Anything else is invalid."""
    result = _token_result(json.dumps(body).encode())
    assert isinstance(result, MobilityDatabaseConnectionError | Metadata)


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b"[" * 100_000 + b"]" * 100_000, id="nested"),
        pytest.param(b"[]", id="array"),
        pytest.param(b'{"access_token": "a"}', id="missing-expiration"),
        pytest.param(
            b'{"access_token": "a", "expiration_datetime_utc": "soon"}',
            id="bad-expiration",
        ),
    ],
)
def test_a_malformed_token_body_is_a_connection_error(body: bytes) -> None:
    assert isinstance(_token_result(body), MobilityDatabaseConnectionError)


# Every method interpolating its argument into the path (the license
# matcher sends its URL in the body).
_ID_METHODS = sorted(
    name
    for name, (_method, _path, args, _model, _many) in _ENDPOINTS.items()
    if args and name != "get_matching_licenses"
)


@pytest.mark.parametrize("method_name", _ID_METHODS)
@pytest.mark.parametrize("feed_id", ["", ".", ".."])
async def test_an_id_that_is_no_path_segment_is_refused(
    mock_api: MockApi, client: MobilityDatabaseClient, method_name: str, feed_id: str
) -> None:
    """ "/v1/feeds/." resolves to "/v1/feeds/" and ".." to "/v1/": the request
    would reach another endpoint. Percent-encoding cannot help (``%2E`` is
    ``.``), so the id is refused before anything is sent."""
    with pytest.raises(ValueError, match="no path segment"):
        await getattr(client, method_name)(feed_id)
    assert mock_api.requests == []


@pytest.mark.parametrize("feed_id", ["...", ".a", "a.", "a..b"])
async def test_other_dotted_ids_are_ordinary_segments(
    mock_api: MockApi, client: MobilityDatabaseClient, feed_id: str
) -> None:
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get(f"/v1/feeds/{feed_id}", payload={**GTFS_FEED, "id": feed_id})
    feed = await client.get_feed(feed_id)
    assert feed.id == feed_id


# -- Decoded data holds its annotated types -----------------------------------------


def _faithful(annotation: Any, raw: Any, decoded: Any) -> bool:  # noqa: PLR0911
    """Whether ``decoded`` is ``raw`` itself, read as ``annotation`` says.

    A value equal to its JSON, of the JSON type the annotation names -- so a
    conversion (bool("false"), str(5), int(2.9), a string's characters as a
    tuple) is caught even when it yields the annotated type.
    """
    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        if raw is None:
            return decoded is None
        options = [o for o in typing.get_args(annotation) if o is not type(None)]
        return any(_faithful(option, raw, decoded) for option in options)
    if origin is tuple:
        item = typing.get_args(annotation)[0]
        return (
            isinstance(raw, list)
            and isinstance(decoded, tuple)
            and len(raw) == len(decoded)
            and all(_faithful(item, r, d) for r, d in zip(raw, decoded, strict=True))
        )
    if dataclasses.is_dataclass(annotation):
        hints = typing.get_type_hints(annotation)
        # A field the JSON leaves out decodes to None (every default is
        # None): the decoder must invent nothing, such as an empty list.
        return isinstance(raw, dict) and all(
            _faithful(hints[field.name], raw[field.name], getattr(decoded, field.name))
            if field.name in raw
            else getattr(decoded, field.name) is None
            for field in dataclasses.fields(annotation)
        )
    if annotation is datetime:
        return isinstance(raw, str) and decoded == _rfc3339(raw)
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return (
            isinstance(raw, str)
            and type(decoded) is annotation
            and decoded == annotation(raw)
        )
    if annotation is float:
        # A JSON number is read as a double (RFC 8259 section 6): an integer
        # past 2**53 rounds, as 0.1 does, so compare against float(raw).
        return (
            type(raw) in (int, float)
            and type(decoded) is float
            and math.isfinite(decoded)
            and decoded == float(raw)
        )
    return type(raw) is annotation and type(decoded) is annotation and decoded == raw


_RFC3339 = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})[Tt ]([0-9]{2}):([0-9]{2}):([0-9]{2})"
    r"(?:\.([0-9]+))?(?:(Z)|([+-])([0-9]{2}):([0-9]{2}))?"
)


def _rfc3339(text: str) -> datetime | None:
    """An independent reading of an RFC 3339 date-time (offset optional):
    the reference the decoder must agree with, not its own parser."""
    match = _RFC3339.fullmatch(text)
    if match is None:
        return None
    year, month, day, hour, minute, second, fraction, zulu, sign, oh, om = (
        match.groups()
    )
    if (
        int(hour) > 23
        or int(minute) > 59
        or int(second) > 59
        or int(oh or 0) > 23
        or int(om or 0) > 59
    ):
        return None
    zone = None
    if zulu:
        zone = UTC
    elif sign:
        offset = timedelta(hours=int(oh), minutes=int(om))
        zone = timezone(-offset if sign == "-" else offset)
    micro = int((fraction or "0")[:6].ljust(6, "0"))
    try:
        return datetime(
            int(year),
            int(month),
            int(day),
            int(hour),
            int(minute),
            int(second),
            micro,
            zone,
        )
    except ValueError:  # no such day, or year 0
        return None


def _flip(payload: Any, data: st.DataObject) -> Any:
    """``payload`` with one value, at any depth, replaced by arbitrary JSON."""
    if isinstance(payload, dict) and payload:
        key = data.draw(st.sampled_from(sorted(payload)))
        if data.draw(st.integers(0, 4)) == 0:  # sometimes the key goes missing
            return {k: v for k, v in payload.items() if k != key}
        deeper = data.draw(st.booleans())
        replaced = _flip(payload[key], data) if deeper else data.draw(_JSON)
        return {**payload, key: replaced}
    if isinstance(payload, list) and payload:
        index = data.draw(st.integers(0, len(payload) - 1))
        return [*payload[:index], _flip(payload[index], data), *payload[index + 1 :]]
    return data.draw(_JSON)


_PAYLOADS = {
    "get_metadata": METADATA,
    "get_feed": GTFS_FEED,
    "get_gtfs_feed": {
        **GTFS_FEED,
        "redirects": [{"target_id": "mdb-2", "comment": "moved"}],
    },
    "get_gtfs_rt_feed": GTFS_RT_FEED,
    "get_gbfs_feed": GBFS_FEED,
    "get_dataset_gtfs": GTFS_DATASET,
    "get_gtfs_feed_availability": AVAILABILITY_RESPONSE,
    "search_feeds": SEARCH_RESPONSE,
    "get_locations": LOCATION_SEARCH_RESPONSE,
    "get_license": LICENSE_WITH_RULES,
    "get_licenses": [LICENSE],
    "get_matching_licenses": [MATCHING_LICENSE],
    "get_gtfs_feed_datasets": [GTFS_DATASET],
    "get_feeds": [GTFS_FEED, GTFS_RT_FEED],
    "get_gtfs_feeds": [GTFS_FEED],
    "get_gtfs_rt_feeds": [GTFS_RT_FEED],
    "get_gbfs_feeds": [GBFS_FEED],
    "get_gtfs_feed_gtfs_rt_feeds": [GTFS_RT_FEED],
}


@given(method_name=st.sampled_from(sorted(_PAYLOADS)), data=st.data())
@settings(max_examples=400, deadline=None)
def test_a_decoded_payload_is_its_json_read_as_annotated(
    method_name: str, data: st.DataObject
) -> None:
    """mashumaro converts rather than checks (bool("false") is True): a
    real payload with one value anywhere replaced either decodes faithfully
    -- every value its JSON, read as annotated -- or is an invalid payload."""
    body = _flip(_PAYLOADS[method_name], data)
    result = _call(method_name, json.dumps(body).encode())
    if isinstance(result, MobilityDatabaseConnectionError):
        return
    model = _ENDPOINTS[method_name][3]
    if isinstance(result, list):
        assert isinstance(body, list)
        assert all(_faithful(model, r, d) for r, d in zip(body, result, strict=True))
    else:
        assert _faithful(model, body, result), result


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("official", "false"),  # bool("false") is True
        ("official", 0),
        ("id", ["x"]),
        ("feed_name", {"x": 1}),
        ("feed_name", 5),
        ("external_ids", "abc"),  # a tuple field would take its characters
    ],
)
def test_a_wrong_json_type_is_an_invalid_payload(key: str, value: Any) -> None:
    result = _call("get_gtfs_feed", json.dumps({**GTFS_FEED, key: value}).encode())
    assert isinstance(result, MobilityDatabaseConnectionError)


def test_a_null_required_string_is_an_invalid_payload() -> None:
    """str(None) would make it the text "None"."""
    body = {**AVAILABILITY_RESPONSE, "feed_id": None}
    result = _call("get_gtfs_feed_availability", json.dumps(body).encode())
    assert isinstance(result, MobilityDatabaseConnectionError)


def test_a_wrong_type_deep_inside_is_an_invalid_payload() -> None:
    bounding_box = {
        **GTFS_FEED["latest_dataset"]["bounding_box"],
        "minimum_latitude": "33",
    }
    latest = {**GTFS_FEED["latest_dataset"], "bounding_box": bounding_box}
    result = _call(
        "get_gtfs_feed", json.dumps({**GTFS_FEED, "latest_dataset": latest}).encode()
    )
    assert isinstance(result, MobilityDatabaseConnectionError)
    assert "latest_dataset.bounding_box.minimum_latitude" in str(result)


@pytest.mark.parametrize("number", [b"1e400", b"-1e400", b"1" + b"0" * 400, b"true"])
def test_a_number_no_float_holds_is_an_invalid_payload(number: bytes) -> None:
    payload = json.dumps(GTFS_FEED).encode()
    body = payload.replace(
        b'"minimum_latitude": 33.721601', b'"minimum_latitude": ' + number
    )
    assert body != payload
    assert isinstance(_call("get_gtfs_feed", body), MobilityDatabaseConnectionError)


# -- Bodies that don't decode as text -----------------------------------------------


async def test_an_undecodable_error_page_is_an_api_error(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """A gateway's Latin-1 page with no charset: aiohttp would decode it as
    UTF-8 and raise. The body only explains the error, so it is replaced."""
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get(
        "/v1/metadata",
        status=502,
        body="Bad Gateway \xe9".encode("latin-1"),
        content_type="text/html",
    )
    with pytest.raises(MobilityDatabaseApiError) as caught:
        await client.get_metadata()
    assert caught.value.status == 502
    assert caught.value.body == "Bad Gateway �"


_CHARSETS = ["iso-8859-1", "cp037", "rot13", "base64", "zlib", "undefined", "utf-16"]


@pytest.mark.parametrize("charset", _CHARSETS)
async def test_a_json_body_is_utf_8_whatever_its_charset_says(
    mock_api: MockApi, client: MobilityDatabaseClient, charset: str
) -> None:
    """RFC 8259 sections 8.1 and 11: JSON between systems is UTF-8, and a
    charset parameter has no effect on a compliant recipient -- read by it,
    "Montr\u00e9al" became mojibake (iso-8859-1) or an error (rot13)."""
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    body = json.dumps({**METADATA, "version": "Montr\u00e9al"}, ensure_ascii=False)
    mock_api.get(
        "/v1/metadata",
        body=body.encode("utf-8"),
        headers={"Content-Type": f"application/json; charset={charset}"},
    )
    assert (await client.get_metadata()).version == "Montr\u00e9al"


@pytest.mark.parametrize("charset", _CHARSETS)
async def test_a_token_body_is_utf_8_whatever_its_charset_says(
    mock_api: MockApi, client: MobilityDatabaseClient, charset: str
) -> None:
    mock_api.post(
        "/v1/tokens",
        body=json.dumps(TOKEN_RESPONSE).encode(),
        headers={"Content-Type": f"application/json; charset={charset}"},
    )
    mock_api.get("/v1/metadata", payload=METADATA)
    await client.get_metadata()


async def test_a_body_that_isnt_utf_8_is_invalid_json(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """A Latin-1 body is no JSON text, whatever charset it names."""
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get(
        "/v1/metadata",
        body='{"version": "Montr\u00e9al"}'.encode("latin-1"),
        headers={"Content-Type": "application/json; charset=iso-8859-1"},
    )
    with pytest.raises(MobilityDatabaseConnectionError, match="invalid JSON"):
        await client.get_metadata()


@pytest.mark.parametrize(
    "content_type", ["application/problem+json", "application/json-seq", "text/plain"]
)
async def test_a_200_that_isnt_application_json_is_invalid(
    mock_api: MockApi, client: MobilityDatabaseClient, content_type: str
) -> None:
    """aiohttp's own check took problem+json, which decoded as an empty
    Metadata."""
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/metadata", body=b"{}", headers={"Content-Type": content_type})
    with pytest.raises(MobilityDatabaseConnectionError, match="not application/json"):
        await client.get_metadata()


async def test_a_401_releases_its_connection_before_refreshing(
    mock_api: MockApi,
) -> None:
    """With a pool of one connection and a 401 too big to buffer, the token
    refresh waited for the connection the 401 still held, until timeout."""
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get(
        "/v1/metadata", status=401, body=b"x" * 2_000_000, content_type="text/plain"
    )
    mock_api.post("/v1/tokens", payload={**TOKEN_RESPONSE, "access_token": "second"})
    mock_api.get("/v1/metadata", payload=METADATA, auth="second")
    connector = aiohttp.TCPConnector(limit=1)
    async with aiohttp.ClientSession(connector=connector) as session:
        client = MobilityDatabaseClient(
            "t", session, base_url=mock_api.url(), request_timeout=5
        )
        assert await client.get_metadata() == Metadata.from_dict(METADATA)


async def test_an_availability_without_its_checks_is_invalid(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """The API spec requires checks: absent, it isn't an empty history."""
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    body = {
        key: value for key, value in AVAILABILITY_RESPONSE.items() if key != "checks"
    }
    mock_api.get("/v1/gtfs_feeds/mdb-1/availability", payload=body)
    with pytest.raises(MobilityDatabaseConnectionError):
        await client.get_gtfs_feed_availability("mdb-1")


@pytest.mark.parametrize(
    "value",
    [
        "2024-01-01",  # midnight, a time never sent
        "2024-01-01T24:00:00Z",
        "20240101T000000Z",
        "2024-W01-1T00:00:00Z",
        "2024-01-01T00:00",
        "2024-01-01T00:00:00+0100",
        "2024-01-01T00:00:00z",  # fromisoformat refuses a lowercase z
    ],
)
def test_a_date_time_rfc_3339_lacks_is_an_invalid_payload(value: str) -> None:
    result = _call(
        "get_gtfs_feed", json.dumps({**GTFS_FEED, "created_at": value}).encode()
    )
    assert isinstance(result, MobilityDatabaseConnectionError)


@pytest.mark.parametrize(
    "value",
    [
        "2024-01-01T00:00:00Z",
        "2024-01-01t00:00:00.5Z",
        "2024-01-01 00:00:00-05:00",
        "2024-01-01T00:00:00",
    ],
)
def test_an_rfc_3339_date_time_decodes(value: str) -> None:
    result = _call(
        "get_gtfs_feed", json.dumps({**GTFS_FEED, "created_at": value}).encode()
    )
    assert not isinstance(result, MobilityDatabaseConnectionError)
    assert result.created_at == _rfc3339(value)


async def test_a_token_error_with_an_undecodable_body_is_still_an_auth_error(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """Any non-200 from the token endpoint is an authentication failure,
    whatever bytes its body holds. (One that can't be read at all -- a
    Content-Encoding its bytes don't follow -- is a connection error.)"""
    mock_api.post(
        "/v1/tokens",
        status=500,
        body="Erreur \xe9".encode("latin-1"),
        content_type="text/plain",
    )
    with pytest.raises(MobilityDatabaseAuthenticationError) as caught:
        await client.get_metadata()
    assert (caught.value.status, caught.value.body) == (500, "Erreur �")


# -- The token's own content ----------------------------------------------------


@pytest.mark.parametrize(
    "access_token",
    ["a\r\nX-Evil: 1", "a b", "", "tokén", "a=b", "\t"],
)
async def test_an_access_token_that_is_no_bearer_credential_is_refused(
    mock_api: MockApi, client: MobilityDatabaseClient, access_token: str
) -> None:
    """RFC 6750 section 2.1 b64token: anything else can't go in the
    Authorization header (a CR LF would forge another header)."""
    mock_api.post(
        "/v1/tokens", payload={**TOKEN_RESPONSE, "access_token": access_token}
    )
    with pytest.raises(MobilityDatabaseConnectionError, match="no Bearer credential"):
        await client.get_metadata()
    assert [r.path for r in mock_api.requests] == ["/v1/tokens"]


async def test_a_token_expiring_in_year_one_is_refreshed_not_an_overflow(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """The first call uses it; the second sees it expired and fetches
    another, rather than overflowing expiration minus the skew."""
    expired = {**TOKEN_RESPONSE, "expiration_datetime_utc": "0001-01-01T00:00:00+00:00"}
    mock_api.post("/v1/tokens", payload=expired)
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/metadata", payload=METADATA)
    mock_api.get("/v1/metadata", payload=METADATA)
    await client.get_metadata()
    await client.get_metadata()
    assert [r.path for r in mock_api.requests].count("/v1/tokens") == 2


# -- Every endpoint is covered; ids --------------------------------------------------


def test_every_public_endpoint_is_in_the_table() -> None:
    """A catalog method added without _one/_many would otherwise go untested."""
    methods = {
        name
        for name, member in inspect.getmembers(
            MobilityDatabaseClient, inspect.iscoroutinefunction
        )
        if not name.startswith("_") and name != "close"
    }
    assert methods == set(_ENDPOINTS)


@pytest.mark.parametrize("feed_id", [b"..", b"mdb-1", 1, None])
async def test_an_id_that_is_no_str_is_refused(
    mock_api: MockApi, client: MobilityDatabaseClient, feed_id: Any
) -> None:
    """b".." isn't "..": without the type check it reached /v1/."""
    with pytest.raises(TypeError, match="must be a str"):
        await client.get_feed(feed_id)
    assert mock_api.requests == []


def _sent_path(feed_id: str) -> str | None:
    """The decoded path the server saw for get_feed(feed_id); None if refused."""

    async def scenario() -> str | None:
        api = MockApi()
        await api.start()
        try:
            api.post("/v1/tokens", payload=TOKEN_RESPONSE)
            api.get(f"/v1/feeds/{feed_id}", payload={**GTFS_FEED, "id": "x"})
            async with MobilityDatabaseClient("t", base_url=api.url()) as client:
                try:
                    await client.get_feed(feed_id)
                except ValueError:
                    return None
                except MobilityDatabaseError:
                    pass
            gets = [r for r in api.requests if r.method == "GET"]
            assert len(gets) == 1
            return gets[0].path
        finally:
            await api.stop()

    return asyncio.run(scenario())


@given(feed_id=st.text(max_size=10))
@settings(max_examples=200, deadline=None)
def test_any_str_id_reaches_the_server_as_itself(feed_id: str) -> None:
    """Refused (ValueError: no segment, or no UTF-8 for a lone surrogate),
    or the server decodes exactly the id -- one segment, whatever it holds."""
    sent = _sent_path(feed_id)
    if sent is None:
        assert feed_id in ("", ".", "..") or not _utf8(feed_id)
    else:
        assert sent == f"/v1/feeds/{feed_id}"


def _utf8(text: str) -> bool:
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


async def test_an_error_page_in_no_text_encoding_is_an_api_error(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """charset=rot13 is no text encoding: the page is read as UTF-8."""
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get(
        "/v1/metadata",
        status=503,
        body=b"Unavailable",
        headers={"Content-Type": "text/plain; charset=rot13"},
    )
    with pytest.raises(MobilityDatabaseApiError) as caught:
        await client.get_metadata()
    assert caught.value.body == "Unavailable"


@pytest.mark.parametrize("charset", ["idna", "undefined"])
async def test_an_error_page_whose_charset_refuses_replace_is_an_api_error(
    mock_api: MockApi, client: MobilityDatabaseClient, charset: str
) -> None:
    """These codecs exist but raise UnicodeError for the "replace" handler
    (or for any decoding): the page is read as UTF-8 instead."""
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get(
        "/v1/metadata",
        status=502,
        body=b"Bad Gateway",
        headers={"Content-Type": f"text/html; charset={charset}"},
    )
    with pytest.raises(MobilityDatabaseApiError) as caught:
        await client.get_metadata()
    assert caught.value.body == "Bad Gateway"


@pytest.mark.parametrize("charset", ["idna", "undefined"])
async def test_a_token_error_whose_charset_refuses_replace_is_an_auth_error(
    mock_api: MockApi, client: MobilityDatabaseClient, charset: str
) -> None:
    mock_api.post(
        "/v1/tokens",
        status=500,
        body=b"Internal error",
        headers={"Content-Type": f"text/plain; charset={charset}"},
    )
    with pytest.raises(MobilityDatabaseAuthenticationError) as caught:
        await client.get_metadata()
    assert caught.value.body == "Internal error"


@pytest.mark.parametrize("token_type", ["mac", "DPoP", "basic", ""])
async def test_a_token_of_another_type_is_refused(
    mock_api: MockApi, client: MobilityDatabaseClient, token_type: str
) -> None:
    """RFC 6749 section 7.1: a client MUST NOT use an access token whose
    type it doesn't understand -- this client sends only Bearer."""
    mock_api.post("/v1/tokens", payload={**TOKEN_RESPONSE, "token_type": token_type})
    with pytest.raises(MobilityDatabaseConnectionError, match="not a Bearer one"):
        await client.get_metadata()
    assert [r.path for r in mock_api.requests] == ["/v1/tokens"]


@pytest.mark.parametrize("token_type", ["Bearer", "bearer", "BEARER", None])
async def test_a_bearer_token_or_an_untyped_one_is_used(
    mock_api: MockApi, client: MobilityDatabaseClient, token_type: str | None
) -> None:
    """The type compares case-insensitively (RFC 6749 section 5.1); a
    response that states none is taken as the Bearer the API issues."""
    body = {**TOKEN_RESPONSE, "token_type": token_type}
    mock_api.post("/v1/tokens", payload=body)
    mock_api.get("/v1/metadata", payload=METADATA)
    await client.get_metadata()
    assert mock_api.requests[-1].headers["Authorization"].startswith("Bearer ")


def test_a_float_field_reads_a_big_integer_as_its_double() -> None:
    """2**53 + 1 has no exact double: it rounds, as any JSON number does."""
    body = (
        json.dumps(GTFS_FEED)
        .encode()
        .replace(
            b'"minimum_latitude": 33.721601', b'"minimum_latitude": 9007199254740993'
        )
    )
    feed = _call("get_gtfs_feed", body)
    assert feed.latest_dataset.bounding_box.minimum_latitude == float(2**53 + 1)


# -- Bodies json.dumps never writes ---------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b'{"version": "mdb-\xed\xa0\x80"}', id="encoded-surrogate"),
        pytest.param(b'{"version": "caf\xe9"}', id="latin-1"),
        pytest.param('{"version": "x"}'.encode("utf-16-le"), id="utf-16"),
        pytest.param('{"version": "x"}'.encode("utf-32"), id="utf-32"),
        pytest.param(b'{"version": "\\ud800"}', id="escaped-lone-surrogate"),
        pytest.param(b'{"version": 5, "version": "2"}', id="repeated-name"),
        pytest.param(b'{"version": "2", "x": NaN}', id="nan"),
        pytest.param(b'{"version": "2", "x": -Infinity}', id="infinity"),
    ],
)
def test_a_body_that_is_no_utf_8_json_text_is_invalid(body: bytes) -> None:
    """RFC 8259: UTF-8 (section 8.1), unique names (section 4), no NaN.
    json.loads on bytes took UTF-16/32 and encoded surrogates; an escaped
    lone surrogate is no text either (an id holding one can't be sent back)."""
    assert isinstance(_call("get_metadata", body), MobilityDatabaseConnectionError)


def test_a_bom_and_an_escaped_pair_are_fine() -> None:
    body = b'\xef\xbb\xbf{"version": "\\ud83d\\ude8c bus"}'
    assert _call("get_metadata", body).version == "\U0001f68c bus"


def test_a_repeated_name_deep_inside_is_invalid() -> None:
    body = (
        json.dumps(GTFS_FEED)
        .encode()
        .replace(b'"country_code": "US"', b'"country_code": "US", "country_code": "CA"')
    )
    assert b'"CA"' in body
    assert isinstance(_call("get_gtfs_feed", body), MobilityDatabaseConnectionError)


# -- The token request stays with the token endpoint ---------------------------------


@pytest.mark.parametrize("status", [301, 302, 307, 308])
async def test_the_token_request_follows_no_redirect(
    mock_api: MockApi, client: MobilityDatabaseClient, status: int
) -> None:
    """RFC 6749 section 10.4: the refresh token goes to the authorization
    server only -- a 307 re-POSTed it to wherever Location said, and the
    client used the access token that server handed back."""
    elsewhere = MockApi()
    await elsewhere.start()
    try:
        elsewhere.post("/steal", payload=TOKEN_RESPONSE)
        mock_api.post(
            "/v1/tokens", status=status, headers={"Location": elsewhere.url("/steal")}
        )
        with pytest.raises(MobilityDatabaseAuthenticationError, match=str(status)):
            await client.get_metadata()
        assert elsewhere.requests == []
    finally:
        await elsewhere.stop()


# -- Absent lists, unreadable error bodies ----------------------------------------


@pytest.mark.parametrize(
    ("method_name", "path", "payload"),
    [
        ("search_feeds", "/v1/search", {"total": 5}),
        ("get_locations", "/v1/locations", {"total": 5}),
    ],
)
async def test_an_absent_result_list_is_none_not_empty(
    mock_api: MockApi,
    client: MobilityDatabaseClient,
    method_name: str,
    path: str,
    payload: dict[str, Any],
) -> None:
    """results is optional in the API spec: absent beside total=5, it is
    unknown, not an empty page."""
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get(path, payload=payload)
    mock_api.get(path, payload={**payload, "results": []})
    assert (await getattr(client, method_name)()).results is None
    assert (await getattr(client, method_name)()).results == ()


async def test_an_unreadable_error_body_keeps_its_status(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """The status is known even when the body can't be read."""
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get(
        "/v1/metadata",
        status=500,
        body=b"not gzip",
        headers={"Content-Type": "text/plain", "Content-Encoding": "gzip"},
    )
    with pytest.raises(MobilityDatabaseApiError) as caught:
        await client.get_metadata()
    assert (caught.value.status, caught.value.body) == (500, "")


async def test_an_unreadable_token_error_body_is_still_an_auth_error(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    mock_api.post(
        "/v1/tokens",
        status=500,
        body=b"not gzip",
        headers={"Content-Type": "text/plain", "Content-Encoding": "gzip"},
    )
    with pytest.raises(MobilityDatabaseAuthenticationError):
        await client.get_metadata()


# -- Date-times near the RFC 3339 shape ----------------------------------------------

_DATE_PARTS = st.tuples(
    st.sampled_from(["2024", "0001", "9999", "0000", "24"]),
    st.sampled_from(["-01", "-02", "-12", "-13", "01", "-W01"]),
    st.sampled_from(["-01", "-29", "-30", "-31", "-1", ""]),
    st.sampled_from(["T", "t", " ", "", "_"]),
    st.sampled_from(
        ["00:00:00", "23:59:59", "24:00:00", "12:60:00", "12:00:60", "12:00", "120000"]
    ),
    st.sampled_from(["", ".5", ".123456789", "."]),
    st.sampled_from(
        ["", "Z", "z", "+00:00", "-05:30", "+0100", "+24:00", "+23:59", "+05:60"]
    ),
)


@given(parts=_DATE_PARTS)
@settings(max_examples=400, deadline=None)
def test_a_date_time_decodes_exactly_when_rfc_3339_reads_it(
    parts: tuple[str, ...],
) -> None:
    """Accepted iff the independent RFC 3339 reading (offset optional, "Z"
    upper case only, a real day) gives an instant, and then to that one."""
    text = "".join(parts)
    result = _call(
        "get_gtfs_feed", json.dumps({**GTFS_FEED, "created_at": text}).encode()
    )
    expected = _rfc3339(text)
    if expected is None:
        assert isinstance(result, MobilityDatabaseConnectionError), text
    else:
        assert not isinstance(result, MobilityDatabaseConnectionError), text
        assert result.created_at == expected


# -- Redirects on data requests are answered, not followed ---------------------------


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
@pytest.mark.parametrize(
    "location",
    ["same-origin-with-credentials", "other-origin", "ftp://x/", "http:/path"],
)
async def test_a_data_redirect_is_an_api_error_never_followed(
    mock_api: MockApi, client: MobilityDatabaseClient, status: int, location: str
) -> None:
    """A same-origin Location with credentials made aiohttp raise a bare
    ValueError beside the Bearer header; another origin's body would have
    been read as catalog data (maintainer's choice: never follow)."""
    elsewhere = MockApi()
    await elsewhere.start()
    try:
        target = {
            "same-origin-with-credentials": mock_api.url("/x").replace(
                "http://", "http://u:p@"
            ),
            "other-origin": elsewhere.url("/v1/metadata"),
        }.get(location, location)
        elsewhere.get("/v1/metadata", payload={"version": "elsewhere"})
        mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
        mock_api.get("/v1/metadata", status=status, headers={"Location": target})
        with pytest.raises(MobilityDatabaseApiError) as caught:
            await client.get_metadata()
        assert caught.value.status == status
        assert elsewhere.requests == []
    finally:
        await elsewhere.stop()


# -- Error pages, whatever their Content-Type says -------------------------------------

_PARAM_VALUES = st.sampled_from(
    [
        "utf-8",
        "utf%00",
        "utf-8''utf%00",
        "utf-8''utf-8",
        "rot13",
        "idna",
        "undefined",
        'x"y',
        "",
        "%ff",
    ]
)
_CONTENT_TYPES = st.tuples(
    st.sampled_from(["text/plain", "text/html", "application/json", "x/y"]),
    st.lists(
        st.tuples(
            st.sampled_from(["charset", "charset*", "q", "charset*0*"]), _PARAM_VALUES
        ),
        max_size=3,
    ),
).map(lambda parts: parts[0] + "".join(f"; {name}={value}" for name, value in parts[1]))


@given(
    status=st.sampled_from([400, 500, 502, 503]),
    content_type=_CONTENT_TYPES,
    body=st.binary(max_size=16),
    on_token=st.booleans(),
)
@settings(max_examples=200, deadline=None)
def test_an_error_page_is_its_status_whatever_its_content_type(
    status: int, content_type: str, body: bytes, on_token: bool
) -> None:
    """The charset is the server's, after RFC 2231 decoding (a NUL, or on
    aiohttp 3.9 a tuple): the error is still the status's -- an API error,
    or from the token endpoint an authentication failure."""

    async def scenario() -> Any:
        api = MockApi()
        await api.start()
        try:
            page = {
                "status": status,
                "body": body,
                "headers": {"Content-Type": content_type},
            }
            if on_token:
                api.post("/v1/tokens", **page)
            else:
                api.post("/v1/tokens", payload=TOKEN_RESPONSE)
                api.get("/v1/metadata", **page)
            async with MobilityDatabaseClient("t", base_url=api.url()) as client:
                try:
                    await client.get_metadata()
                except MobilityDatabaseError as err:
                    return err
        finally:
            await api.stop()

    result = asyncio.run(scenario())
    expected = (
        MobilityDatabaseAuthenticationError if on_token else MobilityDatabaseApiError
    )
    assert isinstance(result, expected), result


async def test_an_echoing_token_error_page_hides_the_refresh_token(
    mock_api: MockApi,
) -> None:
    """A proxy page echoing the request would have put the refresh token in
    the exception's text, and so in logs."""
    mock_api.post(
        "/v1/tokens",
        status=403,
        body=b'echo: {"refresh_token": "SECRET-RT"}',
        content_type="text/plain",
    )
    async with MobilityDatabaseClient("SECRET-RT", base_url=mock_api.url()) as client:
        with pytest.raises(MobilityDatabaseAuthenticationError) as caught:
            await client.get_metadata()
    assert "SECRET-RT" not in str(caught.value)  # the message is the library's
    assert "SECRET-RT" in caught.value.body  # the page itself: the caller's to log


# -- Query values, tokens in error pages, injected sessions ---------------------------


def _sent_query(value: str) -> str | None:
    """The search_query the server saw for search_feeds(value); None if refused."""

    async def scenario() -> str | None:
        api = MockApi()
        await api.start()
        try:
            api.post("/v1/tokens", payload=TOKEN_RESPONSE)
            api.get("/v1/search", payload={"total": 0, "results": []})
            async with MobilityDatabaseClient("t", base_url=api.url()) as client:
                try:
                    await client.search_feeds(search_query=value)
                except ValueError:
                    return None
            search = next(r for r in api.requests if r.method == "GET")
            return search.query["search_query"]
        finally:
            await api.stop()

    return asyncio.run(scenario())


# st.lists, not st.text: st.text never yields a lone surrogate.
@given(
    value=st.lists(
        st.characters(codec="utf-8", exclude_categories=["Cs"])
        | st.sampled_from(["\ud800", "\udcff", "+", "&", "#", "%41", "\x00"]),
        min_size=1,
        max_size=8,
    ).map("".join)
)
@settings(max_examples=200, deadline=None)
def test_a_query_value_is_refused_or_reaches_the_server_as_itself(value: str) -> None:
    """yarl drops a lone surrogate: "new\\ud800york" went out as "newyork",
    and "\\udcff" as no filter at all."""
    sent = _sent_query(value)
    if sent is None:
        assert not _utf8(value)
    else:
        assert sent == value


async def test_a_license_url_that_isnt_text_is_refused(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    with pytest.raises(ValueError, match="lone surrogate"):
        await client.get_matching_licenses("https://x/\ud800")
    assert mock_api.requests == []


async def test_an_echoing_error_page_hides_the_access_token(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    mock_api.post(
        "/v1/tokens", payload={**TOKEN_RESPONSE, "access_token": "SECRET/ACCESS"}
    )
    mock_api.get(
        "/v1/metadata",
        status=400,
        body=b'you sent "Authorization": "Bearer SECRET\\/ACCESS" (SECRET/ACCESS)',
        content_type="text/plain",
    )
    with pytest.raises(MobilityDatabaseApiError) as caught:
        await client.get_metadata()
    assert "SECRET" not in str(caught.value)


async def test_a_session_raising_for_status_keeps_the_error_mapping(
    mock_api: MockApi,
) -> None:
    """raise_for_status=True on the session made a 404 a connection error
    and a 401 skip its refresh: each request opts out."""
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/feeds/mdb-1", status=404)
    mock_api.get("/v1/metadata", status=401)
    mock_api.post("/v1/tokens", payload={**TOKEN_RESPONSE, "access_token": "second"})
    mock_api.get("/v1/metadata", payload=METADATA, auth="second")
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        client = MobilityDatabaseClient("t", session, base_url=mock_api.url())
        with pytest.raises(MobilityDatabaseNotFoundError):
            await client.get_feed("mdb-1")
        assert await client.get_metadata() == Metadata.from_dict(METADATA)


async def test_a_session_raising_for_status_keeps_token_errors_auth_errors(
    mock_api: MockApi,
) -> None:
    mock_api.post("/v1/tokens", status=500, body=b"x", content_type="text/plain")
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        client = MobilityDatabaseClient("t", session, base_url=mock_api.url())
        with pytest.raises(MobilityDatabaseAuthenticationError):
            await client.get_metadata()


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.parametrize(
    "kind", ["auth", "base_url", "header-lf", "header-surrogate", "header-name"]
)
async def test_an_injected_session_default_the_client_cant_use_is_refused(
    kind: str,
) -> None:
    """A session auth would ride to the token endpoint (the caller's
    credentials, to the catalog API) and clash with the Bearer header; a
    base_url makes old aiohttp refuse absolute URLs; a bad default header
    fails every request."""
    options: dict[str, Any] = {
        "auth": {"auth": aiohttp.BasicAuth("user", "pw")},
        "base_url": {"base_url": "http://127.0.0.1:9"},
        "header-lf": {"headers": {"X-A": "a\nb"}},
        "header-surrogate": {"headers": {"X-A": "a\ud800"}},
        "header-name": {"headers": {"X A": "b"}},
    }[kind]
    async with aiohttp.ClientSession(**options) as session:
        with pytest.raises(ValueError):
            MobilityDatabaseClient("t", session)


def test_an_empty_refresh_token_is_refused() -> None:
    """It would also have made the redaction replace every gap of a page."""
    with pytest.raises(ValueError, match="refresh_token is empty"):
        MobilityDatabaseClient("")


# -- Raw responses aiohttp-based mocks can't send ------------------------------------


async def _raw_server(
    data_response: bytes, token_response: bytes | None = None
) -> tuple[asyncio.Server, str]:
    """Serve ``token_response`` (default: a real token) to the token request,
    then ``data_response`` verbatim to anything else."""
    token = json.dumps({**TOKEN_RESPONSE, "access_token": "AccessTok123"}).encode()

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        request = await reader.readuntil(b"\r\n\r\n")
        if request.startswith(b"POST /v1/tokens"):
            length = int(request.split(b"Content-Length: ")[1].split(b"\r\n")[0])
            await reader.readexactly(length)
            if token_response is not None:
                writer.write(token_response)
                await writer.drain()
                writer.close()
                return
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                b"Content-Length: " + str(len(token)).encode() + b"\r\n\r\n" + token
            )
        else:
            writer.write(data_response)
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, f"http://127.0.0.1:{port}"


@pytest.mark.parametrize("content_type", [b"\x80\xc3\xa9", b"text/\x80\xc3\xa9"])
async def test_an_undecodable_content_type_is_invalid(content_type: bytes) -> None:
    """aiohttp before 3.14 raised a bare UnicodeEncodeError reading it."""
    body = b'{"version": "1"}'
    response = (
        b"HTTP/1.1 200 OK\r\nContent-Type: " + content_type + b"\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body
    )
    server, url = await _raw_server(response)
    try:
        async with MobilityDatabaseClient(
            "RefreshTokSecret987", base_url=url
        ) as client:
            with pytest.raises(
                MobilityDatabaseConnectionError, match="not application/json"
            ):
                await client.get_metadata()
    finally:
        server.close()


@pytest.mark.parametrize(
    "header",
    [
        b"X-Echo: Bearer AccessTok123\x00",
        b"Echo Bearer AccessTok123",
        b"X-Echo: AccessTok123\rx",
    ],
)
async def test_aiohttps_error_text_never_carries_the_token(header: bytes) -> None:
    """A malformed header echoing the request: aiohttp quotes it in its own
    error, which went into the message and, as the cause, the traceback."""
    response = (
        b"HTTP/1.1 400 Bad Request\r\n" + header + b"\r\nContent-Length: 0\r\n\r\n"
    )
    server, url = await _raw_server(response)
    try:
        async with MobilityDatabaseClient(
            "RefreshTokSecret987", base_url=url
        ) as client:
            with pytest.raises(MobilityDatabaseConnectionError) as caught:
                await client.get_metadata()
    finally:
        server.close()
    assert "AccessTok123" not in str(caught.value)
    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__


async def test_an_echo_under_another_charset_hides_the_tokens(
    mock_api: MockApi,
) -> None:
    """Decoded as UTF-16, an ASCII echo is CJK text the token re-encodes
    back from: the bytes are redacted before decoding."""
    mock_api.post(
        "/v1/tokens", payload={**TOKEN_RESPONSE, "access_token": "AccessTok123"}
    )
    mock_api.get(
        "/v1/metadata",
        status=500,
        body=b"Bearer AccessTok123 refresh=RefreshTokSecret987",
        headers={"Content-Type": "text/plain; charset=utf-16"},
    )
    async with MobilityDatabaseClient(
        "RefreshTokSecret987", base_url=mock_api.url()
    ) as client:
        with pytest.raises(MobilityDatabaseApiError) as caught:
            await client.get_metadata()
    for form in (
        str(caught.value),
        str(caught.value).encode("utf-16", "replace").decode("latin-1"),
    ):
        assert "AccessTok123" not in form
        assert "RefreshTokSecret987" not in form


async def test_a_session_authorization_header_is_refused() -> None:
    """It would ride on the token request, as a session auth would."""
    async with aiohttp.ClientSession(
        headers={"Authorization": "Basic dXNlcjpzZWNyZXQ="}
    ) as session:
        with pytest.raises(ValueError, match="Authorization header"):
            MobilityDatabaseClient("t", session)


@pytest.mark.parametrize(
    "base_url",
    [
        "https://user:pw@api.example.com",
        "https://api.example.com/?x=1",
        "https://api.example.com/#x",
        "http://api.example.com",  # RFC 6750 section 5.3: TLS for bearer tokens
        "ftp://api.example.com",
        "https://",
        "not a url",
    ],
)
def test_an_unusable_base_url_is_refused(base_url: str) -> None:
    with pytest.raises(ValueError, match="base_url"):
        MobilityDatabaseClient("t", base_url=base_url)


@pytest.mark.parametrize(
    "base_url",
    [
        "https://api.example.com/",
        "http://127.0.0.1:8080",
        "http://localhost",
        "http://[::1]:9",
    ],
)
def test_https_and_loopback_base_urls_are_fine(base_url: str) -> None:
    MobilityDatabaseClient("t", base_url=base_url)


async def test_an_undecodable_charset_on_json_has_no_effect() -> None:
    """application/json with an undecodable charset is still JSON (RFC 8259
    section 11), on every aiohttp: older ones can't parse the header, so the
    media type is read from it directly."""
    body = b'{"version": "1"}'
    response = (
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=\x80\xc3\xa9\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body
    )
    server, url = await _raw_server(response)
    try:
        async with MobilityDatabaseClient(
            "RefreshTokSecret987", base_url=url
        ) as client:
            assert (await client.get_metadata()).version == "1"
    finally:
        server.close()


# -- The boundary, as a property: any answer, only library errors, no secrets ----------

_SECRETS = ("RefreshTokSecret987", "AccessTok123")
_STATUS_LINES = st.sampled_from(
    [
        b"HTTP/1.1 200 OK",
        b"HTTP/1.1 203 X",
        b"HTTP/1.1 302 Found",
        b"HTTP/1.1 400 X",
        b"HTTP/1.1 401 X",
        b"HTTP/1.1 404 X",
        b"HTTP/1.1 500 X",
        b"HTTP/1.1 999 X",
        b"HTTP/1.0 200 OK",
        b"HTTP/2 200",
        b"garbage",
        b"",
    ]
)
_HEADER_LINES = st.sampled_from(
    [
        b"Content-Type: application/json",
        b"Content-Type: text/plain; charset=utf-16",
        b"Content-Type: application/json; charset*=utf-8''utf%00",
        b"Content-Type: \x80\xc3\xa9",
        b"X-Echo: Bearer AccessTok123\x00",
        b"Echo Bearer AccessTok123",
        b"X-Echo: RefreshTokSecret987\rx",
        b"Location: http://u:p@127.0.0.1:9/x",
        b"Location: \xff",
        b"Content-Encoding: gzip",
        b"Content-Length: 99999",
        b"Transfer-Encoding: chunked",
        b"Set-Cookie: a=\x00",
    ]
)
_BODIES = st.sampled_from(
    [
        b"",
        b"{}",
        b'{"version": "1"}',
        b"Bearer AccessTok123 RefreshTokSecret987",
        "RefreshTokSecret987".encode("utf-16"),
        b"\xed\xa0\x80",
        b"[" * 5000,
    ]
)
_RAW_RESPONSES = st.builds(
    lambda status, headers, body: (
        status + b"\r\n" + b"".join(h + b"\r\n" for h in headers) + b"\r\n" + body
    ),
    _STATUS_LINES,
    st.lists(_HEADER_LINES, max_size=4),
    _BODIES,
)


@given(response=_RAW_RESPONSES, on_token=st.booleans())
@settings(max_examples=300, deadline=None)
def test_any_answer_gives_a_library_result_and_no_secret_in_any_message(
    response: bytes, on_token: bool
) -> None:
    """The transport boundary as a property: whatever bytes the server sends
    on the token or the data request -- malformed, echoing the tokens, under
    any charset -- the call returns a model or raises a MobilityDatabaseError,
    and no exception message (nor any chained one) holds a token."""

    async def scenario() -> BaseException | None:
        if on_token:
            server, url = await _raw_server(b"", token_response=response)
        else:
            server, url = await _raw_server(response)
        try:
            async with MobilityDatabaseClient(
                "RefreshTokSecret987", base_url=url
            ) as client:
                try:
                    await asyncio.wait_for(client.get_metadata(), 10)
                except MobilityDatabaseError as err:
                    return err
            return None
        finally:
            server.close()

    err = asyncio.run(scenario())
    while err is not None:
        for secret in _SECRETS:
            assert secret not in str(err), (type(err).__name__, str(err))
        err = err.__cause__ or (None if err.__suppress_context__ else err.__context__)
