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
import types
import typing
from datetime import datetime
from enum import Enum
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.client import MobilityDatabaseClient
from aiomobilitydatabase.exceptions import (
    MobilityDatabaseApiError,
    MobilityDatabaseAuthenticationError,
    MobilityDatabaseConnectionError,
    MobilityDatabaseError,
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
        return isinstance(raw, dict) and all(
            _faithful(hints[field.name], raw[field.name], getattr(decoded, field.name))
            for field in dataclasses.fields(annotation)
            if field.name in raw
        )
    if annotation is datetime:
        return isinstance(raw, str) and decoded == datetime.fromisoformat(raw)
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return isinstance(raw, str) and decoded == annotation(raw)
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


def _flip(payload: Any, data: st.DataObject) -> Any:
    """``payload`` with one value, at any depth, replaced by arbitrary JSON."""
    if isinstance(payload, dict) and payload:
        key = data.draw(st.sampled_from(sorted(payload)))
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
    "get_gtfs_feed": GTFS_FEED,
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


# (An unknown charset name is no error: aiohttp falls back to UTF-8.)
@pytest.mark.parametrize("charset", ["rot13", "base64", "zlib"])
async def test_a_charset_that_is_no_text_encoding_is_invalid_json(
    mock_api: MockApi, client: MobilityDatabaseClient, charset: str
) -> None:
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get(
        "/v1/metadata",
        body=json.dumps(METADATA).encode(),
        headers={"Content-Type": f"application/json; charset={charset}"},
    )
    with pytest.raises(MobilityDatabaseConnectionError):
        await client.get_metadata()


# (idna decodes an ASCII token body fine; undefined refuses all decoding.)
@pytest.mark.parametrize("charset", ["rot13", "hex", "undefined"])
async def test_a_token_body_in_no_text_encoding_is_a_connection_error(
    mock_api: MockApi, client: MobilityDatabaseClient, charset: str
) -> None:
    mock_api.post(
        "/v1/tokens",
        body=json.dumps(TOKEN_RESPONSE).encode(),
        headers={"Content-Type": f"application/json; charset={charset}"},
    )
    with pytest.raises(MobilityDatabaseConnectionError):
        await client.get_metadata()


async def test_a_token_error_with_an_undecodable_body_is_still_an_auth_error(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """Any non-200 from the token endpoint is an authentication failure,
    whatever bytes its body holds."""
    mock_api.post(
        "/v1/tokens",
        status=500,
        body="Erreur \xe9".encode("latin-1"),
        content_type="text/plain",
    )
    with pytest.raises(MobilityDatabaseAuthenticationError, match="Erreur �"):
        await client.get_metadata()


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
    with pytest.raises(MobilityDatabaseAuthenticationError, match="Internal error"):
        await client.get_metadata()


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
