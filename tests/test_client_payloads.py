"""A 200 body that isn't the documented payload is an invalid response.

The README's error table: ``MobilityDatabaseConnectionError`` is a "network
failure, timeout, or invalid response body" -- never a bare ``ValueError``,
mashumaro error or ``RecursionError`` from decoding it.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.client import MobilityDatabaseClient
from aiomobilitydatabase.exceptions import MobilityDatabaseConnectionError
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

from tests.fixtures import GTFS_FEED, METADATA, TOKEN_RESPONSE
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
