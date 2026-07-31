"""Property-based tests (hypothesis) for catalog-side models and encoding.

Companion to ``tests/feeds/test_properties.py`` (feeds side): item H of the
exhaustive property sweep, applied to the catalog client instead.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
from datetime import datetime
from typing import Any
from urllib.parse import quote

from hypothesis import given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.client import MobilityDatabaseClient, encode_params
from aiomobilitydatabase.exceptions import MobilityDatabaseError
from aiomobilitydatabase.models import (
    BoundingFilterMethod,
    DataType,
    FeedStatus,
    GtfsFeed,
    GtfsRtFeed,
    SortOrder,
)

from tests.fixtures import GTFS_FEED, GTFS_RT_FEED, TOKEN_RESPONSE
from tests.mock_server import MockApi

# --- Model null-tolerance ---------------------------------------------------
#
# ``id`` and ``data_type`` are the only fields treated as "required" here:
# they identify the feed. Every other top-level key in the fixture payloads
# is optional per the dataclass (``X | None = None``), so from_dict must
# tolerate any of them being entirely absent OR explicitly null -- the live
# API does both depending on the field and the feed.

_REQUIRED_KEYS = frozenset({"id", "data_type"})


def _optional_keys(payload: dict[str, Any]) -> list[str]:
    return [key for key in payload if key not in _REQUIRED_KEYS]


def _apply_mutations(
    payload: dict[str, Any], mutations: dict[str, bool]
) -> dict[str, Any]:
    """Copy ``payload``, deleting or nulling each key in ``mutations``.

    ``mutations`` maps key -> True (delete) / False (set to None).
    """
    mutated = copy.deepcopy(payload)
    for key, delete in mutations.items():
        if delete:
            del mutated[key]
        else:
            mutated[key] = None
    return mutated


@given(
    mutations=st.dictionaries(
        st.sampled_from(_optional_keys(GTFS_FEED)), st.booleans(), max_size=15
    )
)
@settings(max_examples=100)
def test_gtfs_feed_tolerates_missing_or_null_optional_fields(
    mutations: dict[str, bool],
) -> None:
    mutated_payload = _apply_mutations(GTFS_FEED, mutations)
    feed = GtfsFeed.from_dict(mutated_payload)
    # Required keys were never touched by the mutation set.
    assert feed.id == GTFS_FEED["id"]
    assert feed.data_type is DataType.GTFS


@given(
    mutations=st.dictionaries(
        st.sampled_from(_optional_keys(GTFS_RT_FEED)), st.booleans(), max_size=15
    )
)
@settings(max_examples=100)
def test_gtfs_rt_feed_tolerates_missing_or_null_optional_fields(
    mutations: dict[str, bool],
) -> None:
    mutated_payload = _apply_mutations(GTFS_RT_FEED, mutations)
    feed = GtfsRtFeed.from_dict(mutated_payload)
    assert feed.id == GTFS_RT_FEED["id"]
    assert feed.data_type is DataType.GTFS_RT


# --- encode_params output laws ----------------------------------------------

_ENUM_VALUES = [*DataType, *FeedStatus, *SortOrder, *BoundingFilterMethod]

_scalars = (
    st.booleans()
    | st.integers(min_value=-1_000_000, max_value=1_000_000)
    | st.text(max_size=12)
    | st.sampled_from(_ENUM_VALUES)
    | st.datetimes(min_value=datetime(2000, 1, 1), max_value=datetime(2035, 1, 1))
)
_containers = st.lists(_scalars, max_size=4).flatmap(
    lambda items: st.sampled_from([items, tuple(items)])
)
_values = st.none() | _scalars | _containers
_param_keys = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu")), min_size=1, max_size=10
)
_param_dicts = st.dictionaries(_param_keys, _values, max_size=8)


@given(params=_param_dicts)
@settings(max_examples=200)
def test_encode_params_output_laws(params: dict[str, Any]) -> None:
    encoded = encode_params(params)

    # Every output value is a str.
    assert all(isinstance(value, str) for value in encoded.values())

    for key, value in params.items():
        if value is None:
            # No key with a None input appears in the output.
            assert key not in encoded
        elif isinstance(value, list | tuple) and not value:
            # Empty containers are dropped, just like None.
            assert key not in encoded
        elif isinstance(value, bool):
            # Bools map exactly to "true"/"false".
            assert encoded[key] == ("true" if value else "false")
        else:
            assert key in encoded


# --- Task 15R-b item 2: path-interpolated ids must be quoted ---------------
#
# Fail-first evidence (pre-fix, captured verbatim via a standalone repro
# script -- not this property, which needs the mock_server.py raw_path
# addition to even express the check):
#
#   >>> await client.get_feed("a/b")
#   MobilityDatabaseApiError: API error 599: UNREGISTERED MOCK ROUTE: GET /v1/feeds/a/b
#   recorded requests: [('POST', '/v1/tokens'), ('GET', '/v1/feeds/a/b')]
#
# i.e. feed_id "a/b" was naively f-string-interpolated into "/v1/feeds/a/b"
# -- an extra, unintended path segment -- instead of the single quoted
# segment "/v1/feeds/a%2Fb" a real API would need to look up the literal ID.

_PATH_ID_CHARS = "/?#% "
_PATH_ID_TEXT = st.text(
    alphabet=st.characters(
        categories=["L", "N"],
        include_characters=_PATH_ID_CHARS + "–_🚌",  # noqa: RUF001
    ),
    min_size=1,
    max_size=12,
)

# feed_id/license_id/dataset_id path-taking methods named by the plan item.
_PATH_METHODS: dict[str, str] = {
    "get_feed": "/v1/feeds/",
    "get_license": "/v1/licenses/",
    "get_dataset_gtfs": "/v1/datasets/gtfs/",
}


def _run_path_quoting_probe(kind: str, id_value: str) -> tuple[str, list[str]]:
    """Call one id-taking catalog method against a mock server and return
    (raw wire path of the GET request actually sent, every GET raw_path
    seen) -- regardless of whether the call raised, since the mock records
    a request before it can 599/404 on a route mismatch.

    The mock is registered at the DEcoded path (aiohttp always decodes
    percent-escapes back into ``request.path`` before routing, verified
    empirically), so this reflects real server-side behavior rather than
    a mock artifact.
    """

    async def scenario() -> tuple[str, list[str]]:
        api = MockApi()
        await api.start()
        try:
            api.post("/v1/tokens", payload=TOKEN_RESPONSE)
            prefix = _PATH_METHODS[kind]
            api.get(f"{prefix}{id_value}", payload={"id": id_value})
            async with MobilityDatabaseClient("t", base_url=api.url()) as client:
                with contextlib.suppress(MobilityDatabaseError):
                    if kind == "get_feed":
                        await client.get_feed(id_value)
                    elif kind == "get_license":
                        await client.get_license(id_value)
                    else:
                        await client.get_dataset_gtfs(id_value)
            get_requests = [r for r in api.requests if r.method == "GET"]
            assert len(get_requests) == 1, get_requests
            return get_requests[0].raw_path, [r.raw_path for r in get_requests]
        finally:
            await api.stop()

    return asyncio.run(scenario())


@given(kind=st.sampled_from(list(_PATH_METHODS)), id_value=_PATH_ID_TEXT)
@settings(max_examples=150, deadline=None)
def test_catalog_path_ids_are_quoted(kind: str, id_value: str) -> None:
    recorded_raw_path, _all = _run_path_quoting_probe(kind, id_value)
    expected = f"{_PATH_METHODS[kind]}{quote(id_value, safe='')}"
    assert recorded_raw_path == expected
