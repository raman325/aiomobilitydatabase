"""Property-based tests (hypothesis) for catalog-side models and encoding.

Companion to ``tests/feeds/test_properties.py`` (feeds side): item H of the
exhaustive property sweep, applied to the catalog client instead.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from mashumaro.exceptions import InvalidFieldValue

from aiomobilitydatabase import client as client_module
from aiomobilitydatabase.client import MobilityDatabaseClient, encode_params
from aiomobilitydatabase.exceptions import MobilityDatabaseError
from aiomobilitydatabase.models import (
    BoundingFilterMethod,
    DataType,
    EntityType,
    Feed,
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
# they identify the feed. Every other key in the fixture payloads -- at the
# top level AND one level down inside the nested objects -- is optional per
# the dataclasses (``X | None = None``), so from_dict must tolerate any of
# them being entirely absent OR explicitly null; the live API does both
# depending on the field and the feed.

_REQUIRED_KEYS = frozenset({"id", "data_type"})

# (dataclass, fixture payload, expected data_type) per feed flavor.
_FEED_FLAVORS = [
    pytest.param(GtfsFeed, GTFS_FEED, DataType.GTFS, id="gtfs"),
    pytest.param(GtfsRtFeed, GTFS_RT_FEED, DataType.GTFS_RT, id="gtfs_rt"),
]

# A path into a payload: top-level key, or key + nested key, or key + list
# index + nested key.
_Path = tuple[str | int, ...]


def _mutable_paths(payload: dict[str, Any]) -> list[_Path]:
    """Every optional path in ``payload``, recursing one level into nested
    objects and into the first element of each list of objects.

    Nested objects are where a non-optional field would actually bite:
    nulling a top-level key only exercises the outer dataclass, while
    nulling ``source_info.license_url`` exercises SourceInfo itself.
    """
    paths: list[_Path] = []
    for key, value in payload.items():
        if key in _REQUIRED_KEYS:
            continue
        paths.append((key,))
        if isinstance(value, dict):
            paths.extend((key, nested) for nested in value)
        elif isinstance(value, list):
            for index, item in enumerate(value):
                if isinstance(item, dict):
                    paths.extend((key, index, nested) for nested in item)
    return paths


def _apply_mutations(
    payload: dict[str, Any], mutations: dict[_Path, bool]
) -> tuple[dict[str, Any], list[_Path]]:
    """Copy ``payload``, deleting (True) or nulling (False) each path.

    Returns the mutated payload plus the paths actually applied: a path
    whose ancestor was itself deleted or nulled is skipped, since there is
    no longer a container to mutate (and nothing to assert about it beyond
    what the ancestor's own path already asserts).
    """
    mutated = copy.deepcopy(payload)
    applied: list[_Path] = []
    done: set[_Path] = set()
    for path in sorted(mutations, key=len):
        if any(path[:cut] in done for cut in range(1, len(path))):
            continue
        container: Any = mutated
        for step in path[:-1]:
            container = container[step]
        leaf = path[-1]
        if mutations[path]:
            del container[leaf]
        else:
            container[leaf] = None
        done.add(path)
        applied.append(path)
    return mutated, applied


def _attr_at(obj: Any, path: _Path) -> Any:
    """Follow ``path`` through parsed objects: keys become attributes, list
    indices stay indices."""
    for step in path:
        obj = obj[step] if isinstance(step, int) else getattr(obj, step)
    return obj


@pytest.mark.parametrize(("feed_cls", "payload", "data_type"), _FEED_FLAVORS)
@given(data=st.data())
@settings(max_examples=150)
def test_feed_tolerates_missing_or_null_optional_fields(
    feed_cls: type[Feed],
    payload: dict[str, Any],
    data_type: DataType,
    data: st.DataObject,
) -> None:
    mutations = data.draw(
        st.dictionaries(
            st.sampled_from(_mutable_paths(payload)), st.booleans(), max_size=20
        )
    )
    mutated_payload, applied = _apply_mutations(payload, mutations)
    feed = feed_cls.from_dict(mutated_payload)
    assert feed.id == payload["id"]
    assert feed.data_type is data_type
    # The teeth: every mutated path really parsed to None, rather than the
    # mutation being silently ignored (or defaulted to something truthy).
    for path in applied:
        assert _attr_at(feed, path) is None, path


# Every wire value of the enums reachable from the fields below, so a drawn
# "unknown" string can never collide with a real member.
_ENUM_WIRE = frozenset(
    member.value for enum in (EntityType, FeedStatus) for member in enum
)


@pytest.mark.parametrize(
    ("feed_cls", "payload", "field_name", "known_value"),
    [
        pytest.param(GtfsRtFeed, GTFS_RT_FEED, "entity_types", "vp", id="entity_types"),
        pytest.param(GtfsFeed, GTFS_FEED, "status", "active", id="status"),
    ],
)
@given(unknown=st.text(min_size=1, max_size=12).filter(lambda s: s not in _ENUM_WIRE))
@settings(max_examples=50)
def test_unknown_enum_value_is_rejected(
    feed_cls: type[Feed],
    payload: dict[str, Any],
    field_name: str,
    known_value: str,
    unknown: str,
) -> None:
    """Pin the one place null-tolerance does NOT extend to: an unrecognized
    enum *value*. Nulling a key can never probe this, so it is drawn
    separately -- including inside a list, where the element type is what
    rejects. Live consequence: a new entity type or feed status shipped by
    the API makes from_dict raise rather than degrade.
    """
    mutated = copy.deepcopy(payload)
    existing = payload[field_name]
    mutated[field_name] = (
        [known_value, unknown] if isinstance(existing, list) else unknown
    )
    with pytest.raises(InvalidFieldValue):
        feed_cls.from_dict(mutated)


# --- encode_params output laws ----------------------------------------------

_ENUM_VALUES = [*DataType, *FeedStatus, *SortOrder, *BoundingFilterMethod]

# Values whose own text contains the separator encode_params joins on, so the
# join is exercised at its ambiguous edge rather than only on comma-free text.
_COMMA_TEXT = st.sampled_from(["a,b", ",", "a,,b", "trailing,"])

_scalars = (
    st.booleans()
    | st.integers(min_value=-1_000_000, max_value=1_000_000)
    | st.text(max_size=12)
    # "" is reachable (e.g. provider="") and, unlike an empty list, is KEPT.
    | st.just("")
    | _COMMA_TEXT
    | st.sampled_from(_ENUM_VALUES)
    | st.datetimes(min_value=datetime(2000, 1, 1), max_value=datetime(2035, 1, 1))
)
_containers = st.lists(_scalars, max_size=4).flatmap(
    lambda items: st.sampled_from([items, tuple(items)])
)
# Nested containers are not reachable through any public client signature, but
# _encode_value recurses, so pin the flattening it produces today.
_nested_containers = st.lists(_containers, max_size=3)
_values = st.none() | _scalars | _containers | _nested_containers
_param_keys = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu")), min_size=1, max_size=10
)
_param_dicts = st.dictionaries(_param_keys, _values, max_size=8)


def _expected_scalar(value: Any) -> str:
    """Expected encoding of one non-container value, spelled out per type
    rather than deferring to the implementation's helper."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, datetime):
        # ISO 8601, i.e. "T"-separated -- not str(datetime)'s space.
        return value.isoformat()
    if isinstance(value, int):
        return repr(value)
    assert isinstance(value, str)
    return value


def _expected_encoded(value: Any) -> str:
    """Expected encoding of a (possibly nested) value: containers comma-join
    their items' encodings, recursively.

    Note this is NOT a flatten: an empty inner container encodes to "", so
    [[], []] encodes to "," rather than "". Only a TOP-level empty container
    is dropped. Nested containers are unreachable through the public client
    signatures; this pins the behavior rather than endorsing it.
    """
    if isinstance(value, list | tuple):
        return ",".join(_expected_encoded(item) for item in value)
    return _expected_scalar(value)


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
        elif isinstance(value, list | tuple):
            # Comma-joined, so a ";"-join or a repr()-of-list would fail.
            assert encoded[key] == _expected_encoded(value)
        else:
            assert encoded[key] == _expected_scalar(value)
            if isinstance(value, datetime):
                # Independent of the oracle above: ISO round-trips, and the
                # date/time separator is "T".
                assert datetime.fromisoformat(encoded[key]) == value
                assert "T" in encoded[key]


def test_encode_params_keeps_empty_string_but_drops_empty_container() -> None:
    """The deliberate asymmetry in encode_params' docstring: an empty list
    means "no filter" and vanishes, while "" is a real (if odd) filter value
    and survives as an empty-string param.
    """
    assert encode_params({"provider": "", "features": []}) == {"provider": ""}


def test_encode_params_comma_join_is_ambiguous_for_commas_in_values() -> None:
    """KNOWN GAP, pinned rather than fixed: the API's multi-value convention
    is a comma-joined string with no escape, so a single value containing a
    comma is indistinguishable on the wire from two values. Reachable today
    via the list[str] filters (license_tags, features, license_ids). Fixing
    it needs an escaping convention the API does not document.
    """
    assert encode_params({"license_tags": ["a,b"]}) == encode_params(
        {"license_tags": ["a", "b"]}
    )


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
# Constructive rather than "bury the interesting characters in all of Unicode":
# drawing each character from a 5-way special-character pool OR the letter/
# number space puts a "/" in 78 of 150 draws, against 3 of 150 for the
# previous st.text(categories=["L", "N"], include_characters=...) form (both
# measured). The special characters are what the quoting bug lives in.
_PATH_ID_TEXT = st.lists(
    st.sampled_from(_PATH_ID_CHARS)
    | st.characters(categories=["L", "N"], include_characters="–_🚌"),  # noqa: RUF001
    min_size=1,
    max_size=12,
).map("".join)

# EVERY id-interpolating call site in client.py, as
# method name -> (path prefix, path suffix). The suffixed ones
# (.../datasets, .../gtfs_rt_feeds, .../availability) matter most: there the
# id sits in the MIDDLE of the path, so an unquoted "/" shifts the trailing
# segment the server routes on, not just the lookup key.
_PATH_METHODS: dict[str, tuple[str, str]] = {
    "get_feed": ("/v1/feeds/", ""),
    "get_gtfs_feed": ("/v1/gtfs_feeds/", ""),
    "get_gtfs_rt_feed": ("/v1/gtfs_rt_feeds/", ""),
    "get_gbfs_feed": ("/v1/gbfs_feeds/", ""),
    "get_gtfs_feed_datasets": ("/v1/gtfs_feeds/", "/datasets"),
    "get_gtfs_feed_gtfs_rt_feeds": ("/v1/gtfs_feeds/", "/gtfs_rt_feeds"),
    "get_gtfs_feed_availability": ("/v1/gtfs_feeds/", "/availability"),
    "get_dataset_gtfs": ("/v1/datasets/gtfs/", ""),
    "get_license": ("/v1/licenses/", ""),
}


def _run_path_quoting_probe(method_name: str, id_value: str) -> str:
    """Call one id-taking catalog method against a mock server and return the
    raw wire path of the GET request actually sent.

    The scripted response is a 404 so the probe stays independent of each
    endpoint's response shape; the mock records the request before replying,
    and the resulting error is suppressed.

    The mock is registered at the DEcoded path (aiohttp always decodes
    percent-escapes back into ``request.path`` before routing, verified
    empirically), so this reflects real server-side behavior rather than
    a mock artifact.
    """

    async def scenario() -> str:
        api = MockApi()
        await api.start()
        try:
            api.post("/v1/tokens", payload=TOKEN_RESPONSE)
            prefix, suffix = _PATH_METHODS[method_name]
            api.get(f"{prefix}{id_value}{suffix}", status=404, payload={})
            async with MobilityDatabaseClient("t", base_url=api.url()) as client:
                with contextlib.suppress(MobilityDatabaseError):
                    await getattr(client, method_name)(id_value)
            get_requests = [r for r in api.requests if r.method == "GET"]
            assert len(get_requests) == 1, get_requests
            return get_requests[0].raw_path
        finally:
            await api.stop()

    return asyncio.run(scenario())


@given(method_name=st.sampled_from(list(_PATH_METHODS)), id_value=_PATH_ID_TEXT)
@settings(max_examples=300, deadline=None)
def test_catalog_path_ids_are_quoted(method_name: str, id_value: str) -> None:
    recorded_raw_path = _run_path_quoting_probe(method_name, id_value)
    prefix, suffix = _PATH_METHODS[method_name]
    assert recorded_raw_path == f"{prefix}{quote(id_value, safe='')}{suffix}"


def test_every_quoted_call_site_is_covered() -> None:
    """Fail when a new endpoint interpolates an id into its path without
    being added to _PATH_METHODS above, which is how the previous version of
    this test ended up protecting 3 of the 9 call sites.
    """
    source = Path(client_module.__file__).read_text(encoding="utf-8")
    assert source.count("{_quote_segment(") == len(_PATH_METHODS)
