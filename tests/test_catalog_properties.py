"""Property-based tests (hypothesis) for catalog-side models and encoding.

Companion to ``tests/feeds/test_properties.py`` (feeds side): item H of the
exhaustive property sweep, applied to the catalog client instead.
"""

from __future__ import annotations

import copy
from datetime import datetime
from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.client import encode_params
from aiomobilitydatabase.models import (
    BoundingFilterMethod,
    DataType,
    FeedStatus,
    GtfsFeed,
    GtfsRtFeed,
    SortOrder,
)

from tests.fixtures import GTFS_FEED, GTFS_RT_FEED

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
