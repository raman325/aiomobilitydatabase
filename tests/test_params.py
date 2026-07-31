"""Tests for query parameter encoding."""

from datetime import UTC, datetime

from aiomobilitydatabase.client import encode_params
from aiomobilitydatabase.models import DataType, FeedStatus


def test_none_values_dropped() -> None:
    assert encode_params({"limit": None, "offset": 0}) == {"offset": "0"}


def test_bool_lowercase() -> None:
    assert encode_params({"is_official": True}) == {"is_official": "true"}
    assert encode_params({"is_official": False}) == {"is_official": "false"}


def test_enum_value() -> None:
    assert encode_params({"status": FeedStatus.ACTIVE}) == {"status": "active"}


def test_list_comma_joined() -> None:
    assert encode_params({"data_type": [DataType.GTFS, DataType.GTFS_RT]}) == {
        "data_type": "gtfs,gtfs_rt"
    }
    assert encode_params({"feature": ["Shapes", "Headsigns"]}) == {
        "feature": "Shapes,Headsigns"
    }


def test_tuple_comma_joined() -> None:
    assert encode_params({"dataset_latitudes": (33.5, 34.5)}) == {
        "dataset_latitudes": "33.5,34.5"
    }


def test_datetime_isoformat() -> None:
    assert encode_params({"downloaded_after": datetime(2023, 7, 1, tzinfo=UTC)}) == {
        "downloaded_after": "2023-07-01T00:00:00+00:00"
    }


def test_int_and_str_passthrough() -> None:
    assert encode_params({"limit": 10, "search_query": "new york"}) == {
        "limit": "10",
        "search_query": "new york",
    }
