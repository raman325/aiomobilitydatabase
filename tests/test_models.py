"""Tests for API models."""

from datetime import UTC, datetime

from aiomobilitydatabase.models import (
    BoundingBox,
    DataType,
    EntityType,
    FeedStatus,
    LatestDataset,
    Location,
    SourceInfo,
)
from tests.fixtures import BOUNDING_BOX, LATEST_DATASET, LOCATION, SOURCE_INFO


def test_enums() -> None:
    assert DataType("gtfs_rt") is DataType.GTFS_RT
    assert FeedStatus("active") is FeedStatus.ACTIVE
    assert EntityType("vp") is EntityType.VEHICLE_POSITIONS


def test_source_info() -> None:
    info = SourceInfo.from_dict(SOURCE_INFO)
    assert info.producer_url == "https://ladotbus.com/gtfs"
    assert info.authentication_type == 2
    assert info.is_producer_url_unstable is None
    assert info.license_tags == ["family:ODC"]


def test_location_and_bounding_box() -> None:
    loc = Location.from_dict(LOCATION)
    assert loc.municipality == "Los Angeles"
    box = BoundingBox.from_dict(BOUNDING_BOX)
    assert box.minimum_latitude == 33.721601


def test_latest_dataset() -> None:
    dataset = LatestDataset.from_dict(LATEST_DATASET)
    assert dataset.id == "mdb-1210-202402121801"
    assert dataset.downloaded_at == datetime(2026, 7, 31, 0, 45, 30, 828071, tzinfo=UTC)
    assert dataset.bounding_box is not None
    assert dataset.bounding_box.maximum_latitude == 34.323077
    assert dataset.validation_report is not None
    assert dataset.validation_report.total_error == 10


def test_unknown_fields_ignored() -> None:
    loc = Location.from_dict({**LOCATION, "brand_new_upstream_field": "x"})
    assert loc.country_code == "US"
