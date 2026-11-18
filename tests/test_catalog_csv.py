"""Reading the public catalog export into the catalog API's models."""

from __future__ import annotations

import codecs
import csv
from datetime import datetime

import pytest

from aiomobilitydatabase._catalog_csv import CatalogFormatError, read_catalog
from aiomobilitydatabase.models import (
    BoundingBox,
    DataType,
    EntityType,
    FeedStatus,
    GbfsFeed,
    GtfsFeed,
    GtfsRtFeed,
    Redirect,
    SourceInfo,
)

from tests.public_catalog_data import GBFS_ROW, GTFS_ROW, GTFS_RT_ROW, box, export


def test_a_gtfs_row_maps_to_the_api_s_gtfs_feed() -> None:
    rows = read_catalog(export(GTFS_ROW))
    assert rows.gtfs == (
        GtfsFeed(
            id="mdb-10",
            data_type=DataType.GTFS,
            provider="MTA New York City Transit",
            feed_contact_email="gtfs@example.com",
            source_info=SourceInfo(
                producer_url="https://example.com/subway.zip",
                authentication_type=0,
                license_url="https://example.com/license",
            ),
            redirects=(
                Redirect(target_id="mdb-11", comment="merged"),
                Redirect(target_id="mdb-12", comment=None),
            ),
            status=FeedStatus.ACTIVE,
            official=True,
            seasonal=None,
            feed_name="NYC Subway",
            note="Line one\nline two, with a comma",
            bounding_box=BoundingBox(40.5, 40.9, -74.3, -73.7),
        ),
    )
    assert rows.by_id["mdb-10"] is rows.gtfs[0]


def test_a_gtfs_rt_row_maps_to_the_api_s_gtfs_rt_feed() -> None:
    (feed,) = read_catalog(export(GTFS_RT_ROW)).gtfs_rt
    assert feed == GtfsRtFeed(
        id="mdb-20",
        data_type=DataType.GTFS_RT,
        provider="MTA New York City Transit",
        source_info=SourceInfo(
            producer_url="https://example.com/rt",
            authentication_type=2,
            authentication_info_url="https://example.com/keys",
            api_key_parameter_name="x-api-key",
        ),
        redirects=(),
        status=FeedStatus.ACTIVE,
        official=False,
        seasonal=None,
        entity_types=(
            EntityType.SERVICE_ALERTS,
            EntityType.TRIP_UPDATES,
            EntityType.VEHICLE_POSITIONS,
        ),
        feed_references=("mdb-10", "mdb-13"),
    )


def test_a_gbfs_row_maps_to_the_api_s_gbfs_feed() -> None:
    (feed,) = read_catalog(export(GBFS_ROW)).gbfs
    assert feed == GbfsFeed(
        id="gbfs-citibike",
        data_type=DataType.GBFS,
        provider="Citi Bike",
        source_info=SourceInfo(
            producer_url="https://gbfs.example.com/gbfs.json",
            authentication_type=0,
            license_url="https://example.com/gbfs-license",
        ),
        redirects=(),
        bounding_box=BoundingBox(40.6, 40.9, -74.1, -73.8),
        # Naive UTC: the API's value for a column stored without a zone.
        bounding_box_generated_at=datetime(2025, 2, 3, 4, 5, 6),
    )


def test_an_offset_extracted_on_is_converted_to_utc() -> None:
    row = {
        **GBFS_ROW,
        "location.bounding_box.extracted_on": "2025-02-03 06:05:06+02:00",
    }
    (feed,) = read_catalog(export(row)).gbfs
    assert feed.bounding_box_generated_at == datetime(2025, 2, 3, 4, 5, 6)


@pytest.mark.parametrize(
    ("cells", "expected"),
    [
        ({"status": "retired"}, {"status": None}),
        ({"is_official": "yes"}, {"official": None}),
        ({"is_official": ""}, {"official": None}),
        ({"urls.authentication_type": "two"}, {"authentication_type": None}),
        ({"urls.authentication_type": "+1"}, {"authentication_type": None}),
        ({"location.bounding_box.minimum_latitude": "nan"}, {"bounding_box": None}),
        ({"location.bounding_box.maximum_longitude": ""}, {"bounding_box": None}),
        ({"location.bounding_box.minimum_latitude": "1_0"}, {"bounding_box": None}),
        (box("41", "40", "-74", "-73"), {"bounding_box": None}),
        (
            box("-95", "95", "-200", "200"),
            {"bounding_box": BoundingBox(-95.0, 95.0, -200.0, 200.0)},
        ),
        (
            {"name": "", "note": "", "provider": ""},
            {"feed_name": None, "note": None, "provider": None},
        ),
    ],
)
def test_values_this_library_can_t_read_are_none(
    cells: dict[str, str], expected: dict[str, object]
) -> None:
    (feed,) = read_catalog(export({**GTFS_ROW, **cells})).gtfs
    for name, value in expected.items():
        actual = (
            feed.source_info.authentication_type
            if name == "authentication_type" and feed.source_info is not None
            else getattr(feed, name)
        )
        assert actual == value, name


def test_unknown_entity_types_are_left_out() -> None:
    (feed,) = read_catalog(export({**GTFS_RT_ROW, "entity_type": "tu|xx"})).gtfs_rt
    assert feed.entity_types == (EntityType.TRIP_UPDATES,)


def test_empty_list_columns_are_empty_tuples() -> None:
    row = {**GTFS_RT_ROW, "entity_type": "", "static_reference": ""}
    (feed,) = read_catalog(export(row)).gtfs_rt
    assert feed.entity_types == ()
    assert feed.feed_references == ()
    assert feed.redirects == ()


def test_redirect_comments_that_can_t_be_paired_are_none() -> None:
    row = {**GTFS_ROW, "redirect.id": "mdb-11|mdb-12", "redirect.comment": "a|b|c"}
    (feed,) = read_catalog(export(row)).gtfs
    assert feed.redirects == (
        Redirect(target_id="mdb-11", comment=None),
        Redirect(target_id="mdb-12", comment=None),
    )


def test_rows_without_an_id_or_of_another_type_are_skipped() -> None:
    rows = read_catalog(
        export(
            {**GTFS_ROW, "id": ""},
            {**GTFS_ROW, "id": "mdb-99", "data_type": "gtfs_flex"},
        )
    )
    assert rows.gtfs == rows.gtfs_rt == rows.gbfs == ()
    assert dict(rows.by_id) == {}


def test_a_repeated_id_keeps_the_first_row() -> None:
    rows = read_catalog(export(GTFS_ROW, {**GTFS_ROW, "provider": "Other"}))
    assert [feed.provider for feed in rows.gtfs] == ["MTA New York City Transit"]


def test_feeds_are_ordered_by_provider_then_id_with_no_provider_last() -> None:
    rows = read_catalog(
        export(
            {**GTFS_ROW, "id": "mdb-3", "provider": "B"},
            {**GTFS_ROW, "id": "mdb-2", "provider": ""},
            {**GTFS_ROW, "id": "mdb-1", "provider": "B"},
            {**GTFS_ROW, "id": "mdb-4", "provider": "A"},
        )
    )
    assert [feed.id for feed in rows.gtfs] == ["mdb-4", "mdb-1", "mdb-3", "mdb-2"]


def test_a_byte_order_mark_is_allowed() -> None:
    rows = read_catalog(codecs.BOM_UTF8 + export(GTFS_ROW))
    assert [feed.id for feed in rows.gtfs] == ["mdb-10"]


def test_missing_optional_columns_read_as_empty() -> None:
    rows = read_catalog(
        export({"id": "mdb-1", "data_type": "gtfs"}, header=["id", "data_type"])
    )
    (feed,) = rows.gtfs
    assert feed.provider is None
    assert feed.bounding_box is None
    assert feed.source_info == SourceInfo()


@pytest.mark.parametrize(
    "body",
    [
        b"\xff\xfe not utf-8",
        b"provider,status\nA,active\n",  # no id / data_type
        b"",
        b"id,data_type\n"
        + b"x" * (csv.field_size_limit() + 1)
        + b",gtfs\n",  # csv.Error
    ],
)
def test_a_body_that_is_no_export_raises(body: bytes) -> None:
    with pytest.raises(CatalogFormatError):
        read_catalog(body)
