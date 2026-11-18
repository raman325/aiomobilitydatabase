"""Reading the public catalog export into its models."""

from __future__ import annotations

import codecs
import csv
from datetime import datetime
from itertools import chain

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.feeds import PublicGbfsFeed, PublicGtfsFeed, PublicGtfsRtFeed
from aiomobilitydatabase.feeds._bounds import Box
from aiomobilitydatabase.feeds._catalog_csv import (
    CatalogFormatError,
    CatalogRows,
    _Lines,
    _record_ends,
    read_catalog,
)
from aiomobilitydatabase.models import (
    BoundingBox,
    EntityType,
    FeedStatus,
    Redirect,
    SourceInfo,
)

from tests.feeds.public_catalog_data import GBFS_ROW, GTFS_ROW, GTFS_RT_ROW, box, export


def _rt(rows: CatalogRows) -> list[PublicGtfsRtFeed]:
    """The GTFS Realtime feeds, in the export's order."""
    return [feed for feed in rows.by_id.values() if isinstance(feed, PublicGtfsRtFeed)]


def test_a_gtfs_row_maps_to_a_public_gtfs_feed() -> None:
    rows = read_catalog(export(GTFS_ROW))
    assert rows.gtfs == (
        PublicGtfsFeed(
            id="mdb-10",
            provider="MTA New York City Transit",
            feed_name="NYC Subway",
            note="Line one\nline two, with a comma",
            feed_contact_email="gtfs@example.com",
            status=FeedStatus.ACTIVE,
            official=True,
            source_info=SourceInfo(
                producer_url="https://example.com/subway.zip",
                authentication_type=0,
                license_url="https://example.com/license",
            ),
            redirects=(
                Redirect(target_id="mdb-11", comment="merged"),
                Redirect(target_id="mdb-12", comment=None),
            ),
            bounding_box=BoundingBox(40.5, 40.9, -74.3, -73.7),
        ),
    )
    assert rows.by_id["mdb-10"] is rows.gtfs[0]


def test_a_gtfs_rt_row_maps_to_a_public_gtfs_rt_feed() -> None:
    (feed,) = _rt(read_catalog(export(GTFS_RT_ROW)))
    assert feed == PublicGtfsRtFeed(
        id="mdb-20",
        provider="MTA New York City Transit",
        feed_name=None,
        note=None,
        feed_contact_email=None,
        status=FeedStatus.ACTIVE,
        official=False,
        source_info=SourceInfo(
            producer_url="https://example.com/rt",
            authentication_type=2,
            authentication_info_url="https://example.com/keys",
            api_key_parameter_name="x-api-key",
        ),
        redirects=(),
        entity_types=(
            EntityType.SERVICE_ALERTS,
            EntityType.TRIP_UPDATES,
            EntityType.VEHICLE_POSITIONS,
        ),
        feed_references=("mdb-10", "mdb-13"),
    )


def test_a_gbfs_row_maps_to_the_api_s_gbfs_feed() -> None:
    (feed,) = read_catalog(export(GBFS_ROW)).gbfs
    assert feed == PublicGbfsFeed(
        id="gbfs-citibike",
        provider="Citi Bike",
        feed_contact_email=None,
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
    ("cell", "expected"),
    [
        ("2025-02-03 04:05:06", datetime(2025, 2, 3, 4, 5, 6)),
        ("2025-02-03 04:05:06.872037+00:00", datetime(2025, 2, 3, 4, 5, 6, 872037)),
        ("2025-02-03T04:05:06Z", datetime(2025, 2, 3, 4, 5, 6)),
        ("2025-02-03T06:05:06.5+02:00", datetime(2025, 2, 3, 4, 5, 6, 500000)),
        ("2025-02-03", None),  # a bare date: fromisoformat reads midnight
        ("2025-02-03 04:05", None),
        ("2025-02-03 24:00:00", None),  # fromisoformat rolls to the next day
        ("2025-02-03 04:05:60", None),
        ("2025-02-03 04:05:06.1234567", None),  # fromisoformat truncates
        ("2025-02-03t04:05:06z", None),
        ("20250203T040506", None),
        ("2025-W06-1 04:05:06", None),
        ("2025-02-03 04:05:06+0200", None),
        ("2025-02-03 04:05:06+24:00", None),
        ("٢٠٢٥-02-03 04:05:06", None),  # Arabic-Indic digits
        ("2025-02-30 04:05:06", None),
        (" 2025-02-03 04:05:06", None),
    ],
)
def test_extracted_on_is_read_only_as_a_full_date_time(
    cell: str, expected: datetime | None
) -> None:
    row = {**GBFS_ROW, "location.bounding_box.extracted_on": cell}
    (feed,) = read_catalog(export(row)).gbfs
    assert feed.bounding_box_generated_at == expected


@pytest.mark.parametrize(
    ("cells", "expected"),
    [
        ({"status": "retired"}, {"status": None}),
        ({"is_official": "yes"}, {"official": None}),
        ({"is_official": ""}, {"official": None}),
        ({"location.bounding_box.minimum_latitude": "nan"}, {"bounding_box": None}),
        ({"location.bounding_box.maximum_longitude": ""}, {"bounding_box": None}),
        ({"location.bounding_box.minimum_latitude": "1_0"}, {"bounding_box": None}),
        (box("41", "40", "-74", "-73"), {"bounding_box": None}),
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
        assert getattr(feed, name) == value, name


def test_a_box_past_the_earth_s_ranges_is_kept_as_the_api_returns_it() -> None:
    row = {**GTFS_ROW, **box("-95", "95", "-200", "200")}
    (feed,) = read_catalog(export(row)).gtfs
    assert feed.bounding_box == BoundingBox(-95.0, 95.0, -200.0, 200.0)


@pytest.mark.parametrize(
    ("cell", "expected"),
    [
        ("40.5", 40.5),
        ("4.05e1", 40.5),
        # The shared ASCII-number reader allows surrounding spaces and tabs,
        # which the export never writes.
        (" 40.5\t", 40.5),
        ("inf", None),
        ("1e999", None),  # no finite double
        ("\u0664\u0660", None),  # Arabic-Indic 40
        ("40,5", None),
    ],
)
def test_a_bound_is_read_only_as_a_finite_ascii_number(
    cell: str, expected: float | None
) -> None:
    row = {**GTFS_ROW, "location.bounding_box.minimum_latitude": cell}
    (feed,) = read_catalog(export(row)).gtfs
    if expected is None:
        assert feed.bounding_box is None
    else:
        assert feed.bounding_box == BoundingBox(expected, 40.9, -74.3, -73.7)


@pytest.mark.parametrize(
    ("cell", "expected"),
    [
        ("0", 0),
        ("3", 3),
        ("007", 7),
        ("123456789", 123456789),
        ("", None),
        ("two", None),
        ("+1", None),
        ("-1", None),
        ("1.0", None),
        (" 1", None),
        ("\u0661", None),  # Arabic-Indic one, which int() reads as 1
        ("1234567890", None),
        # Past int()'s 4300-digit limit, which raises a bare ValueError.
        ("1" * 5000, None),
    ],
)
def test_authentication_type_is_read_only_as_ascii_digits(
    cell: str, expected: int | None
) -> None:
    (feed,) = read_catalog(export({**GTFS_ROW, "urls.authentication_type": cell})).gtfs
    assert feed.source_info is not None
    assert feed.source_info.authentication_type == expected


def test_unknown_entity_types_are_left_out() -> None:
    (feed,) = _rt(read_catalog(export({**GTFS_RT_ROW, "entity_type": "tu|xx"})))
    assert feed.entity_types == (EntityType.TRIP_UPDATES,)


def test_empty_list_columns_are_empty_tuples() -> None:
    row = {**GTFS_RT_ROW, "entity_type": "", "static_reference": ""}
    (feed,) = _rt(read_catalog(export(row)))
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
    assert (rows.gtfs, rows.gtfs_rt_listed, rows.gbfs) == ((), (), ())
    assert dict(rows.by_id) == {}


def test_a_repeated_id_keeps_the_first_row() -> None:
    rows = read_catalog(export(GTFS_ROW, {**GTFS_ROW, "provider": "Other"}))
    assert [feed.provider for feed in rows.gtfs] == ["MTA New York City Transit"]


def test_the_first_row_of_an_id_wins_whatever_its_data_type() -> None:
    rows = read_catalog(
        export(
            {**GTFS_ROW, "id": "mdb-1", "data_type": "gtfs_flex"},
            {**GTFS_ROW, "id": "mdb-1"},
            {**GTFS_ROW, "id": "mdb-2"},
            {**GBFS_ROW, "id": "mdb-2"},
            {**GTFS_RT_ROW, "id": "mdb-2"},
        )
    )
    # mdb-1's first row is of a type this library lacks: no feed at all.
    assert list(rows.by_id) == ["mdb-2"]
    assert isinstance(rows.by_id["mdb-2"], PublicGtfsFeed)
    assert (rows.gbfs, rows.gtfs_rt_listed) == ((), ())


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
        # A header past the field size limit: no columns to read rows by.
        b"id,data_type," + b"x" * (csv.field_size_limit() + 1) + b"\nmdb-1,gtfs\n",
        # An unclosed quote would otherwise swallow every later row.
        b'id,data_type,provider\nmdb-1,gtfs,"A\nmdb-2,gtfs,B\n',
        # Text after a closing quote would otherwise be joined onto the cell.
        b'id,data_type\n"mdb-1"x,gtfs\n',
        # An unclosed quote running on to a later quote reads as a stray
        # one: the records it swallowed can't be told, so none is guessed.
        b'id,data_type,provider\nmdb-1,gtfs,"A\nmdb-2,gtfs,"B"\nmdb-3,gtfs,C\n',
        b'id,data_type,provider\nmdb-1,gtfs,"A\nmdb-2,gtfs,B"x\nmdb-3,gtfs,C\n',
        # A broken quote in a later line of a multi-line record.
        b'id,data_type,note\nmdb-1,gtfs,"a\nb"c\nmdb-2,gtfs,d\n',
        # In the header.
        b'id,"data_type"x\nmdb-1,gtfs\n',
    ],
)
def test_a_body_that_is_no_export_raises(body: bytes) -> None:
    with pytest.raises(CatalogFormatError) as caught:
        read_catalog(body)
    # Nothing of the decode or CSV error (which can hold the body) rides along.
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None


def test_cells_past_the_header_are_ignored() -> None:
    body = b"id,data_type,provider\nmdb-1,gtfs,A,extra,more\n"
    (feed,) = read_catalog(body).gtfs
    assert (feed.id, feed.provider) == ("mdb-1", "A")


def test_a_short_row_reads_its_missing_cells_as_empty() -> None:
    body = b"id,data_type,provider,name\nmdb-1,gtfs\n"
    (feed,) = read_catalog(body).gtfs
    assert (feed.provider, feed.feed_name) == (None, None)


def test_a_nul_inside_a_cell_is_kept() -> None:
    body = b"id,data_type,provider\nmdb-1,gtfs,A\x00B\n"
    (feed,) = read_catalog(body).gtfs
    assert feed.provider == "A\x00B"


def test_each_gtfs_and_gbfs_box_is_read_once_for_the_filters() -> None:
    rows = read_catalog(
        export(
            GTFS_ROW,
            GTFS_RT_ROW,
            GBFS_ROW,
            {**GTFS_ROW, "id": "mdb-11", **box("", "", "", "")},
        )
    )
    assert dict(rows.boxes) == {
        "mdb-10": Box(40.5, 40.9, -74.3, -73.7),
        "gbfs-citibike": Box(40.6, 40.9, -74.1, -73.8),
    }


def test_the_rt_list_holds_feeds_with_an_entity_type_cell() -> None:
    rows = read_catalog(
        export(
            GTFS_RT_ROW,
            {**GTFS_RT_ROW, "id": "mdb-21", "entity_type": ""},
            {**GTFS_RT_ROW, "id": "mdb-22", "entity_type": "xx"},
        )
    )
    assert [feed.id for feed in _rt(rows)] == ["mdb-20", "mdb-21", "mdb-22"]
    # The API's list inner-joins the entity types; a type unknown here counts.
    assert [feed.id for feed in rows.gtfs_rt_listed] == ["mdb-20", "mdb-22"]


_TOO_LONG = "x" * (csv.field_size_limit() + 1)


@pytest.mark.parametrize(
    "note",
    [
        pytest.param(_TOO_LONG, id="unquoted"),
        pytest.param(f"{_TOO_LONG}, quoted", id="quoted"),
        # The reader stops inside the cell; its later lines, which here look
        # like a feed's row, are read past rather than as records.
        pytest.param(
            f"{_TOO_LONG}\r\nmdb-3,gtfs,,,,,Fake\r\nmdb-1,gtfs,,,,,Fake\r\n",
            id="multi-line",
        ),
        pytest.param(f'a ""quote"" {_TOO_LONG}\n" end', id="doubled-quotes"),
    ],
)
def test_a_record_past_the_field_size_limit_is_skipped(note: str) -> None:
    limit = csv.field_size_limit()
    rows = read_catalog(
        export(
            {**GTFS_ROW, "id": "mdb-1"},
            {**GTFS_ROW, "id": "mdb-2", "note": note},
            {**GTFS_ROW, "id": "mdb-4"},
        )
    )
    assert list(rows.by_id) == ["mdb-1", "mdb-4"]
    assert csv.field_size_limit() == limit  # the process-wide limit untouched


def test_a_skipped_record_doesn_t_claim_its_id() -> None:
    rows = read_catalog(
        export(
            {**GTFS_ROW, "id": "mdb-1", "note": _TOO_LONG},
            {**GTFS_ROW, "id": "mdb-1", "provider": "Second"},
        )
    )
    assert [feed.provider for feed in rows.gtfs] == ["Second"]


def test_blank_lines_between_records_are_skipped() -> None:
    body = b"id,data_type\r\n\r\nmdb-1,gtfs\r\n\r\n\nmdb-2,gtfs\r\n"
    assert list(read_catalog(body).by_id) == ["mdb-1", "mdb-2"]


def _splits(text: str, *, by_csv: bool) -> list[int | str]:
    """Each record's line count, then "broken" if one's quoting is."""
    lines = _Lines(text)
    reader = csv.reader(lines, strict=True)
    counts: list[int | str] = []
    while True:
        lines.record.clear()
        try:
            if by_csv:
                next(reader)
            elif not _record_ends(chain((next(lines),), lines)):
                return [*counts, "broken"]
        except StopIteration:
            return counts
        except csv.Error:
            return [*counts, "broken"]
        counts.append(len(lines.record))


@settings(max_examples=500)
@given(
    st.lists(
        st.sampled_from(["a", ",", '"', '""', "\r", "\n", "\r\n", "\u00e9", "\x00"]),
        max_size=30,
    ).map("".join)
)
def test_records_end_where_the_csv_module_s_strict_reader_ends_them(
    text: str,
) -> None:
    """Below the field size limit, every csv.Error is broken quoting."""
    assert _splits(text, by_csv=False) == _splits(text, by_csv=True)
