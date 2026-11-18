"""The public catalog's models equal the API client's, field by field.

Each parity case takes an API payload, writes the export row
mobility-feed-api's ``export_csv`` writes for that same feed, reads it, and
compares it with the API payload decoded by the API client. Only the fields
the export doesn't carry are set aside; everything else must be equal.
"""

from __future__ import annotations

import copy
import csv
import dataclasses
import io
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from aiomobilitydatabase._catalog_csv import CatalogFeed, read_catalog
from aiomobilitydatabase.models import (
    EntityType,
    GbfsFeed,
    GtfsFeed,
    GtfsRtFeed,
    Redirect,
)

from tests.fixtures import BOUNDING_BOX, GBFS_FEED, GTFS_FEED, GTFS_RT_FEED
from tests.public_catalog_data import export

SLICE = Path(__file__).parent / "data" / "feeds_v2_slice.csv"


def _text(value: object) -> str:
    """A cell as csv.DictWriter writes the value: None empty, else str()."""
    return "" if value is None else str(value)


def _timestamp(value: str) -> str:
    """str() of the database's datetime, as the export writes one."""
    moment = datetime.fromisoformat(value)
    if moment.tzinfo is None:  # a column without zone, written from UTC
        moment = moment.replace(tzinfo=UTC)
    return str(moment)


def _row(
    api: dict[str, Any], referenced_box: dict[str, float] | None = None
) -> dict[str, str]:
    """The export row mobility-feed-api writes for a feed the API returns so.

    Mirrors functions-python/export_csv/src/main.py: get_feed_csv_data,
    then get_gtfs_feed_csv_data, get_gtfs_rt_feed_csv_data or (MobilityData
    PR #1888) get_gbfs_feed_csv_data. ``referenced_box`` is the bounding box
    of an RT feed's first static reference, which the export copies onto the
    RT row.
    """
    data_type = api["data_type"]
    source = api.get("source_info") or {}
    # get_feed_csv_data: redirects by target id, comments paired by position
    # ("" for none), all-empty comments written as "".
    redirects = sorted(api.get("redirects") or [], key=lambda r: r["target_id"])
    comments = "|".join(r.get("comment") or "" for r in redirects)
    if comments.strip("|") == "":
        comments = ""
    locations = api.get("locations") or [{}]
    row = {
        "id": api["id"],
        "data_type": data_type,
        "entity_type": "",
        "location.country_code": _text(locations[0].get("country_code")),
        "location.subdivision_name": _text(locations[0].get("subdivision_name")),
        "location.municipality": _text(locations[0].get("municipality")),
        "provider": _text(api.get("provider")),
        # GBFS: "no process yet for reviewing whether a GBFS feed is official".
        "is_official": "" if data_type == "gbfs" else _text(api.get("official")),
        "name": _text(api.get("feed_name")),
        "note": _text(api.get("note")),
        "feed_contact_email": _text(api.get("feed_contact_email")),
        "static_reference": "",
        "urls.direct_download": _text(source.get("producer_url")),
        # The database's authentication_type is a string enum, "0" to "2".
        "urls.authentication_type": _text(source.get("authentication_type")),
        "urls.authentication_info": _text(source.get("authentication_info_url")),
        "urls.api_key_parameter_name": _text(source.get("api_key_parameter_name")),
        "urls.license": _text(source.get("license_url")),
        "status": _text(api.get("status")),
        "redirect.id": "|".join(r["target_id"] for r in redirects),
        "redirect.comment": comments,
    }
    box: dict[str, float] | None = None
    extracted_on = ""
    if data_type == "gtfs":
        latest = api.get("latest_dataset")
        if latest:
            url = latest["hosted_url"]
            position = url.find(api["id"])
            if position != -1:
                url = url[: position + len(api["id"]) + 1] + "latest.zip"
            row["urls.latest"] = url
            report = latest.get("validation_report") or {}
            row["features"] = "|".join(sorted(report.get("features") or []))
            extracted_on = _timestamp(latest["downloaded_at"])
        box = api.get("bounding_box")
    elif data_type == "gtfs_rt":
        row["entity_type"] = "|".join(sorted(api.get("entity_types") or []))
        # Active references first, by a stable sort; the references here are
        # all active, so the API's order stands.
        row["static_reference"] = "|".join(api.get("feed_references") or [])
        box = referenced_box if api.get("feed_references") else None
    else:
        box = api.get("bounding_box")
        if box and api.get("bounding_box_generated_at"):
            extracted_on = _timestamp(api["bounding_box_generated_at"])
    if box:
        row["location.bounding_box.minimum_latitude"] = _text(box["minimum_latitude"])
        row["location.bounding_box.maximum_latitude"] = _text(box["maximum_latitude"])
        row["location.bounding_box.minimum_longitude"] = _text(box["minimum_longitude"])
        row["location.bounding_box.maximum_longitude"] = _text(box["maximum_longitude"])
        row["location.bounding_box.extracted_on"] = extracted_on
    return row


# What the export doesn't carry: the API's value is replaced by None, as
# the public catalog leaves it. Everything else must be equal.
_NOT_EXPORTED = (
    "created_at",
    "external_ids",
    "official_updated_at",
    "seasonal",
    "related_links",
    "locations",
    "latest_dataset",
    "visualization_dataset_id",
    "reliability_seal",
    "system_id",
    "provider_url",
    "versions",
)
_SOURCE_NOT_EXPORTED = (
    "is_producer_url_unstable",
    "license_id",
    "license_is_spdx",
    "license_notes",
    "license_tags",
)


def _expected(model: Any) -> Any:
    fields = {f.name for f in dataclasses.fields(model)}
    return dataclasses.replace(
        model,
        **dict.fromkeys(fields.intersection(_NOT_EXPORTED)),
        source_info=dataclasses.replace(
            model.source_info, **dict.fromkeys(_SOURCE_NOT_EXPORTED)
        ),
    )


def _read_one(row: dict[str, str]) -> CatalogFeed:
    rows = read_catalog(export(row))
    (feed,) = rows.by_id.values()
    return feed


# Redirect sets as the API returns them (sorted by target id): none, one with
# a comment, two with the comment on either side, two without comments.
_REDIRECTS = [
    pytest.param([], id="no-redirects"),
    pytest.param(
        [{"target_id": "mdb-2092", "comment": "Only the source URL changed."}],
        id="one-commented",
    ),
    pytest.param(
        [
            {"target_id": "mdb-2093", "comment": "See 2093 for Gdańsk."},
            {"target_id": "mdb-2094", "comment": None},
        ],
        id="first-commented",
    ),
    pytest.param(
        [
            {"target_id": "mdb-2093", "comment": None},
            {"target_id": "mdb-2094", "comment": "See 2094 for Gdynia."},
        ],
        id="second-commented",
    ),
    pytest.param(
        [
            {"target_id": "mdb-2093", "comment": None},
            {"target_id": "tld-461", "comment": None},
        ],
        id="none-commented",
    ),
]


def test_gtfs_parity() -> None:
    api = copy.deepcopy(GTFS_FEED)
    feed = _read_one(_row(api))
    assert feed == _expected(GtfsFeed.from_dict(api))


@pytest.mark.parametrize("redirects", _REDIRECTS)
def test_gtfs_parity_redirects_and_text(redirects: list[dict[str, Any]]) -> None:
    api = copy.deepcopy(GTFS_FEED)
    api.update(
        redirects=redirects,
        status="deprecated",
        official=False,
        note='収録路線 全線\n　GTFS-JP 3版対応, with a comma and a "quote"',
        feed_name=None,
        feed_contact_email=None,
    )
    api["source_info"] = {**api["source_info"], "authentication_type": 0}
    feed = _read_one(_row(api))
    assert feed == _expected(GtfsFeed.from_dict(api))


def test_gtfs_rt_parity() -> None:
    api = copy.deepcopy(GTFS_RT_FEED)
    # The API always sends a list (BaseFeedImpl sorts feed.redirectingids),
    # never the fixture's null.
    api["redirects"] = []
    # The API lists entity types in the database's (unordered) relationship
    # order; the export sorts them, so their order is the export's.
    api["entity_types"] = sorted(api["entity_types"])
    feed = _read_one(_row(api, referenced_box=BOUNDING_BOX))
    assert feed == _expected(GtfsRtFeed.from_dict(api))


@pytest.mark.parametrize("redirects", _REDIRECTS)
def test_gtfs_rt_parity_references_types_and_redirects(
    redirects: list[dict[str, Any]],
) -> None:
    api = copy.deepcopy(GTFS_RT_FEED)
    api.update(
        redirects=redirects,
        entity_types=["sa", "tu", "vp"],
        feed_references=["mdb-1210", "mdb-1209"],
        official=True,
        feed_name="Realtime",
        note="a note",
        feed_contact_email="rt@example.com",
        status="inactive",
    )
    feed = _read_one(_row(api, referenced_box=BOUNDING_BOX))
    expected = _expected(GtfsRtFeed.from_dict(api))
    assert feed == expected
    assert isinstance(feed, GtfsRtFeed)
    assert feed.entity_types == (
        EntityType.SERVICE_ALERTS,
        EntityType.TRIP_UPDATES,
        EntityType.VEHICLE_POSITIONS,
    )
    assert feed.feed_references == ("mdb-1210", "mdb-1209")


@pytest.mark.parametrize(
    "generated_at",
    [
        # The API sends this column without an offset (stored without a zone).
        pytest.param("2023-07-10T22:06:00", id="whole-seconds"),
        pytest.param("2023-07-10T22:06:00.123456", id="microseconds"),
    ],
)
def test_gbfs_parity(generated_at: str) -> None:
    api = copy.deepcopy(GBFS_FEED)
    api["bounding_box_generated_at"] = generated_at
    feed = _read_one(_row(api))
    assert feed == _expected(GbfsFeed.from_dict(api))


@pytest.mark.parametrize("redirects", _REDIRECTS)
def test_gbfs_parity_redirects(redirects: list[dict[str, Any]]) -> None:
    api = copy.deepcopy(GBFS_FEED)
    api.update(
        redirects=redirects,
        bounding_box_generated_at="2025-02-03T04:05:06",
        feed_contact_email="gbfs@example.com",
    )
    feed = _read_one(_row(api))
    assert feed == _expected(GbfsFeed.from_dict(api))


def test_gbfs_parity_without_a_box() -> None:
    api = copy.deepcopy(GBFS_FEED)
    api.update(bounding_box=None, bounding_box_generated_at=None)
    feed = _read_one(_row(api))
    assert feed == _expected(GbfsFeed.from_dict(api))


def test_the_recorded_export_is_the_csv_modules_bytes() -> None:
    """The slice is records 523-822 of the live feeds_v2.csv (2026-11-18).

    Ids jbda-shimadacity-shimada_combus to mdb-108, byte for byte the live
    file's (the export writes with the csv module's defaults: QUOTE_MINIMAL,
    each record ending "\\r\\n"). The window is the first that holds
    multi-line quoted notes, redirect comments, RT rows with entity types
    and non-ASCII text together: the live file's multi-line notes all come
    before its first non-"nan" comment, so this window's comments are the
    database's literal "nan" (the API returns them the same).
    """
    body = SLICE.read_bytes()
    records = list(csv.reader(io.StringIO(body.decode(), newline="")))
    out = io.StringIO(newline="")
    csv.writer(out).writerows(records)
    assert out.getvalue().encode() == body
    assert len(records) == 301


def test_the_recorded_export_reads_and_every_row_maps() -> None:
    rows = read_catalog(SLICE.read_bytes())
    assert (len(rows.gtfs), len(rows.gtfs_rt), len(rows.gbfs)) == (260, 40, 0)
    assert len(rows.by_id) == 300
    for feed in (*rows.gtfs, *rows.gtfs_rt):
        assert feed.id and feed.provider and feed.source_info is not None
        assert feed.source_info.producer_url
        assert feed.source_info.authentication_type == 0
        assert feed.status is not None
    assert sum(feed.bounding_box is not None for feed in rows.gtfs) == 255
    assert all(feed.entity_types for feed in rows.gtfs_rt)
    assert all(feed.feed_references for feed in rows.gtfs_rt)
    assert sum(1 for f in rows.gtfs if f.note and "\n" in f.note) == 23
    assert sum(1 for f in rows.gtfs if f.provider and not f.provider.isascii()) > 0
    redirected = [f for f in (*rows.gtfs, *rows.gtfs_rt) if f.redirects]
    assert len(redirected) == 30
    commented = {
        f.id: f.redirects
        for f in redirected
        if any(r.comment for r in f.redirects or ())
    }
    assert len(commented) == 8
    assert commented["mdb-108"] == (Redirect(target_id="tld-461", comment="nan"),)
