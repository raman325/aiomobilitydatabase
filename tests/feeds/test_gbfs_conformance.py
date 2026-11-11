"""GBFS conformance: the spec's MUST/SHOULD clauses the handle honours."""

from datetime import UTC, date, datetime, timedelta, timezone

import pytest
from hypothesis import given
from hypothesis import strategies as st

from aiomobilitydatabase.feeds.client import MobilityFeedsClient
from aiomobilitydatabase.feeds.exceptions import FeedParseError, SourceConnectionError
from aiomobilitydatabase.feeds.gbfs import (
    _document_lifetime,
    _iso_date,
    _localized,
    _reported_at,
)
from aiomobilitydatabase.feeds.geo import Circle

from tests.feeds.fixtures import GBFS_FEED, TOKEN_RESPONSE, with_base
from tests.mock_server import MockApi

NOW = datetime(2026, 7, 31, 12, 0, tzinfo=UTC)


def _mock_catalog(mock_api: MockApi) -> None:
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get(
        "/v1/gbfs_feeds/gbfs-300", payload=with_base(GBFS_FEED, mock_api.url())
    )


def _doc(data: dict[str, object], *, ttl: int = 60) -> dict[str, object]:
    """A GBFS document updated just now."""
    return {
        "ttl": ttl,
        "last_updated": int(datetime.now(UTC).timestamp()),
        "data": data,
    }


# -- D1, D2, U13: 3.0 localized fields ------------------------------------------


async def test_30_localized_urls_and_names_are_read(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """3.0 made terms_url, privacy_url, short_name, operator and alert urls
    localized lists; each is read in the preferred language."""
    _mock_catalog(mock_api)
    fr, en = "fr", "en"
    mock_api.get(
        "/gbfs/system_information.json",
        payload=_doc(
            {
                "system_id": "s",
                "short_name": [{"text": "TB", "language": en}],
                "operator": [{"text": "Op", "language": en}],
                "terms_url": [
                    {"text": "https://x/conditions", "language": fr},
                    {"text": "https://x/terms", "language": en},
                ],
                "privacy_url": [{"text": "https://x/privacy", "language": en}],
            }
        ),
    )
    alert = {
        "alert_id": "a",
        "type": "other",
        "summary": [{"text": "Closed", "language": en}],
        "url": [{"text": "https://x/alert", "language": en}],
        "times": [{"start": "2026-07-31T12:00:00Z"}],
    }
    mock_api.get("/gbfs/system_alerts.json", payload=_doc({"alerts": [alert]}))
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    info = await handle.get_system_info()
    assert (info.short_name, info.operator) == ("TB", "Op")
    assert (info.terms_url, info.privacy_url) == (
        "https://x/terms",
        "https://x/privacy",
    )
    [read] = await handle.get_alerts()
    assert read.url == "https://x/alert"
    assert read.active_periods == ((datetime(2026, 7, 31, 12, tzinfo=UTC), None),)


@given(url=st.from_regex(r"https://[a-z]{1,8}\.example/[a-z]{0,8}", fullmatch=True))
def test_a_2x_url_and_its_30_localized_form_read_alike(url: str) -> None:
    localized = [
        {"text": "https://other/", "language": "de"},
        {"text": url, "language": "en"},
    ]
    assert _localized(url) == _localized(localized) == url


# -- D4: an OPTIONAL file that answers 404 -----------------------------------------


@pytest.mark.parametrize(
    ("path", "getter"),
    [
        ("/gbfs/vehicle_types.json", "get_vehicle_types"),
        ("/gbfs/system_pricing_plans.json", "get_pricing_plans"),
        ("/gbfs/system_regions.json", "get_regions"),
        ("/gbfs/system_alerts.json", "get_alerts"),
    ],
)
async def test_an_optional_file_answering_404_is_no_records(
    mock_api: MockApi, feeds_client: MobilityFeedsClient, path: str, getter: str
) -> None:
    """ "OPTIONAL files MAY 404. A 404 of an OPTIONAL file SHOULD NOT be
    considered an error." Any other error still is."""
    _mock_catalog(mock_api)
    mock_api.get(path, status=404, body=b"", content_type="text/plain")
    mock_api.get(path, status=500, body=b"", content_type="text/plain")
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    assert await getattr(handle, getter)() == []
    with pytest.raises(SourceConnectionError) as excinfo:
        await getattr(handle, getter)()
    assert excinfo.value.status == 500


# -- D5: the discovery document's ttl --------------------------------------------


async def test_a_moved_endpoint_is_followed_once_discovery_lapses(
    mock_api: MockApi, direct_client: MobilityFeedsClient
) -> None:
    """gbfs.json has a ttl like every file: once it lapses, discovery is
    asked again and an endpoint the system moved is followed."""

    def discovery(path: str) -> dict[str, object]:
        feeds = [{"name": "system_information", "url": mock_api.url(path)}]
        return {**_doc({"feeds": feeds}, ttl=60), "version": "3.0"}

    info = _doc({"system_id": "s", "name": "Old"}, ttl=0)
    mock_api.get("/gbfs/gbfs.json", payload=discovery("/gbfs/si-v1.json"))
    mock_api.get("/gbfs/si-v1.json", payload=info)
    mock_api.get("/gbfs/si-v1.json", payload=info)
    handle = await direct_client.get_gbfs_feed_from_url(mock_api.url("/gbfs/gbfs.json"))
    assert (await handle.get_system_info()).name == "Old"
    assert (await handle.get_system_info()).name == "Old"  # discovery still fresh
    # Discovery's ttl lapses: asked again, and the endpoint has moved.
    stale = handle._docs._entries[mock_api.url("/gbfs/gbfs.json")]
    stale.stored_at -= 61
    mock_api.get("/gbfs/gbfs.json", payload=discovery("/gbfs/si-v2.json"))
    mock_api.get("/gbfs/si-v2.json", payload=_doc({"system_id": "s", "name": "New"}))
    assert (await handle.get_system_info()).name == "New"
    hits = [r.path for r in mock_api.requests if r.path == "/gbfs/gbfs.json"]
    assert len(hits) == 2


@pytest.fixture
async def direct_client():  # type: ignore[no-untyped-def]
    async with MobilityFeedsClient() as client:
        yield client


# -- D9: a docked vehicle without coordinates ------------------------------------


async def test_a_docked_vehicle_without_coordinates_is_kept(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """lat/lon are REQUIRED only without a station_id: a docked vehicle is
    kept with None coordinates (and left out of a zone); a row with neither
    is invalid."""
    _mock_catalog(mock_api)
    bikes = [
        {"bike_id": "docked", "station_id": "st1", "is_reserved": False},
        {"bike_id": "loose", "lat": 34.05, "lon": -118.25},
        {"bike_id": "lost", "is_reserved": False},
    ]
    for _ in range(2):
        mock_api.get("/gbfs/free_bike_status.json", payload=_doc({"bikes": bikes}))
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    by_id = {v.id: v for v in await handle.get_vehicles()}
    assert set(by_id) == {"docked", "loose"}
    assert (by_id["docked"].latitude, by_id["docked"].station_id) == (None, "st1")
    zone = Circle(latitude=34.05, longitude=-118.25, radius_m=1000)
    assert [v.id for v in await handle.get_vehicles(zone)] == ["loose"]


# -- D10: BCP 47 tags are case-insensitive ---------------------------------------


def test_language_tags_match_in_any_case() -> None:
    names = [{"text": "Vélos", "language": "fr"}, {"text": "Bikes", "language": "EN"}]
    assert _localized(names) == "Bikes"


async def test_a_2x_discovery_language_key_matches_in_any_case(
    mock_api: MockApi, direct_client: MobilityFeedsClient
) -> None:
    feeds = [{"name": "system_information", "url": mock_api.url("/gbfs/en.json")}]
    other = [{"name": "system_information", "url": mock_api.url("/gbfs/fr.json")}]
    discovery = _doc({"fr": {"feeds": other}, "EN": {"feeds": feeds}})
    mock_api.get("/gbfs/gbfs.json", payload=discovery)
    mock_api.get("/gbfs/en.json", payload=_doc({"system_id": "s", "name": "Bikes"}))
    handle = await direct_client.get_gbfs_feed_from_url(mock_api.url("/gbfs/gbfs.json"))
    assert (await handle.get_system_info()).name == "Bikes"


# -- D3: timestamps and dates ------------------------------------------------------


@given(
    when=st.datetimes(min_value=datetime(1971, 1, 1), max_value=datetime(9998, 1, 1)),
    offset=st.integers(-14 * 60, 14 * 60),
)
def test_an_rfc3339_timestamp_round_trips_and_needs_its_offset(
    when: datetime, offset: int
) -> None:
    aware = when.replace(tzinfo=timezone(timedelta(minutes=offset)))
    assert _reported_at(aware.isoformat()) == aware
    assert _reported_at(when.isoformat()) is None  # no offset: no instant


@given(day=st.dates())
def test_a_gbfs_date_is_only_yyyy_mm_dd(day: date) -> None:
    if day.year >= 1000:
        assert _iso_date(day.isoformat()) == day
    assert _iso_date(f"{day.year:04d}{day.month:02d}{day.day:02d}") is None
    assert _iso_date(day.strftime("%G-W%V-%u")) is None


# -- D11, U1, U2: the document ttl -------------------------------------------------


@pytest.mark.parametrize("last_updated", [None, "yesterday", "2026-07-31T12:00:00"])
def test_a_ttl_that_cant_be_placed_in_time_declares_nothing_fresh(
    last_updated: object,
) -> None:
    """last_updated is REQUIRED and anchors the ttl: without a usable one,
    the ttl can't be placed, so the document is stale now."""
    document: dict[str, object] = {"ttl": 3600, "data": {}}
    if last_updated is not None:
        document["last_updated"] = last_updated
    assert _document_lifetime(document, None, NOW) == 0.0


@given(
    ttl=st.integers(0, 10**6),
    age=st.integers(-3600, 2 * 10**6),
    offset=st.integers(-14 * 60, 14 * 60),
    header=st.none() | st.floats(0, 10**7),
)
def test_the_ttl_counts_from_last_updated_in_either_format(
    ttl: int, age: int, offset: int, header: float | None
) -> None:
    """A 2.x POSIX and a 3.0 RFC 3339 last_updated anchor the ttl alike; the
    result is what's left of it (never more than ttl, a future
    last_updated being clock skew), the shorter of it and the headers'."""
    updated = NOW - timedelta(seconds=age)
    local = updated.astimezone(timezone(timedelta(minutes=offset)))
    posix = {"ttl": ttl, "last_updated": int(updated.timestamp())}
    rfc3339 = {"ttl": ttl, "last_updated": local.isoformat()}
    left = float(min(ttl, max(0, ttl - age)))
    expected = left if header is None else min(left, header)
    assert _document_lifetime(posix, header, NOW) == expected
    assert _document_lifetime(rfc3339, header, NOW) == expected
    assert _document_lifetime({"data": {}}, header, NOW) == header


# -- U3: 3.0 num_vehicles_disabled -------------------------------------------------


async def test_the_30_disabled_count_is_read(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    station = {"station_id": "st", "lat": 34.05, "lon": -118.25}
    status = {
        "station_id": "st",
        "num_vehicles_available": 3,
        "num_vehicles_disabled": 2,
    }
    mock_api.get(
        "/gbfs/station_information.json", payload=_doc({"stations": [station]})
    )
    mock_api.get("/gbfs/station_status.json", payload=_doc({"stations": [status]}))
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    [read] = await handle.get_stations()
    assert (read.bikes_available, read.bikes_disabled) == (3, 2)


async def test_a_document_that_isnt_json_is_a_parse_error(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    mock_api.get(
        "/gbfs/system_information.json",
        body=b"<html>oops</html>",
        content_type="text/html",
    )
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    with pytest.raises(FeedParseError, match="Malformed GBFS JSON"):
        await handle.get_system_info()
