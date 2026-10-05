"""Tests for GBFS endpoint resolution, ttl caching, system info, stations."""

from datetime import UTC, date, datetime

import pytest

from aiomobilitydatabase.feeds.client import MobilityFeedsClient
from aiomobilitydatabase.feeds.exceptions import FeedParseError, SourceConnectionError
from aiomobilitydatabase.feeds.gbfs import _endpoints_from_discovery
from aiomobilitydatabase.feeds.geo import Circle

from tests.feeds.fixtures import (
    GBFS_FEED,
    GTFS_FEED,
    STATION_INFO_23,
    STATION_STATUS_23,
    SYSTEM_INFO_23,
    SYSTEM_INFO_30,
    TOKEN_RESPONSE,
    with_base,
)
from tests.mock_server import MockApi


def _mock_catalog(mock_api: MockApi) -> None:
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/gbfs_feeds/gbfs-300", payload=with_base(GBFS_FEED, base))


async def test_system_info(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    mock_api.get("/gbfs/system_information.json", payload=SYSTEM_INFO_23)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    info = await handle.get_system_info()
    assert info.system_id == "test-bikes"
    assert info.name == "Test Bikes"


async def test_stations_merged(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    mock_api.get("/gbfs/station_information.json", payload=STATION_INFO_23)
    mock_api.get("/gbfs/station_status.json", payload=STATION_STATUS_23)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    stations = {s.id: s for s in await handle.get_stations()}
    st1 = stations["st1"]
    assert st1.name == "Dock A"
    assert st1.capacity == 20
    assert st1.bikes_available == 5
    assert st1.docks_available == 15
    assert st1.is_renting is True
    assert st1.vehicle_types_available == {"bike": 4, "ebike": 1}
    # 2.x station_information rental_uris pass through as provided.
    assert st1.rental_uris == {
        "android": "https://example.com/app?station=st1&platform=android",
        "ios": "https://example.com/app?station=st1&platform=ios",
        "web": "https://example.com/stations/st1",
    }
    st2 = stations["st2"]
    assert st2.is_renting is False
    assert st2.vehicle_types_available is None
    assert st2.rental_uris is None  # absent in the document


async def test_stations_zone_filter(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    mock_api.get("/gbfs/station_information.json", payload=STATION_INFO_23)
    mock_api.get("/gbfs/station_status.json", payload=STATION_STATUS_23)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    # st1 at (34.05,-118.25); st2 at (34.10,-118.20) ~7 km away.
    zoned = await handle.get_stations(
        zone=Circle(latitude=34.05, longitude=-118.25, radius_m=1000)
    )
    assert [station.id for station in zoned] == ["st1"]


async def test_ttl_micro_cache(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    mock_api.get("/gbfs/station_information.json", payload=STATION_INFO_23)
    mock_api.get("/gbfs/station_status.json", payload=STATION_STATUS_23)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    await handle.get_stations()
    await handle.get_stations()  # within ttl: NO second fetch of either doc
    info_hits = [
        r for r in mock_api.requests if r.path == "/gbfs/station_information.json"
    ]
    status_hits = [
        r for r in mock_api.requests if r.path == "/gbfs/station_status.json"
    ]
    assert len(info_hits) == 1
    assert len(status_hits) == 1


async def test_gbfs_feed_id_resolving_to_gtfs_shaped_payload(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """Task 15R-b item 5: get_gbfs_feed() on an id whose catalog record is
    actually GTFS-shaped (data_type mismatch -- caller/config error, not
    caught at resolution time since GbfsFeed.from_dict tolerates unknown/
    missing fields). ``versions`` is absent from a GTFS payload, so endpoint
    resolution yields an empty dict; verified (not guessed) actual behavior
    per-method, since it isn't uniform:

    - get_system_info() needs one specific endpoint -> raises
      SourceConnectionError("... not published").
    - get_stations() ALSO needs an endpoint up front (station_information)
      -> ALSO raises SourceConnectionError, not an empty list.
    - get_vehicles() degrades gracefully by design (docstring: "Returns []
      for docked-only systems") since it checks endpoint presence itself
      before ever calling _document -> returns [].
    """
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/gbfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    handle = await feeds_client.get_gbfs_feed("mdb-100")
    with pytest.raises(SourceConnectionError, match="not published"):
        await handle.get_system_info()
    with pytest.raises(SourceConnectionError, match="not published"):
        await handle.get_stations()
    assert await handle.get_vehicles() == []


async def test_document_endpoint_not_published_raises(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    base = mock_api.url()
    feed = with_base(GBFS_FEED, base)
    feed["versions"][0]["endpoints"] = [
        e for e in feed["versions"][0]["endpoints"] if e["name"] != "system_information"
    ]
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/gbfs_feeds/gbfs-300", payload=feed)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    with pytest.raises(SourceConnectionError, match="not published"):
        await handle.get_system_info()


async def test_document_fetch_unreachable_raises_source_connection_error(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    base = mock_api.url()
    feed = with_base(GBFS_FEED, base)
    feed["versions"][0]["endpoints"] = [
        {
            "name": "system_information",
            "url": "http://127.0.0.1:1/gbfs/system_information.json",
        },
    ]
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/gbfs_feeds/gbfs-300", payload=feed)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    with pytest.raises(SourceConnectionError, match="Error fetching"):
        await handle.get_system_info()


async def test_document_rejects_non_http_scheme(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """Task 15R-b item 8: a GBFS endpoint URL is catalog/GBFS-document
    DATA; a non-http(s) scheme is rejected explicitly, before any network
    attempt, naming the scheme.
    """
    base = mock_api.url()
    feed = with_base(GBFS_FEED, base)
    feed["versions"][0]["endpoints"] = [
        {"name": "system_information", "url": "file:///etc/passwd"},
    ]
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/gbfs_feeds/gbfs-300", payload=feed)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    with pytest.raises(SourceConnectionError, match="scheme"):
        await handle.get_system_info()


async def test_gbfs_30_localized_name(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    base = mock_api.url()
    feed_30 = with_base(GBFS_FEED, base)
    feed_30["versions"] = [
        {
            "version": "3.0",
            "source": "gbfs_versions",
            "endpoints": [
                {
                    "name": "system_information",
                    "url": f"{base}/gbfs3/system_information.json",
                },
                {"name": "vehicle_status", "url": f"{base}/gbfs3/vehicle_status.json"},
            ],
        },
        *feed_30["versions"],  # 2.3 also present: 3.0 must win
    ]
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/gbfs_feeds/gbfs-300", payload=feed_30)
    mock_api.get("/gbfs3/system_information.json", payload=SYSTEM_INFO_30)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    info = await handle.get_system_info()
    assert info.system_id == "test-bikes-3"
    assert info.name == "Test Bikes 3"


@pytest.mark.parametrize(
    "document",
    [
        pytest.param({}, id="no-data-key"),
        pytest.param({"data": None}, id="data-null"),
        pytest.param({"data": []}, id="data-list"),
        pytest.param({"data": "nope"}, id="data-string"),
        pytest.param({"data": {"feeds": "nope"}}, id="feeds-truthy-non-list"),
        pytest.param({"data": {"feeds": {}}}, id="feeds-empty-dict"),
        pytest.param(None, id="document-not-a-mapping"),
    ],
)
def test_endpoints_from_discovery_malformed_raises_feed_parse_error(
    document: object,
) -> None:
    """A discovery document the spec can't be read out of raises the
    documented FeedParseError -- never KeyError/TypeError/AttributeError.
    """
    with pytest.raises(FeedParseError):
        _endpoints_from_discovery(document)


def test_endpoints_from_discovery_bad_feeds_still_tries_language_blocks() -> None:
    """A junk ``data.feeds`` must not short-circuit the 2.x language-keyed
    fallback: the usable ``data.en.feeds`` block still resolves.
    """
    document = {
        "data": {
            "feeds": "nope",
            "en": {"feeds": [{"name": "system_information", "url": "https://e.com/s"}]},
        }
    }
    assert _endpoints_from_discovery(document) == {
        "system_information": "https://e.com/s"
    }


@pytest.mark.parametrize(
    "ttl",
    [
        pytest.param("60s", id="unit-suffixed-string"),
        pytest.param({"seconds": 60}, id="object"),
        pytest.param([60], id="list"),
        pytest.param("nan", id="nan-string"),
        pytest.param(-5, id="negative"),
    ],
)
async def test_malformed_ttl_degrades_to_no_caching(
    mock_api: MockApi, feeds_client: MobilityFeedsClient, ttl: object
) -> None:
    """A ttl that isn't a usable number must not escape as ValueError or
    TypeError: the document still parses and simply isn't cached.
    """
    _mock_catalog(mock_api)
    payload = {**SYSTEM_INFO_23, "ttl": ttl}
    mock_api.get("/gbfs/system_information.json", payload=payload)
    mock_api.get("/gbfs/system_information.json", payload=payload)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    assert (await handle.get_system_info()).system_id == "test-bikes"
    await handle.get_system_info()
    hits = [r for r in mock_api.requests if r.path == "/gbfs/system_information.json"]
    assert len(hits) == 2  # no usable ttl -> no micro-cache


async def test_numeric_string_ttl_still_caches(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """Producers do ship ttl as a string; a parseable one keeps caching."""
    _mock_catalog(mock_api)
    mock_api.get(
        "/gbfs/system_information.json", payload={**SYSTEM_INFO_23, "ttl": "60"}
    )
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    await handle.get_system_info()
    await handle.get_system_info()
    hits = [r for r in mock_api.requests if r.path == "/gbfs/system_information.json"]
    assert len(hits) == 1


async def test_stations_without_an_id_are_skipped(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """An id-less station row would synthesize the colliding literal id
    "None" (and an empty id collides identically), so it is dropped.
    """
    _mock_catalog(mock_api)
    mock_api.get(
        "/gbfs/station_information.json",
        payload={
            "ttl": 60,
            "data": {
                "stations": [
                    {"lat": 34.05, "lon": -118.25},
                    {"station_id": None, "lat": 34.05, "lon": -118.25},
                    {"station_id": "", "lat": 34.05, "lon": -118.25},
                    {"station_id": "real", "lat": 34.05, "lon": -118.25},
                ]
            },
        },
    )
    mock_api.get("/gbfs/station_status.json", payload={"ttl": 60, "data": {}})
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    assert [s.id for s in await handle.get_stations()] == ["real"]


async def test_system_info_without_system_id_raises(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """system_id is the identity consumers key a device/config entry on;
    a document that omits it is unusable, not a system named "None".
    """
    _mock_catalog(mock_api)
    mock_api.get(
        "/gbfs/system_information.json",
        payload={"ttl": 60, "data": {"name": "Nameless"}},
    )
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    with pytest.raises(FeedParseError, match="system_id"):
        await handle.get_system_info()


_STATION_ROW = {"station_id": "real", "lat": 34.05, "lon": -118.25}


@pytest.mark.parametrize(
    ("info_data", "status_data", "expected_ids"),
    [
        pytest.param([], {}, [], id="info-data-is-a-list"),
        pytest.param({"stations": "nope"}, {}, [], id="stations-not-a-list"),
        pytest.param(
            {"stations": [1, "x", None, _STATION_ROW]},
            {},
            ["real"],
            id="non-dict-info-entries",
        ),
        pytest.param(
            {"stations": [_STATION_ROW]},
            [],
            ["real"],
            id="status-data-is-a-list",
        ),
        pytest.param(
            {"stations": [_STATION_ROW]},
            {"stations": [3, None, {"station_id": "orphan"}]},
            ["real"],
            id="non-dict-status-entries-and-orphans",
        ),
    ],
)
async def test_get_stations_total_over_malformed_envelopes(
    mock_api: MockApi,
    feeds_client: MobilityFeedsClient,
    info_data: object,
    status_data: object,
    expected_ids: list[str],
) -> None:
    """get_stations() is total over arbitrary JSON-shaped documents: a
    wrongly typed data envelope, station list, or station entry yields the
    usable rows rather than AttributeError/TypeError.
    """
    _mock_catalog(mock_api)
    mock_api.get("/gbfs/station_information.json", payload={"data": info_data})
    mock_api.get("/gbfs/station_status.json", payload={"data": status_data})
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    assert [s.id for s in await handle.get_stations()] == expected_ids


@pytest.mark.parametrize(
    ("available", "expected"),
    [
        pytest.param([{"vehicle_type_id": "bike", "count": 4}], {"bike": 4}, id="ok"),
        pytest.param([{"vehicle_type_id": "bike"}], {"bike": 0}, id="absent-count"),
        pytest.param(
            [{"vehicle_type_id": "bike", "count": "4"}], {"bike": 4}, id="str"
        ),
        pytest.param([{"vehicle_type_id": "bike", "count": None}], None, id="null"),
        pytest.param([{"count": 4}], None, id="no-type-id"),
        pytest.param({"bike": 4}, None, id="object-not-a-list"),
        pytest.param("bike", None, id="string"),
        pytest.param([], None, id="empty"),
        pytest.param(
            [{"vehicle_type_id": "bike", "count": 2}, "not-a-dict", None, 7],
            {"bike": 2},
            id="list-with-non-mapping-entries",
        ),
    ],
)
async def test_vehicle_types_available_shapes(
    mock_api: MockApi,
    feeds_client: MobilityFeedsClient,
    available: object,
    expected: dict[str, int] | None,
) -> None:
    """vehicle_types_available: int(entry.get("count", 0)) raised on a null
    count and iterating an object yielded its keys. Unusable entries drop;
    an empty result is None (the documented "not published" value).
    """
    _mock_catalog(mock_api)
    mock_api.get(
        "/gbfs/station_information.json", payload={"data": {"stations": [_STATION_ROW]}}
    )
    mock_api.get(
        "/gbfs/station_status.json",
        payload={
            "data": {
                "stations": [
                    {"station_id": "real", "vehicle_types_available": available}
                ]
            }
        },
    )
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    (station,) = await handle.get_stations()
    assert station.vehicle_types_available == expected


async def test_station_coordinates_normalized_and_zone_filter_stays_total(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """Coordinates are normalized before in_circle() ever sees them: a
    numeric string is a usable coordinate, anything else is unknown (and
    an unknown coordinate is excluded when filtering, never a TypeError).
    """
    _mock_catalog(mock_api)
    rows = [
        {"station_id": "stringy", "lat": "34.05", "lon": "-118.25"},
        {"station_id": "junk", "lat": "near the pier", "lon": -118.25},
        {"station_id": "boolean", "lat": True, "lon": -118.25},
    ]
    mock_api.get(
        "/gbfs/station_information.json",
        payload={"ttl": 60, "data": {"stations": rows}},
    )
    mock_api.get(
        "/gbfs/station_status.json", payload={"ttl": 60, "data": {"stations": []}}
    )
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    by_id = {s.id: s for s in await handle.get_stations()}
    assert by_id["stringy"].latitude == 34.05
    assert by_id["junk"].latitude is None
    assert by_id["boolean"].latitude is None
    zoned = await handle.get_stations(
        zone=Circle(latitude=34.05, longitude=-118.25, radius_m=1000)
    )
    assert [s.id for s in zoned] == ["stringy"]


async def test_station_full_status_and_information_surface(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    """Every GBFS station field the documents carry reaches the model.

    is_installed in particular: a station can be renting and returning
    while not being installed at all, so exposing only the first two of
    the triple makes a removed station read as operational.
    """
    _mock_catalog(mock_api)
    mock_api.get("/gbfs/station_information.json", payload=STATION_INFO_23)
    mock_api.get("/gbfs/station_status.json", payload=STATION_STATUS_23)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    by_id = {station.id: station for station in await handle.get_stations()}

    full = by_id["st1"]
    assert full.short_name == "A"
    assert full.bikes_disabled == 2
    assert full.docks_disabled == 1
    assert full.is_installed is True
    assert full.is_virtual_station is False
    assert full.last_reported == datetime(2026, 7, 31, 12, 13, 20, tzinfo=UTC)
    assert full.address == "1 Main St"
    assert full.cross_street == "2nd Ave"
    assert full.post_code == "90001"
    assert full.region_id == "r1"

    # st2 is renting/returning per its status but has been uninstalled.
    removed = by_id["st2"]
    assert removed.is_installed is False
    assert removed.is_returning is True
    assert removed.bikes_disabled is None  # absent, not zero
    assert removed.address is None


async def test_system_info_full_surface(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    mock_api.get("/gbfs/system_information.json", payload=SYSTEM_INFO_23)
    handle = await feeds_client.get_gbfs_feed("gbfs-300")
    info = await handle.get_system_info()
    assert info.short_name == "TB"
    assert info.languages == ["en"]  # 2.x singular "language"
    assert info.url == "https://example.com"
    assert info.purchase_url == "https://example.com/buy"
    assert info.start_date == date(2026, 1, 15)
    assert info.phone_number == "555-0100"
    assert info.email == "hello@example.com"
    assert info.feed_contact_email == "feeds@example.com"
    assert info.license_url == "https://example.com/license"
    assert info.terms_url == "https://example.com/terms"
    assert info.privacy_url == "https://example.com/privacy"
    assert info.opening_hours == "Mo-Su 00:00-24:00"
