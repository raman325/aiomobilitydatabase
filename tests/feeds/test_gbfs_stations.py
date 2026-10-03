"""Tests for GBFS endpoint resolution, ttl caching, system info, stations."""

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
