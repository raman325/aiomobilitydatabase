"""Tests for get_vehicles, get_alerts, and refresh_static."""

import io
import sqlite3
import zipfile
from datetime import UTC, datetime

import pytest

from aiomobilitydatabase.feeds.client import MobilityFeedsClient
from aiomobilitydatabase.feeds.geo import Circle
from aiomobilitydatabase.feeds.models import ServiceAlert

from tests.feeds.fixtures import (
    _FILES,
    ALERTS,
    GTFS_FEED,
    GTFS_RT_FEED,
    TOKEN_RESPONSE,
    VEHICLE_POSITIONS,
    build_gtfs_zip_bytes,
    with_base,
)
from tests.mock_server import MockApi

PB = "application/octet-stream"
ZIP_PATH = "/hosted/mdb-100.zip"


def _mock_catalog(mock_api: MockApi) -> None:
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get(
        "/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds", payload=[with_base(GTFS_RT_FEED, base)]
    )
    mock_api.get(ZIP_PATH, body=build_gtfs_zip_bytes(), content_type="application/zip")


async def test_get_vehicles_resolves_route_names(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    mock_api.get("/rt/all", body=VEHICLE_POSITIONS, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    vehicles = await handle.get_vehicles()
    assert len(vehicles) == 2
    v2 = next(v for v in vehicles if v.vehicle_id == "V2")
    assert v2.route_id == "R2"  # via trip T3 -> R2 static lookup
    assert v2.route_name == "20 Night Owl"


async def test_get_vehicles_skips_rt_feed_without_producer_url(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    base = mock_api.url()
    rt_feed = with_base(GTFS_RT_FEED, base)
    rt_feed["source_info"] = {**rt_feed["source_info"], "producer_url": None}
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get("/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds", payload=[rt_feed])
    mock_api.get(ZIP_PATH, body=build_gtfs_zip_bytes(), content_type="application/zip")
    handle = await feeds_client.get_transit_feed("mdb-100")
    # No producer_url to fetch from: the VP-capable sibling is silently
    # skipped rather than attempted, so no RT request is even made.
    assert await handle.get_vehicles() == []
    assert not any(r.path == "/rt/all" for r in mock_api.requests)


async def test_get_alerts(mock_api: MockApi, feeds_client: MobilityFeedsClient) -> None:
    _mock_catalog(mock_api)
    mock_api.get("/rt/all", body=ALERTS, content_type=PB)
    handle = await feeds_client.get_transit_feed("mdb-100")
    alerts = await handle.get_alerts()
    assert alerts[0].header == "Detour on Main"
    assert alerts[0].route_ids == ["R1"]


async def test_refresh_static_noop_when_dataset_unchanged(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    handle = await feeds_client.get_transit_feed("mdb-100")
    base = mock_api.url()
    mock_api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    assert await handle.refresh_static() is False
    assert len([r for r in mock_api.requests if r.path == ZIP_PATH]) == 1


async def test_refresh_static_rebuilds_on_new_dataset(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    handle = await feeds_client.get_transit_feed("mdb-100")
    base = mock_api.url()
    newer = with_base(GTFS_FEED, base)
    newer["latest_dataset"] = {**newer["latest_dataset"], "id": "mdb-100-202608010000"}
    mock_api.get("/v1/gtfs_feeds/mdb-100", payload=newer)
    mock_api.get(ZIP_PATH, body=build_gtfs_zip_bytes(), content_type="application/zip")
    assert await handle.refresh_static() is True
    assert len([r for r in mock_api.requests if r.path == ZIP_PATH]) == 2
    assert {s.id for s in handle.stops} == {"S1", "S2", "S3", "ST1"}


async def test_stops_in_zone(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    handle = await feeds_client.get_transit_feed("mdb-100")
    # S1 is at (34.05, -118.25); S2 ~1.4 km away; S3 ~2.9 km away.
    nearby = handle.stops_in(Circle(latitude=34.05, longitude=-118.25, radius_m=200.0))
    assert [stop.id for stop in nearby] == ["S1"]
    wider = handle.stops_in(Circle(latitude=34.05, longitude=-118.25, radius_m=2000.0))
    assert {stop.id for stop in wider} == {"S1", "S2"}


def _zip_bytes_with_coordless_entrance() -> bytes:
    """The fixture GTFS zip plus an S4 station-entrance row under ST1 with
    blank lat/lon -- GTFS entrances (location_type=2) legitimately omit
    coordinates, and stops_in must silently exclude them rather than treat
    a missing coordinate as "anywhere".
    """
    files = dict(_FILES)
    files["stops.txt"] = files["stops.txt"] + "S4,Depot Entrance,,,ST1,2\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


async def test_stops_in_excludes_coordless_stop(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    base = mock_api.url()
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get("/v1/gtfs_feeds/mdb-100", payload=with_base(GTFS_FEED, base))
    mock_api.get(
        "/v1/gtfs_feeds/mdb-100/gtfs_rt_feeds", payload=[with_base(GTFS_RT_FEED, base)]
    )
    mock_api.get(
        ZIP_PATH,
        body=_zip_bytes_with_coordless_entrance(),
        content_type="application/zip",
    )
    handle = await feeds_client.get_transit_feed("mdb-100")
    assert {stop.id for stop in handle.stops} == {"S1", "S2", "S3", "ST1", "S4"}
    # A wide zone centered on ST1 would admit S4 on distance alone; it must
    # still be excluded because it carries no coordinates.
    nearby = handle.stops_in(
        Circle(latitude=34.0705, longitude=-118.2295, radius_m=50_000.0)
    )
    assert "S4" not in {stop.id for stop in nearby}


async def test_routes_serving_stop(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    handle = await feeds_client.get_transit_feed("mdb-100")
    routes = await handle.routes_serving("S1")
    # T1/T2/T4 (R1) and T3 (R2) stop at S1
    assert {route.id for route in routes} == {"R1", "R2"}
    routes_s2 = await handle.routes_serving("S2")
    assert {route.id for route in routes_s2} == {"R1"}  # only T1 continues to S2


async def test_headsigns_serving_stop(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    handle = await feeds_client.get_transit_feed("mdb-100")
    # S1 sees R1 trips (headsigns Downtown, Holiday) and R2's T3 (Owl Loop).
    assert await handle.headsigns_serving("S1") == ["Downtown", "Holiday", "Owl Loop"]
    # Narrowed to R1: only its two headsigns.
    assert await handle.headsigns_serving("S1", route_id="R1") == [
        "Downtown",
        "Holiday",
    ]
    assert await handle.headsigns_serving("S2") == ["Downtown"]


async def test_routes_and_headsigns_serving_parent_station(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    handle = await feeds_client.get_transit_feed("mdb-100")
    # ST1 is a parent station (location_type=1); only its child S3 carries
    # stop_times rows, per GTFS station-grouping semantics ST1 itself never
    # appears in stop_times. Both queries key strictly off stop_times.stop_id,
    # so a parent station always resolves to [], never its children's routes.
    assert await handle.routes_serving("ST1") == []
    assert await handle.headsigns_serving("ST1") == []


async def test_close_releases_the_static_index_connection(
    mock_api: MockApi, feeds_client: MobilityFeedsClient
) -> None:
    _mock_catalog(mock_api)
    handle = await feeds_client.get_transit_feed("mdb-100")
    handle.close()
    with pytest.raises(sqlite3.ProgrammingError):
        await handle.routes_serving("S1")


def test_service_alert_is_active() -> None:
    def alert_with(
        periods: list[tuple[datetime | None, datetime | None]],
    ) -> ServiceAlert:
        return ServiceAlert(
            id="a",
            header=None,
            description=None,
            cause=None,
            effect=None,
            severity=None,
            route_ids=[],
            stop_ids=[],
            active_periods=periods,
            url=None,
        )

    now = datetime(2026, 7, 31, 12, 0, tzinfo=UTC)
    assert alert_with([]).is_active(now)  # no periods: always active
    assert alert_with([(None, None)]).is_active(now)
    assert alert_with([(datetime(2026, 7, 1, tzinfo=UTC), None)]).is_active(now)
    assert not alert_with([(datetime(2026, 8, 1, tzinfo=UTC), None)]).is_active(now)
    assert not alert_with([(None, datetime(2026, 7, 1, tzinfo=UTC))]).is_active(now)
    assert alert_with(
        [
            (None, datetime(2026, 7, 1, tzinfo=UTC)),
            (datetime(2026, 7, 30, tzinfo=UTC), None),
        ]
    ).is_active(now)  # second period matches
    # Exact-boundary inclusivity: both endpoints use <=, not strict < / >.
    assert alert_with([(now, None)]).is_active(now)  # start == at
    assert alert_with([(None, now)]).is_active(now)  # at == end
