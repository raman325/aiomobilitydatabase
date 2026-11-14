"""GTFS-Flex queries: which zones and on-demand calls serve a circular zone.

A Flex call serves a place: a zone from locations.geojson, a group of stops,
or a stop within a pickup/drop-off window. ``locations_in`` picks the zones
a circle overlaps; ``get_on_demand_services`` lists the calls whose place
falls in it, with the records they name.
"""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.feeds.geo import Circle, polygon_overlaps_circle
from aiomobilitydatabase.feeds.models import (
    BookingType,
    OnDemandService,
    PickupDropOffType,
)
from aiomobilitydatabase.feeds.static_index import StaticIndex
from aiomobilitydatabase.feeds.transit import TransitFeedHandle

from tests.feeds.fixtures import build_flex_gtfs_zip_bytes
from tests.feeds.test_gtfs_schedule_conformance import _build, _feed
from tests.feeds.test_time_window_properties import _run

# In the fixture: TOWN spans 40.99-41.03 N, 87.05-86.99 W, less a park
# (41.00-41.01 N, 87.02-87.01 W); HOSPITAL groups H1 (41.01, -87.03) and
# H2 (41.01, -87.04).
_AT_H2 = Circle(41.01, -87.04, 10.0)
_IN_THE_PARK = Circle(41.005, -87.015, 10.0)
_FAR = Circle(42.0, -88.0, 100.0)


def _flex_index(tmp_path: Path) -> StaticIndex:
    zip_path = tmp_path / "flex.zip"
    zip_path.write_bytes(build_flex_gtfs_zip_bytes())
    return StaticIndex.build(zip_path, ":memory:", "ds", None)


def _services(
    index: StaticIndex, zone: Circle, route_ids: list[str] | None = None
) -> list[OnDemandService]:
    async def body(handle: TransitFeedHandle) -> list[OnDemandService]:
        return await handle.get_on_demand_services(zone, route_ids=route_ids)

    return _run(index, body)


def _calls(services: list[OnDemandService]) -> list[tuple[str, int]]:
    return [(service.trip_id, service.stop_sequence) for service in services]


def test_a_zone_inside_a_flex_zone_and_at_a_group_stop(tmp_path: Path) -> None:
    """At H2: the TOWN zone it lies in serves DEV1's deviation and ZONE1's
    drop-off; the HOSPITAL group it belongs to serves ZONE1's pickup."""
    services = _services(_flex_index(tmp_path), _AT_H2)
    assert _calls(services) == [("DEV1", 2), ("ZONE1", 1), ("ZONE1", 2)]
    pickup = services[1]
    assert pickup.location is None and pickup.stop is None
    assert pickup.location_group is not None
    assert pickup.location_group.stop_ids == ("H1", "H2")
    assert (pickup.start_pickup_drop_off_window, pickup.end_pickup_drop_off_window) == (
        timedelta(hours=9),
        timedelta(hours=17),
    )
    assert (pickup.pickup_type, pickup.drop_off_type) == (
        PickupDropOffType.PHONE_AGENCY,
        PickupDropOffType.NONE,
    )
    assert pickup.pickup_booking_rule is not None
    assert pickup.pickup_booking_rule.booking_type is BookingType.PRIOR_DAYS
    assert pickup.drop_off_booking_rule is None
    assert (pickup.route_id, pickup.service_id) == ("ZONE", "WEEKDAYS")
    assert pickup.route is not None and pickup.route.long_name == "Dial-a-Ride"
    deviation = services[0]
    assert deviation.location is not None and deviation.location.id == "TOWN"
    assert deviation.headsign == "Depot"
    assert deviation.pickup_booking_rule is not None
    assert deviation.pickup_booking_rule.id == "NOW"


def test_a_zone_in_a_hole_or_far_away_is_served_by_nothing(tmp_path: Path) -> None:
    """The park is cut out of TOWN, and holds no stop: no call serves it."""
    index = _flex_index(tmp_path)
    assert _services(index, _IN_THE_PARK) == []
    assert _services(_flex_index(tmp_path), _FAR) == []


def test_route_ids_narrow_the_calls(tmp_path: Path) -> None:
    assert _calls(_services(_flex_index(tmp_path), _AT_H2, ["ZONE"])) == [
        ("ZONE1", 1),
        ("ZONE1", 2),
    ]
    assert _calls(_services(_flex_index(tmp_path), _AT_H2, [])) == [
        ("DEV1", 2),
        ("ZONE1", 1),
        ("ZONE1", 2),
    ]


def test_locations_in_picks_the_zones_a_circle_overlaps(tmp_path: Path) -> None:
    async def body(handle: TransitFeedHandle) -> list[list[str]]:
        return [
            [location.id for location in handle.locations_in(zone)]
            for zone in (_AT_H2, _IN_THE_PARK, Circle(41.005, -87.015, 600.0), _FAR)
        ]

    assert _run(_flex_index(tmp_path), body) == [["TOWN"], [], ["TOWN"], []]


_WINDOWS = (
    "trip_id,arrival_time,departure_time,stop_id,location_id,"
    "start_pickup_drop_off_window,end_pickup_drop_off_window,stop_sequence,"
    "pickup_type,drop_off_type,pickup_booking_rule_id\n"
)


def test_a_stop_served_within_a_window(tmp_path: Path) -> None:
    """A stop's window call serves a zone the stop lies in, naming the stop;
    a booking rule id booking_rules.txt lacks resolves to no rule."""
    index = _build(
        tmp_path,
        _feed(
            stop_times=_WINDOWS
            + "T,08:00:00,08:00:00,S0,,,,1,,,\n"
            + "T,,,S1,,08:05:00,08:30:00,2,2,2,MISSING\n"
        ),
    )
    [service] = _services(index, Circle(0.0, 0.0, 10.0))
    assert (service.trip_id, service.stop_sequence) == ("T", 2)
    assert service.stop is not None and service.stop.id == "S1"
    assert service.pickup_booking_rule is None
    assert service.start_pickup_drop_off_window == timedelta(hours=8, minutes=5)


def test_repetitions_carry_their_shifted_windows_and_an_unknown_one_is_none(
    tmp_path: Path,
) -> None:
    """Each frequencies.txt repetition is its own call, its window moved
    with it; one moved before its service day starts is None, not open."""
    index = _build(
        tmp_path,
        _feed(
            stop_times=_WINDOWS
            + "T,08:00:00,08:00:00,S0,,,,1,,,\n"
            + "T,,,,Z,07:00:00,09:00:00,2,2,2,\n",
            frequencies="trip_id,start_time,end_time,headway_secs\n"
            "T,00:30:00,01:31:00,3600\n",
        )
        | {
            "locations.geojson": json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "id": "Z",
                            "properties": {},
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [
                                    [[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]]
                                ],
                            },
                        }
                    ],
                }
            )
        },
    )
    services = _services(index, Circle(0.0, 0.0, 10.0))
    assert [
        (s.trip_id, s.start_pickup_drop_off_window, s.end_pickup_drop_off_window)
        for s in services
    ] == [
        # 07:00-09:00 anchored on the 08:00 departure, repeated from 00:30:
        # its start would fall half an hour before the service day.
        ("T#1800", None, timedelta(hours=1, minutes=30)),
        ("T#5400", timedelta(minutes=30), timedelta(hours=2, minutes=30)),
    ]


@given(
    lat=st.floats(40.98, 41.04),
    lon=st.floats(-87.06, -86.98),
    radius=st.floats(0, 3000),
)
@settings(deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_every_call_serving_a_zone_is_listed_and_nothing_else(
    tmp_path: Path, lat: float, lon: float, radius: float
) -> None:
    """Two-sided against the records: a call is listed exactly when its zone
    overlaps the circle, its group has a stop in it, or its window stop is
    in it."""
    zone = Circle(lat, lon, radius)
    index = _flex_index(tmp_path)

    async def body(
        handle: TransitFeedHandle,
    ) -> tuple[set[tuple[str, int]], set[tuple[str, int]]]:
        near = {stop.id for stop in handle.stops_in(zone)}
        zones = {
            location.id
            for location in handle.flex_locations
            if any(polygon_overlaps_circle(part, zone) for part in location.polygons)
        }
        groups = {
            group.id
            for group in handle.location_groups
            if near.intersection(group.stop_ids)
        }
        rows = handle._index._conn.execute(
            "SELECT trip_id, stop_sequence, location_id, location_group_id, "
            "stop_id FROM flex_stop_times"
        )
        expected = {
            (trip_id, sequence)
            for trip_id, sequence, location_id, group_id, stop_id in rows
            if location_id in zones or group_id in groups or stop_id in near
        }
        listed = await handle.get_on_demand_services(zone)
        return {(s.trip_id, s.stop_sequence) for s in listed}, expected

    listed, expected = _run(index, body)
    assert listed == expected
