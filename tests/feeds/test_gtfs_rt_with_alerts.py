"""Service alerts attached to boards and journeys (``with_alerts``)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest
from google.transit import gtfs_realtime_pb2
from hypothesis import given, settings
from hypothesis import strategies as st

from aiomobilitydatabase import EntityType
from aiomobilitydatabase.feeds.models import ArrivalsQuery, TripQuery
from aiomobilitydatabase.feeds.transit import TransitFeedHandle

from tests.feeds.test_gtfs_rt_conformance import _EIGHT, _descriptor_message
from tests.feeds.test_gtfs_schedule_conformance import _feed
from tests.feeds.test_properties import _build_index_from_files
from tests.feeds.test_time_window_properties import _run

_FEED = _feed(
    stops=(
        "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
        "ST,Station,0,0,1,\nS0,A,0,0,0,ST\nS1,B,0,0,0,\n"
    ),
    trips=(
        "route_id,service_id,trip_id,trip_headsign,direction_id\nR,S,T,Downtown,0\n"
    ),
)


def _alerts(*alerts: tuple[str, list[dict[str, Any]]]) -> gtfs_realtime_pb2.FeedMessage:
    """One alert per (id, selectors); a selector's ``trip`` is a dict."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    for alert_id, selectors in alerts:
        entity = message.entity.add()
        entity.id = alert_id
        entity.alert.SetInParent()
        for selector in selectors:
            informed = entity.alert.informed_entity.add()
            for name, value in selector.items():
                if name == "trip":
                    informed.trip.SetInParent()
                    for trip_field, trip_value in value.items():
                        setattr(informed.trip, trip_field, trip_value)
                else:
                    setattr(informed, name, value)
    return message


def _run_with[T](
    files: dict[str, str],
    body: Callable[[TransitFeedHandle], Awaitable[T]],
    *,
    alerts: Sequence[gtfs_realtime_pb2.FeedMessage],
    trip_updates: Sequence[gtfs_realtime_pb2.FeedMessage] = (),
) -> T:
    index = _build_index_from_files(files)

    async def scenario(handle: TransitFeedHandle) -> T:
        async def fetch(
            entity_type: EntityType, *_args: object
        ) -> list[gtfs_realtime_pb2.FeedMessage]:
            if entity_type is EntityType.TRIP_UPDATES:
                return list(trip_updates)
            if entity_type is EntityType.SERVICE_ALERTS:
                return list(alerts)
            return []

        handle._fetch_entity_messages = fetch  # type: ignore[method-assign,assignment]
        return await body(handle)

    return _run(index, scenario)


def _attached(
    *, with_alerts: bool = True
) -> Callable[[TransitFeedHandle], Awaitable[tuple[dict[Any, Any], dict[Any, Any]]]]:
    """Each board row's and journey's alert ids, keyed by (trip, stop) and
    (trip, departure)."""

    async def body(handle: TransitFeedHandle) -> tuple[dict[Any, Any], dict[Any, Any]]:
        start = _EIGHT - timedelta(minutes=30)
        [board] = await handle.get_arrivals(
            [ArrivalsQuery(["S0", "S1"], limit=None)],
            start=start,
            end=start + timedelta(hours=2),
            with_alerts=with_alerts,
        )
        [journeys] = await handle.get_upcoming_trips(
            [TripQuery("S0", "S1", limit=None)],
            start=start,
            end=start + timedelta(hours=2),
            with_alerts=with_alerts,
        )
        return (
            {
                (row.trip_id, row.stop_id): {alert.id for alert in row.alerts}
                for row in board
            },
            {
                (row.trip_id, row.scheduled_departure): {
                    alert.id for alert in row.alerts
                }
                for row in journeys
            },
        )

    return body


_SCOPE_LIST: list[tuple[str, list[dict[str, Any]]]] = [
    ("route", [{"route_id": "R"}]),
    ("route_type", [{"route_type": 3}]),
    ("agency", [{"agency_id": "A"}]),  # R names none: the feed's only agency
    ("other_route", [{"route_id": "R2"}]),
    ("route_at_s0", [{"route_id": "R", "stop_id": "S0"}]),
    ("stop_s1", [{"stop_id": "S1"}]),
    ("station", [{"stop_id": "ST"}]),  # S0's parent station
    ("direction", [{"route_id": "R", "direction_id": 0}]),
    ("other_direction", [{"route_id": "R", "direction_id": 1}]),
    ("trip", [{"trip": {"trip_id": "T"}}]),
    ("trip_today", [{"trip": {"trip_id": "T", "start_date": "20260730"}}]),
    ("trip_tomorrow", [{"trip": {"trip_id": "T", "start_date": "20260731"}}]),
    # A plain trip's start time is its first departure.
    ("trip_at_eight", [{"trip": {"trip_id": "T", "start_time": "08:00:00"}}]),
    ("trip_at_nine", [{"trip": {"trip_id": "T", "start_time": "09:00:00"}}]),
    (
        "run",
        [
            {
                "trip": {
                    "route_id": "R",
                    "direction_id": 0,
                    "start_time": "08:00:00",
                    "start_date": "20260730",
                }
            }
        ],
    ),
]
_SCOPES = _alerts(*_SCOPE_LIST)
_EVERYWHERE = {
    "route",
    "route_type",
    "agency",
    "direction",
    "trip",
    "trip_today",
    "trip_at_eight",
    "run",
}


def test_each_row_carries_the_alerts_whose_selectors_match_its_run() -> None:
    """EntitySelector: "it must match all of the provided EntitySelector
    fields". The library matches each row's run, route, agency, direction
    and stop; a station's alert reaches its platform S0."""
    board, journeys = _run_with(_FEED, _attached(), alerts=[_SCOPES])
    assert board == {
        ("T", "S0"): _EVERYWHERE | {"route_at_s0", "station"},
        ("T", "S1"): _EVERYWHERE | {"stop_s1"},
    }
    # A journey: at its origin or its destination.
    assert journeys == {
        ("T", _EIGHT): _EVERYWHERE | {"route_at_s0", "station", "stop_s1"},
    }


@pytest.mark.parametrize(
    "scope", _SCOPE_LIST, ids=[alert_id for alert_id, _ in _SCOPE_LIST]
)
def test_each_alert_alone_lands_on_the_same_rows(
    scope: tuple[str, list[dict[str, Any]]],
) -> None:
    """Sent alone, each alert lands exactly where it does among the others:
    no selector leans on another's lookups."""
    together, _ = _run_with(_FEED, _attached(), alerts=[_SCOPES])
    alone, _ = _run_with(_FEED, _attached(), alerts=[_alerts(scope)])
    alert_id = scope[0]
    assert {key for key, ids in alone.items() if ids} == {
        key for key, ids in together.items() if alert_id in ids
    }


def test_a_trip_run_names_only_a_unique_plain_trip() -> None:
    """A trip without trip_id resolves as a TripUpdate's does: to the one
    trip that is not frequency based leaving at that start on that day,
    else to none."""
    run = {
        "trip": {
            "route_id": "R",
            "direction_id": 0,
            "start_time": "08:00:00",
            "start_date": "20260730",
        }
    }
    frequent = {
        **_FEED,
        "frequencies.txt": (
            "trip_id,start_time,end_time,headway_secs,exact_times\n"
            "T,08:00:00,08:30:00,600,1\n"
        ),
    }
    twin = {
        **_FEED,
        "trips.txt": (
            "route_id,service_id,trip_id,trip_headsign,direction_id\n"
            "R,S,T,Downtown,0\nR,S,U,Downtown,0\n"
        ),
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "T,08:00:00,08:00:00,S0,1\nT,08:10:00,08:10:00,S1,2\n"
            "U,08:00:00,08:00:00,S0,1\nU,08:10:00,08:10:00,S1,2\n"
        ),
    }
    message = _alerts(("run", [run]))
    for files in (frequent, twin):
        board, journeys = _run_with(files, _attached(), alerts=[message])
        assert board
        assert all(not ids for ids in board.values())
        assert all(not ids for ids in journeys.values())


def test_a_stop_realtime_defines_takes_its_parent_stations_alerts() -> None:
    """A detour's replacement stop may exist only in realtime; its own
    parent_station is the station whose alerts reach it."""
    updates = _descriptor_message({"trip_id": "T", "start_date": "20260730"})
    stop = updates.entity.add()
    stop.id = "pop"
    stop.stop.stop_id = "POP"
    stop.stop.parent_station = "ST"
    detour = updates.entity.add()
    detour.id = "detour"
    modification = detour.trip_modifications.modifications.add()
    modification.start_stop_selector.stop_sequence = 2
    modification.end_stop_selector.stop_sequence = 2
    replacement = modification.replacement_stops.add()
    replacement.stop_id = "POP"
    replacement.travel_time_to_stop = 600
    detour.trip_modifications.selected_trips.add().trip_ids.append("T")
    detour.trip_modifications.service_dates.append("20260730")

    async def body(handle: TransitFeedHandle) -> dict[str, set[str]]:
        start = _EIGHT - timedelta(minutes=30)
        [board] = await handle.get_arrivals(
            [ArrivalsQuery(["S0", "POP"], limit=None)],
            start=start,
            end=start + timedelta(hours=2),
            with_alerts=True,
        )
        return {row.stop_id: {alert.id for alert in row.alerts} for row in board}

    message = _alerts(("station", [{"stop_id": "ST"}]))
    assert _run_with(_FEED, body, alerts=[message], trip_updates=[updates]) == {
        "S0": {"station"},
        "POP": {"station"},
    }


def test_rows_carry_no_alerts_unless_asked() -> None:
    board, journeys = _run_with(_FEED, _attached(with_alerts=False), alerts=[_SCOPES])
    assert board and journeys
    assert all(not alerts for alerts in board.values())
    assert all(not alerts for alerts in journeys.values())


def test_a_repetition_carries_the_alerts_naming_its_start() -> None:
    """A frequency-based trip's start_time names one repetition: only that
    run's rows carry it."""
    files = {
        **_FEED,
        "frequencies.txt": (
            "trip_id,start_time,end_time,headway_secs,exact_times\n"
            "T,08:00:00,08:30:00,600,1\n"
        ),
    }
    message = _alerts(
        ("ten_past", [{"trip": {"trip_id": "T", "start_time": "08:10:00"}}])
    )
    board, _ = _run_with(files, _attached(), alerts=[message])
    assert {key for key, alerts in board.items() if alerts} == {
        ("T#29400", "S0"),
        ("T#29400", "S1"),
    }


def test_realtime_trips_carry_the_alerts_of_the_run_they_are() -> None:
    """A REPLACEMENT is the run it replaces, so that run's alerts apply; a
    NEW trip is its own."""
    descriptor = gtfs_realtime_pb2.TripDescriptor
    updates = _descriptor_message(
        {
            "trip_id": "T",
            "start_date": "20260730",
            "relationship": descriptor.REPLACEMENT,
            "calls": [
                ("S0", _EIGHT + timedelta(minutes=5), None, False),
                ("S1", _EIGHT + timedelta(minutes=15), None, False),
            ],
        },
        {
            "trip_id": "N",
            "start_date": "20260730",
            "route_id": "R",
            "relationship": descriptor.NEW,
            "calls": [
                ("S0", _EIGHT + timedelta(minutes=20), None, False),
                ("S1", _EIGHT + timedelta(minutes=30), None, False),
            ],
        },
    )
    message = _alerts(
        ("on_t", [{"trip": {"trip_id": "T", "start_date": "20260730"}}]),
        ("on_n", [{"trip": {"trip_id": "N"}}]),
    )
    board, journeys = _run_with(
        _FEED, _attached(), alerts=[message], trip_updates=[updates]
    )
    assert board == {
        ("T", "S0"): {"on_t"},
        ("T", "S1"): {"on_t"},
        ("N", "S0"): {"on_n"},
        ("N", "S1"): {"on_n"},
    }
    assert sorted(journeys.values(), key=sorted) == [{"on_n"}, {"on_t"}]


def test_a_headway_based_run_carries_the_alerts_naming_its_own_start() -> None:
    """An exact_times=0 run is its trip at whatever start realtime gives
    ("may be arbitrary"): an alert naming that start is that run's, shown
    under its own id, and no idealized repetition's."""
    files = {
        **_FEED,
        "frequencies.txt": (
            "trip_id,start_time,end_time,headway_secs,exact_times\n"
            "T,08:00:00,08:30:00,600,0\n"
        ),
    }
    updates = _descriptor_message(
        {
            "trip_id": "T",
            "start_date": "20260730",
            "start_time": "08:03:00",
            "relationship": gtfs_realtime_pb2.TripDescriptor.UNSCHEDULED,
        }
    )
    message = _alerts(
        ("three_past", [{"trip": {"trip_id": "T", "start_time": "08:03:00"}}])
    )
    board, journeys = _run_with(
        files, _attached(), alerts=[message], trip_updates=[updates]
    )
    assert {key for key, alerts in board.items() if alerts} == {
        ("T#28980", "S0"),
        ("T#28980", "S1"),
    }
    assert [alerts for alerts in journeys.values() if alerts] == [{"three_past"}]


_SELECTOR_FIELDS = {
    "agency_id": st.sampled_from(["A", "B"]),
    "route_id": st.sampled_from(["R", "R2"]),
    "route_type": st.sampled_from([0, 3]),
    "stop_id": st.sampled_from(["ST", "S0", "S1"]),
}


@st.composite
def _selector(draw: st.DrawFn) -> dict[str, Any]:
    selector = {
        name: draw(strategy)
        for name, strategy in _SELECTOR_FIELDS.items()
        if draw(st.booleans())
    }
    if draw(st.booleans()):
        selector["trip"] = {
            "route_id": "R",
            "direction_id": 0,
            "start_time": draw(st.sampled_from(["08:00:00", "08:10:00"])),
            "start_date": "20260730",
        }
    elif draw(st.booleans()):
        selector["trip"] = {"trip_id": draw(st.sampled_from(["T", "N"]))}
        if draw(st.booleans()):
            selector["trip"]["start_time"] = draw(
                st.sampled_from(["08:00:00", "08:10:00"])
            )
    return selector


@settings(max_examples=40, deadline=None)
@given(
    selectors=st.lists(st.lists(_selector(), min_size=1, max_size=2), max_size=4),
    frequent=st.booleans(),
    canceled=st.booleans(),
)
def test_with_alerts_only_adds_alerts(
    selectors: list[list[dict[str, Any]]], frequent: bool, canceled: bool
) -> None:
    """Asking for alerts changes no row and no other field: the same rows
    come back, each with the alerts that apply to it, in feed order -- and
    every row of route R carries the alert on route R."""
    files = dict(_FEED)
    if frequent:
        files["frequencies.txt"] = (
            "trip_id,start_time,end_time,headway_secs,exact_times\n"
            "T,08:00:00,08:30:00,600,1\n"
        )
    message = _alerts(
        *((f"a{n}", chosen) for n, chosen in enumerate(selectors)),
        ("on_r", [{"route_id": "R"}]),
    )
    updates = _descriptor_message(
        {
            "trip_id": "T",
            "start_date": "20260730",
            **({"start_time": "08:00:00"} if frequent else {}),
            **(
                {"relationship": gtfs_realtime_pb2.TripDescriptor.CANCELED}
                if canceled
                else {"delay": 60}
            ),
        }
    )

    async def body(handle: TransitFeedHandle) -> list[list[Any]]:
        start = _EIGHT - timedelta(minutes=30)
        results: list[list[Any]] = []
        for with_alerts in (False, True):
            [board] = await handle.get_arrivals(
                [ArrivalsQuery(["S0", "S1"], limit=None)],
                start=start,
                end=start + timedelta(hours=2),
                with_alerts=with_alerts,
            )
            [journeys] = await handle.get_upcoming_trips(
                [TripQuery("S0", "S1", limit=None)],
                start=start,
                end=start + timedelta(hours=2),
                with_alerts=with_alerts,
            )
            results.append([*board, *journeys])
        return results

    plain, alerted = _run_with(files, body, alerts=[message], trip_updates=[updates])
    assert [replace(row, alerts=()) for row in alerted] == plain
    order = [*(f"a{n}" for n in range(len(selectors))), "on_r"]
    assert alerted
    for row in alerted:
        assert "on_r" in [alert.id for alert in row.alerts]
        ids = [alert.id for alert in row.alerts]
        assert ids == [alert_id for alert_id in order if alert_id in ids]
