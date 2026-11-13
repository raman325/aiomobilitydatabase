"""GTFS-RT TripUpdates conformance: how StopTimeUpdates resolve.

Each test quotes the rule it pins. The resolver's state machine is
covered piecewise in ``test_rt`` and by the propagation oracles in
``test_properties``; these are the end-to-end consequences and the laws
the spec states outright.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any

from google.transit import gtfs_realtime_pb2
from hypothesis import given, settings
from hypothesis import strategies as st

from aiomobilitydatabase import EntityType
from aiomobilitydatabase.feeds.models import ArrivalsQuery, StopArrival, TripQuery
from aiomobilitydatabase.feeds.rt import (
    TripStopUpdate,
    TripUpdateEntry,
    resolve_trip_predictions,
    trip_updates_from_message,
)
from aiomobilitydatabase.feeds.static_index import TripCall
from aiomobilitydatabase.feeds.transit import TransitFeedHandle

from tests.feeds.test_gtfs_schedule_conformance import _feed
from tests.feeds.test_properties import _build_index_from_files
from tests.feeds.test_time_window_properties import _run

_EIGHT = datetime(2026, 7, 30, 8, tzinfo=UTC)
_STU = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate
_THREE_STOPS = _feed(
    stops="stop_id,stop_name,stop_lat,stop_lon\nS0,A,0,0\nS1,B,0,0\nS2,C,0,0\n",
    stop_times=(
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        "T,08:00:00,08:00:00,S0,1\nT,08:10:00,08:10:00,S1,2\n"
        "T,08:20:00,08:20:00,S2,3\n"
    ),
)


def _message(*updates: dict[str, object]) -> gtfs_realtime_pb2.FeedMessage:
    """One TripUpdate for T on 2026-07-30, one StopTimeUpdate per dict."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "T"
    entity.trip_update.trip.trip_id = "T"
    entity.trip_update.trip.start_date = "20260730"
    for update in updates:
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_sequence = update["sequence"]  # type: ignore[assignment]
        for event in ("arrival", "departure"):
            for part in ("time", "delay"):
                value = update.get(f"{event}_{part}")
                if value is not None:
                    setattr(getattr(stu, event), part, value)
    return message


def _predicted(message: gtfs_realtime_pb2.FeedMessage) -> dict[str, tuple[object, ...]]:
    index = _build_index_from_files(_THREE_STOPS)

    async def body(handle: TransitFeedHandle) -> dict[str, tuple[object, ...]]:
        [board] = await handle.get_arrivals(
            [ArrivalsQuery(["S0", "S1", "S2"])],
            start=_EIGHT - timedelta(minutes=30),
            end=_EIGHT + timedelta(hours=1),
        )
        return {
            row.stop_id: (row.predicted_arrival, row.predicted_departure)
            for row in board
        }

    return _run(index, body, message)


def test_a_time_only_update_propagates_its_computed_delay() -> None:
    """ "If only time is provided in the update, a delay computed by
    comparing the time against the GTFS schedule time" is propagated to
    all subsequent stops: S0 leaving at 08:05 makes S1 and S2 five minutes
    late, not on time."""
    five = timedelta(minutes=5)
    departs = int((_EIGHT + five).timestamp())
    assert _predicted(_message({"sequence": 1, "departure_time": departs})) == {
        "S0": (None, _EIGHT + five),  # the update gives S0's departure only
        "S1": (_EIGHT + timedelta(minutes=10) + five,) * 2,
        "S2": (_EIGHT + timedelta(minutes=20) + five,) * 2,
    }


def test_each_event_keeps_its_own_delay() -> None:
    """A StopTimeEvent is "timing information for a single predicted event
    (either arrival or departure)": a 60 s arrival delay and a 120 s
    departure delay at S0 predict each by its own, and the departure's goes
    on."""
    predicted = _predicted(
        _message({"sequence": 1, "arrival_delay": 60, "departure_delay": 120})
    )
    assert predicted["S0"] == (
        _EIGHT + timedelta(seconds=60),
        _EIGHT + timedelta(seconds=120),
    )
    assert predicted["S1"] == (_EIGHT + timedelta(minutes=10, seconds=120),) * 2


def test_each_run_resolves_against_its_own_days_schedule() -> None:
    """A window of two days holds two runs of T. A dated time-only update
    for each -- 08:05 on the first day, 08:02 on the second -- is that
    day's run 5 or 2 minutes late, never a day off."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    for day, minutes in ((30, 5), (31, 2)):
        entity = message.entity.add()
        entity.id = f"T-{day}"
        entity.trip_update.trip.trip_id = "T"
        entity.trip_update.trip.start_date = f"202607{day}"
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_sequence = 1
        stu.departure.time = int(
            datetime(2026, 7, day, 8, minutes, tzinfo=UTC).timestamp()
        )
    index = _build_index_from_files(_THREE_STOPS)

    async def body(handle: TransitFeedHandle) -> list[tuple[object, ...]]:
        [board] = await handle.get_arrivals(
            [ArrivalsQuery(["S2"])],
            start=_EIGHT - timedelta(hours=1),
            end=_EIGHT + timedelta(days=1, hours=1),
        )
        return [(row.scheduled_departure, row.delay_seconds) for row in board]

    assert _run(index, body, message) == [
        (_EIGHT + timedelta(minutes=20), 300),
        (_EIGHT + timedelta(days=1, minutes=20), 120),
    ]


def test_a_journey_from_a_late_origin_given_arrivals_only_is_kept() -> None:
    """A feed sending only arrivals: S0 and S2 arrive 10 min late. At 08:01
    the bus has not reached S0, so the journey S0 -> S2 is still ahead --
    its departure is not predicted, but it is ordered and kept by S0's
    predicted arrival, 08:10, exactly as the board keeps S0's row."""
    message = _message(
        {"sequence": 1, "arrival_delay": 600}, {"sequence": 3, "arrival_delay": 600}
    )
    index = _build_index_from_files(_THREE_STOPS)
    now = _EIGHT + timedelta(minutes=1)

    async def body(handle: TransitFeedHandle) -> list[tuple[object, ...]]:
        [trips] = await handle.get_upcoming_trips(
            [TripQuery("S0", "S2")], start=now, end=now + timedelta(hours=1)
        )
        return [
            (
                trip.predicted_departure,
                trip.origin_predicted_arrival,
                trip.predicted_arrival,
                trip.delay_seconds,
            )
            for trip in trips
        ]

    assert _run(index, body, message) == [
        (None, _EIGHT + timedelta(minutes=10), _EIGHT + timedelta(minutes=30), 600)
    ]


_TWO_TRIPS = _feed(
    trips="route_id,service_id,trip_id,trip_headsign\nR,S,T,Downtown\nR,S,U,Downtown\n",
    stop_times=(
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        "T,08:00:00,08:00:00,S0,1\nT,08:10:00,08:10:00,S1,2\n"
        "U,08:05:00,08:05:00,S0,1\nU,08:15:00,08:15:00,S1,2\n"
        "V,10:20:00,10:20:00,S0,1\nV,10:30:00,10:30:00,S1,2\n"
    ),
)
_TWO_TRIPS["trips.txt"] += "R,S,V,Downtown\n"


def _arrivals_only(
    trip: str, sequence: int, at: datetime
) -> gtfs_realtime_pb2.FeedMessage:
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = trip
    entity.trip_update.trip.trip_id = trip
    entity.trip_update.trip.start_date = "20260730"
    stu = entity.trip_update.stop_time_update.add()
    stu.stop_sequence = sequence
    stu.arrival.time = int(at.timestamp())
    return message


def _ordered(
    message: gtfs_realtime_pb2.FeedMessage,
    start: datetime,
    *,
    limit: int | None = None,
    end: datetime | None = None,
) -> tuple[list[object], list[object]]:
    index = _build_index_from_files(_TWO_TRIPS)

    async def body(handle: TransitFeedHandle) -> tuple[list[object], list[object]]:
        [board] = await handle.get_arrivals(
            [ArrivalsQuery(["S0"], limit=limit)], start=start, end=end
        )
        [trips] = await handle.get_upcoming_trips(
            [TripQuery("S0", "S1", limit=limit)], start=start, end=end
        )
        return [row.trip_id for row in board], [trip.trip_id for trip in trips]

    return _run(index, body, message)


def test_boards_and_journeys_order_by_when_the_vehicle_leaves() -> None:
    """T is due 08:00 but its update says it reaches S0 only at 08:10; U is
    due 08:05 with no realtime. U leaves first: both lists put it ahead."""
    message = _arrivals_only("T", 1, _EIGHT + timedelta(minutes=10))
    assert _ordered(
        message, _EIGHT - timedelta(minutes=30), end=_EIGHT + timedelta(minutes=30)
    ) == (["U", "T"], ["U", "T"])


def test_a_late_row_does_not_settle_the_open_ended_scan() -> None:
    """Open-ended, limit 1, from 08:06: T (due 08:00) reaches S0 only at
    10:30, past the first two-hour slice read; V, due 10:20, is unread
    then but leaves first -- the board is not settled by T."""
    message = _arrivals_only("T", 1, _EIGHT + timedelta(hours=2, minutes=30))
    assert _ordered(message, _EIGHT - timedelta(minutes=10), limit=1) == (["U"], ["U"])
    late = _ordered(message, _EIGHT + timedelta(minutes=6), limit=1)
    assert late == (["V"], ["V"])


def test_an_early_arrival_leaves_at_the_scheduled_departure() -> None:
    """T's update says it reaches S0 two minutes early, and nothing about
    leaving: at 07:59 it is still to leave at its scheduled 08:00."""
    message = _arrivals_only("T", 1, _EIGHT - timedelta(minutes=2))
    board, trips = _ordered(
        message, _EIGHT - timedelta(minutes=1), end=_EIGHT + timedelta(minutes=1)
    )
    assert board == trips == ["T"]


def test_an_assigned_stop_names_its_call_end_to_end() -> None:
    """A platform reassignment as the spec sends it: stop_sequence 2 with a
    stop_id that "must match assigned_stop_id". Its 600 s reaches S2."""
    message = _message({"sequence": 2, "departure_delay": 600})
    stu = message.entity[0].trip_update.stop_time_update[0]
    stu.stop_id = "S1B"
    stu.stop_time_properties.assigned_stop_id = "S1B"
    assert _predicted(message)["S2"] == (_EIGHT + timedelta(minutes=30),) * 2


# -- laws over drawn StopTimeUpdates -----------------------------------------


def _calls(n: int, stop_ids: list[str] | None = None) -> list[TripCall]:
    stop_ids = stop_ids or [f"S{i}" for i in range(n)]
    return [
        TripCall(
            stop_sequence=10 * (i + 1),
            stop_id=stop_ids[i],
            arrival=_EIGHT + timedelta(minutes=10 * i),
            departure=_EIGHT + timedelta(minutes=10 * i, seconds=30),
            pickup_type=None,
            drop_off_type=None,
            timepoint_exact=None,
            stop_headsign=None,
        )
        for i in range(n)
    ]


_OFFSET = st.none() | st.integers(-600, 1800)


@st.composite
def _updates(draw: st.DrawFn, n: int) -> list[tuple[int, dict[str, object]]]:
    """Drawn content for a subset of ``n`` positions, in trip order."""
    out: list[tuple[int, dict[str, object]]] = []
    for position in sorted(draw(st.sets(st.integers(0, n - 1), max_size=n))):
        out.append(
            (
                position,
                {
                    "relationship": draw(
                        st.sampled_from([_STU.SCHEDULED, _STU.SKIPPED, _STU.NO_DATA])
                    ),
                    "arrival_delay": draw(_OFFSET),
                    "departure_delay": draw(_OFFSET),
                    "arrival_offset": draw(_OFFSET),
                    "departure_offset": draw(_OFFSET),
                },
            )
        )
    return out


def _entry(
    calls: list[TripCall],
    updates: list[tuple[int, dict[str, object]]],
    *,
    by: str,
    trip_delay: int | None = None,
) -> TripUpdateEntry:
    def at(base: datetime | None, offset: object) -> datetime | None:
        if base is None or offset is None:
            return None
        return base + timedelta(seconds=offset)  # type: ignore[arg-type]

    return TripUpdateEntry(
        stop_updates=tuple(
            TripStopUpdate(
                stop_id=calls[position].stop_id if by == "stop_id" else None,
                stop_sequence=(
                    calls[position].stop_sequence if by == "stop_sequence" else None
                ),
                relationship=content["relationship"],  # type: ignore[arg-type]
                arrival=at(calls[position].arrival, content["arrival_offset"]),
                departure=at(calls[position].departure, content["departure_offset"]),
                arrival_delay=content["arrival_delay"],  # type: ignore[arg-type]
                departure_delay=content["departure_delay"],  # type: ignore[arg-type]
            )
            for position, content in updates
        ),
        delay_seconds=trip_delay,
        vehicle_id=None,
    )


@settings(max_examples=200, deadline=None)
@given(data=st.data(), n=st.integers(2, 6), trip_delay=_OFFSET)
def test_stop_id_and_stop_sequence_address_alike_on_a_simple_trip(
    data: st.DataObject, n: int, trip_delay: int | None
) -> None:
    """A StopTimeUpdate is "linked to a specific stop either through
    stop_sequence or stop_id": on a trip visiting each stop once, the two
    forms resolve identically."""
    calls = _calls(n)
    updates = data.draw(_updates(n))
    assert resolve_trip_predictions(
        _entry(calls, updates, by="stop_id", trip_delay=trip_delay), calls
    ) == resolve_trip_predictions(
        _entry(calls, updates, by="stop_sequence", trip_delay=trip_delay), calls
    )


@settings(max_examples=200, deadline=None)
@given(data=st.data(), n=st.integers(2, 6), trip_delay=_OFFSET)
def test_a_no_data_updates_own_content_never_surfaces(
    data: st.DataObject, n: int, trip_delay: int | None
) -> None:
    """ "When NO_DATA is set, arrival or departure must not be supplied":
    whatever times and delays a NO_DATA update carries anyway change
    nothing."""
    calls = _calls(n)
    updates = data.draw(_updates(n))
    silent = [
        (
            position,
            {
                **content,
                "arrival_delay": None,
                "departure_delay": None,
                "arrival_offset": None,
                "departure_offset": None,
            }
            if content["relationship"] == _STU.NO_DATA
            else content,
        )
        for position, content in updates
    ]
    assert resolve_trip_predictions(
        _entry(calls, updates, by="stop_sequence", trip_delay=trip_delay), calls
    ) == resolve_trip_predictions(
        _entry(calls, silent, by="stop_sequence", trip_delay=trip_delay), calls
    )


@settings(max_examples=100, deadline=None)
@given(
    relationship=st.sampled_from([None, gtfs_realtime_pb2.TripDescriptor.UNSCHEDULED]),
    delay=st.integers(-600, 1800),
    sequence=st.integers(1, 3),
)
def test_unscheduled_and_unset_trips_read_as_scheduled(
    relationship: int | None, delay: int, sequence: int
) -> None:
    """The trip-level schedule_relationship defaults to SCHEDULED, and
    UNSCHEDULED (frequency trips not run to a schedule) is matched the same
    way: neither changes what the update means."""
    scheduled = _message({"sequence": sequence, "departure_delay": delay})
    scheduled.entity[
        0
    ].trip_update.trip.schedule_relationship = (
        gtfs_realtime_pb2.TripDescriptor.SCHEDULED
    )
    other = _message({"sequence": sequence, "departure_delay": delay})
    if relationship is not None:
        other.entity[0].trip_update.trip.schedule_relationship = relationship
    assert trip_updates_from_message(other) == trip_updates_from_message(scheduled)


# --- Which run a TripDescriptor names, and what it says about it -----------

_DIRECTED = _feed(
    stops="stop_id,stop_name,stop_lat,stop_lon\nS0,A,0,0\nS1,B,0,0\nS2,C,0,0\n",
    trips=(
        "route_id,service_id,trip_id,trip_headsign,direction_id\n"
        "R,S,T,Out,0\nR,S,U,Back,1\nR,S,X,Out,0\n"
    ),
    stop_times=(
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        "T,08:00:00,08:00:00,S0,1\nT,08:10:00,08:10:00,S1,2\n"
        "U,08:00:00,08:00:00,S1,1\nU,08:10:00,08:10:00,S2,2\n"
        "X,09:00:00,09:00:00,S0,1\nX,09:10:00,09:10:00,S1,2\n"
    ),
)


def _add_stop(stu: Any, stop: dict[str, Any]) -> None:
    """Fill a StopTimeUpdate: ``stop``, each event's ``time`` (``arrival``,
    ``departure``), ``scheduled_*`` and ``*_delay``, ``no_data``/``skipped``."""
    stu.stop_id = stop["stop"]
    if stop.get("no_data"):
        stu.schedule_relationship = _STU.NO_DATA
    if stop.get("skipped"):
        stu.schedule_relationship = _STU.SKIPPED
    for event in ("arrival", "departure"):
        holder = getattr(stu, event)
        for part, value in (
            ("time", stop.get(event)),
            ("scheduled_time", stop.get(f"scheduled_{event}")),
        ):
            if value is not None:
                setattr(holder, part, int(value.timestamp()))
        if stop.get(f"{event}_delay") is not None:
            holder.delay = stop[f"{event}_delay"]


def _descriptor_message(
    *descriptors: dict[str, object], timestamp: datetime | None = None
) -> gtfs_realtime_pb2.FeedMessage:
    """One TripUpdate per dict: TripDescriptor fields, plus ``delay`` (an
    S0 departure delay) or a ``relationship``."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    if timestamp is not None:
        message.header.timestamp = int(timestamp.timestamp())
    for n, fields in enumerate(descriptors):
        entity = message.entity.add()
        entity.id = f"e{n}"
        trip = entity.trip_update.trip
        for name in ("trip_id", "route_id", "direction_id", "start_time", "start_date"):
            if name in fields:
                setattr(trip, name, fields[name])
        if "relationship" in fields:
            trip.schedule_relationship = fields["relationship"]
        if "delay" in fields:
            stu = entity.trip_update.stop_time_update.add()
            stu.stop_id = "S0"
            stu.departure.delay = fields["delay"]
        if "headsign" in fields:
            entity.trip_update.trip_properties.trip_headsign = fields["headsign"]
        if "copy" in fields:
            properties = entity.trip_update.trip_properties
            copy_id, copy_date, copy_time = fields["copy"]
            for name, value in (
                ("trip_id", copy_id),
                ("start_date", copy_date),
                ("start_time", copy_time),
            ):
                if value:
                    setattr(properties, name, value)
        stops = [
            *fields.get("stops", ()),
            *(
                {"stop": stop, "arrival": a, "departure": d, "skipped": skipped}
                for stop, a, d, skipped in fields.get("calls", ())
            ),
        ]
        for stop in stops:
            _add_stop(entity.trip_update.stop_time_update.add(), stop)
    return message


def _run_feeds[T](
    files: dict[str, str],
    body: Callable[[TransitFeedHandle], Awaitable[T]],
    trip_updates: Sequence[gtfs_realtime_pb2.FeedMessage],
    vehicles: Sequence[gtfs_realtime_pb2.FeedMessage] = (),
) -> T:
    """Run ``body`` with several TripUpdates feeds (in catalog order) and
    VehiclePositions feeds, each its own message."""
    index = _build_index_from_files(files)

    async def scenario(handle: TransitFeedHandle) -> T:
        async def fetch(
            entity_type: EntityType, *_args: object
        ) -> list[gtfs_realtime_pb2.FeedMessage]:
            if entity_type is EntityType.TRIP_UPDATES:
                return list(trip_updates)
            return list(vehicles)

        handle._fetch_entity_messages = fetch  # type: ignore[method-assign,assignment]
        return await body(handle)

    return _run(index, scenario)


def _board(
    start: datetime, *, with_vehicles: bool = False
) -> Callable[[TransitFeedHandle], Awaitable[list[StopArrival]]]:
    async def body(handle: TransitFeedHandle) -> list[StopArrival]:
        [board] = await handle.get_arrivals(
            [ArrivalsQuery(["S0"])],
            start=start,
            end=start + timedelta(hours=2),
            with_vehicles=with_vehicles,
        )
        return board

    return body


def test_a_deleted_trip_is_not_shown_and_a_canceled_one_is() -> None:
    """DELETED: "A trip that existed in the schedule but was removed that
    must not be shown to users" -- "so the trip is not shown as cancelled
    to riders". A CANCELED one is: it stays, flagged, on boards and
    journeys alike."""
    descriptor = gtfs_realtime_pb2.TripDescriptor
    message = _descriptor_message(
        {"trip_id": "T", "start_date": "20260730", "relationship": descriptor.DELETED},
        {"trip_id": "X", "start_date": "20260730", "relationship": descriptor.CANCELED},
    )

    async def body(handle: TransitFeedHandle) -> tuple[list[object], list[object]]:
        start = _EIGHT - timedelta(minutes=30)
        [board] = await handle.get_arrivals(
            [ArrivalsQuery(["S0"])], start=start, end=start + timedelta(hours=3)
        )
        [journeys] = await handle.get_upcoming_trips(
            [TripQuery("S0", "S1")], start=start, end=start + timedelta(hours=3)
        )
        return (
            [(row.trip_id, row.canceled) for row in board],
            [(trip.trip_id, trip.canceled) for trip in journeys],
        )

    assert _run_feeds(_DIRECTED, body, [message]) == (
        [("X", True)],
        [("X", True)],
    )


def test_a_deletion_wins_over_a_cancellation_and_predictions() -> None:
    """At most one update per run, but a producer sending a deletion beside
    a cancellation or predictions for the same run gets it removed
    outright, in either order."""
    descriptor = gtfs_realtime_pb2.TripDescriptor
    parts: list[dict[str, object]] = [
        {"trip_id": "T", "start_date": "20260730", "delay": 60},
        {"trip_id": "T", "start_date": "20260730", "relationship": descriptor.CANCELED},
        {"trip_id": "T", "start_date": "20260730", "relationship": descriptor.DELETED},
    ]
    for ordered in (parts, parts[::-1]):
        board = _run_feeds(
            _DIRECTED,
            _board(_EIGHT - timedelta(minutes=30)),
            [_descriptor_message(*ordered)],
        )
        assert [row.trip_id for row in board] == ["X"]


def test_a_trip_id_less_descriptor_names_its_unique_trip() -> None:
    """ "If the trip_id field can't be provided, then route_id,
    direction_id, start_date, and start_time must all be provided": route
    R, direction 0, leaving at 08:00:00 on 2026-07-30 is T -- U leaves at
    08:00:00 the other way (from S1), X later the same way."""
    message = _descriptor_message(
        {
            "route_id": "R",
            "direction_id": 0,
            "start_time": "08:00:00",
            "start_date": "20260730",
            "delay": 120,
        }
    )
    board = _run_feeds(_DIRECTED, _board(_EIGHT - timedelta(minutes=30)), [message])
    assert [(row.trip_id, row.delay_seconds) for row in board] == [
        ("T", 120),
        ("X", None),
    ]


def test_a_trip_id_less_descriptor_names_no_trip_unless_unique_and_complete() -> None:
    """Alternative matching holds only "as long as the combination of ids
    provided resolves to a unique trip", and needs all four ids: two trips
    leaving together the same way name neither, and a descriptor missing
    one id, naming a day the trip does not run, or naming a frequency
    trip names nothing."""
    twin = _feed(
        trips=(
            "route_id,service_id,trip_id,trip_headsign,direction_id\n"
            "R,S,T,Out,0\nR,S,V,Out,0\nR,S,F,Out,0\n"
        ),
        stop_times=(
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "T,08:00:00,08:00:00,S0,1\nT,08:10:00,08:10:00,S1,2\n"
            "V,08:00:00,08:00:00,S0,1\nV,08:10:00,08:10:00,S1,2\n"
            "F,09:00:00,09:00:00,S0,1\nF,09:10:00,09:10:00,S1,2\n"
        ),
        frequencies="trip_id,start_time,end_time,headway_secs\nF,09:00:00,09:30:00,600\n",
    )
    complete = {
        "route_id": "R",
        "direction_id": 0,
        "start_time": "08:00:00",
        "start_date": "20260730",
        "delay": 120,
    }
    cases = [
        complete,
        {**complete, "start_time": "09:00:00"},  # a frequency repetition
        {**complete, "start_date": "20280101"},  # outside the calendar
        *(
            {k: v for k, v in complete.items() if k != gone}
            for gone in ("route_id", "direction_id", "start_time", "start_date")
        ),
    ]
    for fields in cases:
        board = _run_feeds(
            twin, _board(_EIGHT - timedelta(minutes=30)), [_descriptor_message(fields)]
        )
        assert all(row.delay_seconds is None for row in board), fields


def test_each_feeds_dateless_updates_are_judged_at_its_own_time() -> None:
    """Each FeedHeader.timestamp "identifies the moment when the content of
    this feed has been created". Feed A, stamped Thursday 07:45, cancels T
    without a date: Thursday's run. Feed B is stamped Friday 07:45. On
    Friday morning's board T runs -- B's later time must not move A's
    cancellation onto Friday."""
    thursday = datetime(2026, 7, 30, 7, 45, tzinfo=UTC)
    friday = thursday + timedelta(days=1)
    descriptor = gtfs_realtime_pb2.TripDescriptor
    feed_a = _descriptor_message(
        {"trip_id": "T", "relationship": descriptor.CANCELED}, timestamp=thursday
    )
    feed_b = _descriptor_message({"trip_id": "X", "delay": 60}, timestamp=friday)
    for feeds in ([feed_a, feed_b], [feed_b, feed_a]):
        thursdays = _run_feeds(_DIRECTED, _board(thursday), feeds)
        fridays = _run_feeds(_DIRECTED, _board(friday), feeds)
        assert [(r.trip_id, r.canceled) for r in thursdays] == [
            ("T", True),
            ("X", False),
        ]
        assert [(r.trip_id, r.canceled, r.delay_seconds) for r in fridays] == [
            ("T", False, None),
            ("X", False, 60),
        ]


def test_a_dateless_vehicle_is_judged_at_its_own_feeds_time() -> None:
    """The same for VehiclePositions: a vehicle naming T without a date in
    a feed stamped Thursday 07:45 rides Thursday's run, whatever time the
    TripUpdates feed carries."""
    thursday = datetime(2026, 7, 30, 7, 45, tzinfo=UTC)
    friday = thursday + timedelta(days=1)
    vehicles = gtfs_realtime_pb2.FeedMessage()
    vehicles.header.gtfs_realtime_version = "2.0"
    vehicles.header.timestamp = int(thursday.timestamp())
    entity = vehicles.entity.add()
    entity.id = "v"
    entity.vehicle.vehicle.id = "BUS"
    entity.vehicle.trip.trip_id = "T"
    entity.vehicle.position.latitude = 0
    entity.vehicle.position.longitude = 0
    trip_updates = _descriptor_message(timestamp=friday)
    for start, expected in ((thursday, "BUS"), (friday, None)):
        board = _run_feeds(
            _DIRECTED,
            _board(start, with_vehicles=True),
            [trip_updates],
            [vehicles],
        )
        [t_row] = [row for row in board if row.trip_id == "T"]
        assert (t_row.vehicle.vehicle_id if t_row.vehicle else None) == expected


def test_the_most_explicit_reference_to_a_run_wins() -> None:
    """One update per run: when two name the same run, the one naming its
    date beats one leaving it to the feed's time, and one omitting a plain
    trip's start time beats one spelling it out -- in any feed order."""
    stamp = datetime(2026, 7, 30, 7, 45, tzinfo=UTC)
    dated = _descriptor_message(
        {"trip_id": "T", "start_date": "20260730", "delay": 60}, timestamp=stamp
    )
    dateless = _descriptor_message({"trip_id": "T", "delay": 120}, timestamp=stamp)
    bare = _descriptor_message(
        {"trip_id": "T", "start_date": "20260730", "delay": 180}, timestamp=stamp
    )
    spelled = _descriptor_message(
        {
            "trip_id": "T",
            "start_date": "20260730",
            "start_time": "08:00:00",
            "delay": 240,
        },
        timestamp=stamp,
    )
    for feeds, delay in (
        ([dated, dateless], 60),
        ([dateless, dated], 60),
        ([bare, spelled], 180),
        ([spelled, bare], 180),
    ):
        board = _run_feeds(_DIRECTED, _board(stamp), feeds)
        assert [(r.trip_id, r.delay_seconds) for r in board] == [
            ("T", delay),
            ("X", None),
        ]


@settings(max_examples=40, deadline=None)
@given(
    other_offset=st.integers(-3 * 86_400, 3 * 86_400),
    kind=st.sampled_from(["cancel", "delay"]),
    board_day=st.integers(0, 1),
)
def test_another_feeds_time_never_moves_a_dateless_update(
    other_offset: int, kind: str, board_day: int
) -> None:
    """Law: the run a date-less update in feed A addresses is a function of
    A's header timestamp only. Feed B's timestamp -- earlier, later, across
    service-day boundaries -- leaves every board exactly as without B's
    clock (B updates an unrelated trip)."""
    stamp = datetime(2026, 7, 30, 7, 45, tzinfo=UTC)
    descriptor = gtfs_realtime_pb2.TripDescriptor
    update: dict[str, object] = (
        {"trip_id": "T", "relationship": descriptor.CANCELED}
        if kind == "cancel"
        else {"trip_id": "T", "delay": 90}
    )
    feed_a = _descriptor_message(update, timestamp=stamp)
    feed_b = _descriptor_message(
        {"trip_id": "X", "start_date": "20260730", "delay": 60},
        timestamp=stamp + timedelta(seconds=other_offset),
    )
    feed_b_same = _descriptor_message(
        {"trip_id": "X", "start_date": "20260730", "delay": 60}, timestamp=stamp
    )
    start = stamp + timedelta(days=board_day)
    assert _run_feeds(_DIRECTED, _board(start), [feed_a, feed_b]) == _run_feeds(
        _DIRECTED, _board(start), [feed_a, feed_b_same]
    )


@settings(max_examples=40, deadline=None)
@given(
    trip=st.sampled_from([("T", 0, "08:00:00"), ("X", 0, "09:00:00")]),
    day=st.sampled_from(["20260730", "20260731"]),
    kind=st.sampled_from(["cancel", "delete", "delay"]),
    delay=st.integers(-600, 1800),
)
def test_a_unique_trip_id_less_descriptor_reads_as_its_trip_id(
    trip: tuple[str, int, str], day: str, kind: str, delay: int
) -> None:
    """Law: where route, direction, start time and date name exactly one
    trip, a descriptor giving them instead of the trip_id means exactly
    what the trip_id descriptor means -- boards and journeys alike."""
    trip_id, direction, start_time = trip
    descriptor = gtfs_realtime_pb2.TripDescriptor
    payloads: dict[str, dict[str, object]] = {
        "cancel": {"relationship": descriptor.CANCELED},
        "delete": {"relationship": descriptor.DELETED},
        "delay": {"delay": delay},
    }
    payload = payloads[kind]
    by_id = _descriptor_message({"trip_id": trip_id, "start_date": day, **payload})
    by_route = _descriptor_message(
        {
            "route_id": "R",
            "direction_id": direction,
            "start_time": start_time,
            "start_date": day,
            **payload,
        }
    )

    async def body(handle: TransitFeedHandle) -> tuple[object, ...]:
        start = datetime(2026, 7, 30, 7, 30, tzinfo=UTC)
        boards = await handle.get_arrivals(
            [ArrivalsQuery(["S0", "S1"])], start=start, end=start + timedelta(days=2)
        )
        journeys = await handle.get_upcoming_trips(
            [TripQuery("S0", "S1")], start=start, end=start + timedelta(days=2)
        )
        return boards, journeys

    assert _run_feeds(_DIRECTED, body, [by_route]) == _run_feeds(
        _DIRECTED, body, [by_id]
    )


def test_trip_id_less_matching_is_bounded_by_the_references_not_the_route() -> None:
    """Resolving a route/direction/start reference reads only the trips with
    a call at that time: a route with more trips than SQLite allows bound
    variables still resolves (the limit is lowered here to stand in for a
    very large route)."""
    many = "".join(f"R,S,M{n},Out,0\n" for n in range(40))
    calls = "".join(
        f"M{n},10:{n:02d}:00,10:{n:02d}:00,S0,1\nM{n},11:{n:02d}:00,11:{n:02d}:00,S1,2\n"
        for n in range(40)
    )
    files = {
        **_DIRECTED,
        "trips.txt": _DIRECTED["trips.txt"] + many,
        "stop_times.txt": _DIRECTED["stop_times.txt"] + calls,
    }
    message = _descriptor_message(
        {
            "route_id": "R",
            "direction_id": 0,
            "start_time": "08:00:00",
            "start_date": "20260730",
            "delay": 60,
        }
    )

    async def body(handle: TransitFeedHandle) -> list[tuple[object, ...]]:
        handle._index._conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 30)
        board = await _board(_EIGHT - timedelta(minutes=30))(handle)
        return [(row.trip_id, row.delay_seconds) for row in board]

    assert _run_feeds(files, body, [message]) == [("T", 60), ("X", None)]


def test_a_canceled_run_carries_no_vehicle() -> None:
    """A cancelled run is shown "with no prediction and no vehicle", even
    when a vehicle claims it -- boards and journeys alike."""
    stamp = datetime(2026, 7, 30, 7, 45, tzinfo=UTC)
    descriptor = gtfs_realtime_pb2.TripDescriptor
    trip_updates = _descriptor_message(
        {"trip_id": "T", "start_date": "20260730", "relationship": descriptor.CANCELED},
        timestamp=stamp,
    )
    vehicles = gtfs_realtime_pb2.FeedMessage()
    vehicles.header.gtfs_realtime_version = "2.0"
    vehicles.header.timestamp = int(stamp.timestamp())
    entity = vehicles.entity.add()
    entity.id = "v"
    entity.vehicle.vehicle.id = "BUS"
    entity.vehicle.trip.trip_id = "T"
    entity.vehicle.trip.start_date = "20260730"
    entity.vehicle.position.latitude = 0
    entity.vehicle.position.longitude = 0

    async def body(handle: TransitFeedHandle) -> tuple[object, ...]:
        [[row]] = await handle.get_arrivals(
            [ArrivalsQuery(["S0"], limit=1)], start=stamp, with_vehicles=True
        )
        [[trip]] = await handle.get_upcoming_trips(
            [TripQuery("S0", "S1", limit=1)], start=stamp, with_vehicles=True
        )
        return (row.trip_id, row.canceled, row.vehicle), (
            trip.trip_id,
            trip.canceled,
            trip.vehicle,
        )

    assert _run_feeds(_DIRECTED, body, [trip_updates], [vehicles]) == (
        ("T", True, None),
        ("T", True, None),
    )


def test_an_added_trip_without_a_trip_id_keeps_its_stop_times() -> None:
    """Only a SCHEDULED-style reference needs the schedule to resolve it; an
    ADDED trip's stop times stand on their own, trip_id or not."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "a"
    entity.trip_update.trip.route_id = "R"
    entity.trip_update.trip.schedule_relationship = (
        gtfs_realtime_pb2.TripDescriptor.ADDED
    )
    stu = entity.trip_update.stop_time_update.add()
    stu.stop_id = "S0"
    stu.departure.time = int(_EIGHT.timestamp())
    [added] = trip_updates_from_message(message).realtime_trips
    assert added.trip_id == ""
    assert [(call.stop_id, call.departure) for call in added.calls] == [("S0", _EIGHT)]


def _detoured(message: gtfs_realtime_pb2.FeedMessage) -> None:
    """Add a detour of T on 2026-07-30: S1 replaced by S2, 600 s on."""
    entity = message.entity.add()
    entity.id = "detour"
    modification = entity.trip_modifications.modifications.add()
    modification.start_stop_selector.stop_sequence = 2
    modification.end_stop_selector.stop_sequence = 2
    replacement = modification.replacement_stops.add()
    replacement.stop_id = "S2"
    replacement.travel_time_to_stop = 600
    entity.trip_modifications.selected_trips.add().trip_ids.append("T")
    entity.trip_modifications.service_dates.append("20260730")


def test_a_removed_run_is_not_rebuilt_by_its_detour() -> None:
    """A detour of a run realtime cancels or deletes does not bring it back
    as a live, rerouted trip: the cancelled run is shown cancelled on its
    scheduled stops, and the deleted one not at all."""
    descriptor = gtfs_realtime_pb2.TripDescriptor
    start = _EIGHT - timedelta(minutes=30)

    async def body(handle: TransitFeedHandle) -> list[tuple[object, ...]]:
        [board] = await handle.get_arrivals(
            [ArrivalsQuery(["S0", "S1", "S2"])],
            start=start,
            end=start + timedelta(hours=2),
        )
        return [(r.trip_id, r.stop_id, r.canceled) for r in board if r.trip_id == "T"]

    detour_only = _descriptor_message()
    _detoured(detour_only)
    assert _run_feeds(_DIRECTED, body, [detour_only]) == [
        ("T", "S0", False),
        ("T", "S2", False),
    ]
    for relationship, expected in (
        (descriptor.CANCELED, [("T", "S0", True), ("T", "S1", True)]),
        (descriptor.DELETED, []),
    ):
        message = _descriptor_message(
            {"trip_id": "T", "start_date": "20260730", "relationship": relationship}
        )
        _detoured(message)
        assert _run_feeds(_DIRECTED, body, [message]) == expected


def test_an_added_trip_without_a_trip_id_is_shown_on_its_own() -> None:
    """An id-less ADDED trip on T's route, direction and start is its own
    row, never matched onto T."""
    message = _descriptor_message(
        {
            "route_id": "R",
            "direction_id": 0,
            "start_time": "08:00:00",
            "start_date": "20260730",
            "relationship": gtfs_realtime_pb2.TripDescriptor.ADDED,
        }
    )
    stu = message.entity[0].trip_update.stop_time_update.add()
    stu.stop_id = "S0"
    stu.departure.time = int((_EIGHT + timedelta(minutes=5)).timestamp())
    board = _run_feeds(_DIRECTED, _board(_EIGHT - timedelta(minutes=30)), [message])
    assert [(r.trip_id, r.predicted_departure, r.delay_seconds) for r in board] == [
        ("T", None, None),
        ("", _EIGHT + timedelta(minutes=5), None),
        ("X", None, None),
    ]


def test_first_departures_agree_with_trip_spans() -> None:
    """The stored first departure is a trip's start as GTFS-RT reads it --
    its first timed call's departure, the earliest where that sequence
    repeats, a one-call trip's arrival -- the same as the span reading; and
    a route/direction/start lookup reads it through its index."""
    index = _build_index_from_files(
        _feed(
            trips=(
                "route_id,service_id,trip_id,trip_headsign,direction_id\n"
                "R,S,A,H,0\nR,S,B,H,0\nR,S,C,H,0\nR,S,D,H,0\nR,S,F,H,0\n"
            ),
            stop_times=(
                "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                "A,07:59:00,08:00:00,S0,1\nA,08:10:00,08:10:00,S1,2\n"
                "B,08:20:00,08:25:00,S0,1\n"
                "C,,,S0,1\nC,09:00:00,09:01:00,S1,2\nC,09:10:00,09:10:00,S0,3\n"
                "D,09:30:00,09:31:00,S0,1\nD,09:20:00,09:29:00,S0,1\n"
                "D,09:50:00,09:50:00,S1,2\n"
                "F,10:00:00,10:00:00,S0,1\nF,10:10:00,10:10:00,S1,2\n"
            ),
            frequencies="trip_id,start_time,end_time,headway_secs\nF,10:00:00,10:20:00,600\n",
        )
    )
    try:
        spans = {
            trip: start
            for trip, _service, repetition, start, _end in index._trip_spans(
                ["A", "B", "C", "D"]
            )
            if repetition is None
        }
        assert index.plain_trip_starts(["A", "B", "C", "D", "F", "F#36000"]) == spans
        assert spans == {"A": 28800, "B": 30000, "C": 32460, "D": 34140}
        plan = " ".join(
            str(row)
            for row in index._conn.execute(
                "EXPLAIN QUERY PLAN SELECT id, service_id FROM trips "
                "WHERE route_id = ? AND direction_id = ? AND first_secs = ? "
                "AND start_secs IS NULL AND source_trip_id = id",
                ("R", 0, 28800),
            )
        )
        assert "ix_trips_route_start" in plan
    finally:
        index.close()


def test_one_vehicle_in_two_feeds_is_one_claimant() -> None:
    """The same vehicle reported by two VehiclePositions feeds (or one url
    listed twice) still rides its run: one vehicle, not an ambiguous two."""
    stamp = datetime(2026, 7, 30, 7, 45, tzinfo=UTC)
    vehicles = gtfs_realtime_pb2.FeedMessage()
    vehicles.header.gtfs_realtime_version = "2.0"
    vehicles.header.timestamp = int(stamp.timestamp())
    entity = vehicles.entity.add()
    entity.id = "v"
    entity.vehicle.vehicle.id = "BUS"
    entity.vehicle.trip.trip_id = "T"
    entity.vehicle.trip.start_date = "20260730"
    entity.vehicle.position.latitude = 0
    entity.vehicle.position.longitude = 0
    board = _run_feeds(
        _DIRECTED,
        _board(stamp, with_vehicles=True),
        [_descriptor_message(timestamp=stamp)],
        [vehicles, vehicles],
    )
    [t_row] = [row for row in board if row.trip_id == "T"]
    assert t_row.vehicle is not None and t_row.vehicle.vehicle_id == "BUS"


# --- Trips realtime states in full: NEW, ADDED, REPLACEMENT -----------------


def _at(minutes: int) -> datetime:
    return _EIGHT + timedelta(minutes=minutes)


def _boards_and_journeys(
    *pairs: tuple[str, str],
) -> Callable[[TransitFeedHandle], Awaitable[tuple[object, ...]]]:
    """Board rows at S0-S2 and journeys for each pair, as comparable tuples."""

    async def body(handle: TransitFeedHandle) -> tuple[object, ...]:
        start = _EIGHT - timedelta(minutes=30)
        [board] = await handle.get_arrivals(
            [ArrivalsQuery(["S0", "S1", "S2"], limit=None)],
            start=start,
            end=start + timedelta(hours=3),
        )
        journeys = await handle.get_upcoming_trips(
            [
                TripQuery(origin, destination, limit=None)
                for origin, destination in pairs
            ],
            start=start,
            end=start + timedelta(hours=3),
        )
        return (
            [
                (
                    r.trip_id,
                    r.stop_id,
                    r.scheduled_departure,
                    r.predicted_arrival,
                    r.predicted_departure,
                    r.canceled,
                )
                for r in board
            ],
            [
                [
                    (
                        j.trip_id,
                        j.scheduled_departure,
                        j.predicted_departure,
                        j.predicted_arrival,
                        j.headsign,
                        j.route_id,
                    )
                    for j in rows
                ]
                for rows in journeys
            ],
        )

    return body


def test_a_new_trip_is_shown_on_boards_and_journeys() -> None:
    """NEW: "An extra trip unrelated to any existing trips ... The complete
    journey of the new trip, including all stops and times, must be
    specified via StopTimeUpdates." It is shown at its stops and offered
    between them -- even between stops no scheduled trip links -- with its
    headsign from TripProperties; a skipped stop and one without a time
    are not shown."""
    message = _descriptor_message(
        {
            "trip_id": "N",
            "route_id": "R",
            "start_date": "20260730",
            "relationship": gtfs_realtime_pb2.TripDescriptor.NEW,
            "headsign": "Extra",
            "calls": [
                ("S0", None, _at(30), False),
                ("S1", _at(35), None, True),  # skipped
                ("S2", None, None, False),  # no time
                ("S1", _at(40), _at(41), False),
                ("S2", _at(50), None, False),
            ],
        }
    )
    board, (s0_s1, s0_s2, s1_s0) = _run_feeds(
        _DIRECTED,
        _boards_and_journeys(("S0", "S1"), ("S0", "S2"), ("S1", "S0")),
        [message],
    )
    assert [row for row in board if row[0] == "N"] == [
        ("N", "S0", None, None, _at(30), False),
        ("N", "S1", None, _at(40), _at(41), False),
        ("N", "S2", None, _at(50), None, False),
    ]
    assert ("N", None, _at(30), _at(40), "Extra", "R") in s0_s1
    # No scheduled trip runs S0 to S2; the new one does.
    assert s0_s2 == [("N", None, _at(30), _at(50), "Extra", "R")]
    assert all(row[0] != "N" for row in s1_s0)  # it never runs S1 to S0


def test_a_replacement_replaces_the_scheduled_run() -> None:
    """REPLACEMENT: "The complete journey of the replacement trip must be
    specified via StopTimeUpdates, and the original schedule from the GTFS
    static isn't used for the replaced instance." T's scheduled rows give
    way to its replacement's, which keep T's route and headsign."""
    message = _descriptor_message(
        {
            "trip_id": "T",
            "start_date": "20260730",
            "relationship": gtfs_realtime_pb2.TripDescriptor.REPLACEMENT,
            "calls": [
                ("S0", None, _at(5), False),
                ("S2", _at(25), None, False),
            ],
        }
    )
    board, (s0_s1, s0_s2) = _run_feeds(
        _DIRECTED, _boards_and_journeys(("S0", "S1"), ("S0", "S2")), [message]
    )
    assert [row for row in board if row[0] == "T"] == [
        ("T", "S0", None, None, _at(5), False),
        ("T", "S2", None, _at(25), None, False),
    ]
    assert all(row[0] != "T" for row in s0_s1)  # the rerouted T skips S1
    assert s0_s2 == [("T", None, _at(5), _at(25), "Out", "R")]


def test_a_removal_wins_over_a_replacement_or_an_extra_trip() -> None:
    """A run cancelled as well as replaced is shown cancelled on its
    schedule; deleted, it is gone; and a NEW trip any removal names is not
    shown -- whatever the order."""
    descriptor = gtfs_realtime_pb2.TripDescriptor
    replacement = {
        "trip_id": "T",
        "start_date": "20260730",
        "relationship": descriptor.REPLACEMENT,
        "calls": [("S0", None, _at(5), False)],
    }
    extra = {
        "trip_id": "N",
        "start_date": "20260730",
        "relationship": descriptor.NEW,
        "calls": [("S0", None, _at(30), False)],
    }
    for removal, expected in (
        (
            descriptor.CANCELED,
            [
                ("T", "S0", _EIGHT, None, None, True),
                ("T", "S1", _at(10), None, None, True),
            ],
        ),
        (descriptor.DELETED, []),
    ):
        removals = [
            {"trip_id": trip_id, "start_date": "20260730", "relationship": removal}
            for trip_id in ("T", "N")
        ]
        for parts in ([replacement, extra, *removals], [*removals, replacement, extra]):
            board, _ = _run_feeds(
                _DIRECTED, _boards_and_journeys(), [_descriptor_message(*parts)]
            )
            assert [row for row in board if row[0] in {"T", "N"}] == expected


def test_a_replaced_run_is_not_rebuilt_by_its_detour() -> None:
    """A detour of a replaced run does not bring its schedule back: the
    replacement's journey is the run."""
    message = _descriptor_message(
        {
            "trip_id": "T",
            "start_date": "20260730",
            "relationship": gtfs_realtime_pb2.TripDescriptor.REPLACEMENT,
            "calls": [("S0", None, _at(5), False)],
        }
    )
    _detoured(message)
    board, _ = _run_feeds(_DIRECTED, _boards_and_journeys(), [message])
    assert [row for row in board if row[0] == "T"] == [
        ("T", "S0", None, None, _at(5), False)
    ]


def test_trips_stated_in_full_carry_their_vehicle() -> None:
    """A NEW trip's vehicle (named by the trip it runs) and a replacement's
    (named by the replaced trip's run) ride their rows."""
    stamp = datetime(2026, 7, 30, 7, 45, tzinfo=UTC)
    descriptor = gtfs_realtime_pb2.TripDescriptor
    trip_updates = _descriptor_message(
        {
            "trip_id": "N",
            "start_date": "20260730",
            "relationship": descriptor.NEW,
            "calls": [("S0", None, _at(30), False)],
        },
        {
            "trip_id": "T",
            "start_date": "20260730",
            "relationship": descriptor.REPLACEMENT,
            "calls": [("S0", None, _at(5), False)],
        },
        timestamp=stamp,
    )
    vehicles = gtfs_realtime_pb2.FeedMessage()
    vehicles.header.gtfs_realtime_version = "2.0"
    vehicles.header.timestamp = int(stamp.timestamp())
    for vehicle_id, trip_id in (("BUS-N", "N"), ("BUS-T", "T")):
        entity = vehicles.entity.add()
        entity.id = vehicle_id
        entity.vehicle.vehicle.id = vehicle_id
        entity.vehicle.trip.trip_id = trip_id
        entity.vehicle.trip.start_date = "20260730"
        entity.vehicle.position.latitude = 0
        entity.vehicle.position.longitude = 0
    board = _run_feeds(
        _DIRECTED, _board(stamp, with_vehicles=True), [trip_updates], [vehicles]
    )
    assert {
        row.trip_id: row.vehicle.vehicle_id if row.vehicle else None
        for row in board
        if row.trip_id in {"N", "T"}
    } == {"N": "BUS-N", "T": "BUS-T"}


_STOPS = ("S0", "S1", "S2")


@settings(max_examples=60, deadline=None)
@given(
    calls=st.lists(
        st.tuples(
            st.sampled_from(_STOPS),
            st.none() | st.integers(0, 120),
            st.none() | st.integers(0, 120),
            st.booleans(),
        ),
        max_size=5,
    ),
    relationship=st.sampled_from(
        [
            gtfs_realtime_pb2.TripDescriptor.NEW,
            gtfs_realtime_pb2.TripDescriptor.ADDED,
        ]
    ),
)
def test_a_trip_stated_in_full_is_exactly_its_calls(
    calls: list[tuple[str, int | None, int | None, bool]], relationship: int
) -> None:
    """Law: an extra trip's board rows are exactly its unskipped, timed
    calls at the queried stops, at the times given; its journeys exactly
    each such call at an origin with the next one at the destination."""
    timed = [
        (stop, None if a is None else _at(a), None if d is None else _at(d))
        for stop, a, d, skipped in calls
        if not skipped and (a is not None or d is not None)
    ]
    message = _descriptor_message(
        {
            "trip_id": "N",
            "route_id": "R",
            "start_date": "20260730",
            "relationship": relationship,
            "calls": [
                (stop, None if a is None else _at(a), None if d is None else _at(d), k)
                for stop, a, d, k in calls
            ],
        }
    )
    pairs = [(o, d) for o in _STOPS for d in _STOPS if o != d]
    start = _EIGHT - timedelta(minutes=30)
    board, journeys = _run_feeds(_DIRECTED, _boards_and_journeys(*pairs), [message])
    expected_rows = sorted(
        (
            ("N", stop, None, arrival, departure, False)
            for stop, arrival, departure in timed
            if (departure or arrival or start) >= start
        ),
        key=lambda row: (row[4] or row[3], row[1]),
    )
    assert (
        sorted(
            (row for row in board if row[0] == "N"),
            key=lambda row: (row[4] or row[3], row[1]),
        )
        == expected_rows
    )
    for (origin, destination), rows in zip(pairs, journeys, strict=True):
        expected = []
        for index, (stop, _arrival, departure) in enumerate(timed):
            if stop != origin:
                continue
            alight = next((c for c in timed[index + 1 :] if c[0] == destination), None)
            if alight is not None:
                expected.append(("N", None, departure, alight[1], None, "R"))
        assert sorted((r for r in rows if r[0] == "N"), key=repr) == sorted(
            expected, key=repr
        )


def test_a_replacements_no_data_stops_keep_its_schedule() -> None:
    """For a NEW or REPLACEMENT trip, NO_DATA stops "must still be given
    with scheduled times, as the StopTimeUpdate defines the stop list of
    the trip": they show, scheduled, unpredicted -- the run is not lost."""
    message = _descriptor_message(
        {
            "trip_id": "T",
            "start_date": "20260730",
            "relationship": gtfs_realtime_pb2.TripDescriptor.REPLACEMENT,
            "stops": [
                {"stop": "S0", "scheduled_departure": _at(5), "no_data": True},
                {"stop": "S2", "scheduled_arrival": _at(25), "no_data": True},
            ],
        }
    )
    board, (s0_s2,) = _run_feeds(
        _DIRECTED, _boards_and_journeys(("S0", "S2")), [message]
    )
    assert [row for row in board if row[0] == "T"] == [
        ("T", "S0", _at(5), None, None, False),
        ("T", "S2", _at(25), None, None, False),
    ]
    assert s0_s2 == [("T", _at(5), None, None, "Out", "R")]


def test_a_new_trips_scheduled_time_and_delay_predict_it() -> None:
    """A new trip's scheduled_time is its schedule: a delay moves it, as
    "Real-time prediction delay values are applied to this calculated
    schedule time"; a time wins over both."""
    message = _descriptor_message(
        {
            "trip_id": "N",
            "start_date": "20260730",
            "relationship": gtfs_realtime_pb2.TripDescriptor.NEW,
            "stops": [
                {"stop": "S0", "scheduled_departure": _at(30), "departure_delay": 120},
                {"stop": "S1", "scheduled_arrival": _at(40), "arrival": _at(41)},
            ],
        }
    )
    board = _run_feeds(_DIRECTED, _board(_EIGHT - timedelta(minutes=30)), [message])
    assert [
        (r.scheduled_departure, r.predicted_departure, r.delay_seconds)
        for r in board
        if r.trip_id == "N"
    ] == [(_at(30), _at(32), 120)]


def test_one_new_trip_from_two_feeds_is_shown_once() -> None:
    """The same NEW trip from two feeds is one trip, the later feed's."""
    descriptor = gtfs_realtime_pb2.TripDescriptor

    def new(minutes: int) -> gtfs_realtime_pb2.FeedMessage:
        return _descriptor_message(
            {
                "trip_id": "N",
                "start_date": "20260730",
                "relationship": descriptor.NEW,
                "calls": [
                    ("S0", None, _at(minutes), False),
                    ("S2", _at(50), None, False),
                ],
            }
        )

    board, (s0_s2,) = _run_feeds(
        _DIRECTED, _boards_and_journeys(("S0", "S2")), [new(30), new(35)]
    )
    assert [row[:5] for row in board if row[0] == "N"] == [
        ("N", "S0", None, None, _at(35)),
        ("N", "S2", None, _at(50), None),
    ]
    assert s0_s2 == [("N", None, _at(35), _at(50), None, None)]


def test_a_replacement_naming_no_run_replaces_nothing() -> None:
    """A replacement is a scheduled run on a new schedule: one whose
    descriptor names no run -- here T with a start_time other than its
    first departure -- replaces nothing and is not shown beside T."""
    message = _descriptor_message(
        {
            "trip_id": "T",
            "start_date": "20260730",
            "start_time": "08:05:00",
            "relationship": gtfs_realtime_pb2.TripDescriptor.REPLACEMENT,
            "calls": [("S0", None, _at(5), False)],
        }
    )
    board, _ = _run_feeds(_DIRECTED, _boards_and_journeys(), [message])
    assert [row[:5] for row in board if row[0] == "T"] == [
        ("T", "S0", _EIGHT, None, None),
        ("T", "S1", _at(10), None, None),
    ]


def test_a_replaced_repetition_is_that_run() -> None:
    """A replacement of one frequency repetition hides that repetition only,
    shows under its trip id, and rides that repetition's vehicle -- not one
    naming no repetition."""
    files = _feed(
        frequencies=(
            "trip_id,start_time,end_time,headway_secs\nT,08:00:00,09:00:00,1800\n"
        ),
    )
    stamp = datetime(2026, 7, 30, 7, 45, tzinfo=UTC)
    trip_updates = _descriptor_message(
        {
            "trip_id": "T",
            "start_date": "20260730",
            "start_time": "08:30:00",
            "relationship": gtfs_realtime_pb2.TripDescriptor.REPLACEMENT,
            "calls": [("S0", None, _at(33), False)],
        },
        timestamp=stamp,
    )
    vehicles = gtfs_realtime_pb2.FeedMessage()
    vehicles.header.gtfs_realtime_version = "2.0"
    vehicles.header.timestamp = int(stamp.timestamp())
    for vehicle_id, start_time, start_date in (
        ("BUS-0800", "08:00:00", "20260730"),
        ("BUS-0830", "08:30:00", "20260730"),
        ("BUS-X", "", ""),
    ):
        entity = vehicles.entity.add()
        entity.id = vehicle_id
        entity.vehicle.vehicle.id = vehicle_id
        entity.vehicle.trip.trip_id = "T"
        if start_time:
            entity.vehicle.trip.start_time = start_time
            entity.vehicle.trip.start_date = start_date
        entity.vehicle.position.latitude = 0
        entity.vehicle.position.longitude = 0
    board = _run_feeds(
        files, _board(stamp, with_vehicles=True), [trip_updates], [vehicles]
    )
    assert [
        (
            r.trip_id,
            r.scheduled_departure,
            r.predicted_departure,
            r.vehicle.vehicle_id if r.vehicle else None,
        )
        for r in board
    ] == [
        ("T#28800", _EIGHT, None, "BUS-0800"),
        ("T#30600", None, _at(33), "BUS-0830"),
    ]


def test_extra_trips_without_a_trip_id_never_merge() -> None:
    """A trip without a trip_id has no identity to merge on: two such
    added trips leaving at once on different routes both show."""
    descriptor = gtfs_realtime_pb2.TripDescriptor
    message = _descriptor_message(
        *(
            {
                "route_id": route,
                "direction_id": direction,
                "start_date": "20260730",
                "start_time": "08:30:00",
                "relationship": descriptor.ADDED,
                "calls": [("S0", None, _at(minutes), False)],
            }
            for route, direction, minutes in (("R", 0, 30), ("R2", 1, 31))
        )
    )
    board = _run_feeds(_DIRECTED, _board(_EIGHT - timedelta(minutes=30)), [message])
    assert [(r.route_id, r.predicted_departure) for r in board if not r.trip_id] == [
        ("R", _at(30)),
        ("R2", _at(31)),
    ]


def test_a_new_trip_is_one_trip_however_its_feeds_date_it() -> None:
    """An extra trip is its trip_id: one feed dating it and another not is
    still one trip, the later feed's."""
    descriptor = gtfs_realtime_pb2.TripDescriptor
    dated = _descriptor_message(
        {
            "trip_id": "N",
            "start_date": "20260730",
            "relationship": descriptor.NEW,
            "calls": [("S0", None, _at(30), False)],
        }
    )
    undated = _descriptor_message(
        {
            "trip_id": "N",
            "relationship": descriptor.NEW,
            "calls": [("S0", None, _at(35), False)],
        }
    )
    board = _run_feeds(
        _DIRECTED, _board(_EIGHT - timedelta(minutes=30)), [dated, undated]
    )
    assert [r.predicted_departure for r in board if r.trip_id == "N"] == [_at(35)]


def test_end_bounds_realtime_only_trips_as_scheduled_ones() -> None:
    """One window rule for every row: a scheduled time -- a realtime-only
    trip's scheduled_time, else its predicted time -- at or before ``end``.
    A NEW trip at 13:00 is not on an 07:30-08:30 board, nor tomorrow's
    replacement of T on today's."""
    descriptor = gtfs_realtime_pb2.TripDescriptor
    message = _descriptor_message(
        {
            "trip_id": "N",
            "start_date": "20260730",
            "relationship": descriptor.NEW,
            "stops": [
                {"stop": "S0", "scheduled_departure": _at(300), "departure": _at(300)},
                {"stop": "S2", "scheduled_arrival": _at(320)},
            ],
        },
        {
            "trip_id": "P",
            "start_date": "20260730",
            "relationship": descriptor.NEW,
            "calls": [("S0", None, _at(200), False)],  # predicted only
        },
        {
            "trip_id": "T",
            "start_date": "20260731",
            "relationship": descriptor.REPLACEMENT,
            "calls": [("S0", None, _at(24 * 60 + 5), False)],
        },
    )

    async def body(handle: TransitFeedHandle) -> tuple[object, ...]:
        start = _EIGHT - timedelta(minutes=30)
        [board] = await handle.get_arrivals(
            [ArrivalsQuery(["S0", "S2"], limit=None)],
            start=start,
            end=start + timedelta(hours=1),
        )
        [journeys] = await handle.get_upcoming_trips(
            [TripQuery("S0", "S2", limit=None)],
            start=start,
            end=start + timedelta(hours=1),
        )
        return [r.trip_id for r in board], [j.trip_id for j in journeys]

    assert _run_feeds(_DIRECTED, body, [message]) == (["T", "U"], [])


def test_a_new_trips_vehicle_is_the_one_naming_its_trip() -> None:
    """A NEW trip's vehicle is the one VehiclePosition naming its trip_id,
    however that position spells the start; two naming it make neither."""
    stamp = datetime(2026, 7, 30, 7, 45, tzinfo=UTC)
    trip_updates = _descriptor_message(
        {
            "trip_id": "N",
            "start_date": "20260730",
            "start_time": "08:30:00",
            "relationship": gtfs_realtime_pb2.TripDescriptor.NEW,
            "calls": [("S0", None, _at(30), False)],
        },
        timestamp=stamp,
    )

    def positions(*vehicle_ids: str) -> gtfs_realtime_pb2.FeedMessage:
        message = gtfs_realtime_pb2.FeedMessage()
        message.header.gtfs_realtime_version = "2.0"
        message.header.timestamp = int(stamp.timestamp())
        for vehicle_id in vehicle_ids:
            entity = message.entity.add()
            entity.id = vehicle_id
            entity.vehicle.vehicle.id = vehicle_id
            entity.vehicle.trip.trip_id = "N"
            entity.vehicle.trip.start_date = "20260730"
            entity.vehicle.trip.start_time = "08:30:00"
            entity.vehicle.position.latitude = 0
            entity.vehicle.position.longitude = 0
        return message

    for vehicles, expected in (
        (positions("BUS-N"), "BUS-N"),
        (positions("BUS-N", "BUS-M"), None),
    ):
        board = _run_feeds(
            _DIRECTED, _board(stamp, with_vehicles=True), [trip_updates], [vehicles]
        )
        [row] = [r for r in board if r.trip_id == "N"]
        assert (row.vehicle.vehicle_id if row.vehicle else None) == expected


def test_an_extra_trip_reusing_a_scheduled_id_never_takes_its_vehicle() -> None:
    """A vehicle naming a scheduled trip rides that trip's run: an ADDED
    trip reusing the id (as feeds did before DUPLICATED) does not take it,
    and one bus never shows on two rows."""
    stamp = datetime(2026, 7, 30, 7, 45, tzinfo=UTC)
    trip_updates = _descriptor_message(
        {
            "trip_id": "T",
            "relationship": gtfs_realtime_pb2.TripDescriptor.ADDED,
            "calls": [("S0", None, _at(40), False)],
        },
        timestamp=stamp,
    )
    vehicles = gtfs_realtime_pb2.FeedMessage()
    vehicles.header.gtfs_realtime_version = "2.0"
    vehicles.header.timestamp = int(stamp.timestamp())
    entity = vehicles.entity.add()
    entity.id = "BUS-T"
    entity.vehicle.vehicle.id = "BUS-T"
    entity.vehicle.trip.trip_id = "T"
    entity.vehicle.trip.start_date = "20260730"
    entity.vehicle.position.latitude = 0
    entity.vehicle.position.longitude = 0
    board = _run_feeds(
        _DIRECTED, _board(stamp, with_vehicles=True), [trip_updates], [vehicles]
    )
    assert [
        (r.trip_id, r.predicted_departure, r.vehicle.vehicle_id if r.vehicle else None)
        for r in board
        if r.trip_id == "T"
    ] == [("T", None, "BUS-T"), ("T", _at(40), None)]


def test_a_replacement_keeps_its_trips_route_and_direction() -> None:
    """A TripDescriptor's route and direction only select the trip: a
    replacement of T naming another route is still T on T's route and
    direction, with T's headsign unless its own differs."""
    message = _descriptor_message(
        {
            "trip_id": "T",
            "route_id": "R2",
            "direction_id": 1,
            "start_date": "20260730",
            "relationship": gtfs_realtime_pb2.TripDescriptor.REPLACEMENT,
            "calls": [("S0", None, _at(5), False)],
        }
    )
    board = _run_feeds(_DIRECTED, _board(_EIGHT - timedelta(minutes=30)), [message])
    assert [
        (r.route_id, r.direction_id, r.headsign) for r in board if r.trip_id == "T"
    ] == [("R", 0, "Out")]


def test_filters_see_realtime_only_trips_by_what_their_updates_give() -> None:
    """Route and headsign filters apply to realtime-only rows: a NEW trip
    passes by its route and TripProperties headsign, a replacement by its
    run's route and headsign."""
    descriptor = gtfs_realtime_pb2.TripDescriptor
    message = _descriptor_message(
        {
            "trip_id": "N",
            "route_id": "R",
            "start_date": "20260730",
            "relationship": descriptor.NEW,
            "headsign": "Extra",
            "calls": [("S0", None, _at(30), False)],
        },
        {
            "trip_id": "T",
            "start_date": "20260730",
            "relationship": descriptor.REPLACEMENT,
            "calls": [("S0", None, _at(5), False)],
        },
    )

    async def body(handle: TransitFeedHandle) -> list[list[str | None]]:
        start = _EIGHT - timedelta(minutes=30)
        boards = await handle.get_arrivals(
            [
                ArrivalsQuery(["S0"], headsigns=["Extra"]),
                ArrivalsQuery(["S0"], headsigns=["Out"]),
                ArrivalsQuery(["S0"], route_ids=["R"]),
                ArrivalsQuery(["S0"], route_ids=["NOPE"]),
            ],
            start=start,
            end=start + timedelta(hours=1),
        )
        return [[row.trip_id for row in board] for board in boards]

    assert _run_feeds(_DIRECTED, body, [message]) == [["N"], ["T"], ["T", "N"], []]


# --- DUPLICATED: a copy of a scheduled trip at another start ---------------


def _duplicate(
    copy: tuple[str, str, str], *, stops: list[dict[str, Any]] | None = None
) -> dict[str, object]:
    return {
        "trip_id": "T",
        "relationship": gtfs_realtime_pb2.TripDescriptor.DUPLICATED,
        "copy": copy,
        "stops": stops or [],
    }


def test_a_duplicate_runs_the_original_at_its_own_start() -> None:
    """DUPLICATED: "A new trip that is the same as an existing scheduled trip
    except for service start date and time"; "Scheduled arrival and
    departure times for the duplicated trip are calculated based on the
    offset between the original trip departure_time and this field", and
    "delay values are applied to this calculated schedule time". The
    original is untouched: "This enumeration does not modify the existing
    trip"."""
    message = _descriptor_message(
        _duplicate(
            ("T-2", "20260730", "09:30:00"),
            stops=[{"stop": "S1", "departure_delay": 120}],
        )
    )
    board, (s0_s1,) = _run_feeds(
        _DIRECTED, _boards_and_journeys(("S0", "S1")), [message]
    )
    assert [row for row in board if row[0] in {"T", "T-2"}] == [
        ("T", "S0", _EIGHT, None, None, False),
        ("T", "S1", _at(10), None, None, False),
        ("T-2", "S0", _at(90), None, None, False),
        ("T-2", "S1", _at(100), None, _at(102), False),
    ]
    assert ("T-2", _at(90), None, None, "Out", "R") in s0_s1
    assert ("T", _EIGHT, None, None, "Out", "R") in s0_s1


def test_a_duplicate_runs_on_a_day_its_original_does_not() -> None:
    """The copy runs at its own start_date even where the original's
    service does not (the feed's calendar ends in 2027)."""
    message = _descriptor_message(_duplicate(("T-2", "20280105", "09:30:00")))
    start = datetime(2028, 1, 5, 9, tzinfo=UTC)
    board = _run_feeds(_DIRECTED, _board(start), [message])
    assert [(r.trip_id, r.scheduled_departure) for r in board] == [
        ("T-2", datetime(2028, 1, 5, 9, 30, tzinfo=UTC))
    ]


def test_a_duplicates_vehicle_names_the_copy() -> None:
    """ "The VehiclePosition.TripDescriptor.trip_id for the new trip must
    contain the matching value from TripUpdate.TripProperties.trip_id": the
    copy rides that vehicle, the original keeps its own."""
    stamp = datetime(2026, 7, 30, 7, 45, tzinfo=UTC)
    trip_updates = _descriptor_message(
        _duplicate(("T-2", "20260730", "09:30:00")), timestamp=stamp
    )
    vehicles = gtfs_realtime_pb2.FeedMessage()
    vehicles.header.gtfs_realtime_version = "2.0"
    vehicles.header.timestamp = int(stamp.timestamp())
    for vehicle_id, trip_id in (("BUS-T", "T"), ("BUS-COPY", "T-2")):
        entity = vehicles.entity.add()
        entity.id = vehicle_id
        entity.vehicle.vehicle.id = vehicle_id
        entity.vehicle.trip.trip_id = trip_id
        entity.vehicle.trip.start_date = "20260730"
        if trip_id == "T-2":
            entity.vehicle.trip.schedule_relationship = (
                gtfs_realtime_pb2.TripDescriptor.DUPLICATED
            )
        entity.vehicle.position.latitude = 0
        entity.vehicle.position.longitude = 0
    board = _run_feeds(
        _DIRECTED, _board(stamp, with_vehicles=True), [trip_updates], [vehicles]
    )
    assert {
        r.trip_id: r.vehicle.vehicle_id if r.vehicle else None
        for r in board
        if r.trip_id in {"T", "T-2"}
    } == {"T": "BUS-T", "T-2": "BUS-COPY"}


def test_a_cancelled_copy_is_not_shown_and_leaves_the_original() -> None:
    """A copy is identified by its own trip_id: a cancellation naming it
    removes the copy, never the original (cancelling the original "must"
    be its own TripUpdate)."""
    descriptor = gtfs_realtime_pb2.TripDescriptor
    message = _descriptor_message(
        _duplicate(("T-2", "20260730", "09:30:00")),
        {
            "trip_id": "T-2",
            "start_date": "20260730",
            "relationship": descriptor.CANCELED,
        },
    )
    board, _ = _run_feeds(_DIRECTED, _boards_and_journeys(), [message])
    assert {row[0] for row in board} == {"T", "U", "X"}
    assert not any(row[5] for row in board)


def test_a_duplicate_of_a_repetition_copies_that_repetition() -> None:
    """A frequency trip's run is named by its start_time: the copy is that
    repetition's calls at the copy's start."""
    files = _feed(
        frequencies=(
            "trip_id,start_time,end_time,headway_secs\nT,08:00:00,09:00:00,1800\n"
        ),
    )
    message = _descriptor_message(
        {**_duplicate(("T-2", "20260730", "10:15:00")), "start_time": "08:30:00"}
    )
    board, _ = _run_feeds(files, _boards_and_journeys(), [message])
    assert [row[:3] for row in board if row[0] == "T-2"] == [
        ("T-2", "S0", _at(135)),
        ("T-2", "S1", _at(145)),
    ]


def test_a_duplicate_missing_its_copys_identity_is_ignored() -> None:
    """TripProperties' trip_id, start_date and start_time are all "required
    if schedule_relationship is DUPLICATED": without one there is no copy,
    and the original is untouched either way."""
    for copy in (
        ("", "20260730", "09:30:00"),
        ("T-2", "", "09:30:00"),
        ("T-2", "20260730", ""),
    ):
        board, _ = _run_feeds(
            _DIRECTED, _boards_and_journeys(), [_descriptor_message(_duplicate(copy))]
        )
        assert {row[0] for row in board} == {"T", "U", "X"}, copy
        assert all(row[3] is row[4] is None for row in board)


def test_a_duplicate_keeps_its_originals_pickup_rules() -> None:
    """A copy is the original's stop list with its descriptors: no journey
    boards it where the original allows no pickup."""
    files = _feed(
        stop_times=(
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type\n"
            "T,08:00:00,08:00:00,S0,1,1\nT,08:10:00,08:10:00,S1,2,0\n"
        ),
    )
    message = _descriptor_message(_duplicate(("T-2", "20260730", "09:30:00")))
    board, (s0_s1,) = _run_feeds(files, _boards_and_journeys(("S0", "S1")), [message])
    assert {row[0] for row in board} == {"T", "T-2"}  # still shown at S0
    assert s0_s1 == []


def test_cancelling_the_original_leaves_its_copy() -> None:
    """ "If a producer wants to cancel the original trip, it must publish a
    separate TripUpdate with the value of CANCELED": the copy still runs."""
    message = _descriptor_message(
        _duplicate(("T-2", "20260730", "09:30:00")),
        {
            "trip_id": "T",
            "start_date": "20260730",
            "relationship": gtfs_realtime_pb2.TripDescriptor.CANCELED,
        },
    )
    board, _ = _run_feeds(_DIRECTED, _boards_and_journeys(), [message])
    assert {(row[0], row[5]) for row in board if row[0] in {"T", "T-2"}} == {
        ("T", True),
        ("T-2", False),
    }


def test_a_duplicate_keeps_its_originals_drop_off_rules() -> None:
    """No journey alights a copy where the original allows no drop-off."""
    files = _feed(
        stop_times=(
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence,drop_off_type\n"
            "T,08:00:00,08:00:00,S0,1,0\nT,08:10:00,08:10:00,S1,2,1\n"
        ),
    )
    message = _descriptor_message(_duplicate(("T-2", "20260730", "09:30:00")))
    _, (s0_s1,) = _run_feeds(files, _boards_and_journeys(("S0", "S1")), [message])
    assert s0_s1 == []


def test_copied_run_names_one_run_and_moves_it() -> None:
    """copied_run: a plain trip (by no start, or its own first departure)
    or a repetition (by its start), moved to the copy's start on any day;
    a start naming no run names nothing."""
    files = _feed(
        trips=(
            "route_id,service_id,trip_id,trip_headsign,direction_id\n"
            "R,S,T,Out,0\nR,S,F,Out,0\n"
        ),
        stop_times=(
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "T,08:00:00,08:00:00,S0,1\nT,08:10:00,08:10:00,S1,2\n"
            "F,09:00:00,09:00:00,S0,1\nF,09:10:00,09:10:00,S1,2\n"
        ),
        frequencies="trip_id,start_time,end_time,headway_secs\nF,09:00:00,10:00:00,1800\n",
    )
    index = _build_index_from_files(files)
    day = date(2026, 7, 30)
    try:
        for source, secs, copy, first in (
            ("T", None, 37800, ("T", _at(150))),
            ("T", 28800, 37800, ("T", _at(150))),
            ("F", 34200, 37800, ("F#34200", _at(150))),
        ):
            run = index.copied_run(source, secs, day, copy)
            assert run is not None
            assert (run.trip_id, run.calls[0].departure) == first
            assert run.calls[1].arrival == first[1] + timedelta(minutes=10)
        assert index.copied_run("T", 28900, day, 37800) is None
        assert index.copied_run("F", None, day, 37800) is None
        assert index.copied_run("F", 33000, day, 37800) is None
        assert index.copied_run("NOPE", None, day, 37800) is None
    finally:
        index.close()


_DWELL = _feed(
    stops="stop_id,stop_name,stop_lat,stop_lon\nS0,A,0,0\nS1,B,0,0\nS2,C,0,0\n",
    stop_times=(
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        "T,08:00:00,08:00:00,S0,1\nT,08:10:00,08:15:00,S1,2\n"
        "T,08:25:00,08:25:00,S2,3\n"
    ),
)


def _copy_at_s1(stops: list[dict[str, Any]]) -> list[tuple[object, ...]]:
    """The copy of T at 09:30 (S1 due 09:40, leaving 09:45): its rows."""
    message = _descriptor_message(
        _duplicate(("T-2", "20260730", "09:30:00"), stops=stops)
    )

    async def body(handle: TransitFeedHandle) -> list[tuple[object, ...]]:
        start = _EIGHT - timedelta(minutes=30)
        [board] = await handle.get_arrivals(
            [ArrivalsQuery(["S0", "S1", "S2"], limit=None)],
            start=start,
            end=start + timedelta(hours=3),
        )
        return [
            (
                r.stop_id,
                r.scheduled_arrival,
                r.scheduled_departure,
                r.predicted_departure,
                r.delay_seconds,
            )
            for r in board
            if r.trip_id == "T-2"
        ]

    return _run_feeds(_DWELL, body, [message])


def test_a_new_trip_and_a_copy_sharing_an_id_are_one_trip() -> None:
    """An extra trip is its id, whatever its kind: a NEW trip and a copy
    under one trip_id are one trip, the last sent."""
    descriptor = gtfs_realtime_pb2.TripDescriptor
    new = {
        "trip_id": "N",
        "start_date": "20260730",
        "relationship": descriptor.NEW,
        "calls": [("S0", None, _at(60), False)],
    }
    copy = _duplicate(("N", "20260730", "09:30:00"))
    for parts, expected in (
        ([new, copy], [("N", _at(90))]),
        ([copy, new], [("N", None)]),
    ):
        board = _run_feeds(
            _DIRECTED,
            _board(_EIGHT - timedelta(minutes=30)),
            [_descriptor_message(*parts)],
        )
        assert [
            (r.trip_id, r.scheduled_departure) for r in board if r.trip_id == "N"
        ] == expected


def test_an_added_paired_with_its_duplicate_is_ignored() -> None:
    """The ADDED-to-DUPLICATED migration guide: a producer may send both,
    and consumers ignore the ADDED whose trip_id is the DUPLICATED's
    trip_id (the original) or its TripProperties.trip_id (the copy) --
    whichever comes first. One bus is listed once, on its copy schedule."""
    descriptor = gtfs_realtime_pb2.TripDescriptor
    copy = _duplicate(("T-2", "20260730", "09:30:00"))
    for added_id in ("T", "T-2"):
        added = {
            "trip_id": added_id,
            "start_date": "20260730",
            "relationship": descriptor.ADDED,
            "calls": [("S0", None, _at(90), False), ("S1", _at(100), None, False)],
        }
        for parts in ([added, copy], [copy, added]):
            board = _run_feeds(
                _DIRECTED,
                _board(_EIGHT - timedelta(minutes=30)),
                [_descriptor_message(*parts)],
            )
            assert [
                (r.trip_id, r.scheduled_departure, r.predicted_departure)
                for r in board
                if r.predicted_departure is not None or r.trip_id == "T-2"
            ] == [("T-2", _at(90), None)], (added_id, parts[0]["relationship"])


def test_a_copy_stating_its_own_times_is_its_updates_alone() -> None:
    """An update stating scheduled_time -- "if a duplicated trip runs have
    different run or dwell times compared to the original" -- states the
    copy's own schedule: the copy is then its updates alone, as a NEW trip
    is, never a mix of their times and the original's. A copy stated to
    run faster is on time where it says so."""
    rows = _copy_at_s1(
        [
            {"stop": "S0", "scheduled_departure": _at(90)},
            {"stop": "S1", "scheduled_arrival": _at(95)},
            {"stop": "S2", "scheduled_arrival": _at(100), "arrival": _at(100)},
        ]
    )
    assert rows == [
        ("S0", _at(90), _at(90), None, None),
        ("S1", _at(95), _at(95), None, None),
        ("S2", _at(100), _at(100), None, 0),
    ]
    # Only the stops its updates state.
    assert [
        row[0] for row in _copy_at_s1([{"stop": "S1", "scheduled_arrival": _at(101)}])
    ] == ["S1"]


def test_a_moved_copys_skipped_and_no_data_stops() -> None:
    """On its original's moved schedule, a stop the copy skips is not shown,
    and a NO_DATA stop cuts its delay from carrying on."""
    assert [row[0] for row in _copy_at_s1([{"stop": "S1", "skipped": True}])] == [
        "S0",
        "S2",
    ]
    assert _copy_at_s1(
        [
            {"stop": "S0", "departure_delay": 120},
            {"stop": "S1", "no_data": True},
        ]
    ) == [
        ("S0", _at(90), _at(90), _at(92), 120),
        ("S1", _at(100), _at(105), None, None),
        ("S2", _at(115), _at(115), None, None),
    ]


def test_a_copys_trip_short_name_may_differ() -> None:
    """TripProperties.trip_short_name: "the name for this trip when it
    differs from the original" -- a copy's own train number."""
    files = _feed(
        trips=(
            "route_id,service_id,trip_id,trip_headsign,trip_short_name\nR,S,T,Out,101\n"
        ),
    )
    message = _descriptor_message(_duplicate(("T-2", "20260730", "09:30:00")))
    message.entity[0].trip_update.trip_properties.trip_short_name = "901"
    board = _run_feeds(files, _board(_EIGHT - timedelta(minutes=30)), [message])
    assert {r.trip_id: r.trip_short_name for r in board} == {"T": "101", "T-2": "901"}


def test_a_copy_stating_times_keeps_its_originals_stops_and_rules() -> None:
    """A copy stating its own times still runs its original's stopping
    pattern: each stated call is the original's stop, descriptors and all,
    so no journey boards it where the original allows no pickup; and
    updates naming stops by stop_sequence alone still name them."""
    files = _feed(
        stop_times=(
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type\n"
            "T,08:00:00,08:00:00,S0,1,1\nT,08:10:00,08:10:00,S1,2,0\n"
        ),
    )
    stated = [
        {"stop": "S0", "scheduled_departure": _at(90)},
        {"stop": "S1", "scheduled_arrival": _at(100)},
    ]
    message = _descriptor_message(
        _duplicate(("T-2", "20260730", "09:30:00"), stops=stated)
    )
    board, (s0_s1,) = _run_feeds(files, _boards_and_journeys(("S0", "S1")), [message])
    assert [row[:3] for row in board if row[0] == "T-2"] == [
        ("T-2", "S0", _at(90)),
        ("T-2", "S1", _at(100)),
    ]
    assert s0_s1 == []
    by_sequence = _descriptor_message(_duplicate(("T-2", "20260730", "09:30:00")))
    for sequence, minutes, event in ((1, 90, "departure"), (2, 100, "arrival")):
        stu = by_sequence.entity[0].trip_update.stop_time_update.add()
        stu.stop_sequence = sequence
        getattr(stu, event).scheduled_time = int(_at(minutes).timestamp())
    board, _ = _run_feeds(_DIRECTED, _boards_and_journeys(), [by_sequence])
    assert [row[:3] for row in board if row[0] == "T-2"] == [
        ("T-2", "S0", _at(90)),
        ("T-2", "S1", _at(100)),
    ]


def test_the_migration_guides_added_rules_apply_only_where_due() -> None:
    """An ADDED is ignored for a DUPLICATED only when that DUPLICATED names
    a copy; and, per the same guide, for a NEW trip with its trip_id."""
    descriptor = gtfs_realtime_pb2.TripDescriptor
    added = {
        "trip_id": "N",
        "start_date": "20260730",
        "relationship": descriptor.ADDED,
        "calls": [("S0", None, _at(61), False)],
    }
    broken_copy = _duplicate(("N", "20260730", ""))  # no start_time: no copy
    new = {
        "trip_id": "N",
        "start_date": "20260730",
        "relationship": descriptor.NEW,
        "headsign": "NewSign",
        "calls": [("S0", None, _at(60), False)],
    }
    start = _EIGHT - timedelta(minutes=30)
    for parts, expected in (
        ([added, broken_copy], [("N", _at(61), None)]),
        ([new, added], [("N", _at(60), "NewSign")]),
        ([added, new], [("N", _at(60), "NewSign")]),
    ):
        board = _run_feeds(_DIRECTED, _board(start), [_descriptor_message(*parts)])
        assert [
            (r.trip_id, r.predicted_departure, r.headsign)
            for r in board
            if r.trip_id == "N"
        ] == expected
