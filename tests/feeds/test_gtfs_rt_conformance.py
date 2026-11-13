"""GTFS-RT TripUpdates conformance: how StopTimeUpdates resolve.

Each test quotes the rule it pins. The resolver's state machine is
covered piecewise in ``test_rt`` and by the propagation oracles in
``test_properties``; these are the end-to-end consequences and the laws
the spec states outright.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta

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
    [added] = trip_updates_from_message(message).added
    assert (added.trip_id, added.stop_id, added.departure) == ("", "S0", _EIGHT)
