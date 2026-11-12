"""GTFS-RT TripUpdates conformance: how StopTimeUpdates resolve.

Each test quotes the rule it pins. The resolver's state machine is
covered piecewise in ``test_rt`` and by the propagation oracles in
``test_properties``; these are the end-to-end consequences and the laws
the spec states outright.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from google.transit import gtfs_realtime_pb2
from hypothesis import given, settings
from hypothesis import strategies as st

from aiomobilitydatabase.feeds.models import ArrivalsQuery
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
        "S0": (_EIGHT + five, _EIGHT + five),
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
