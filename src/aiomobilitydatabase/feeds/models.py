"""Public snapshot models returned by feed handles."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import IntEnum, StrEnum
from typing import Any

from ._frozen import FrozenDict


def _freeze_mappings(model: object, *names: str) -> None:
    """Store each named mapping field as a :class:`FrozenDict`.

    Library-built models already hold one; this covers models a consumer
    builds or ``dataclasses.replace``s with a plain ``dict`` (a test
    fixture, say), which would otherwise be mutable and unhashable.
    """
    for name in names:
        value = getattr(model, name)
        if value is not None and not isinstance(value, FrozenDict):
            object.__setattr__(model, name, FrozenDict(value))


def _effective_departure(
    predicted_departure: datetime | None,
    scheduled_departure: datetime | None,
    predicted_arrival: datetime | None,
) -> datetime | None:
    """Return the predicted departure, else the later of the other two."""
    if predicted_departure is not None:
        return predicted_departure
    known = [
        when for when in (scheduled_departure, predicted_arrival) if when is not None
    ]
    return max(known) if known else None


class WheelchairAccess(IntEnum):
    """Wheelchair support, as GTFS's shared 0/1/2 vocabulary.

    One enum for BOTH ``trips.wheelchair_accessible`` (can the vehicle carry
    a rider in a wheelchair?) and ``stops.wheelchair_boarding`` (is boarding
    possible at the stop?): the spec defines the identical value shape for
    the two columns, and POSSIBLE/NOT_POSSIBLE reads naturally in either
    position. Values match the spec ints, so ``int(...)``/``.value``
    comparisons keep working for consumers who want the raw number. A
    blank or absent cell is UNKNOWN, GTFS's "0 or empty".
    """

    UNKNOWN = 0
    POSSIBLE = 1
    NOT_POSSIBLE = 2


class BikesAllowed(IntEnum):
    """GTFS ``trips.bikes_allowed`` vocabulary (values match the spec ints).

    A blank or absent cell is UNKNOWN, GTFS's "0 or empty".
    """

    UNKNOWN = 0
    ALLOWED = 1
    NOT_ALLOWED = 2


class PickupDropOffType(IntEnum):
    """GTFS ``stop_times.pickup_type``/``drop_off_type`` shared vocabulary.

    The spec defines the identical value shape for both columns, so one
    enum serves the two fields (values match the spec ints). A blank or
    absent cell is REGULAR, GTFS's "0 or empty".
    """

    REGULAR = 0
    NONE = 1
    PHONE_AGENCY = 2
    COORDINATE_WITH_DRIVER = 3


class StopLocationType(IntEnum):
    """GTFS ``stops.location_type`` vocabulary (values match the spec ints).

    Named ``StopLocationType`` because the catalog-side
    :class:`aiomobilitydatabase.models.LocationType` (a StrEnum of catalog
    location kinds) is an unrelated vocabulary. A blank or absent cell is
    STOP, GTFS's "0 (or empty)".
    """

    STOP = 0
    STATION = 1
    ENTRANCE_EXIT = 2
    GENERIC_NODE = 3
    BOARDING_AREA = 4


class AlertCause(StrEnum):
    """GTFS-RT ``Alert.Cause`` vocabulary.

    Member values are the protobuf enum NAMES (the strings the previous
    untyped surface exposed), so ``alert.cause == "CONSTRUCTION"`` keeps
    working while ``alert.cause is AlertCause.CONSTRUCTION`` becomes
    available. A protobuf value this library doesn't know (a future spec
    addition) degrades to None rather than raising -- same leniency as
    every other closed vocabulary at the model boundary.
    """

    UNKNOWN_CAUSE = "UNKNOWN_CAUSE"
    OTHER_CAUSE = "OTHER_CAUSE"
    TECHNICAL_PROBLEM = "TECHNICAL_PROBLEM"
    STRIKE = "STRIKE"
    DEMONSTRATION = "DEMONSTRATION"
    ACCIDENT = "ACCIDENT"
    HOLIDAY = "HOLIDAY"
    WEATHER = "WEATHER"
    MAINTENANCE = "MAINTENANCE"
    CONSTRUCTION = "CONSTRUCTION"
    POLICE_ACTIVITY = "POLICE_ACTIVITY"
    MEDICAL_EMERGENCY = "MEDICAL_EMERGENCY"
    SPECIAL_EVENT = "SPECIAL_EVENT"


class AlertEffect(StrEnum):
    """GTFS-RT ``Alert.Effect`` vocabulary (values are the protobuf names)."""

    NO_SERVICE = "NO_SERVICE"
    REDUCED_SERVICE = "REDUCED_SERVICE"
    SIGNIFICANT_DELAYS = "SIGNIFICANT_DELAYS"
    DETOUR = "DETOUR"
    ADDITIONAL_SERVICE = "ADDITIONAL_SERVICE"
    MODIFIED_SERVICE = "MODIFIED_SERVICE"
    OTHER_EFFECT = "OTHER_EFFECT"
    UNKNOWN_EFFECT = "UNKNOWN_EFFECT"
    STOP_MOVED = "STOP_MOVED"
    NO_EFFECT = "NO_EFFECT"
    ACCESSIBILITY_ISSUE = "ACCESSIBILITY_ISSUE"


class AlertSeverity(StrEnum):
    """GTFS-RT ``Alert.SeverityLevel`` vocabulary (protobuf names)."""

    UNKNOWN_SEVERITY = "UNKNOWN_SEVERITY"
    INFO = "INFO"
    WARNING = "WARNING"
    SEVERE = "SEVERE"


class VehicleStopStatus(StrEnum):
    """GTFS-RT ``VehiclePosition.VehicleStopStatus`` vocabulary (protobuf names).

    Describes the vehicle's relationship to its current stop
    (:attr:`VehiclePosition.stop_id` / ``current_stop_sequence``). The
    protobuf field defaults to IN_TRANSIT_TO; see
    :attr:`VehiclePosition.current_status` for exactly when that default is
    surfaced versus None.
    """

    INCOMING_AT = "INCOMING_AT"
    STOPPED_AT = "STOPPED_AT"
    IN_TRANSIT_TO = "IN_TRANSIT_TO"


class CongestionLevel(StrEnum):
    """GTFS-RT ``VehiclePosition.CongestionLevel`` vocabulary (protobuf names)."""

    UNKNOWN_CONGESTION_LEVEL = "UNKNOWN_CONGESTION_LEVEL"
    RUNNING_SMOOTHLY = "RUNNING_SMOOTHLY"
    STOP_AND_GO = "STOP_AND_GO"
    CONGESTION = "CONGESTION"
    SEVERE_CONGESTION = "SEVERE_CONGESTION"


class OccupancyStatus(StrEnum):
    """GTFS-RT ``VehiclePosition.OccupancyStatus`` vocabulary (protobuf names)."""

    EMPTY = "EMPTY"
    MANY_SEATS_AVAILABLE = "MANY_SEATS_AVAILABLE"
    FEW_SEATS_AVAILABLE = "FEW_SEATS_AVAILABLE"
    STANDING_ROOM_ONLY = "STANDING_ROOM_ONLY"
    CRUSHED_STANDING_ROOM_ONLY = "CRUSHED_STANDING_ROOM_ONLY"
    FULL = "FULL"
    NOT_ACCEPTING_PASSENGERS = "NOT_ACCEPTING_PASSENGERS"
    NO_DATA_AVAILABLE = "NO_DATA_AVAILABLE"
    NOT_BOARDABLE = "NOT_BOARDABLE"


@dataclass(frozen=True)
class Stop:
    """A transit stop from the static GTFS index.

    ``description``/``url``/``zone_id`` are the verbatim ``stop_desc``/
    ``stop_url``/``zone_id`` cells (empty -> None). ``wheelchair_boarding``
    and ``timezone`` are the values GTFS resolves for the stop, not its raw
    cells: a stop under a parent station takes the parent's
    ``wheelchair_boarding`` when its own is 0 or empty, and always takes
    the parent's timezone; a stop without one uses its ``stop_timezone``,
    or the agency's when that is empty. ``timezone`` is DISPLAY METADATA
    ONLY: every stop_times value is expressed in the AGENCY timezone
    regardless of any stop_timezone, so no time computation in this library
    reads it -- it exists so consumers can render "local time at this
    stop". It is None only for a stop a realtime feed defines without one.
    """

    id: str
    name: str | None
    latitude: float | None
    longitude: float | None
    parent_station: str | None
    location_type: StopLocationType | None
    stop_code: str | None
    platform_code: str | None
    wheelchair_boarding: WheelchairAccess | None
    description: str | None
    url: str | None
    zone_id: str | None
    timezone: str | None


@dataclass(frozen=True)
class Agency:
    """A transit agency from the static GTFS index (agency.txt).

    ``id`` is None for single-agency feeds that omit the optional
    ``agency_id`` column. Route rows reference agencies through
    :attr:`Route.agency_id`.
    """

    id: str | None
    name: str | None
    url: str | None
    timezone: str | None
    lang: str | None
    phone: str | None
    fare_url: str | None
    email: str | None


@dataclass(frozen=True)
class StationGroup:
    """A logical station: boarding stops grouped for presentation.

    Boarding stops sharing a GTFS ``parent_station`` form one group; a
    stop without one is a group of its own. ``id`` is the parent station id,
    or the stop's own id; it is stable across rebuilds of the same feed.
    """

    id: str
    name: str
    stop_ids: tuple[str, ...]


@dataclass(frozen=True)
class Route:
    """A transit route from the static GTFS index.

    ``type`` stays a raw int (not an enum): Google's extended route types
    (e.g. 103) are an open vocabulary a closed enum could not represent.
    ``color``/``text_color`` are the raw GTFS hex strings without a leading
    ``#`` (e.g. ``"FFD700"``), defaulting to ``"FFFFFF"``/``"000000"`` when
    omitted or empty, as the spec does; ``agency_id`` resolves against
    :class:`Agency` records exposed on the handle/index. ``description``
    is the verbatim ``route_desc`` cell; ``sort_order`` is
    ``route_sort_order`` parsed leniently (blank/garbage -> None, any
    parseable int kept verbatim -- an open ordering key, not a closed
    vocabulary).
    """

    id: str
    short_name: str | None
    long_name: str | None
    type: int | None
    agency_id: str | None
    color: str | None
    text_color: str | None
    url: str | None
    description: str | None
    sort_order: int | None

    @property
    def display_name(self) -> str:
        """Best human-readable name for the route."""
        if self.short_name and self.long_name:
            return f"{self.short_name} {self.long_name}"
        return self.short_name or self.long_name or self.id


@dataclass(frozen=True)
class ArrivalsQuery:
    """One stop-board request inside a :meth:`TransitFeedHandle.get_arrivals` batch.

    ``stop_ids`` is the set of GTFS stops the board covers (a station's
    platforms, a direction pair). ``route_ids`` and ``headsigns`` narrow
    the rows; ``None`` or an empty list means no narrowing. ``headsigns``
    matches the headsign shown at the stop -- the stop_time's
    ``stop_headsign`` where set, else the trip's headsign; the same values
    :meth:`TransitFeedHandle.get_headsigns_serving` offers -- so a realtime-only
    trip passes one only by the headsign its update gives
    (``TripProperties.trip_headsign``, or a replaced trip's own), or, for a
    DUPLICATED copy with no headsign of its own, by its original's trip and
    stop headsigns.
    ``limit``
    caps the merged, filtered result for this query as a whole,
    nearest effective departure first; zero asks for an empty board and
    ``None`` means unlimited (only allowed with an explicit ``end``).
    """

    stop_ids: Sequence[str]
    route_ids: Sequence[str] | None = None
    headsigns: Sequence[str] | None = None
    limit: int | None = 10

    def __post_init__(self) -> None:
        """Validate, then copy each id sequence into a tuple.

        The copy is what makes the frozen query actually immutable: storing
        the caller's list by reference would let a later ``append`` on that
        list change a query already handed to the library.
        """
        for name in ("stop_ids", "route_ids", "headsigns"):
            value = getattr(self, name)
            if isinstance(value, str):
                raise TypeError(f"{name} must be a sequence of ids, not a str")
            if value is not None:
                object.__setattr__(self, name, tuple(value))
        # A negative limit would slice rows off the END of the board.
        if self.limit is not None and self.limit < 0:
            raise ValueError(f"limit must be non-negative, got {self.limit}")


@dataclass(frozen=True)
class TripQuery:
    """One origin->destination request in a ``get_upcoming_trips`` batch.

    ``limit`` caps this query's result, nearest effective origin departure
    first; ``None`` means unlimited (only allowed with an explicit ``end``)
    and zero asks for an empty result.
    """

    origin_stop_id: str
    destination_stop_id: str
    limit: int | None = 10

    def __post_init__(self) -> None:
        """Reject a negative limit, which would slice rows off the end."""
        if self.limit is not None and self.limit < 0:
            raise ValueError(f"limit must be non-negative, got {self.limit}")


@dataclass(frozen=True)
class StopArrival:
    """An upcoming (or realtime-added) arrival/departure at a stop.

    The trailing descriptive fields come from the static schedule:
    ``wheelchair_accessible``/``bikes_allowed`` describe the trip
    (``wheelchair_accessible`` as its TripUpdate's VehicleDescriptor
    states it where it does, which "will overwrite the value from the
    GTFS"; an attached ``vehicle`` carries its own),
    ``pickup_type``/``drop_off_type``/``timepoint_exact``/``stop_headsign``
    describe this stop's stop_time row (``stop_headsign`` is the per-stop
    override of the trip-level ``headsign``). ``timepoint_exact`` is True
    when the scheduled times are exact (GTFS's default when the column is
    absent or blank), False for approximate times -- including every
    repetition of headway-based (``exact_times`` 0) frequency service, which
    "does not follow a fixed schedule" -- and None when the value is outside
    the 0/1 vocabulary. A call the feed times by only its arrival or only
    its departure reports that time as both. ``trip_short_name``/``block_id`` are the
    trip's verbatim ``trip_short_name``/``block_id`` cells -- ``block_id``
    is raw string exposure only (no block-continuation computation).
    ``vehicle`` is the live position of the vehicle serving this call,
    present only when the query asked for vehicles AND exactly one
    vehicle could be pinned to the row; it stays None when the feed
    publishes no positions, when none matches, or when the match would be
    a guess. ``vehicle_id`` is the id the TripUpdates feed named, and is
    independent of whether a position for it was found.
    ``stop`` and ``route`` are the whole static records rather than a
    handful of copied columns, so platform codes, coordinates, route
    colours and the rest are reachable without a second lookup; use
    ``route.display_name``/``stop.name`` for the human-readable strings.
    ``stop_id``/``route_id`` stay as the raw ids a feed referenced, which
    is what RT rows and filters key on, and a ``route_id`` naming a route
    absent from routes.txt leaves ``route`` None rather than inventing
    one. ``direction_id`` stays a raw int (0/1 with feed-defined meaning),
    matching :attr:`UpcomingTrip.direction_id`.
    Rows of a realtime-only trip (NEW, ADDED, or a REPLACEMENT
    run, whose static schedule "isn't used") have no stop_times row: their
    scheduled times are the update's own ``scheduled_time`` (None where it
    gives none) and their predictions its times. ``service_id``, the
    stop_time descriptors, ``bikes_allowed`` and ``block_id`` are None,
    and ``wheelchair_accessible`` too unless the update states it (a
    replacement's is its trip's unless the update states one);
    ``route_id``, ``direction_id``,
    ``headsign`` and ``trip_short_name`` are what the update gives
    (TripProperties for the last two) -- a replacement shows under the
    trip it replaces (its ``trip_id``, a frequency repetition's own -- a
    headway-based run's, its own start's -- route
    and direction; its headsign where the update gives none). A
    DUPLICATED copy is its original moved to the copy's start: its own
    ``trip_id`` (``TripProperties.trip_id``), the original's schedule
    shifted -- or, where its updates state ``scheduled_time``, those
    updates' stops and times -- and the original's descriptors (headsign
    and short name unless TripProperties gives them), but no
    ``service_id`` and no ``block_id``: a block is the vehicle's sequence
    of scheduled trips, which a copy at another time is not part of. A
    realtime run of headway-based (``exact_times`` 0) frequency service is
    built the same way: its trip moved to the run's own start, under the
    ``{trip_id}#{start_secs}`` id of that start, in place of the repetition
    whose headway slot holds it; it keeps its trip's ``service_id``, only
    its updates' times predict it, and its ``delay_seconds`` is None (such
    service "does not follow a fixed schedule"). A run a TripModifications
    detour rebuilds is shown the same way, as realtime's: still its own
    trip -- id, descriptors, ``service_id`` and ``block_id`` -- on its
    rebuilt calls, a replacement stop with stop_times' defaults and no
    prediction, and a kept stop's ``delay_seconds`` only where the detour
    left its schedule alone.

    ``predicted_arrival``/``predicted_departure`` are each event's own
    prediction; ``delay_seconds`` is the delay realtime gives this stop --
    its own update's departure delay, else its arrival delay (an event's
    time against the schedule, else its delay; None if the update states
    neither), or, for a stop with no update of its own, the delay
    propagated from an earlier stop or the trip-level one. With different
    arrival and
    departure delays it is the departure's, so read the arrival's lateness
    off ``predicted_arrival``, not ``delay_seconds``. An event a stop's own
    update does not give is not predicted (None), even when the other event
    is; :attr:`effective_departure` is when the vehicle leaves as the board
    orders it.

    ``canceled`` marks a scheduled run realtime says will not operate
    (GTFS-RT CANCELED): the row stays on the board, at its scheduled time,
    so riders see the cancellation, with no prediction and no vehicle;
    ``realtime`` is True, a cancellation being realtime information. A run
    realtime DELETES "must not be shown to users" and has no row at all.

    ``alerts`` are the service alerts that apply to this row, only with
    ``with_alerts``: those with a selector matching its run (its trip, the
    repetition's start or a plain trip's first departure, and service
    day), route, route type, agency, direction, and this stop or its
    parent station (see :meth:`ServiceAlert.applies_to`). An alert's trip
    names its run as a TripUpdate's does: without a trip_id, the one trip
    its route, direction and start resolve to; a plain trip without a
    date, the run its trip_id names at the alert feed's time. Every period
    is kept: :meth:`ServiceAlert.is_active` says which apply now.
    """

    stop_id: str
    stop: Stop | None
    route_id: str | None
    route: Route | None
    trip_id: str | None
    service_id: str | None
    headsign: str | None
    scheduled_arrival: datetime | None
    scheduled_departure: datetime | None
    predicted_arrival: datetime | None
    predicted_departure: datetime | None
    delay_seconds: int | None
    realtime: bool
    canceled: bool
    vehicle_id: str | None
    vehicle: VehiclePosition | None
    wheelchair_accessible: WheelchairAccess | None
    bikes_allowed: BikesAllowed | None
    direction_id: int | None
    pickup_type: PickupDropOffType | None
    drop_off_type: PickupDropOffType | None
    timepoint_exact: bool | None
    stop_headsign: str | None
    trip_short_name: str | None
    block_id: str | None
    alerts: tuple[ServiceAlert, ...] = ()

    @property
    def effective_departure(self) -> datetime | None:
        """When the vehicle leaves this stop, as boards are ordered and kept.

        The predicted departure if realtime gives one; else the later of
        the scheduled departure and the predicted arrival -- a vehicle does
        not leave before it arrives, so a late terminus whose update gives
        only its arrival is shown at that arrival. None for a row with no
        time at all.
        """
        return _effective_departure(
            self.predicted_departure, self.scheduled_departure, self.predicted_arrival
        )


@dataclass(frozen=True)
class UpcomingTrip:
    """An upcoming origin-to-destination journey on one trip.

    The shape Home Assistant's legacy ``gtfs`` sensor needs: the next
    departure from an origin stop on a trip that later serves the
    destination stop. A scheduled trip always has both scheduled times; a
    realtime-only trip (NEW, or a REPLACEMENT run, whose static
    schedule "isn't used") has only the ones its update gives
    (``scheduled_time``), a DUPLICATED copy or a run of headway-based
    frequency service its trip's moved to its start (a copy's updates'
    own, where they state ``scheduled_time``), and its other fields as on
    :class:`StopArrival`.
    Realtime predicts the events its updates give:
    ``predicted_departure``/``origin_predicted_arrival`` at the origin and
    ``predicted_arrival`` at the destination, each None when realtime says
    nothing about that event (a feed sending only arrivals predicts no
    departure). ``delay_seconds`` is the origin's delay -- its update's
    departure delay, else its arrival delay (None if the origin's own
    update states neither); an origin with no update of its own takes the
    carried or trip-level delay. A trip
    is offered only where the rider can board and alight: never from a call
    with no pickup (``pickup_type`` 1) or to one with no drop-off
    (``drop_off_type`` 1).

    ``wheelchair_accessible``/``bikes_allowed``/``direction_id`` describe
    the trip (``wheelchair_accessible`` overridden by realtime as on
    :class:`StopArrival`; ``direction_id`` stays a raw int: 0/1 with
    feed-defined meaning), as do ``trip_short_name``/``block_id`` (verbatim cells;
    ``block_id`` is raw string exposure only -- no block-continuation
    computation). The ``origin_*``/``destination_*`` descriptors come from the
    stop_time rows at each end, with the same semantics as the matching
    :class:`StopArrival` fields (``timepoint_exact`` defaults to True when
    the GTFS column is absent or blank).

    ``route``/``origin_stop``/``destination_stop`` are the whole static
    records, matching :class:`StopArrival`: route colours, platform codes
    and coordinates are reachable without a second lookup, and
    ``route.display_name``/``stop.name`` give the human-readable strings.
    The ``*_stop_id`` fields stay as the ids the query named, which is
    what callers passed in and what RT rows key on.

    ``alerts`` are those that apply to the journey, only with
    ``with_alerts``: as on :class:`StopArrival`, at the origin or the
    destination.

    ``is_first``/``is_last`` mark whether this departure is the first/last
    scheduled departure OF ITS SERVICE DAY for this origin->destination
    pair -- legacy ``gtfs`` sensor parity; they describe the static
    schedule, so a realtime-only trip is neither. A
    past-midnight departure (>24:00:00) is flagged relative to the
    service day it belongs to, not the clock day it happens on, and
    frequency-materialized repetitions count as ordinary trips (the day's
    first repetition is the first departure).

    ``vehicle_id``/``vehicle`` identify the vehicle running this trip, as
    on :class:`StopArrival`: the id from the origin's realtime prediction,
    and the live :class:`VehiclePosition` when the query asked for
    vehicles and one could be pinned to this trip instance.

    ``canceled`` marks a run realtime says will not operate, kept and
    ordered at its scheduled departure with no prediction and no vehicle,
    as on :class:`StopArrival`; it counts toward a query's ``limit``.
    """

    trip_id: str
    route_id: str | None
    service_id: str | None
    route: Route | None
    headsign: str | None
    origin_stop_id: str
    origin_stop: Stop | None
    destination_stop_id: str
    destination_stop: Stop | None
    scheduled_departure: datetime | None
    predicted_departure: datetime | None
    origin_predicted_arrival: datetime | None
    scheduled_arrival: datetime | None
    predicted_arrival: datetime | None
    delay_seconds: int | None
    realtime: bool
    canceled: bool
    vehicle_id: str | None
    vehicle: VehiclePosition | None
    wheelchair_accessible: WheelchairAccess | None
    bikes_allowed: BikesAllowed | None
    direction_id: int | None
    origin_pickup_type: PickupDropOffType | None
    origin_drop_off_type: PickupDropOffType | None
    origin_timepoint_exact: bool | None
    origin_stop_headsign: str | None
    destination_pickup_type: PickupDropOffType | None
    destination_drop_off_type: PickupDropOffType | None
    destination_timepoint_exact: bool | None
    destination_stop_headsign: str | None
    is_first: bool
    is_last: bool
    trip_short_name: str | None
    block_id: str | None
    alerts: tuple[ServiceAlert, ...] = ()

    @property
    def effective_departure(self) -> datetime | None:
        """When the trip leaves its origin, as journeys are ordered and kept.

        The predicted departure if realtime gives one; else the later of
        the scheduled departure and the origin's predicted arrival -- a
        vehicle does not leave before it arrives. Never None for a journey
        a query returns.
        """
        return _effective_departure(
            self.predicted_departure,
            self.scheduled_departure,
            self.origin_predicted_arrival,
        )


@dataclass(frozen=True)
class CarriageDetail:
    """One carriage of a multi-carriage vehicle (GTFS-RT 2.1).

    ``occupancy_percentage`` is -1 in the protobuf when unset; that
    sentinel becomes None here rather than surfacing as a negative
    percentage. An unset ``occupancy_status`` is the proto's declared
    default, NO_DATA_AVAILABLE (None only for a value outside the
    vocabulary). ``carriage_sequence`` numbers the carriages 1 to n from
    the vehicle's front: a vehicle whose carriages are not numbered so
    reports none (the spec says to discard them all).
    """

    id: str | None
    label: str | None
    occupancy_status: OccupancyStatus | None
    occupancy_percentage: int | None
    carriage_sequence: int


@dataclass(frozen=True)
class AlertImage:
    """One localized variant of an alert's image."""

    url: str
    media_type: str | None
    language: str | None


@dataclass(frozen=True)
class VehiclePosition:
    """A live vehicle position from a GTFS-RT feed.

    ``latitude``/``longitude`` (and ``bearing``, ``speed``,
    ``odometer``) are None for a vehicle that reports no position: it may
    say only which trip it runs and where along it, as a docked
    :class:`GbfsVehicle` may. A zone filter never includes it.

    ``stop_id``/``current_stop_sequence`` identify the vehicle's current
    stop as the producer sent them (raw, unresolved). ``current_status``
    describes the vehicle's relationship to the stop
    ``current_stop_sequence`` names: it is "Ignored if
    current_stop_sequence is missing" (None), and with one an unset status
    is IN_TRANSIT_TO, which "is assumed".
    ``timestamp`` is when the position was measured, or, for a vehicle
    that does not say, when the message was built (the spec has consumers
    "use the message timestamp"); None only when neither says.
    ``congestion_level`` is None when unset (its protobuf default,
    UNKNOWN_CONGESTION_LEVEL, carries no information to synthesize);
    ``license_plate`` is the vehicle descriptor's verbatim plate.

    ``route``/``stop`` are the whole static records, matching
    :class:`StopArrival`; ``route_id``/``stop_id`` stay as the raw ids the
    producer sent, which may name something the static dataset does not
    have (leaving the record None).

    ``trip_start_date``/``trip_start_secs`` are the parsed
    ``TripDescriptor.start_date``/``start_time`` -- the pair that pins
    WHICH instance of a trip this vehicle is running. Without them a
    frequency-based trip's concurrent repetitions cannot be told apart.
    Both degrade to None on absent or malformed values, like every other
    RT descriptor here.
    """

    vehicle_id: str | None
    label: str | None
    latitude: float | None
    longitude: float | None
    bearing: float | None
    speed: float | None
    route_id: str | None
    route: Route | None
    trip_id: str | None
    trip_start_date: date | None
    trip_start_secs: int | None
    occupancy_status: OccupancyStatus | None
    occupancy_percentage: int | None
    carriages: tuple[CarriageDetail, ...]
    wheelchair_accessible: WheelchairAccess | None
    odometer: float | None
    timestamp: datetime | None
    current_status: VehicleStopStatus | None
    congestion_level: CongestionLevel | None
    stop_id: str | None
    stop: Stop | None
    current_stop_sequence: int | None
    license_plate: str | None


@dataclass(frozen=True)
class TripSelector:
    """The trip an alert's EntitySelector names (a GTFS-RT TripDescriptor).

    Field names are the spec's; ``start_date``/``start_secs`` are the
    parsed ``start_date``/``start_time``, as on :class:`VehiclePosition`.
    With a ``trip_id`` it names that trip -- one run of it where
    ``start_date``/``start_secs`` say which. Without one it names the run
    of ``route_id`` and ``direction_id`` that starts at ``start_secs`` on
    ``start_date``, all four set (a trip without them is not kept).
    """

    trip_id: str | None
    route_id: str | None
    direction_id: int | None
    start_date: date | None
    start_secs: int | None


@dataclass(frozen=True)
class EntitySelector:
    """One ``informed_entity`` of an alert: the fields it names, joined by AND.

    "If several are given, they should be interpreted as being joined by
    the logical AND operator ... in order for an alert to apply to an
    entity in GTFS it must match all of the provided EntitySelector
    fields." ``route_type`` is a raw int (an open vocabulary, as on
    :attr:`Route.type`); ``direction_id`` a raw 0/1.
    """

    agency_id: str | None
    route_id: str | None
    route_type: int | None
    direction_id: int | None
    trip: TripSelector | None
    stop_id: str | None


def _selects(
    selector: EntitySelector,
    *,
    agency_id: str | None,
    route_id: str | None,
    route_type: int | None,
    direction_id: int | None,
    stop_id: str | None,
    trip_id: str | None,
    start_date: date | None,
    start_secs: int | None,
) -> bool:
    """Whether an entity with these values matches every field ``selector`` names.

    A value the entity does not give cannot match a field the selector
    names. A selector naming nothing selects nothing (the spec requires at
    least one field).
    """
    named: tuple[tuple[object, object], ...] = (
        (selector.agency_id, agency_id),
        (selector.route_id, route_id),
        (selector.route_type, route_type),
        (selector.direction_id, direction_id),
        (selector.stop_id, stop_id),
    )
    trip = selector.trip
    if trip is not None:
        # A trip_id implies its route and direction; without one, they and
        # the start pick the run out.
        named += (
            ((trip.trip_id, trip_id),)
            if trip.trip_id is not None
            else ((trip.route_id, route_id), (trip.direction_id, direction_id))
        ) + ((trip.start_date, start_date), (trip.start_secs, start_secs))
    wanted = [(field, value) for field, value in named if field is not None]
    return bool(wanted) and all(field == value for field, value in wanted)


@dataclass(frozen=True)
class ServiceAlert:
    """A service alert from a GTFS-RT feed.

    ``informed_entities`` are the alert's EntitySelectors, one per
    ``informed_entity``, each the AND of the fields it names; the alert
    applies to an entity any of them matches (:meth:`applies_to`). A
    selector that cannot say what it names is left out rather than read
    wider: a malformed start date or time; a trip without trip_id that
    lacks any of route_id, direction_id, start_date and start_time (it is
    not one run, and its route_id "cannot be used ... to specify a
    route-wide alert"); a direction_id without a route_id ("route_id must
    also be provided"). An alert with no selector applies to nothing.

    ``agency_ids``/``route_ids``/``route_types``/``direction_ids``/
    ``stop_ids``/``trip_ids`` list the distinct values the selectors name,
    sorted: an index, not a scope test (a selector naming route R and stop
    S puts R and S in them, though the alert applies to R only at S).

    ``cause``/``effect``/``severity`` take the proto's declared defaults
    (UNKNOWN_CAUSE, UNKNOWN_EFFECT, UNKNOWN_SEVERITY) when unset; None only
    for a value outside the vocabulary. ``active_periods`` are (start, end)
    pairs, None where a bound is absent (minus or plus infinity).
    """

    id: str
    header: str | None
    description: str | None
    tts_header: str | None
    tts_description: str | None
    cause: AlertCause | None
    cause_detail: str | None
    effect: AlertEffect | None
    effect_detail: str | None
    severity: AlertSeverity | None
    images: tuple[AlertImage, ...]
    image_alternative_text: str | None
    informed_entities: tuple[EntitySelector, ...]
    active_periods: tuple[tuple[datetime | None, datetime | None], ...]
    url: str | None

    @property
    def agency_ids(self) -> tuple[str, ...]:
        """Every agency_id a selector names."""
        return _named(s.agency_id for s in self.informed_entities)

    @property
    def route_ids(self) -> tuple[str, ...]:
        """Every route_id a selector names, a trip-run selector's included."""
        return _named(
            route_id
            for s in self.informed_entities
            for route_id in (s.route_id, _run_part(s.trip, "route_id"))
        )

    @property
    def route_types(self) -> tuple[int, ...]:
        """Every route_type a selector names."""
        return _named(s.route_type for s in self.informed_entities)

    @property
    def direction_ids(self) -> tuple[int, ...]:
        """Every direction_id a selector names, a trip-run selector's included."""
        return _named(
            direction_id
            for s in self.informed_entities
            for direction_id in (s.direction_id, _run_part(s.trip, "direction_id"))
        )

    @property
    def stop_ids(self) -> tuple[str, ...]:
        """Every stop_id a selector names."""
        return _named(s.stop_id for s in self.informed_entities)

    @property
    def trip_ids(self) -> tuple[str, ...]:
        """Every trip_id a selector's trip names."""
        return _named(s.trip.trip_id for s in self.informed_entities if s.trip)

    def applies_to(
        self,
        *,
        agency_id: str | None = None,
        route_id: str | None = None,
        route_type: int | None = None,
        direction_id: int | None = None,
        stop_id: str | None = None,
        trip_id: str | None = None,
        start_date: date | None = None,
        start_secs: int | None = None,
    ) -> bool:
        """Return True if the alert applies to the entity these values describe.

        It does when one of its selectors matches: every field the selector
        names equals the value given for it ("it must match all of the
        provided EntitySelector fields"). A value not given matches no
        field that names one, so describe the entity in full: a stop and
        route alert applies to ``stop_id=S, route_id=R``, not to
        ``stop_id=S`` alone, and a selector naming one run of a trip
        (``start_date``, ``start_secs``: its TripDescriptor start date and
        start time) applies only where those are given. A ``trip_id`` is
        the trip a selector names, its route and direction implied.
        """
        return any(
            _selects(
                selector,
                agency_id=agency_id,
                route_id=route_id,
                route_type=route_type,
                direction_id=direction_id,
                stop_id=stop_id,
                trip_id=trip_id,
                start_date=start_date,
                start_secs=start_secs,
            )
            for selector in self.informed_entities
        )

    def is_active(self, at: datetime) -> bool:
        """Return True if the alert is active at the given instant.

        An alert with no active periods is always active. A period "is
        considered active at time t if t is greater than or equal to the
        start time and less than the end time"; an absent bound is
        unbounded on that side.
        """
        if not self.active_periods:
            return True
        return any(
            (start is None or start <= at) and (end is None or at < end)
            for start, end in self.active_periods
        )


def _run_part(trip: TripSelector | None, name: str) -> Any:
    """Return a trip-run selector's route_id or direction_id, else None.

    Only a trip without trip_id names them (one with an id implies its own).
    """
    return getattr(trip, name) if trip is not None and trip.trip_id is None else None


def _named[T: (str, int)](values: Iterable[T | None]) -> tuple[T, ...]:
    """Return the distinct values present, sorted."""
    return tuple(sorted({value for value in values if value is not None}))


@dataclass(frozen=True)
class GbfsStation:
    """A GBFS station: information + status merged.

    ``rental_uris`` passes through the station's deep-link table as
    provided (keys ``android``/``ios``/``web`` per the spec, any string
    key kept); None when the document omits it.

    ``vehicle_types_available`` maps vehicle type id -> count; {} when the
    station publishes an empty breakdown (zero of every type), None when it
    is absent or has no usable entries.
    """

    id: str
    name: str | None
    latitude: float | None
    longitude: float | None
    short_name: str | None
    capacity: int | None
    bikes_available: int | None
    docks_available: int | None
    bikes_disabled: int | None
    docks_disabled: int | None
    is_installed: bool | None
    is_renting: bool | None
    is_returning: bool | None
    is_virtual_station: bool | None
    last_reported: datetime | None
    address: str | None
    cross_street: str | None
    post_code: str | None
    region_id: str | None
    vehicle_types_available: Mapping[str, int] | None
    rental_uris: Mapping[str, str] | None

    def __post_init__(self) -> None:
        """Freeze mapping fields given as plain dicts."""
        _freeze_mappings(self, "vehicle_types_available", "rental_uris")


@dataclass(frozen=True)
class GbfsVehicle:
    """A GBFS vehicle that is not in active rental.

    A free-floating vehicle always has a position; a docked one (with a
    ``station_id``) need not report one (GBFS 2.1+: ``lat``/``lon`` are
    REQUIRED only without ``station_id``), so its ``latitude`` and
    ``longitude`` may be None -- it is at its station.

    ``rental_uris`` passes through the vehicle's deep-link table as
    provided (keys ``android``/``ios``/``web`` per the spec, any string
    key kept); None when the document omits it.
    """

    id: str
    latitude: float | None
    longitude: float | None
    is_reserved: bool | None
    is_disabled: bool | None
    vehicle_type_id: str | None
    current_range_m: float | None
    current_fuel_percent: float | None
    last_reported: datetime | None
    station_id: str | None
    home_station_id: str | None
    pricing_plan_id: str | None
    rental_uris: Mapping[str, str] | None

    def __post_init__(self) -> None:
        """Freeze the mapping field when given as a plain dict."""
        _freeze_mappings(self, "rental_uris")


@dataclass(frozen=True)
class FeedInfo:
    """The static feed's own metadata record (feed_info.txt).

    Text fields are verbatim cells (empty -> None); ``start_date``/
    ``end_date`` are the ``feed_start_date``/``feed_end_date`` cells
    parsed leniently from YYYYMMDD (absent, malformed, or
    calendar-invalid -> None -- descriptive metadata never fails a
    build). GTFS defines feed_info.txt as a single-record file; if a
    producer ships several data rows, the FIRST one wins and the rest
    are ignored.
    """

    publisher_name: str | None
    publisher_url: str | None
    lang: str | None
    version: str | None
    start_date: date | None
    end_date: date | None
    contact_email: str | None
    contact_url: str | None


@dataclass(frozen=True)
class GbfsVehicleType:
    """One vehicle type a GBFS system rents.

    Resolves the ``vehicle_type_id`` that stations and vehicles reference.
    ``form_factor``/``propulsion_type`` stay raw strings: GBFS adds new
    values across minor versions, so a closed enum would reject a feed
    that is merely newer than this library.
    """

    id: str
    form_factor: str | None
    propulsion_type: str | None
    name: str | None
    max_range_m: float | None
    rider_capacity: int | None


@dataclass(frozen=True)
class GbfsPricingPlan:
    """One pricing plan, resolving a vehicle's ``pricing_plan_id``.

    ``price`` is the base price in ``currency``; GBFS also allows per-km
    and per-minute segment tables, which are not modeled -- the plan's
    ``description`` is the human-readable fallback for those.
    """

    id: str
    name: str | None
    currency: str | None
    price: float | None
    is_taxable: bool | None
    description: str | None


@dataclass(frozen=True)
class GbfsSystemRegion:
    """A named service region, resolving a station's ``region_id``."""

    id: str
    name: str | None


@dataclass(frozen=True)
class GbfsAlert:
    """A GBFS system alert.

    Distinct from :class:`ServiceAlert`, which is GTFS-RT: this one scopes
    to GBFS stations and regions rather than routes and trips, and its
    ``type`` is a raw string for the same reason as
    :attr:`GbfsVehicleType.form_factor`. Each active period has the start
    GBFS requires; its end is None when it is open-ended.
    """

    id: str
    type: str | None
    summary: str | None
    description: str | None
    url: str | None
    station_ids: tuple[str, ...]
    region_ids: tuple[str, ...]
    last_updated: datetime | None
    active_periods: tuple[tuple[datetime, datetime | None], ...]


@dataclass(frozen=True)
class GbfsSystemInfo:
    """GBFS system information."""

    system_id: str
    name: str | None
    short_name: str | None
    operator: str | None
    timezone: str | None
    languages: tuple[str, ...]
    url: str | None
    purchase_url: str | None
    start_date: date | None
    phone_number: str | None
    email: str | None
    feed_contact_email: str | None
    license_url: str | None
    terms_url: str | None
    privacy_url: str | None
    opening_hours: str | None


@dataclass(frozen=True)
class StaticBuildProgress:
    """Progress of a static dataset acquisition (download, then index)."""

    phase: str  # "download" or "index"
    done_bytes: int
    total_bytes: int | None

    @property
    def fraction(self) -> float | None:
        """Completed fraction in [0, 1], or None when total is unknown."""
        if self.total_bytes:
            return min(1.0, self.done_bytes / self.total_bytes)
        return None
