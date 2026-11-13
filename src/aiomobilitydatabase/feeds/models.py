"""Public snapshot models returned by feed handles."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import IntEnum, StrEnum

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
    :meth:`TransitFeedHandle.get_headsigns_serving` offers -- so a trip
    realtime states in full passes one only by the headsign its update
    gives (``TripProperties.trip_headsign``, or a replaced trip's own).
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
    ``wheelchair_accessible``/``bikes_allowed`` describe the trip,
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
    Rows of a trip realtime states in full (NEW, ADDED, or a REPLACEMENT
    run, whose static schedule "isn't used") have no stop_times row: their
    scheduled times are the update's own ``scheduled_time`` (None where it
    gives none) and their predictions its times. ``service_id``, the
    stop_time descriptors, ``wheelchair_accessible``, ``bikes_allowed``,
    ``trip_short_name`` and ``block_id`` are None; ``route_id``,
    ``direction_id`` and ``headsign`` are what the update gives, a
    replacement's gaps filled from the trip it replaces, whose ``trip_id``
    (a frequency repetition's own) it shows under.

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
    trip realtime states in full (NEW, or a REPLACEMENT run, whose static
    schedule "isn't used") has only the ones its update gives
    (``scheduled_time``), and its other fields as on
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
    the trip (``direction_id`` stays a raw int: 0/1 with feed-defined
    meaning), as do ``trip_short_name``/``block_id`` (verbatim cells;
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

    ``is_first``/``is_last`` mark whether this departure is the first/last
    scheduled departure OF ITS SERVICE DAY for this origin->destination
    pair -- legacy ``gtfs`` sensor parity; they describe the static
    schedule, so a trip realtime states in full is neither. A
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
    percentage. ``carriage_sequence`` orders the carriages from the
    vehicle's front.
    """

    id: str | None
    label: str | None
    occupancy_status: OccupancyStatus | None
    occupancy_percentage: int | None
    carriage_sequence: int | None


@dataclass(frozen=True)
class AlertImage:
    """One localized variant of an alert's image."""

    url: str
    media_type: str | None
    language: str | None


@dataclass(frozen=True)
class VehiclePosition:
    """A live vehicle position from a GTFS-RT feed.

    ``stop_id``/``current_stop_sequence`` identify the vehicle's current
    stop as the producer sent them (raw, unresolved). ``current_status``
    describes the vehicle's relationship to that stop. The protobuf field
    carries an implicit default (IN_TRANSIT_TO), which per the spec is
    only meaningful with respect to a current stop -- so the default is
    surfaced ONLY when ``current_stop_sequence`` or ``stop_id`` is
    present; with neither, an unset ``current_status`` is None (there is
    no stop to be in transit to). An EXPLICITLY set ``current_status`` is
    always surfaced verbatim, referent or not -- consumers decide.
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
    latitude: float
    longitude: float
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
class ServiceAlert:
    """A service alert from a GTFS-RT feed.

    ``agency_ids``/``route_ids``/``route_types``/``direction_ids``/
    ``stop_ids``/``trip_ids`` are the alert's scope: the distinct values
    an ``informed_entity`` named, each sorted. GTFS-RT's EntitySelector
    can scope by any of these, so an alert is UNSCOPED (applies
    feed-wide) only when EVERY list is empty. An alert informing only one
    agency is agency-scoped, not feed-wide -- which matters as soon as a
    feed carries more than one agency.

    ``route_types`` holds raw ints for the same reason as
    :attr:`Route.type`: Google's extended route types are an open
    vocabulary. ``direction_ids`` are raw 0/1 with feed-defined meaning.
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
    agency_ids: tuple[str, ...]
    route_ids: tuple[str, ...]
    route_types: tuple[int, ...]
    direction_ids: tuple[int, ...]
    stop_ids: tuple[str, ...]
    trip_ids: tuple[str, ...]
    active_periods: tuple[tuple[datetime | None, datetime | None], ...]
    url: str | None

    def is_active(self, at: datetime) -> bool:
        """Return True if the alert is active at the given instant.

        An alert with no active periods is always active; open-ended bounds
        (None start or end) are unbounded on that side.
        """
        if not self.active_periods:
            return True
        return any(
            (start is None or start <= at) and (end is None or at <= end)
            for start, end in self.active_periods
        )


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
