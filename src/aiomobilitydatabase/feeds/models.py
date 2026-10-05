"""Public snapshot models returned by feed handles."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import IntEnum, StrEnum


class WheelchairAccess(IntEnum):
    """Wheelchair support, as GTFS's shared 0/1/2 vocabulary.

    One enum for BOTH ``trips.wheelchair_accessible`` (can the vehicle carry
    a rider in a wheelchair?) and ``stops.wheelchair_boarding`` (is boarding
    possible at the stop?): the spec defines the identical value shape for
    the two columns, and POSSIBLE/NOT_POSSIBLE reads naturally in either
    position. Values match the spec ints, so ``int(...)``/``.value``
    comparisons keep working for consumers who want the raw number.
    """

    UNKNOWN = 0
    POSSIBLE = 1
    NOT_POSSIBLE = 2


class BikesAllowed(IntEnum):
    """GTFS ``trips.bikes_allowed`` vocabulary (values match the spec ints)."""

    UNKNOWN = 0
    ALLOWED = 1
    NOT_ALLOWED = 2


class PickupDropOffType(IntEnum):
    """GTFS ``stop_times.pickup_type``/``drop_off_type`` shared vocabulary.

    The spec defines the identical value shape for both columns, so one
    enum serves the two fields (values match the spec ints).
    """

    REGULAR = 0
    NONE = 1
    PHONE_AGENCY = 2
    COORDINATE_WITH_DRIVER = 3


class StopLocationType(IntEnum):
    """GTFS ``stops.location_type`` vocabulary (values match the spec ints).

    Named ``StopLocationType`` because the catalog-side
    :class:`aiomobilitydatabase.models.LocationType` (a StrEnum of catalog
    location kinds) is an unrelated vocabulary.
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
    ``stop_url``/``zone_id`` cells (empty -> None). ``timezone`` is
    ``stop_timezone`` and is DISPLAY METADATA ONLY: per the GTFS spec,
    every stop_times value is expressed in the AGENCY timezone regardless
    of any stop_timezone, so no time computation in this library reads it
    -- it exists so consumers can render "local time at this stop".
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


@dataclass(frozen=True)
class StationGroup:
    """A logical station: boarding stops grouped for presentation.

    Boarding stops sharing a GTFS ``parent_station`` — or, without one, an
    identical name (for example direction pairs at an intersection) — form
    one group. ``id`` is the parent station id when present, else the
    casefolded shared name; it is stable across rebuilds of the same feed.
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
    ``#`` (e.g. ``"FFD700"``); ``agency_id`` resolves against
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
    matches the trip-level headsign (the same values
    :meth:`TransitFeedHandle.headsigns_serving` offers), so RT-added trips,
    which announce no headsign, never pass a headsign filter. ``limit``
    caps the merged, filtered result for this query as a whole,
    nearest effective departure first; zero asks for an empty board.
    """

    stop_ids: Sequence[str]
    route_ids: Sequence[str] | None = None
    headsigns: Sequence[str] | None = None
    limit: int = 10

    def __post_init__(self) -> None:
        """Reject inputs a slice or an id-iteration would silently misread."""
        for name in ("stop_ids", "route_ids", "headsigns"):
            if isinstance(getattr(self, name), str):
                raise TypeError(f"{name} must be a sequence of ids, not a str")
        # A negative limit would slice rows off the END of the board.
        if self.limit < 0:
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
    absent or blank), False for approximate times, None when the value is
    outside the 0/1 vocabulary. ``trip_short_name``/``block_id`` are the
    trip's verbatim ``trip_short_name``/``block_id`` cells -- ``block_id``
    is raw string exposure only (no block-continuation computation).
    ``stop`` and ``route`` are the whole static records rather than a
    handful of copied columns, so platform codes, coordinates, route
    colours and the rest are reachable without a second lookup; use
    ``route.display_name``/``stop.name`` for the human-readable strings.
    ``stop_id``/``route_id`` stay as the raw ids a feed referenced, which
    is what RT rows and filters key on, and a ``route_id`` naming a route
    absent from routes.txt leaves ``route`` None rather than inventing
    one. ``direction_id`` stays a raw int (0/1 with feed-defined meaning),
    matching :attr:`UpcomingTrip.direction_id`.
    RT-added rows have no static schedule row, so every descriptive field
    is None.
    """

    stop_id: str
    stop: Stop | None
    route_id: str | None
    route: Route | None
    trip_id: str | None
    headsign: str | None
    scheduled_arrival: datetime | None
    scheduled_departure: datetime | None
    predicted_arrival: datetime | None
    predicted_departure: datetime | None
    delay_seconds: int | None
    realtime: bool
    vehicle_id: str | None
    wheelchair_accessible: WheelchairAccess | None
    bikes_allowed: BikesAllowed | None
    direction_id: int | None
    pickup_type: PickupDropOffType | None
    drop_off_type: PickupDropOffType | None
    timepoint_exact: bool | None
    stop_headsign: str | None
    trip_short_name: str | None
    block_id: str | None


@dataclass(frozen=True)
class UpcomingTrip:
    """An upcoming origin-to-destination journey on one scheduled trip.

    The shape Home Assistant's legacy ``gtfs`` sensor needs: the next
    departure from an origin stop on a trip that later serves the
    destination stop. Scheduled times are non-optional because the
    producing query requires an origin departure time and a destination
    arrival time; ``delay_seconds`` is the origin departure delay.

    ``wheelchair_accessible``/``bikes_allowed``/``direction_id`` describe
    the trip (``direction_id`` stays a raw int: 0/1 with feed-defined
    meaning), as do ``trip_short_name``/``block_id`` (verbatim cells;
    ``block_id`` is raw string exposure only -- no block-continuation
    computation). The ``origin_*``/``destination_*`` descriptors come from the
    stop_time rows at each end, with the same semantics as the matching
    :class:`StopArrival` fields (``timepoint_exact`` defaults to True when
    the GTFS column is absent or blank).

    ``is_first``/``is_last`` mark whether this departure is the first/last
    departure OF ITS SERVICE DAY for this origin->destination pair --
    legacy ``gtfs`` sensor parity. A past-midnight departure (>24:00:00)
    is flagged relative to the service day it belongs to, not the clock
    day it happens on, and frequency-materialized repetitions count as
    ordinary trips (the day's first repetition is the first departure).
    """

    trip_id: str
    route_id: str
    route_name: str | None
    headsign: str | None
    origin_stop_id: str
    destination_stop_id: str
    scheduled_departure: datetime
    predicted_departure: datetime | None
    scheduled_arrival: datetime
    predicted_arrival: datetime | None
    delay_seconds: int | None
    realtime: bool
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
    """

    vehicle_id: str | None
    label: str | None
    latitude: float
    longitude: float
    bearing: float | None
    speed: float | None
    route_id: str | None
    route_name: str | None
    trip_id: str | None
    occupancy_status: OccupancyStatus | None
    timestamp: datetime | None
    current_status: VehicleStopStatus | None
    congestion_level: CongestionLevel | None
    stop_id: str | None
    current_stop_sequence: int | None
    license_plate: str | None


@dataclass(frozen=True)
class ServiceAlert:
    """A service alert from a GTFS-RT feed.

    ``route_ids``/``stop_ids``/``trip_ids`` are the alert's scope: the
    distinct route ids, stop ids, and informed-entity trip descriptor
    trip ids across ``informed_entity``, each sorted. An alert is
    UNSCOPED (applies feed-wide) only when ALL THREE lists are empty --
    an alert informing only trips is trip-scoped, not agency-wide.
    """

    id: str
    header: str | None
    description: str | None
    cause: AlertCause | None
    effect: AlertEffect | None
    severity: AlertSeverity | None
    route_ids: list[str]
    stop_ids: list[str]
    trip_ids: list[str]
    active_periods: list[tuple[datetime | None, datetime | None]]
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
class Station:
    """A GBFS station: information + status merged.

    ``rental_uris`` passes through the station's deep-link table as
    provided (keys ``android``/``ios``/``web`` per the spec, any string
    key kept); None when the document omits it.
    """

    id: str
    name: str | None
    latitude: float | None
    longitude: float | None
    capacity: int | None
    bikes_available: int | None
    docks_available: int | None
    is_renting: bool | None
    is_returning: bool | None
    vehicle_types_available: dict[str, int] | None
    rental_uris: dict[str, str] | None


@dataclass(frozen=True)
class GbfsVehicle:
    """A free-floating GBFS vehicle.

    ``rental_uris`` passes through the vehicle's deep-link table as
    provided (keys ``android``/``ios``/``web`` per the spec, any string
    key kept); None when the document omits it.
    """

    id: str
    latitude: float
    longitude: float
    is_reserved: bool | None
    is_disabled: bool | None
    vehicle_type_id: str | None
    current_range_m: float | None
    rental_uris: dict[str, str] | None


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


@dataclass(frozen=True)
class SystemInfo:
    """GBFS system information."""

    system_id: str
    name: str | None
    operator: str | None
    timezone: str | None


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
