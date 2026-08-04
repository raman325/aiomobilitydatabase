"""Public snapshot models returned by feed handles."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Stop:
    """A transit stop from the static GTFS index."""

    id: str
    name: str | None
    latitude: float | None
    longitude: float | None
    parent_station: str | None = None
    location_type: int | None = None


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
    """A transit route from the static GTFS index."""

    id: str
    short_name: str | None
    long_name: str | None
    type: int | None

    @property
    def display_name(self) -> str:
        """Best human-readable name for the route."""
        if self.short_name and self.long_name:
            return f"{self.short_name} {self.long_name}"
        return self.short_name or self.long_name or self.id


@dataclass(frozen=True)
class StopArrival:
    """An upcoming (or realtime-added) arrival/departure at a stop."""

    stop_id: str
    stop_name: str | None
    route_id: str | None
    route_name: str | None
    trip_id: str | None
    headsign: str | None
    scheduled_arrival: datetime | None
    scheduled_departure: datetime | None
    predicted_arrival: datetime | None
    predicted_departure: datetime | None
    delay_seconds: int | None
    realtime: bool
    vehicle_id: str | None


@dataclass(frozen=True)
class UpcomingTrip:
    """An upcoming origin-to-destination journey on one scheduled trip.

    The shape Home Assistant's legacy ``gtfs`` sensor needs: the next
    departure from an origin stop on a trip that later serves the
    destination stop. Scheduled times are non-optional because the
    producing query requires an origin departure time and a destination
    arrival time; ``delay_seconds`` is the origin departure delay.
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


@dataclass(frozen=True)
class VehiclePosition:
    """A live vehicle position from a GTFS-RT feed."""

    vehicle_id: str | None
    label: str | None
    latitude: float
    longitude: float
    bearing: float | None
    speed: float | None
    route_id: str | None
    route_name: str | None
    trip_id: str | None
    occupancy_status: str | None
    timestamp: datetime | None


@dataclass(frozen=True)
class ServiceAlert:
    """A service alert from a GTFS-RT feed."""

    id: str
    header: str | None
    description: str | None
    cause: str | None
    effect: str | None
    severity: str | None
    route_ids: list[str]
    stop_ids: list[str]
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
    """A GBFS station: information + status merged."""

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


@dataclass(frozen=True)
class GbfsVehicle:
    """A free-floating GBFS vehicle."""

    id: str
    latitude: float
    longitude: float
    is_reserved: bool | None
    is_disabled: bool | None
    vehicle_type_id: str | None
    current_range_m: float | None


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
