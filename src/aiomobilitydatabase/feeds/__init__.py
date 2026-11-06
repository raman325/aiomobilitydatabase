"""Typed async consumption of Mobility Database transit and GBFS feeds.

GBFS works with the base install. GTFS and GTFS-RT (``TransitFeedHandle``,
``MobilityFeedsClient.get_transit_feed*``) need the ``feeds`` extra
(``pip install aiomobilitydatabase[feeds]``) for protobuf; using them
without it raises an ``ImportError`` naming the extra.
"""

from typing import TYPE_CHECKING, Any

from ._extra import load_transit_handle
from .client import MobilityFeedsClient
from .exceptions import (
    FeedParseError,
    MobilityFeedsError,
    SourceAuthenticationError,
    SourceConnectionError,
    StaticDataUnavailableError,
)
from .gbfs import GbfsFeedHandle
from .geo import Circle
from .models import (
    Agency,
    AlertCause,
    AlertEffect,
    AlertImage,
    AlertSeverity,
    ArrivalsQuery,
    BikesAllowed,
    CarriageDetail,
    CongestionLevel,
    FeedInfo,
    GbfsAlert,
    GbfsVehicle,
    OccupancyStatus,
    PickupDropOffType,
    PricingPlan,
    Route,
    ServiceAlert,
    StaticBuildProgress,
    Station,
    StationGroup,
    Stop,
    StopArrival,
    StopLocationType,
    SystemInfo,
    SystemRegion,
    UpcomingTrip,
    VehiclePosition,
    VehicleStopStatus,
    VehicleType,
    WheelchairAccess,
)

if TYPE_CHECKING:
    from .transit import TransitFeedHandle


def __getattr__(name: str) -> Any:
    """Load TransitFeedHandle on first access, so GBFS needs no protobuf."""
    if name == "TransitFeedHandle":
        return load_transit_handle()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "Agency",
    "AlertCause",
    "AlertEffect",
    "AlertImage",
    "AlertSeverity",
    "ArrivalsQuery",
    "BikesAllowed",
    "CarriageDetail",
    "Circle",
    "CongestionLevel",
    "FeedInfo",
    "FeedParseError",
    "GbfsAlert",
    "GbfsFeedHandle",
    "GbfsVehicle",
    "MobilityFeedsClient",
    "MobilityFeedsError",
    "OccupancyStatus",
    "PickupDropOffType",
    "PricingPlan",
    "Route",
    "ServiceAlert",
    "SourceAuthenticationError",
    "SourceConnectionError",
    "StaticBuildProgress",
    "StaticDataUnavailableError",
    "Station",
    "StationGroup",
    "Stop",
    "StopArrival",
    "StopLocationType",
    "SystemInfo",
    "SystemRegion",
    "TransitFeedHandle",
    "UpcomingTrip",
    "VehiclePosition",
    "VehicleStopStatus",
    "VehicleType",
    "WheelchairAccess",
]
