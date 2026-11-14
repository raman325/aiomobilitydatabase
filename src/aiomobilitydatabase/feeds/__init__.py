"""Typed async consumption of Mobility Database transit and GBFS feeds.

GBFS works with the base install. GTFS and GTFS-RT (``TransitFeedHandle``,
``MobilityFeedsClient.get_transit_feed*``) need the ``feeds`` extra
(``pip install aiomobilitydatabase[feeds]``) for protobuf; using them
without it raises an ``ImportError`` naming the extra.
"""

from typing import TYPE_CHECKING, Any

from . import _extra
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
    EntitySelector,
    FeedInfo,
    GbfsAlert,
    GbfsPricingPlan,
    GbfsStation,
    GbfsSystemInfo,
    GbfsSystemRegion,
    GbfsVehicle,
    GbfsVehicleType,
    OccupancyStatus,
    PickupDropOffType,
    Route,
    ServiceAlert,
    StaticBuildProgress,
    StationGroup,
    Stop,
    StopArrival,
    StopLocationType,
    TripQuery,
    TripSelector,
    UpcomingTrip,
    VehiclePosition,
    VehicleStopStatus,
    WheelchairAccess,
)

if TYPE_CHECKING:
    from .transit import TransitFeedHandle


if not TYPE_CHECKING:

    def __getattr__(name: str) -> Any:
        """Load TransitFeedHandle on first access, so GBFS needs no protobuf."""
        if name == "TransitFeedHandle":
            return _extra.load_transit_handle()
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
    "EntitySelector",
    "FeedInfo",
    "FeedParseError",
    "GbfsAlert",
    "GbfsFeedHandle",
    "GbfsPricingPlan",
    "GbfsStation",
    "GbfsSystemInfo",
    "GbfsSystemRegion",
    "GbfsVehicle",
    "GbfsVehicleType",
    "MobilityFeedsClient",
    "MobilityFeedsError",
    "OccupancyStatus",
    "PickupDropOffType",
    "Route",
    "ServiceAlert",
    "SourceAuthenticationError",
    "SourceConnectionError",
    "StaticBuildProgress",
    "StaticDataUnavailableError",
    "StationGroup",
    "Stop",
    "StopArrival",
    "StopLocationType",
    "TransitFeedHandle",
    "TripQuery",
    "TripSelector",
    "UpcomingTrip",
    "VehiclePosition",
    "VehicleStopStatus",
    "WheelchairAccess",
]
