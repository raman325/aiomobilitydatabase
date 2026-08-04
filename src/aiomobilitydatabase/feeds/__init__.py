"""Typed async consumption of Mobility Database transit and GBFS feeds.

Requires the ``feeds`` extra (``pip install aiomobilitydatabase[feeds]``) for
the ``gtfs-realtime-bindings`` protobuf dependency.
"""

try:
    from google.transit import gtfs_realtime_pb2 as _pb2_check  # noqa: F401
except ImportError as err:
    raise ImportError(
        "aiomobilitydatabase.feeds requires the 'feeds' extra: "
        "pip install aiomobilitydatabase[feeds]"
    ) from err

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
    GbfsVehicle,
    Route,
    ServiceAlert,
    StaticBuildProgress,
    Station,
    StationGroup,
    Stop,
    StopArrival,
    SystemInfo,
    UpcomingTrip,
    VehiclePosition,
)
from .transit import TransitFeedHandle

__all__ = [
    "Circle",
    "FeedParseError",
    "GbfsFeedHandle",
    "GbfsVehicle",
    "MobilityFeedsClient",
    "MobilityFeedsError",
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
    "SystemInfo",
    "TransitFeedHandle",
    "UpcomingTrip",
    "VehiclePosition",
]
