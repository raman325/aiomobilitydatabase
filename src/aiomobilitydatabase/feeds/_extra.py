"""Lazy access to the GTFS / GTFS-RT handle, which needs the ``feeds`` extra."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .transit import TransitFeedHandle

EXTRA_HINT = (
    "GTFS and GTFS-RT feeds require the 'feeds' extra: "
    "pip install aiomobilitydatabase[feeds]"
)


def load_transit_handle() -> type[TransitFeedHandle]:
    """Import TransitFeedHandle, naming the missing extra if protobuf is absent."""
    try:
        from .transit import TransitFeedHandle  # noqa: PLC0415
    except ImportError as err:
        raise ImportError(EXTRA_HINT) from err
    return TransitFeedHandle
