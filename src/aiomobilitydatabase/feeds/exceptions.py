"""Exceptions for the feeds subpackage."""

from __future__ import annotations

from ..exceptions import MobilityDatabaseError


class MobilityFeedsError(MobilityDatabaseError):
    """Base exception for all feeds errors.

    Subclasses :class:`~aiomobilitydatabase.exceptions.MobilityDatabaseError`
    so callers can catch a single root exception across both the catalog and
    the feeds layer.
    """


class SourceConnectionError(MobilityFeedsError):
    """A producer or GBFS endpoint could not be reached or errored."""

    def __init__(self, message: str, *, status: int | None = None) -> None:
        """Initialize with a message and optional HTTP status."""
        super().__init__(message)
        self.status = status


class SourceAuthenticationError(MobilityFeedsError):
    """The producer rejected the supplied credentials (401/403)."""


class FeedParseError(MobilityFeedsError):
    """A feed payload could not be parsed (protobuf, GBFS JSON, or GTFS zip)."""


class StaticDataUnavailableError(MobilityFeedsError):
    """The feed has no usable hosted static dataset."""
