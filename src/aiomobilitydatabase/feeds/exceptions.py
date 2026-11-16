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
    """A producer or GBFS endpoint could not be reached or errored.

    The message is the library's own: never a URL, a response body or
    another library's error text, any of which can carry a credential.
    ``status`` is the HTTP status when there was one, and ``url`` the URL
    asked for without its credentials, query or fragment (None when it
    doesn't parse).
    """

    def __init__(
        self, message: str, *, status: int | None = None, url: str | None = None
    ) -> None:
        """Initialize with a message, and the HTTP status and URL when known."""
        super().__init__(message)
        self.status = status
        self.url = url


class SourceAuthenticationError(MobilityFeedsError):
    """The producer rejected the supplied credentials (401/403)."""


class FeedParseError(MobilityFeedsError):
    """A feed payload could not be parsed (protobuf, GBFS JSON, or GTFS zip).

    ``url`` is the document's URL, without credentials, query or fragment,
    when one was fetched (None otherwise).
    """

    def __init__(self, message: str, *, url: str | None = None) -> None:
        """Initialize with a message and, for a fetched document, its URL."""
        super().__init__(message)
        self.url = url


class StaticDataUnavailableError(MobilityFeedsError):
    """The feed has no usable hosted static dataset.

    Also raised, rarely, when concurrent builds of another dataset into the
    same cache keep replacing this one's: that passes, and a later attempt
    succeeds.
    """
