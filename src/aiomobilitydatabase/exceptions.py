"""Exceptions for aiomobilitydatabase."""

from __future__ import annotations


class MobilityDatabaseError(Exception):
    """Base exception for all aiomobilitydatabase errors.

    ``aiomobilitydatabase.feeds.MobilityFeedsError`` (the feeds subpackage's
    own base exception) subclasses this, so catching ``MobilityDatabaseError``
    catches errors from both the catalog client and the feeds layer.
    """


class MobilityDatabaseConnectionError(MobilityDatabaseError):
    """The API could not be reached or returned an invalid payload."""


class MobilityDatabaseAuthenticationError(MobilityDatabaseError):
    """Authentication failed (invalid refresh token or unauthorized request).

    ``status`` and ``body`` carry the API's answer when there was one. The
    body stays out of the message: an error page can echo the request, and
    the refresh token with it.
    """

    def __init__(
        self, message: str, *, status: int | None = None, body: str | None = None
    ) -> None:
        """Initialize with a message and, when the API answered, its answer."""
        super().__init__(message)
        self.status = status
        self.body = body


class MobilityDatabaseNotFoundError(MobilityDatabaseError):
    """The requested resource was not found (HTTP 404)."""


class MobilityDatabaseRateLimitError(MobilityDatabaseError):
    """The API rate limit was exceeded (HTTP 429)."""


class MobilityDatabaseApiError(MobilityDatabaseError):
    """The API answered with an unexpected status (a 4xx/5xx error, or no answer)."""

    def __init__(self, status: int, body: str) -> None:
        """Initialize with the HTTP status and the response body.

        The body is an attribute, never part of the message: an error page
        can echo the request, the access token with it.
        """
        super().__init__(f"API error {status}")
        self.status = status
        self.body = body
