"""Exceptions for aiomobilitydatabase."""

from __future__ import annotations


class MobilityDatabaseError(Exception):
    """Base exception for all aiomobilitydatabase errors."""


class MobilityDatabaseConnectionError(MobilityDatabaseError):
    """The API could not be reached or returned an invalid payload."""


class MobilityDatabaseAuthenticationError(MobilityDatabaseError):
    """Authentication failed (invalid refresh token or unauthorized request)."""


class MobilityDatabaseNotFoundError(MobilityDatabaseError):
    """The requested resource was not found (HTTP 404)."""


class MobilityDatabaseRateLimitError(MobilityDatabaseError):
    """The API rate limit was exceeded (HTTP 429)."""


class MobilityDatabaseApiError(MobilityDatabaseError):
    """The API returned an unexpected 4xx/5xx error."""

    def __init__(self, status: int, body: str) -> None:
        """Initialize with the HTTP status and raw response body."""
        super().__init__(f"API error {status}: {body}")
        self.status = status
        self.body = body
