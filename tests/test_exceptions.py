"""Tests for the exception hierarchy."""

import pytest

from aiomobilitydatabase.exceptions import (
    MobilityDatabaseApiError,
    MobilityDatabaseAuthenticationError,
    MobilityDatabaseConnectionError,
    MobilityDatabaseError,
    MobilityDatabaseNotFoundError,
    MobilityDatabaseRateLimitError,
)


@pytest.mark.parametrize(
    "exc_class",
    [
        MobilityDatabaseConnectionError,
        MobilityDatabaseAuthenticationError,
        MobilityDatabaseNotFoundError,
        MobilityDatabaseRateLimitError,
        MobilityDatabaseApiError,
    ],
)
def test_hierarchy(exc_class: type[MobilityDatabaseError]) -> None:
    assert issubclass(exc_class, MobilityDatabaseError)


def test_api_error_attributes() -> None:
    err = MobilityDatabaseApiError(500, '{"error": "boom"}')
    assert err.status == 500
    assert err.body == '{"error": "boom"}'
    assert "500" in str(err)
