"""Tests for the exception hierarchy."""

import pytest

from aiomobilitydatabase.feeds.exceptions import (
    FeedParseError,
    MobilityFeedsError,
    SourceAuthenticationError,
    SourceConnectionError,
    StaticDataUnavailableError,
)


@pytest.mark.parametrize(
    "exc_class",
    [
        SourceConnectionError,
        SourceAuthenticationError,
        FeedParseError,
        StaticDataUnavailableError,
    ],
)
def test_hierarchy(exc_class: type[MobilityFeedsError]) -> None:
    assert issubclass(exc_class, MobilityFeedsError)


def test_source_connection_error_status() -> None:
    err = SourceConnectionError("boom", status=503)
    assert err.status == 503
    assert "boom" in str(err)
    assert SourceConnectionError("net down").status is None
