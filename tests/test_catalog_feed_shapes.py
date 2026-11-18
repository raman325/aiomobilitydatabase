"""The spec's feed shapes: BasicFeed under GbfsFeed, ``seasonal``, the seal."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from typing import Any

import pytest

from aiomobilitydatabase.client import MobilityDatabaseClient, _one
from aiomobilitydatabase.exceptions import MobilityDatabaseConnectionError
from aiomobilitydatabase.models import (
    BasicFeed,
    Feed,
    GbfsFeed,
    GtfsFeed,
    SearchFeedItemResult,
)

from tests.fixtures import (
    GBFS_FEED,
    GTFS_FEED,
    RELIABILITY_SEAL,
    SEARCH_RESPONSE,
    TOKEN_RESPONSE,
)
from tests.mock_server import MockApi


def test_a_gbfs_feed_is_a_basic_feed_not_a_feed() -> None:
    """The spec builds GbfsFeed on BasicFeed: no status, official flag,
    seasonal flag, name, note or related links."""
    feed = GbfsFeed.from_dict(GBFS_FEED)
    assert isinstance(feed, BasicFeed)
    assert not isinstance(feed, Feed)
    feed_only = {f.name for f in dataclasses.fields(Feed)} - {
        f.name for f in dataclasses.fields(BasicFeed)
    }
    assert feed_only == {
        "status",
        "official",
        "official_updated_at",
        "seasonal",
        "feed_name",
        "note",
        "related_links",
    }
    assert not {f.name for f in dataclasses.fields(GbfsFeed)} & feed_only


@pytest.mark.parametrize("model", [GtfsFeed, SearchFeedItemResult])
def test_seasonal_defaults_to_false_and_the_seal_decodes(model: Any) -> None:
    payload = GTFS_FEED if model is GtfsFeed else SEARCH_RESPONSE["results"][0]
    absent = _one(model, {k: v for k, v in payload.items() if k != "seasonal"})
    assert absent.seasonal is False  # "Defaults to false"
    assert absent.reliability_seal is None
    marked = _one(
        model, {**payload, "seasonal": True, "reliability_seal": RELIABILITY_SEAL}
    )
    assert marked.seasonal is True
    assert marked.reliability_seal.on_probation is True
    assert marked.reliability_seal.lost_at == datetime(2026, 8, 1, tzinfo=UTC)
    with pytest.raises(MobilityDatabaseConnectionError, match="seasonal"):
        _one(model, {**payload, "seasonal": None})  # not nullable
    with pytest.raises(MobilityDatabaseConnectionError, match="reliability_seal"):
        _one(
            model, {**payload, "reliability_seal": {"on_probation": False}}
        )  # no has_seal


async def test_search_filters_on_the_seal(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/search", payload=SEARCH_RESPONSE)
    await client.search_feeds(has_seal=True)
    search = next(r for r in mock_api.requests if r.path == "/v1/search")
    assert search.query == {"has_seal": "true"}
