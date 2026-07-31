"""The public API surface must be importable from the package root."""

import aiomobilitydatabase


def test_public_exports() -> None:
    for name in (
        "MobilityDatabaseClient",
        "Feed",
        "GtfsFeed",
        "GtfsRtFeed",
        "GbfsFeed",
        "GtfsDataset",
        "SearchResults",
        "DataType",
        "FeedStatus",
        "EntityType",
        "RequestMethod",
        "MobilityDatabaseError",
        "MobilityDatabaseAuthenticationError",
        "__version__",
    ):
        assert hasattr(aiomobilitydatabase, name), name
        assert name in aiomobilitydatabase.__all__, name
