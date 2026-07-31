"""Tests for API models."""

from datetime import UTC, datetime

from aiomobilitydatabase.models import (
    AccessToken,
    BoundingBox,
    DataType,
    EntityType,
    FeedStatus,
    GbfsFeed,
    GbfsVersionSource,
    GtfsDataset,
    GtfsFeed,
    GtfsFeedAvailability,
    GtfsRtFeed,
    LatestDataset,
    License,
    LicenseRuleType,
    LicenseWithRules,
    Location,
    LocationSearchResults,
    LocationType,
    MatchingLicense,
    Metadata,
    SearchResults,
    SourceInfo,
)
from tests.fixtures import (
    AVAILABILITY_RESPONSE,
    BOUNDING_BOX,
    GBFS_FEED,
    GTFS_DATASET,
    GTFS_FEED,
    GTFS_RT_FEED,
    LATEST_DATASET,
    LICENSE,
    LICENSE_WITH_RULES,
    LOCATION,
    LOCATION_SEARCH_RESPONSE,
    MATCHING_LICENSE,
    METADATA,
    SEARCH_RESPONSE,
    SOURCE_INFO,
    TOKEN_RESPONSE,
)


def test_enums() -> None:
    assert DataType("gtfs_rt") is DataType.GTFS_RT
    assert FeedStatus("active") is FeedStatus.ACTIVE
    assert EntityType("vp") is EntityType.VEHICLE_POSITIONS


def test_source_info() -> None:
    info = SourceInfo.from_dict(SOURCE_INFO)
    assert info.producer_url == "https://ladotbus.com/gtfs"
    assert info.authentication_type == 2
    assert info.is_producer_url_unstable is None
    assert info.license_tags == ["family:ODC"]


def test_location_and_bounding_box() -> None:
    loc = Location.from_dict(LOCATION)
    assert loc.municipality == "Los Angeles"
    box = BoundingBox.from_dict(BOUNDING_BOX)
    assert box.minimum_latitude == 33.721601


def test_latest_dataset() -> None:
    dataset = LatestDataset.from_dict(LATEST_DATASET)
    assert dataset.id == "mdb-1210-202402121801"
    assert dataset.downloaded_at == datetime(2026, 7, 31, 0, 45, 30, 828071, tzinfo=UTC)
    assert dataset.bounding_box is not None
    assert dataset.bounding_box.maximum_latitude == 34.323077
    assert dataset.validation_report is not None
    assert dataset.validation_report.total_error == 10


def test_unknown_fields_ignored() -> None:
    loc = Location.from_dict({**LOCATION, "brand_new_upstream_field": "x"})
    assert loc.country_code == "US"


def test_gtfs_feed() -> None:
    feed = GtfsFeed.from_dict(GTFS_FEED)
    assert feed.id == "mdb-1210"
    assert feed.data_type is DataType.GTFS
    assert feed.status is FeedStatus.ACTIVE
    assert feed.official is True
    assert feed.source_info is not None
    assert feed.source_info.producer_url == "https://ladotbus.com/gtfs"
    assert feed.latest_dataset is not None
    assert feed.latest_dataset.hosted_url is not None
    assert feed.locations is not None
    assert feed.locations[0].country_code == "US"
    assert feed.related_links is not None
    assert feed.related_links[0].code == "next_1"
    assert feed.related_links[0].created_at is not None


def test_gtfs_rt_feed() -> None:
    feed = GtfsRtFeed.from_dict(GTFS_RT_FEED)
    assert feed.data_type is DataType.GTFS_RT
    assert feed.entity_types == [EntityType.VEHICLE_POSITIONS, EntityType.TRIP_UPDATES]
    assert feed.feed_references == ["mdb-1210"]
    assert feed.redirects is None


def test_gbfs_feed() -> None:
    feed = GbfsFeed.from_dict(GBFS_FEED)
    assert feed.system_id == "system-1234"
    assert feed.versions is not None
    version = feed.versions[0]
    assert version.source is GbfsVersionSource.AUTODISCOVERY
    assert version.endpoints is not None
    assert version.endpoints[0].name == "system_information"
    assert version.latest_validation_report is not None
    assert version.latest_validation_report.total_error == 0


def test_search_results() -> None:
    results = SearchResults.from_dict(SEARCH_RESPONSE)
    assert results.total == 1
    item = results.results[0]
    assert item.id == "mdb-1210"
    assert item.data_type is DataType.GTFS
    assert item.status is FeedStatus.ACTIVE
    assert item.latest_dataset is not None


def test_gtfs_dataset() -> None:
    dataset = GtfsDataset.from_dict(GTFS_DATASET)
    assert dataset.feed_id == "mdb-10"
    assert dataset.validation_report is not None
    assert dataset.validation_report.validator_version == "4.2.0"
    assert dataset.bounding_box is None


def test_availability() -> None:
    availability = GtfsFeedAvailability.from_dict(AVAILABILITY_RESPONSE)
    assert availability.feed_id == "mdb-123"
    assert availability.total == 42
    check = availability.checks[0]
    assert check.success is True
    assert check.request_method == "HEAD"
    assert check.error_type is None


def test_location_search() -> None:
    results = LocationSearchResults.from_dict(LOCATION_SEARCH_RESPONSE)
    result = results.results[0]
    assert result.location_id == 175905
    assert result.location_type is LocationType.MUNICIPALITY
    assert result.path_names == ["Canada", "Quebec", "Montréal"]


def test_licenses() -> None:
    license_ = License.from_dict(LICENSE)
    assert license_.id == "0BSD"
    with_rules = LicenseWithRules.from_dict(LICENSE_WITH_RULES)
    assert with_rules.license_rules is not None
    assert with_rules.license_rules[0].type is LicenseRuleType.PERMISSION
    matching = MatchingLicense.from_dict(MATCHING_LICENSE)
    assert matching.confidence == 0.99


def test_metadata_and_token() -> None:
    metadata = Metadata.from_dict(METADATA)
    assert metadata.version == "1.0.0"
    token = AccessToken.from_dict(TOKEN_RESPONSE)
    assert token.access_token == "test-access-token"
    assert token.expiration_datetime_utc.year == 2030
