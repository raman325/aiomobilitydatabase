"""Models for the Mobility Database catalog API."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from mashumaro import DataClassDictMixin


class DataType(StrEnum):
    """Type of data a feed provides."""

    GTFS = "gtfs"
    GTFS_RT = "gtfs_rt"
    GBFS = "gbfs"


class FeedStatus(StrEnum):
    """Lifecycle status of a feed."""

    ACTIVE = "active"
    DEPRECATED = "deprecated"
    INACTIVE = "inactive"
    DEVELOPMENT = "development"
    FUTURE = "future"


class EntityType(StrEnum):
    """GTFS Realtime entity type."""

    VEHICLE_POSITIONS = "vp"
    TRIP_UPDATES = "tu"
    SERVICE_ALERTS = "sa"


class LocationType(StrEnum):
    """Type of a location in location search results."""

    COUNTRY = "country"
    SUBDIVISION = "subdivision"
    MUNICIPALITY = "municipality"


class GbfsVersionSource(StrEnum):
    """Origin of GBFS version information."""

    AUTODISCOVERY = "autodiscovery"
    GBFS_VERSIONS = "gbfs_versions"


class BoundingFilterMethod(StrEnum):
    """Filtering method for bounding-box queries."""

    COMPLETELY_ENCLOSED = "completely_enclosed"
    PARTIALLY_ENCLOSED = "partially_enclosed"
    DISJOINT = "disjoint"


class SortOrder(StrEnum):
    """Sort order for list endpoints that support it."""

    ASC = "asc"
    DESC = "desc"


class LicenseRuleType(StrEnum):
    """Type of a license rule."""

    PERMISSION = "permission"
    CONDITION = "condition"
    LIMITATION = "limitation"


@dataclass
class Redirect(DataClassDictMixin):
    """A feed redirect to a replacement feed ID."""

    target_id: str | None = None
    comment: str | None = None


@dataclass
class ExternalId(DataClassDictMixin):
    """An ID for the feed in an external or legacy database."""

    external_id: str | None = None
    source: str | None = None


@dataclass
class SourceInfo(DataClassDictMixin):
    """Information about the feed producer's source URL and license."""

    producer_url: str | None = None
    is_producer_url_unstable: bool | None = None
    authentication_type: int | None = None
    authentication_info_url: str | None = None
    api_key_parameter_name: str | None = None
    license_url: str | None = None
    license_id: str | None = None
    license_is_spdx: bool | None = None
    license_notes: str | None = None
    license_tags: list[str] | None = None


@dataclass
class BoundingBox(DataClassDictMixin):
    """Geographic bounding box of a dataset."""

    minimum_latitude: float | None = None
    maximum_latitude: float | None = None
    minimum_longitude: float | None = None
    maximum_longitude: float | None = None


@dataclass
class Location(DataClassDictMixin):
    """Geographic location served by a feed."""

    country_code: str | None = None
    country: str | None = None
    subdivision_name: str | None = None
    municipality: str | None = None


@dataclass
class FeedRelatedLink(DataClassDictMixin):
    """A link related to a feed."""

    code: str | None = None
    description: str | None = None
    url: str | None = None
    created_at: datetime | None = None


@dataclass
class ValidationReportSummary(DataClassDictMixin):
    """Validation counts embedded in a latest-dataset summary."""

    features: list[str] | None = None
    total_error: int | None = None
    total_warning: int | None = None
    total_info: int | None = None
    unique_error_count: int | None = None
    unique_warning_count: int | None = None
    unique_info_count: int | None = None


@dataclass
class LatestDataset(DataClassDictMixin):
    """Summary of the latest dataset for a GTFS feed."""

    id: str | None = None
    hosted_url: str | None = None
    bounding_box: BoundingBox | None = None
    downloaded_at: datetime | None = None
    hash: str | None = None
    hash_md5: str | None = None
    service_date_range_start: datetime | None = None
    service_date_range_end: datetime | None = None
    agency_timezone: str | None = None
    zipped_folder_size_mb: float | None = None
    unzipped_folder_size_mb: float | None = None
    validation_report: ValidationReportSummary | None = None


@dataclass
class Feed(DataClassDictMixin):
    """Common feed fields shared by all feed types."""

    id: str | None = None
    data_type: DataType | None = None
    created_at: datetime | None = None
    external_ids: list[ExternalId] | None = None
    provider: str | None = None
    feed_contact_email: str | None = None
    source_info: SourceInfo | None = None
    redirects: list[Redirect] | None = None
    status: FeedStatus | None = None
    official: bool | None = None
    official_updated_at: datetime | None = None
    feed_name: str | None = None
    note: str | None = None
    related_links: list[FeedRelatedLink] | None = None


@dataclass
class GtfsFeed(Feed):
    """A GTFS schedule feed."""

    locations: list[Location] | None = None
    latest_dataset: LatestDataset | None = None
    bounding_box: BoundingBox | None = None
    visualization_dataset_id: str | None = None


@dataclass
class GtfsRtFeed(Feed):
    """A GTFS Realtime feed."""

    entity_types: list[EntityType] | None = None
    feed_references: list[str] | None = None
    locations: list[Location] | None = None


@dataclass
class GbfsEndpoint(DataClassDictMixin):
    """An endpoint available in a GBFS version."""

    name: str | None = None
    url: str | None = None
    language: str | None = None
    is_feature: bool | None = None


@dataclass
class GbfsValidationReport(DataClassDictMixin):
    """A validation report for a GBFS feed version."""

    validated_at: datetime | None = None
    total_error: int | None = None
    report_summary_url: str | None = None
    validator_version: str | None = None


@dataclass
class GbfsVersion(DataClassDictMixin):
    """A GBFS specification version supported by a feed."""

    version: str | None = None
    created_at: datetime | None = None
    last_updated_at: datetime | None = None
    source: GbfsVersionSource | None = None
    endpoints: list[GbfsEndpoint] | None = None
    latest_validation_report: GbfsValidationReport | None = None


@dataclass
class GbfsFeed(Feed):
    """A GBFS feed.

    The spec derives GbfsFeed from BasicFeed (without status/official/related
    fields); sharing the Feed base here is harmless since all fields are
    optional and unknown keys are ignored.
    """

    locations: list[Location] | None = None
    system_id: str | None = None
    provider_url: str | None = None
    versions: list[GbfsVersion] | None = None
    bounding_box: BoundingBox | None = None
    bounding_box_generated_at: datetime | None = None


@dataclass
class SearchFeedItemResult(DataClassDictMixin):
    """A single feed result from the search endpoint (union of feed types)."""

    id: str
    data_type: DataType
    status: FeedStatus
    created_at: datetime | None = None
    official: bool | None = None
    external_ids: list[ExternalId] | None = None
    provider: str | None = None
    feed_name: str | None = None
    note: str | None = None
    feed_contact_email: str | None = None
    source_info: SourceInfo | None = None
    redirects: list[Redirect] | None = None
    locations: list[Location] | None = None
    latest_dataset: LatestDataset | None = None
    entity_types: list[EntityType] | None = None
    versions: list[str] | None = None
    feed_references: list[str] | None = None


@dataclass
class SearchResults(DataClassDictMixin):
    """Response from the feed search endpoint."""

    total: int | None = None
    results: list[SearchFeedItemResult] = field(default_factory=list)


@dataclass
class ValidationReport(DataClassDictMixin):
    """Full validation report for a GTFS dataset."""

    validated_at: datetime | None = None
    features: list[str] | None = None
    validator_version: str | None = None
    total_error: int | None = None
    total_warning: int | None = None
    total_info: int | None = None
    unique_error_count: int | None = None
    unique_warning_count: int | None = None
    unique_info_count: int | None = None
    url_json: str | None = None
    url_html: str | None = None


@dataclass
class GtfsDataset(DataClassDictMixin):
    """A GTFS dataset for a feed."""

    id: str | None = None
    feed_id: str | None = None
    hosted_url: str | None = None
    note: str | None = None
    downloaded_at: datetime | None = None
    hash: str | None = None
    hash_md5: str | None = None
    bounding_box: BoundingBox | None = None
    validation_report: ValidationReport | None = None
    service_date_range_start: datetime | None = None
    service_date_range_end: datetime | None = None
    agency_timezone: str | None = None
    zipped_folder_size_mb: float | None = None
    unzipped_folder_size_mb: float | None = None


@dataclass
class GtfsFeedAvailabilityCheck(DataClassDictMixin):
    """A single availability check for a GTFS feed."""

    checked_at: datetime
    success: bool
    request_method: str
    status_code: int | None = None
    latency_ms: float | None = None
    error_type: str | None = None


@dataclass
class GtfsFeedAvailability(DataClassDictMixin):
    """Availability check history for a GTFS feed."""

    feed_id: str
    total: int
    offset: int
    limit: int
    checks: list[GtfsFeedAvailabilityCheck] = field(default_factory=list)


@dataclass
class LocationSearchResult(DataClassDictMixin):
    """A single location from the locations search endpoint."""

    location_id: int | None = None
    parent_location_id: int | None = None
    name: str | None = None
    alt_name: str | None = None
    location_type: LocationType | None = None
    country_name: str | None = None
    country_code: str | None = None
    subdivision_name: str | None = None
    subdivision_code: str | None = None
    path_names: list[str] | None = None
    display_name: str | None = None


@dataclass
class LocationSearchResults(DataClassDictMixin):
    """Response from the locations search endpoint."""

    total: int | None = None
    results: list[LocationSearchResult] = field(default_factory=list)


@dataclass
class LicenseRule(DataClassDictMixin):
    """A rule (permission/condition/limitation) of a license."""

    name: str | None = None
    label: str | None = None
    description: str | None = None
    type: LicenseRuleType | None = None


@dataclass
class License(DataClassDictMixin):
    """A license in the Mobility Database."""

    id: str | None = None
    type: str | None = None
    is_spdx: bool | None = None
    name: str | None = None
    url: str | None = None
    description: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    license_tags: list[str] | None = None


@dataclass
class LicenseWithRules(License):
    """A license including its rules."""

    license_rules: list[LicenseRule] | None = None


@dataclass
class MatchingLicense(DataClassDictMixin):
    """A license matched from a license URL."""

    license_id: str | None = None
    license_url: str | None = None
    normalized_url: str | None = None
    match_type: str | None = None
    confidence: float | None = None
    spdx_id: str | None = None
    matched_name: str | None = None
    matched_catalog_url: str | None = None
    matched_source: str | None = None
    notes: str | None = None
    regional_id: str | None = None


@dataclass
class Metadata(DataClassDictMixin):
    """Metadata about the API itself."""

    version: str | None = None
    commit_hash: str | None = None


@dataclass
class AccessToken(DataClassDictMixin):
    """An access token response from the token endpoint."""

    access_token: str
    expiration_datetime_utc: datetime
    token_type: str | None = None
