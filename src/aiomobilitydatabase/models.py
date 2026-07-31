"""Models for the Mobility Database catalog API."""

from __future__ import annotations

from dataclasses import dataclass
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
