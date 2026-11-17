"""Sample API payloads derived from the OpenAPI spec examples."""

from typing import Any

SOURCE_INFO: dict[str, Any] = {
    "producer_url": "https://ladotbus.com/gtfs",
    "is_producer_url_unstable": None,
    "authentication_type": 2,
    "authentication_info_url": "https://apidevelopers.ladottransit.com",
    "api_key_parameter_name": "Ocp-Apim-Subscription-Key",
    "license_url": "https://www.ladottransit.com/dla.html",
    "license_id": "0BSD",
    "license_is_spdx": True,
    "license_notes": None,
    "license_tags": ["family:ODC"],
}

LOCATION: dict[str, Any] = {
    "country_code": "US",
    "country": "United States",
    "subdivision_name": "California",
    "municipality": "Los Angeles",
}

BOUNDING_BOX: dict[str, Any] = {
    "minimum_latitude": 33.721601,
    "maximum_latitude": 34.323077,
    "minimum_longitude": -118.882829,
    "maximum_longitude": -118.131748,
}

LATEST_DATASET: dict[str, Any] = {
    "id": "mdb-1210-202402121801",
    "hosted_url": "https://storage.googleapis.com/mdb-1210-202402121801.zip",
    "bounding_box": BOUNDING_BOX,
    "downloaded_at": "2026-07-31T00:45:30.828071Z",
    "hash": "ad3805c4941cd37881ff40c342e831b5f5224f3d8a9a2ec3ac197d3652c78e42",
    "hash_md5": "098f6bcd4621d373cade4e832627b4f6",
    "service_date_range_start": "2026-05-20T07:00:00Z",
    "service_date_range_end": "2028-01-01T07:59:00Z",
    "agency_timezone": "America/Los_Angeles",
    "zipped_folder_size_mb": 100.2,
    "unzipped_folder_size_mb": 200.5,
    "validation_report": {
        "features": ["Shapes", "Headsigns"],
        "total_error": 10,
        "total_warning": 20,
        "total_info": 30,
        "unique_error_count": 1,
        "unique_warning_count": 2,
        "unique_info_count": 3,
    },
}

GTFS_FEED: dict[str, Any] = {
    "id": "mdb-1210",
    "data_type": "gtfs",
    "created_at": "2023-07-10T22:06:00Z",
    "external_ids": [{"external_id": "1210", "source": "mdb"}],
    "provider": "Los Angeles Department of Transportation",
    "feed_contact_email": "someEmail@ladotbus.com",
    "source_info": SOURCE_INFO,
    "redirects": [],
    "status": "active",
    "official": True,
    "official_updated_at": "2023-07-10T22:06:00Z",
    "feed_name": "Bus",
    "note": None,
    "related_links": [
        {
            "code": "next_1",
            "description": "URL for a future feed version.",
            "url": "https://example.com/next",
            "created_at": "2023-07-10T22:06:00Z",
        }
    ],
    "locations": [LOCATION],
    "latest_dataset": LATEST_DATASET,
    "bounding_box": BOUNDING_BOX,
    "visualization_dataset_id": "mdb-1210-202402121801",
}

GTFS_RT_FEED: dict[str, Any] = {
    "id": "mdb-1211",
    "data_type": "gtfs_rt",
    "created_at": "2023-07-10T22:06:00Z",
    "external_ids": [],
    "provider": "LADOT",
    "feed_contact_email": None,
    "source_info": SOURCE_INFO,
    "redirects": None,
    "status": "active",
    "official": None,
    "official_updated_at": None,
    "feed_name": None,
    "note": None,
    "related_links": None,
    "entity_types": ["vp", "tu"],
    "feed_references": ["mdb-1210"],
    "locations": [LOCATION],
}

GBFS_FEED: dict[str, Any] = {
    "id": "gbfs-citibike",
    "data_type": "gbfs",
    "created_at": "2023-07-10T22:06:00Z",
    "external_ids": [],
    "provider": "Citi Bike",
    "feed_contact_email": None,
    "source_info": SOURCE_INFO,
    "redirects": [],
    "locations": [LOCATION],
    "system_id": "system-1234",
    "provider_url": "https://www.citybikenyc.com/",
    "versions": [
        {
            "version": "2.3",
            "created_at": "2023-07-10T22:06:00Z",
            "last_updated_at": "2023-07-10T22:06:00Z",
            "source": "autodiscovery",
            "endpoints": [
                {
                    "name": "system_information",
                    "url": "https://gbfs.citibikenyc.com/gbfs/system_information.json",
                    "language": "en",
                    "is_feature": False,
                }
            ],
            "latest_validation_report": {
                "validated_at": "2023-07-10T22:06:00Z",
                "total_error": 0,
                "report_summary_url": "https://example.com/report.json",
                "validator_version": "1.0.13",
            },
        }
    ],
    "bounding_box": BOUNDING_BOX,
    "bounding_box_generated_at": "2023-07-10T22:06:00Z",
}

SEARCH_RESPONSE: dict[str, Any] = {
    "total": 1,
    "results": [
        {
            "id": "mdb-1210",
            "data_type": "gtfs",
            "status": "active",
            "created_at": "2023-07-10T22:06:00Z",
            "official": True,
            "external_ids": [],
            "provider": "LADOT",
            "feed_name": "Bus",
            "note": None,
            "feed_contact_email": None,
            "source_info": SOURCE_INFO,
            "redirects": None,
            "locations": [LOCATION],
            "latest_dataset": LATEST_DATASET,
            "entity_types": None,
            "versions": None,
            "feed_references": None,
        }
    ],
}

GTFS_DATASET: dict[str, Any] = {
    "id": "mdb-10-202402080058",
    "feed_id": "mdb-10",
    "hosted_url": "https://storage.googleapis.com/datasets/mdb-10.zip",
    "note": None,
    "downloaded_at": "2026-07-31T00:45:30.828071Z",
    "hash": "6497e85e34390b8b377130881f2f10ec29c18a80dd6005d504a2038cdd00aa71",
    "hash_md5": "098f6bcd4621d373cade4e832627b4f6",
    "bounding_box": None,
    "validation_report": {
        "validated_at": "2023-07-10T22:06:00Z",
        "features": ["Shapes"],
        "validator_version": "4.2.0",
        "total_error": 10,
        "total_warning": 20,
        "total_info": 30,
        "unique_error_count": 1,
        "unique_warning_count": 2,
        "unique_info_count": 3,
        "url_json": "https://example.com/report.json",
        "url_html": "https://example.com/report.html",
    },
    "service_date_range_start": "2026-05-20T07:00:00Z",
    "service_date_range_end": "2028-01-01T07:59:00Z",
    "agency_timezone": "America/Los_Angeles",
    "zipped_folder_size_mb": 100.2,
    "unzipped_folder_size_mb": 200.5,
}

AVAILABILITY_RESPONSE: dict[str, Any] = {
    "feed_id": "mdb-123",
    "total": 42,
    "offset": 0,
    "limit": 100,
    "checks": [
        {
            "checked_at": "2026-05-14T10:00:00Z",
            "success": True,
            "request_method": "HEAD",
            "status_code": 200,
            "latency_ms": 845.3,
            "error_type": None,
        }
    ],
}

LOCATION_SEARCH_RESPONSE: dict[str, Any] = {
    "total": 1,
    "results": [
        {
            "location_id": 175905,
            "parent_location_id": 161950,
            "name": "Montréal",
            "alt_name": "City of Montréal",
            "location_type": "municipality",
            "country_name": "Canada",
            "country_code": "CA",
            "subdivision_name": "Quebec",
            "subdivision_code": "CA-QC",
            "path_names": ["Canada", "Quebec", "Montréal"],
            "display_name": "Canada, Quebec, Montréal",
        }
    ],
}

LICENSE: dict[str, Any] = {
    "id": "0BSD",
    "type": "standard",
    "is_spdx": True,
    "name": "BSD Zero Clause License",
    "url": "https://example.com/license",
    "description": "This is the 0BSD license.",
    "created_at": "2023-07-10T22:06:00Z",
    "updated_at": "2023-07-10T22:06:00Z",
    "license_tags": ["family:ODC"],
}

LICENSE_WITH_RULES: dict[str, Any] = {
    **LICENSE,
    "license_rules": [
        {
            "name": "commercial-use",
            "label": "Commercial use",
            "description": "Allows commercial use.",
            "type": "permission",
        }
    ],
}

MATCHING_LICENSE: dict[str, Any] = {
    "license_id": "CC-BY-4.0",
    "license_url": "https://creativecommons.org/licenses/by/4.0/deed.nl",
    "normalized_url": "creativecommons.org/licenses/by/4.0",
    "match_type": "heuristic",
    "confidence": 0.99,
    "spdx_id": "CC-BY-4.0",
    "matched_name": "Creative Commons Attribution 4.0 International",
    "matched_catalog_url": "https://creativecommons.org/licenses/by/4.0/legalcode",
    "matched_source": "cc-resolver",
    "notes": None,
    "regional_id": "CC-BY-4.0-nl",
}

METADATA: dict[str, Any] = {
    "version": "1.0.0",
    "commit_hash": "8635fdac4fbff025b4eaca6972fcc9504bc1552d",
}

TOKEN_RESPONSE: dict[str, Any] = {
    "access_token": "test-access-token",
    "expiration_datetime_utc": "2030-01-01T00:00:00Z",
    "token_type": "Bearer",
}

RELIABILITY_REPORT: dict[str, Any] = {
    "feed_id": "mdb-123",
    "has_seal": False,
    "seal_status": "not_granted",
    "earned_at": "2026-01-10T00:00:00Z",
    "lost_at": "2026-08-01T00:00:00Z",
    "evaluated_at": "2026-09-14T03:00:00Z",
    "on_probation": True,
    "probation_ends_at": "2027-02-01T00:00:00Z",
    "criteria": [
        {
            "criterion": criterion,
            "status": "fail" if criterion == "available" else "pass",
            "in_grace_period": False,
            "grace_period_ends_at": None,
            "on_probation": criterion == "available",
            "probation_ends_at": (
                "2027-02-01T00:00:00Z" if criterion == "available" else None
            ),
            "evaluated_at": "2026-09-14T03:00:00Z",
            "first_failure_at": (
                "2026-07-20T00:00:00Z" if criterion == "available" else None
            ),
            "last_failure_at": None,
        }
        for criterion in (
            "official",
            "stable",
            "available",
            "compliant",
            "fresh_coverage",
            "fresh_continuous",
        )
    ],
}

_COVERAGE_FILES = [
    {"name": "calendar.txt", "present": True},
    {"name": "calendar_dates.txt", "present": False},
    {"name": "feed_info.txt", "present": True},
]

COVERAGE_LATEST: dict[str, Any] = {
    "dataset_id": "mdb-123-202609010000",
    "is_latest": True,
    "downloaded_at": "2026-09-01T00:00:00Z",
    "coverage_window": {"start": "2026-09-01", "end": "2027-03-31", "days": 212},
    "coverage_window_source": "feed_info",
    "within_max_coverage_window": True,
    "service_window": {"start": "2026-09-01", "end": "2027-03-31", "days": 212},
    "feed_info_window": {"start": "2026-09-01", "end": "2027-03-31", "days": 212},
    "feed_info_matches": True,
    "previous_dataset_id": "mdb-123-202603010000",
    "overlap_days": 30,
    "gap_days": None,
    "files": _COVERAGE_FILES,
}

COVERAGE_OLDER: dict[str, Any] = {
    "dataset_id": "mdb-123-202603010000",
    "is_latest": False,
    "downloaded_at": "2026-03-01T00:00:00Z",
    "coverage_window": {"start": "2026-03-01", "end": "2026-09-30", "days": 214},
    "coverage_window_source": "service_dates",
    "within_max_coverage_window": True,
    "service_window": {"start": "2026-03-01", "end": "2026-09-30", "days": 214},
    "feed_info_window": None,
    "feed_info_matches": None,
    "previous_dataset_id": None,
    "overlap_days": None,
    "gap_days": None,
    "files": _COVERAGE_FILES,
}

CONTINUOUS_COVERAGE_RESPONSE: dict[str, Any] = {
    "feed_id": "mdb-123",
    "latest_state": {"newer": COVERAGE_LATEST, "older": COVERAGE_OLDER},
    "latest_failure": None,
    "total": 2,
    "offset": 0,
    "limit": 20,
    "items": [COVERAGE_LATEST, COVERAGE_OLDER],
}

FEED_VALIDATION_REPORT: dict[str, Any] = {
    "dataset_id": "mdb-123-202609010000",
    "is_latest": True,
    "validated_at": "2026-09-01T01:00:00Z",
    "validator_version": "7.1.0",
    "total_error": 2,
    "total_warning": 15,
    "total_info": 3,
    "unique_error_count": 1,
    "unique_warning_count": 4,
    "unique_info_count": 1,
    "url_json": "https://files.mobilitydatabase.org/mdb-123/report.json",
    "url_html": "https://files.mobilitydatabase.org/mdb-123/report.html",
    "notices": [
        {"code": "foreign_key_violation", "severity": "ERROR", "total": 2},
        {"code": "unused_shape", "severity": "WARNING", "total": 9},
    ],
}

VALIDATION_REPORTS_RESPONSE: dict[str, Any] = {
    "feed_id": "mdb-123",
    "latest": FEED_VALIDATION_REPORT,
    "total": 1,
    "offset": 0,
    "limit": 20,
    "items": [FEED_VALIDATION_REPORT],
}
