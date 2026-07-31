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
