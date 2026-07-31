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
