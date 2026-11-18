"""Public catalog export rows for tests: the export's exact header."""

from __future__ import annotations

import csv
import io

# mobility-feed-api functions-python/export_csv/src/main.py, column order.
HEADER = [
    "id",
    "data_type",
    "entity_type",
    "location.country_code",
    "location.subdivision_name",
    "location.municipality",
    "provider",
    "is_official",
    "name",
    "note",
    "feed_contact_email",
    "static_reference",
    "urls.direct_download",
    "urls.authentication_type",
    "urls.authentication_info",
    "urls.api_key_parameter_name",
    "urls.latest",
    "urls.license",
    "location.bounding_box.minimum_latitude",
    "location.bounding_box.maximum_latitude",
    "location.bounding_box.minimum_longitude",
    "location.bounding_box.maximum_longitude",
    "location.bounding_box.extracted_on",
    "status",
    "features",
    "redirect.id",
    "redirect.comment",
]


def export(*rows: dict[str, str], header: list[str] | None = None) -> bytes:
    """An export body: the header, then each row (missing cells empty)."""
    out = io.StringIO()
    writer = csv.DictWriter(out, header or HEADER, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return out.getvalue().encode()


def box(min_lat: str, max_lat: str, min_lon: str, max_lon: str) -> dict[str, str]:
    """The four bounding-box cells."""
    return {
        "location.bounding_box.minimum_latitude": min_lat,
        "location.bounding_box.maximum_latitude": max_lat,
        "location.bounding_box.minimum_longitude": min_lon,
        "location.bounding_box.maximum_longitude": max_lon,
    }


GTFS_ROW = {
    "id": "mdb-10",
    "data_type": "gtfs",
    "location.country_code": "US",
    "location.subdivision_name": "New York",
    "location.municipality": "New York",
    "provider": "MTA New York City Transit",
    "is_official": "True",
    "name": "NYC Subway",
    "note": "Line one\nline two, with a comma",
    "feed_contact_email": "gtfs@example.com",
    "urls.direct_download": "https://example.com/subway.zip",
    "urls.authentication_type": "0",
    "urls.latest": "https://files.mobilitydatabase.org/mdb-10/latest.zip",
    "urls.license": "https://example.com/license",
    **box("40.5", "40.9", "-74.3", "-73.7"),
    "location.bounding_box.extracted_on": "2026-11-01 10:00:00+00:00",
    "status": "active",
    "features": "Shapes|Stops Wheelchair Accessibility",
    "redirect.id": "mdb-11|mdb-12",
    "redirect.comment": "merged|",
}

GTFS_RT_ROW = {
    "id": "mdb-20",
    "data_type": "gtfs_rt",
    "entity_type": "sa|tu|vp",
    "provider": "MTA New York City Transit",
    "is_official": "False",
    "feed_contact_email": "",
    "static_reference": "mdb-10|mdb-13",
    "urls.direct_download": "https://example.com/rt",
    "urls.authentication_type": "2",
    "urls.authentication_info": "https://example.com/keys",
    "urls.api_key_parameter_name": "x-api-key",
    **box("40.5", "40.9", "-74.3", "-73.7"),
    "status": "active",
}

GBFS_ROW = {
    "id": "gbfs-citibike",
    "data_type": "gbfs",
    "location.country_code": "US",
    "provider": "Citi Bike",
    "name": "Citi Bike NYC",
    "note": "a note",
    "urls.direct_download": "https://gbfs.example.com/gbfs.json",
    "urls.authentication_type": "0",
    "urls.license": "https://example.com/gbfs-license",
    **box("40.6", "40.9", "-74.1", "-73.8"),
    "location.bounding_box.extracted_on": "2025-02-03 04:05:06+00:00",
    "status": "active",
}
