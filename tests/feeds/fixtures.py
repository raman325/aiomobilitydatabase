"""Loads feeds test fixtures from the ``fixtures/`` data directory.

Static payloads (catalog API responses, GBFS documents, GTFS static files)
live as data files under ``fixtures/`` rather than as inline Python literals,
so the fixture *data* stays separate from the fixture *code*. Only the
GTFS-RT protobuf builders (``rt_fixture.py``) stay as procedural Python code
-- they're parameterized (varying epoch/stop/count per test) rather than
static dumps, and protobuf binary isn't a practical flat-file format.
"""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from typing import Any

_DATA_DIR = Path(__file__).parent / "fixtures"
_GTFS_DIR = _DATA_DIR / "gtfs"


def _load_json(name: str) -> dict[str, Any]:
    return json.loads((_DATA_DIR / f"{name}.json").read_text(encoding="utf-8"))


# -- Catalog API payloads (mocked aiomobilitydatabase responses) -----------

TOKEN_RESPONSE: dict[str, Any] = _load_json("token_response")
GTFS_FEED: dict[str, Any] = _load_json("gtfs_feed")
GTFS_RT_FEED: dict[str, Any] = _load_json("gtfs_rt_feed")
GBFS_FEED: dict[str, Any] = _load_json("gbfs_feed")


def with_base(payload: dict[str, Any], base_url: str) -> dict[str, Any]:
    """Deep-replace the __MOCK__ marker with the live mock server URL."""
    return json.loads(json.dumps(payload).replace("__MOCK__", base_url))


# -- GBFS 2.3 and 3.0 JSON documents ----------------------------------------

SYSTEM_INFO_23: dict[str, Any] = _load_json("system_info_23")
STATION_INFO_23: dict[str, Any] = _load_json("station_info_23")
STATION_STATUS_23: dict[str, Any] = _load_json("station_status_23")
FREE_BIKE_STATUS_23: dict[str, Any] = _load_json("free_bike_status_23")
SYSTEM_INFO_30: dict[str, Any] = _load_json("system_info_30")
VEHICLE_STATUS_30: dict[str, Any] = _load_json("vehicle_status_30")


# -- The miniature GTFS zip used across static-index tests ------------------
# fixtures/gtfs/*.txt hold the real GTFS filenames/content on disk.
#
# fixtures/gtfs/calendar_dates.txt: SPECIAL is added on 2026-07-04 (a one-off
# holiday) AND on 2026-07-31 (the fixture's own "today" in service-day
# tests): the latter lets test_lookahead_crossing_midnight_catches_tomorrow
# see T4 alongside T1/T2 on that Friday, per the plan's inline comment for
# that test.

_GTFS_FILENAMES = (
    "agency.txt",
    "stops.txt",
    "routes.txt",
    "trips.txt",
    "stop_times.txt",
    "calendar.txt",
    "calendar_dates.txt",
)
_FILES: dict[str, str] = {
    name: (_GTFS_DIR / name).read_text(encoding="utf-8") for name in _GTFS_FILENAMES
}


def build_gtfs_zip_bytes(omit: frozenset[str] = frozenset()) -> bytes:
    """Return the fixture GTFS zip, optionally omitting named files."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in _FILES.items():
            if name not in omit:
                zf.writestr(name, content)
    return buf.getvalue()
