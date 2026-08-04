"""Loads feeds test fixtures from the ``data/`` directory.

Static payloads (catalog API responses, GBFS documents, GTFS static files,
GTFS-RT protobuf messages) live as data files under ``data/``, grouped by
kind, rather than as inline Python literals -- so fixture *data* stays
separate from fixture *code* and the folder layout says what everything is:

    data/catalog/   mocked aiomobilitydatabase catalog API responses
    data/gbfs/      GBFS 2.3/3.0 protocol documents
    data/gtfs/      the miniature GTFS static feed (real GTFS filenames)
    data/rt/        GTFS-RT protobuf messages (see scripts/generate_rt_fixtures.py)
"""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from typing import Any

_DATA_DIR = Path(__file__).parent / "data"
_CATALOG_DIR = _DATA_DIR / "catalog"
_GBFS_DIR = _DATA_DIR / "gbfs"
_GTFS_DIR = _DATA_DIR / "gtfs"
_RT_DIR = _DATA_DIR / "rt"


def _load_json(directory: Path, name: str) -> dict[str, Any]:
    return json.loads((directory / f"{name}.json").read_text(encoding="utf-8"))


# -- Catalog API payloads (mocked aiomobilitydatabase responses) -----------

TOKEN_RESPONSE: dict[str, Any] = _load_json(_CATALOG_DIR, "token_response")
GTFS_FEED: dict[str, Any] = _load_json(_CATALOG_DIR, "gtfs_feed")
GTFS_RT_FEED: dict[str, Any] = _load_json(_CATALOG_DIR, "gtfs_rt_feed")
GBFS_FEED: dict[str, Any] = _load_json(_CATALOG_DIR, "gbfs_feed")


def with_base(payload: dict[str, Any], base_url: str) -> dict[str, Any]:
    """Deep-replace the __MOCK__ marker with the live mock server URL."""
    return json.loads(json.dumps(payload).replace("__MOCK__", base_url))


# -- GBFS 2.3 and 3.0 JSON documents ----------------------------------------

DISCOVERY_23: dict[str, Any] = _load_json(_GBFS_DIR, "discovery_23")
DISCOVERY_30: dict[str, Any] = _load_json(_GBFS_DIR, "discovery_30")
SYSTEM_INFO_23: dict[str, Any] = _load_json(_GBFS_DIR, "system_info_23")
STATION_INFO_23: dict[str, Any] = _load_json(_GBFS_DIR, "station_info_23")
STATION_STATUS_23: dict[str, Any] = _load_json(_GBFS_DIR, "station_status_23")
FREE_BIKE_STATUS_23: dict[str, Any] = _load_json(_GBFS_DIR, "free_bike_status_23")
SYSTEM_INFO_30: dict[str, Any] = _load_json(_GBFS_DIR, "system_info_30")
VEHICLE_STATUS_30: dict[str, Any] = _load_json(_GBFS_DIR, "vehicle_status_30")


# -- The miniature GTFS zip used across static-index tests ------------------
# data/gtfs/*.txt hold the real GTFS filenames/content on disk.
#
# data/gtfs/calendar_dates.txt: SPECIAL is added on 2026-07-04 (a one-off
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


# -- Variant zip for origin→destination trip queries -------------------------
# Several arrival tests pin exact per-stop trip sets against the shared zip
# (e.g. test_added_trip_outside_queried_stops_excluded asserts S2's full row
# list), so the extra stop calls trip-query tests need live in a variant
# builder rather than the base data/gtfs files:
#   T1 gains a terminal S3 call (arrival-only)  -> S1→S3 with intermediate S2
#   T9 is a NEW reverse trip S3→S2→S1 (WKDY)    -> wrong-direction exclusion
#   T2 gains an S3 call with NO arrival time    -> NULL-arrival exclusion
#   T9's S2 call has NO departure time          -> NULL-departure exclusion
#   T3 gains an S2 call past midnight (25:40)   -> service-day boundary
_TRIP_QUERY_EXTRA_ROWS: dict[str, str] = {
    "trips.txt": "R1,WKDY,T9,Uptown\n",
    "stop_times.txt": (
        "T1,08:20:00,,S3,3\n"
        "T2,,08:45:00,S3,2\n"
        "T3,25:40:00,25:41:00,S2,2\n"
        "T9,08:05:00,08:05:30,S3,1\n"
        "T9,08:15:00,,S2,2\n"
        "T9,08:25:00,08:25:30,S1,3\n"
    ),
}


def build_trip_query_gtfs_zip_bytes() -> bytes:
    """Return the fixture GTFS zip extended for origin→destination queries."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in _FILES.items():
            zf.writestr(name, content + _TRIP_QUERY_EXTRA_ROWS.get(name, ""))
    return buf.getvalue()


# -- GTFS-RT protobuf messages ----------------------------------------------
# Regenerate via `uv run python scripts/generate_rt_fixtures.py` (repo root).

VEHICLE_POSITIONS: bytes = (_RT_DIR / "vehicle_positions.pb").read_bytes()
ALERTS: bytes = (_RT_DIR / "alerts.pb").read_bytes()
TRIP_UPDATES_BASELINE: bytes = (_RT_DIR / "trip_updates_baseline.pb").read_bytes()
TRIP_UPDATES_T1_DELAYED: bytes = (_RT_DIR / "trip_updates_t1_delayed.pb").read_bytes()
TRIP_UPDATES_T1_DEST_ARRIVAL: bytes = (
    _RT_DIR / "trip_updates_t1_dest_arrival.pb"
).read_bytes()
TRIP_UPDATES_T1_BOTH_ENDS: bytes = (
    _RT_DIR / "trip_updates_t1_both_ends.pb"
).read_bytes()
TRIP_UPDATES_T1_CANCELED: bytes = (_RT_DIR / "trip_updates_t1_canceled.pb").read_bytes()
ADDED_TRIPS_S1: bytes = (_RT_DIR / "added_trips_s1.pb").read_bytes()
