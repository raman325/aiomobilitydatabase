# aiomobilitydatabase

Async Python client for the [Mobility Database](https://mobilitydatabase.org/) — the
catalog MobilityData stewards of GTFS, GTFS Realtime, and GBFS feeds from transit
agencies and bike/scooter share systems around the world.

The core package wraps the catalog's
[REST API](https://mobilitydata.github.io/mobility-feed-api/SwaggerUI/index.html)
with a fully typed asyncio client: search feeds, look up their metadata, find the
hosted dataset URL. An optional `feeds` layer goes one step further and consumes
the actual transit/bike-share data those feeds point to — scheduled and live
arrivals, vehicle positions, service alerts, GBFS stations and vehicles — giving you
a feed ID in and typed snapshots out.

## Installation

```bash
pip install aiomobilitydatabase
```

That installs the catalog client only. To also consume feed data (schedules,
GTFS-RT, GBFS), install the `feeds` extra, which pulls in
[`gtfs-realtime-bindings`](https://pypi.org/project/gtfs-realtime-bindings/) for
protobuf parsing:

```bash
pip install aiomobilitydatabase[feeds]
```

Importing `aiomobilitydatabase.feeds` without the extra installed raises an
`ImportError` telling you to install it — there is no silent partial-functionality
mode.

Requires Python 3.12+.

## Authentication

Sign up at [mobilitydatabase.org](https://mobilitydatabase.org) and copy your
**refresh token** from the account page. The client exchanges it for short-lived
access tokens automatically (including proactive refresh before expiry). Both
`MobilityDatabaseClient` and `MobilityFeedsClient` take the same refresh token —
the feeds client wraps a catalog client internally and shares its authentication.

## Quick start — catalog

```python
import asyncio

from aiomobilitydatabase import DataType, MobilityDatabaseClient


async def main() -> None:
    async with MobilityDatabaseClient("YOUR_REFRESH_TOKEN") as client:
        # Free-text search across feed name, provider, and location
        results = await client.search_feeds(
            search_query="new york", data_types=[DataType.GTFS_RT], limit=10
        )
        for item in results.results:
            print(item.id, item.provider, item.status)

        # Full detail for one feed, including the producer URL
        feed = await client.get_gtfs_rt_feed(results.results[0].id)
        assert feed.source_info is not None
        print(feed.source_info.producer_url)


asyncio.run(main())
```

## Quick start — feeds (optional extra)

`MobilityFeedsClient` resolves a feed ID (GTFS, GTFS-RT, or GBFS) into a handle
that fetches and caches the actual data. Requires `pip install
aiomobilitydatabase[feeds]`.

```python
import asyncio

from aiomobilitydatabase.feeds import Circle, MobilityFeedsClient


async def main() -> None:
    async with MobilityFeedsClient(
        "YOUR_REFRESH_TOKEN", cache_dir="/path/to/cache"
    ) as client:
        # The full catalog client is available via .catalog:
        results = await client.catalog.search_feeds(search_query="portland", limit=5)
        for item in results.results:
            print(item.id, item.provider, item.data_type)

        # Transit: accepts a GTFS or GTFS-RT feed ID; resolves the sibling.
        # api_key authenticates with the PRODUCER (distinct from your catalog
        # refresh token); on_progress reports download/index build progress
        # for the first (uncached) fetch of a feed's static dataset.
        transit = await client.get_transit_feed(
            "mdb-100",
            api_key="PRODUCER_API_KEY",
            on_progress=lambda p: print(f"{p.phase}: {p.fraction:.0%}"),
        )

        # Picker-style helpers: stops in a zone, routes serving a stop.
        home = Circle(latitude=45.52, longitude=-122.68, radius_m=800)
        nearby_stops = transit.stops_in(home)
        routes = await transit.routes_serving(nearby_stops[0].id)

        # Scheduled arrivals merged with realtime (delays, cancellations,
        # RT-added trips) when the feed publishes GTFS-RT TripUpdates.
        arrivals = await transit.get_arrivals([nearby_stops[0].id], limit=2)
        for arrival in arrivals:
            when = arrival.predicted_departure or arrival.scheduled_departure
            live = "live" if arrival.realtime else "scheduled"
            print(f"{arrival.route_name} -> {arrival.headsign}: {when} ({live})")

        vehicles = await transit.get_vehicles()
        alerts = await transit.get_alerts()

        # GBFS bike/scooter share:
        bikes = await client.get_gbfs_feed("gbfs-300")
        stations = await bikes.get_stations(zone=home)
        free_vehicles = await bikes.get_vehicles(zone=home)


asyncio.run(main())
```

## How it works

- **Scheduled arrivals for every feed**: the hosted GTFS zip is indexed into
  SQLite (stops, routes, trips, stop_times, calendars) in a worker thread — the
  event loop is never blocked. Arrivals work with or without realtime coverage;
  GTFS-RT TripUpdates overlay delays, cancellations, and added trips when
  present, with cancellation always winning over stale predictions.
- **`cache_dir` strongly recommended**: the static index is cached on disk keyed
  by dataset ID, so restarts are instant and rebuilds only happen when the
  agency publishes a new dataset (`await transit.refresh_static()` — call it
  daily; it swaps in the new index only after it's built, so lookups never see
  a half-built database). Without a `cache_dir` the index is built in memory
  on every startup.
- **Pull, not push**: `TransitFeedHandle`/`GbfsFeedHandle` return snapshots on
  demand — there's no built-in polling loop or scheduler. Bring your own (e.g.
  Home Assistant's `DataUpdateCoordinator`).
- **`purge_cache()`** deletes a feed's cached static data (or all feeds' when
  called with no argument) — call it on cleanup/removal of a configured feed.
- **Producer authentication**: pass `api_key=` to `get_transit_feed()`; it is
  applied per the catalog's `authentication_type` (query parameter or header) —
  distinct from the refresh token used to authenticate with the catalog itself.
- **Zones are circles** (matching Home Assistant zone entities); GBFS vehicle
  filtering happens client-side because GBFS feeds are full-system dumps by
  design, with no server-side geo-query.
- **Timestamps are tz-aware UTC** throughout the feeds layer.

## Error handling

All errors derive from `MobilityDatabaseError`. The catalog and feeds layers
each define their own subtree beneath it, so you can catch broadly
(`MobilityDatabaseError`) or narrowly per layer:

| Exception | Layer | Meaning |
| --- | --- | --- |
| `MobilityDatabaseConnectionError` | catalog | Network failure, timeout, or invalid response body |
| `MobilityDatabaseAuthenticationError` | catalog | Invalid refresh token, or unauthorized after a token refresh |
| `MobilityDatabaseNotFoundError` | catalog | Resource not found (404) |
| `MobilityDatabaseRateLimitError` | catalog | Rate limited (429) |
| `MobilityDatabaseApiError` | catalog | Any other 4xx/5xx (carries `.status` and `.body`) |
| `MobilityFeedsError` | feeds | Base class for all feeds errors (subclasses `MobilityDatabaseError`) |
| `SourceConnectionError` | feeds | Producer/GBFS endpoint unreachable or errored (`.status` when HTTP) |
| `SourceAuthenticationError` | feeds | Producer rejected the feed's `api_key` |
| `FeedParseError` | feeds | Undecodable protobuf, malformed GBFS JSON, or unreadable GTFS zip |
| `StaticDataUnavailableError` | feeds | Feed has no usable hosted static dataset |

Note the two independent auth flows: a `MobilityDatabaseAuthenticationError`
means your **catalog refresh token** is invalid; a `SourceAuthenticationError`
means the **producer** rejected the feed-specific `api_key` you passed to
`get_transit_feed()`. They fail independently and are never confused for one
another.

## Session injection

With an externally managed session (e.g. Home Assistant's shared session), the
client never closes it. One shared session serves both catalog requests and
feeds fetches (GTFS-RT polls, GBFS documents, dataset zip downloads) when
passed to `MobilityFeedsClient`:

```python
import aiohttp

from aiomobilitydatabase import MobilityDatabaseClient

session = aiohttp.ClientSession()
client = MobilityDatabaseClient("YOUR_REFRESH_TOKEN", session)
try:
    metadata = await client.get_metadata()
finally:
    await client.close()  # session remains open; you own it
```

## Testing methodology

The suite (174 tests) combines example-based and property-based testing:

- **Example-based tests** cover the catalog's endpoint methods and the feeds
  layer's client/transit/GBFS/static-index modules against a real
  `aiohttp.test_utils.TestServer` running on loopback (`tests/mock_server.py`)
  — scripted per-`(method, path)` responses, with every request recorded for
  assertions on headers, query params, and bodies. This replaces
  [aioresponses](https://github.com/pnuckowski/aioresponses), which cannot
  construct mocked responses under aiohttp >=3.14.
- **Property-based tests** (`tests/feeds/test_properties.py`, via
  [Hypothesis](https://hypothesis.readthedocs.io/)) target the modules where
  correctness is a matter of an invariant holding across a wide input space
  rather than a handful of hand-picked cases:
  - the static-index date/time math (GTFS elapsed-seconds-since-service-day
    arithmetic against a naive oracle, calendar/calendar_dates service-id
    resolution, DST-transition handling) — this is where the original
    hardcoded `±1 day` scan-window bug was found;
  - the circular-zone geometry (`in_circle`'s bbox prefilter is checked
    against the exact haversine distance at generated boundary points);
  - GBFS field parsing (`_localized`, `_version_key`, station/vehicle merging)
    and GTFS-RT protobuf parsing, both checked for **totality** — every
    generated input must produce a typed result or a documented
    `MobilityFeedsError` subclass, never an unhandled exception;
  - error-contract fuzzing for malformed GTFS zips, RT payloads, and GBFS
    documents (garbage bytes, wrong types, out-of-range values) — the
    contract under test is "raises `FeedParseError`/`SourceConnectionError`",
    not "does anything in particular with the garbage."

  Hypothesis is a `dev` dependency and part of the default suite (no separate
  opt-in marker); `.hypothesis/` (its example database) is gitignored.

## Development

```bash
uv sync --dev
uv run pytest
uv run ruff check . && uv run ruff format --check .
uv run mypy src
```

## License

[Apache-2.0](LICENSE)
