# aiomobilitydatabase

Async Python client for the [Mobility Database](https://mobilitydatabase.org/) — the
catalog MobilityData stewards of GTFS, GTFS Realtime, and GBFS feeds from transit
agencies and bike/scooter share systems around the world.

The core package wraps the catalog's
[REST API](https://mobilitydata.github.io/mobility-feed-api/SwaggerUI/index.html)
with a fully typed asyncio client: search feeds, look up their metadata, find the
hosted dataset URL. An optional `feeds` layer goes one step further and consumes
the actual transit/bike-share data those feeds point to — scheduled and live
arrivals, origin→destination trip queries, vehicle positions, service alerts,
GBFS stations and vehicles — giving you a feed ID (or a plain feed URL) in and
typed snapshots out.

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

The token is only required for catalog operations: `MobilityFeedsClient`'s
[direct-URL methods](#direct-urls-no-catalog-account) work without one.

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

from aiomobilitydatabase.feeds import ArrivalsQuery, Circle, MobilityFeedsClient


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
        # (p.fraction is None when the server doesn't announce a total.)
        transit = await client.get_transit_feed(
            "mdb-100",
            api_key="PRODUCER_API_KEY",
            on_progress=lambda p: print(p.phase, p.fraction),
        )

        # Picker-style helpers: stops in a zone, routes serving a stop.
        # stations_in collapses GTFS station hierarchies (platforms group
        # under their parent station, entrances are dropped) for clean UIs.
        home = Circle(latitude=45.52, longitude=-122.68, radius_m=800)
        stations = transit.stations_in(home)
        nearby_stops = transit.stops_in(home)
        routes = await transit.routes_serving(stations[0].stop_ids[0])

        # Scheduled arrivals merged with realtime (delays, cancellations,
        # RT-added trips) when the feed publishes GTFS-RT TripUpdates.
        [arrivals] = await transit.get_arrivals(
            [ArrivalsQuery([nearby_stops[0].id], limit=2)]
        )
        for arrival in arrivals:
            when = arrival.predicted_departure or arrival.scheduled_departure
            live = "live" if arrival.realtime else "scheduled"
            print(f"{arrival.route_name} -> {arrival.headsign}: {when} ({live})")

        # Origin→destination: the next departures from stop A on trips that
        # later reach stop B, with the same realtime overlay. is_first/is_last
        # flag the first/last such departure of the service day.
        trips = await transit.upcoming_trips(
            nearby_stops[0].id, nearby_stops[1].id, limit=2
        )
        for trip in trips:
            print(trip.route_name, trip.scheduled_departure, trip.is_last)

        vehicles = await transit.get_vehicles()
        alerts = await transit.get_alerts()

        # GBFS bike/scooter share:
        bikes = await client.get_gbfs_feed("gbfs-300")
        stations = await bikes.get_stations(zone=home)
        free_vehicles = await bikes.get_vehicles(zone=home)


asyncio.run(main())
```

### Direct URLs (no catalog account)

If you already know your feed URLs, you can skip the catalog — and the
refresh token — entirely. `get_transit_feed_from_urls` and
`get_gbfs_feed_from_url` return the exact same handle objects as the
catalog methods, so everything above works unchanged:

```python
import asyncio

from aiomobilitydatabase.feeds import ArrivalsQuery, MobilityFeedsClient


async def main() -> None:
    # No refresh token: only catalog operations need one.
    async with MobilityFeedsClient(cache_dir="/path/to/cache") as client:
        transit = await client.get_transit_feed_from_urls(
            "https://agency.example/gtfs.zip",
            trip_updates_urls=["https://agency.example/gtfs-rt/trip-updates.pb"],
            vehicle_positions_urls=["https://agency.example/gtfs-rt/positions.pb"],
            headers={"Authorization": "Bearer PRODUCER_TOKEN"},  # optional
        )
        [arrivals] = await transit.get_arrivals([ArrivalsQuery([transit.stops[0].id])])

        bikes = await client.get_gbfs_feed_from_url(
            "https://bikes.example/gbfs/gbfs.json"
        )
        stations = await bikes.get_stations()


asyncio.run(main())
```

Notes on direct mode:

- Catalog static datasets are served from the Mobility Database's own
  storage; direct mode fetches **your** URL, with the optional `headers`
  applied to the static download and every GTFS-RT/GBFS fetch the handle
  makes.
- RT URLs are declared per layer (`trip_updates_urls`,
  `vehicle_positions_urls`, `service_alerts_urls`), so each operation only
  fetches sources that can serve it. A combined feed listed under several
  layers is deduplicated and fetched once per operation; if you don't know
  a producer's layer split, pass the same URL to every layer.
- Dataset identity comes from HTTP validators (`ETag`, then
  `Last-Modified`, via a `HEAD` probe) instead of catalog dataset IDs;
  servers offering neither fall back to hashing the downloaded bytes, so
  `refresh_static()` still only rebuilds on real changes. The cache key is
  derived from the static URL (`url-<hash>`, exposed as
  `transit.static_feed_id` and accepted by `purge_cache()`), and
  `transit.static_dataset` is `None` (there is no catalog metadata).
- Accessing `client.catalog` (or any catalog-backed method) on a tokenless
  client raises `MobilityDatabaseError`.

## How it works

- **Scheduled arrivals for every feed**: the hosted GTFS zip is indexed into
  SQLite (agencies, stops, routes, trips, stop_times, calendars, frequencies,
  feed_info) in a worker thread — the event loop is never blocked. Arrivals
  work with or without realtime coverage; GTFS-RT TripUpdates overlay delays,
  cancellations, and added trips when present, with cancellation always
  winning over stale predictions.
- **frequencies.txt is materialized at build time**: headway-based trips
  (common for metro/BRT) expand into concrete repetitions under synthetic
  `{trip_id}#{start_secs}` ids, so every schedule query — arrivals,
  origin→destination, routes-serving — sees them as ordinary trips.
- **Spec-correct realtime matching**: GTFS-RT updates are matched by
  `(trip_id, start_date, start_time)`, so an update addresses exactly one
  repetition of a frequency trip and exactly one service-day instance — a
  "trip X is canceled tomorrow" posting never cancels today's run. A
  StopTimeUpdate's delay propagates to subsequent stops until newer
  information, `NO_DATA` makes a stop schedule-only, `SKIPPED` suppresses it
  (and any origin→destination row boarding or alighting there), and the
  trip-level delay covers stops no StopTimeUpdate reaches.
- **The full descriptive surface is exposed, typed by shape**: closed GTFS
  vocabularies are enums (`WheelchairAccess`, `BikesAllowed`,
  `PickupDropOffType`, `StopLocationType`, alert cause/effect/severity,
  vehicle status/congestion/occupancy), `timepoint` is a bool with the spec's
  absent-means-exact default, and open vocabularies (`route_type`,
  `direction_id`) stay raw ints. Out-of-vocabulary values degrade to `None` —
  descriptive metadata never fails a build. Consumers decide what to keep.
- **Static metadata accessors**: `transit.agencies`, `transit.feed_info`
  (publisher, version, validity dates — useful for staleness checks), and
  `headsigns_serving()` alongside the stop/route helpers.
- **Alert scoping**: `ServiceAlert` carries `route_ids`, `stop_ids`, and
  `trip_ids`; an alert is agency-wide only when all three are empty.
  `is_active(at)` evaluates its active periods.
- **GBFS extras**: `rental_uris` deep links on stations and vehicles,
  `get_system_info()`, and TTL-based document caching.
- **`cache_dir` strongly recommended**: the static index is cached on disk
  keyed by feed, validated against the dataset ID, so restarts are instant and
  rebuilds only happen when the agency publishes a new dataset
  (`await transit.refresh_static()` — call it daily; it swaps in the new index
  only after it's built, so lookups never see a half-built database). Without
  a `cache_dir` the index is built in memory on every startup.
- **Pull, not push**: `TransitFeedHandle`/`GbfsFeedHandle` return snapshots on
  demand — there's no built-in polling loop or scheduler. Bring your own (e.g.
  Home Assistant's `DataUpdateCoordinator`).
- **`purge_cache()`** deletes a feed's cached static data (or all feeds' when
  called with no argument) — call it on cleanup/removal of a configured feed.
  `transit.close()` releases a handle's SQLite connection — call it when you
  are done with a handle; the client's `close()` does not do it for you.
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
import asyncio

import aiohttp

from aiomobilitydatabase import MobilityDatabaseClient


async def main() -> None:
    async with aiohttp.ClientSession() as session:
        client = MobilityDatabaseClient("YOUR_REFRESH_TOKEN", session)
        try:
            metadata = await client.get_metadata()
        finally:
            await client.close()  # session remains open; you own it


asyncio.run(main())
```

## Testing methodology

The suite combines example-based, property-based, and conformance testing:

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
  - frequencies.txt materialization (arithmetic-progression oracles,
    descriptor carry-through), GTFS-RT delay propagation (an independent
    piecewise oracle over generated StopTimeUpdate sets, run against the pure
    resolver at volume), and `start_date`/`start_time` instance matching over
    windows containing two service-day instances of the same trip;
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
- **Conformance tests** (`tests/feeds/test_sample_feed.py`) run the index
  against Google's canonical GTFS sample feed — the feed the rest of the GTFS
  ecosystem validates against — with hand-computed expected schedules,
  frequency repetitions, and calendar exceptions pinned from the raw CSVs.

## Development

```bash
uv sync --dev
uv run pytest
uv run ruff check . && uv run ruff format --check .
uv run mypy src
```

## License

[Apache-2.0](LICENSE)
