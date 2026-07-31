# aiomobilitydatabase

Async Python client for the [Mobility Database](https://mobilitydatabase.org/) catalog API.

The Mobility Database is a catalog of GTFS, GTFS Realtime, and GBFS feeds from
around the world. This library wraps its
[catalog API](https://mobilitydata.github.io/mobility-feed-api/SwaggerUI/index.html)
with a fully typed asyncio client.

## Installation

```bash
pip install aiomobilitydatabase
```

Requires Python 3.12+.

## Authentication

Sign up at [mobilitydatabase.org](https://mobilitydatabase.org) and copy your
**refresh token** from the account page. The client exchanges it for short-lived
access tokens automatically (including proactive refresh before expiry).

## Usage

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

With an externally managed session (e.g. Home Assistant's shared session), the
client never closes it:

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

## Error handling

All errors derive from `MobilityDatabaseError`:

| Exception | Meaning |
| --- | --- |
| `MobilityDatabaseConnectionError` | Network failure, timeout, or invalid response body |
| `MobilityDatabaseAuthenticationError` | Invalid refresh token, or unauthorized after a token refresh |
| `MobilityDatabaseNotFoundError` | Resource not found (404) |
| `MobilityDatabaseRateLimitError` | Rate limited (429) |
| `MobilityDatabaseApiError` | Any other 4xx/5xx (carries `.status` and `.body`) |

## Development

```bash
uv sync --dev
uv run pytest
uv run ruff check . && uv run ruff format --check .
uv run mypy src
```

## License

[Apache-2.0](LICENSE)
