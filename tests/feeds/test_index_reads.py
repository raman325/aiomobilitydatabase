"""Index reads from concurrent calls."""

from __future__ import annotations

import asyncio

from aiomobilitydatabase.feeds.transit import TransitFeedHandle

from tests.feeds.test_gtfs_schedule_conformance import _feed
from tests.feeds.test_properties import _build_index_from_files
from tests.feeds.test_time_window_properties import _run


def test_concurrent_index_reads_never_interleave() -> None:
    """Every read shares the index's one SQLite connection. Two queries run
    on it at once by different worker threads return each other's rows, or
    empty ones; concurrent calls (arrivals and vehicles polled together)
    must each still read exactly what a lone call reads."""
    index = _build_index_from_files(_feed())

    async def body(handle: TransitFeedHandle) -> int:
        reads = (handle._index.routes_by_id, handle._index.stops_by_id)
        alone = [await handle._index_read(read) for read in reads]
        wrong = 0
        for _ in range(100):
            together = await asyncio.gather(
                *(handle._index_read(reads[i % 2]) for i in range(16)),
                return_exceptions=True,
            )
            wrong += sum(result != alone[i % 2] for i, result in enumerate(together))
        return wrong

    assert _run(index, body) == 0
