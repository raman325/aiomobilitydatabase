"""HTTP freshness for producer fetches: RFC 9111 lifetimes plus a shared cache.

Both realtime (GTFS-RT) and GBFS fetches go through :class:`FreshnessCache`.
It answers from a stored parse while the producer-declared lifetime lasts,
revalidates with ETag/Last-Modified once it lapses, shares one request
between concurrent callers, and never replaces a stored copy with an older
one. Semantics are a strict private cache (RFC 9111): no heuristic
freshness, invalid freshness information counts as stale.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Protocol


@dataclass(frozen=True)
class Validators:
    """HTTP validators a producer offered for one URL."""

    etag: str | None
    last_modified: str | None

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> Validators | None:
        """Read a response's validators; None when it offered neither."""
        fields = _fold(headers)
        etag = fields.get("etag")
        last_modified = fields.get("last-modified")
        if not etag and not last_modified:
            return None
        return cls(etag=etag or None, last_modified=last_modified or None)

    def request_headers(self) -> dict[str, str]:
        """Conditional-request headers that revalidate against these."""
        headers: dict[str, str] = {}
        if self.etag:
            headers["If-None-Match"] = self.etag
        if self.last_modified:
            headers["If-Modified-Since"] = self.last_modified
        return headers


@dataclass(frozen=True)
class Freshness:
    """How a response may be cached, from its headers.

    ``lifetime`` is the remaining freshness in seconds at receipt, or None
    when the response declares none (callers may combine it with another
    declaration, e.g. a GBFS document's ``ttl``). ``storable`` is False for
    ``no-store``.
    """

    lifetime: float | None
    storable: bool = True


def _http_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if parsed.tzinfo is None:  # "-0000" parses naive; HTTP dates are GMT
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _delta_seconds(value: str) -> int | None:
    """Parse an RFC 9111 delta-seconds value: ASCII digits only."""
    value = value.strip().strip('"')
    if not value or not value.isascii() or not value.isdigit():
        return None
    return int(value)


def _fold(headers: Mapping[str, str]) -> dict[str, str]:
    """Lower-case header names, joining repeated fields with commas.

    A repeated ``Cache-Control`` keeps every directive (RFC 9110 section
    5.3); a repeated ``Date``/``Expires`` becomes unparseable, which the
    caller treats as stale -- the conservative reading of a broken response.
    """
    folded: dict[str, str] = {}
    for name, value in headers.items():
        key = name.lower()
        folded[key] = f"{folded[key]}, {value}" if key in folded else value
    return folded


def _cache_control(header: str) -> dict[str, list[str | None]]:
    directives: dict[str, list[str | None]] = {}
    for part in header.split(","):
        name, sep, value = part.strip().partition("=")
        name = name.strip().lower()
        if name:
            directives.setdefault(name, []).append(value if sep else None)
    return directives


def freshness_from_headers(
    headers: Mapping[str, str], *, requested_at: datetime, received_at: datetime
) -> Freshness:
    """Remaining freshness of a response, per RFC 9111 for a private cache.

    The freshness lifetime comes from ``Cache-Control: max-age``, else from
    ``Expires - Date`` (``Date`` absent: the receipt time). It is reduced by
    the response's initial age, ``max(apparent_age, Age + response_delay)``
    (RFC 9111 section 4.2.3), and clamped at zero.

    Strict throughout: ``no-store`` is never stored; any ``no-cache``, a
    ``max-age`` that is missing its value, invalid or repeated, or an
    unparseable ``Expires`` all mean "stale now" (lifetime 0). A response
    with no freshness information at all declares nothing (None) -- this
    cache never invents a heuristic lifetime.
    """
    fields = _fold(headers)
    directives = _cache_control(fields.get("cache-control", ""))
    if "no-store" in directives:
        return Freshness(lifetime=None, storable=False)
    if "no-cache" in directives:
        return Freshness(lifetime=0.0)
    date = _http_date(fields.get("date"))
    lifetime: float | None
    if "max-age" in directives:
        values = directives["max-age"]
        parsed = _delta_seconds(values[0]) if values[0] is not None else None
        lifetime = float(parsed) if len(values) == 1 and parsed is not None else 0.0
    elif "expires" in fields:
        expires = _http_date(fields["expires"])
        lifetime = (
            0.0
            if expires is None
            else (expires - (date or received_at)).total_seconds()
        )
    else:
        return Freshness(lifetime=None)
    apparent_age = max(0.0, (received_at - date).total_seconds()) if date else 0.0
    age_header = _delta_seconds(fields.get("age", ""))
    response_delay = max(0.0, (received_at - requested_at).total_seconds())
    corrected_age = (age_header or 0) + response_delay
    initial_age = max(apparent_age, corrected_age)
    return Freshness(lifetime=max(0.0, lifetime - initial_age))


@dataclass(frozen=True)
class Response[T]:
    """One fetch's outcome, as a :class:`FreshnessCache` consumes it.

    ``value`` is None for a ``304 Not Modified``. ``lifetime`` is the
    remaining freshness in seconds (0 = revalidate on every use).
    """

    value: T | None
    validators: Validators | None
    lifetime: float
    storable: bool = True


class Fetch[T](Protocol):
    """One producer request: conditional when ``validators`` is given."""

    def __call__(self, *, validators: Validators | None) -> Awaitable[Response[T]]:
        """Fetch the resource, revalidating against ``validators`` if any."""
        ...


@dataclass
class _Entry[T]:
    value: T
    validators: Validators | None
    stored_at: float
    lifetime: float


class FreshnessCache[T]:
    """Per-handle cache of parsed producer responses, keyed by URL.

    ``get`` answers from the stored parse while it is fresh -- younger than
    its lifetime, or than ``min_interval`` when a caller opts into one --
    and otherwise fetches, passing the stored validators so the producer can
    answer ``304``. Concurrent ``get`` calls for one key share one fetch.
    ``is_older(stored, fetched)`` lets the caller reject a response older
    than what is already stored (a stale CDN edge): the stored copy is kept
    and marked stale, so the next call asks again.
    """

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        """Create an empty cache reading ages from ``clock`` (monotonic)."""
        self._clock = clock
        self._entries: dict[str, _Entry[T]] = {}
        self._in_flight: dict[str, asyncio.Task[T]] = {}

    async def get(
        self,
        key: str,
        fetch: Fetch[T],
        *,
        min_interval: float = 0.0,
        is_older: Callable[[T, T], bool] | None = None,
    ) -> T:
        """Return the value for ``key``, fetching only when it is not fresh."""
        entry = self._entries.get(key)
        if entry is not None:
            age = self._clock() - entry.stored_at
            if age < max(entry.lifetime, min_interval):
                return entry.value
        task = self._in_flight.get(key)
        if task is None:
            task = asyncio.ensure_future(self._refresh(key, fetch, is_older))
            self._in_flight[key] = task
            task.add_done_callback(lambda done: self._finished(key, done))
        # Shielded: a caller cancelling its own wait must not cancel the
        # request every other waiter is sharing.
        return await asyncio.shield(task)

    def _finished(self, key: str, task: asyncio.Task[T]) -> None:
        if self._in_flight.get(key) is task:
            del self._in_flight[key]
        if not task.cancelled():
            task.exception()  # retrieved: every waiter may have gone away

    async def _refresh(
        self,
        key: str,
        fetch: Fetch[T],
        is_older: Callable[[T, T], bool] | None,
    ) -> T:
        entry = self._entries.get(key)
        response = await fetch(
            validators=entry.validators if entry is not None else None
        )
        now = self._clock()
        lifetime = response.lifetime if math.isfinite(response.lifetime) else 0.0
        if response.value is None:
            if entry is None:
                # The fetch was unconditional, so the caller's fetch must
                # have rejected a 304 already; reaching here is a bug.
                raise RuntimeError(f"304 for {key} with nothing stored")
            if not response.storable:
                self._entries.pop(key, None)
                return entry.value
            entry.stored_at = now
            entry.lifetime = lifetime
            if response.validators is not None:
                entry.validators = response.validators
            return entry.value
        if not response.storable:
            self._entries.pop(key, None)
            return response.value
        if (
            entry is not None
            and is_older is not None
            and is_older(entry.value, response.value)
        ):
            entry.stored_at = now
            entry.lifetime = 0.0
            return entry.value
        self._entries[key] = _Entry(
            value=response.value,
            validators=response.validators,
            stored_at=now,
            lifetime=lifetime,
        )
        return response.value
