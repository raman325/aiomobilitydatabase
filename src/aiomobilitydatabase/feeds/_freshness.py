"""HTTP freshness for producer fetches: RFC 9111 lifetimes plus a shared cache.

Both realtime (GTFS-RT) and GBFS fetches go through :class:`FreshnessCache`.
It answers from a stored parse while it is fresh, revalidates with
ETag/Last-Modified once it lapses, shares one request between concurrent
callers, and never replaces a stored copy with an older one. Semantics are
a strict private cache (RFC 9111): no heuristic freshness, invalid
freshness information counts as stale.
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

# RFC 9111 section 1.2.2: a cache treats any delta-seconds value it cannot
# represent, or larger than this, as this value.
_DELTA_SECONDS_MAX = 2_147_483_648
_DELTA_SECONDS_DIGITS = len(str(_DELTA_SECONDS_MAX))


@dataclass(frozen=True)
class Validators:
    """HTTP validators a producer offered for one URL."""

    etag: str | None
    last_modified: str | None

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> Validators | None:
        """Read a response's validators; None when it offered neither."""
        fields = _fields(headers)
        etag = _first(fields, "etag")
        last_modified = _first(fields, "last-modified")
        if not etag and not last_modified:
            return None
        return cls(etag=etag or None, last_modified=last_modified or None)

    def updated_by(self, newer: Validators | None) -> Validators:
        """Return these validators with any field a 304 carries replaced.

        RFC 9111 section 4.3.4: a 304 updates only the header fields it
        actually sends, so a 304 naming just Last-Modified keeps the ETag.
        """
        if newer is None:
            return self
        return Validators(
            etag=newer.etag or self.etag,
            last_modified=newer.last_modified or self.last_modified,
        )

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
    when the response declares none -- distinct from 0, which declares
    "revalidate every time" (``no-cache``, ``max-age=0``, invalid values).
    ``storable`` is False for ``no-store``.
    """

    lifetime: float | None
    storable: bool = True


def _fields(headers: Mapping[str, str]) -> dict[str, list[str]]:
    """Group header values by lower-cased name, in the order received."""
    fields: dict[str, list[str]] = {}
    for name, value in headers.items():
        fields.setdefault(name.lower(), []).append(value)
    return fields


def _first(fields: Mapping[str, list[str]], name: str) -> str | None:
    """Return the first value of a header field a sender may have repeated.

    For singleton fields (Date, Expires, Age, validators) RFC 9111 has a
    cache use the first value. HTTP dates contain a comma ("Thu, 01 Jan
    ..."), so this takes the first FIELD, never a comma split.
    """
    values = fields.get(name)
    return values[0] if values else None


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


def _delta_seconds(value: str | None) -> int | None:
    """Parse an RFC 9111 delta-seconds value: ASCII digits only.

    Values too large to represent, or above 2^31, are 2^31 (section 1.2.2),
    so a hostile header can't overflow the arithmetic that follows.
    """
    if value is None:
        return None
    value = value.strip().strip('"')
    if not value or not value.isascii() or not value.isdigit():
        return None
    if len(value.lstrip("0")) > _DELTA_SECONDS_DIGITS:
        return _DELTA_SECONDS_MAX
    return min(int(value), _DELTA_SECONDS_MAX)


def _directives(header: str) -> dict[str, list[str | None]]:
    """Split Cache-Control into directives, respecting quoted strings.

    A comma inside a quoted value (``community="a, max-age=3600"``) belongs
    to that value; splitting on it would invent directives.
    """
    parts: list[str] = []
    current: list[str] = []
    quoted = escaped = False
    for char in header:
        if escaped:
            escaped = False
        elif char == "\\" and quoted:
            escaped = True
        elif char == '"':
            quoted = not quoted
        elif char == "," and not quoted:
            parts.append("".join(current))
            current = []
            continue
        current.append(char)
    parts.append("".join(current))
    directives: dict[str, list[str | None]] = {}
    for part in parts:
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
    cache never invents a heuristic lifetime. Total over arbitrary headers.
    """
    fields = _fields(headers)
    directives = _directives(", ".join(fields.get("cache-control", [])))
    if "no-store" in directives:
        return Freshness(lifetime=None, storable=False)
    if "no-cache" in directives:
        return Freshness(lifetime=0.0)
    date = _http_date(_first(fields, "date"))
    lifetime: float | None
    if "max-age" in directives:
        values = directives["max-age"]
        parsed = _delta_seconds(values[0])
        lifetime = float(parsed) if len(values) == 1 and parsed is not None else 0.0
    elif "expires" in fields:
        expires = _http_date(_first(fields, "expires"))
        lifetime = (
            0.0
            if expires is None
            else (expires - (date or received_at)).total_seconds()
        )
    else:
        return Freshness(lifetime=None)
    apparent_age = max(0.0, (received_at - date).total_seconds()) if date else 0.0
    age_field = _first(fields, "age")
    age = _delta_seconds(age_field.split(",")[0]) if age_field else None
    response_delay = max(0.0, (received_at - requested_at).total_seconds())
    initial_age = max(apparent_age, (age or 0) + response_delay)
    return Freshness(lifetime=max(0.0, lifetime - initial_age))


@dataclass(frozen=True)
class Response[T]:
    """One fetch's outcome, as a :class:`FreshnessCache` consumes it.

    ``value`` is None for a ``304 Not Modified``. ``freshness`` is what the
    response's headers declare; ``received_at`` is the wall-clock instant
    its headers arrived, for interpreting body-level freshness (a GBFS
    document's ``ttl`` from its ``last_updated``) and timestamps.
    """

    value: T | None
    validators: Validators | None
    freshness: Freshness
    received_at: datetime


class Fetch[T](Protocol):
    """One producer request: conditional when ``validators`` is given."""

    def __call__(self, *, validators: Validators | None) -> Awaitable[Response[T]]:
        """Fetch the resource, revalidating against ``validators`` if any."""
        ...


@dataclass
class _Entry[T]:
    value: T
    validators: Validators | None
    stored_at: float  # monotonic, taken BEFORE the request: never overstates
    lifetime: float | None  # None: the producer declared nothing
    received_at: datetime  # wall clock when ``value`` itself arrived


class FreshnessCache[T]:
    """Per-handle cache of parsed producer responses, keyed by URL.

    ``get`` answers from the stored parse while it is fresh and otherwise
    fetches, passing the stored validators so the producer can answer
    ``304``. Concurrent ``get`` calls for one key share one fetch.

    ``lifetime(value, header_lifetime, received_at)``, when given, decides
    a response's freshness from its body as well as its headers (GBFS: the
    shorter of the document's ``ttl`` and the HTTP lifetime); it runs on
    every 200 AND every 304 -- against the stored value, the only body a
    304 has. ``timestamp(value)`` dates a body for the "never older" rule:
    a 200 dated before the stored copy is a stale CDN edge, so the stored
    copy is kept and marked stale (the next call asks again). A stored
    timestamp later than its own receipt cannot be a real observation (a
    producer clock bug), so it never blocks a newer response -- one bad
    timestamp can't pin the cache. ``no-store`` responses are never stored,
    so there is nothing to compare them against.
    """

    def __init__(
        self,
        clock: Callable[[], float] = time.monotonic,
        *,
        lifetime: Callable[[T, float | None, datetime], float | None] | None = None,
        timestamp: Callable[[T], datetime | None] | None = None,
    ) -> None:
        """Create an empty cache reading ages from ``clock`` (monotonic)."""
        self._clock = clock
        self._lifetime = lifetime
        self._timestamp = timestamp
        self._entries: dict[str, _Entry[T]] = {}
        self._in_flight: dict[str, asyncio.Task[T]] = {}

    def _fresh(self, entry: _Entry[T], min_interval: float) -> bool:
        age = self._clock() - entry.stored_at
        if entry.lifetime is not None:
            # A declared lifetime (0 included: revalidate) is binding.
            return age < entry.lifetime
        # Declared nothing: only the caller's opt-in interval applies.
        return age < min_interval

    async def get(self, key: str, fetch: Fetch[T], *, min_interval: float = 0.0) -> T:
        """Return the value for ``key``, fetching only when it is not fresh.

        ``min_interval`` (seconds) lets a caller reuse a parse the producer
        declared NO freshness for; it never extends a declared lifetime,
        ``no-cache``, ``max-age=0``, a stale-edge rejection or ``no-store``.
        """
        entry = self._entries.get(key)
        if entry is not None and self._fresh(entry, min_interval):
            return entry.value
        task = self._in_flight.get(key)
        if task is None:
            task = asyncio.ensure_future(self._refresh(key, fetch))
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

    def _lifetime_of(self, value: T, response: Response[T]) -> float | None:
        declared = response.freshness.lifetime
        if self._lifetime is not None:
            declared = self._lifetime(value, declared, response.received_at)
        if declared is not None and not math.isfinite(declared):
            return 0.0
        return declared

    def _rejects(self, entry: _Entry[T], fetched: T) -> bool:
        if self._timestamp is None:
            return False
        stored_at = self._timestamp(entry.value)
        fetched_at = self._timestamp(fetched)
        return (
            stored_at is not None
            and fetched_at is not None
            and fetched_at < stored_at <= entry.received_at
        )

    async def _refresh(self, key: str, fetch: Fetch[T]) -> T:
        entry = self._entries.get(key)
        requested = self._clock()
        response = await fetch(
            validators=entry.validators if entry is not None else None
        )
        if response.value is None:
            if entry is None:
                # The fetch was unconditional, so the caller's fetch must
                # have rejected a 304 already; reaching here is a bug.
                raise RuntimeError(f"304 for {key} with nothing stored")
            if not response.freshness.storable:
                self._entries.pop(key, None)
                return entry.value
            entry.stored_at = requested
            entry.lifetime = self._lifetime_of(entry.value, response)
            entry.validators = (
                entry.validators.updated_by(response.validators)
                if entry.validators is not None
                else response.validators
            )
            return entry.value
        if not response.freshness.storable:
            self._entries.pop(key, None)
            return response.value
        if entry is not None and self._rejects(entry, response.value):
            entry.stored_at = requested
            entry.lifetime = 0.0  # declared stale: the next call asks again
            return entry.value
        self._entries[key] = _Entry(
            value=response.value,
            validators=response.validators,
            stored_at=requested,
            lifetime=self._lifetime_of(response.value, response),
            received_at=response.received_at,
        )
        return response.value
