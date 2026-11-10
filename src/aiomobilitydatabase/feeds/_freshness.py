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
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Protocol

# RFC 9111 section 1.2.2: a cache treats any delta-seconds value it cannot
# represent, or larger than this, as this value.
_DELTA_SECONDS_MAX = 2_147_483_648
_DELTA_SECONDS_DIGITS = len(str(_DELTA_SECONDS_MAX))


# A response's header fields: lower-cased name -> values, in the order
# received (a sender may repeat a field).
Fields = Mapping[str, Sequence[str]]


def header_fields(headers: Mapping[str, str]) -> dict[str, tuple[str, ...]]:
    """Group header values by lower-cased name, in the order received.

    ``headers.items()`` may repeat a name (aiohttp's multidict does).
    """
    fields: dict[str, list[str]] = {}
    for name, value in headers.items():
        fields.setdefault(name.lower(), []).append(value)
    return {name: tuple(values) for name, values in fields.items()}


@dataclass(frozen=True)
class Validators:
    """HTTP validators a producer offered for one URL."""

    etag: str | None
    last_modified: str | None

    @classmethod
    def from_fields(cls, fields: Fields) -> Validators | None:
        """Read the validators in a set of header fields; None when neither."""
        etag = _first(fields, "etag")
        last_modified = _first(fields, "last-modified")
        if not etag and not last_modified:
            return None
        return cls(etag=etag or None, last_modified=last_modified or None)

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> Validators | None:
        """Read a response's validators; None when it offered neither."""
        return cls.from_fields(header_fields(headers))

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

    ``lifetime`` is how long the response stays fresh, in seconds counted
    from when its REQUEST was sent, or None when it declares nothing --
    distinct from 0, which declares "revalidate every time" (``no-cache``,
    ``max-age=0``, invalid values). ``storable`` is False for ``no-store``.
    """

    lifetime: float | None
    storable: bool = True


def _first(fields: Fields, name: str) -> str | None:
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


def _split_field(value: str) -> list[str] | None:
    """Split one Cache-Control field on commas outside quoted strings.

    A comma inside a quoted value (``community="a, max-age=3600"``) belongs
    to that value; splitting on it would invent directives. None when a
    quote is still open at the end of the field: the field is malformed.
    """
    parts: list[str] = []
    current: list[str] = []
    quoted = escaped = False
    for char in value:
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
    if quoted:
        return None
    parts.append("".join(current))
    return parts


def _directives(values: Sequence[str]) -> tuple[dict[str, list[str | None]], bool]:
    """Parse Cache-Control fields into directives; also report malformation.

    Each field is split on its own, so one field's dangling quote can't
    swallow the next field. A malformed field is still split on every
    comma -- a ``no-store`` or ``no-cache`` in it must not hide -- and
    reported, so the caller can treat its freshness as invalid.
    """
    directives: dict[str, list[str | None]] = {}
    malformed = False
    for value in values:
        parts = _split_field(value)
        if parts is None:
            malformed = True
            parts = value.split(",")
        for part in parts:
            name, sep, arg = part.strip().partition("=")
            name = name.strip().lower()
            if name:
                directives.setdefault(name, []).append(arg if sep else None)
    return directives, malformed


def freshness_from_headers(
    headers: Mapping[str, str], *, requested_at: datetime, received_at: datetime
) -> Freshness:
    """Freshness of a response with these headers (see :func:`freshness_of`)."""
    return freshness_of(
        header_fields(headers), requested_at=requested_at, received_at=received_at
    )


def freshness_of(
    fields: Fields, *, requested_at: datetime, received_at: datetime
) -> Freshness:
    """Freshness of a response, per RFC 9111 for a private cache.

    The freshness lifetime comes from ``Cache-Control: max-age``, else from
    ``Expires - Date`` (``Date`` absent: the receipt time). The response's
    initial age is ``max(apparent_age, Age + response_delay)`` (RFC 9111
    section 4.2.3) and it is fresh until ``received_at + lifetime -
    initial_age``; the result counts that from ``requested_at``, so a
    cache can start its clock before the request without counting the
    response delay twice.

    Strict throughout: ``no-store`` is never stored; any ``no-cache``, a
    ``max-age`` that is missing its value, invalid or repeated, an
    unparseable ``Expires``, or a malformed Cache-Control field (an
    unterminated quote) all mean "stale now" (lifetime 0). A response with
    no freshness information at all declares nothing (None) -- this cache
    never invents a heuristic lifetime. Total over arbitrary fields.
    """
    directives, malformed = _directives(fields.get("cache-control", ()))
    if "no-store" in directives:
        return Freshness(lifetime=None, storable=False)
    if "no-cache" in directives or malformed:
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
    return Freshness(lifetime=max(0.0, lifetime - initial_age + response_delay))


@dataclass(frozen=True)
class Response[T]:
    """One fetch's outcome, as a :class:`FreshnessCache` consumes it.

    ``value`` is None for a ``304 Not Modified``. ``fields`` are the
    response's header fields (see :func:`header_fields`): the cache keeps
    them, because a 304 updates the STORED fields rather than replacing
    them (RFC 9111 section 4.3.4). ``requested_at``/``received_at`` are the
    wall-clock instants the request was sent and the headers arrived.
    """

    value: T | None
    fields: Fields
    requested_at: datetime
    received_at: datetime


class Fetch[T](Protocol):
    """One producer request: conditional when ``validators`` is given."""

    def __call__(self, *, validators: Validators | None) -> Awaitable[Response[T]]:
        """Fetch the resource, revalidating against ``validators`` if any."""
        ...


@dataclass
class _Entry[T]:
    value: T
    fields: Fields  # the stored response's header fields, 304s merged in
    stored_at: float  # monotonic, taken when the request was sent
    lifetime: float | None  # None: the producer declared nothing
    received_at: datetime  # wall clock when ``value`` itself arrived


class FreshnessCache[T]:
    """Per-handle cache of parsed producer responses, keyed by URL.

    ``get`` answers from the stored parse while it is fresh and otherwise
    fetches, passing the stored validators so the producer can answer
    ``304``. A 304's header fields replace the stored fields of the same
    name and the rest stay (RFC 9111 section 4.3.4) -- a bare 304 after
    ``no-cache`` is still ``no-cache`` -- and freshness and validators are
    recomputed from the merged set. Concurrent ``get`` calls for one key
    share one fetch.

    ``lifetime(value, header_lifetime, requested_at)``, when given, decides
    a response's freshness from its body as well as its headers (GBFS: the
    shorter of the document's ``ttl`` and the HTTP lifetime), counted from
    ``requested_at`` like ``header_lifetime``; it runs on every 200 AND
    every 304 -- against the stored value, the only body a 304 has.

    ``timestamp(value)`` dates a body for the "never older" rule: a 200
    dated before the stored copy is a stale CDN edge, so the stored copy is
    kept and marked stale (the next call asks again). A stored timestamp
    later than the stored copy's own receipt (a producer clock running
    ahead, or a clock bug) counts as that receipt time, to the second: a
    far-future stamp
    can't pin the cache, and a slightly fast clock doesn't switch the rule
    off. ``no-store`` responses are never stored, so there is nothing to
    compare them against.
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
        # Normally _refresh already left _in_flight; this covers a task
        # cancelled before it ever ran.
        if self._in_flight.get(key) is task:
            del self._in_flight[key]
        if not task.cancelled():
            task.exception()  # retrieved: every waiter may have gone away

    def _lifetime_of(
        self, value: T, freshness: Freshness, requested_at: datetime
    ) -> float | None:
        declared = freshness.lifetime
        if self._lifetime is not None:
            declared = self._lifetime(value, declared, requested_at)
        if declared is not None and not math.isfinite(declared):
            return 0.0
        return declared

    def _rejects(self, entry: _Entry[T], fetched: T) -> bool:
        if self._timestamp is None:
            return False
        stored_at = self._timestamp(entry.value)
        fetched_at = self._timestamp(fetched)
        if stored_at is None or fetched_at is None:
            return False
        # Body timestamps have whole-second precision (GTFS-RT, GBFS), so
        # the receipt clamp does too: a snapshot stamped within the second
        # the stored copy arrived is not older than it.
        received = entry.received_at.replace(microsecond=0)
        return fetched_at < min(stored_at, received)

    async def _refresh(self, key: str, fetch: Fetch[T]) -> T:
        try:
            return await self._revalidate(key, fetch)
        finally:
            # Leave _in_flight in the same step the result lands, so no
            # caller can join a request that has already answered.
            if self._in_flight.get(key) is asyncio.current_task():
                del self._in_flight[key]

    async def _revalidate(self, key: str, fetch: Fetch[T]) -> T:
        entry = self._entries.get(key)
        requested = self._clock()
        response = await fetch(
            validators=Validators.from_fields(entry.fields) if entry else None
        )
        if response.value is None:
            if entry is None:
                # The fetch was unconditional, so the caller's fetch must
                # have rejected a 304 already; reaching here is a bug.
                raise RuntimeError(f"304 for {key} with nothing stored")
            fields = {**entry.fields, **response.fields}
            freshness = freshness_of(
                fields,
                requested_at=response.requested_at,
                received_at=response.received_at,
            )
            if not freshness.storable:
                self._entries.pop(key, None)
                return entry.value
            entry.fields = fields
            entry.stored_at = requested
            entry.lifetime = self._lifetime_of(
                entry.value, freshness, response.requested_at
            )
            return entry.value
        freshness = freshness_of(
            response.fields,
            requested_at=response.requested_at,
            received_at=response.received_at,
        )
        if not freshness.storable:
            self._entries.pop(key, None)
            return response.value
        if entry is not None and self._rejects(entry, response.value):
            entry.stored_at = requested
            entry.lifetime = 0.0  # declared stale: the next call asks again
            return entry.value
        self._entries[key] = _Entry(
            value=response.value,
            fields=response.fields,
            stored_at=requested,
            lifetime=self._lifetime_of(
                response.value, freshness, response.requested_at
            ),
            received_at=response.received_at,
        )
        return response.value
