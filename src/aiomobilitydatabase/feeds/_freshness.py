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
import re
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import format_datetime
from typing import Protocol

# RFC 9111 section 1.2.2: a cache treats any delta-seconds value it cannot
# represent, or larger than this, as this value.
_DELTA_SECONDS_MAX = 2_147_483_648
_DELTA_SECONDS_DIGITS = len(str(_DELTA_SECONDS_MAX))
# RFC 9110 section 5.6.2 token, and section 8.8.3 entity-tag.
_TOKEN = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+")
# etagc is any visible character but DQUOTE, plus obs-text -- which arrives
# decoded (aiohttp: UTF-8 with surrogateescape), so anything non-ASCII.
_ENTITY_TAG = re.compile(r'(W/)?"[^\x00-\x20"\x7f]*"')
# RFC 9110 section 5.6.7: the three HTTP-date forms, and nothing else.
# ASCII digits only ([0-9], not \d, which matches any Unicode digit).
_DAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")  # fmt: skip
_DAY = "(Mon|Tue|Wed|Thu|Fri|Sat|Sun)"
_LONG_DAY = "(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)"
_MONTH = "(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
_CLOCK = "([0-9]{2}):([0-9]{2}):([0-9]{2})"
_IMF_FIXDATE = re.compile(rf"{_DAY}, ([0-9]{{2}}) {_MONTH} ([0-9]{{4}}) {_CLOCK} GMT")
_RFC850_DATE = re.compile(
    rf"{_LONG_DAY}, ([0-9]{{2}})-{_MONTH}-([0-9]{{2}}) {_CLOCK} GMT"
)
_ASCTIME_DATE = re.compile(rf"{_DAY} {_MONTH} ([ 0-9][0-9]) {_CLOCK} ([0-9]{{4}})")


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
        """Read the validators in a set of header fields; None when neither.

        An ETag that isn't exactly one quoted entity-tag (``*``, a list, an
        unquoted ``abc123``) validates nothing, nor does a Last-Modified
        that isn't an HTTP-date.
        """
        etag = _etag(fields)
        last_modified = _valid_last_modified(fields)
        if not etag and not last_modified:
            return None
        return cls(etag=etag or None, last_modified=last_modified)

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


def _etag(fields: Fields) -> str | None:
    """Return the ETag if it is exactly one entity-tag; None otherwise."""
    value = _first(fields, "etag")
    if value is None:
        return None
    value = value.strip()
    return value if _ENTITY_TAG.fullmatch(value) else None


def _http_date(value: str | None, now: datetime | None = None) -> datetime | None:
    """Parse an HTTP-date (RFC 9110 section 5.6.7); None for anything else.

    Only the three standard forms -- IMF-fixdate, RFC 850, asctime -- in
    ASCII, around optional space or tab, naming a real instant whose
    weekday matches. RFC 9111 section 5.3 has a cache treat any other
    format as invalid: an ``Expires`` in it means "already expired", not
    whatever a lenient parser makes of it. Built from the matched fields
    rather than by a general parser, which would move a four-digit year
    0026 into 2026 or read other scripts' digits. An RFC 850 two-digit
    year is the one at most 50 years after ``now`` (RFC 9110). A leap
    second (``:60``) names no representable instant and is invalid too.
    """
    if not value:
        return None
    value = value.strip(" \t")
    if (match := _IMF_FIXDATE.fullmatch(value)) is not None:
        day, mday, month, year, hour, minute, second = match.groups()
        full_year = int(year)
    elif (match := _RFC850_DATE.fullmatch(value)) is not None:
        day, mday, month, year, hour, minute, second = match.groups()
        reference = (now or datetime.now(UTC)).year
        full_year = reference - reference % 100 + int(year)
        if full_year > reference + 50:
            full_year -= 100
    elif (match := _ASCTIME_DATE.fullmatch(value)) is not None:
        day, month, mday, hour, minute, second, year = match.groups()
        full_year = int(year)
    else:
        return None
    try:
        parsed = datetime(
            full_year,
            _MONTHS.index(month) + 1,
            int(mday),
            int(hour),
            int(minute),
            int(second),
            tzinfo=UTC,
        )
    except ValueError:
        return None  # no such instant: 31 Feb, year 0, hour 24, :60
    # By index, never strftime: "%a" follows the process locale.
    return parsed if _DAYS[parsed.weekday()] == day[:3] else None


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
    to that value; splitting on it would invent directives. A quote may
    only open an argument (right after ``=``), once per part, so a stray
    quote anywhere else (``foo"bar``) or one still open at the end of the
    field makes the field malformed: None. Text glued after a closing
    quote is left for :func:`_directive`'s grammar to reject.
    """
    parts: list[str] = []
    current: list[str] = []
    quoted = escaped = closed = False
    for char in value:
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted, closed = False, True
            current.append(char)
            continue
        if char == ",":
            parts.append("".join(current))
            current, closed = [], False
            continue
        if char == '"':
            if closed or not "".join(current).rstrip().endswith("="):
                return None
            quoted = True
        current.append(char)
    if quoted:
        return None
    parts.append("".join(current))
    return parts


def _directive(part: str) -> tuple[str, str | None] | None:
    """Parse one ``token [ "=" ( token / quoted-string ) ]`` directive.

    None when it doesn't fit that grammar (RFC 9111 section 5.2): a name
    or argument holding whitespace or separators -- ``no-cache no-store``,
    a missing comma -- is not a directive.
    """
    name, sep, arg = part.partition("=")
    if not _TOKEN.fullmatch(name):
        return None
    if not sep:
        return name.lower(), None
    # _split_field already rejected unbalanced quotes, so a leading quote
    # here opens a complete quoted-string.
    quoted = arg.startswith('"') and arg.endswith('"')
    if not (quoted or _TOKEN.fullmatch(arg)):
        return None
    return name.lower(), arg


def _directives(values: Sequence[str]) -> tuple[dict[str, list[str | None]], bool]:
    """Parse Cache-Control fields into directives; also report malformation.

    Each field is split on its own, so one field's dangling quote can't
    swallow the next field. A field that breaks the grammar -- a stray
    quote, a missing comma -- is malformed, and reported so the caller can
    treat its freshness as invalid; every run of token characters in it is
    still read as a directive name, so a ``no-store`` or ``no-cache`` never
    hides, whatever it is glued to (``;``, ``/``, a quote, a space). That
    can also read one out of an argument (``ext="no-store"`` in a broken
    field): conservative -- not storing beats a stale-now copy -- though
    such a response then skips the "never older" comparison.
    """
    directives: dict[str, list[str | None]] = {}
    malformed = False
    for value in values:
        parts = _split_field(value)
        parsed = [] if parts is None else [_directive(p.strip()) for p in parts]
        if parts is None or any(
            directive is None and part.strip()
            for directive, part in zip(parsed, parts, strict=True)
        ):
            malformed = True
            # Its freshness is invalid either way; what must still be seen
            # is a no-store, however it is glued on (``no-cache;no-store``).
            for name in _TOKEN.findall(value):
                directives.setdefault(name.lower(), []).append(None)
            continue
        for directive in parsed:
            if directive is not None:
                name, arg = directive
                directives.setdefault(name, []).append(arg)
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
    date = _http_date(_first(fields, "date"), received_at)
    lifetime: float | None
    if "max-age" in directives:
        values = directives["max-age"]
        parsed = _delta_seconds(values[0])
        lifetime = float(parsed) if len(values) == 1 and parsed is not None else 0.0
    elif "expires" in fields:
        expires = _http_date(_first(fields, "expires"), received_at)
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


def _entity_tag(fields: Fields) -> tuple[bool, str] | None:
    """Return the ETag as (weak, opaque tag); None when there is no valid one."""
    value = _etag(fields)
    if value is None:
        return None
    if value.startswith("W/"):
        return True, value[2:]
    return False, value


def _valid_last_modified(fields: Fields) -> str | None:
    """Return Last-Modified if it is an HTTP-date; None otherwise."""
    value = _first(fields, "last-modified")
    return value if _http_date(value) is not None else None


def _same_instant(new: str, stored: str | None) -> bool:
    """Whether two Last-Modified values name the same instant.

    Compared as dates (an RFC 850 date equals its IMF-fixdate form); a value
    that isn't an HTTP-date names no instant and matches nothing.
    """
    new_date = _http_date(new)
    return new_date is not None and new_date == _http_date(stored)


def _selects(stored: Fields, new: Fields) -> bool:
    """Whether a 304 identifies the stored response (RFC 9111 section 4.3.4).

    A 304 carrying a strong ETag selects only a stored response with that
    same strong ETag. One carrying only weak validators (a weak ETag,
    Last-Modified) selects one they ALL correspond to -- a matching weak
    tag with a different Last-Modified is another edge's copy. One
    carrying no validator selects only a stored response that has none
    either. A 304 that selects nothing MUST NOT update anything: it may
    describe some other representation entirely.
    """
    new_tag, stored_tag = _entity_tag(new), _entity_tag(stored)
    if new_tag is not None and not new_tag[0]:
        return stored_tag is not None and stored_tag == new_tag
    # A Last-Modified that isn't an HTTP-date is no validator (it is never
    # sent, see Validators.from_fields), on either side.
    new_modified = _valid_last_modified(new)
    stored_modified = _valid_last_modified(stored)
    if new_tag is None and not new_modified:
        return stored_tag is None and not stored_modified
    tag_matches = new_tag is None or (
        stored_tag is not None and stored_tag[1] == new_tag[1]
    )
    modified_matches = not new_modified or _same_instant(new_modified, stored_modified)
    return tag_matches and modified_matches


def _dated(fields: Fields, received_at: datetime) -> Fields:
    """Give a response with no Date its receipt time as Date.

    RFC 9110 section 6.6.1: a recipient caching a message without Date
    MUST record when it was received as its Date -- so a 304 lacking one
    replaces the stored Date instead of leaving the old one to age it.
    """
    if _first(fields, "date") is not None:
        return fields
    return {**fields, "date": (format_datetime(received_at, usegmt=True),)}


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
    ``304``. A 304 updates the stored response only if its validator
    identifies it (RFC 9111 section 4.3.4); one that doesn't -- a
    different strong ETag, or no validator at all -- updates nothing, and
    the cache asks again unconditionally. A selecting 304's header fields
    replace the stored fields of the same name and the rest stay (a 304
    carrying only its ETag after ``no-cache`` is still ``no-cache``; one
    without Date is dated at receipt; a stored Age stays when a 304 brings
    none, which only shortens the renewal), and freshness and validators are
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
    far-future stamp can't pin the cache, and a slightly fast clock doesn't
    switch the rule off. The price of the clamp: under a fast producer
    clock, an edge copy stamped between the stored copy's receipt and its
    (future) stamp is accepted, as is one stamped within the receipt
    second when stamps carry fractions (GBFS 3.0). ``no-store`` responses
    are never stored, so there is nothing to compare them against.
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
            # An eager task factory can run the whole request inside
            # ensure_future; a finished one must never be joined.
            if not task.done():
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
        validators = Validators.from_fields(entry.fields) if entry else None
        requested = self._clock()
        response = await fetch(validators=validators)
        if (
            response.value is None
            and entry is not None
            and validators is not None
            and not _selects(entry.fields, response.fields)
        ):
            # A 304 that doesn't identify the stored response can't update
            # it (RFC 9111 section 4.3.4): ask again, unconditionally.
            validators = None
            requested = self._clock()
            response = await fetch(validators=None)
        fields = _dated(response.fields, response.received_at)
        if response.value is None:
            if entry is None or validators is None:
                # A 304 answers only a conditional request; the caller's
                # fetch must have rejected this one already.
                raise RuntimeError(f"304 to an unconditional request for {key}")
            merged = {**entry.fields, **fields}
            freshness = freshness_of(
                merged,
                requested_at=response.requested_at,
                received_at=response.received_at,
            )
            if not freshness.storable:
                self._entries.pop(key, None)
                return entry.value
            entry.fields = merged
            entry.stored_at = requested
            entry.lifetime = self._lifetime_of(
                entry.value, freshness, response.requested_at
            )
            return entry.value
        freshness = freshness_of(
            fields,
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
            fields=fields,
            stored_at=requested,
            lifetime=self._lifetime_of(
                response.value, freshness, response.requested_at
            ),
            received_at=response.received_at,
        )
        return response.value
