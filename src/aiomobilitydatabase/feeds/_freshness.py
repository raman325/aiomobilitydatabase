"""HTTP freshness for producer fetches: RFC 9111 lifetimes plus a shared cache.

Both realtime (GTFS-RT) and GBFS fetches go through :class:`FreshnessCache`.
It answers from a stored parse while it is fresh, revalidates with
ETag/Last-Modified once it lapses, and shares one request between
concurrent callers. Semantics are
a strict private cache (RFC 9111): no heuristic freshness, invalid
freshness information counts as stale.
"""

from __future__ import annotations

import asyncio
import math
import re
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import format_datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Protocol

from yarl import URL

if TYPE_CHECKING:
    import aiohttp

# RFC 9111 section 1.2.2: a cache treats any delta-seconds value it cannot
# represent, or larger than this, as this value.
_DELTA_SECONDS_MAX = 2_147_483_648
_DELTA_SECONDS_DIGITS = len(str(_DELTA_SECONDS_MAX))
# RFC 9110 section 5.6.2 token, and section 8.8.3 entity-tag.
_TOKEN = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+")
# etagc is any visible character but DQUOTE, plus obs-text -- which arrives
# decoded (aiohttp: UTF-8 with surrogateescape), so anything non-ASCII.
_ENTITY_TAG = re.compile(r'(W/)?"[^\x00-\x20"\x7f]*"')
# RFC 9110 section 5.6.3: optional whitespace is space and tab, nothing else
# (str.strip() would also eat NBSP, NEL, U+2028 and friends).
_OWS = " \t"
# RFC 9110 section 5.6.7: the three HTTP-date forms, and nothing else.
# ASCII digits only ([0-9], not \d, which matches any Unicode digit). Names
# match in any case: RFC 9111 section 4.2 has a cache recipient match "the
# field value case-insensitively".
_DAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")  # fmt: skip
_DAY = "(Mon|Tue|Wed|Thu|Fri|Sat|Sun)"
_LONG_DAY = "(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)"
_MONTH = "(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
_CLOCK = "([0-9]{2}):([0-9]{2}):([0-9]{2})"
_IMF_FIXDATE = re.compile(
    rf"{_DAY}, ([0-9]{{2}}) {_MONTH} ([0-9]{{4}}) {_CLOCK} GMT", re.IGNORECASE
)
_RFC850_DATE = re.compile(
    rf"{_LONG_DAY}, ([0-9]{{2}})-{_MONTH}-([0-9]{{2}}) {_CLOCK} GMT", re.IGNORECASE
)
_ASCTIME_DATE = re.compile(
    rf"{_DAY} {_MONTH} ([ 0-9][0-9]) {_CLOCK} ([0-9]{{4}})", re.IGNORECASE
)


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
    """HTTP validators a producer offered for one URL.

    ``url`` is the resource they came from -- after redirects, the one that
    answered: only a request for that URL may carry them (RFC 9111 section
    4.3.1). None: the URL first asked.
    """

    etag: str | None
    last_modified: str | None
    url: str | None = None

    @classmethod
    def from_fields(cls, fields: Fields, url: str | None = None) -> Validators | None:
        """Read the validators in a set of header fields; None when neither.

        An ETag that isn't exactly one quoted entity-tag (``*``, a list, an
        unquoted ``abc123``) validates nothing, nor does a Last-Modified
        that isn't an HTTP-date.
        """
        etag = _etag(fields)
        last_modified = _valid_last_modified(fields)
        if not etag and not last_modified:
            return None
        return cls(etag=etag or None, last_modified=last_modified, url=url)

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
    value = value.strip(_OWS)
    return value if _ENTITY_TAG.fullmatch(value) else None


def _http_date(value: str | None, now: datetime | None = None) -> datetime | None:
    """Parse an HTTP-date to its instant; None for anything else.

    See :func:`_parse_http_date`. A leap second reads as the second before
    it (the POSIX convention: datetime can't hold ``:60``), which is how
    freshness arithmetic counts it.
    """
    parsed = _parse_http_date(value, now)
    return None if parsed is None else parsed[0]


def _canonical_date(value: str | None, now: datetime | None = None) -> str | None:
    """Return an HTTP-date as IMF-fixdate text; None when it is no HTTP-date.

    One text per instant, a leap second's ``23:59:60`` kept distinct from
    the ``23:59:59`` before it -- so two dates are the same exactly when
    their canonical texts are equal.
    """
    parsed = _parse_http_date(value, now)
    if parsed is None:
        return None
    instant, leap = parsed
    text = format_datetime(instant, usegmt=True)
    return text[: -len("59 GMT")] + "60 GMT" if leap else text


def _parse_http_date(
    value: str | None, now: datetime | None = None
) -> tuple[datetime, bool] | None:
    """Parse an HTTP-date (RFC 9110 section 5.6.7) to (instant, leap second).

    Only the three standard forms -- IMF-fixdate, RFC 850, asctime -- in
    ASCII (names in any case), around optional space or tab, naming a real
    instant whose weekday matches. RFC 9111 section 5.3 has a cache treat any other
    format as invalid: an ``Expires`` in it means "already expired", not
    whatever a lenient parser makes of it. Built from the matched fields
    rather than by a general parser, which would move a four-digit year
    0026 into 2026 or read other scripts' digits. An RFC 850 two-digit
    year is placed by RFC 9110's 50-year rule against ``now`` (see
    :func:`_rfc850_instant`). A leap second (``23:59:60``, the only valid
    ``:60``) is the instant of the second before it, flagged as a leap.
    """
    if not value:
        return None
    value = value.strip(_OWS)
    if (match := _IMF_FIXDATE.fullmatch(value)) is not None:
        day, mday, month, year, hour, minute, second = match.groups()
        full_year = int(year)
    elif (match := _RFC850_DATE.fullmatch(value)) is not None:
        day, mday, month, year, hour, minute, second = match.groups()
        full_year = None
    elif (match := _ASCTIME_DATE.fullmatch(value)) is not None:
        day, month, mday, hour, minute, second, year = match.groups()
        full_year = int(year)
    else:
        return None
    # 23:59:60 is a leap second (RFC 9110 lists it); datetime can't hold
    # one, so it is the second before. Any other :60 is no time at all.
    leap = (hour, minute, second) == ("23", "59", "60")
    fields = (_MONTHS.index(month.title()) + 1, int(mday), int(hour), int(minute))
    seconds = 59 if leap else int(second)
    if full_year is not None:
        try:
            parsed = datetime(full_year, *fields, seconds, tzinfo=UTC)
        except ValueError:
            return None  # no such instant: 31 Feb, year 0, hour 24, :60
    else:
        parsed_850 = _rfc850_instant(int(year), fields, seconds, now)
        if parsed_850 is None:
            return None
        parsed = parsed_850
    # By index, never strftime: "%a" follows the process locale.
    return (parsed, leap) if _DAYS[parsed.weekday()] == day[:3].title() else None


def _rfc850_instant(
    two_digits: int,
    fields: tuple[int, int, int, int],
    seconds: int,
    now: datetime | None,
) -> datetime | None:
    """Place an RFC 850 two-digit year (RFC 9110 section 5.6.7).

    A timestamp that "appears to be more than 50 years in the future" is
    the most recent past year with those two digits -- an instant-level
    test: start a century ahead and step back while the candidate lies
    beyond ``now`` plus 50 calendar years. A candidate that doesn't exist
    in its century (29 Feb) is skipped.
    """
    reference = now or datetime.now(UTC)
    if reference.year + 50 > datetime.max.year:
        limit = datetime.max.replace(tzinfo=UTC)
    else:
        try:
            limit = reference.replace(year=reference.year + 50)
        except ValueError:  # 29 Feb, and 50 years on is no leap year
            limit = reference.replace(year=reference.year + 50, day=28)
    century = reference.year - reference.year % 100
    for base in (century + 100, century, century - 100):
        try:
            candidate = datetime(base + two_digits, *fields, seconds, tzinfo=UTC)
        except ValueError:
            continue
        if candidate <= limit:
            return candidate
    return None


def _delta_seconds(value: str | None, *, quoted: bool = False) -> int | None:
    """Parse an RFC 9111 delta-seconds value: ASCII digits only.

    Values too large to represent, or above 2^31, are 2^31 (section 1.2.2),
    so a hostile header can't overflow the arithmetic that follows.
    ``quoted`` admits the quoted form a ``max-age`` argument may arrive in;
    an Age field has no such form.
    """
    if value is None:
        return None
    value = value.strip(_OWS)
    if quoted and len(value) > 1 and value[0] == value[-1] == '"':
        value = re.sub(r"\\(.)", r"\1", value[1:-1])  # quoted-pairs
    if not value or not value.isascii() or not value.isdigit():
        return None
    if len(value.lstrip("0")) > _DELTA_SECONDS_DIGITS:
        return _DELTA_SECONDS_MAX
    return min(int(value), _DELTA_SECONDS_MAX)


def _split_field(value: str) -> list[str] | None:
    """Split one Cache-Control field on commas outside quoted strings.

    A comma inside a quoted value (``community="a, max-age=3600"``) belongs
    to that value; splitting on it would invent directives. A backslash
    escapes the next character inside quotes. A quote still open at the end
    makes the field malformed: None. Anything else that breaks the grammar
    is left for :func:`_directive` to reject.
    """
    parts: list[str] = []
    current: list[str] = []
    quoted = escaped = False
    for char in value:
        if escaped:
            escaped = False
        elif quoted and char == "\\":
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
    quoted = (
        len(arg) > 1
        and arg[0] == arg[-1] == '"'
        and '"' not in arg[1:-1].replace('\\"', "")
    )
    if not (quoted or _TOKEN.fullmatch(arg)):
        return None
    return name.lower(), arg


def _directives(values: Sequence[str]) -> tuple[dict[str, list[str | None]], bool]:
    """Parse Cache-Control fields into directives; also report malformation.

    Each field is split on its own, so one field's dangling quote can't
    swallow the next field. A field that breaks the grammar -- a stray
    quote, a missing comma (``no-cache;no-store``) -- contributes nothing
    and is reported, so the caller treats the freshness as invalid: stale
    now (RFC 9111 section 4.2.1), stored but revalidated on every use.
    """
    directives: dict[str, list[str | None]] = {}
    malformed = False
    for value in values:
        parts = _split_field(value)
        parsed = [] if parts is None else [_directive(p.strip(_OWS)) for p in parts]
        if parts is None or any(
            directive is None and part.strip(_OWS)
            for directive, part in zip(parsed, parts, strict=True)
        ):
            malformed = True
            continue
        for directive in parsed:
            if directive is not None:
                name, arg = directive
                directives.setdefault(name, []).append(arg)
    return directives, malformed


def _vary_names(fields: Fields) -> set[str]:
    """Return the request header fields a response's Vary nominates, lower-cased.

    Every Vary field line counts, each a comma-separated list.
    """
    return {
        name.lower()
        for value in fields.get("vary", ())
        for member in value.split(",")
        if (name := member.strip(_OWS))
    }


def _varies_by_everything(fields: Fields) -> bool:
    """Whether Vary lists ``*``: such a response never matches a request.

    RFC 9111 section 4.1: a stored response whose Vary contains ``*``
    "always fails to match", and a cache MUST NOT reuse one that doesn't
    match -- so it is never worth storing.
    """
    return "*" in _vary_names(fields)


def _vary_matches(
    names: set[str],
    sent: Fields | None,
    upcoming: Fields | None,
    *,
    redirected: bool = False,
) -> bool:
    """Whether a stored response may answer the upcoming request (RFC 9111 4.1).

    Every request field its Vary ``names`` nominate must carry the same
    value in the request that drew it (``sent``) and in the one it would
    now stand in for (``upcoming``), both as :func:`request_fields`
    predicts them; a field absent from both matches. Unknown fields (None)
    confirm nothing. Through a redirect only aiohttp's automatic headers can
    be confirmed -- they are the same on every hop and every request unless
    the client set them, which both sides record -- so any other name fails
    to match (the target's own cookies, say, are in neither prediction).
    """
    if not names:
        return True
    if sent is None or upcoming is None:
        return False
    return all(
        (not redirected or name in _AUTOMATIC)
        and tuple(sent.get(name, ())) == tuple(upcoming.get(name, ()))
        for name in names
    )


# aiohttp adds these itself when neither the session nor the request sets
# them -- the same values on every request of the process (Host: per URL).
_AUTOMATIC = frozenset({"host", "accept", "accept-encoding", "user-agent"})


def _cookie_pairs(values: Iterable[str]) -> tuple[str, ...]:
    """Return Cookie field values as ``name=value`` pairs sorted by name.

    Stably -- a repeated name keeps its values in order, since a server may
    read either.
    """
    cookies: list[tuple[str, str]] = []
    for value in values:
        for part in value.split(";"):
            if pair := part.strip(_OWS):
                name, _, cookie = pair.partition("=")
                cookies.append((name.strip(_OWS), cookie.strip(_OWS)))
    return tuple(
        f"{name}={cookie}" for name, cookie in sorted(cookies, key=lambda c: c[0])
    )


def request_fields(
    session: aiohttp.ClientSession, url: str, headers: Mapping[str, str]
) -> dict[str, tuple[str, ...]] | None:
    """Return the request header fields this client controls for a GET of ``url``.

    The session's default headers merged with ``headers`` as aiohttp merges
    them (a request header replaces the defaults' same-named one, whatever
    its case; a later spelling of one name replaces an earlier one), and
    the cookies sent: explicit Cookie headers, then the session jar's for
    ``url``, canonical (see :func:`_cookie_pairs`), and the session's
    ``auth`` as the Authorization it sends when nothing sets one. aiohttp's
    automatic headers are left out: unless set here, they are fixed for the
    process. A stored response's Vary is matched against them: the
    prediction for the request that drew it against the one for the next
    request. Pass ``headers`` without the cache's validators.

    None (unknown) when the session has client middlewares (aiohttp 3.12+):
    those can add a header the prediction can't see, so any response with
    a Vary is revalidated rather than matched.
    """
    if getattr(session, "_middlewares", ()):
        return None
    fields: dict[str, list[str]] = {
        name: list(values) for name, values in header_fields(session.headers).items()
    }
    added: set[str] = set()
    for key, value in headers.items():
        if key in added:
            fields[key.lower()].append(value)
        else:
            fields[key.lower()] = [value]
            added.add(key)
    explicit = fields.pop("cookie", [])
    jar = session.cookie_jar.filter_cookies(URL(url))
    pairs = _cookie_pairs(
        [*explicit, *(f"{morsel.key}={morsel.coded_value}" for morsel in jar.values())]
    )
    # Deprecated in aiohttp 3.14 (and gone in 4), but still sent while set.
    auth = getattr(session, "auth", None)
    if auth is not None and "authorization" not in fields:
        fields["authorization"] = [auth.encode()]
    result = {name: tuple(values) for name, values in fields.items()}
    if pairs:
        result["cookie"] = pairs
    return result


@dataclass(frozen=True)
class Hop:
    """One redirect a response was reached through: its status and fields."""

    status: int
    fields: Fields


def hops(
    response: aiohttp.ClientResponse,
    history: Sequence[aiohttp.ClientResponse] | None = None,
) -> tuple[Hop, ...]:
    """Return the redirects a response was reached through, in order.

    ``history`` overrides ``response.history`` for redirects followed by hand.
    """
    return tuple(
        Hop(status=hop.status, fields=header_fields(hop.headers))
        for hop in (response.history if history is None else history)
    )


# RFC 9110 section 15.1: the statuses a cache may reuse without explicit
# freshness (heuristically). A redirect with any other status (302, 303,
# 307) and no freshness of its own may not be reused at all.
_HEURISTICALLY_CACHEABLE = frozenset(
    {200, 203, 204, 206, 300, 301, 308, 404, 405, 410, 414, 501}
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

    Dates are read as :func:`_received` pins them (against
    ``received_at``), so this is exactly what the cache computes.

    Strict throughout: ``no-store`` -- or ``Vary: *``, which never matches
    -- is never stored; a bare ``no-cache``, a
    ``max-age`` that is missing its value, invalid or repeated, an
    unparseable ``Expires``, or a malformed Cache-Control field (an
    unterminated quote) all mean "stale now" (lifetime 0). A response with
    no freshness information at all declares nothing (None) -- this cache
    never invents a heuristic lifetime. Total over arbitrary fields.

    A ``no-cache`` naming fields (``no-cache="Set-Cookie"``) only forbids
    passing those fields on unrevalidated (RFC 9111 section 5.2.2.4); this
    cache hands out parsed bodies, never stored fields, so it leaves the
    lifetime alone.
    """
    fields = _received(fields, received_at)  # idempotent for pinned fields
    directives, malformed = _directives(fields.get("cache-control", ()))
    if "no-store" in directives or _varies_by_everything(fields):
        return Freshness(lifetime=None, storable=False)
    if None in directives.get("no-cache", ()) or malformed:
        return Freshness(lifetime=0.0)
    date = _http_date(_first(fields, "date"), received_at)
    lifetime: float | None
    if "max-age" in directives:
        values = directives["max-age"]
        parsed = _delta_seconds(values[0], quoted=True)
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
    age = _age_when_requested(
        fields, requested_at=requested_at, received_at=received_at
    )
    return Freshness(lifetime=max(0.0, lifetime - age))


def _age_when_requested(
    fields: Fields, *, requested_at: datetime, received_at: datetime
) -> float:
    """Return a response's age as of its request, in seconds (RFC 9111 4.2.3).

    The initial age -- ``max(apparent_age, Age + response_delay)`` -- less
    the response delay, so that adding the time since the request gives the
    current age without counting the delay twice. Reads pinned fields (see
    :func:`_received`); an invalid Age is ignored (section 5.1).
    """
    date = _http_date(_first(fields, "date"), received_at)
    apparent_age = max(0.0, (received_at - date).total_seconds()) if date else 0.0
    age_field = _first(fields, "age")
    age = _delta_seconds(age_field.split(",")[0]) if age_field else None
    response_delay = max(0.0, (received_at - requested_at).total_seconds())
    return max(0.0, max(apparent_age, (age or 0) + response_delay) - response_delay)


def _entity_tag(fields: Fields) -> tuple[bool, str] | None:
    """Return the ETag as (weak, opaque tag); None when there is no valid one."""
    value = _etag(fields)
    if value is None:
        return None
    if value.startswith("W/"):
        return True, value[2:]
    return False, value


def _valid_last_modified(fields: Fields) -> str | None:
    """Return Last-Modified if it named an instant; None otherwise.

    Reads fields pinned at receipt (see :func:`_received`): a date is
    IMF-fixdate text there, one text per instant (a leap second's ``:60``
    its own), so two are the same instant exactly when equal.
    """
    value = _first(fields, "last-modified")
    return None if value in (None, _NO_INSTANT) else value


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
    modified_matches = not new_modified or new_modified == stored_modified
    return tag_matches and modified_matches


# Stands in for a date that named no instant when its response arrived.
_NO_INSTANT = "-"


def _received(fields: Fields, received_at: datetime) -> Fields:
    """Pin a response's dates to its own arrival.

    Date, Expires and Last-Modified are read once, against when THIS
    response arrived (which places an RFC 850 year), and kept as
    IMF-fixdate, a form whose meaning no later reading can change -- so
    after a 304 merges its fields into a stored response's, each date
    still means what it meant on arrival. One that named no instant
    becomes a marker that never parses: read later against another time,
    an RFC 850 year could otherwise turn a stale Expires into fifty years.
    A Date that is absent or invalid is the receipt time (RFC 9110 section
    6.6.1: a recipient caching a message without one MUST record it), so a
    304 lacking one replaces the stored Date instead of leaving it to age.
    """
    received_at = received_at.astimezone(UTC)
    pinned = dict(fields)
    for name in ("date", "expires", "last-modified"):
        value = _first(fields, name)
        # IMF-fixdate text, a leap second's :60 included: an IMF-fixdate as
        # sent comes back unchanged, so If-Modified-Since echoes the
        # producer exactly, and the other forms name the very same second.
        canonical = _canonical_date(value, received_at)
        if canonical is not None:
            pinned[name] = (canonical,)
        elif name == "date":
            pinned[name] = (format_datetime(received_at, usegmt=True),)
        elif value is not None:
            pinned[name] = (_NO_INSTANT,)
    return pinned


@dataclass(frozen=True)
class Response[T]:
    """One fetch's outcome, as a :class:`FreshnessCache` consumes it.

    ``value`` is None for a ``304 Not Modified``. ``fields`` are the
    response's header fields (see :func:`header_fields`): the cache keeps
    them, because a 304 updates the STORED fields rather than replacing
    them (RFC 9111 section 4.3.4). ``requested_at``/``received_at`` are the
    wall-clock instants the request was sent and the headers arrived.
    ``hops`` are
    the redirects followed to reach it, in order: the response answers the
    ORIGINAL URL only for as long as every hop may be reused too. ``url``
    is the URL that answered (the last hop's target): a 304 from any other
    resource says nothing about the stored one (RFC 9110 section 8.8.3).
    """

    value: T | None
    fields: Fields
    requested_at: datetime
    received_at: datetime
    hops: Sequence[Hop] = ()
    url: str | None = None


class Fetch[T](Protocol):
    """One producer request: conditional when ``validators`` is given."""

    def __call__(self, *, validators: Validators | None) -> Awaitable[Response[T]]:
        """Fetch the resource, revalidating against ``validators`` if any."""
        ...


# An in-flight request's identity: its key and the request fields it carries.
_Slot = tuple[str, tuple[tuple[str, tuple[str, ...]], ...] | None]


@dataclass
class _Entry[T]:
    value: T
    fields: Fields  # the stored response's header fields, 304s merged in
    stored_at: float  # monotonic, taken when the request was sent
    lifetime: float  # the chain's shortest; min_interval where none declared
    # The predicted fields of the request it answers (None: unknown).
    request_fields: Fields | None = field(default_factory=dict)
    url: str | None = None  # the resource that answered (after redirects)
    redirected: bool = False  # reached through redirects
    # The redirects' own Vary values: replaced whole by each response, never
    # merged into ``fields`` (which a 304 updates). The first redirect's are
    # kept apart: its request was the original one, the predicted one.
    hop_vary: tuple[str, ...] = ()
    first_hop_vary: tuple[str, ...] = ()


def _hop_freshness(response: Response[object]) -> list[Freshness]:
    """Freshness of each redirect a response was reached through.

    Read against the whole fetch's request and receipt instants (aiohttp
    keeps no per-hop timing): exact when a hop carries Date, otherwise a
    hop's age can count up to the later hops' time too.

    One that declares nothing on a status that may not be reused without
    explicit freshness (302, 303, 307) is stale now: the original URL must
    be asked every time.
    """
    found = []
    for hop in response.hops:
        hop_freshness = freshness_of(
            hop.fields,
            requested_at=response.requested_at,
            received_at=response.received_at,
        )
        if (
            hop_freshness.storable
            and hop_freshness.lifetime is None
            and hop.status not in _HEURISTICALLY_CACHEABLE
        ):
            hop_freshness = Freshness(lifetime=0.0)
        found.append(hop_freshness)
    return found


def _hop_vary(response: Response[object]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return (first redirect's, later redirects') Vary values.

    A redirect varying by request fields answers only requests that match
    (RFC 9111 section 4.1), just as the target does. The first was drawn by
    the original request, so its Vary is matched against that request's
    predicted fields; the later ones only through automatic headers.
    """
    if not response.hops:
        return (), ()
    first, *later = response.hops
    return tuple(first.fields.get("vary", ())), tuple(
        value for hop in later for value in hop.fields.get("vary", ())
    )


def _storable(freshness: Freshness, response: Response[object]) -> bool:
    """Whether a response, and every redirect to it, may be stored."""
    return freshness.storable and all(hop.storable for hop in _hop_freshness(response))


def _entry_matches(entry: _Entry[Any], upcoming: Fields | None) -> bool:
    """Whether a stored response (and its redirects) answer a request.

    The first redirect's Vary is matched exactly: it answered the original
    request. The target's and later redirects' Vary only through automatic
    headers when the stored response was redirected.
    """
    later = _vary_names({"vary": (*entry.fields.get("vary", ()), *entry.hop_vary)})
    return _vary_matches(
        _vary_names({"vary": entry.first_hop_vary}), entry.request_fields, upcoming
    ) and _vary_matches(
        later, entry.request_fields, upcoming, redirected=entry.redirected
    )


# No request fields: the default for a fetch that sets none.
_NONE: Fields = MappingProxyType({})


def _frozen(fields: Fields | None) -> tuple[tuple[str, tuple[str, ...]], ...] | None:
    """Return a hashable, order-independent form of a set of request fields."""
    if fields is None:
        return None
    return tuple(sorted((name, tuple(values)) for name, values in fields.items()))


class FreshnessCache[T]:
    """Per-handle cache of parsed producer responses, keyed by URL.

    ``get`` answers from the stored parse while it is fresh and otherwise
    fetches, passing the stored validators so the producer can answer
    ``304``. A 304 updates the stored response only if its validator
    identifies it (RFC 9111 section 4.3.4); one that doesn't -- a
    different strong ETag, or no validator at all -- updates nothing, and
    the cache asks again unconditionally. A selecting 304's header fields
    replace the stored fields of the same name and the rest stay (RFC 9111
    section 3.2: a 304 carrying only its ETag after ``no-cache`` is still
    ``no-cache``; one without Date is dated at receipt; a stored Age stays
    when a 304 brings none; Content-Length is never replaced), and
    freshness and validators are recomputed from the merged set.

    A stored response answers only a request whose header fields match
    those its Vary nominates (RFC 9111 section 4.1): ``get`` takes the
    controlled fields the next request would carry (see
    :func:`request_fields`) and fetches whenever they differ from the
    stored response's -- a cookie the producer set since, say. A
    selecting 304 makes the stored response the answer to the request that
    drew it. Concurrent ``get`` calls for one key with the same request
    fields share one fetch -- callers that joined before it was sent, which
    is one request (RFC 9111 section 4); a caller arriving after waits for
    it and then re-checks the store, so it never gets a response it may not
    reuse (``no-store``, ``no-cache``). Fetches for one key run one at a
    time, each re-checking the store first.

    A redirected response answers its original URL only while every hop
    may be reused as well (RFC 9111 section 4: the URL is the key): the
    shortest lifetime of the chain, and not stored when a hop may not be.

    ``min_interval`` (seconds) is an opt-in heuristic lifetime (RFC 9111
    section 4.2.2) for a link of the chain that declares no freshness at
    all; it never extends a declared one, ``no-cache``, ``max-age=0`` or
    ``no-store``, nor reuses a 302, 303 or 307 that declared nothing.

    ``lifetime(value, header_lifetime, requested_at)``, when given, decides
    a response's freshness from its body as well as its headers (GBFS: the
    shorter of the document's ``ttl`` and the HTTP lifetime), counted from
    ``requested_at`` like ``header_lifetime``; it runs on every 200 AND
    every 304 -- against the stored value, the only body a 304 has.

    A full response always replaces the stored one (RFC 9111 section
    4.3.3: after a conditional request, the cache "MUST use the full
    response"), whatever its body says about when it was built.
    """

    def __init__(
        self,
        clock: Callable[[], float] = time.monotonic,
        *,
        lifetime: Callable[[T, float | None, datetime], float | None] | None = None,
        min_interval: float = 0.0,
    ) -> None:
        """Create an empty cache reading ages from ``clock`` (monotonic)."""
        self._clock = clock
        self._lifetime = lifetime
        self._min_interval = min_interval
        self._entries: dict[str, _Entry[T]] = {}
        self._in_flight: dict[_Slot, asyncio.Task[T]] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _fresh(self, entry: _Entry[T], upcoming: Fields | None) -> bool:
        # A response that answers a different request (RFC 9111 4.1) isn't.
        return (
            _entry_matches(entry, upcoming)
            and self._clock() - entry.stored_at < entry.lifetime
        )

    async def get(
        self, key: str, fetch: Fetch[T], *, request_fields: Fields | None = _NONE
    ) -> T:
        """Return the value for ``key``, fetching only when it is not fresh.

        ``request_fields`` are the predicted header fields ``fetch``'s
        request will carry (see :func:`request_fields`; default none; None:
        unknown), which a stored response's Vary must match.
        """
        entry = self._entries.get(key)
        if entry is not None and self._fresh(entry, request_fields):
            return entry.value
        # Only a request carrying the same fields draws the same answer.
        slot = (key, _frozen(request_fields))
        task = self._in_flight.get(slot)
        if task is None:
            task = asyncio.ensure_future(self._refresh(slot, fetch, request_fields))
            # An eager task factory can run the whole request inside
            # ensure_future; a finished one must never be joined.
            if not task.done():
                self._in_flight[slot] = task
            task.add_done_callback(lambda done: self._finished(slot, done))
        # A caller cancelling its own wait must not cancel the request every
        # other waiter is sharing: asyncio.wait never cancels what it waits
        # on (and unlike shield leaves no handler that would log a failure
        # every waiter abandoned).
        await asyncio.wait((task,))
        return task.result()

    def _finished(self, slot: _Slot, task: asyncio.Task[T]) -> None:
        # Normally _refresh already left _in_flight; this covers a task
        # cancelled before it ever ran.
        if self._in_flight.get(slot) is task:
            del self._in_flight[slot]
        if not task.cancelled():
            task.exception()  # retrieved: every waiter may have gone away

    def _lifetime_of(
        self, value: T, freshness: Freshness, response: Response[T]
    ) -> float:
        """Return how long a response, and the redirects to it, stay fresh.

        The shortest lifetime of the chain, where a link that declares
        nothing counts as ``min_interval`` (a declared 0, ``no-cache``,
        stays 0; so does a 302, 303 or 307 that declared nothing).
        """
        declared = freshness.lifetime
        if self._lifetime is not None:
            declared = self._lifetime(value, declared, response.requested_at)
        if declared is not None and not math.isfinite(declared):
            declared = 0.0
        return min(
            self._min_interval if lifetime is None else lifetime
            for lifetime in (
                declared,
                *(hop.lifetime for hop in _hop_freshness(response)),
            )
        )

    async def _refresh(
        self, slot: _Slot, fetch: Fetch[T], upcoming: Fields | None
    ) -> T:
        key = slot[0]
        try:
            lock = self._locks.get(key)
            if lock is None:
                lock = self._locks[key] = asyncio.Lock()
            async with lock:
                # Another fetch for this key may have stored what this
                # request needs while it waited.
                entry = self._entries.get(key)
                if entry is not None and self._fresh(entry, upcoming):
                    return entry.value
                # Sent now: a caller arriving from here on is another request,
                # which may not be handed this one's response (RFC 9111
                # sections 5.2.2.4, 5.2.2.5); it waits for the lock instead.
                if self._in_flight.get(slot) is asyncio.current_task():
                    del self._in_flight[slot]
                return await self._revalidate(key, fetch, upcoming)
        finally:
            if self._in_flight.get(slot) is asyncio.current_task():
                del self._in_flight[slot]

    async def _revalidate(
        self, key: str, fetch: Fetch[T], upcoming: Fields | None
    ) -> T:
        entry = self._entries.get(key)
        # A request carrying no-store: no part of any response to it may be
        # stored (RFC 9111 section 5.2.1.5) -- a 304 updates nothing either.
        # (The other request directives are advisory, section 5.2.1.)
        asked = (upcoming or {}).get("cache-control", ())
        keep = "no-store" not in _directives(asked)[0]
        validators = Validators.from_fields(entry.fields, entry.url) if entry else None
        requested = self._clock()
        response = await fetch(validators=validators)
        if (
            response.value is None
            and entry is not None
            and validators is not None
            and (
                response.url != entry.url
                or not _selects(
                    entry.fields, _received(response.fields, response.received_at)
                )
            )
        ):
            # A 304 that doesn't identify the stored response -- or comes
            # from another resource, a redirect having moved -- can't update
            # it (RFC 9111 section 4.3.4): ask again, unconditionally.
            validators = None
            requested = self._clock()
            response = await fetch(validators=None)
        fields = _received(response.fields, response.received_at)
        if response.value is None:
            if entry is None or validators is None:
                # A 304 answers only a conditional request; the caller's
                # fetch must have rejected this one already.
                raise RuntimeError(f"304 to an unconditional request for {key}")
            # RFC 9111 section 3.2: the 304's fields replace the stored ones
            # of the same name, except Content-Length (which describes the
            # 304 itself); a field it lacks -- Age included -- stays.
            merged = {
                **entry.fields,
                **{name: v for name, v in fields.items() if name != "content-length"},
            }
            freshness = freshness_of(
                merged,
                requested_at=response.requested_at,
                received_at=response.received_at,
            )
            if not (keep and _storable(freshness, response)):
                if keep:
                    self._entries.pop(key, None)
                return entry.value
            entry.fields = merged
            entry.request_fields = upcoming
            entry.redirected = bool(response.hops)
            entry.first_hop_vary, entry.hop_vary = _hop_vary(response)
            entry.stored_at = requested
            entry.lifetime = self._lifetime_of(entry.value, freshness, response)
            return entry.value
        freshness = freshness_of(
            fields,
            requested_at=response.requested_at,
            received_at=response.received_at,
        )
        if not (keep and _storable(freshness, response)):
            if keep:
                self._entries.pop(key, None)
            return response.value
        first_hop_vary, hop_vary = _hop_vary(response)
        self._entries[key] = _Entry(
            value=response.value,
            fields=fields,
            stored_at=requested,
            lifetime=self._lifetime_of(response.value, freshness, response),
            request_fields=upcoming,
            url=response.url,
            redirected=bool(response.hops),
            hop_vary=hop_vary,
            first_hop_vary=first_hop_vary,
        )
        return response.value
