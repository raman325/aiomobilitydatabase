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
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import format_datetime
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
    ASCII, around optional space or tab, naming a real instant whose
    weekday matches. RFC 9111 section 5.3 has a cache treat any other
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
    fields = (_MONTHS.index(month) + 1, int(mday), int(hour), int(minute))
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
    return (parsed, leap) if _DAYS[parsed.weekday()] == day[:3] else None


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
                # quoted-pair: a backslash escapes HTAB, SP, VCHAR or obs-text.
                if not (char == "\t" or " " <= char <= "~" or char >= "\x80"):
                    return None
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted, closed = False, True
            elif not (char in "\t " or "!" <= char <= "~" or char >= "\x80"):
                return None  # qdtext: no controls, no DEL (RFC 9110 5.6.4)
            current.append(char)
            continue
        if char == ",":
            parts.append("".join(current))
            current, closed = [], False
            continue
        if char == '"':
            if closed or not "".join(current).endswith("="):
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
        parsed = [] if parts is None else [_directive(p.strip(_OWS)) for p in parts]
        if parts is None or any(
            directive is None and part.strip(_OWS)
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


def _normalized(name: str, values: Sequence[str] | None) -> tuple[str, ...]:
    """Normalize a request field's value as RFC 9111 section 4.1 allows.

    Field lines combine into one, joined by ", " as RFC 9110 section 5.3
    defines, each trimmed of the optional whitespace around it; nothing
    inside a line is touched (a quoted-string's spaces count). Cookie
    arrives already canonical (see :func:`_cookie_pairs`).
    """
    if not values:
        return ()
    if name == "cookie":
        return tuple(values)
    return (", ".join(value.strip(_OWS) for value in values),)


def _vary_matches(
    names: set[str],
    sent: Fields | None,
    upcoming: Fields | None,
    *,
    redirected: bool = False,
) -> bool:
    """Whether a stored response may answer the upcoming request (RFC 9111 4.1).

    Every request field its Vary ``names`` nominate must carry the same
    value, after normalization, in the request that produced it (``sent``)
    and in the one it would now stand in for (``upcoming``); a field absent
    from both matches. Unknown fields (None) confirm nothing. Through a
    redirect only aiohttp's automatic headers can be confirmed -- they are
    the same on every hop and every request unless the client set them,
    which both sides record -- so any other name fails to match.
    """
    if not names:
        return True
    if sent is None or upcoming is None:
        return False
    return all(
        (not redirected or name in _AUTOMATIC)
        and _normalized(name, sent.get(name)) == _normalized(name, upcoming.get(name))
        for name in names
    )


# aiohttp adds these itself when neither the session nor the request sets
# them -- the same values on every request of the process (Host: per URL).
_AUTOMATIC = frozenset({"host", "accept", "accept-encoding", "user-agent"})
# The cache's own conditional headers: not part of the request it answers
# (RFC 9111 section 4.3.1 has the cache generate them).
_CONDITIONAL = frozenset({"if-none-match", "if-modified-since"})


def _cookie_pairs(values: Iterable[str], *, merge: bool = False) -> tuple[str, ...]:
    """Return Cookie field values as canonical ``name=value`` pairs.

    Sorted by name, stably -- a repeated name keeps its values in the order
    sent, since a server may read either. ``merge`` keeps only the last
    value of a name, as aiohttp does when it rebuilds the header from the
    jar (``SimpleCookie``: jar cookies replace same-named explicit ones,
    output sorted); with an empty jar it sends the header verbatim. So the
    header predicted and the one sent compare equal when they carry the
    same cookies. A header aiohttp would parse differently (``k=v w``) can
    only fail to match, never match falsely.
    """
    cookies: list[tuple[str, str]] = []
    for value in values:
        for part in value.split(";"):
            if pair := part.strip(_OWS):
                name, _, cookie = pair.partition("=")
                cookies.append((name.strip(_OWS), cookie.strip(_OWS)))
    if merge:
        cookies = list(dict(cookies).items())
    return tuple(
        f"{name}={cookie}" for name, cookie in sorted(cookies, key=lambda c: c[0])
    )


def request_fields(
    session: aiohttp.ClientSession, url: str, headers: Mapping[str, str]
) -> dict[str, tuple[str, ...]]:
    """Return the request header fields this client controls for a GET of ``url``.

    The session's default headers merged with ``headers`` as aiohttp merges
    them (a request header replaces the defaults' same-named one, whatever
    its case; a later spelling of one name replaces an earlier one), and
    the cookies sent: explicit Cookie headers, then the session jar's for
    ``url``, canonical (see :func:`_cookie_pairs`), and the session's
    ``auth`` as the Authorization it sends when nothing sets one. aiohttp's
    automatic headers are left out: unless set here, they are fixed for the
    process. These are what a stored response's Vary is matched against
    (see :func:`sent_fields`). Pass ``headers`` without the cache's
    validators.
    """
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
        [*explicit, *(f"{morsel.key}={morsel.coded_value}" for morsel in jar.values())],
        merge=bool(jar),
    )
    # Deprecated in aiohttp 3.14 (and gone in 4), but still sent while set.
    auth = getattr(session, "auth", None)
    if auth is not None and "authorization" not in fields:
        fields["authorization"] = [auth.encode()]
    result = {name: tuple(values) for name, values in fields.items()}
    if pairs:
        result["cookie"] = pairs
    return result


def sent_fields(
    controlled: Fields, response: aiohttp.ClientResponse
) -> dict[str, tuple[str, ...]]:
    """Return the request fields that reached the response's target, as sent.

    For a direct response, every field on the wire (see
    :func:`_wire_fields`). A redirected request's later hops carried headers
    no later request can be checked against in advance, all but the
    automatic ones: those the client set are kept, the rest are aiohttp's
    own on every hop. (The original request's fields, which the first
    redirect answered, are :func:`first_hop_fields`.)
    """
    if response.history:
        return {
            name: tuple(controlled[name]) for name in controlled if name in _AUTOMATIC
        }
    return _wire_fields(controlled, response.request_info.headers)


def first_hop_fields(
    controlled: Fields, response: aiohttp.ClientResponse
) -> dict[str, tuple[str, ...]] | None:
    """Return the original request's fields when a response was redirected.

    The first redirect answered exactly that request, as sent (see
    :func:`_wire_fields`); None for a response that wasn't redirected.
    """
    if not response.history:
        return None
    return _wire_fields(controlled, response.history[0].request_info.headers)


def _wire_fields(
    controlled: Fields, headers: Mapping[str, str]
) -> dict[str, tuple[str, ...]]:
    """Return a request's fields as sent, for matching a Vary against.

    Every field on the wire, Cookie canonical, except what no later request
    can differ in: aiohttp's automatic headers and the cache's own
    validators, unless ``controlled`` (the :func:`request_fields` taken
    before sending) set them. So any other header a middleware or trace hook
    added counts -- it can't be predicted, and a Vary naming it never
    matches (an automatic name is taken to be aiohttp's fixed value).
    """
    sent = header_fields(headers)
    fields = {
        name: values
        for name, values in sent.items()
        if name != "cookie"
        and (name in controlled or name not in _AUTOMATIC | _CONDITIONAL)
    }
    if pairs := _cookie_pairs(sent.get("cookie", ())):
        fields["cookie"] = pairs
    return fields


@dataclass(frozen=True)
class Hop:
    """One redirect a response was reached through: its status and fields."""

    status: int
    fields: Fields


def hops(response: aiohttp.ClientResponse) -> tuple[Hop, ...]:
    """Return the redirects a response was reached through, in order."""
    return tuple(
        Hop(status=hop.status, fields=header_fields(hop.headers))
        for hop in response.history
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
    """Return Last-Modified if it is an HTTP-date; None otherwise.

    The cache only ever reads fields pinned at receipt (see
    :func:`_received`), whose dates mean the same whenever they are read.
    """
    value = _first(fields, "last-modified")
    return value if _http_date(value) is not None else None


def _same_instant(new: str, stored: str | None) -> bool:
    """Whether two Last-Modified values name the same instant.

    Compared as dates (an RFC 850 date equals its IMF-fixdate form, and a
    leap second is not the second before it); a value that isn't an
    HTTP-date names no instant and matches nothing.
    """
    new_date = _canonical_date(new)
    return new_date is not None and new_date == _canonical_date(stored)


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
    ``request_fields`` are the request header fields it was drawn by, as
    sent (see :func:`sent_fields`; None when unknown): what a stored
    response's Vary is checked against before it is reused. ``hops`` are
    the redirects followed to reach it, in order: the response answers the
    ORIGINAL URL only for as long as every hop may be reused too. ``url``
    is the URL that answered (the last hop's target): a 304 from any other
    resource says nothing about the stored one (RFC 9110 section 8.8.3).
    ``first_hop_request_fields`` are the original request's fields when it
    was redirected, which the first redirect's own Vary is matched against.
    """

    value: T | None
    fields: Fields
    requested_at: datetime
    received_at: datetime
    request_fields: Fields | None = field(default_factory=dict)
    hops: Sequence[Hop] = ()
    url: str | None = None
    # The original request's fields, as sent (see :func:`sent_fields`), when
    # the response was redirected; None otherwise or when unknown.
    first_hop_request_fields: Fields | None = None


class Fetch[T](Protocol):
    """One producer request: conditional when ``validators`` is given."""

    def __call__(self, *, validators: Validators | None) -> Awaitable[Response[T]]:
        """Fetch the resource, revalidating against ``validators`` if any."""
        ...


# An in-flight request's identity: its key and the request fields it carries.
_Slot = tuple[str, tuple[tuple[str, tuple[str, ...]], ...]]


@dataclass
class _Entry[T]:
    value: T
    fields: Fields  # the stored response's header fields, 304s merged in
    stored_at: float  # monotonic, taken when the request was sent
    lifetime: float | None  # None: the producer declared nothing
    received_at: datetime  # wall clock when ``value`` itself arrived
    # The controlled fields of the request it answers (None: unknown).
    request_fields: Fields | None = field(default_factory=dict)
    url: str | None = None  # the resource that answered (after redirects)
    redirected: bool = False  # reached through redirects
    # The redirects' own Vary values: replaced whole by each response, never
    # merged into ``fields`` (which a 304 updates). The first redirect's are
    # kept apart: its request was the original one, whose fields are known.
    hop_vary: tuple[str, ...] = ()
    first_hop_vary: tuple[str, ...] = ()
    first_hop_request_fields: Fields | None = None
    # The response's age when its request was sent (stored_at): its current
    # age is this plus the time held.
    age: float = 0.0
    # Through a redirect declaring nothing: ``lifetime`` is None, and an
    # opt-in ``min_interval`` may reuse it no longer than this.
    cap: float | None = None


def _hop_freshness(response: Response[object]) -> list[Freshness]:
    """Freshness of each redirect a response was reached through.

    Read against the whole fetch's request and receipt instants (aiohttp
    keeps no per-hop timing): exact when a hop carries Date, otherwise a
    hop's age can count up to the later hops' time too.

    One that declares nothing on a status that may not be reused without
    explicit freshness (302, 303, 307), and isn't marked ``public``, is
    stale now: the original URL must be asked every time.
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
            # RFC 9111 section 4.2.2: "public" makes any status heuristically
            # reusable.
            and "public" not in _directives(hop.fields.get("cache-control", ()))[0]
        ):
            hop_freshness = Freshness(lifetime=0.0)
        found.append(hop_freshness)
    return found


def _hop_vary(response: Response[object]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return (first redirect's, later redirects') Vary values.

    A redirect varying by request fields answers only requests that match
    (RFC 9111 section 4.1), just as the target does. The first was drawn by
    the original request, so its Vary is matched against that request's
    known fields; the later ones only through automatic headers.
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


def _entry_matches(
    entry: _Entry[Any],
    original: Fields | None,
    final: Fields | None,
    *,
    redirected: bool,
) -> bool:
    """Whether a stored response (and its redirects) answer a request.

    ``original`` are the request's own fields, which the first redirect's
    Vary is matched against exactly; ``final`` those reaching the target,
    which the target's and later redirects' Vary are matched against --
    only through automatic headers when either side was ``redirected``.
    """
    later = _vary_names({"vary": (*entry.fields.get("vary", ()), *entry.hop_vary)})
    return _vary_matches(
        _vary_names({"vary": entry.first_hop_vary}),
        entry.first_hop_request_fields,
        original,
    ) and _vary_matches(later, entry.request_fields, final, redirected=redirected)


def _answers(entry: _Entry[Any], response: Response[Any]) -> bool:
    """Whether a stored response answers the request a new response drew."""
    original = (
        response.first_hop_request_fields if response.hops else response.request_fields
    )
    return _entry_matches(
        entry,
        original,
        response.request_fields,
        redirected=entry.redirected or bool(response.hops),
    )


def _request_accepts(
    request_fields: Fields, current_age: float, remaining: float | None
) -> bool:
    """Whether a request's own Cache-Control accepts a stored response.

    RFC 9111 section 5.2.1, read as strictly as a response's directives:
    ``no-cache`` asks for validation; ``max-age=N`` accepts a response no
    older than N (its current age: initial age plus time held);
    ``min-fresh=N`` one still fresh for N more seconds (``remaining``; a
    response declaring no lifetime has none). A malformed field, or a
    repeated or invalid argument, accepts nothing.
    """
    asked, malformed = _directives(request_fields.get("cache-control", ()))
    if malformed or "no-cache" in asked:
        return False
    for name in ("max-age", "min-fresh"):
        if name not in asked:
            continue
        values = asked[name]
        limit = _delta_seconds(values[0], quoted=True) if len(values) == 1 else None
        if limit is None:
            return False
        if name == "max-age" and current_age > limit:
            return False
        if name == "min-fresh" and (remaining is None or remaining < limit):
            return False
    return True


def _frozen(fields: Fields) -> tuple[tuple[str, tuple[str, ...]], ...]:
    """Return a hashable, order-independent form of a set of request fields."""
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
    fields share one fetch; fetches for one key with different fields run
    one at a time, each re-checking the store first, so none acts on an
    entry another has replaced meanwhile.

    A redirected response answers its original URL only while every hop
    may be reused as well (RFC 9111 section 4: the URL is the key): the
    shortest lifetime of the chain, nothing when a hop declares nothing,
    and not stored when a hop may not be.

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
    second when stamps carry fractions (GBFS 3.0). An older copy is
    rejected even when it couldn't be stored (``no-store``, ``Vary: *``):
    it must not replace a newer stored one. Only a stored copy the new
    response's request would match is compared: another variant (Vary) is
    no answer to it, older or not.
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
        self._in_flight: dict[_Slot, asyncio.Task[T]] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _fresh(
        self, entry: _Entry[T], min_interval: float, request_fields: Fields
    ) -> bool:
        if not _entry_matches(
            entry, request_fields, request_fields, redirected=entry.redirected
        ):
            return False  # it answers a different request (RFC 9111 4.1)
        age = self._clock() - entry.stored_at
        if not _request_accepts(
            request_fields,
            entry.age + age,
            None if entry.lifetime is None else entry.lifetime - age,
        ):
            return False
        if entry.lifetime is not None:
            # A declared lifetime (0 included: revalidate) is binding.
            return age < entry.lifetime
        # Declared nothing: only the caller's opt-in interval applies, and
        # never past a lifetime some redirect or the target did declare.
        return age < min_interval and (entry.cap is None or age < entry.cap)

    async def get(
        self,
        key: str,
        fetch: Fetch[T],
        *,
        request_fields: Fields | None = None,
        min_interval: float = 0.0,
    ) -> T:
        """Return the value for ``key``, fetching only when it is not fresh.

        ``request_fields`` are the controlled header fields ``fetch``'s
        request will carry (see :func:`request_fields`; None: none), which
        a stored response's Vary must match. ``min_interval`` (seconds)
        lets a caller reuse a parse the producer declared NO freshness for;
        it never extends a declared lifetime, ``no-cache``, ``max-age=0``, a
        stale-edge rejection, ``no-store`` or a Vary mismatch.
        """
        upcoming: Fields = request_fields or {}
        entry = self._entries.get(key)
        if entry is not None and self._fresh(entry, min_interval, upcoming):
            return entry.value
        # Only a request carrying the same fields draws the same answer.
        slot = (key, _frozen(upcoming))
        task = self._in_flight.get(slot)
        if task is None:
            task = asyncio.ensure_future(
                self._refresh(slot, fetch, upcoming, min_interval)
            )
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
    ) -> tuple[float | None, float | None]:
        """Return (lifetime, cap) for a response and the redirects to it.

        The shortest declared lifetime of the chain binds. When a link
        declares nothing (no heuristics, so a bare 301 too) the chain
        declares nothing either -- None, for an opt-in ``min_interval`` --
        but never more than the shortest lifetime another link declared,
        returned as the cap (a declared 0, ``no-cache``, stays 0).
        """
        declared = freshness.lifetime
        if self._lifetime is not None:
            declared = self._lifetime(value, declared, response.requested_at)
        if declared is not None and not math.isfinite(declared):
            declared = 0.0
        lifetimes = [declared, *(hop.lifetime for hop in _hop_freshness(response))]
        known = [lifetime for lifetime in lifetimes if lifetime is not None]
        shortest = min(known) if known else None
        if len(known) < len(lifetimes):
            return None, shortest
        return shortest, None

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

    async def _refresh(
        self, slot: _Slot, fetch: Fetch[T], upcoming: Fields, min_interval: float
    ) -> T:
        key = slot[0]
        try:
            lock = self._locks.get(key)
            if lock is None:
                lock = self._locks[key] = asyncio.Lock()
            async with lock:
                # Another fetch for this key (other fields) may have stored
                # what this request needs while it waited. (Judged with the
                # min_interval of the call that started this fetch; each
                # cache passes one value.)
                entry = self._entries.get(key)
                if entry is not None and self._fresh(entry, min_interval, upcoming):
                    return entry.value
                return await self._revalidate(key, fetch, upcoming)
        finally:
            # Leave _in_flight in the same step the result lands, so no
            # caller can join a request that has already answered.
            if self._in_flight.get(slot) is asyncio.current_task():
                del self._in_flight[slot]

    async def _revalidate(self, key: str, fetch: Fetch[T], upcoming: Fields) -> T:
        entry = self._entries.get(key)
        # A request carrying no-store: no part of any response to it may be
        # stored (RFC 9111 section 5.2.1.5) -- a 304 updates nothing either.
        keep = "no-store" not in _directives(upcoming.get("cache-control", ()))[0]
        validators = Validators.from_fields(entry.fields) if entry else None
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
            entry.request_fields = response.request_fields
            entry.redirected = bool(response.hops)
            entry.first_hop_vary, entry.hop_vary = _hop_vary(response)
            entry.first_hop_request_fields = response.first_hop_request_fields
            entry.age = _age_when_requested(
                merged,
                requested_at=response.requested_at,
                received_at=response.received_at,
            )
            entry.stored_at = requested
            entry.lifetime, entry.cap = self._lifetime_of(
                entry.value, freshness, response
            )
            return entry.value
        if (
            entry is not None
            and _answers(entry, response)
            and self._rejects(entry, response.value)
        ):
            entry.stored_at = requested
            entry.lifetime = 0.0  # declared stale: the next call asks again
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
        lifetime, cap = self._lifetime_of(response.value, freshness, response)
        self._entries[key] = _Entry(
            value=response.value,
            fields=fields,
            stored_at=requested,
            lifetime=lifetime,
            received_at=response.received_at,
            request_fields=response.request_fields,
            url=response.url,
            cap=cap,
            redirected=bool(response.hops),
            hop_vary=_hop_vary(response)[1],
            first_hop_vary=_hop_vary(response)[0],
            first_hop_request_fields=response.first_hop_request_fields,
            age=_age_when_requested(
                fields,
                requested_at=response.requested_at,
                received_at=response.received_at,
            ),
        )
        return response.value
