"""RFC 9111 freshness parsing and the shared producer-response cache."""

import asyncio
import locale
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime, parsedate_to_datetime

import pytest
from hypothesis import event, given, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, precondition, rule

from aiomobilitydatabase.feeds import _freshness as freshness_module
from aiomobilitydatabase.feeds._freshness import (
    Freshness,
    FreshnessCache,
    Response,
    Validators,
    freshness_from_headers,
    header_fields,
)

T0 = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)


def _fresh(headers: dict[str, str], *, delay: float = 0.0) -> Freshness:
    return freshness_from_headers(
        headers, requested_at=T0, received_at=T0 + timedelta(seconds=delay)
    )


def _http(when: datetime) -> str:
    return format_datetime(when, usegmt=True)


# Last-Modified values must be HTTP-dates to validate anything.
MON = _http(T0 - timedelta(days=3))
TUE = _http(T0 - timedelta(days=2))
THU = _http(T0)
SUN = _http(T0 - timedelta(days=4))


# -- freshness_from_headers: examples -------------------------------------------


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({}, Freshness(lifetime=None)),
        ({"Cache-Control": "max-age=30"}, Freshness(lifetime=30.0)),
        ({"cache-control": "public, MAX-AGE=30"}, Freshness(lifetime=30.0)),
        ({"Cache-Control": 'max-age="30"'}, Freshness(lifetime=30.0)),
        ({"Cache-Control": "max-age=0"}, Freshness(lifetime=0.0)),
        ({"Cache-Control": "no-cache, max-age=30"}, Freshness(lifetime=0.0)),
        ({"Cache-Control": 'no-cache="Set-Cookie"'}, Freshness(lifetime=0.0)),
        (
            {"Cache-Control": "no-store, max-age=30"},
            Freshness(lifetime=None, storable=False),
        ),
        # Invalid or repeated max-age: stale, never a guess.
        ({"Cache-Control": "max-age=abc"}, Freshness(lifetime=0.0)),
        ({"Cache-Control": "max-age=-5"}, Freshness(lifetime=0.0)),
        ({"Cache-Control": "max-age"}, Freshness(lifetime=0.0)),
        ({"Cache-Control": "max-age=30, max-age=60"}, Freshness(lifetime=0.0)),
        # max-age wins over Expires.
        (
            {"Cache-Control": "max-age=30", "Expires": _http(T0 + timedelta(hours=1))},
            Freshness(lifetime=30.0),
        ),
        # Expires against Date, then against receipt when Date is absent.
        (
            {
                "Expires": _http(T0 + timedelta(seconds=90)),
                "Date": _http(T0 - timedelta(seconds=10)),
            },
            Freshness(lifetime=90.0),  # 100s lifetime minus 10s apparent age
        ),
        ({"Expires": _http(T0 + timedelta(seconds=45))}, Freshness(lifetime=45.0)),
        ({"Expires": "0"}, Freshness(lifetime=0.0)),
        # A numeric zone isn't an HTTP-date (RFC 9110 section 5.6.7): an
        # invalid Expires means already expired (RFC 9111 section 5.3).
        (
            {"Expires": _http(T0 + timedelta(seconds=45)).replace("GMT", "-0000")},
            Freshness(lifetime=0.0),
        ),
        ({"Expires": _http(T0 - timedelta(hours=1))}, Freshness(lifetime=0.0)),
        # Age reduces the remaining lifetime; an invalid Age is ignored.
        ({"Cache-Control": "max-age=30", "Age": "12"}, Freshness(lifetime=18.0)),
        ({"Cache-Control": "max-age=30", "Age": "40"}, Freshness(lifetime=0.0)),
        ({"Cache-Control": "max-age=30", "Age": "soon"}, Freshness(lifetime=30.0)),
    ],
)
def test_freshness_examples(headers: dict[str, str], expected: Freshness) -> None:
    assert _fresh(headers) == expected


def test_response_delay_is_counted_once() -> None:
    """max-age=30 with headers 4s after the request: RFC 9111 has it fresh
    until receipt + 30 - 4, which is request + 30 -- the lifetime is
    counted from the request, so the delay must not come off twice.
    """
    assert _fresh({"Cache-Control": "max-age=30"}, delay=4.0) == Freshness(30.0)
    assert _fresh({"Cache-Control": "max-age=30", "Age": "12"}, delay=4.0) == (
        Freshness(18.0)
    )
    # No Date: the receipt time stands in for it (RFC 9110), and the initial
    # age still applies -- 4s short of Expires, never past it.
    expires = {"Expires": _http(T0 + timedelta(seconds=45))}
    assert _fresh(expires, delay=4.0) == Freshness(41.0)


def test_repeated_cache_control_fields_merge() -> None:
    class TwoFields(dict[str, str]):
        def items(self):  # type: ignore[override]
            return [("Cache-Control", "max-age=30"), ("cache-control", "no-cache")]

    assert _fresh(TwoFields()) == Freshness(lifetime=0.0)


@pytest.mark.parametrize(
    "etag", ["*", '"a", "b"', "bare", 'W/"x" junk', 'w/"x"', '\xa0"v1"', '"v1"\u2028']
)
def test_an_etag_that_isnt_one_entity_tag_validates_nothing(etag: str) -> None:
    """``*`` or a list would change what If-None-Match means."""
    assert Validators.from_headers({"ETag": etag}) is None
    assert Validators.from_headers({"ETag": etag, "Last-Modified": MON}) == (
        Validators(etag=None, last_modified=MON)
    )


def test_a_non_ascii_etag_is_still_one_entity_tag() -> None:
    """obs-text is allowed in an entity-tag; it arrives decoded."""
    assert Validators.from_headers({"ETag": '"€1"'}) == Validators('"€1"', None)


def test_a_last_modified_that_isnt_an_http_date_validates_nothing() -> None:
    assert Validators.from_headers({"Last-Modified": "garbage"}) is None
    assert Validators.from_headers({"ETag": '"1"', "Last-Modified": "Mon"}) == (
        Validators('"1"', None)
    )


@pytest.mark.parametrize(
    "expires",
    [
        "Thu, 30 Jul 2026 13:00:00 +0000",  # numeric zone
        "Thu, 30 Jul 2026 13:00:00 UTC",  # not GMT
        "30 Jul 2026 13:00:00 GMT",  # no weekday
        "Thu, 3 Jul 2026 13:00:00 GMT",  # one-digit day
        "Thu, 30 Jul 2026 13:00 GMT",  # no seconds
        "Wed, 30 Jul 2026 13:00:00 GMT",  # wrong weekday
        "Thu, 31 Feb 2026 13:00:00 GMT",  # the right shape, no such day
        "Thu, \u0663\u0660 Jul 2026 13:00:00 GMT",  # Arabic-Indic digits
        "Thu, 30 Jul \uff12\uff10\uff12\uff16 13:00:00 GMT",  # full-width digits
        "\xa0Thu, 30 Jul 2026 13:00:00 GMT",  # not optional whitespace
        "Thu, 30 Jul 2026 12:00:60 GMT",  # :60 that isn't a leap second
        "Thu, 30 Jul 2026 1\u0663:00:00 GMT",  # an Arabic-Indic digit in the time
        "Thursday, \u0663\u0660-Jul-26 13:00:00 GMT",  # RFC 850, other digits
        "Thu Jul 30 13:00:00 \uff12\uff10\uff12\uff16",  # asctime, full-width year
        "Thu, 30-Jul-26 13:00:00 GMT",  # RFC 850 needs the long day name
        "Thursdayx, 30-Jul-26 13:00:00 GMT",
        "Thursday, 31-Feb-26 13:00:00 GMT",  # RFC 850, in no century
        "Thu Jul 2 13:00:00 2026",  # asctime pads a one-digit day with a space
        "Thu, 30 Jul 2026 13:00:00 gmt",  # case-sensitive
    ],
)
def test_an_expires_that_isnt_an_http_date_is_already_expired(expires: str) -> None:
    """RFC 9111 section 5.3: a cache MUST treat invalid date formats as in
    the past -- not as whatever a lenient parser makes of them.
    """
    assert _fresh({"Date": _http(T0), "Expires": expires}) == Freshness(0.0)


@pytest.mark.parametrize(
    "expires",
    [
        "Thu, 30 Jul 2026 13:00:00 GMT",  # IMF-fixdate
        "Thursday, 30-Jul-26 13:00:00 GMT",  # RFC 850
        "Thu Jul 30 13:00:00 2026",  # asctime
    ],
)
def test_every_http_date_form_is_read(expires: str) -> None:
    assert _fresh({"Date": _http(T0), "Expires": expires}) == Freshness(3600.0)
    padded = f" {expires}\t"  # optional whitespace is space and tab
    assert _fresh({"Date": _http(T0), "Expires": padded}) == Freshness(3600.0)


def test_a_leap_second_is_the_second_before_it() -> None:
    """RFC 9110 allows 23:59:60; datetime can't hold it, so 23:59:59."""
    assert freshness_module._http_date("Sat, 31 Dec 2016 23:59:60 GMT") == datetime(
        2016, 12, 31, 23, 59, 59, tzinfo=UTC
    )


def _day_name(when: datetime) -> str:
    return ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
            "Sunday")[when.weekday()]  # fmt: skip


def test_an_rfc850_date_is_placed_by_its_own_receipt_time() -> None:
    """Received in 2090, "-90" is 2090: the receipt time places the year,
    for Expires and for Date alike -- not the wall clock (2026's "-90" is
    1990, whose weekday doesn't match).
    """
    received = datetime(2090, 7, 30, 12, tzinfo=UTC)

    def rfc850(when: datetime) -> str:
        return f"{_day_name(when)}, {when:%d-%b-%y %H:%M:%S} GMT"

    expires = {"Expires": rfc850(received + timedelta(hours=1))}
    assert freshness_module.freshness_of(
        header_fields(expires), requested_at=received, received_at=received
    ) == Freshness(3600.0)
    dated = {"Cache-Control": "max-age=3600", "Date": rfc850(received)}
    later = received + timedelta(minutes=30)
    assert freshness_module.freshness_of(
        header_fields(dated), requested_at=later, received_at=later
    ) == Freshness(1800.0)


def test_an_asctime_single_digit_day_is_space_padded() -> None:
    assert freshness_module._http_date("Thu Jan  1 00:00:00 2026") == datetime(
        2026, 1, 1, tzinfo=UTC
    )


def test_a_four_digit_year_is_taken_as_written() -> None:
    """Year 0026 is AD 26 -- long expired -- not 2026."""
    expires = "Tue, 10 Nov 0026 13:00:00 GMT"
    assert _fresh({"Date": _http(T0), "Expires": expires}) == Freshness(0.0)


@pytest.mark.parametrize(
    ("value", "year"),
    [
        ("Thursday, 30-Jul-26 12:00:00 GMT", 2026),
        # 2069 is less than 50 years after 2026, so "69" is 2069 (RFC 9110).
        ("Tuesday, 01-Jan-69 00:00:00 GMT", 2069),
        # 2077 is more than 50 years ahead: the past century's 1977.
        ("Saturday, 01-Jan-77 00:00:00 GMT", 1977),
    ],
)
def test_an_rfc850_year_follows_the_fifty_year_rule(value: str, year: int) -> None:
    parsed = freshness_module._http_date(value, T0)
    assert parsed is not None and parsed.year == year


@pytest.mark.parametrize(
    ("value", "now", "expected"),
    [
        # The rule compares instants, not years: from 2026-11-10, 1 Nov 2076
        # is within 50 years, 1 Dec 2076 is not (so it is 1976).
        (
            "Sunday, 01-Nov-76 00:00:00 GMT",
            datetime(2026, 11, 10, tzinfo=UTC),
            datetime(2076, 11, 1, tzinfo=UTC),
        ),
        (
            "Wednesday, 01-Dec-76 00:00:00 GMT",
            datetime(2026, 11, 10, tzinfo=UTC),
            datetime(1976, 12, 1, tzinfo=UTC),
        ),
        # 1 Dec 2076's own weekday is no longer a valid reading.
        ("Tuesday, 01-Dec-76 00:00:00 GMT", datetime(2026, 11, 10, tzinfo=UTC), None),
        # After 2050 the window reaches into the next century: from 2060,
        # "05" is 2105, not 2005.
        (
            "Thursday, 01-Jan-05 00:00:00 GMT",
            datetime(2060, 1, 1, tzinfo=UTC),
            datetime(2105, 1, 1, tzinfo=UTC),
        ),
        # From 1990, "20" is 2020 -- not 1920, and not a fixed 2000s century.
        (
            "Wednesday, 01-Jan-20 00:00:00 GMT",
            datetime(1990, 1, 1, tzinfo=UTC),
            datetime(2020, 1, 1, tzinfo=UTC),
        ),
        # Asked on a leap day: 50 years on is 28 Feb.
        (
            "Monday, 28-Feb-78 00:00:00 GMT",
            datetime(2028, 2, 29, tzinfo=UTC),
            datetime(2078, 2, 28, tzinfo=UTC),
        ),
        # Near the end of datetime the window simply ends there.
        (
            "Monday, 01-Jan-90 00:00:00 GMT",
            datetime(9990, 6, 1, tzinfo=UTC),
            datetime(9990, 1, 1, tzinfo=UTC),
        ),
    ],
)
def test_the_fifty_year_rule_compares_instants(
    value: str, now: datetime, expected: datetime | None
) -> None:
    assert freshness_module._http_date(value, now) == expected


def test_dates_do_not_depend_on_the_process_locale() -> None:
    """The weekday check must not use strftime("%a"), which follows LC_TIME."""
    try:
        previous = locale.setlocale(locale.LC_TIME)
        locale.setlocale(locale.LC_TIME, "de_DE.UTF-8")
    except locale.Error:
        pytest.skip("de_DE.UTF-8 is not installed")
    try:
        expires = _http(T0 + timedelta(hours=1))
        assert _fresh({"Date": _http(T0), "Expires": expires}) == Freshness(3600.0)
        assert Validators.from_headers({"Last-Modified": THU}) == Validators(None, THU)
    finally:
        locale.setlocale(locale.LC_TIME, previous)


def test_validators_from_headers_and_back() -> None:
    assert Validators.from_headers({}) is None
    validators = Validators.from_headers({"etag": '"v1"', "Last-Modified": THU})
    assert validators == Validators(etag='"v1"', last_modified=THU)
    assert validators.request_headers() == {
        "If-None-Match": '"v1"',
        "If-Modified-Since": THU,
    }


# -- freshness_from_headers: properties -----------------------------------------


@given(
    max_age=st.integers(0, 10**6),
    age=st.integers(0, 10**6),
    delay=st.floats(0, 3600, allow_nan=False),
)
def test_lifetime_is_max_age_minus_age_clamped(
    max_age: int, age: int, delay: float
) -> None:
    freshness = _fresh(
        {"Cache-Control": f"max-age={max_age}", "Age": str(age)}, delay=delay
    )
    assert freshness.storable
    # Fresh until receipt + max_age - (age + delay); counted from the
    # request, which was ``delay`` earlier.
    expected = max_age - (age + delay) + delay
    assert freshness.lifetime == pytest.approx(max(0.0, expected), abs=1e-6)


@given(
    max_age=st.integers(0, 10**5),
    ages=st.tuples(st.integers(0, 10**5), st.integers(0, 10**5)),
)
def test_older_responses_are_never_fresher(max_age: int, ages: tuple[int, int]) -> None:
    younger, older = sorted(ages)
    fresh_young = _fresh({"Cache-Control": f"max-age={max_age}", "Age": str(younger)})
    fresh_old = _fresh({"Cache-Control": f"max-age={max_age}", "Age": str(older)})
    assert fresh_old.lifetime is not None and fresh_young.lifetime is not None
    assert fresh_old.lifetime <= fresh_young.lifetime


_PREFIX = st.lists(
    st.sampled_from(
        [
            "max-age=60",
            "public",
            "private",
            'ext="x',
            'ext="a, b"',
            '"',
            "\\",
            'foo"bar',
            'ext="a"b',
            "max-age=60 public",
            "ext=a;b",
        ]
    )
)


class _Fields(dict[str, str]):
    """Headers whose items() repeat a name, like aiohttp's multidict."""

    def __init__(self, pairs: list[tuple[str, str]]) -> None:
        super().__init__()
        self.pairs = pairs

    def items(self):  # type: ignore[override]
        return self.pairs


@given(prefix=_PREFIX, directive=st.sampled_from(["no-store", "no-cache"]))
def test_no_store_and_no_cache_always_win(prefix: list[str], directive: str) -> None:
    """Whatever comes first -- an unterminated quote included -- a later
    no-store or no-cache, in the same field or the next, is never hidden.
    """
    expected = (
        Freshness(None, storable=False) if directive == "no-store" else Freshness(0.0)
    )
    same_field = ", ".join([*prefix, directive])
    assert _fresh({"Cache-Control": same_field}) == expected
    two_fields = [("Cache-Control", ", ".join(prefix)), ("Cache-Control", directive)]
    assert _fresh(_Fields(two_fields)) == expected


@pytest.mark.parametrize(
    "header",
    [
        'max-age=60, foo"bar, no-store"',
        'max-age=60, private", no-store, x"',
        'max-age=60, ext="a"b, no-store',
    ],
)
def test_a_quote_that_isnt_an_argument_cannot_hide_no_store(header: str) -> None:
    """A quoted string may only be a directive's argument; a stray quote
    makes the field malformed, and no-store in it is still seen.
    """
    assert _fresh({"Cache-Control": header}) == Freshness(None, storable=False)


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        # A missing comma: not a directive list, and never a hidden no-store.
        ("max-age=600, no-cache no-store", Freshness(None, storable=False)),
        ("no-store no-cache", Freshness(None, storable=False)),
        ("max-age=600, public no-cache", Freshness(0.0)),
        ('foo="x"no-store', Freshness(None, storable=False)),
        ("max-age=60 no-store", Freshness(None, storable=False)),
        # Grammar alone: no BWS around "=", no text glued to a quoted string.
        ("max-age = 60", Freshness(0.0)),
        ('max-age=60, ext="a"b', Freshness(0.0)),
        ("max-age=60, ext=a;b", Freshness(0.0)),
        # Empty list elements are fine (RFC 9110 section 5.6.1).
        ("max-age=60,, public", Freshness(60.0)),
        # A no-store glued on by any separator is still a no-store.
        ("no-cache;no-store", Freshness(None, storable=False)),
        ("max-age=60;no-store", Freshness(None, storable=False)),
        ("public/no-store", Freshness(None, storable=False)),
        ("no-store:true", Freshness(None, storable=False)),
        ("x=y=no-store", Freshness(None, storable=False)),
        # Directive names are case-insensitive.
        ("No-Store", Freshness(None, storable=False)),
        ("NO-CACHE", Freshness(0.0)),
        ("NO-STORE;x", Freshness(None, storable=False)),
        # Optional whitespace is space and tab (RFC 9110 section 5.6.3); any
        # other space character breaks the field.
        ("\xa0max-age=600", Freshness(0.0)),
        ("max-age=600\x85", Freshness(0.0)),
        ("max-age=600,\u2028private", Freshness(0.0)),
        ("\x1fmax-age=600", Freshness(0.0)),
        (" \tmax-age=600 \t, public", Freshness(600.0)),
    ],
)
def test_cache_control_follows_its_grammar(header: str, expected: Freshness) -> None:
    """RFC 9111 section 5.2: ``token [ "=" ( token / quoted-string ) ]``.
    A field breaking it is stale now, and still scanned for no-store and
    no-cache split on commas and whitespace.
    """
    assert _fresh({"Cache-Control": header}) == expected


def test_an_unterminated_quote_makes_the_field_invalid() -> None:
    assert _fresh({"Cache-Control": 'max-age=60, ext="x'}) == Freshness(0.0)
    two_fields = [("Cache-Control", 'max-age=60, ext="x'), ("Cache-Control", "a")]
    assert _fresh(_Fields(two_fields)) == Freshness(0.0)


_HEADER_TEXT = st.one_of(
    st.text(max_size=40),
    # Digit-heavy values: huge max-age / Age must not overflow (RFC 9111
    # section 1.2.2 caps delta-seconds at 2^31).
    st.integers(0, 6000).map(lambda n: "9" * n),
    st.integers(0, 6000).map(lambda n: f"max-age={'9' * n}"),
)


@given(header=_HEADER_TEXT, age=_HEADER_TEXT, date=_HEADER_TEXT)
def test_parser_is_total_and_never_negative(header: str, age: str, date: str) -> None:
    freshness = _fresh(
        {"Cache-Control": header, "Age": age, "Expires": header, "Date": date}
    )
    assert freshness.lifetime is None or freshness.lifetime >= 0


def test_huge_values_are_capped_not_crashes() -> None:
    huge = "9" * 5000
    assert _fresh({"Cache-Control": f"max-age={huge}"}) == Freshness(2_147_483_648.0)
    assert _fresh({"Cache-Control": "max-age=60", "Age": huge}) == Freshness(0.0)


def test_a_list_valued_age_uses_its_first_member() -> None:
    # Layered CDNs append: RFC 9111 has a cache use the first value.
    assert _fresh({"Cache-Control": "max-age=60", "Age": "50, 50"}) == Freshness(10.0)


def test_a_comma_inside_a_quoted_directive_is_not_a_separator() -> None:
    header = 'community="a, max-age=3600", max-age=10'
    assert _fresh({"Cache-Control": header}) == Freshness(10.0)
    assert _fresh({"Cache-Control": 'community="x, max-age=3600"'}) == Freshness(None)
    escaped = r'ext="a \" , max-age=3600", max-age=5'
    assert _fresh({"Cache-Control": escaped}) == Freshness(5.0)


def test_repeated_date_fields_use_the_first() -> None:
    class TwoExpires(dict[str, str]):
        def items(self):  # type: ignore[override]
            return [
                ("Expires", _http(T0 + timedelta(seconds=30))),
                ("Expires", _http(T0 + timedelta(hours=1))),
            ]

    assert _fresh(TwoExpires()) == Freshness(lifetime=30.0)


# -- FreshnessCache: examples ----------------------------------------------------


@dataclass
class _Clock:
    now: float = 0.0

    def __call__(self) -> float:
        return self.now


def _headers(
    validators: Validators | None, lifetime: float | None, *, storable: bool = True
) -> dict[str, str]:
    headers: dict[str, str] = {}
    if not storable:
        headers["Cache-Control"] = "no-store"
    elif lifetime is not None:
        headers["Cache-Control"] = f"max-age={int(lifetime)}"
    if validators is not None and validators.etag:
        headers["ETag"] = validators.etag
    if validators is not None and validators.last_modified:
        headers["Last-Modified"] = validators.last_modified
    return headers


def _r[T](
    value: T | None,
    validators: Validators | None,
    lifetime: float | None,
    *,
    storable: bool = True,
) -> Response[T]:
    """A response whose headers declare exactly this; zero response delay."""
    fields = header_fields(_headers(validators, lifetime, storable=storable))
    return Response(value, fields, T0, T0)


def _raw[T](value: T | None, headers: dict[str, str]) -> Response[T]:
    return Response(value, header_fields(headers), T0, T0)


@dataclass
class _Producer:
    """A scripted producer: one queued Response (or exception) per request."""

    queue: list[Response[str] | BaseException] = field(default_factory=list)
    seen: list[Validators | None] = field(default_factory=list)
    gate: asyncio.Event | None = None

    async def __call__(self, *, validators: Validators | None) -> Response[str]:
        self.seen.append(validators)
        if self.gate is not None:
            await self.gate.wait()
        item = self.queue.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


V1 = Validators(etag='"1"', last_modified=None)


async def test_fresh_entries_are_served_without_a_request() -> None:
    clock = _Clock()
    cache: FreshnessCache[str] = FreshnessCache(clock)
    producer = _Producer([_r("a", V1, 30.0)])
    assert await cache.get("u", producer) == "a"
    clock.now = 29.9
    assert await cache.get("u", producer) == "a"
    assert len(producer.seen) == 1


async def test_freshness_counts_from_the_request_not_the_receipt() -> None:
    """A slow download doesn't add freshness: the clock starts at the request."""
    clock = _Clock()
    cache: FreshnessCache[str] = FreshnessCache(clock)

    async def slow(*, validators: Validators | None) -> Response[str]:
        clock.now += 5  # five seconds to download and parse
        return _r("a", None, 30.0)

    await cache.get("u", slow)
    clock.now = 30.0
    calls = []

    async def counting(*, validators: Validators | None) -> Response[str]:
        calls.append(validators)
        return _r("b", None, 30.0)

    assert await cache.get("u", counting) == "b"
    assert len(calls) == 1


async def test_expired_entries_revalidate_and_304_extends() -> None:
    clock = _Clock()
    cache: FreshnessCache[str] = FreshnessCache(clock)
    producer = _Producer([_r("a", V1, 10.0), _r(None, V1, 10.0)])
    await cache.get("u", producer)
    clock.now = 10.0
    assert await cache.get("u", producer) == "a"
    assert producer.seen == [None, V1]  # second request was conditional
    clock.now = 19.0
    assert await cache.get("u", producer) == "a"  # 304 refreshed the lifetime
    assert len(producer.seen) == 2


async def test_min_interval_only_covers_responses_that_declared_nothing() -> None:
    clock = _Clock()
    cache: FreshnessCache[str] = FreshnessCache(clock)
    producer = _Producer(
        [
            _r("a", None, None),  # declared nothing: the interval applies
            _r("b", None, 0.0),  # declared "revalidate": binding
            _r("c", None, None, storable=False),  # no-store: never stored
            _r("d", None, None),
        ]
    )
    assert await cache.get("u", producer, min_interval=5.0) == "a"
    clock.now = 4.0
    assert await cache.get("u", producer, min_interval=5.0) == "a"
    clock.now = 5.0
    assert await cache.get("u", producer, min_interval=5.0) == "b"
    assert await cache.get("u", producer, min_interval=5.0) == "c"
    assert await cache.get("u", producer, min_interval=5.0) == "d"
    assert len(producer.seen) == 4


async def test_no_store_on_a_304_drops_the_entry() -> None:
    cache: FreshnessCache[str] = FreshnessCache(_Clock())
    producer = _Producer(
        [
            _r("a", V1, 0.0),
            _r(None, V1, None, storable=False),
            _r("b", None, 0.0),
        ]
    )
    await cache.get("u", producer)
    assert await cache.get("u", producer) == "a"
    assert await cache.get("u", producer) == "b"
    assert producer.seen == [None, V1, None]  # nothing left to revalidate


async def test_a_selecting_304_rotates_only_the_validators_it_carries() -> None:
    both = Validators(etag='"1"', last_modified=MON)
    cache: FreshnessCache[str] = FreshnessCache(_Clock())
    producer = _Producer(
        [
            _r("a", both, 0.0),
            # Same strong ETag (selects), new Last-Modified: rotates it.
            _r(None, Validators(etag='"1"', last_modified=TUE), 0.0),
            # Only a Last-Modified, matching what is now stored: selects.
            _r(None, Validators(etag=None, last_modified=TUE), 0.0),
            _r(None, V1, 0.0),
        ]
    )
    for _ in range(4):
        assert await cache.get("u", producer) == "a"
    assert producer.seen == [
        None,
        both,
        Validators(etag='"1"', last_modified=TUE),  # ETag kept
        Validators(etag='"1"', last_modified=TUE),
    ]


@pytest.mark.parametrize(
    "not_modified",
    [
        pytest.param({}, id="no-validator"),
        pytest.param({"ETag": '"2"'}, id="another-strong-etag"),
        pytest.param({"ETag": 'W/"2"'}, id="a-non-matching-weak-etag"),
        pytest.param({"Last-Modified": SUN}, id="a-non-matching-last-modified"),
    ],
)
async def test_a_304_that_selects_nothing_updates_nothing(
    not_modified: dict[str, str],
) -> None:
    """RFC 9111 section 4.3.4: such a 304 MUST NOT update the stored
    response -- it may describe another representation. The cache asks
    again unconditionally instead of renewing ``a`` for ten minutes.
    """
    cache: FreshnessCache[str] = FreshnessCache(_Clock())
    stored = {"ETag": '"1"', "Last-Modified": MON, "Cache-Control": "max-age=0"}
    producer = _Producer(
        [
            _raw("a", stored),
            _raw(None, {**not_modified, "Cache-Control": "max-age=600"}),
            _raw("b", {"Cache-Control": "max-age=0"}),
        ]
    )
    await cache.get("u", producer)
    assert await cache.get("u", producer) == "b"
    assert producer.seen == [None, Validators('"1"', MON), None]


@pytest.mark.parametrize(
    ("stored", "not_modified"),
    [
        # A strong 304 tag never selects a weak stored one.
        pytest.param({"ETag": 'W/"1"'}, {"ETag": '"1"'}, id="strong-vs-weak-stored"),
        # Weak validators must ALL correspond: another edge's copy.
        pytest.param(
            {"ETag": 'W/"1"', "Last-Modified": _http(T0)},
            {"ETag": 'W/"1"', "Last-Modified": _http(T0 + timedelta(days=1))},
            id="weak-tag-matches-last-modified-doesnt",
        ),
        # A Last-Modified the stored response never had corresponds to
        # nothing stored.
        pytest.param(
            {"ETag": 'W/"1"'},
            {"ETag": 'W/"1"', "Last-Modified": _http(T0)},
            id="last-modified-the-stored-copy-lacks",
        ),
        # A bare 304 against Last-Modified alone: the strict rule's cost --
        # a producer leaving Last-Modified off its 304s gets a full retry.
        pytest.param({"Last-Modified": THU}, {}, id="bare-304-vs-last-modified"),
    ],
)
async def test_a_304_must_match_every_validator_it_carries(
    stored: dict[str, str], not_modified: dict[str, str]
) -> None:
    cache: FreshnessCache[str] = FreshnessCache(_Clock())
    producer = _Producer(
        [
            _raw("a", {**stored, "Cache-Control": "max-age=0"}),
            _raw(None, {**not_modified, "Cache-Control": "max-age=600"}),
            _raw("b", {"Cache-Control": "max-age=0"}),
        ]
    )
    await cache.get("u", producer)
    assert await cache.get("u", producer) == "b"
    assert producer.seen[2] is None  # re-asked unconditionally


async def test_an_invalid_last_modified_is_no_validator_on_either_side() -> None:
    """A weak tag plus a Last-Modified that isn't an HTTP-date: the request
    carries only the tag, and the 304 repeating both selects on the tag --
    the invalid Last-Modified is absent, not a mismatch forcing a retry.
    """
    clock = _Clock()
    cache: FreshnessCache[str] = FreshnessCache(clock)
    both = {"ETag": 'W/"v1"', "Last-Modified": "garbage"}
    producer = _Producer(
        [
            _raw("a", {**both, "Cache-Control": "max-age=0"}),
            _raw(None, {**both, "Cache-Control": "max-age=60"}),
        ]
    )
    await cache.get("u", producer)
    await cache.get("u", producer)
    clock.now = 59.0
    assert await cache.get("u", producer) == "a"
    assert producer.seen == [None, Validators('W/"v1"', None)]


async def test_last_modified_is_compared_as_an_instant() -> None:
    """The same instant in RFC 850 form still selects the stored response."""
    clock = _Clock()
    cache: FreshnessCache[str] = FreshnessCache(clock)
    rfc850 = "Thursday, 30-Jul-26 12:00:00 GMT"
    producer = _Producer(
        [
            _raw("a", {"Last-Modified": _http(T0), "Cache-Control": "max-age=0"}),
            _raw(None, {"Last-Modified": rfc850, "Cache-Control": "max-age=60"}),
        ]
    )
    await cache.get("u", producer)
    await cache.get("u", producer)
    clock.now = 59.0
    assert await cache.get("u", producer) == "a"
    assert len(producer.seen) == 2


async def test_a_retry_counts_freshness_from_its_own_request() -> None:
    """The non-selecting 304 took 5s; the retry's max-age=10 starts at the
    retry, not at the first request.
    """
    clock = _Clock()
    cache: FreshnessCache[str] = FreshnessCache(clock)
    first = {"ETag": '"1"', "Cache-Control": "max-age=0"}
    queue = [
        _raw("a", first),
        _raw(None, {}),
        _raw("b", {"Cache-Control": "max-age=10"}),
    ]

    async def fetch(*, validators: Validators | None) -> Response[str]:
        item = queue.pop(0)
        if item.value is None:
            clock.now += 5
        return item

    await cache.get("u", fetch)
    assert await cache.get("u", fetch) == "b"  # at 0: 304 (to 5s), retry at 5
    clock.now = 14.0
    assert await cache.get("u", fetch) == "b"  # still fresh: 5 + 10 > 14
    assert not queue


@pytest.mark.parametrize("tags", [('W/"1"', 'W/"1"'), ('"1"', 'W/"1"')])
async def test_a_weak_etag_selects_by_weak_comparison(tags: tuple[str, str]) -> None:
    stored_tag, new_tag = tags
    clock = _Clock()
    cache: FreshnessCache[str] = FreshnessCache(clock)
    producer = _Producer(
        [
            _raw("a", {"ETag": stored_tag, "Cache-Control": "max-age=0"}),
            _raw(None, {"ETag": new_tag, "Cache-Control": "max-age=60"}),
        ]
    )
    await cache.get("u", producer)
    await cache.get("u", producer)
    clock.now = 59.0
    assert await cache.get("u", producer) == "a"
    assert len(producer.seen) == 2


async def test_a_304_without_date_is_dated_at_receipt() -> None:
    """RFC 9110 section 6.6.1: a 304 with no Date takes its receipt time as
    Date, so the stored response's old Date can't age the renewal to 0.
    """
    clock = _Clock()
    cache: FreshnessCache[str] = FreshnessCache(clock)
    later = T0 + timedelta(seconds=120)
    first = {"Date": _http(T0), "Cache-Control": "max-age=60", "ETag": '"1"'}
    producer = _Producer(
        [
            _raw("a", first),
            Response(None, header_fields({"ETag": '"1"'}), later, later),
        ]
    )
    await cache.get("u", producer)
    clock.now = 120.0
    await cache.get("u", producer)
    clock.now = 179.0
    await cache.get("u", producer)
    assert len(producer.seen) == 2  # renewed for 60s from the 304


async def test_an_eagerly_finished_request_is_never_joined() -> None:
    """Under an eager task factory a request that never suspends finishes
    inside ensure_future; it must not be left for the next caller to join.
    """
    loop = asyncio.get_running_loop()
    loop.set_task_factory(asyncio.eager_task_factory)
    try:
        cache: FreshnessCache[str] = FreshnessCache(_Clock())
        calls: list[Validators | None] = []

        async def instant(*, validators: Validators | None) -> Response[str]:
            calls.append(validators)
            return _r("a", None, 0.0)

        assert await cache.get("u", instant) == "a"
        assert not cache._in_flight
        assert await cache.get("u", instant) == "a"
        assert len(calls) == 2
    finally:
        loop.set_task_factory(None)


async def test_a_304_carrying_only_its_etag_keeps_the_stored_freshness() -> None:
    """RFC 9111 section 4.3.4: a 304 updates the stored fields it carries
    and leaves the rest. One carrying only its ETag after no-cache is still
    no-cache, so a caller's min_interval (only for "declared nothing")
    can't extend it.
    """
    clock = _Clock()
    cache: FreshnessCache[str] = FreshnessCache(clock)
    producer = _Producer(
        [
            _raw("a", {"Cache-Control": "no-cache", "ETag": '"1"'}),
            _raw(None, {"ETag": '"1"'}),
            _raw(None, {"ETag": '"1"'}),
        ]
    )
    await cache.get("u", producer, min_interval=30.0)
    await cache.get("u", producer, min_interval=30.0)
    clock.now = 1.0
    await cache.get("u", producer, min_interval=30.0)
    assert producer.seen == [None, V1, V1]


async def test_a_304s_own_fields_replace_the_stored_ones() -> None:
    clock = _Clock()
    cache: FreshnessCache[str] = FreshnessCache(clock)
    producer = _Producer(
        [
            _raw("a", {"Cache-Control": "max-age=10", "ETag": '"1"'}),
            _raw(None, {"Cache-Control": "max-age=60", "ETag": '"1"'}),
        ]
    )
    await cache.get("u", producer)
    clock.now = 10.0
    await cache.get("u", producer)
    clock.now = 69.0
    assert await cache.get("u", producer) == "a"
    assert len(producer.seen) == 2  # the 304's max-age=60 replaced 10


async def test_the_lifetime_hook_runs_on_a_304_against_the_stored_value() -> None:
    """A long HTTP lifetime on a 304 can't outlast the body's own limit."""
    clock = _Clock()

    def lifetime(value: str, header: float | None, _requested: datetime) -> float:
        body = float(value.removeprefix("ttl="))
        return body if header is None else min(body, header)

    cache: FreshnessCache[str] = FreshnessCache(clock, lifetime=lifetime)
    producer = _Producer([_r("ttl=60", V1, 600.0), _r(None, V1, 600.0)])
    await cache.get("u", producer)
    clock.now = 61.0
    await cache.get("u", producer)  # 304, max-age=600 again
    clock.now = 125.0
    producer.queue.append(_r(None, V1, 600.0))
    await cache.get("u", producer)
    assert len(producer.seen) == 3  # the 304 renewed 60s, not 600s


async def test_a_304_without_cache_control_keeps_the_stored_lifetime() -> None:
    """Stored max-age=10 with a body allowing 300: a 304 carrying only its
    ETag keeps 10, under the hook too.
    """
    clock = _Clock()

    def lifetime(value: str, header: float | None, _requested: datetime) -> float:
        body = float(value.removeprefix("ttl="))
        return body if header is None else min(body, header)

    cache: FreshnessCache[str] = FreshnessCache(clock, lifetime=lifetime)
    producer = _Producer(
        [
            _raw("ttl=300", {"Cache-Control": "max-age=10", "ETag": '"1"'}),
            _raw(None, {"ETag": '"1"'}),
            _raw(None, {"ETag": '"1"'}),
        ]
    )
    await cache.get("u", producer)
    clock.now = 10.0
    await cache.get("u", producer)
    clock.now = 20.0
    await cache.get("u", producer)
    assert len(producer.seen) == 3


@pytest.mark.parametrize("bad", [float("inf"), float("-inf"), float("nan")])
async def test_a_non_finite_hook_lifetime_is_stale(bad: float) -> None:
    clock = _Clock()
    cache: FreshnessCache[str] = FreshnessCache(clock, lifetime=lambda *_: bad)
    producer = _Producer([_r("a", None, None), _r("b", None, None)])
    await cache.get("u", producer)
    clock.now = 1e9
    assert await cache.get("u", producer) == "b"
    assert cache._entries["u"].lifetime == 0.0


def _dated(value: int) -> datetime:
    return datetime.fromtimestamp(value, UTC)


async def test_older_responses_never_replace_newer_ones() -> None:
    cache: FreshnessCache[int] = FreshnessCache(_Clock(), timestamp=_dated)
    queue: list[Response[int]] = [
        _r(500, V1, 60.0),
        _r(400, None, 60.0),
        _r(600, None, 60.0),
    ]

    async def fetch(*, validators: Validators | None) -> Response[int]:
        return queue.pop(0)

    assert await cache.get("u", fetch) == 500
    cache._entries["u"].lifetime = 0.0  # force a refresh
    # The stale copy is rejected AND the entry marked stale: next call asks.
    assert await cache.get("u", fetch) == 500
    assert await cache.get("u", fetch) == 600


async def test_a_future_dated_copy_cannot_pin_the_cache() -> None:
    """A producer clock bug stamps one snapshot years ahead, then recovers.

    A stamp later than its own receipt counts as the receipt time, so any
    snapshot stamped after we received the bad one replaces it; a CDN copy
    stamped before that receipt is still an older copy.
    """
    now = int(T0.timestamp())
    future = now + 10 * 365 * 86_400
    cache: FreshnessCache[int] = FreshnessCache(_Clock(), timestamp=_dated)
    queue = [_r(future, None, 0.0), _r(now - 5, None, 0.0), _r(now + 5, None, 0.0)]

    async def fetch(*, validators: Validators | None) -> Response[int]:
        return queue.pop(0)

    assert await cache.get("u", fetch) == future
    assert await cache.get("u", fetch) == future  # older than the receipt
    assert await cache.get("u", fetch) == now + 5


async def test_a_slightly_fast_producer_clock_keeps_the_rule() -> None:
    """Stamped 2s after we received it (no NTP), then a 60s-older edge copy."""
    now = int(T0.timestamp())
    cache: FreshnessCache[int] = FreshnessCache(_Clock(), timestamp=_dated)
    queue = [_r(now + 2, None, 0.0), _r(now - 58, None, 0.0)]

    async def fetch(*, validators: Validators | None) -> Response[int]:
        return queue.pop(0)

    assert await cache.get("u", fetch) == now + 2
    assert await cache.get("u", fetch) == now + 2


async def test_concurrent_callers_share_one_request_and_its_error() -> None:
    cache: FreshnessCache[str] = FreshnessCache(_Clock())
    producer = _Producer([RuntimeError("boom")], gate=asyncio.Event())
    waiters = [asyncio.ensure_future(cache.get("u", producer)) for _ in range(5)]
    await asyncio.sleep(0)
    producer.gate.set()  # type: ignore[union-attr]
    results = await asyncio.gather(*waiters, return_exceptions=True)
    assert len(producer.seen) == 1
    assert all(isinstance(r, RuntimeError) for r in results)
    producer.gate = None
    producer.queue.append(_r("a", None, 30.0))
    assert await cache.get("u", producer) == "a"
    assert len(producer.seen) == 2


async def test_one_cancelled_waiter_does_not_cancel_the_others() -> None:
    cache: FreshnessCache[str] = FreshnessCache(_Clock())
    producer = _Producer([_r("a", None, 30.0)], gate=asyncio.Event())
    first = asyncio.ensure_future(cache.get("u", producer))
    second = asyncio.ensure_future(cache.get("u", producer))
    await asyncio.sleep(0)
    first.cancel()
    await asyncio.sleep(0)
    producer.gate.set()  # type: ignore[union-attr]
    assert await second == "a"
    assert first.cancelled()
    assert len(producer.seen) == 1


async def test_an_abandoned_failure_is_not_reported_as_unretrieved() -> None:
    cache: FreshnessCache[str] = FreshnessCache(_Clock())
    producer = _Producer([RuntimeError("boom")], gate=asyncio.Event())
    waiter = asyncio.ensure_future(cache.get("u", producer))
    await asyncio.sleep(0)
    waiter.cancel()
    producer.gate.set()  # type: ignore[union-attr]
    for _ in range(3):
        await asyncio.sleep(0)
    assert not cache._in_flight


async def test_the_receipt_clamp_works_in_whole_seconds() -> None:
    """Stamps are whole seconds: one stamped in the second the stored copy
    arrived (half a second in) is not older than that copy.
    """
    now = int(T0.timestamp())
    arrived = T0 + timedelta(milliseconds=500)
    cache: FreshnessCache[int] = FreshnessCache(_Clock(), timestamp=_dated)
    queue = [Response(now + 2, {}, arrived, arrived), _r(now, None, 0.0)]

    async def fetch(*, validators: Validators | None) -> Response[int]:
        return queue.pop(0)

    assert await cache.get("u", fetch) == now + 2
    assert await cache.get("u", fetch) == now


async def test_a_finished_request_is_never_joined() -> None:
    """When the shared request answers, it has already left the in-flight
    table: a caller arriving in that same loop step makes its own request
    instead of reusing a no-cache (or failed) answer.
    """
    cache: FreshnessCache[str] = FreshnessCache(_Clock())
    producer = _Producer([_r("a", None, 0.0)], gate=asyncio.Event())
    waiter = asyncio.ensure_future(cache.get("u", producer))
    await asyncio.sleep(0)
    task = cache._in_flight["u"]
    observed: list[tuple[bool, bool]] = []
    producer.gate.set()  # type: ignore[union-attr]
    # Runs right after the request's own step, before any done-callback.
    asyncio.get_running_loop().call_soon(
        lambda: observed.append((task.done(), "u" in cache._in_flight))
    )
    assert await waiter == "a"
    assert observed == [(True, False)]


async def test_a_request_cancelled_before_it_runs_leaves_no_trace() -> None:
    """Loop shutdown can cancel the shared request before its first step;
    the next call must make a fresh request, not await a dead one.
    """
    cache: FreshnessCache[str] = FreshnessCache(_Clock())
    producer = _Producer([_r("a", None, 30.0)])
    waiter = asyncio.ensure_future(cache.get("u", producer))
    await asyncio.sleep(0)  # get() has scheduled the request, not run it
    assert not producer.seen
    cache._in_flight["u"].cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    await asyncio.sleep(0)
    assert not cache._in_flight
    assert await cache.get("u", producer) == "a"


async def test_a_304_with_nothing_stored_is_a_bug_not_a_value() -> None:
    cache: FreshnessCache[str] = FreshnessCache(_Clock())
    producer = _Producer([_r(None, None, 0.0)])
    with pytest.raises(RuntimeError, match="unconditional"):
        await cache.get("u", producer)


# -- FreshnessCache: stateful model ---------------------------------------------


@dataclass(frozen=True)
class _Doc:
    version: int
    timestamp: int


def _doc_time(doc: _Doc) -> datetime:
    return datetime.fromtimestamp(doc.timestamp, UTC)


def _model_lifetime(
    cache_control: str | None, date: int | None, age: int | None, received: float
) -> tuple[bool, float | None]:
    """(storable, lifetime) for the header shapes the fake producer sends.

    Requests are instantaneous here, so the lifetime is max-age less the
    initial age: the larger of Date's apparent age and Age. Date is whole
    seconds; a missing one is the (whole-second) receipt time.
    """
    if cache_control is None:
        return True, None
    if cache_control == "no-store":
        return False, None
    if cache_control == "no-cache":
        return True, 0.0
    max_age = float(cache_control.removeprefix("max-age="))
    dated = date if date is not None else int(received)
    initial = max(0.0, received - dated, float(age or 0))
    return True, max(0.0, max_age - initial)


def _model_selects(
    stored_etag: str | None, stored_lm: str | None, sent: dict[str, str]
) -> bool:
    """RFC 9111 section 4.3.4, restated for what the producer sends.

    A strong tag must equal the stored tag. Otherwise every validator sent
    must correspond: a weak tag to the stored tag's opaque part, a
    Last-Modified to the stored one (the producer always writes the same
    format, so text equality is the instant). Nothing sent corresponds only
    to nothing stored.
    """
    tag, lm = sent.get("ETag"), sent.get("Last-Modified")
    if tag is not None and not tag.startswith("W/"):
        return tag == stored_etag
    if tag is None and lm is None:
        return stored_etag is None and stored_lm is None
    tag_ok = tag is None or (
        stored_etag is not None and stored_etag.removeprefix("W/") == tag[2:]
    )
    return tag_ok and (lm is None or lm == stored_lm)


_STYLES_304 = st.sampled_from(
    ["full", "redated", "validator_only", "weakened", "strengthened", "bare", "another"]
)


class FreshnessMachine(RuleBasedStateMachine):
    """Random clocks, publishes, failures, stale edges, calls and bursts.

    The fake producer records exactly what it served, header by header; a
    reference model applies the cache's documented rules to that record,
    and every call is checked against it: which requests were made with
    which validators, and what came back. The wall clock moves with the
    monotonic one; documents are stamped against it (a few seconds behind,
    slightly ahead, years ahead); responses carry Date (current, stale or
    none) and sometimes Age; ETags are strong or weak; and a 304 repeats
    the headers, carries only its ETag, carries nothing, or names another
    representation -- the last two select nothing and must be re-asked.
    """

    def __init__(self) -> None:
        super().__init__()
        self.clock = _Clock()
        self.cache: FreshnessCache[_Doc] = FreshnessCache(
            self.clock, timestamp=_doc_time
        )
        self.min_interval = 0.0
        # Producer state.
        self.version = 1
        self.published = _Doc(1, self._wall() - 1000)
        self.cache_control: str | None = None
        self.validators_offered = True
        self.weak = False
        self.lm_offered = False
        self.date_mode = "now"
        self.age: int | None = None
        self.style_304 = "full"
        self.fail_next = False
        self.stale_next = False
        # What the producer saw and served during one call:
        # (validators sent, headers served, document or None for a 304).
        self.log: list[tuple[Validators | None, dict[str, str], _Doc | None]] = []
        # Model of the cache.
        self.stored: _Doc | None = None
        self.stored_cc: str | None = None
        self.stored_etag: str | None = None
        self.stored_lm: str | None = None
        self.stored_date: int | None = None
        self.stored_age: int | None = None
        self.stored_received = 0
        self.stored_at = 0.0
        self.stored_lifetime: float | None = None
        self.last_kind = "none"

    def _wall(self) -> int:
        return int(T0.timestamp() + self.clock.now)

    def _received(self) -> float:
        return T0.timestamp() + self.clock.now

    def _etag(self, doc: _Doc) -> str | None:
        if not self.validators_offered:
            return None
        tag = f'"{doc.version}.{doc.timestamp}"'
        return f"W/{tag}" if self.weak else tag

    def _lm(self, doc: _Doc) -> str | None:
        if not self.lm_offered:
            return None
        return _http(datetime.fromtimestamp(doc.timestamp, UTC))

    def _headers(self, etag: str | None, lm: str | None = None) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.cache_control is not None:
            headers["Cache-Control"] = self.cache_control
        if etag is not None:
            headers["ETag"] = etag
        if lm is not None:
            headers["Last-Modified"] = lm
        if self.date_mode != "none":
            date = self._wall() - (20 if self.date_mode == "stale" else 0)
            headers["Date"] = _http(datetime.fromtimestamp(date, UTC))
        if self.age is not None:
            headers["Age"] = str(self.age)
        return headers

    async def _fetch(self, *, validators: Validators | None) -> Response[_Doc]:
        await asyncio.sleep(0)  # let a burst pile up on the in-flight task
        if self.fail_next:
            self.fail_next = False
            self.log.append((validators, {}, None))
            raise ConnectionError("producer down")
        doc = self.published
        if self.stale_next:
            self.stale_next = False
            doc = _Doc(doc.version, doc.timestamp - 50)  # an older edge copy
        etag, lm = self._etag(doc), self._lm(doc)
        now = T0 + timedelta(seconds=self.clock.now)
        unchanged = validators is not None and (
            (etag is not None and validators.etag == etag)
            or (etag is None and lm is not None and validators.last_modified == lm)
        )
        # Another edge's copy: the same tag, a different Last-Modified.
        redated = _http(datetime.fromtimestamp(doc.timestamp + 3600, UTC))
        if unchanged and etag is None:
            # Last-Modified alone: the tag-shaped styles collapse to these.
            style = {"bare": "bare", "another": "bare", "redated": "redated"}.get(
                self.style_304, "lm_only"
            )
            headers = {
                "lm_only": {**self._headers(None), "Last-Modified": lm or ""},
                "redated": {**self._headers(None), "Last-Modified": redated},
                "bare": {},
            }[style]
            self.log.append((validators, headers, None))
            return Response(None, header_fields(headers), now, now)
        if unchanged:
            assert etag is not None
            headers = {
                "full": self._headers(etag, lm),
                "validator_only": {"ETag": etag},
                # A CDN weakening the tag (as compression does): selects by
                # weak comparison.
                "weakened": {
                    **self._headers(etag),
                    "ETag": f"W/{etag.removeprefix('W/')}",
                },
                # The opposite: a strong tag for a weak stored one -- selects
                # nothing (a strong validator must match strongly).
                "strengthened": {
                    **self._headers(etag),
                    "ETag": etag.removeprefix("W/"),
                },
                # A matching tag but another Last-Modified: selects only if
                # the tag is strong (a weak one needs every validator).
                "redated": {**self._headers(etag, lm), "Last-Modified": redated},
                "bare": {},
                "another": {"ETag": '"another"', "Cache-Control": "max-age=600"},
            }[self.style_304]
            self.log.append((validators, headers, None))
            return Response(None, header_fields(headers), now, now)
        headers = self._headers(etag, lm)
        self.log.append((validators, headers, doc))
        return Response(doc, header_fields(headers), now, now)

    def _expect_fresh(self) -> bool:
        if self.stored is None:
            return False
        age = self.clock.now - self.stored_at
        if self.stored_lifetime is not None:
            return age < self.stored_lifetime
        return age < self.min_interval

    async def _call(self, count: int) -> list[_Doc | BaseException]:
        return await asyncio.gather(
            *(
                self.cache.get("u", self._fetch, min_interval=self.min_interval)
                for _ in range(count)
            ),
            return_exceptions=True,
        )

    @rule(
        seconds=st.one_of(
            # Lifetime boundaries (just under / exactly at 5s, 10s and 30s),
            # where an off-by-one or a missed refresh would show.
            st.sampled_from([0.0, 1.0, 4.9, 5.0, 9.9, 10.0, 29.9, 30.0]),
            st.floats(0, 120, allow_nan=False),
        )
    )
    def advance(self, seconds: float) -> None:
        self.clock.now += seconds

    @rule(
        cache_control=st.sampled_from(
            [None, "no-store", "no-cache", "max-age=0", "max-age=5", "max-age=30"]
        ),
        validators=st.booleans(),
        weak=st.booleans(),
        last_modified=st.booleans(),
    )
    def publish(
        self,
        cache_control: str | None,
        validators: bool,
        weak: bool,
        last_modified: bool = False,
    ) -> None:
        self.version += 1
        self.published = _Doc(self.version, self._wall() - 5)
        self.cache_control = cache_control
        self.validators_offered, self.weak = validators, weak
        self.lm_offered = last_modified

    @rule()
    def publish_cacheable(self) -> None:
        """The common real-world shape: fresh for 30s, a strong ETag, full
        304s. Weighted in so store -> expire -> 304 -> fresh again happens.
        """
        self.publish(cache_control="max-age=30", validators=True, weak=False)
        self.style_304, self.date_mode = "full", "now"

    @rule(weak=st.booleans())
    def publish_dated_cacheable(self, weak: bool) -> None:
        """Fresh for 30s with Last-Modified, and a weak tag or none: the shapes
        where Last-Modified decides whether a 304 selects.
        """
        self.publish(cache_control="max-age=30", validators=weak, weak=True)
        self.lm_offered = True
        self.date_mode = "now"

    @rule(ahead=st.sampled_from([2, 10**8]))
    def publish_from_a_fast_clock(self, ahead: int) -> None:
        """A producer clock a little (no NTP) or years (a bug) ahead."""
        self.version += 1
        self.published = _Doc(self.version, self._wall() + ahead)

    @rule()
    def publish_corrected(self) -> None:
        """Back on real time after a fast clock."""
        self.version += 1
        self.published = _Doc(self.version, self._wall())

    @rule(style=_STYLES_304)
    def set_304_style(self, style: str) -> None:
        self.style_304 = style

    @rule(
        date_mode=st.sampled_from(["now", "stale", "none"]),
        age=st.sampled_from([None, None, 5]),
    )
    def set_message_age(self, date_mode: str, age: int | None) -> None:
        """Date current, 20s stale, or missing; maybe an Age."""
        self.date_mode, self.age = date_mode, age

    @rule(interval=st.sampled_from([0.0, 10.0]))
    def set_min_interval(self, interval: float) -> None:
        self.min_interval = interval

    @rule()
    def fail_next_request(self) -> None:
        self.fail_next = True

    @rule()
    def serve_stale_edge_next(self) -> None:
        self.stale_next = True

    @precondition(lambda self: self.stored is not None and bool(self.stored_lifetime))
    @rule(style=_STYLES_304)
    def lapse_then_call_twice(self, style: str) -> None:
        """Let a declared lifetime lapse, revalidate, and call straight
        again: a selecting 304 must renew it -- with its own Date, Age and
        Cache-Control merged over the stored ones -- for the next call.
        """
        assert self.stored_lifetime is not None
        self.style_304 = style
        # Just past it: the exact instant is float-rounding territory, and
        # the examples pin the boundary itself.
        self.clock.now = self.stored_at + self.stored_lifetime + 0.001
        self.call(count=1)
        self.call(count=1)

    @rule()
    def call_twice(self) -> None:
        """Back to back: a 304 that renewed freshness must answer the next."""
        self.call(count=1)
        self.call(count=1)

    @rule(count=st.integers(1, 4))
    def call(self, count: int) -> None:
        expect_fresh = self._expect_fresh()
        will_fail = self.fail_next and not expect_fresh
        sent = (
            Validators(etag=self.stored_etag, last_modified=self.stored_lm)
            if self.stored is not None
            and (self.stored_etag is not None or self.stored_lm is not None)
            else None
        )
        self.log = []
        results = asyncio.run(self._call(count))
        if expect_fresh:
            assert self.log == []  # freshness: no request at all
            event(
                f"fresh hit after {self.last_kind}"
                + (" (declared lifetime)" if self.stored_lifetime else "")
            )
            assert all(r == self.stored for r in results)
            return
        # Sharing: a burst costs one request (two when a 304 is re-asked),
        # carrying exactly the stored validators.
        assert self.log and self.log[0][0] == sent
        if will_fail:
            assert len(self.log) == 1
            assert all(isinstance(r, ConnectionError) for r in results)
            return
        assert all(r == results[0] for r in results)
        _, headers, served = self.log[0]
        if served is None and "ETag" not in headers:
            event(
                "304 by Last-Modified alone"
                if "Last-Modified" in headers
                else "304 with no validator"
            )
        if served is None and not _model_selects(
            self.stored_etag, self.stored_lm, headers
        ):
            # A 304 that identifies nothing: asked again, unconditionally.
            event(f"304 ({self.style_304}) selects nothing: re-asked")
            assert len(self.log) == 2
            assert self.log[1][0] is None and self.log[1][2] is not None
            _, headers, served = self.log[1]
        else:
            assert len(self.log) == 1
        self.last_kind = "200" if served is not None else f"304 ({self.style_304})"
        event(f"request answered {self.last_kind}")
        assert results[0] == self._absorb(headers, served)

    def _absorb(self, headers: dict[str, str], served: _Doc | None) -> _Doc:
        """Apply the cache rules to what the producer served; return the answer."""
        received = self._received()
        date = (
            int(parsedate_to_datetime(headers["Date"]).timestamp())
            if "Date" in headers
            else int(received)  # dated at receipt
        )
        age = int(headers["Age"]) if "Age" in headers else None
        if served is None:
            # A selecting 304: its fields replace the stored ones of the same
            # name; Date always (it is dated at receipt if it had none).
            assert self.stored is not None
            answer = self.stored
            if "Cache-Control" in headers:
                self.stored_cc = headers["Cache-Control"]
            if "ETag" in headers:
                self.stored_etag = headers["ETag"]
            if "Last-Modified" in headers:
                self.stored_lm = headers["Last-Modified"]
            if "Age" in headers:
                self.stored_age = age
            self.stored_date = date
            storable, lifetime = _model_lifetime(
                self.stored_cc, self.stored_date, self.stored_age, received
            )
            if storable:
                self.stored_at, self.stored_lifetime = self.clock.now, lifetime
            else:
                self.stored = None
            return answer
        storable, lifetime = _model_lifetime(
            headers.get("Cache-Control"), date, age, received
        )
        if not storable:
            self.stored = None
            return served
        if self.stored is not None:
            # The newest thing we know of is the stored copy, as of its stamp
            # -- but no later than the second it actually reached us.
            known = min(self.stored.timestamp, self.stored_received)
            if served.timestamp < known:
                event("stale edge rejected")
                self.stored_at, self.stored_lifetime = self.clock.now, 0.0
                return self.stored
        self.stored = served
        self.stored_cc = headers.get("Cache-Control")
        self.stored_etag = headers.get("ETag")
        self.stored_lm = headers.get("Last-Modified")
        self.stored_date, self.stored_age = date, age
        self.stored_received = self._wall()
        self.stored_at, self.stored_lifetime = self.clock.now, lifetime
        return served


FreshnessMachine.TestCase.settings = settings(max_examples=600, deadline=None)
TestFreshnessMachine = FreshnessMachine.TestCase
