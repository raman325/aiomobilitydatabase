"""RFC 9111 freshness parsing and the shared producer-response cache."""

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import pytest
from hypothesis import event, given, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, rule

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
        # A "-0000" zone parses naive; HTTP dates are always GMT.
        (
            {"Expires": _http(T0 + timedelta(seconds=45)).replace("GMT", "-0000")},
            Freshness(lifetime=45.0),
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


def test_validators_from_headers_and_back() -> None:
    assert Validators.from_headers({}) is None
    validators = Validators.from_headers({"etag": '"v1"', "Last-Modified": "Thu"})
    assert validators == Validators(etag='"v1"', last_modified="Thu")
    assert validators.request_headers() == {
        "If-None-Match": '"v1"',
        "If-Modified-Since": "Thu",
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
        ["max-age=60", "public", "private", 'ext="x', 'ext="a, b"', '"', "\\"]
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
    producer = _Producer([_r("a", V1, 10.0), _r(None, None, 10.0)])
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
            _r(None, None, None, storable=False),
            _r("b", None, 0.0),
        ]
    )
    await cache.get("u", producer)
    assert await cache.get("u", producer) == "a"
    assert await cache.get("u", producer) == "b"
    assert producer.seen == [None, V1, None]  # nothing left to revalidate


async def test_a_304_rotates_only_the_validators_it_carries() -> None:
    both = Validators(etag='"1"', last_modified="Mon")
    cache: FreshnessCache[str] = FreshnessCache(_Clock())
    producer = _Producer(
        [
            _r("a", both, 0.0),
            _r(None, Validators(etag=None, last_modified="Tue"), 0.0),
            _r(None, None, 0.0),
            _r(None, None, 0.0),
        ]
    )
    for _ in range(4):
        assert await cache.get("u", producer) == "a"
    assert producer.seen == [
        None,
        both,
        Validators(etag='"1"', last_modified="Tue"),  # ETag kept
        Validators(etag='"1"', last_modified="Tue"),
    ]


async def test_an_unvalidated_entry_takes_a_304s_validators() -> None:
    cache: FreshnessCache[str] = FreshnessCache(_Clock())
    producer = _Producer([_r("a", None, 0.0)])
    await cache.get("u", producer)
    cache._entries["u"].fields = {}
    producer.queue += [_r(None, V1, 0.0), _r(None, None, 0.0)]
    await cache.get("u", producer)  # an unsolicited-looking 304 still lands
    await cache.get("u", producer)
    assert producer.seen[-1] == V1


async def test_a_bare_304_keeps_the_stored_freshness_headers() -> None:
    """RFC 9111 section 4.3.4: a 304 updates the stored fields it carries
    and leaves the rest. A bare 304 after no-cache is still no-cache, so a
    caller's min_interval (only for "declared nothing") can't extend it.
    """
    clock = _Clock()
    cache: FreshnessCache[str] = FreshnessCache(clock)
    producer = _Producer(
        [
            _raw("a", {"Cache-Control": "no-cache", "ETag": '"1"'}),
            _raw(None, {}),
            _raw(None, {}),
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
            _raw(None, {"Cache-Control": "max-age=60"}),
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
    producer = _Producer([_r("ttl=60", V1, 600.0), _r(None, None, 600.0)])
    await cache.get("u", producer)
    clock.now = 61.0
    await cache.get("u", producer)  # 304, max-age=600 again
    clock.now = 125.0
    producer.queue.append(_r(None, None, 600.0))
    await cache.get("u", producer)
    assert len(producer.seen) == 3  # the 304 renewed 60s, not 600s


async def test_a_bare_304_keeps_the_shorter_stored_lifetime_under_the_hook() -> None:
    """Stored max-age=10 with a body allowing 300: a bare 304 keeps 10."""
    clock = _Clock()

    def lifetime(value: str, header: float | None, _requested: datetime) -> float:
        body = float(value.removeprefix("ttl="))
        return body if header is None else min(body, header)

    cache: FreshnessCache[str] = FreshnessCache(clock, lifetime=lifetime)
    producer = _Producer(
        [
            _raw("ttl=300", {"Cache-Control": "max-age=10", "ETag": '"1"'}),
            _raw(None, {}),
            _raw(None, {}),
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
    with pytest.raises(RuntimeError, match="nothing stored"):
        await cache.get("u", producer)


# -- FreshnessCache: stateful model ---------------------------------------------


@dataclass(frozen=True)
class _Doc:
    version: int
    timestamp: int


def _doc_time(doc: _Doc) -> datetime:
    return datetime.fromtimestamp(doc.timestamp, UTC)


def _model_freshness(cache_control: str | None) -> tuple[bool, float | None]:
    """The four header shapes the fake producer sends: (storable, lifetime)."""
    if cache_control is None:
        return True, None
    if cache_control == "no-store":
        return False, None
    if cache_control == "no-cache":
        return True, 0.0
    return True, float(cache_control.removeprefix("max-age="))


class FreshnessMachine(RuleBasedStateMachine):
    """Random clocks, publishes, failures, stale edges, calls and bursts.

    The fake producer records exactly what it served and checks the
    validators each request carries; a reference model applies the cache's
    documented rules to that record, and every call is checked against it:
    how many requests were made, what was sent, and what came back. The
    wall clock moves with the monotonic one, and documents are stamped
    against it -- a few seconds behind, slightly ahead (an un-synced
    producer clock), or years ahead (a clock bug).
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
        self.bare_304 = False
        self.fail_next = False
        self.stale_next = False
        self.requests = 0
        self.served: list[_Doc | None] = []  # None = answered 304
        # Model of the cache.
        self.stored: _Doc | None = None
        self.stored_cc: str | None = None
        self.stored_etag: str | None = None
        self.stored_received = 0
        self.stored_at = 0.0
        self.stored_lifetime: float | None = None
        self.last_kind = "none"

    def _wall(self) -> int:
        return int(T0.timestamp() + self.clock.now)

    async def _fetch(self, *, validators: Validators | None) -> Response[_Doc]:
        self.requests += 1
        # What the cache must send: the validators of what it stores.
        expected = (
            Validators(etag=self.stored_etag, last_modified=None)
            if self.stored is not None and self.stored_etag is not None
            else None
        )
        assert validators == expected
        await asyncio.sleep(0)  # let a burst pile up on the in-flight task
        if self.fail_next:
            self.fail_next = False
            raise ConnectionError("producer down")
        doc = self.published
        if self.stale_next:
            self.stale_next = False
            doc = _Doc(doc.version, doc.timestamp - 50)  # an older edge copy
        etag = f'"{doc.version}.{doc.timestamp}"' if self.validators_offered else None
        headers: dict[str, str] = {}
        if self.cache_control is not None:
            headers["Cache-Control"] = self.cache_control
        if etag is not None:
            headers["ETag"] = etag
        now = T0 + timedelta(seconds=self.clock.now)
        if validators is not None and validators.etag == etag:
            self.served.append(None)
            return Response(
                None, header_fields({} if self.bare_304 else headers), now, now
            )
        self.served.append(doc)
        self._served_cc, self._served_etag = self.cache_control, etag
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
    )
    def publish(self, cache_control: str | None, validators: bool) -> None:
        self.version += 1
        self.published = _Doc(self.version, self._wall() - 5)
        self.cache_control = cache_control
        self.validators_offered = validators

    @rule()
    def publish_cacheable(self) -> None:
        """Publish the common real-world shape: fresh for 30s, with an ETag.

        Weighted in on purpose: the store -> expire -> 304 -> fresh again
        path needs a cacheable, validated response, which ``publish`` only
        draws now and then.
        """
        self.publish(cache_control="max-age=30", validators=True)

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

    @rule(cache_control=st.sampled_from(["max-age=30", "no-cache", "max-age=5"]))
    def publish_validated_then_bare_304s(self, cache_control: str) -> None:
        """Validated, with freshness, and every later 304 bare: the stored
        headers must keep governing (RFC 9111 section 4.3.4).
        """
        self.publish(cache_control=cache_control, validators=True)
        self.bare_304 = True

    @rule(bare=st.booleans())
    def set_304_style(self, bare: bool) -> None:
        """Whether a 304 repeats the freshness headers or carries none."""
        self.bare_304 = bare

    @rule(interval=st.sampled_from([0.0, 10.0]))
    def set_min_interval(self, interval: float) -> None:
        self.min_interval = interval

    @rule()
    def fail_next_request(self) -> None:
        self.fail_next = True

    @rule()
    def serve_stale_edge_next(self) -> None:
        self.stale_next = True

    @rule(count=st.integers(1, 4))
    def call(self, count: int) -> None:
        expect_fresh = self._expect_fresh()
        will_fail = self.fail_next and not expect_fresh
        before_requests, before_served = self.requests, len(self.served)
        results = asyncio.run(self._call(count))
        # Sharing: a burst costs at most one request; freshness: none at all.
        assert self.requests - before_requests == (0 if expect_fresh else 1)
        if will_fail:
            assert all(isinstance(r, ConnectionError) for r in results)
            return
        assert all(r == results[0] for r in results)
        returned = results[0]
        if expect_fresh:
            event(f"fresh hit after {self.last_kind}")
            assert returned == self.stored
            return
        assert len(self.served) == before_served + 1
        self.last_kind = "304" if self.served[-1] is None else "200"
        if self.served[-1] is None:
            self.last_kind += " (bare)" if self.bare_304 else " (headers)"
        event(f"request answered {self.last_kind}")
        assert returned == self._absorb(self.served[-1])

    def _absorb(self, served: _Doc | None) -> _Doc:
        """Apply the cache rules to what the producer served; return the answer."""
        if served is None:
            # A 304 confirms the stored copy. Its own headers replace the
            # stored ones of the same name; a bare 304 changes nothing.
            assert self.stored is not None
            answer = self.stored
            if not self.bare_304:
                self.stored_cc = self.cache_control
            storable, lifetime = _model_freshness(self.stored_cc)
            if storable:
                self.stored_at, self.stored_lifetime = self.clock.now, lifetime
            else:
                self.stored = None
            return answer
        storable, lifetime = _model_freshness(self._served_cc)
        if not storable:
            self.stored = None
            return served
        if self.stored is not None:
            # The newest thing we know of is the stored copy, as of its stamp
            # -- but no later than when it actually reached us.
            known = min(self.stored.timestamp, self.stored_received)
            if served.timestamp < known:
                event("stale edge rejected")
                self.stored_at, self.stored_lifetime = self.clock.now, 0.0
                return self.stored
        self.stored, self.stored_cc, self.stored_etag = (
            served,
            self._served_cc,
            self._served_etag,
        )
        self.stored_received = self._wall()
        self.stored_at, self.stored_lifetime = self.clock.now, lifetime
        return served


FreshnessMachine.TestCase.settings = settings(max_examples=200, deadline=None)
TestFreshnessMachine = FreshnessMachine.TestCase
