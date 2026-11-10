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


def test_response_delay_counts_against_freshness() -> None:
    assert _fresh({"Cache-Control": "max-age=30"}, delay=4.0) == Freshness(26.0)


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
    assert freshness.lifetime == pytest.approx(max(0.0, max_age - (age + delay)))


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


@given(directives=st.lists(st.sampled_from(["max-age=60", "public", "private"])))
def test_no_store_always_wins(directives: list[str]) -> None:
    header = ", ".join([*directives, "no-store"])
    assert _fresh({"Cache-Control": header}) == Freshness(None, storable=False)


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


def test_validators_merge_on_a_304() -> None:
    stored = Validators(etag='"1"', last_modified="Mon")
    assert stored.updated_by(None) == stored
    assert stored.updated_by(Validators(etag=None, last_modified="Tue")) == Validators(
        etag='"1"', last_modified="Tue"
    )


# -- FreshnessCache: examples ----------------------------------------------------


@dataclass
class _Clock:
    now: float = 0.0

    def __call__(self) -> float:
        return self.now


def _r[T](
    value: T | None,
    validators: Validators | None,
    lifetime: float | None,
    *,
    storable: bool = True,
) -> Response[T]:
    return Response(value, validators, Freshness(lifetime, storable), T0)


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
    cache._entries["u"].validators = None
    producer.queue += [_r(None, V1, 0.0), _r(None, None, 0.0)]
    await cache.get("u", producer)  # an unsolicited-looking 304 still lands
    await cache.get("u", producer)
    assert producer.seen[-1] == V1


async def test_the_lifetime_hook_runs_on_a_304_against_the_stored_value() -> None:
    """A long HTTP lifetime on a 304 can't outlast the body's own limit."""
    clock = _Clock()

    def lifetime(value: str, header: float | None, _received: datetime) -> float:
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


async def test_a_non_finite_hook_lifetime_is_stale() -> None:
    cache: FreshnessCache[str] = FreshnessCache(
        _Clock(), lifetime=lambda *_: float("nan")
    )
    producer = _Producer([_r("a", None, None), _r("b", None, None)])
    await cache.get("u", producer)
    assert await cache.get("u", producer) == "b"


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
    """A producer clock bug stamps one snapshot in the future, then recovers.

    A timestamp later than its own receipt is no observation, so it never
    blocks the corrected (earlier-stamped) snapshots that follow.
    """
    future = int(T0.timestamp()) + 10 * 365 * 86_400
    cache: FreshnessCache[int] = FreshnessCache(_Clock(), timestamp=_dated)
    queue = [_r(future, None, 0.0), _r(int(T0.timestamp()) - 5, None, 0.0)]

    async def fetch(*, validators: Validators | None) -> Response[int]:
        return queue.pop(0)

    assert await cache.get("u", fetch) == future
    assert await cache.get("u", fetch) == int(T0.timestamp()) - 5


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


class FreshnessMachine(RuleBasedStateMachine):
    """Random clocks, publishes, failures, stale edges, calls and bursts.

    The fake producer records exactly what it served; a reference model
    applies the cache's documented rules to that record, and every call is
    checked against it: how many requests were made, and what came back.
    Every response is "received" at T0 (wall clock); document timestamps
    count up from T0 - 1000s, so they are real observations -- except a
    future-dated publish, which must never pin the cache.
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
        self.published = _Doc(1, int(T0.timestamp()) - 1000)
        self.lifetime: float | None = None
        self.storable = True
        self.validators_offered = True
        self.fail_next = False
        self.stale_next = False
        self.requests = 0
        self.served: list[_Doc | None] = []  # None = answered 304
        # Model of the cache.
        self.stored: _Doc | None = None
        self.stored_at = 0.0
        self.stored_lifetime: float | None = None
        self.last_kind = "none"

    async def _fetch(self, *, validators: Validators | None) -> Response[_Doc]:
        self.requests += 1
        await asyncio.sleep(0)  # let a burst pile up on the in-flight task
        if self.fail_next:
            self.fail_next = False
            raise ConnectionError("producer down")
        doc = self.published
        if self.stale_next:
            self.stale_next = False
            doc = _Doc(doc.version, doc.timestamp - 50)  # an older edge copy
        etag = Validators(etag=f'"{doc.version}.{doc.timestamp}"', last_modified=None)
        offered = etag if self.validators_offered else None
        freshness = Freshness(self.lifetime, self.storable)
        if validators is not None and validators == offered:
            self.served.append(None)
            return Response(None, None, freshness, T0)
        self.served.append(doc)
        return Response(doc, offered, freshness, T0)

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
        lifetime=st.sampled_from([None, 0.0, 5.0, 30.0]),
        storable=st.booleans(),
        validators=st.booleans(),
    )
    def publish(self, lifetime: float | None, storable: bool, validators: bool) -> None:
        self.version += 1
        self.published = _Doc(self.version, self.published.timestamp + 10)
        self.lifetime, self.storable = lifetime, storable
        self.validators_offered = validators

    @rule()
    def publish_cacheable(self) -> None:
        """Publish the common real-world shape: fresh for 30s, with an ETag.

        Weighted in on purpose: the store -> expire -> 304 -> fresh again
        path needs a cacheable, validated response, which ``publish`` only
        draws now and then.
        """
        self.publish(lifetime=30.0, storable=True, validators=True)

    @rule()
    def publish_future_dated(self) -> None:
        """A producer clock bug: one snapshot stamped years ahead.

        The next publish returns to real time; the cache must take it.
        """
        self.version += 1
        self.published = _Doc(self.version, int(T0.timestamp()) + 10**8)
        self.lifetime, self.storable = 0.0, True

    @rule()
    def publish_corrected(self) -> None:
        self.version += 1
        self.published = _Doc(self.version, int(T0.timestamp()) - 500 + self.version)
        self.lifetime, self.storable = 0.0, True

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
        event(f"request answered {self.last_kind}")
        assert returned == self._absorb(self.served[-1])

    def _absorb(self, served: _Doc | None) -> _Doc:
        """Apply the cache rules to what the producer served; return the answer."""
        if served is None:  # 304: the stored copy is confirmed
            assert self.stored is not None
            answer = self.stored
            if self.storable:
                self.stored_lifetime = self.lifetime
                self.stored_at = self._requested
            else:
                self.stored = None
            return answer
        if not self.storable:
            self.stored = None
            return served
        if self.stored is not None and served.timestamp < self.stored.timestamp <= int(
            T0.timestamp()
        ):
            # Never older (only against a real observation): keep, stale.
            event("stale edge rejected")
            self.stored_at, self.stored_lifetime = self._requested, 0.0
            return self.stored
        self.stored = served
        self.stored_at, self.stored_lifetime = self._requested, self.lifetime
        return served

    @property
    def _requested(self) -> float:
        # Requests are instantaneous on the fake clock: request = receipt.
        return self.clock.now


FreshnessMachine.TestCase.settings = settings(max_examples=200, deadline=None)
TestFreshnessMachine = FreshnessMachine.TestCase
