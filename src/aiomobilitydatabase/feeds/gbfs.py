"""GbfsFeedHandle: station and vehicle snapshots for a GBFS system."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from datetime import UTC, date, datetime
from functools import partial
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import aiohttp

from ._freshness import (
    FreshnessCache,
    Response,
    Validators,
    header_fields,
    request_fields,
)
from ._freshness import hops as _hops
from ._frozen import FrozenDict
from ._redirects import get_withholding
from ._urls import _require_http_url  # deliberate friend access: shared URL guard
from .const import GBFS_LANGUAGE_PREFERENCE
from .exceptions import FeedParseError, SourceConnectionError
from .geo import Circle, in_circle
from .models import (
    GbfsAlert,
    GbfsPricingPlan,
    GbfsStation,
    GbfsSystemInfo,
    GbfsSystemRegion,
    GbfsVehicle,
    GbfsVehicleType,
)

if TYPE_CHECKING:
    from ..models import GbfsFeed
    from .client import MobilityFeedsClient


# RFC 3339 section 5.6 date-time: the offset is REQUIRED; "T" may be "t"
# or a space (its NOTE). Its own ranges -- hour 00-23, minute 00-59, second
# 00-60 -- not ISO 8601's (Python 3.14's fromisoformat takes 24:00). ASCII
# digits only.
_RFC3339 = re.compile(
    r"([0-9]{4}-[0-9]{2}-[0-9]{2})[Tt ]"
    r"((?:[01][0-9]|2[0-3]):[0-5][0-9]:(?:[0-5][0-9]|60))(\.[0-9]+)?"
    r"([Zz]|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])"
)
# GBFS Date: "ISO 8601 Complete Date Extended Format: YYYY-MM-DD".
_ISO_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")


def _entry_text(entry: dict[str, Any]) -> str | None:
    """One localized entry's ``text``: anything but a non-empty string is None.

    ``str(entry.get("text"))`` would render a missing text as the literal
    string ``"None"`` (and ``false`` as ``"False"``) — the selected entry
    must instead degrade to "no text available".
    """
    return _plain_text(entry.get("text"))


def _record_id(value: Any) -> str | None:
    """One record's own id as a string, or None when it has none.

    ``str(value)`` would synthesize the literal id ``"None"`` for an
    id-less record — and those synthetic ids COLLIDE across records,
    corrupting any consumer that keys on them (an entity registry, say).
    A blank id collides identically, so it is None too. Same motivation
    as :func:`_entry_text`: never surface a stringified None. GBFS ids are
    strings; a number is read as one -- JSON has one number type, so
    ``7.0`` is the id ``"7"``, as ``7`` is -- but a boolean, object, array
    or non-finite number (``NaN`` would make every such id ``"nan"``) is
    no id at all.
    """
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        if value.is_integer():
            value = int(value)
    if isinstance(value, bool) or not isinstance(value, str | int | float):
        return None
    return str(value) or None


def _is_preferred_language(tag: Any) -> bool:
    """Whether a BCP 47 tag is the preferred language (tags ignore case)."""
    return isinstance(tag, str) and tag.casefold() == GBFS_LANGUAGE_PREFERENCE


def _localized(value: Any) -> str | None:
    """Normalize GBFS text: 3.x localized lists vs 2.x plain strings.

    Also the 3.0 localized URL fields (``terms_url``, ``alerts[].url``):
    a Localized URL has the same ``{text, language}`` shape.
    """
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, list):
        for entry in value:
            if isinstance(entry, dict) and _is_preferred_language(
                entry.get("language")
            ):
                return _entry_text(entry)
        if value and isinstance(value[0], dict):
            return _entry_text(value[0])
    return None


def _version_key(version: str | None) -> tuple[int, ...]:
    try:
        return tuple(int(part) for part in (version or "0").split("."))
    except ValueError:
        return (0,)


def _rental_uris(value: Any) -> Mapping[str, str] | None:
    """Pass through a GBFS ``rental_uris`` object: platform key -> URI.

    Identical shape in 2.x and 3.x (``android``/``ios``/``web`` keys on
    station_information rows and free_bike_status/vehicle_status rows).
    Keys are kept as provided; non-string URI values are dropped rather
    than coerced, and anything that isn't a non-empty object (absent,
    null, wrong type, or nothing string-valued left) is None.
    """
    if not isinstance(value, dict):
        return None
    uris = {str(key): uri for key, uri in value.items() if isinstance(uri, str)}
    return FrozenDict(uris) if uris else None


def _coordinate(value: Any) -> float | None:
    """Normalize one ``lat``/``lon`` cell to a finite float, else None (unknown).

    :func:`~.geo.in_circle` compares the value and takes its cosine, so a
    string coordinate would raise and a NaN would answer every comparison
    False; both are UNKNOWN instead. Numeric strings are accepted because
    producers ship them, and an integer literal too large for a float
    (JSON allows one of any size) is UNKNOWN rather than an OverflowError.
    """
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        return None
    try:
        coordinate = float(value)
    except (OverflowError, ValueError):
        return None
    return coordinate if math.isfinite(coordinate) else None


def _vehicle_types(value: Any) -> Mapping[str, int] | None:
    """Normalize a station's ``vehicle_types_available`` to id -> count.

    ``count`` is a REQUIRED non-negative integer: an entry without a usable
    type id or count (absent, negative, fractional, a string) is dropped
    rather than coerced. An explicitly empty list is {}, i.e. zero of every
    type. Anything that isn't a list, or a non-empty list with no usable
    entry, is None, i.e. "not published".
    """
    if not isinstance(value, list):
        return None
    if not value:
        return FrozenDict({})
    types: dict[str, int] = {}
    for entry in value:
        if not isinstance(entry, Mapping):
            continue
        type_id = _record_id(entry.get("vehicle_type_id"))
        count = _count(entry.get("count"))
        if type_id is not None and count is not None:
            types[type_id] = count
    return FrozenDict(types) if types else None


def _reported_at(value: Any) -> datetime | None:
    """Parse a GBFS ``last_reported``, which changed type across versions.

    2.x ships POSIX seconds, 3.0 ships an RFC 3339 string, whose offset
    is REQUIRED (section 5.6): one without (``2026-07-31T12:13:20``), a
    bare date, or another ISO 8601 form names no instant, so None -- GBFS
    defines no default zone. A leap second (``:60``) reads as the second
    before it. Anything else -- including a bool, which is an int in Python
    -- degrades to None like every other malformed scalar at this boundary.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        try:
            return datetime.fromtimestamp(value, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str) and (match := _RFC3339.fullmatch(value)) is not None:
        day, clock, fraction, offset = match.groups()
        if clock.endswith(":60"):
            clock = clock[:-2] + "59"
        offset = "+00:00" if offset in ("Z", "z") else offset
        try:
            return datetime.fromisoformat(f"{day}T{clock}{fraction or ''}{offset}")
        except ValueError:
            return None
    return None


def _iso_date(value: Any) -> date | None:
    """Parse a GBFS ``YYYY-MM-DD`` cell, degrading to None on anything else.

    Only that form: ISO 8601's basic (``20260115``) and week dates aren't.
    """
    if not isinstance(value, str) or not _ISO_DATE.fullmatch(value):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _languages(data: Mapping[str, Any]) -> list[str]:
    """GBFS 3.0 ``languages`` (array) or 2.x ``language`` (single string)."""
    raw = data.get("languages")
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, str) and item]
    single = data.get("language")
    return [single] if isinstance(single, str) and single else []


def _plain_text(value: Any) -> str | None:
    """Return a verbatim string cell; empty or non-string degrades to None."""
    return value if isinstance(value, str) and value else None


def _count(value: Any) -> int | None:
    """Return a non-negative count; bools and junk degrade to None.

    JSON has one number type: ``2.0`` is the integer 2 (JSON Schema's
    ``integer`` accepts it); ``2.5`` is no count.
    """
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if value >= 0 else None


def _number(value: Any) -> float | None:
    """Return a finite number; bools, strings and junk degrade to None.

    An int wider than a float can hold raises OverflowError from
    ``float()``, so the conversion is guarded rather than the result
    checked -- a producer's absurd integer is malformed input like any
    other, and this boundary degrades instead of raising (see
    ``_ttl_seconds``).
    """
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    try:
        converted = float(value)
    except (OverflowError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def _id_list(value: Any) -> list[str]:
    """Ids from a GBFS array, dropping entries that are not usable ids."""
    if not isinstance(value, list):
        return []
    return [found for item in value if (found := _record_id(item)) is not None]


def _alert_times(value: Any) -> list[tuple[datetime, datetime | None]]:
    """Return a GBFS alert's ``times`` array as (start, end) pairs.

    ``start`` is REQUIRED: a period without a usable one is dropped, never
    read as active since forever. A missing ``end`` is open-ended.
    """
    if not isinstance(value, list):
        return []
    return [
        (start, _reported_at(item.get("end")))
        for item in value
        if isinstance(item, Mapping)
        and (start := _reported_at(item.get("start"))) is not None
    ]


def _rows(document: Mapping[str, Any], key: str) -> list[Mapping[str, Any]]:
    """Extract the ``data.<key>`` row list, keeping object-shaped rows only.

    A GBFS document's envelope is producer data: ``data`` may be a list,
    the row list a string, a row an int. Every non-conforming shape
    degrades to "no rows" so the snapshot methods stay total.
    """
    data = document.get("data")
    rows = data.get(key) if isinstance(data, Mapping) else None
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, Mapping)]


def _as_bool(value: Any) -> bool | None:
    """Coerce a GBFS status flag to bool without lying on ambiguous input.

    ``bool("false")`` is ``True`` in Python, so strings (and anything else
    that isn't already a bool/int/float) are treated as UNKNOWN (``None``)
    rather than silently coerced.
    """
    if isinstance(value, bool | int | float):
        return bool(value)
    return None


def _ttl_seconds(value: Any) -> float | None:
    """Read a document's ``ttl`` in seconds; None when it is unusable.

    A ttl that isn't a finite, non-negative number is unusable (None)
    rather than failing a document that otherwise parsed; ``0`` is a real
    declaration ("always refresh"). Plain numeric strings are accepted
    because producers do ship them, and an integer literal too large for a
    float is unusable like any other.
    """
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        return None
    try:
        ttl = float(value)
    except (OverflowError, ValueError):
        return None
    return ttl if math.isfinite(ttl) and ttl >= 0 else None


def _document_lifetime(
    document: Mapping[str, Any], header_lifetime: float | None, requested_at: datetime
) -> float | None:
    """How long a GBFS document stays fresh, in seconds from ``requested_at``.

    The document's ``ttl`` counts from its own ``last_updated`` (GBFS: the
    data will be updated again ``ttl`` seconds after it last was), so a
    document already ``n`` seconds old when requested has ``ttl - n`` left,
    and the result never exceeds ``ttl`` (a ``last_updated`` in the future
    is clock skew, not extra freshness). A ``ttl`` that is present but
    unusable -- or can't be placed in time, its REQUIRED ``last_updated``
    missing or unparseable -- declares 0, just as an invalid ``max-age``
    would. When the
    HTTP headers declare a lifetime too (``header_lifetime``, also counted
    from the request), the shorter one wins; neither declaring anything is
    None. Runs on every response, a 304's included (with the stored
    document and the stored headers merged with the 304's), so a CDN's
    long ``max-age`` can never outlast the document's own ttl.
    """
    declared = [header_lifetime] if header_lifetime is not None else []
    if "ttl" in document:
        ttl = _ttl_seconds(document["ttl"])
        updated = _reported_at(document.get("last_updated"))
        if ttl is None or updated is None:
            declared.append(0.0)
        else:
            elapsed = max(0.0, (requested_at - updated).total_seconds())
            declared.append(max(0.0, ttl - elapsed))
    return min(declared, default=None)


def _endpoints_from_discovery(document: Any) -> dict[str, str]:
    """Resolve the name->url endpoint table from a GBFS discovery document.

    GBFS 3.x publishes ``data.feeds`` directly; 2.x nests the feed list
    under language codes (``data.en.feeds``) — prefer the library's
    language preference and otherwise take the first language block that
    carries feeds, mirroring the fallback order of :func:`_localized`.
    The result feeds the same endpoint table the catalog path builds from
    version metadata, so every handle method works identically after this.

    Total over arbitrary JSON: any document that yields no usable
    name/url pair raises :class:`FeedParseError` — including an absent or
    wrongly typed ``data``, so a malformed discovery document can never
    surface as a KeyError/TypeError to a caller.
    """
    data = document.get("data") if isinstance(document, Mapping) else None
    feeds: Any = None
    if isinstance(data, Mapping):
        feeds = data.get("feeds")
        if not isinstance(feeds, list):  # 2.x language-keyed layout
            candidates = [
                block
                for block in data.values()
                if isinstance(block, Mapping) and isinstance(block.get("feeds"), list)
            ]
            preferred = next(
                (
                    block
                    for tag, block in data.items()
                    if _is_preferred_language(tag)
                    and isinstance(block, Mapping)
                    and isinstance(block.get("feeds"), list)
                ),
                None,
            )
            if preferred is not None:
                candidates.insert(0, preferred)
            if candidates:
                feeds = candidates[0]["feeds"]
    endpoints = {
        feed["name"]: feed["url"]
        for feed in (feeds if isinstance(feeds, list) else [])
        if isinstance(feed, Mapping)
        and _plain_text(feed.get("name"))
        and _plain_text(feed.get("url"))
    }
    if not endpoints:
        raise FeedParseError("GBFS discovery document lists no usable feeds")
    return endpoints


class GbfsFeedHandle:
    """Snapshot access to one GBFS system.

    Endpoints resolve either from catalog metadata (:meth:`create`) or
    straight from the system's own auto-discovery document
    (:meth:`create_from_url`); every snapshot method behaves identically
    on both. A discovery document is a GBFS file like any other: once its
    ``ttl`` lapses it is asked again, so a long-lived handle follows
    endpoints the system moves. Catalog endpoints are fixed for the
    handle's life.
    """

    def __init__(
        self,
        client: MobilityFeedsClient,
        feed: GbfsFeed | None,
        endpoints: dict[str, str],
        headers: Mapping[str, str] | None = None,
        *,
        discovery_url: str | None = None,
    ) -> None:
        """Initialize; internal — use MobilityFeedsClient.get_gbfs_feed()."""
        self._client = client
        self._feed = feed
        self._endpoints = endpoints
        self._headers = headers
        self._discovery_url = discovery_url
        # Per-url parsed documents, reused only as the producer allows: its
        # ttl (from last_updated) and HTTP freshness, the shorter winning,
        # or after a 304. Shared between concurrent calls.
        self._docs: FreshnessCache[dict[str, Any]] = FreshnessCache(
            lifetime=_document_lifetime
        )

    @classmethod
    async def create(cls, client: MobilityFeedsClient, feed_id: str) -> GbfsFeedHandle:
        """Resolve endpoint URLs from the newest catalog-listed version."""
        feed = await client.catalog.get_gbfs_feed(feed_id)
        best: dict[str, str] = {}
        for version in sorted(
            feed.versions or [], key=lambda v: _version_key(v.version)
        ):
            endpoints = {
                endpoint.name: endpoint.url
                for endpoint in (version.endpoints or [])
                if endpoint.name and endpoint.url
            }
            if endpoints:
                best = endpoints  # last (highest) version with endpoints wins
        return cls(client, feed, best)

    @classmethod
    async def create_from_url(
        cls,
        client: MobilityFeedsClient,
        discovery_url: str,
        headers: Mapping[str, str] | None = None,
    ) -> GbfsFeedHandle:
        """Resolve endpoint URLs from a GBFS auto-discovery document.

        The discovery document (``gbfs.json``) is the spec's standard
        entry point and already lists every published endpoint, so no
        catalog lookup is needed — its feed list becomes the same endpoint
        table :meth:`create` builds from catalog version metadata.
        ``headers`` apply to the discovery fetch and every subsequent
        document fetch made through this handle.
        """
        handle = cls(client, None, {}, headers=headers, discovery_url=discovery_url)
        await handle._endpoint_table()  # fetched now: a bad URL fails here
        return handle

    async def _endpoint_table(self) -> dict[str, str]:
        """Return the endpoint table, re-read from discovery once its ttl lapses.

        A re-read that fails keeps the last table that worked: it only lists
        where the documents are, which are each still fetched fresh. The
        first read, when the handle is created, has none to keep and raises.
        """
        if self._discovery_url is not None:
            try:
                self._endpoints = _endpoints_from_discovery(
                    await self._fetch_cached(self._discovery_url, "GBFS discovery URL")
                )
            except (SourceConnectionError, FeedParseError):
                if not self._endpoints:
                    raise
        return self._endpoints

    @staticmethod
    async def _fetch_json_document(
        client: MobilityFeedsClient,
        url: str,
        headers: Mapping[str, str] | None,
        context: str,
        validators: Validators | None = None,
    ) -> Response[dict[str, Any]]:
        """GET one GBFS JSON document and validate its data envelope.

        With ``validators`` the request is conditional and a ``304`` comes
        back with no value; a 304 to an unconditional request raises. The
        cache derives the document's freshness from it (see
        :func:`_document_lifetime`).
        """
        _require_http_url(url, context)
        session = client._get_session()  # deliberate friend access
        requested_at = datetime.now(UTC)
        try:
            async with get_withholding(
                session,
                url,
                params={},
                headers=dict(headers) if headers else {},
                total=client.timeout_seconds,
                secret=None,
                conditional=(
                    None if validators is None else validators.request_headers()
                ),
                conditional_url=None if validators is None else validators.url,
            ) as (resp, history):
                received_at = datetime.now(UTC)
                fields = header_fields(resp.headers)
                if resp.status == HTTPStatus.NOT_MODIFIED:
                    if validators is None:
                        raise SourceConnectionError(
                            f"GBFS endpoint answered 304 to an unconditional "
                            f"request: {url}",
                            status=int(HTTPStatus.NOT_MODIFIED),
                        )
                    return Response(
                        value=None,
                        fields=fields,
                        requested_at=requested_at,
                        received_at=received_at,
                        hops=_hops(resp, history),
                        url=str(resp.url),
                    )
                if resp.status >= HTTPStatus.BAD_REQUEST:
                    raise SourceConnectionError(
                        f"GBFS endpoint error {resp.status}: {url}", status=resp.status
                    )
                if resp.status not in (
                    HTTPStatus.OK,
                    HTTPStatus.NON_AUTHORITATIVE_INFORMATION,
                ):
                    raise SourceConnectionError(
                        f"GBFS endpoint answered {resp.status} without a document: "
                        f"{url}",
                        status=resp.status,
                    )
                try:
                    document = await resp.json(content_type=None)
                except ValueError as err:
                    raise FeedParseError(f"Malformed GBFS JSON from {url}") from err
                hops = _hops(resp, history)
                answered = str(resp.url)
        except (TimeoutError, aiohttp.ClientError) as err:
            raise SourceConnectionError(f"Error fetching {url}: {err}") from err
        if not isinstance(document, dict) or "data" not in document:
            raise FeedParseError(f"GBFS document missing data envelope: {url}")
        return Response(
            value=document,
            fields=fields,
            requested_at=requested_at,
            received_at=received_at,
            hops=hops,
            url=answered,
        )

    async def _document(
        self, name: str, endpoints: Mapping[str, str] | None = None
    ) -> dict[str, Any]:
        """One endpoint's document; ``endpoints`` is a table read this call."""
        if endpoints is None:
            endpoints = await self._endpoint_table()
        url = endpoints.get(name)
        if url is None:
            raise SourceConnectionError(f"GBFS endpoint not published: {name}")
        return await self._fetch_cached(url, f"GBFS {name} endpoint URL")

    async def _fetch_cached(self, url: str, context: str) -> dict[str, Any]:
        """One GBFS document, reused as long as the producer allows."""
        return await self._docs.get(
            url,
            partial(
                self._fetch_json_document, self._client, url, self._headers, context
            ),
            request_fields=request_fields(
                self._client._get_session(),  # deliberate friend access
                url,
                self._headers or {},
            ),
        )

    async def get_system_info(self) -> GbfsSystemInfo:
        """GBFS system information.

        A document without a usable ``system_id`` raises
        :class:`FeedParseError`: the id is this system's identity, and a
        synthesized one would silently key consumer state to nothing.
        """
        data = (await self._document("system_information"))["data"]
        system_id = _record_id(
            data.get("system_id") if isinstance(data, Mapping) else None
        )
        if system_id is None:
            raise FeedParseError("GBFS system_information document has no system_id")
        return GbfsSystemInfo(
            system_id=system_id,
            name=_localized(data.get("name")),
            short_name=_localized(data.get("short_name")),
            operator=_localized(data.get("operator")),
            timezone=_plain_text(data.get("timezone")),
            languages=tuple(_languages(data)),
            url=_plain_text(data.get("url")),
            purchase_url=_plain_text(data.get("purchase_url")),
            start_date=_iso_date(data.get("start_date")),
            phone_number=_plain_text(data.get("phone_number")),
            email=_plain_text(data.get("email")),
            feed_contact_email=_plain_text(data.get("feed_contact_email")),
            license_url=_plain_text(data.get("license_url")),
            terms_url=_localized(data.get("terms_url")) or None,
            privacy_url=_localized(data.get("privacy_url")) or None,
            opening_hours=_plain_text(data.get("opening_hours")),
        )

    async def _optional_rows(self, name: str, key: str) -> list[Mapping[str, Any]]:
        """Rows from a document a system need not publish at all.

        These endpoints are optional in GBFS (``vehicle_types`` only
        conditionally required -- without it every vehicle is a
        non-motorized bicycle), and discovery only lists what a system
        actually serves -- so "not published" is a normal answer meaning
        "no such records", not a failure. So is a listed one answering 404:
        "OPTIONAL files MAY 404. A 404 of an OPTIONAL file SHOULD NOT be
        considered an error." Any other error still raises.
        """
        endpoints = await self._endpoint_table()
        if name not in endpoints:
            return []
        try:
            return _rows(await self._document(name, endpoints), key)
        except SourceConnectionError as err:
            if err.status == HTTPStatus.NOT_FOUND:
                return []
            raise

    async def get_vehicle_types(self) -> list[GbfsVehicleType]:
        """Vehicle types, resolving the ids stations and vehicles reference."""
        return [
            GbfsVehicleType(
                id=type_id,
                form_factor=_plain_text(row.get("form_factor")),
                propulsion_type=_plain_text(row.get("propulsion_type")),
                name=_localized(row.get("name")),
                max_range_m=_number(row.get("max_range_meters")),
                rider_capacity=_count(row.get("rider_capacity")),
            )
            for row in await self._optional_rows("vehicle_types", "vehicle_types")
            if (type_id := _record_id(row.get("vehicle_type_id"))) is not None
        ]

    async def get_pricing_plans(self) -> list[GbfsPricingPlan]:
        """Pricing plans, resolving a vehicle's ``pricing_plan_id``."""
        return [
            GbfsPricingPlan(
                id=plan_id,
                name=_localized(row.get("name")),
                currency=_plain_text(row.get("currency")),
                price=_number(row.get("price")),
                is_taxable=_as_bool(row.get("is_taxable")),
                description=_localized(row.get("description")),
            )
            for row in await self._optional_rows("system_pricing_plans", "plans")
            if (plan_id := _record_id(row.get("plan_id"))) is not None
        ]

    async def get_regions(self) -> list[GbfsSystemRegion]:
        """Service regions, resolving a station's ``region_id``."""
        return [
            GbfsSystemRegion(id=region_id, name=_localized(row.get("name")))
            for row in await self._optional_rows("system_regions", "regions")
            if (region_id := _record_id(row.get("region_id"))) is not None
        ]

    async def get_alerts(self) -> list[GbfsAlert]:
        """GBFS system alerts.

        The same name as the transit handle's ``get_alerts``, scoped the
        GBFS way: to stations and regions, not routes and trips.
        """
        return [
            GbfsAlert(
                id=alert_id,
                type=_plain_text(row.get("type")),
                summary=_localized(row.get("summary")),
                description=_localized(row.get("description")),
                url=_localized(row.get("url")) or None,
                station_ids=tuple(_id_list(row.get("station_ids"))),
                region_ids=tuple(_id_list(row.get("region_ids"))),
                last_updated=_reported_at(row.get("last_updated")),
                active_periods=tuple(_alert_times(row.get("times"))),
            )
            for row in await self._optional_rows("system_alerts", "alerts")
            if (alert_id := _record_id(row.get("alert_id"))) is not None
        ]

    async def get_stations(self, zone: Circle | None = None) -> list[GbfsStation]:
        """Stations with information and status merged by station_id.

        With ``zone``, only stations inside the circle are returned (stations
        without coordinates are excluded when filtering) — this powers both
        the config-flow station multi-select and zone-scoped station sensors.
        Information rows without a usable ``station_id`` are skipped: the
        merge and the returned ``id`` both key on it. Systems that publish no
        ``station_information`` return ``[]``; an empty endpoint table (a
        non-GBFS payload) still raises.
        """
        # Dockless-only systems publish no stations; mirror get_vehicles()'s
        # [] for docked-only systems instead of failing.
        endpoints = await self._endpoint_table()
        if endpoints and "station_information" not in endpoints:
            return []
        info_rows = _rows(
            await self._document("station_information", endpoints), "stations"
        )
        status_rows = _rows(
            await self._document("station_status", endpoints), "stations"
        )
        status_by_id = {_record_id(row.get("station_id")): row for row in status_rows}
        stations: list[GbfsStation] = []
        for info in info_rows:
            latitude = _coordinate(info.get("lat"))
            longitude = _coordinate(info.get("lon"))
            if latitude is None or longitude is None:
                latitude = longitude = None  # half a position is no position
            if zone is not None and (
                latitude is None
                or longitude is None
                or not in_circle(zone, latitude, longitude)
            ):
                continue
            station_id = _record_id(info.get("station_id"))
            if station_id is None:
                continue
            status: Mapping[str, Any] = status_by_id.get(station_id, {})
            stations.append(
                GbfsStation(
                    id=station_id,
                    name=_localized(info.get("name")),
                    short_name=_localized(info.get("short_name")),
                    latitude=latitude,
                    longitude=longitude,
                    # Same _count guard as the disabled counts beside them:
                    # these are declared int | None, and a producer string
                    # would otherwise flow straight through the type.
                    capacity=_count(info.get("capacity")),
                    bikes_available=_count(
                        status.get(
                            "num_bikes_available",
                            status.get("num_vehicles_available"),
                        )
                    ),
                    docks_available=_count(status.get("num_docks_available")),
                    bikes_disabled=_count(
                        status.get(
                            "num_bikes_disabled",
                            status.get("num_vehicles_disabled"),
                        )
                    ),
                    docks_disabled=_count(status.get("num_docks_disabled")),
                    is_installed=_as_bool(status.get("is_installed")),
                    is_renting=_as_bool(status.get("is_renting")),
                    is_returning=_as_bool(status.get("is_returning")),
                    is_virtual_station=_as_bool(info.get("is_virtual_station")),
                    last_reported=_reported_at(status.get("last_reported")),
                    address=_plain_text(info.get("address")),
                    cross_street=_plain_text(info.get("cross_street")),
                    post_code=_plain_text(info.get("post_code")),
                    region_id=_record_id(info.get("region_id")),
                    vehicle_types_available=_vehicle_types(
                        status.get("vehicle_types_available")
                    ),
                    rental_uris=_rental_uris(info.get("rental_uris")),
                )
            )
        return stations

    async def _station_positions(
        self, endpoints: Mapping[str, str]
    ) -> dict[str, tuple[float | None, float | None]]:
        """Each station's published position, by id ({} without stations)."""
        if "station_information" not in endpoints:
            return {}
        rows = _rows(await self._document("station_information", endpoints), "stations")
        return {
            station_id: (_coordinate(row.get("lat")), _coordinate(row.get("lon")))
            for row in rows
            if (station_id := _record_id(row.get("station_id"))) is not None
        }

    async def get_vehicles(self, zone: Circle | None = None) -> list[GbfsVehicle]:
        """Vehicles not in active rental, optionally filtered to a circular zone.

        Uses ``vehicle_status`` (GBFS 3.x) when published, else falls back to
        ``free_bike_status`` (2.x). Returns [] for docked-only systems.
        Filtering is client-side: GBFS has no server-side geo-query.
        Rows without a usable id are skipped. A vehicle at a station need not
        report coordinates (``lat``/``lon`` are REQUIRED only without a
        ``station_id``): it is kept with None coordinates, and a ``zone``
        places it at its station (from ``station_information``). A row
        with neither is invalid and skipped (a free-floating vehicle IS its
        position).
        """
        endpoints = await self._endpoint_table()
        if "vehicle_status" in endpoints:
            rows = _rows(await self._document("vehicle_status", endpoints), "vehicles")
            id_key = "vehicle_id"
        elif "free_bike_status" in endpoints:
            rows = _rows(await self._document("free_bike_status", endpoints), "bikes")
            id_key = "bike_id"
        else:
            return []
        station_at = (
            await self._station_positions(endpoints)
            if zone is not None
            and any(
                _coordinate(row.get("lat")) is None
                or _coordinate(row.get("lon")) is None
                for row in rows
            )
            else {}
        )
        vehicles: list[GbfsVehicle] = []
        for row in rows:
            vehicle_id = _record_id(row.get(id_key))
            latitude = _coordinate(row.get("lat"))
            longitude = _coordinate(row.get("lon"))
            station_id = _record_id(row.get("station_id"))
            located = latitude is not None and longitude is not None
            if vehicle_id is None or not (located or station_id is not None):
                continue
            position = (
                (latitude, longitude)
                if located
                else station_at.get(station_id or "", (None, None))
            )
            if zone is not None and not (
                position[0] is not None
                and position[1] is not None
                and in_circle(zone, position[0], position[1])
            ):
                continue
            vehicles.append(
                GbfsVehicle(
                    id=vehicle_id,
                    # Half a position is no position.
                    latitude=latitude if located else None,
                    longitude=longitude if located else None,
                    is_reserved=_as_bool(row.get("is_reserved")),
                    is_disabled=_as_bool(row.get("is_disabled")),
                    # _record_id, matching _vehicle_types and
                    # get_vehicle_types: a producer using numeric type ids
                    # would otherwise yield int 7 here and "7" there, and
                    # the GbfsVehicleType join would never match.
                    vehicle_type_id=_record_id(row.get("vehicle_type_id")),
                    current_range_m=_number(row.get("current_range_meters")),
                    current_fuel_percent=_number(row.get("current_fuel_percent")),
                    last_reported=_reported_at(row.get("last_reported")),
                    station_id=station_id,
                    home_station_id=_record_id(row.get("home_station_id")),
                    pricing_plan_id=_record_id(row.get("pricing_plan_id")),
                    rental_uris=_rental_uris(row.get("rental_uris")),
                )
            )
        return vehicles
