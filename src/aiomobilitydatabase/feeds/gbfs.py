"""GbfsFeedHandle: station and vehicle snapshots for a GBFS system."""

from __future__ import annotations

import math
import time
from collections.abc import Mapping
from datetime import UTC, date, datetime
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import aiohttp

from .const import GBFS_LANGUAGE_PREFERENCE
from .exceptions import FeedParseError, SourceConnectionError
from .geo import Circle, in_circle
from .models import (
    GbfsAlert,
    GbfsVehicle,
    PricingPlan,
    Station,
    SystemInfo,
    SystemRegion,
    VehicleType,
)
from .rt import _require_http_url  # deliberate friend access: shared URL guard

if TYPE_CHECKING:
    from ..models import GbfsFeed
    from .client import MobilityFeedsClient


def _entry_text(entry: dict[str, Any]) -> str | None:
    """One localized entry's ``text``: an absent/null text is None.

    ``str(entry.get("text"))`` would render a missing text as the literal
    string ``"None"`` — the selected entry must instead degrade to "no
    text available".
    """
    text = entry.get("text")
    return None if text is None else str(text)


def _record_id(value: Any) -> str | None:
    """One record's own id as a string, or None when it has none.

    ``str(value)`` would synthesize the literal id ``"None"`` for an
    id-less record — and those synthetic ids COLLIDE across records,
    corrupting any consumer that keys on them (an entity registry, say).
    A blank id collides identically, so it is None too. Same motivation
    as :func:`_entry_text`: never surface a stringified None.
    """
    if value is None:
        return None
    return str(value) or None


def _localized(value: Any) -> str | None:
    """Normalize GBFS text: 3.x localized lists vs 2.x plain strings."""
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, list):
        for entry in value:
            if (
                isinstance(entry, dict)
                and entry.get("language") == GBFS_LANGUAGE_PREFERENCE
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


def _rental_uris(value: Any) -> dict[str, str] | None:
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
    return uris or None


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


def _vehicle_types(value: Any) -> dict[str, int] | None:
    """Normalize a station's ``vehicle_types_available`` to id -> count.

    Entries without a usable type id, and counts that aren't parseable as
    an int (``int(None)`` used to raise), are dropped rather than
    coerced; an absent count is the spec's 0. Anything that isn't a list
    with at least one usable entry is None, i.e. "not published". A count
    of ``1e999`` parses as inf, which int() refuses, so it is dropped too.
    """
    if not isinstance(value, list):
        return None
    types: dict[str, int] = {}
    for entry in value:
        if not isinstance(entry, Mapping):
            continue
        type_id = _record_id(entry.get("vehicle_type_id"))
        try:
            count = int(entry.get("count", 0))
        except (OverflowError, TypeError, ValueError):
            continue
        if type_id is not None:
            types[type_id] = count
    return types or None


def _reported_at(value: Any) -> datetime | None:
    """Parse a GBFS ``last_reported``, which changed type across versions.

    2.x ships POSIX seconds, 3.0 ships an RFC3339 string. Anything else --
    including a bool, which is an int in Python -- degrades to None like
    every other malformed scalar at this boundary.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        try:
            return datetime.fromtimestamp(value, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        # A naive RFC3339 value is UTC by GBFS's own definition.
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return None


def _iso_date(value: Any) -> date | None:
    """Parse a GBFS ``YYYY-MM-DD`` cell, degrading to None on anything else."""
    if not isinstance(value, str):
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
    """Return a non-negative count; bools and junk degrade to None."""
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


def _alert_times(value: Any) -> list[tuple[datetime | None, datetime | None]]:
    """Return a GBFS alert's ``times`` array as (start, end) pairs."""
    if not isinstance(value, list):
        return []
    return [
        (_reported_at(item.get("start")), _reported_at(item.get("end")))
        for item in value
        if isinstance(item, Mapping)
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


def _ttl_seconds(value: Any) -> float:
    """Micro-cache lifetime from a document's ``ttl``, leniently.

    A ttl that isn't a usable, finite, non-negative number degrades to 0
    (no caching) like every other malformed scalar at this boundary: a
    bad cache HINT must not fail a document that otherwise parsed. Plain
    numeric strings are accepted because producers do ship them, and an
    integer literal too large for a float degrades like any other
    unusable ttl.
    """
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        return 0.0
    try:
        ttl = float(value)
    except (OverflowError, ValueError):
        return 0.0
    return ttl if math.isfinite(ttl) and ttl > 0 else 0.0


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
            preferred = data.get(GBFS_LANGUAGE_PREFERENCE)
            if isinstance(preferred, Mapping) and isinstance(
                preferred.get("feeds"), list
            ):
                candidates.insert(0, preferred)
            if candidates:
                feeds = candidates[0]["feeds"]
    endpoints = {
        str(feed["name"]): str(feed["url"])
        for feed in (feeds if isinstance(feeds, list) else [])
        if isinstance(feed, Mapping) and feed.get("name") and feed.get("url")
    }
    if not endpoints:
        raise FeedParseError("GBFS discovery document lists no usable feeds")
    return endpoints


class GbfsFeedHandle:
    """Snapshot access to one GBFS system.

    Endpoints resolve either from catalog metadata (:meth:`create`) or
    straight from the system's own auto-discovery document
    (:meth:`create_from_url`); every snapshot method behaves identically
    on both.
    """

    def __init__(
        self,
        client: MobilityFeedsClient,
        feed: GbfsFeed | None,
        endpoints: dict[str, str],
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Initialize; internal — use MobilityFeedsClient.get_gbfs_feed()."""
        self._client = client
        self._feed = feed
        self._endpoints = endpoints
        self._headers = headers
        self._doc_cache: dict[str, tuple[float, float, dict[str, Any]]] = {}

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
        document = await cls._fetch_json_document(
            client, discovery_url, headers, "GBFS discovery URL"
        )
        return cls(client, None, _endpoints_from_discovery(document), headers=headers)

    @staticmethod
    async def _fetch_json_document(
        client: MobilityFeedsClient,
        url: str,
        headers: Mapping[str, str] | None,
        context: str,
    ) -> dict[str, Any]:
        """GET one GBFS JSON document and validate its data envelope."""
        _require_http_url(url, context)
        session = client._get_session()  # deliberate friend access
        try:
            async with session.get(
                url,
                headers=dict(headers) if headers else None,
                timeout=aiohttp.ClientTimeout(total=client.timeout_seconds),
            ) as resp:
                if resp.status >= HTTPStatus.BAD_REQUEST:
                    raise SourceConnectionError(
                        f"GBFS endpoint error {resp.status}: {url}", status=resp.status
                    )
                try:
                    document = await resp.json(content_type=None)
                except ValueError as err:
                    raise FeedParseError(f"Malformed GBFS JSON from {url}") from err
        except (TimeoutError, aiohttp.ClientError) as err:
            raise SourceConnectionError(f"Error fetching {url}: {err}") from err
        if not isinstance(document, dict) or "data" not in document:
            raise FeedParseError(f"GBFS document missing data envelope: {url}")
        return document

    async def _document(self, name: str) -> dict[str, Any]:
        cached = self._doc_cache.get(name)
        if cached is not None:
            fetched_at, ttl, data = cached
            if time.monotonic() - fetched_at < ttl:
                return data
        url = self._endpoints.get(name)
        if url is None:
            raise SourceConnectionError(f"GBFS endpoint not published: {name}")
        document = await self._fetch_json_document(
            self._client, url, self._headers, f"GBFS {name} endpoint URL"
        )
        ttl = _ttl_seconds(document.get("ttl"))
        self._doc_cache[name] = (time.monotonic(), ttl, document)
        return document

    async def get_system_info(self) -> SystemInfo:
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
        return SystemInfo(
            system_id=system_id,
            name=_localized(data.get("name")),
            short_name=_localized(data.get("short_name")),
            operator=_localized(data.get("operator")),
            timezone=_plain_text(data.get("timezone")),
            languages=_languages(data),
            url=_plain_text(data.get("url")),
            purchase_url=_plain_text(data.get("purchase_url")),
            start_date=_iso_date(data.get("start_date")),
            phone_number=_plain_text(data.get("phone_number")),
            email=_plain_text(data.get("email")),
            feed_contact_email=_plain_text(data.get("feed_contact_email")),
            license_url=_plain_text(data.get("license_url")),
            terms_url=_plain_text(data.get("terms_url")),
            privacy_url=_plain_text(data.get("privacy_url")),
            opening_hours=_plain_text(data.get("opening_hours")),
        )

    async def _optional_rows(self, name: str, key: str) -> list[Mapping[str, Any]]:
        """Rows from a document a system need not publish at all.

        Every one of these endpoints is optional in GBFS, and discovery
        only lists what a system actually serves -- so "not published"
        is a normal answer meaning "no such records", not a failure.
        """
        if name not in self._endpoints:
            return []
        return _rows(await self._document(name), key)

    async def get_vehicle_types(self) -> list[VehicleType]:
        """Vehicle types, resolving the ids stations and vehicles reference."""
        return [
            VehicleType(
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

    async def get_pricing_plans(self) -> list[PricingPlan]:
        """Pricing plans, resolving a vehicle's ``pricing_plan_id``."""
        return [
            PricingPlan(
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

    async def get_regions(self) -> list[SystemRegion]:
        """Service regions, resolving a station's ``region_id``."""
        return [
            SystemRegion(id=region_id, name=_localized(row.get("name")))
            for row in await self._optional_rows("system_regions", "regions")
            if (region_id := _record_id(row.get("region_id"))) is not None
        ]

    async def get_system_alerts(self) -> list[GbfsAlert]:
        """GBFS system alerts.

        Named apart from the GTFS-RT ``get_alerts`` on the transit handle:
        these scope to GBFS stations and regions, not routes and trips.
        """
        return [
            GbfsAlert(
                id=alert_id,
                type=_plain_text(row.get("type")),
                summary=_localized(row.get("summary")),
                description=_localized(row.get("description")),
                url=_plain_text(row.get("url")),
                station_ids=_id_list(row.get("station_ids")),
                region_ids=_id_list(row.get("region_ids")),
                last_updated=_reported_at(row.get("last_updated")),
                active_periods=_alert_times(row.get("times")),
            )
            for row in await self._optional_rows("system_alerts", "alerts")
            if (alert_id := _record_id(row.get("alert_id"))) is not None
        ]

    async def get_stations(self, zone: Circle | None = None) -> list[Station]:
        """Stations with information and status merged by station_id.

        With ``zone``, only stations inside the circle are returned (stations
        without coordinates are excluded when filtering) — this powers both
        the config-flow station multi-select and zone-scoped station sensors.
        Information rows without a usable ``station_id`` are skipped: the
        merge and the returned ``id`` both key on it.
        """
        info_rows = _rows(await self._document("station_information"), "stations")
        status_rows = _rows(await self._document("station_status"), "stations")
        status_by_id = {_record_id(row.get("station_id")): row for row in status_rows}
        stations: list[Station] = []
        for info in info_rows:
            latitude = _coordinate(info.get("lat"))
            longitude = _coordinate(info.get("lon"))
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
                Station(
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

    async def get_vehicles(self, zone: Circle | None = None) -> list[GbfsVehicle]:
        """Free-floating vehicles, optionally filtered to a circular zone.

        Uses ``vehicle_status`` (GBFS 3.x) when published, else falls back to
        ``free_bike_status`` (2.x). Returns [] for docked-only systems.
        Filtering is client-side: GBFS has no server-side geo-query.
        Rows without a usable id are skipped, as are rows without
        coordinates (a free-floating vehicle IS its position).
        """
        if "vehicle_status" in self._endpoints:
            rows = _rows(await self._document("vehicle_status"), "vehicles")
            id_key = "vehicle_id"
        elif "free_bike_status" in self._endpoints:
            rows = _rows(await self._document("free_bike_status"), "bikes")
            id_key = "bike_id"
        else:
            return []
        vehicles: list[GbfsVehicle] = []
        for row in rows:
            vehicle_id = _record_id(row.get(id_key))
            latitude = _coordinate(row.get("lat"))
            longitude = _coordinate(row.get("lon"))
            if vehicle_id is None or latitude is None or longitude is None:
                continue
            if zone is not None and not in_circle(zone, latitude, longitude):
                continue
            vehicles.append(
                GbfsVehicle(
                    id=vehicle_id,
                    latitude=latitude,
                    longitude=longitude,
                    is_reserved=_as_bool(row.get("is_reserved")),
                    is_disabled=_as_bool(row.get("is_disabled")),
                    # _record_id, matching _vehicle_types and
                    # get_vehicle_types: a producer using numeric type ids
                    # would otherwise yield int 7 here and "7" there, and
                    # the VehicleType join would never match.
                    vehicle_type_id=_record_id(row.get("vehicle_type_id")),
                    current_range_m=_number(row.get("current_range_meters")),
                    current_fuel_percent=_number(row.get("current_fuel_percent")),
                    last_reported=_reported_at(row.get("last_reported")),
                    station_id=_record_id(row.get("station_id")),
                    home_station_id=_record_id(row.get("home_station_id")),
                    pricing_plan_id=_record_id(row.get("pricing_plan_id")),
                    rental_uris=_rental_uris(row.get("rental_uris")),
                )
            )
        return vehicles
