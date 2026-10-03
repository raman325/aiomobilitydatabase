"""GbfsFeedHandle: station and vehicle snapshots for a GBFS system."""

from __future__ import annotations

import time
from collections.abc import Mapping
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import aiohttp

from .const import GBFS_LANGUAGE_PREFERENCE
from .exceptions import FeedParseError, SourceConnectionError
from .geo import Circle, in_circle
from .models import GbfsVehicle, Station, SystemInfo
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


def _as_bool(value: Any) -> bool | None:
    """Coerce a GBFS status flag to bool without lying on ambiguous input.

    ``bool("false")`` is ``True`` in Python, so strings (and anything else
    that isn't already a bool/int/float) are treated as UNKNOWN (``None``)
    rather than silently coerced.
    """
    if isinstance(value, bool | int | float):
        return bool(value)
    return None


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
        ttl = float(document.get("ttl") or 0)
        self._doc_cache[name] = (time.monotonic(), ttl, document)
        return document

    async def get_system_info(self) -> SystemInfo:
        """GBFS system information."""
        data = (await self._document("system_information"))["data"]
        return SystemInfo(
            system_id=str(data.get("system_id")),
            name=_localized(data.get("name")),
            operator=_localized(data.get("operator")),
            timezone=data.get("timezone"),
        )

    async def get_stations(self, zone: Circle | None = None) -> list[Station]:
        """Stations with information and status merged by station_id.

        With ``zone``, only stations inside the circle are returned (stations
        without coordinates are excluded when filtering) — this powers both
        the config-flow station multi-select and zone-scoped station sensors.
        """
        info_rows = (await self._document("station_information"))["data"].get(
            "stations", []
        )
        status_rows = (await self._document("station_status"))["data"].get(
            "stations", []
        )
        status_by_id = {row.get("station_id"): row for row in status_rows}
        stations: list[Station] = []
        for info in info_rows:
            if zone is not None:
                lat, lon = info.get("lat"), info.get("lon")
                if lat is None or lon is None or not in_circle(zone, lat, lon):
                    continue
            station_id = info.get("station_id")
            status = status_by_id.get(station_id, {})
            types_list = status.get("vehicle_types_available")
            types = (
                {
                    str(entry.get("vehicle_type_id")): int(entry.get("count", 0))
                    for entry in types_list
                }
                if types_list
                else None
            )
            stations.append(
                Station(
                    id=str(station_id),
                    name=_localized(info.get("name")),
                    latitude=info.get("lat"),
                    longitude=info.get("lon"),
                    capacity=info.get("capacity"),
                    bikes_available=status.get(
                        "num_bikes_available", status.get("num_vehicles_available")
                    ),
                    docks_available=status.get("num_docks_available"),
                    is_renting=_as_bool(status.get("is_renting")),
                    is_returning=_as_bool(status.get("is_returning")),
                    vehicle_types_available=types,
                    rental_uris=_rental_uris(info.get("rental_uris")),
                )
            )
        return stations

    async def get_vehicles(self, zone: Circle | None = None) -> list[GbfsVehicle]:
        """Free-floating vehicles, optionally filtered to a circular zone.

        Uses ``vehicle_status`` (GBFS 3.x) when published, else falls back to
        ``free_bike_status`` (2.x). Returns [] for docked-only systems.
        Filtering is client-side: GBFS has no server-side geo-query.
        """
        if "vehicle_status" in self._endpoints:
            rows = (await self._document("vehicle_status"))["data"].get("vehicles", [])
            id_key = "vehicle_id"
        elif "free_bike_status" in self._endpoints:
            rows = (await self._document("free_bike_status"))["data"].get("bikes", [])
            id_key = "bike_id"
        else:
            return []
        vehicles: list[GbfsVehicle] = []
        for row in rows:
            latitude, longitude = row.get("lat"), row.get("lon")
            if latitude is None or longitude is None:
                continue
            if zone is not None and not in_circle(zone, latitude, longitude):
                continue
            vehicles.append(
                GbfsVehicle(
                    id=str(row.get(id_key)),
                    latitude=latitude,
                    longitude=longitude,
                    is_reserved=row.get("is_reserved"),
                    is_disabled=row.get("is_disabled"),
                    vehicle_type_id=row.get("vehicle_type_id"),
                    current_range_m=row.get("current_range_meters"),
                    rental_uris=_rental_uris(row.get("rental_uris")),
                )
            )
        return vehicles
