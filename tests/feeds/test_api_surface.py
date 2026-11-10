"""Public feeds API invariants: naming rule and read-only snapshot data.

Rule: a public async *query* method is named ``get_*``. Synchronous methods
(in-memory pickers) are not. Lifecycle actions are exempt. Enforced by
reflection so a later change can't add an off-rule method unnoticed.
"""

import dataclasses
import inspect
import re

import pytest

from aiomobilitydatabase import feeds
from aiomobilitydatabase import models as catalog_models
from aiomobilitydatabase.feeds import ArrivalsQuery, GbfsFeedHandle, MobilityFeedsClient
from aiomobilitydatabase.feeds import models as feed_models
from aiomobilitydatabase.feeds.transit import TransitFeedHandle

# Async methods that perform an action rather than answer a query.
_ACTIONS = {
    "close",
    "create",
    "create_from_url",
    "create_from_urls",
    "purge_cache",
    "refresh_static",
}


def _public_methods(cls: type) -> dict[str, object]:
    return {
        name: member
        for name, member in vars(cls).items()
        if not name.startswith("_")
        and (inspect.isfunction(member) or isinstance(member, classmethod))
    }


@pytest.mark.parametrize(
    "cls", [TransitFeedHandle, GbfsFeedHandle, MobilityFeedsClient]
)
def test_async_queries_are_get_prefixed_and_sync_methods_are_not(cls: type) -> None:
    for name, member in _public_methods(cls).items():
        func = member.__func__ if isinstance(member, classmethod) else member
        if name in _ACTIONS:
            continue
        if inspect.iscoroutinefunction(func):
            assert name.startswith("get_"), f"{cls.__name__}.{name} is async"
        else:
            assert not name.startswith("get_"), f"{cls.__name__}.{name} is sync"


@pytest.mark.parametrize(
    "old_name",
    [
        "upcoming_trips",
        "scheduled_departures",
        "services_on",
        "routes_serving",
        "headsigns_serving",
    ],
)
def test_old_transit_names_are_gone(old_name: str) -> None:
    assert not hasattr(TransitFeedHandle, old_name)


def test_gbfs_alerts_method_matches_transit() -> None:
    assert hasattr(GbfsFeedHandle, "get_alerts")
    assert not hasattr(GbfsFeedHandle, "get_system_alerts")


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("Station", "GbfsStation"),
        ("VehicleType", "GbfsVehicleType"),
        ("PricingPlan", "GbfsPricingPlan"),
        ("SystemRegion", "GbfsSystemRegion"),
        ("SystemInfo", "GbfsSystemInfo"),
    ],
)
def test_gbfs_models_are_prefixed(old: str, new: str) -> None:
    assert new in feeds.__all__
    assert old not in feeds.__all__
    assert getattr(feeds, new).__name__ == new


@pytest.mark.parametrize(
    "name", ["stops", "routes", "agencies", "feed_info", "rt_feeds"]
)
def test_transit_snapshot_data_is_read_only(name: str) -> None:
    attr = inspect.getattr_static(TransitFeedHandle, name)
    assert isinstance(attr, property), f"{name} must be a property"
    assert attr.fset is None, f"{name} must not be settable"


def _public_models(module: object) -> list[type]:
    return [
        obj
        for name, obj in vars(module).items()
        if not name.startswith("_")
        and isinstance(obj, type)
        and dataclasses.is_dataclass(obj)
        and obj.__module__ == getattr(module, "__name__", None)
    ]


_MUTABLE_CONTAINER = re.compile(r"\b(list|dict|set)\[")


@pytest.mark.parametrize(
    "model",
    [*_public_models(catalog_models), *_public_models(feed_models)],
    ids=lambda model: f"{model.__module__.rsplit('.', 2)[-2]}.{model.__name__}",
)
def test_public_models_are_deeply_immutable(model: type) -> None:
    params = model.__dataclass_params__  # type: ignore[attr-defined]
    assert params.frozen, f"{model.__name__} must be frozen"
    for field in dataclasses.fields(model):
        annotation = str(field.type)
        assert not _MUTABLE_CONTAINER.search(annotation), (
            f"{model.__name__}.{field.name}: {annotation} is a mutable container; "
            "use tuple[...] or Mapping[...]"
        )


def test_arrivals_query_copies_caller_sequences() -> None:
    stop_ids = ["S1"]
    route_ids = ["R1"]
    query = ArrivalsQuery(stop_ids, route_ids=route_ids, headsigns=["Downtown"])
    stop_ids.append("S2")
    route_ids.append("R2")
    assert query.stop_ids == ("S1",)
    assert query.route_ids == ("R1",)
    assert query.headsigns == ("Downtown",)


def test_client_cache_dir_is_read_only() -> None:
    attr = inspect.getattr_static(MobilityFeedsClient, "cache_dir")
    assert isinstance(attr, property)
    assert attr.fset is None


@pytest.mark.parametrize("name", ["stops", "routes", "agencies", "rt_feeds"])
def test_transit_snapshot_sequences_are_tuples(name: str) -> None:
    prop = inspect.getattr_static(TransitFeedHandle, name)
    assert "tuple[" in str(inspect.signature(prop.fget).return_annotation)
