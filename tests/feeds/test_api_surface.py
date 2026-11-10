"""Public feeds API invariants: naming rule and read-only snapshot data.

Rule: a public async *query* method is named ``get_*``. Synchronous methods
(in-memory pickers) are not. Lifecycle actions are exempt. Enforced by
reflection so a later change can't add an off-rule method unnoticed.
"""

import inspect

import pytest

from aiomobilitydatabase import feeds
from aiomobilitydatabase.feeds import GbfsFeedHandle, MobilityFeedsClient
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
