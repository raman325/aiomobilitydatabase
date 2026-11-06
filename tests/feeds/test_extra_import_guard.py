"""Tests for the ``feeds`` extra: GBFS works without it, transit explains it."""

import builtins
import importlib
import sys
from collections.abc import Iterator
from types import ModuleType
from typing import Any

import pytest

import aiomobilitydatabase.feeds

EXTRA_HINT = r"pip install aiomobilitydatabase\[feeds\]"


@pytest.fixture
def feeds_without_extra(monkeypatch: pytest.MonkeyPatch) -> Iterator[ModuleType]:
    """Re-import ``aiomobilitydatabase.feeds`` as if protobuf were missing.

    Every ``aiomobilitydatabase.feeds*`` module is dropped from
    ``sys.modules`` so the import re-executes; monkeypatch restores the
    original modules, the parent package attribute and ``__import__``.
    """
    real_import = builtins.__import__

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name.split(".")[:2] in (["google", "transit"], ["google", "protobuf"]):
            raise ModuleNotFoundError(
                "simulated: gtfs-realtime-bindings not installed", name=name
            )
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(aiomobilitydatabase, "feeds", aiomobilitydatabase.feeds)
    for module in [m for m in sys.modules if m.startswith("aiomobilitydatabase.feeds")]:
        monkeypatch.delitem(sys.modules, module)
    monkeypatch.setattr(builtins, "__import__", fake_import)
    yield importlib.import_module("aiomobilitydatabase.feeds")


def test_gbfs_imports_without_extra(feeds_without_extra: ModuleType) -> None:
    assert feeds_without_extra.GbfsFeedHandle.__name__ == "GbfsFeedHandle"
    assert feeds_without_extra.MobilityFeedsClient.__name__ == "MobilityFeedsClient"


def test_transit_handle_without_extra_raises(feeds_without_extra: ModuleType) -> None:
    with pytest.raises(ImportError, match=EXTRA_HINT):
        feeds_without_extra.TransitFeedHandle  # noqa: B018


def test_unrelated_import_error_propagates_unchanged(
    feeds_without_extra: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture_import = builtins.__import__

    def failing_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "aiohttp":
            raise ImportError("unrelated breakage", name="somethingelse")
        return fixture_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", failing_import)
    with pytest.raises(ImportError, match="unrelated breakage") as excinfo:
        feeds_without_extra.TransitFeedHandle  # noqa: B018
    assert excinfo.value.name == "somethingelse"
    assert "feeds" not in str(excinfo.value)


@pytest.mark.parametrize(
    ("method", "args"),
    [
        pytest.param("get_transit_feed", ("mdb-1",), id="catalog"),
        pytest.param(
            "get_transit_feed_from_urls", ("https://example.com/gtfs.zip",), id="urls"
        ),
    ],
)
async def test_transit_methods_without_extra_raise(
    feeds_without_extra: ModuleType, method: str, args: tuple[str, ...]
) -> None:
    client = feeds_without_extra.MobilityFeedsClient()
    try:
        with pytest.raises(ImportError, match=EXTRA_HINT):
            await getattr(client, method)(*args)
    finally:
        await client.close()


def test_unknown_attribute_raises_attribute_error() -> None:
    with pytest.raises(AttributeError, match="NoSuchThing"):
        aiomobilitydatabase.feeds.NoSuchThing  # noqa: B018


def test_transit_handle_resolves_to_transit_module_with_extra() -> None:
    assert (
        aiomobilitydatabase.feeds.TransitFeedHandle
        is aiomobilitydatabase.feeds.transit.TransitFeedHandle
    )
