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
        if name == "google.transit" or name.startswith("google.transit."):
            raise ImportError("simulated: gtfs-realtime-bindings not installed")
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
    with pytest.raises(ImportError, match=EXTRA_HINT):
        await getattr(client, method)(*args)
    await client.close()


def test_unknown_attribute_raises_attribute_error() -> None:
    with pytest.raises(AttributeError, match="NoSuchThing"):
        aiomobilitydatabase.feeds.NoSuchThing  # noqa: B018
