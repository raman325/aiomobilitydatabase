"""Tests for the ``feeds`` extra's import guard (missing gtfs-realtime-bindings)."""

import builtins
import importlib
import sys
from collections.abc import Iterator
from typing import Any

import pytest


@pytest.fixture
def _simulate_missing_gtfs_realtime_bindings(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[None]:
    """Make ``import google.transit`` fail, as it would without the extra.

    ``aiomobilitydatabase.feeds`` is removed from ``sys.modules`` so the next
    import re-executes its top-level guard; monkeypatch restores both the
    real ``__import__`` and the original module entry on teardown.
    """
    real_import = builtins.__import__

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "google.transit" or name.startswith("google.transit."):
            raise ImportError("simulated: gtfs-realtime-bindings not installed")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    monkeypatch.delitem(sys.modules, "aiomobilitydatabase.feeds", raising=False)
    yield


def test_feeds_import_without_extra_raises_actionable_error(
    _simulate_missing_gtfs_realtime_bindings: None,
) -> None:
    with pytest.raises(ImportError, match=r"pip install aiomobilitydatabase\[feeds\]"):
        importlib.import_module("aiomobilitydatabase.feeds")
