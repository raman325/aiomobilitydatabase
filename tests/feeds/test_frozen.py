"""FrozenDict: read-only, hashable, and still a dict to serializers."""

import copy
import dataclasses
import json
import pickle
from collections.abc import Callable

import pytest

from aiomobilitydatabase.feeds._frozen import FrozenDict


def _mutations() -> list[tuple[str, Callable[[FrozenDict[str, int]], object]]]:
    def ior(fd: FrozenDict[str, int]) -> None:
        fd |= {"b": 2}

    return [
        ("setitem", lambda fd: fd.__setitem__("b", 2)),
        ("delitem", lambda fd: fd.__delitem__("a")),
        ("ior", ior),
        ("clear", lambda fd: fd.clear()),
        ("pop", lambda fd: fd.pop("a")),
        ("popitem", lambda fd: fd.popitem()),
        ("setdefault", lambda fd: fd.setdefault("b", 2)),
        ("update", lambda fd: fd.update(b=2)),
    ]


@pytest.mark.parametrize(
    "mutate", [m for _, m in _mutations()], ids=[n for n, _ in _mutations()]
)
def test_every_mutator_refuses(
    mutate: Callable[[FrozenDict[str, int]], object],
) -> None:
    fd = FrozenDict({"a": 1})
    with pytest.raises(TypeError, match="read-only"):
        mutate(fd)
    assert fd == {"a": 1}


def test_no_instance_attributes() -> None:
    with pytest.raises(AttributeError):
        FrozenDict({"a": 1}).extra = 1  # type: ignore[attr-defined]


@pytest.mark.parametrize("protocol", range(pickle.HIGHEST_PROTOCOL + 1))
def test_round_trips_and_hashes_like_a_value(protocol: int) -> None:
    fd = FrozenDict({"a": 1, "b": 2})
    restored = pickle.loads(pickle.dumps(fd, protocol=protocol))
    assert type(restored) is FrozenDict
    assert restored == fd
    assert hash(restored) == hash(FrozenDict({"b": 2, "a": 1}))
    assert copy.copy(fd) == copy.deepcopy(fd) == fd


def test_serializes_as_a_plain_object() -> None:
    @dataclasses.dataclass(frozen=True)
    class Holder:
        mapping: FrozenDict[str, int]

    holder = Holder(FrozenDict({"a": 1}))
    assert json.loads(json.dumps(dataclasses.asdict(holder))) == {"mapping": {"a": 1}}
    assert hash(holder) == hash(Holder(FrozenDict({"a": 1})))
