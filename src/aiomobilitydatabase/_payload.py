"""Check a catalog API payload's JSON types against a model's annotations.

mashumaro converts a simple field with its type's constructor rather than
checking it: ``bool("false")`` is True, ``str(None)`` is ``"None"``,
``int(2.9)`` is 2, and a tuple field iterates whatever it is given -- a
string into its characters. Each turns a malformed response into wrong
data. So the raw JSON is checked first, against the same annotations
mashumaro reads, and a mismatch is an invalid payload.
"""

from __future__ import annotations

import dataclasses
import math
import types
import typing
from datetime import datetime
from enum import Enum
from functools import cache
from typing import Any


@cache
def _hints(model: type) -> dict[str, Any]:
    """Return a dataclass's resolved field annotations (postponed in models)."""
    return typing.get_type_hints(model)


def mismatch(annotation: Any, value: Any, where: str = "$") -> str | None:
    """Return where ``value`` breaks ``annotation``'s JSON type, or None.

    JSON types per annotation: ``str`` a string; ``int`` an integer, never
    a boolean (JSON's ``true`` isn't 1); ``float`` a finite number (a
    ``1e400`` literal decodes to infinity); ``bool`` a boolean; a
    ``datetime`` or a string enum a string, whose content mashumaro then
    parses or refuses; ``tuple[X, ...]`` an array of X; a dataclass an
    object whose present fields conform (a missing required field is
    mashumaro's to report; keys the model doesn't know are ignored, as the
    API may add fields); ``X | None`` null or an X.
    """
    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        return _union_mismatch(typing.get_args(annotation), value, where)
    if origin is tuple:
        return _array_mismatch(typing.get_args(annotation)[0], value, where)
    if dataclasses.is_dataclass(annotation):
        return _object_mismatch(annotation, value, where)  # type: ignore[arg-type]
    if _scalar_fits(annotation, value):
        return None
    return f"{where} is not a {getattr(annotation, '__name__', annotation)}"


def _union_mismatch(options: tuple[Any, ...], value: Any, where: str) -> str | None:
    if value is None and type(None) in options:
        return None
    found = [
        mismatch(option, value, where) for option in options if option is not type(None)
    ]
    return None if None in found else found[0]


def _array_mismatch(item: Any, value: Any, where: str) -> str | None:
    if not isinstance(value, list):
        return f"{where} is not an array"
    for index, element in enumerate(value):
        if (found := mismatch(item, element, f"{where}[{index}]")) is not None:
            return found
    return None


def _object_mismatch(model: type, value: Any, where: str) -> str | None:
    if not isinstance(value, dict):
        return f"{where} is not an object"
    hints = _hints(model)
    for field in dataclasses.fields(model):
        if field.name in value:
            found = mismatch(
                hints[field.name], value[field.name], f"{where}.{field.name}"
            )
            if found is not None:
                return found
    return None


def _scalar_fits(annotation: Any, value: Any) -> bool:
    if annotation is bool:
        return isinstance(value, bool)
    if isinstance(value, bool):
        return False  # JSON's true and false are no numbers, nor strings
    if annotation is int:
        return isinstance(value, int)
    if annotation is float:
        return isinstance(value, int | float) and _finite(value)
    if annotation is str or annotation is datetime or issubclass(annotation, Enum):
        return isinstance(value, str)
    raise TypeError(f"no JSON type for {annotation!r}")  # pragma: no cover


def _finite(number: float) -> bool:
    """Return whether a JSON number is a finite float.

    An integer too big for one isn't: mashumaro's ``float()`` would raise
    OverflowError.
    """
    try:
        return math.isfinite(float(number))
    except OverflowError:
        return False
