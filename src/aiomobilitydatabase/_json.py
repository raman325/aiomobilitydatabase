"""Strict JSON text from bytes, shared by the catalog client and the feeds."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from ._http import is_text


def loads(
    raw: bytes,
    *,
    parse_int: Callable[[str], Any] | None = None,
    lone_surrogates: bool = False,
) -> Any:
    r"""Parse JSON text, or raise ValueError when the bytes are none.

    RFC 8259: JSON exchanged between systems is UTF-8 (section 8.1), so the
    bytes are decoded as strict UTF-8 whatever a response's charset says (a
    charset parameter has no effect on a compliant recipient, section 11);
    a leading BOM is ignored, as section 8.1 allows. ``json.loads`` on the
    bytes themselves would also take UTF-16/32 and, by surrogatepass,
    encoded surrogates. A name repeated in an object (section 4: the result
    is unpredictable) and ``NaN``/``Infinity`` (no JSON) are refused, and so
    is nesting deeper than the interpreter's stack -- and a string (a name or
    a value) holding a lone surrogate, which an escape can spell
    (``"\ud800"``; section 8.2 leaves its meaning unpredictable): no text,
    it can't even be sent back. With ``lone_surrogates`` such strings are
    kept, for a caller that judges each value itself (a GeoJSON Feature
    with one is skipped, not the whole file). ``parse_int`` is
    ``json.loads``'s.
    """
    try:
        value = json.loads(
            raw.decode("utf-8-sig"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
            parse_int=parse_int,
        )
    except RecursionError:
        failure = "JSON nests deeper than can be parsed"
    else:
        if not lone_surrogates:
            _require_text(value)
        return value
    raise ValueError(failure)  # outside the except: nothing chained


def _require_text(value: Any) -> None:
    """Raise ValueError for a lone surrogate in any string, at any depth.

    Walked with a stack, not recursion: a document may nest as deep as the
    parser allowed.
    """
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, str):
            if not is_text(item):
                raise ValueError("a JSON string holds a lone surrogate")
        elif isinstance(item, dict):
            stack.extend(item.keys())
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Build a JSON object, refusing a name that appears twice."""
    obj = dict(pairs)
    if len(obj) != len(pairs):
        raise ValueError("a JSON object repeats a name")
    return obj


def _reject_constant(name: str) -> None:
    """Refuse ``NaN``/``Infinity``: Python's json accepts them, JSON does not."""
    raise ValueError(f"{name} is not valid JSON")
