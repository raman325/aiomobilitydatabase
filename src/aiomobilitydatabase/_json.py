"""Strict JSON text from bytes, shared by the catalog client and the feeds."""

from __future__ import annotations

import json
from typing import Any


def loads(raw: bytes) -> Any:
    """Parse JSON text, or raise ValueError when the bytes are none.

    RFC 8259: JSON exchanged between systems is UTF-8 (section 8.1), so the
    bytes are decoded as strict UTF-8 whatever a response's charset says (a
    charset parameter has no effect on a compliant recipient, section 11);
    a leading BOM is ignored, as section 8.1 allows. ``json.loads`` on the
    bytes themselves would also take UTF-16/32 and, by surrogatepass,
    encoded surrogates. A name repeated in an object (section 4: the result
    is unpredictable) and ``NaN``/``Infinity`` (no JSON) are refused, and so
    is nesting deeper than the interpreter's stack.
    """
    try:
        return json.loads(
            raw.decode("utf-8-sig"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except RecursionError as err:
        raise ValueError("JSON nests deeper than can be parsed") from err


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Build a JSON object, refusing a name that appears twice."""
    obj = dict(pairs)
    if len(obj) != len(pairs):
        raise ValueError("a JSON object repeats a name")
    return obj


def _reject_constant(name: str) -> None:
    """Refuse ``NaN``/``Infinity``: Python's json accepts them, JSON does not."""
    raise ValueError(f"{name} is not valid JSON")
