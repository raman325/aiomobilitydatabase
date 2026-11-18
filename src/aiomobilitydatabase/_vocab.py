"""Spec enumerations at the model boundary: unknown values become None.

Shared by the GTFS-RT and GBFS layers and the public catalog (protobuf-free,
so GBFS and the catalog keep working without the ``feeds`` extra), as the
static side's ``_enum_or_none`` is for GTFS's integer vocabularies.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any


def vocab_or_none[StrEnumT: StrEnum](
    enum_cls: type[StrEnumT], name: Any
) -> StrEnumT | None:
    """Return the member named ``name``, or None for any other value.

    Each spec adds members over time (protobuf bindings and GBFS minor
    versions alike), so a value the model enum doesn't know -- from a feed
    newer than this library -- degrades to None ("unknown"), never raises;
    so does a value that is no string at all.
    """
    if not isinstance(name, str):
        return None
    try:
        return enum_cls(name)
    except ValueError:
        return None
