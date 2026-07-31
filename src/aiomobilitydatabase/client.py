"""Async client for the Mobility Database catalog API."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any


def _encode_value(value: Any) -> str:
    """Encode a single query parameter value as a string."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return ",".join(_encode_value(item) for item in value)
    return str(value)


def encode_params(params: dict[str, Any]) -> dict[str, str]:
    """Encode query parameters, dropping None values.

    Booleans become "true"/"false", enums use their value, datetimes use ISO
    format, and lists/tuples are comma-joined (the API's convention for
    multi-value filters).
    """
    return {
        key: _encode_value(value) for key, value in params.items() if value is not None
    }
