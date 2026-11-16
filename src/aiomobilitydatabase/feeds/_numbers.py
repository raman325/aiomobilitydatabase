"""The one reading of a number written as text, shared by every feed parser."""

from __future__ import annotations

import math
import re

# ASCII only: a sign, digits with an optional fraction (or a bare fraction),
# an optional exponent. float() alone also reads other scripts' digits,
# PEP 515 grouping ("1_0"), "inf", "nan" and surrounding Unicode whitespace.
_ASCII_NUMBER = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")


def ascii_number(text: str) -> float | None:
    """Return the finite number ``text`` spells in ASCII, or None.

    Surrounding spaces and tabs are allowed; ``1e999`` (no finite double)
    is None like any other non-number.
    """
    stripped = text.strip(" \t")
    if not _ASCII_NUMBER.fullmatch(stripped):
        return None
    number = float(stripped)
    return number if math.isfinite(number) else None
