"""The one reading of a number written as text, shared by every feed parser."""

from __future__ import annotations

import math
import re
from decimal import Decimal, InvalidOperation

# ASCII only: a sign, digits with an optional fraction (or a bare fraction),
# an optional exponent. float() alone also reads other scripts' digits,
# PEP 515 grouping ("1_0"), "inf", "nan" and surrounding Unicode whitespace.
_ASCII_NUMBER = re.compile(
    r"([+-]?)([0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE]([+-]?)[0-9]+)?"
)


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


def exact_number(text: str) -> Decimal | None:
    """Return the value ``text`` spells in ASCII, exactly, or None.

    The same spellings as :func:`ascii_number`, read as written rather than
    rounded to the nearest double: ``90.000000000000001`` is past 90, though
    its double is 90.0. A value Decimal cannot hold (an exponent past about
    10**18 digits) comes back as a signed infinity when it is that far from
    zero, or a signed zero when it is that close to it -- its place against
    any ordinary limit, and its nearest double, unchanged.
    """
    stripped = text.strip(" \t")
    if not (match := _ASCII_NUMBER.fullmatch(stripped)):
        return None
    try:
        return Decimal(stripped)
    except InvalidOperation:
        sign, digits, exponent_sign = match.groups()
        zero = not digits.strip("0.") or exponent_sign == "-"
        return Decimal(f"{sign}{'0' if zero else 'Infinity'}")
