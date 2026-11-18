"""The catalog client's one transport boundary.

Everything between handing a request to aiohttp and holding the response's
bytes happens in :func:`exchange`, inside a single ``try``. Whatever aiohttp
or yarl raise there -- and which exceptions they raise varies by version and
build -- is a :class:`MobilityDatabaseConnectionError` named by its type
alone: its own text can quote the request (a token with it), so neither the
text nor the exception itself (``from None``) is passed on. What comes out is
plain data, which the client maps to models and errors without asking
aiohttp to parse anything more.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import aiohttp

from ._freshness import header_fields
from ._http import REQUEST_OPTIONS
from .exceptions import MobilityDatabaseConnectionError


@dataclass(frozen=True)
class Answer:
    """A response, read whole: status, media type, body and header fields."""

    status: int
    # The Content-Type's media type, lower-cased, parameters dropped; "" when
    # absent. Read from the raw header: aiohttp before 3.14 raises parsing
    # one with an undecodable byte, and a charset has no effect on JSON.
    media_type: str
    # None when the body couldn't be read (a Content-Encoding its bytes
    # don't follow): the status still counts.
    body: bytes | None
    # Lower-cased name -> values in the order received (see
    # :func:`._freshness.header_fields`): what a cache keeps.
    fields: Mapping[str, tuple[str, ...]] = field(default_factory=dict)


async def exchange(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    *,
    what: str,
    client_timeout: aiohttp.ClientTimeout,
    **request: Any,
) -> Answer:
    """Send one request and read its answer whole, never following redirects.

    A redirect is answered as the 3xx it is: the catalog's answers come from
    the catalog's origin, and the token request carries the refresh token,
    which RFC 6749 section 10.4 keeps between client and authorization
    server. The status mapping is the client's, whatever the session's
    ``raise_for_status``.
    """
    if session.closed:
        raise RuntimeError("the session is closed")  # the caller's, not the API's
    try:
        async with session.request(
            method,
            url,
            timeout=client_timeout,
            **REQUEST_OPTIONS,
            **request,
        ) as resp:
            body: bytes | None
            try:
                body = await resp.read()
            except aiohttp.ClientPayloadError:
                body = None
            header = resp.headers.get("Content-Type", "")
            media_type = header.split(";", 1)[0].strip(" \t").lower()
            return Answer(resp.status, media_type, body, header_fields(resp.headers))
    except Exception as err:  # the boundary: see the module docstring
        failure = type(err).__name__
    # Raised outside the except, so the original isn't even the context:
    # from None would only hide it from tracebacks, not drop it.
    raise MobilityDatabaseConnectionError(f"{what} failed ({failure})")
