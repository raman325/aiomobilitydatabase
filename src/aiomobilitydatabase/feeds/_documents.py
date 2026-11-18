"""One document fetched for a :class:`FreshnessCache`: GBFS's and the export's.

GBFS documents and the public catalog export are fetched alike: a GET
through :func:`get_withholding` (redirects followed, conditional when the
cache revalidates), any error status a :class:`SourceConnectionError`, and
the body handed to a parser of the caller's, which raises
:class:`FeedParseError` for a body it can't read.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from http import HTTPStatus
from typing import TYPE_CHECKING

from ._freshness import (
    Fetch,
    FreshnessCache,
    Response,
    Validators,
    header_fields,
    request_fields,
)
from ._freshness import hops as _hops
from ._redirects import get_withholding, network
from ._urls import _require_http_url, redact
from .exceptions import SourceConnectionError

if TYPE_CHECKING:
    from .client import MobilityFeedsClient


async def fetch_document[T](
    client: MobilityFeedsClient,
    url: str,
    headers: Mapping[str, str] | None,
    context: str,
    *,
    label: str,
    missing: str,
    parse: Callable[[bytes, str | None], Awaitable[T]],
    validators: Validators | None = None,
) -> Response[T]:
    """GET one document on the client's session and parse it.

    With ``validators`` the request is conditional and a ``304`` comes back
    with no value; a 304 to an unconditional request raises. ``context``
    names the URL for :func:`_require_http_url`; ``label`` starts each
    status error's message and ``missing`` names what a non-200 answer
    lacks ("a document"). ``parse`` gets the body and the redacted URL.
    """
    _require_http_url(url, context)
    session = client._get_session()  # deliberate friend access
    requested_at = datetime.now(UTC)
    shown = redact(url)
    async with get_withholding(
        session,
        url,
        params={},
        headers=dict(headers) if headers else {},
        total=client.timeout_seconds,
        secret=None,
        conditional=None if validators is None else validators.request_headers(),
        conditional_url=None if validators is None else validators.url,
    ) as (resp, history):
        received_at = datetime.now(UTC)
        fields = header_fields(resp.headers)
        if resp.status == HTTPStatus.NOT_MODIFIED:
            if validators is None:
                raise SourceConnectionError(
                    f"{label} answered 304 to an unconditional request",
                    status=int(HTTPStatus.NOT_MODIFIED),
                    url=shown,
                )
            return Response(
                value=None,
                fields=fields,
                requested_at=requested_at,
                received_at=received_at,
                hops=_hops(resp, history),
                url=str(resp.url),
            )
        if resp.status >= HTTPStatus.BAD_REQUEST:
            raise SourceConnectionError(
                f"{label} error {resp.status}", status=resp.status, url=shown
            )
        if resp.status not in (
            HTTPStatus.OK,
            HTTPStatus.NON_AUTHORITATIVE_INFORMATION,
        ):
            raise SourceConnectionError(
                f"{label} answered {resp.status} without {missing}",
                status=resp.status,
                url=shown,
            )
        raw = await network(resp.read(), url)
        hops = _hops(resp, history)
        answered = str(resp.url)
    return Response(
        value=await parse(raw, shown),
        fields=fields,
        requested_at=requested_at,
        received_at=received_at,
        hops=hops,
        url=answered,
    )


async def cached_document[T](
    cache: FreshnessCache[T],
    client: MobilityFeedsClient,
    url: str,
    *,
    headers: Mapping[str, str],
    context: str,
    fetch: Fetch[T],
) -> T:
    """One document from ``cache``, fetched by ``fetch`` when not fresh."""
    _require_http_url(url, context)  # before request_fields parses it
    return await cache.get(
        url,
        fetch,
        request_fields=request_fields(
            client._get_session(),  # deliberate friend access
            url,
            headers,
        ),
    )
