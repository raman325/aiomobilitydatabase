"""One document fetched for a :class:`FreshnessCache`: GBFS's and the export's.

GBFS documents and the public catalog export are fetched alike: a GET
through :func:`get_withholding` (redirects followed, conditional when the
cache revalidates), any error status a :class:`SourceConnectionError`, and
the body handed to a parser of the caller's, which raises
:class:`FeedParseError` for a body it can't read. The URL is the caller's
to check (:func:`._urls._require_http_url`) before it gets here.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from functools import partial
from http import HTTPStatus
from typing import TYPE_CHECKING

from ._freshness import (
    FreshnessCache,
    Response,
    Validators,
    header_fields,
    request_fields,
)
from ._freshness import hops as _hops
from ._redirects import get_withholding, network
from ._urls import redact
from .exceptions import SourceConnectionError

if TYPE_CHECKING:
    from .client import MobilityFeedsClient


type Parse[T] = Callable[[bytes, str | None], Awaitable[T]]


async def cached_document[T](
    cache: FreshnessCache[T],
    client: MobilityFeedsClient,
    url: str,
    *,
    headers: Mapping[str, str],
    label: str,
    missing: str,
    parse: Parse[T],
) -> T:
    """One document from ``cache``, fetched when it isn't fresh.

    ``url`` has passed :func:`._urls._require_http_url`. ``headers`` go
    with the request; ``label`` starts each status error's message and
    ``missing`` names what a non-200 answer lacks ("a document").
    ``parse`` gets the body and the redacted URL.
    """
    fetch = partial(
        _fetch_document,
        client,
        url,
        headers,
        label=label,
        missing=missing,
        parse=parse,
    )
    return await cache.get(
        url,
        fetch,
        request_fields=request_fields(
            client._get_session(),  # deliberate friend access
            url,
            headers,
        ),
    )


async def _fetch_document[T](
    client: MobilityFeedsClient,
    url: str,
    headers: Mapping[str, str],
    *,
    label: str,
    missing: str,
    parse: Parse[T],
    validators: Validators | None,
) -> Response[T]:
    """GET one document on the client's session and parse it.

    With ``validators`` the request is conditional and a ``304`` comes back
    with no value; a 304 to an unconditional request raises.
    """
    session = client._get_session()  # deliberate friend access
    requested_at = datetime.now(UTC)
    shown = redact(url)
    async with get_withholding(
        session,
        url,
        params={},
        headers=dict(headers),
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
