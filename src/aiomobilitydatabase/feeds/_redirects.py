"""GET with redirects followed by hand, withholding headers they mustn't carry.

aiohttp drops Authorization, Cookie and Proxy-Authorization when a
redirect leaves the origin, but forwards every other header -- including
a header-auth API key, and the cache's own validators. RFC 9110 section
15.4 says to remove both: fields with security implications when the
origin changes, and validators the cache added on any redirect. A
request carrying either is fetched through here.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from time import monotonic

import aiohttp
from yarl import URL

from .exceptions import SourceConnectionError

# aiohttp's own redirect limit: the tenth redirect raises.
_MAX_REDIRECTS = 10
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
# Credentials aiohttp itself drops when a redirect leaves the origin.
_ORIGIN_CREDENTIALS = frozenset({"authorization", "cookie", "proxy-authorization"})


def _origin(url: URL) -> tuple[str, str | None, int | None]:
    """RFC 6454 origin: an explicit default port names the same origin.

    (yarl's ``URL.origin()`` keeps an explicit ``:443``, so it can't be
    compared directly.)
    """
    return url.scheme, url.host, url.port


def _remaining(deadline: float | None) -> float | None:
    """Seconds left before ``deadline`` (None: no bound); none left raises."""
    if deadline is None:
        return None
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise TimeoutError
    return remaining


def _same_resource(target: URL, query: Mapping[str, str] | None, other: URL) -> bool:
    """Whether a hop requesting ``target`` (plus ``query``) asks for ``other``.

    Credentials in the URL aren't part of the resource; query parameters
    are, in any order.
    """
    asked = target.with_user(None)
    if query:
        asked = asked.update_query(query)
    other = other.with_user(None)
    return asked.with_query(None) == other.with_query(None) and sorted(
        asked.query.items()
    ) == sorted(other.query.items())


def _resolve(base: URL, location: str, session: aiohttp.ClientSession, url: str) -> URL:
    """Resolve a Location as aiohttp does; only an HTTP(S) URL is followed."""
    try:
        following = base.join(URL(location, encoded=not session.requote_redirect_url))
        _origin(following)
    except ValueError as err:
        raise SourceConnectionError(
            f"Producer redirected to an invalid URL ({location!r}): {url}"
        ) from err
    if following.scheme not in ("http", "https"):
        raise SourceConnectionError(
            f"Producer redirected to a non-HTTP URL ({following}): {url}"
        )
    return following


@asynccontextmanager
async def get_withholding(
    session: aiohttp.ClientSession,
    url: str,
    *,
    params: Mapping[str, str],
    headers: Mapping[str, str],
    total: float | None,
    secret: str | None,
    conditional: Mapping[str, str] | None = None,
    conditional_url: str | None = None,
) -> AsyncIterator[tuple[aiohttp.ClientResponse, Sequence[aiohttp.ClientResponse]]]:
    """GET ``url``; yield the final response and the redirects followed.

    ``conditional`` are the cache's validators (If-None-Match and
    If-Modified-Since), for the resource at ``conditional_url`` (None:
    ``url``): sent only on a hop asking for exactly that resource. Never
    carried along a redirect to another (RFC 9110 section 15.4 step 2.4),
    and never sent to a URL that redirected to the one they came from
    (RFC 9111 section 4.3.1) -- whose own request then gets them. Where a
    hop by hand can't withhold the session's own credentials (below) and
    no ``secret`` is at stake, the request is made again unconditionally,
    aiohttp following the redirects and dropping them itself.

    Without a ``secret`` header or ``conditional`` aiohttp follows
    redirects itself. Otherwise they are followed here: from the first hop
    to another origin on, the secret is never sent again, nor are the
    Authorization, Cookie and Proxy-Authorization headers aiohttp drops
    there, nor credentials in the URL (which, as on aiohttp's redirects,
    follow hops within their origin).

    A session's own defaults among those (its ``auth`` or such default
    headers) ride on every request it makes, so a hop to another origin
    that would carry them fails instead (:class:`SourceConnectionError`):
    following a redirect is optional (RFC 9110 section 15.4), sending
    them on is not. So does a Location that isn't an HTTP(S) URL. The
    redirects followed, their limit and how a Location resolves are
    aiohttp's. ``total`` seconds bound the whole fetch, every hop and the
    body read, as aiohttp's own ``total`` does (``None``, zero or less:
    no bound).
    """
    if secret is None and not conditional:
        async with session.get(
            url,
            params=params or None,
            headers=headers or None,
            timeout=aiohttp.ClientTimeout(total=total),
        ) as response:
            yield response, response.history
        return
    deadline = monotonic() + total if total is not None and total > 0 else None
    withheld = _ORIGIN_CREDENTIALS | ({secret.lower()} if secret else set())
    history: list[aiohttp.ClientResponse] = []
    target = URL(url)
    sent = dict(headers)
    validated = URL(conditional_url) if conditional_url is not None else target
    query = params or None
    # Credentials in the URL: aiohttp sends them for the first hop, and its
    # own redirects keep sending them while the chain stays in their origin.
    userinfo = (target.user, target.password) if target.user is not None else None
    userinfo_origin = _origin(target)
    while True:
        remaining = _remaining(deadline)
        # The validators go to the resource they came from, and only to it.
        asked = _same_resource(target, query, validated)
        hop_headers = {**sent, **conditional} if conditional and asked else sent
        response = await session.get(
            target,
            params=query,
            headers=hop_headers or None,
            timeout=aiohttp.ClientTimeout(total=remaining),
            allow_redirects=False,
        )
        location = response.headers.get("Location")
        if response.status not in _REDIRECT_STATUSES or location is None:
            try:
                yield response, history
            finally:
                response.release()
            return
        response.release()
        history.append(response)
        if len(history) >= _MAX_REDIRECTS:
            raise SourceConnectionError(f"Too many redirects fetching {url}")
        following = _resolve(response.url, location, session, url)
        origin = _origin(following)
        if origin != _origin(response.url):
            if session.auth is not None or any(
                name.lower() in withheld for name in session.headers
            ):
                if secret is None:
                    break  # only validators made this hand-followed: retry
                raise SourceConnectionError(
                    f"Producer redirected to another origin ({following.origin()}) "
                    f"and the session carries credentials that can't be withheld: {url}"
                )
            sent = {k: v for k, v in sent.items() if k.lower() not in withheld}
            userinfo = None
        if following.user is not None:
            # Credentials the Location gives are addressed to its origin.
            userinfo = (following.user, following.password)
            userinfo_origin = origin
        elif userinfo is not None and origin == userinfo_origin:
            following = following.with_user(userinfo[0]).with_password(userinfo[1])
        target, query = following, None
    remaining = _remaining(deadline)
    async with session.get(
        url,
        params=params or None,
        headers=headers or None,
        timeout=aiohttp.ClientTimeout(total=remaining),
    ) as response:
        yield response, response.history
