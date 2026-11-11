"""GET with redirects followed by hand, withholding a secret header.

aiohttp drops Authorization, Cookie and Proxy-Authorization when a
redirect leaves the origin, but forwards every other header -- including
a header-auth API key. RFC 9110 section 15.4 says to remove fields with
security implications there, so a secret header is fetched through here.
"""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from time import monotonic
from typing import Any

import aiohttp
from yarl import URL

from .exceptions import SourceConnectionError

# aiohttp's own redirect limit: the tenth redirect raises.
_MAX_REDIRECTS = 10
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
# Credentials aiohttp itself drops when a redirect leaves the origin.
_ORIGIN_CREDENTIALS = frozenset({"authorization", "cookie", "proxy-authorization"})

# ClientSession settings a credential-free sibling copies, by the private
# attribute each is kept in (aiohttp exposes only some of them publicly).
_COPIED = {
    "proxy": "_default_proxy",
    "proxy_auth": "_default_proxy_auth",
    "skip_auto_headers": "_skip_auto_headers",
    "request_class": "_request_class",
    "response_class": "_response_class",
    "version": "_version",
    "raise_for_status": "_raise_for_status",
    "timeout": "_timeout",
    "auto_decompress": "_auto_decompress",
    "trust_env": "_trust_env",
    "requote_redirect_url": "_requote_redirect_url",
    "trace_configs": "_trace_configs",
    "read_bufsize": "_read_bufsize",
    "max_line_size": "_max_line_size",
    "max_field_size": "_max_field_size",
    "max_headers": "_max_headers",
    "fallback_charset_resolver": "_resolve_charset",
    "middlewares": "_middlewares",
}
# Settings that can't change a GET to an absolute URL on a shared
# connector and jar, or that the sibling sets itself.
_NOT_COPIED = frozenset(
    {
        "self",
        "base_url",
        "connector",
        "loop",
        "cookies",  # already in the shared jar
        "headers",
        "auth",
        "json_serialize",
        "json_serialize_bytes",
        "ws_response_class",
        "cookie_jar",
        "connector_owner",
        "read_timeout",  # deprecated; folded into the timeout
        "conn_timeout",  # deprecated; folded into the timeout
        "ssl_shutdown_timeout",  # only when closing an owned connector
    }
)


def _origin(url: URL) -> tuple[str, str | None, int | None]:
    """RFC 6454 origin: an explicit default port names the same origin.

    (yarl's ``URL.origin()`` keeps an explicit ``:443``, so it can't be
    compared directly.)
    """
    return url.scheme, url.host, url.port


def _without_credentials(
    session: aiohttp.ClientSession, withheld: frozenset[str]
) -> aiohttp.ClientSession:
    """Return a session like ``session`` minus its ``withheld`` defaults and auth.

    It shares the connection pool and cookie jar, so the target origin's
    own cookies still go, as on aiohttp's own redirects. A setting this
    aiohttp version has that can't be copied fails instead: a request
    made without it might go somewhere, or carry something, the caller's
    session never would.
    """
    kwargs: dict[str, Any] = {}
    for name in inspect.signature(aiohttp.ClientSession.__init__).parameters:
        if name in _NOT_COPIED:
            continue
        attribute = _COPIED.get(name)
        if attribute is None or not hasattr(session, attribute):
            raise SourceConnectionError(
                f"Can't withhold the session's credentials from another origin: "
                f"its {name!r} setting can't be carried over"
            )
        kwargs[name] = getattr(session, attribute)
    return aiohttp.ClientSession(
        connector=session.connector,
        connector_owner=False,
        cookie_jar=session.cookie_jar,
        headers={k: v for k, v in session.headers.items() if k.lower() not in withheld},
        **kwargs,
    )


@asynccontextmanager
async def get_withholding(
    session: aiohttp.ClientSession,
    url: str,
    *,
    params: Mapping[str, str],
    headers: Mapping[str, str],
    total: float,
    secret: str | None,
) -> AsyncIterator[tuple[aiohttp.ClientResponse, Sequence[aiohttp.ClientResponse]]]:
    """GET ``url``; yield the final response and the redirects followed.

    Without a ``secret`` header aiohttp follows redirects itself. With one
    they are followed here: from the first hop to another origin on, the
    secret is never sent again, nor are the Authorization, Cookie and
    Proxy-Authorization headers aiohttp drops there. A session's own
    defaults among those (or its ``auth``) can't be left off a request it
    makes, so those hops go through a sibling session without them
    (:func:`_without_credentials`).

    The redirects followed, their limit and how a Location resolves are
    aiohttp's; a Location that isn't an HTTP(S) URL fails
    (:class:`SourceConnectionError`). ``total`` seconds bound the whole
    fetch, every hop and the body read, as aiohttp's own ``total`` does.
    """
    if secret is None:
        async with session.get(
            url,
            params=params or None,
            headers=headers or None,
            timeout=aiohttp.ClientTimeout(total=total),
        ) as response:
            yield response, response.history
        return
    deadline = monotonic() + total
    history: list[aiohttp.ClientResponse] = []
    target = URL(url)
    sent = dict(headers)
    query = params or None
    hop_session = session
    sibling: aiohttp.ClientSession | None = None
    try:
        while True:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError
            response = await hop_session.get(
                target,
                params=query,
                headers=sent or None,
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
            try:
                following = response.url.join(
                    URL(location, encoded=not session.requote_redirect_url)
                )
                origin = _origin(following)
            except ValueError as err:
                raise SourceConnectionError(
                    f"Producer redirected to an invalid URL ({location!r}): {url}"
                ) from err
            if following.scheme not in ("http", "https"):
                raise SourceConnectionError(
                    f"Producer redirected to a non-HTTP URL ({following}): {url}"
                )
            if origin != _origin(response.url):
                withheld = _ORIGIN_CREDENTIALS | {secret.lower()}
                sent = {k: v for k, v in sent.items() if k.lower() not in withheld}
                if sibling is None and (
                    session.auth is not None
                    or any(name.lower() in withheld for name in session.headers)
                ):
                    sibling = hop_session = _without_credentials(session, withheld)
            target, query = following, None
    finally:
        if sibling is not None:
            await sibling.close()
