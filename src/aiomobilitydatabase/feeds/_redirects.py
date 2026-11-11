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
from aiohttp.abc import AbstractCookieJar
from yarl import URL

from .exceptions import SourceConnectionError

# aiohttp's own redirect limit: the tenth redirect raises.
_MAX_REDIRECTS = 10
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
# Credentials aiohttp itself drops when a redirect leaves the origin.
_ORIGIN_CREDENTIALS = frozenset({"authorization", "cookie", "proxy-authorization"})
_NO_URL = URL()

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


def _shared_cookie_keys(jar: aiohttp.CookieJar) -> list[tuple[str, str]]:
    """Return the jar keys of its shared cookies (no domain: every host).

    aiohttp keeps the session's ``cookies=`` there, keyed ``("", path)``.
    """
    return [key for key, cookies in jar._cookies.items() if not key[0] and cookies]


class _OwnCookiesOnly(AbstractCookieJar):
    """A view of a session's jar that never sends its shared cookies.

    Everything else -- including cookies set by the hops -- goes to and
    comes from the session's own jar.
    """

    def __init__(self, jar: aiohttp.CookieJar) -> None:
        super().__init__()
        self._jar = jar

    @property
    def unsafe(self) -> bool:
        value = getattr(self._jar, "unsafe", None)  # public from aiohttp 3.10
        return bool(self._jar._unsafe if value is None else value)

    @property
    def quote_cookie(self) -> bool:
        value = getattr(self._jar, "quote_cookie", None)  # public from aiohttp 3.10
        return bool(self._jar._quote_cookie if value is None else value)

    @property
    def treat_as_secure_origin(self) -> Any:
        return getattr(self._jar, "treat_as_secure_origin", frozenset())

    # Newer aiohttp versions only: read through getattr.
    @property
    def cookies(self) -> Any:
        return getattr(self._jar, "cookies")  # noqa: B009

    @property
    def host_only_cookies(self) -> Any:
        return getattr(self._jar, "host_only_cookies")  # noqa: B009

    def clear(self, predicate: Any = None) -> None:
        self._jar.clear(predicate)

    def clear_domain(self, domain: str) -> None:
        self._jar.clear_domain(domain)

    def update_cookies(self, cookies: Any, response_url: URL = _NO_URL) -> None:
        self._jar.update_cookies(cookies, response_url)

    def update_cookies_from_headers(self, headers: Any, response_url: URL) -> None:
        getattr(self._jar, "update_cookies_from_headers")(headers, response_url)  # noqa: B009

    def filter_cookies(self, request_url: URL = _NO_URL) -> Any:
        # The full filter first, so expired cookies are purged as usual;
        # then again with the shared ones hidden. No await in between:
        # nothing else can observe the jar without them.
        self._jar.filter_cookies(request_url)
        stored = self._jar._cookies
        hidden = {key: stored.pop(key) for key in _shared_cookie_keys(self._jar)}
        try:
            return self._jar.filter_cookies(request_url)
        finally:
            stored.update(hidden)

    def __iter__(self) -> Any:
        return iter(self._jar)

    def __len__(self) -> int:
        return len(self._jar)


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
    jar = session.cookie_jar
    sibling_jar: AbstractCookieJar
    if isinstance(jar, aiohttp.DummyCookieJar):
        sibling_jar = jar
    elif type(jar) is aiohttp.CookieJar and hasattr(jar, "_cookies"):
        sibling_jar = _OwnCookiesOnly(jar)
    else:
        raise SourceConnectionError(
            "Can't withhold the session's credentials from another origin: "
            f"its cookie jar ({type(jar).__name__}) can't be inspected"
        )
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
    # A multidict copy: a repeated default keeps every value.
    headers = session.headers.copy()
    for name in withheld:
        headers.popall(name, None)
    return aiohttp.ClientSession(
        connector=session.connector,
        connector_owner=False,
        cookie_jar=sibling_jar,
        headers=headers,
        **kwargs,
    )


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


def _carries_credentials(
    session: aiohttp.ClientSession, withheld: frozenset[str]
) -> bool:
    """Tell whether a request ``session`` makes carries a withheld default."""
    jar = session.cookie_jar
    if isinstance(jar, aiohttp.DummyCookieJar):
        shared = False
    elif type(jar) is aiohttp.CookieJar and hasattr(jar, "_cookies"):
        shared = bool(_shared_cookie_keys(jar))
    else:
        shared = True  # can't tell: assume it might
    return (
        shared
        or session.auth is not None
        or any(name.lower() in withheld for name in session.headers)
    )


@asynccontextmanager
async def get_withholding(
    session: aiohttp.ClientSession,
    url: str,
    *,
    params: Mapping[str, str],
    headers: Mapping[str, str],
    total: float | None,
    secret: str | None,
) -> AsyncIterator[tuple[aiohttp.ClientResponse, Sequence[aiohttp.ClientResponse]]]:
    """GET ``url``; yield the final response and the redirects followed.

    Without a ``secret`` header aiohttp follows redirects itself. With one
    they are followed here: from the first hop to another origin on, the
    secret is never sent again, nor are the Authorization, Cookie and
    Proxy-Authorization headers aiohttp drops there, nor credentials in
    the URL (which, as on aiohttp's redirects, follow hops within their
    origin). A session's own defaults among those (its ``auth``, or the
    shared cookies its ``cookies=`` put in the jar) can't be left off a
    request it makes, so those hops go through a sibling session without
    them (:func:`_without_credentials`).

    The redirects followed, their limit and how a Location resolves are
    aiohttp's; a Location that isn't an HTTP(S) URL fails
    (:class:`SourceConnectionError`). ``total`` seconds bound the whole
    fetch, every hop and the body read, as aiohttp's own ``total`` does
    (``None``, zero or less: no bound).
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
    deadline = monotonic() + total if total is not None and total > 0 else None
    history: list[aiohttp.ClientResponse] = []
    target = URL(url)
    # Credentials in the URL: aiohttp sends them for the first hop, and its
    # own redirects keep sending them while the chain stays in their origin.
    userinfo = (target.user, target.password) if target.user is not None else None
    userinfo_origin = _origin(target)
    sent = dict(headers)
    query = params or None
    hop_session = session
    sibling: aiohttp.ClientSession | None = None
    try:
        while True:
            remaining = None if deadline is None else deadline - monotonic()
            if remaining is not None and remaining <= 0:
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
            following = _resolve(response.url, location, session, url)
            origin = _origin(following)
            if origin != _origin(response.url):
                withheld = _ORIGIN_CREDENTIALS | {secret.lower()}
                sent = {k: v for k, v in sent.items() if k.lower() not in withheld}
                userinfo = None
                if sibling is None and _carries_credentials(session, withheld):
                    sibling = hop_session = _without_credentials(session, withheld)
            if following.user is not None:
                # Credentials the Location gives are addressed to its origin.
                userinfo = (following.user, following.password)
                userinfo_origin = origin
            elif userinfo is not None and origin == userinfo_origin:
                following = following.with_user(userinfo[0]).with_password(userinfo[1])
            target, query = following, None
    finally:
        if sibling is not None:
            await sibling.close()
