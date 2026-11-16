"""The feeds' network boundary: every request, and every redirect, by hand.

Two rules keep producer-controlled input from escaping as a bare exception
or reaching another URL than the producer named:

- :func:`network` is the only place a network await can fail. Whatever
  aiohttp or yarl raise there -- which exceptions they raise varies by
  version and build -- is a :class:`SourceConnectionError` named by its type
  alone, raised after the ``except`` so not even its context rides along:
  its text can quote the request, a credential with it. Only network
  awaits go through it, never a caller's callback or a file write, which
  keep their own exceptions.
- :func:`get_withholding` follows every redirect itself, through one loop:
  each Location is checked like a first URL (:func:`._urls.require_requestable`)
  before anything is sent to it. aiohttp never follows one, so no hop can
  skip the check. The loop also withholds, from the first hop to another
  origin on, what RFC 9110 section 15.4 says to remove there: fields with
  security implications (a header-auth secret, and the Authorization, Cookie
  and Proxy-Authorization aiohttp would drop) and validators the cache added.
"""

from __future__ import annotations

import base64
from collections.abc import AsyncIterator, Awaitable, Mapping, Sequence
from contextlib import asynccontextmanager
from http import HTTPStatus
from time import monotonic

import aiohttp
from yarl import URL

from .._http import CREDENTIAL_HEADERS, REQUEST_OPTIONS, URL_ERRORS, require_session
from ._urls import _require_http_url, has_credentials, redact, require_requestable
from .exceptions import MobilityFeedsError, SourceConnectionError

# aiohttp's own redirect limit: the tenth redirect raises.
_MAX_REDIRECTS = 10
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})


async def network[T](awaitable: Awaitable[T], url: str) -> T:
    """Await one network step; any failure is SourceConnectionError.

    The boundary (see the module docstring): a library error passes
    through, anything else is named by type, without its text.
    """
    try:
        return await awaitable
    except MobilityFeedsError:
        raise
    except Exception as err:
        failure = type(err).__name__
    # Outside the except: from None would only hide the original, still
    # attached as the context.
    raise SourceConnectionError(f"Request failed ({failure})", url=redact(url))


def _origin(url: URL) -> tuple[str, str | None, int | None]:
    """RFC 6454 origin: an explicit default port names the same origin.

    (yarl's ``URL.origin()`` keeps an explicit ``:443``, so it can't be
    compared directly.) The host in its ASCII form, as RFC 6454 compares
    it: decoding punycode (``url.host``) raises for a label such as
    ``xn--`` that a request can still be made to.
    """
    return url.scheme, url.raw_host, url.port


def same_origin(a: str, b: str) -> bool:
    """Return whether two checked URLs share an RFC 6454 origin."""
    return _origin(URL(a)) == _origin(URL(b))


def _basic(user: str | None, password: str | None) -> str:
    """Return the Basic credentials header value (RFC 7617).

    The URL guard has checked them: Latin-1 (aiohttp's encoding), no
    control character, no colon in the user-id.
    """
    pair = f"{user or ''}:{password or ''}".encode("latin-1")
    return "Basic " + base64.b64encode(pair).decode("ascii")


def _remaining(deadline: float | None, url: str) -> float | None:
    """Seconds left before ``deadline`` (None: no bound); none left raises."""
    if deadline is None:
        return None
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise SourceConnectionError("Request timed out", url=redact(url))
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
    """Resolve a Location by RFC 3986; refuse what no request can go to.

    A reference without a scheme is joined onto the base; one with a scheme
    of its own is used as it is (section 5.2.2, the strict parser), where
    joining -- as aiohttp would -- gives ``http:///x`` the base's host.
    Then the same checks as a first URL: UTF-8 text (yarl drops a lone
    surrogate, naming another URL), a host the resolver can encode,
    credentials Basic auth can carry -- and an http(s) scheme.
    """
    try:
        location.encode("utf-8")
        reference = URL(location, encoded=not session.requote_redirect_url)
        following = reference if reference.scheme else base.join(reference)
        require_requestable(following)
    except URL_ERRORS:
        pass
    else:
        if following.scheme in ("http", "https"):
            return following
        raise SourceConnectionError(
            "Producer redirected to a non-HTTP URL", url=redact(url)
        )
    raise SourceConnectionError(
        "Producer redirected to an invalid URL", url=redact(url)
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
    conditional: Mapping[str, str] | None = None,
    conditional_url: str | None = None,
    method: str = "GET",
    sock_read: float | None = None,
) -> AsyncIterator[tuple[aiohttp.ClientResponse, Sequence[aiohttp.ClientResponse]]]:
    """Request ``url``; yield the final response and the redirects followed.

    Every redirect is followed here (see the module docstring), up to
    aiohttp's own limit. From the first hop to another origin on, the
    ``secret`` header is never sent again, nor are the Authorization, Cookie
    and Proxy-Authorization headers aiohttp drops there, nor credentials in
    the URL (which, as on aiohttp's redirects, follow hops within their
    origin). The session itself carries no credential (the shared policy,
    :func:`.._http.require_session`, checked here too), so there is none of
    its own to shed.

    ``conditional`` are the cache's validators (If-None-Match and
    If-Modified-Since), for the resource at ``conditional_url`` (None:
    ``url``): sent only on a hop asking for exactly that resource. Never
    carried along a redirect to another (RFC 9110 section 15.4 step 2.4),
    and never sent to a URL that redirected to the one they came from
    (RFC 9111 section 4.3.1) -- whose own request then gets them.

    ``total`` seconds bound every hop (``None``, zero or less: no bound);
    ``sock_read`` bounds each read. A 303 turns a request other than HEAD
    into a GET (RFC 9110 section 15.4.4). The caller reads the body through
    :func:`network`.
    """
    if session.closed:
        raise RuntimeError("the session is closed")  # the caller's, not a source's
    require_session(session)
    # Callers check first, naming what they fetch; checked here too, so no
    # path reaches the network with a URL yarl would change or can't send.
    _require_http_url(url, "a URL")
    deadline = monotonic() + total if total is not None and total > 0 else None
    withheld = CREDENTIAL_HEADERS | ({secret.lower()} if secret else set())
    history: list[aiohttp.ClientResponse] = []
    target = URL(url)
    sent = dict(headers)
    validated = URL(conditional_url) if conditional_url is not None else target
    query = params or None
    # Credentials in the URL: sent for the first hop, and kept while the
    # chain stays in their origin. Sent as Basic auth by the library, the URL
    # stripped of them: whether aiohttp derives auth from a URL (3.10 skips a
    # password without a user; 3.14 deprecates its auth) no longer decides.
    userinfo = (target.user, target.password) if has_credentials(target) else None
    target = target.with_user(None)
    while True:
        remaining = _remaining(deadline, url)
        # The validators go to the resource they came from, and only to it.
        asked = _same_resource(target, query, validated)
        hop_headers = {**sent, **conditional} if conditional and asked else sent
        if userinfo is not None:
            if any(name.lower() == "authorization" for name in hop_headers):
                # Which would be sent is aiohttp's choice (it refused the
                # pair): neither is, the request is the source's fault.
                raise SourceConnectionError(
                    "The URL's credentials and an Authorization header "
                    "can't both be sent",
                    url=redact(url),
                )
            hop_headers = {**hop_headers, "Authorization": _basic(*userinfo)}
        response = await network(
            session.request(
                method,
                target,
                params=query,
                headers=hop_headers or None,
                timeout=aiohttp.ClientTimeout(total=remaining, sock_read=sock_read),
                **REQUEST_OPTIONS,
            ),
            url,
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
            raise SourceConnectionError("Too many redirects", url=redact(url))
        following = _resolve(response.url, location, session, url)
        origin = _origin(following)
        if origin != _origin(response.url):
            sent = {k: v for k, v in sent.items() if k.lower() not in withheld}
            userinfo = None
        if has_credentials(following):
            # Credentials the Location gives are addressed to its origin.
            userinfo = (following.user, following.password)
            following = following.with_user(None)
        if response.status == HTTPStatus.SEE_OTHER and method != "HEAD":
            method = "GET"
        target, query = following, None
