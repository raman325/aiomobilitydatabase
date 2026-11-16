"""Checks on the URLs the feeds request, and on the caller's own inputs.

Producer and catalog URLs are data, checked by :func:`_require_http_url`
before a request and by the redirect loop at every hop: an http(s) URL, UTF-8
text (yarl drops a lone surrogate and requests another URL), with a host, and
credentials RFC 7617 allows. Everything else a request can trip on is the
network boundary's (:func:`._redirects.network`): it needs no list here.

The caller's inputs are checked when a handle is made, against an allowlist
rather than a copy of aiohttp's rules (:func:`require_caller_inputs`), so a
caller's mistake is a ValueError up front, never a source error mid-request:
credentials in the caller's own URLs included.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING

from yarl import URL

from .._http import URL_ERRORS, is_token, is_visible, require_session
from .exceptions import SourceConnectionError

if TYPE_CHECKING:
    import aiohttp

_ALLOWED_URL_SCHEMES = frozenset({"http", "https"})
_CONTROL = re.compile(r"[\x00-\x1f\x7f]")  # RFC 5234 CTL
# What Basic auth can send of a URL's credentials: Latin-1, the octets
# aiohttp encodes them to (RFC 7617 section 2 leaves the charset open).
_LATIN_1 = re.compile(r"[\x00-\xff]*")


class UnfetchableURLError(ValueError):
    """Why a URL is refused, in text that is safe to show (no URL in it)."""


def require_caller_inputs(
    session: aiohttp.ClientSession,
    urls: Iterable[str] = (),
    headers: Mapping[str, str] | None = None,
    api_key: str | None = None,
) -> None:
    """Raise ValueError for a caller input outside the shared allowlist.

    Checked when a handle is made (:mod:`.._http`): the session's defaults
    (sendable, no credential), every ``headers`` entry (a token name, a
    visible-ASCII value), the ``api_key`` (visible ASCII), and credentials
    in the caller's ``urls`` (what :func:`require_requestable` asks of
    them). An Authorization header beside credentials in the caller's URLs
    is refused too: one request carries one Authorization field.
    """
    require_session(session)
    for url in urls:
        try:
            parsed = URL(url)
        except URL_ERRORS:
            continue  # the guard's to report, naming what it fetches
        problem = _credentials_problem(parsed)
        if problem is not None:
            raise ValueError(problem)
    for name, value in (headers or {}).items():
        if not is_token(name):
            raise ValueError(f"header name {name!r} is no HTTP field name")
        if not is_visible(value):
            raise ValueError(f"header {name!r} must be visible ASCII")
    if api_key is not None and not is_visible(api_key):
        raise ValueError("api_key must be visible ASCII")
    if any(name.lower() == "authorization" for name in headers or {}) and any(
        _has_credentials(url) for url in urls
    ):
        raise ValueError("an Authorization header can't go with credentials in the URL")


def _has_credentials(url: str) -> bool:
    """Return whether a URL carries credentials (a user, or a password alone).

    False when it doesn't parse: a malformed URL is the guard's to report.
    """
    try:
        parsed = URL(url)
    except URL_ERRORS:
        return False
    return has_credentials(parsed)


def has_credentials(parsed: URL) -> bool:
    """Return whether a parsed URL carries credentials: a user, or a password.

    A password alone counts (``http://:pw@host/``, whose ``user`` yarl
    reads as None): aiohttp sends it as Basic auth all the same.
    """
    return parsed.raw_user is not None or parsed.raw_password is not None


def _credentials_problem(parsed: URL) -> str | None:
    """Why a URL's credentials can't be sent as Basic auth (None: they can).

    RFC 7617 section 2: neither user-id nor password holds a control
    character, and the user-id no colon (it ends the user-id); and every
    character must be one the Basic credentials are encoded to (Latin-1).
    """
    user, password = parsed.user or "", parsed.password or ""
    if _CONTROL.search(user) or _CONTROL.search(password):
        return "the URL's credentials hold a control character"
    if ":" in user:
        return "the URL's user-id holds a colon"
    if not (_LATIN_1.fullmatch(user) and _LATIN_1.fullmatch(password)):
        return "the URL's credentials hold a character outside Latin-1"
    return None


def redact(url: str) -> str | None:
    """Return a URL for an exception's ``url``: no credentials, query or fragment.

    Either can hold a secret (an API key travels in the query). None for
    anything that isn't an absolute URL: nothing of it is shown.
    """
    try:
        parsed = URL(url)
        if not parsed.is_absolute():
            return None
        return str(parsed.with_user(None).with_query(None).with_fragment(None))
    except URL_ERRORS:
        return None


def require_requestable(parsed: URL) -> None:
    """Raise :class:`UnfetchableURLError` for an http(s) URL not to request.

    A host is required (RFC 9110 section 4.2.1: a recipient rejects an
    http(s) URI with an empty one, and the redirect loop's URL arithmetic
    needs it), and credentials must be ones Basic auth can carry
    (:func:`_credentials_problem`): aiohttp would send a control character,
    and fail mid-request on a colon in the user-id or a character Latin-1
    can't encode.
    """
    if not parsed.raw_host:
        raise UnfetchableURLError("the URL names no host")
    problem = _credentials_problem(parsed)
    if problem is not None:
        raise UnfetchableURLError(problem)


def _require_http_url(url: str, context: str) -> None:
    """Refuse a data-origin URL before it is fetched (SourceConnectionError).

    Producer URLs, hosted dataset URLs and GBFS endpoint URLs come from data
    (catalog payloads, GBFS documents an agency controls), so a corrupt or
    malicious source could point at a local resource (``file://``) or name a
    URL that yarl would quietly change. This is a proportionate guard, not
    SSRF prevention: producer hosts are legitimately arbitrary, and a
    stricter policy belongs to the consuming application. The message
    names the reason only; the URL, redacted, is the exception's ``url``.
    """
    try:
        url.encode("utf-8")
    except UnicodeEncodeError:
        reason = "the URL isn't UTF-8 text"
    else:
        try:
            parsed = URL(url)
            scheme = parsed.scheme.lower()
            if scheme not in _ALLOWED_URL_SCHEMES:
                reason = "unsupported URL scheme"  # the scheme is the source's text
            else:
                require_requestable(parsed)
                return
        except UnfetchableURLError as err:
            reason = str(err)
        except URL_ERRORS:
            reason = "the URL doesn't parse"
    raise SourceConnectionError(
        f"Refusing to fetch {context}: {reason}", url=redact(url)
    )
