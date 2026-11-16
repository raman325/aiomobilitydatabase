"""Checks on the URLs the feeds request, and on the caller's own inputs.

Producer and catalog URLs are data, checked by :func:`_require_http_url`
before a request and by the redirect loop at every hop: an http(s) URL, UTF-8
text (yarl drops a lone surrogate and requests another URL), with a host, and
credentials RFC 7617 allows. Everything else a request can trip on is the
network boundary's (:func:`._redirects.network`): it needs no list here.

The caller's inputs are checked when a handle is made, against an allowlist
rather than a copy of aiohttp's rules (:func:`require_caller_inputs`), so a
caller's mistake is a ValueError up front, never a source error mid-request.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING

from yarl import URL

from .exceptions import SourceConnectionError

if TYPE_CHECKING:
    import aiohttp

_ALLOWED_URL_SCHEMES = frozenset({"http", "https"})
# What parsing a URL with yarl can raise: ValueError (UnicodeError among
# them), and IndexError for some malformed authorities.
URL_ERRORS = (ValueError, IndexError)
# RFC 9110 section 5.1: a field name is a token.
_FIELD_NAME = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+")
# The header values (and API key, and Basic credentials) a caller may hand
# over: visible ASCII, space and tab -- an allowlist. RFC 9110 section 5.5
# allows obs-text too, but no client library sends it the same way.
_VISIBLE = re.compile(r"[\t\x20-\x7e]*")
_CONTROL = re.compile(r"[\x00-\x1f\x7f]")  # RFC 5234 CTL


class UnfetchableURLError(ValueError):
    """Why a URL is refused, in text that is safe to show (no URL in it)."""


def require_caller_inputs(
    session: aiohttp.ClientSession,
    urls: Iterable[str] = (),
    headers: Mapping[str, str] | None = None,
    api_key: str | None = None,
) -> None:
    """Raise ValueError for a caller input outside the allowlist.

    Checked when a handle is made: every header (the session's defaults with
    ``headers``) is a token name with a visible-ASCII value, as is the
    ``api_key`` and a session ``auth``'s login and password (the login
    without a colon, RFC 7617 section 2). An Authorization header beside a
    session ``auth`` or credentials in the caller's URLs is refused too:
    one request carries one Authorization field. (A session ``auth`` beside
    URL credentials is fine: the URL's are sent.)
    """
    for name, value in (*session.headers.items(), *(headers or {}).items()):
        if not (isinstance(name, str) and _FIELD_NAME.fullmatch(name)):
            raise ValueError(f"header name {name!r} is no HTTP field name")
        if not (isinstance(value, str) and _VISIBLE.fullmatch(value)):
            raise ValueError(f"header {name!r} must be visible ASCII")
    if api_key is not None and not _VISIBLE.fullmatch(api_key):
        raise ValueError("api_key must be visible ASCII")
    auth = getattr(session, "auth", None)
    if auth is not None and not (
        _VISIBLE.fullmatch(auth.login)
        and _VISIBLE.fullmatch(auth.password)
        and ":" not in auth.login
    ):
        raise ValueError("the session's auth must be visible ASCII, the login no colon")
    names = (*session.headers, *(headers or {}))
    if any(name.lower() == "authorization" for name in names) and (
        auth is not None or any(_has_credentials(url) for url in urls)
    ):
        raise ValueError(
            "an Authorization header can't go with credentials in the URL "
            "or the session's auth"
        )


def _has_credentials(url: str) -> bool:
    """Return whether a URL carries credentials (a user, or a password alone).

    False when it doesn't parse: a malformed URL is the guard's to report.
    """
    try:
        parsed = URL(url)
    except URL_ERRORS:
        return False
    return parsed.user is not None or parsed.raw_password is not None


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
    needs it), and credentials hold no control character (RFC 7617 section
    2), which aiohttp would otherwise send.
    """
    if not parsed.raw_host:
        raise UnfetchableURLError("the URL names no host")
    if any(
        part is not None and _CONTROL.search(part)
        for part in (parsed.user, parsed.password)
    ):
        raise UnfetchableURLError("the URL's credentials hold a control character")


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
                reason = f"unsupported URL scheme {scheme!r}"
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
