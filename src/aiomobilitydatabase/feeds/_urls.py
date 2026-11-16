"""Data-origin URL guard shared by every feed handle."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping

from yarl import URL

from .exceptions import SourceConnectionError

_ALLOWED_URL_SCHEMES = frozenset({"http", "https"})
# What parsing a URL with yarl can raise: ValueError (UnicodeError among
# them, for IDNA), and IndexError for some malformed authorities, such as
# ``http://[::1]:@``.
URL_ERRORS = (ValueError, IndexError)


# RFC 9110 section 5.1: a field name is a token; section 5.5: a field value
# holds no control character but HTAB (aiohttp refuses the same ones).
FIELD_NAME = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+")
_FIELD_VALUE_FORBIDDEN = re.compile(r"[\x00-\x08\x0a-\x1f\x7f]")
_CONTROL = re.compile(r"[\x00-\x1f\x7f]")  # RFC 5234 CTL


def require_header_fields(
    headers: Mapping[str, str] | None, api_key: str | None = None
) -> None:
    """Raise ValueError for a caller's header (or API key) HTTP can't carry.

    Checked when a handle is made, so the mistake is reported as the
    caller's: a ValueError during a request is then the producer's doing
    (see :func:`._redirects.open_response`), never a header passed here.
    """
    for name, value in (headers or {}).items():
        if not isinstance(name, str) or not FIELD_NAME.fullmatch(name):
            raise ValueError(f"header name {name!r} is no HTTP field name")
        if not _carriable(value):
            raise ValueError(f"header {name!r} has a value HTTP can't carry")
    if api_key is not None and not _carriable(api_key):
        raise ValueError("api_key holds a character HTTP can't carry")


def _carriable(value: object) -> bool:
    """Return whether a header value (or API key) reaches the server as is.

    No control character but HTAB, and UTF-8 encodable: a lone surrogate
    has no bytes, so aiohttp (and yarl, in a query) either drops it --
    sending another value, another key -- or raises mid-request.
    """
    if not isinstance(value, str) or _FIELD_VALUE_FORBIDDEN.search(value):
        return False
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def _has_credentials(url: str) -> bool:
    """Return whether a URL carries credentials.

    False when it doesn't parse: a malformed URL is the guard's to report,
    as SourceConnectionError.
    """
    try:
        return URL(url).user is not None
    except URL_ERRORS:
        return False


def require_compatible_credentials(
    urls: Iterable[str], headers: Mapping[str, str] | None, session_auth: object
) -> None:
    """Raise ValueError when a caller's credentials can't all be sent.

    aiohttp sends one Authorization field, so it refuses (mid-request, as a
    ValueError) an Authorization header beside a session ``auth`` or
    credentials in the URL, and a session ``auth`` beside URL credentials.
    Checked for the caller's own URLs when a handle is made, so the mistake
    is reported as the caller's.
    """
    has_header = any(name.lower() == "authorization" for name in headers or {})
    in_url = any(_has_credentials(url) for url in urls)
    if has_header and (in_url or session_auth is not None):
        raise ValueError(
            "an Authorization header can't go with credentials in the URL "
            "or the session's auth"
        )
    if in_url and session_auth is not None:
        raise ValueError("credentials in the URL can't go with the session's auth")


def require_requestable(parsed: URL) -> None:
    """Raise ValueError for a URL these checks find no request can go to.

    No host at all (``https:``, ``http:/x``): RFC 9110 section 4.2.1 has a
    recipient reject an http(s) URI with an empty host as invalid -- and
    hand-followed redirects would trip over it in yarl's URL arithmetic.
    A host the resolver can't IDNA-encode (an empty label, ``a..b``, or one
    past 63 octets) fails there, which aiohttp raises mid-request as a bare
    UnicodeError (a ValueError too). yarl has already punycoded any
    non-ASCII name, so only those limits are left to check. A user name
    holding a colon (``a%3Ab@``) can't be sent as Basic credentials
    (RFC 7617 section 2: the user-id has none), which aiohttp raises as a
    bare ValueError while building the request.
    """
    if not parsed.raw_host:
        raise ValueError("the URL names no host")
    parsed.raw_host.encode("idna")  # as socket.getaddrinfo encodes it
    if parsed.user is not None and ":" in parsed.user:
        raise ValueError("the URL's user name holds a colon")
    # RFC 7617 section 2: neither user-id nor password holds a control
    # character. (One that isn't Latin-1 fails in aiohttp as UnicodeError,
    # which open_response reports as the source's.)
    if any(
        part is not None and _CONTROL.search(part)
        for part in (parsed.user, parsed.password)
    ):
        raise ValueError("the URL's credentials hold a control character")


def _require_http_url(url: str, context: str) -> None:
    """Reject a data-origin URL whose scheme isn't http/https before fetch.

    Producer URLs, hosted dataset URLs, and GBFS endpoint URLs all
    originate from data (catalog payloads, GBFS documents an agency
    controls) rather than from a caller-supplied parameter, so a malicious
    or corrupt source could point at a local resource (``file://``) or
    another unintended scheme. This is a proportionate library-level guard,
    not full SSRF prevention -- producer hosts are legitimately arbitrary,
    so host/IP validation is out of scope; a stricter policy belongs in the
    consuming application (e.g. the HA integration) if needed.

    A URL no request can be made to is refused the same way, rather than
    escaping later as a bare exception: one yarl can't parse (a port past
    65535, an unclosed IPv6 bracket; yarl raises IndexError for some, such
    as ``http://[::1]:@``), and a host the resolver can't IDNA-encode (an
    empty label, ``a..b``, or one past 63 octets), which aiohttp raises as
    UnicodeError mid-request.
    """
    try:
        parsed = URL(url)
        scheme = parsed.scheme.lower()
        if scheme in _ALLOWED_URL_SCHEMES:  # the scheme refusal comes first
            require_requestable(parsed)
    except URL_ERRORS as err:
        raise SourceConnectionError(
            f"Refusing to fetch {context}: malformed URL ({url!r}): {err}"
        ) from err
    if scheme not in _ALLOWED_URL_SCHEMES:
        raise SourceConnectionError(
            f"Refusing to fetch {context}: unsupported URL scheme {scheme!r} ({url})"
        )
