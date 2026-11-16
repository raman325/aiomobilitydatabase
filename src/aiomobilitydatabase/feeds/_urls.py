"""Data-origin URL guard shared by every feed handle."""

from __future__ import annotations

from yarl import URL

from .exceptions import SourceConnectionError

_ALLOWED_URL_SCHEMES = frozenset({"http", "https"})
# What parsing a URL with yarl can raise: ValueError (UnicodeError among
# them, for IDNA), and IndexError for some malformed authorities, such as
# ``http://[::1]:@``.
URL_ERRORS = (ValueError, IndexError)


def require_resolvable_host(parsed: URL) -> None:
    """Raise UnicodeError when the resolver can't IDNA-encode the host.

    An empty label (``a..b``) or one past 63 octets fails there, which
    aiohttp raises mid-request as a bare UnicodeError. yarl has already
    punycoded any non-ASCII name, so only those limits are left to check.
    """
    if parsed.raw_host:
        parsed.raw_host.encode("idna")  # as socket.getaddrinfo encodes it


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
        require_resolvable_host(parsed)
        scheme = parsed.scheme.lower()
    except URL_ERRORS as err:
        raise SourceConnectionError(
            f"Refusing to fetch {context}: malformed URL ({url!r}): {err}"
        ) from err
    if scheme not in _ALLOWED_URL_SCHEMES:
        raise SourceConnectionError(
            f"Refusing to fetch {context}: unsupported URL scheme {scheme!r} ({url})"
        )
