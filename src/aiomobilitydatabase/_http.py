"""What both clients put in a request, and how they send it.

One copy for the catalog client and the feeds, so the two can't drift:

- what a caller may hand over, as an allowlist (not a copy of aiohttp's
  rules): header names are RFC 9110 tokens, header values (an API key,
  Basic credentials) visible ASCII, and text that goes into a URL is UTF-8
  text -- yarl silently drops a lone surrogate, asking for something else;
- which session defaults a client refuses: an ``auth`` or a credential
  header rides on every request the session makes, to every origin,
  where a client can't withhold it;
- the options every request carries: redirects are never followed by
  aiohttp (the catalog answers 3xx as errors; the feeds follow them by
  hand), and the status mapping is the client's, whatever the session's
  ``raise_for_status``.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    import aiohttp

# RFC 9110 section 5.1: a field name is a token.
_TOKEN = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+")
# Visible ASCII, space and tab. RFC 9110 section 5.5 also allows obs-text,
# which no client sends the same way: not accepted from callers.
_VISIBLE = re.compile(r"[\t\x20-\x7e]*")
# Header names whose value is a credential: never sent beyond its origin.
CREDENTIAL_HEADERS: Final = frozenset(
    {"authorization", "cookie", "proxy-authorization"}
)
# Passed on every request either client makes.
REQUEST_OPTIONS: Final[dict[str, Any]] = {
    "allow_redirects": False,
    "raise_for_status": False,
}


def is_token(name: object) -> bool:
    """Return whether ``name`` is a header field name (an RFC 9110 token)."""
    return isinstance(name, str) and _TOKEN.fullmatch(name) is not None


def is_visible(value: object) -> bool:
    """Return whether ``value`` is visible ASCII, space or tab."""
    return isinstance(value, str) and _VISIBLE.fullmatch(value) is not None


def is_text(value: str) -> bool:
    """Return whether ``value`` is UTF-8 text (no lone surrogate)."""
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def require_text(value: str, what: str) -> str:
    """Return ``value``, or raise ValueError when it isn't UTF-8 text."""
    if not is_text(value):
        raise ValueError(f"{what} holds a lone surrogate, not text")
    return value


def require_session(session: aiohttp.ClientSession) -> None:
    """Raise ValueError for an injected session a client can't send through.

    Its default headers must be token names with visible-ASCII values, and
    it may carry no credential -- no ``auth``, no Authorization, Cookie or
    Proxy-Authorization default: those ride on every request it makes, to
    every origin (a producer's redirect target, the catalog API), where no
    client can withhold them. Pass a credential per request instead.
    """
    if getattr(session, "auth", None) is not None:
        raise ValueError("the session's auth would ride on every request")
    for name, value in session.headers.items():
        if not is_token(name) or not is_visible(value):
            raise ValueError(f"the session's header {name!r} can't be sent")
        if name.lower() in CREDENTIAL_HEADERS:
            raise ValueError(f"the session's {name} header would ride on every request")
