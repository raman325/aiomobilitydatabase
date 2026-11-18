"""The session lifecycle the catalog handles share."""

from __future__ import annotations

from types import TracebackType
from typing import Self

import aiohttp


class OwnedSession:
    """An injected session, or one made on first use and closed with the handle.

    A subclass's ``__init__`` sets ``_session`` (the injected session, or
    None) and ``_owns_session`` (True when none was injected). An injected
    session is never closed here: its owner closes it.
    """

    _session: aiohttp.ClientSession | None
    _owns_session: bool

    async def __aenter__(self) -> Self:
        """Enter the async context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Exit the async context manager, closing owned resources."""
        await self.close()

    async def close(self) -> None:
        """Close the underlying session if this handle owns it. Idempotent."""
        if (
            self._owns_session
            and self._session is not None
            and not self._session.closed
        ):
            await self._session.close()
            self._session = None

    def _get_session(self) -> aiohttp.ClientSession:
        """Return the session, lazily creating an owned one if needed."""
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session
