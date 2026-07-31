"""In-repo HTTP mock: a real aiohttp TestServer with scripted responses.

Used instead of aioresponses, which is incompatible with aiohttp >=3.14.
"""

from __future__ import annotations

import json as jsonlib
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any

from aiohttp import web
from aiohttp.test_utils import TestServer


@dataclass
class RecordedRequest:
    """A single request observed by the mock server."""

    method: str
    path: str
    # NOTE: dict(request.query) keeps only the last value per repeated key.
    # Fine while clients comma-join multi-values; beware if that changes.
    query: dict[str, str]
    json: Any | None
    headers: dict[str, str]
    # The percent-ENCODED wire path, unlike ``path`` above which aiohttp
    # decodes (e.g. "%2F" -> "/"). Needed to verify a client actually quoted
    # a path segment rather than sending a raw special character; excludes
    # the query string (aiohttp's raw_path includes it when present).
    raw_path: str = ""


@dataclass
class _MockResponse:
    """A single scripted response to return for a queued request."""

    status: int = 200
    payload: Any | None = None
    body: bytes | str | None = None
    content_type: str = "application/json"


@dataclass
class MockApi:
    """Scripted responses served by a real local aiohttp server."""

    requests: list[RecordedRequest] = field(default_factory=list)
    _queues: dict[tuple[str, str], deque[_MockResponse]] = field(
        default_factory=lambda: defaultdict(deque)
    )
    server: TestServer | None = None

    def get(self, path: str, **kwargs: Any) -> None:
        """Queue a scripted GET response for the given path."""
        self._queues[("GET", path)].append(_MockResponse(**kwargs))

    def post(self, path: str, **kwargs: Any) -> None:
        """Queue a scripted POST response for the given path."""
        self._queues[("POST", path)].append(_MockResponse(**kwargs))

    def url(self, path: str = "") -> str:
        """Return the base URL of the running mock server plus an optional path."""
        assert self.server is not None
        return str(self.server.make_url(path)).rstrip("/")

    async def _handle(self, request: web.Request) -> web.Response:
        body_json: Any | None = None
        if request.can_read_body and request.content_type == "application/json":
            try:
                # ValueError covers JSONDecodeError: malformed bodies record as None.
                body_json = await request.json()
            except ValueError:
                body_json = None
        self.requests.append(
            RecordedRequest(
                method=request.method,
                path=request.path,
                query=dict(request.query),
                json=body_json,
                headers=dict(request.headers),
                raw_path=request.raw_path.split("?", 1)[0],
            )
        )
        queue = self._queues.get((request.method, request.path))
        if not queue:
            return web.Response(
                status=599,
                text=f"UNREGISTERED MOCK ROUTE: {request.method} {request.path}",
            )
        scripted = queue.popleft()
        if scripted.payload is not None:
            return web.Response(
                status=scripted.status,
                body=jsonlib.dumps(scripted.payload).encode(),
                content_type="application/json",
            )
        # Deliberate divergence from the pkg1 reference mock: no payload/body scripted
        # returns a truly empty body (204-style), not JSON `null`.
        body = scripted.body or b""
        if isinstance(body, str):
            body = body.encode()
        return web.Response(
            status=scripted.status, body=body, content_type=scripted.content_type
        )

    async def start(self) -> None:
        """Start the underlying aiohttp TestServer."""
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", self._handle)
        self.server = TestServer(app)
        await self.server.start_server()

    async def stop(self) -> None:
        """Stop the underlying aiohttp TestServer."""
        if self.server is not None:
            await self.server.close()
