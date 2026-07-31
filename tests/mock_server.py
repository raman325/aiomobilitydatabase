"""In-repo aiohttp TestServer mock, replacing aioresponses.

aiohttp 3.14 made ``ClientResponse.__init__``'s ``stream_writer`` kwarg
required; aioresponses 0.7.9 (latest on PyPI, fix unmerged upstream) can no
longer construct mocked responses under it. This module runs a real
``aiohttp.test_utils.TestServer`` on loopback instead, so it is version-proof
against the actually-installed aiohttp.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any

from aiohttp import web
from aiohttp.test_utils import TestServer

UNREGISTERED_STATUS = 599
_UNREGISTERED_BODY = "MOCK_API: no response registered for {method} {path}"


@dataclass
class _MockResponse:
    status: int = 200
    payload: Any | None = None
    body: str | None = None
    content_type: str = "application/json"


@dataclass
class RecordedRequest:
    """A single request captured by the mock server."""

    method: str
    path: str
    query: dict[str, str]
    json: Any | None


class MockApi:
    """A minimal loopback aiohttp server that replays queued responses.

    Register responses per ``(method, path)`` as a FIFO queue -- each
    registration is consumed exactly once, mirroring aioresponses' queueing
    semantics. Every request that reaches the server (matched or not) is
    recorded in ``requests``. Unregistered or exhausted routes return
    ``UNREGISTERED_STATUS`` with a loud marker body so tests fail visibly
    instead of hanging or silently mismatching.
    """

    def __init__(self) -> None:
        """Build the backing app/server; call ``start()`` to serve it."""
        self._queues: dict[tuple[str, str], deque[_MockResponse]] = defaultdict(deque)
        self.requests: list[RecordedRequest] = []
        self.server = TestServer(self._build_app())

    def get(
        self,
        path: str,
        *,
        status: int = 200,
        payload: Any | None = None,
        body: str | None = None,
        content_type: str = "application/json",
    ) -> None:
        """Queue a response for the next GET to ``path``."""
        self._register(
            "GET",
            path,
            _MockResponse(
                status=status, payload=payload, body=body, content_type=content_type
            ),
        )

    def post(
        self,
        path: str,
        *,
        status: int = 200,
        payload: Any | None = None,
        body: str | None = None,
        content_type: str = "application/json",
    ) -> None:
        """Queue a response for the next POST to ``path``."""
        self._register(
            "POST",
            path,
            _MockResponse(
                status=status, payload=payload, body=body, content_type=content_type
            ),
        )

    def _register(self, method: str, path: str, response: _MockResponse) -> None:
        self._queues[(method, path)].append(response)

    async def start(self) -> None:
        """Start the backing TestServer."""
        await self.server.start_server()

    async def stop(self) -> None:
        """Stop the backing TestServer."""
        await self.server.close()

    def _build_app(self) -> web.Application:
        application = web.Application()
        application.router.add_route("*", "/{tail:.*}", self._handle)
        return application

    async def _handle(self, request: web.Request) -> web.Response:
        """Record the request and serve the next queued response, if any."""
        json_body: Any | None = None
        if request.body_exists:
            try:
                json_body = await request.json()
            except ValueError:
                json_body = None
        self.requests.append(
            RecordedRequest(
                method=request.method,
                path=request.path,
                query=dict(request.query),
                json=json_body,
            )
        )
        queue = self._queues.get((request.method, request.path))
        if not queue:
            return web.Response(
                status=UNREGISTERED_STATUS,
                text=_UNREGISTERED_BODY.format(
                    method=request.method, path=request.path
                ),
            )
        mock_response = queue.popleft()
        if mock_response.body is not None:
            return web.Response(
                status=mock_response.status,
                text=mock_response.body,
                content_type=mock_response.content_type,
            )
        return web.json_response(mock_response.payload, status=mock_response.status)
