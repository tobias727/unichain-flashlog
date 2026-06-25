"""Unichain Flashblocks feed.

Thin, schema-agnostic wrapper over the websocket manager. It connects to the
Unichain Flashblocks (Rollup-Boost) stream and yields each RAW frame as a
``str``, before any JSON parsing. The capture path deliberately does not depend
on any field of the payload, so it keeps working even if the upstream schema
changes.
"""

from __future__ import annotations

from typing import AsyncIterator

from infra.ws import WebSocketManager


class FlashblockFeed:
    """Yields raw Flashblocks frames from the configured websocket endpoint."""

    def __init__(self, ws_url: str, **manager_kwargs) -> None:
        self._manager = WebSocketManager(ws_url, **manager_kwargs)

    async def frames(self) -> AsyncIterator[str]:
        """Yield each raw frame (str) as received, reconnecting forever."""

        async for frame in self._manager.frames():
            yield frame
