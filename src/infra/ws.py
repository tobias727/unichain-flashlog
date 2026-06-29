"""WebSocket connection manager with automatic reconnect.

Yields raw frames forever. On any disconnect it reconnects with exponential
backoff + jitter (capped). It does NOT backfill: a reconnect simply resumes the
live stream, leaving any gap visible in the captured data (there is no source to
backfill flashblocks from).
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import AsyncIterator

import brotli
import websockets
from websockets.exceptions import WebSocketException

log = logging.getLogger("flashlog.ws")

# Allow large individual frames (flashblocks with many transactions) rather than
# dropping them. Bounded so a single frame cannot exhaust memory.
_MAX_FRAME_BYTES = 64 * 1024 * 1024


class WebSocketManager:
    """Maintains a single websocket connection and re-establishes it on failure."""

    def __init__(
        self,
        url: str,
        *,
        backoff_base_s: float = 0.5,
        backoff_cap_s: float = 30.0,
        ping_interval_s: float = 20.0,
        ping_timeout_s: float = 20.0,
        subscribe_payload: str | None = None,
        on_connect=None,
        on_disconnect=None,
    ) -> None:
        self._url = url
        self._backoff_base_s = backoff_base_s
        self._backoff_cap_s = backoff_cap_s
        self._ping_interval_s = ping_interval_s
        self._ping_timeout_s = ping_timeout_s
        # JSON-RPC handshake text sent immediately after every (re)connect, e.g.
        # the eth_subscribe request for the Base provider path. ``None`` for the
        # Unichain raw_ws stream, which needs no handshake.
        self._subscribe_payload = subscribe_payload
        self._on_connect = on_connect
        self._on_disconnect = on_disconnect

    async def frames(self) -> AsyncIterator[str]:
        """Yield raw frames as ``str``, reconnecting indefinitely.

        Binary frames are decoded as UTF-8; the public Unichain endpoint streams
        text JSON, so this is normally a no-op.
        """

        backoff = self._backoff_base_s
        while True:
            try:
                async with websockets.connect(
                    self._url,
                    ping_interval=self._ping_interval_s,
                    ping_timeout=self._ping_timeout_s,
                    close_timeout=5,
                    max_size=_MAX_FRAME_BYTES,
                    max_queue=256,
                ) as ws:
                    log.info("connected ws_url=%s", self._url)
                    backoff = self._backoff_base_s  # reset after a clean connect
                    # Re-send the subscribe handshake on every (re)connect so the
                    # eth_subscribe path resubscribes after any drop.
                    if self._subscribe_payload is not None:
                        await ws.send(self._subscribe_payload)
                        log.info("sent subscribe handshake")
                    if self._on_connect is not None:
                        self._on_connect()
                    async for message in ws:
                        if isinstance(message, bytes):
                            try:
                                yield brotli.decompress(message).decode("utf-8")
                            except brotli.error:
                                try:
                                    yield message.decode("utf-8")
                                except UnicodeDecodeError:
                                    log.warning(
                                        "dropping undecodable binary frame len=%d; not brotli and not utf-8",
                                        len(message),
                                    )
                        else:
                            yield message
            # asyncio.CancelledError is a BaseException and is intentionally not
            # caught here, so a shutdown cancellation propagates out cleanly.
            except (WebSocketException, OSError) as exc:
                log.warning("ws disconnected: %s", exc)
            except Exception as exc:  # pylint: disable=broad-except
                log.warning("ws error: %s", exc)
            else:
                # Server closed the stream cleanly; treat as a disconnect.
                log.warning("ws stream ended; reconnecting")

            if self._on_disconnect is not None:
                self._on_disconnect()

            sleep_for = backoff + random.uniform(0, backoff)
            log.info("reconnecting in %.1fs", sleep_for)
            await asyncio.sleep(sleep_for)
            backoff = min(self._backoff_cap_s, backoff * 2)
