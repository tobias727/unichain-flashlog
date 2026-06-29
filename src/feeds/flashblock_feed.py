"""Flashblocks feed (multi-venue).

Thin, schema-agnostic wrapper over the websocket manager. It yields each RAW
flashblock frame as a ``str``, before any meaningful JSON parsing. The capture
path deliberately does not depend on any field of the flashblock payload, so it
keeps working even if the upstream schema changes.

Two connection modes share the same downstream sink:

* ``raw_ws`` — Unichain-style direct sequencer stream. Frames arrive as
  brotli-compressed binary (decompressed by the websocket manager) and are
  yielded verbatim. No handshake.
* ``eth_subscribe`` — Base-style stream through a node provider's WebSocket.
  On connect we send an ``eth_subscribe(newFlashblocks)`` JSON-RPC request, then
  for each notification we extract ``params.result`` (the flashblock object) and
  yield it. The provider wraps every flashblock in the JSON-RPC pubsub envelope,
  so the subscription ack and any keepalives are skipped here, not parsed
  further.
"""

from __future__ import annotations

import json
import logging
from typing import AsyncIterator

from infra.ws import WebSocketManager

log = logging.getLogger("flashlog.feed")

# eth_subscribe handshake for the Base provider path (CONNECTION_MODE=eth_subscribe).
_SUBSCRIBE_REQUEST = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "eth_subscribe",
    "params": ["newFlashblocks"],
}


class FlashblockFeed:
    """Yields raw Flashblocks frames from the configured websocket endpoint."""

    def __init__(self, ws_url: str, *, connection_mode: str = "raw_ws", **manager_kwargs) -> None:
        self._mode = connection_mode
        subscribe_payload = None
        if connection_mode == "eth_subscribe":
            subscribe_payload = json.dumps(_SUBSCRIBE_REQUEST)
        elif connection_mode != "raw_ws":
            raise ValueError(f"unknown connection_mode {connection_mode!r}")
        self._manager = WebSocketManager(
            ws_url, subscribe_payload=subscribe_payload, **manager_kwargs
        )

    async def frames(self) -> AsyncIterator[str]:
        """Yield each raw flashblock frame (str) as received, reconnecting forever."""

        if self._mode == "eth_subscribe":
            async for message in self._manager.frames():
                frame = _extract_subscription_result(message)
                if frame is not None:
                    yield frame
        else:
            async for frame in self._manager.frames():
                yield frame


def _extract_subscription_result(message: str) -> str | None:
    """Pull the raw flashblock object out of one JSON-RPC pubsub message.

    Returns the ``params.result`` object re-serialized as compact JSON (a faithful
    round-trip of the received object), or ``None`` for non-flashblock traffic
    (the subscription-id ack, keepalives, or errors) which is logged and skipped.
    The flashblock object itself is not normalized beyond extraction.
    """

    try:
        obj = json.loads(message)
    except (ValueError, TypeError):
        log.warning("eth_subscribe: dropping non-JSON message len=%d", len(message))
        return None

    if not isinstance(obj, dict):
        return None

    # JSON-RPC error (e.g. provider rate-limit / bad subscription). Log it; a real
    # connection-level rate limit closes the socket and the manager backs off.
    if obj.get("error") is not None:
        log.warning("eth_subscribe: rpc error: %s", obj["error"])
        return None

    # Subscription ack: {"jsonrpc","id","result":"0x.."} — record id, then skip.
    if "method" not in obj and "result" in obj:
        log.info("eth_subscribe: subscription established id=%s", obj["result"])
        return None

    params = obj.get("params")
    if not isinstance(params, dict) or "result" not in params:
        return None

    return json.dumps(params["result"], ensure_ascii=False, separators=(",", ":"))
