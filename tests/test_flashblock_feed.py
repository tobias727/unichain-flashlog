"""Unit tests for the eth_subscribe frame extraction (Base provider path)."""

# pylint: disable=missing-function-docstring,protected-access

from __future__ import annotations

import json

from feeds.flashblock_feed import _extract_subscription_result


def test_extracts_flashblock_from_pubsub_envelope():
    # A real flashblock arrives wrapped in the JSON-RPC pubsub notification.
    frame = {"payload_id": "0xabc", "index": 0, "base": {"block_number": "0x1"}}
    message = json.dumps(
        {
            "jsonrpc": "2.0",
            "method": "eth_subscription",
            "params": {"subscription": "0xdeadbeef", "result": frame},
        }
    )
    out = _extract_subscription_result(message)
    assert out is not None
    # Stored verbatim as a string that round-trips to the exact frame object.
    assert json.loads(out) == frame


def test_subscription_ack_is_skipped():
    # First reply to eth_subscribe is just the subscription id; not a frame.
    message = json.dumps({"jsonrpc": "2.0", "id": 1, "result": "0xdeadbeef"})
    assert _extract_subscription_result(message) is None


def test_rpc_error_is_skipped():
    # e.g. provider rate-limit / bad params -> log + skip, never crash.
    message = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "error": {"code": -32005, "message": "rate limited"}}
    )
    assert _extract_subscription_result(message) is None


def test_non_json_is_skipped():
    assert _extract_subscription_result("not json at all") is None


def test_message_without_result_is_skipped():
    message = json.dumps({"jsonrpc": "2.0", "method": "eth_subscription", "params": {}})
    assert _extract_subscription_result(message) is None
