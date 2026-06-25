"""Configuration loaded from environment variables.

All capture behaviour is controlled here so the collector can be tuned purely
through the environment (and therefore through ``.env`` / docker-compose).
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# (wss://sepolia-flashblocks.unichain.org/ws).
DEFAULT_WS_URL = "wss://mainnet-flashblocks.unichain.org/ws"


@dataclass(frozen=True)
class Config:
    """Immutable runtime configuration."""

    ws_url: str
    out_dir: str
    flush_every: int
    stall_s: float
    log_level: str
    heartbeat_s: float
    backoff_cap_s: float
    disk_min_free_mb: float
    retention_days: int | None


def _get_str(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _get_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return default
    return int(raw)


def _get_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return default
    return float(raw)


def _get_optional_int(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return None
    return int(raw)


def load_config() -> Config:
    """Build a :class:`Config` from the process environment."""

    return Config(
        ws_url=_get_str("WS_URL", DEFAULT_WS_URL),
        out_dir=_get_str("OUT_DIR", "/data"),
        flush_every=_get_int("FLUSH_EVERY", 50),
        stall_s=_get_float("STALL_S", 5.0),
        log_level=_get_str("LOG_LEVEL", "INFO").upper(),
        heartbeat_s=_get_float("HEARTBEAT_S", 30.0),
        backoff_cap_s=_get_float("BACKOFF_CAP_S", 30.0),
        disk_min_free_mb=_get_float("DISK_MIN_FREE_MB", 500.0),
        retention_days=_get_optional_int("RETENTION_DAYS"),
    )
