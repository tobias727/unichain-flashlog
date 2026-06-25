"""Structured logging and a background monitor.

Logs go to stdout (captured by Docker). The :class:`Monitor` emits a heartbeat
with cumulative frame count and current rate, warns when the stream stalls, and
warns when free disk space on the output directory is low. No secrets are ever
logged.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import sys
import time

log = logging.getLogger("flashlog.monitor")

_TICK_S = 1.0


def setup_logging(level: str) -> None:
    """Configure root logging to stdout with a compact structured format."""

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
    )
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(getattr(logging, level, logging.INFO))


class Monitor:
    """Tracks throughput and emits heartbeat / stall / disk warnings."""

    def __init__(
        self,
        out_dir: str,
        *,
        heartbeat_s: float = 30.0,
        stall_s: float = 5.0,
        disk_min_free_mb: float = 500.0,
    ) -> None:
        self._out_dir = out_dir
        self._heartbeat_s = heartbeat_s
        self._stall_s = stall_s
        self._disk_min_free_mb = disk_min_free_mb

        self._total = 0
        self._last_frame_mono = time.monotonic()
        self._stalled = False

    def record_frame(self) -> None:
        """Note that one frame was received (called on the hot path)."""

        self._total += 1
        self._last_frame_mono = time.monotonic()
        if self._stalled:
            log.info("stream resumed after stall (total=%d)", self._total)
            self._stalled = False

    def on_disconnect(self) -> None:
        """Reset the stall clock so a reconnect window is not double-counted."""

        self._last_frame_mono = time.monotonic()

    async def run(self) -> None:
        """Run the monitor loop until cancelled."""

        last_heartbeat = time.monotonic()
        last_heartbeat_total = self._total
        while True:
            await asyncio.sleep(_TICK_S)
            now = time.monotonic()

            idle = now - self._last_frame_mono
            if idle > self._stall_s and not self._stalled:
                log.warning("stream stall: no frame for %.1fs", idle)
                self._stalled = True

            if now - last_heartbeat >= self._heartbeat_s:
                window = now - last_heartbeat
                rate = (self._total - last_heartbeat_total) / window if window else 0.0
                log.info(
                    "heartbeat total=%d rate=%.2f/s idle=%.1fs",
                    self._total,
                    rate,
                    idle,
                )
                self._check_disk()
                last_heartbeat = now
                last_heartbeat_total = self._total

    def _check_disk(self) -> None:
        try:
            free_mb = shutil.disk_usage(self._out_dir).free / (1024 * 1024)
        except OSError as exc:
            log.warning("disk check failed: %s", exc)
            return
        if free_mb < self._disk_min_free_mb:
            log.warning(
                "low disk space: %.0f MB free on %s (threshold %.0f MB)",
                free_mb,
                self._out_dir,
                self._disk_min_free_mb,
            )
