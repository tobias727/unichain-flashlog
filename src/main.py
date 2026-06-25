"""Entry point: wire the Flashblocks feed to the JSONL sink and run forever.

Captures both timestamps the instant a frame is received (before any other
work), writes it verbatim, and shuts down gracefully on SIGTERM/SIGINT so
``docker stop`` flushes, fsyncs, and gzips the current file cleanly.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time

from config import load_config
from feeds.flashblock_feed import FlashblockFeed
from infra.monitoring import Monitor, setup_logging
from sink.jsonl_writer import JsonlWriter

log = logging.getLogger("flashlog.main")


async def _consume(feed: FlashblockFeed, writer: JsonlWriter, monitor: Monitor) -> None:
    async for frame in feed.frames():
        # Timestamp at the moment of receipt, before any other work.
        t_wall_ns = time.time_ns()
        t_mono_ns = time.perf_counter_ns()
        writer.write(t_wall_ns, t_mono_ns, frame)
        monitor.record_frame()


async def run() -> None:
    """Build the pipeline and run until a shutdown signal arrives."""

    cfg = load_config()
    setup_logging(cfg.log_level)
    log.info(
        "starting flashlog ws_url=%s out_dir=%s flush_every=%d",
        cfg.ws_url,
        cfg.out_dir,
        cfg.flush_every,
    )

    writer = JsonlWriter(
        cfg.out_dir,
        flush_every=cfg.flush_every,
        retention_days=cfg.retention_days,
    )
    monitor = Monitor(
        cfg.out_dir,
        heartbeat_s=cfg.heartbeat_s,
        stall_s=cfg.stall_s,
        disk_min_free_mb=cfg.disk_min_free_mb,
    )
    feed = FlashblockFeed(
        cfg.ws_url,
        backoff_cap_s=cfg.backoff_cap_s,
        on_disconnect=monitor.on_disconnect,
    )

    monitor_task = asyncio.create_task(monitor.run(), name="monitor")
    consume_task = asyncio.create_task(_consume(feed, writer, monitor), name="consume")

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, consume_task.cancel)
        except NotImplementedError:  # pragma: no cover - non-unix
            pass

    try:
        await consume_task
    except asyncio.CancelledError:
        log.info("shutdown signal received")
    finally:
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
        writer.close()
        log.info("shutdown complete")


def main() -> None:
    """Synchronous wrapper for ``python -m main``."""

    asyncio.run(run())


if __name__ == "__main__":
    main()
