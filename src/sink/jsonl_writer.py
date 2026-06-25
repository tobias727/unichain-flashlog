"""Append-only JSONL writer for raw frames.

Design goals: crash-safe by construction and never lose already-flushed data.

* One line per frame: ``{"t_wall_ns": ..., "t_mono_ns": ..., "raw": "<frame>"}``.
* ``raw`` is stored verbatim as a STRING (the frame is not re-parsed or
  re-serialized) so the exact bytes round-trip:
  ``json.loads(json.loads(line)["raw"])``.
* Files rotate hourly (UTC): ``flashblocks_YYYY-MM-DDTHH.jsonl``.
* On rotation the previous file is gzipped (``.jsonl`` -> ``.jsonl.gz``) and the
  raw file removed. Compression runs on a background thread so capture never
  stalls.
* Line-buffered with periodic ``fsync`` (every ``flush_every`` records and on
  rotation), so a crash can at worst leave a partial trailing line, which is
  recoverable by readers that skip an unparsable last line.
* On startup it appends to the current-hour file if it already exists, and
  gzips any stale ``.jsonl`` files left behind by a previous crash.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import shutil
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("flashlog.sink")

_FILE_PREFIX = "flashblocks_"
_HOUR_FMT = "%Y-%m-%dT%H"


class JsonlWriter:
    """Hourly-rotating, fsync-ing, append-only JSONL writer."""

    def __init__(
        self,
        out_dir: str,
        *,
        flush_every: int = 50,
        retention_days: Optional[int] = None,
    ) -> None:
        self._out_dir = out_dir
        self._flush_every = max(1, flush_every)
        self._retention_days = retention_days

        os.makedirs(self._out_dir, exist_ok=True)

        self._hour: Optional[str] = None
        self._fh = None
        self._since_fsync = 0
        self._compressor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="gzip")
        self._pending: list[Future] = []

        # Recover from any prior crash before opening the live file.
        self._compress_stale_files()
        self._apply_retention()

    # -- public API ---------------------------------------------------------

    def write(self, t_wall_ns: int, t_mono_ns: int, raw: str) -> None:
        """Append one frame envelope, rotating and fsync-ing as needed."""

        hour = _hour_key(t_wall_ns)
        if hour != self._hour:
            self._rotate_to(hour)

        line = json.dumps(
            {"t_wall_ns": t_wall_ns, "t_mono_ns": t_mono_ns, "raw": raw},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        self._fh.write(line)
        self._fh.write("\n")

        self._since_fsync += 1
        if self._since_fsync >= self._flush_every:
            self._fsync()

    def close(self) -> None:
        """Flush, fsync, gzip the current file, and shut down cleanly."""

        if self._fh is not None:
            self._fsync()
            path = self._fh.name
            self._fh.close()
            self._fh = None
            self._hour = None
            self._compress_file(path)
        self._compressor.shutdown(wait=True)

    # -- rotation / fsync ---------------------------------------------------

    def _rotate_to(self, hour: str) -> None:
        old_path = None
        if self._fh is not None:
            self._fsync()
            old_path = self._fh.name
            self._fh.close()
            self._fh = None

        path = os.path.join(self._out_dir, f"{_FILE_PREFIX}{hour}.jsonl")
        # Mode "a": resume an existing current-hour file after a restart.
        self._fh = open(path, "a", encoding="utf-8")  # pylint: disable=consider-using-with
        self._hour = hour
        self._since_fsync = 0
        log.info("opened %s", path)

        if old_path is not None:
            self._compress_file(old_path)
            self._apply_retention()

    def _fsync(self) -> None:
        if self._fh is None:
            return
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._since_fsync = 0

    # -- compression --------------------------------------------------------

    def _compress_file(self, path: str) -> None:
        """Submit a closed .jsonl file for background gzip compression."""

        self._pending = [f for f in self._pending if not f.done()]
        future = self._compressor.submit(_gzip_in_place, path, self._out_dir)
        self._pending.append(future)

    def _compress_stale_files(self) -> None:
        """Gzip any leftover past-hour .jsonl files from an earlier crash.

        The current-hour file (if any) is left alone so live writes append to it.
        """

        current_hour = _hour_key(time.time_ns())
        for name in os.listdir(self._out_dir):
            if not name.startswith(_FILE_PREFIX) or not name.endswith(".jsonl"):
                continue
            if name == f"{_FILE_PREFIX}{current_hour}.jsonl":
                continue
            self._compress_file(os.path.join(self._out_dir, name))

    # -- retention ----------------------------------------------------------

    def _apply_retention(self) -> None:
        if self._retention_days is None:
            return
        cutoff = time.time() - self._retention_days * 86400
        for name in os.listdir(self._out_dir):
            if not name.startswith(_FILE_PREFIX) or not name.endswith(".jsonl.gz"):
                continue
            path = os.path.join(self._out_dir, name)
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
                    log.info("retention: removed %s", name)
            except OSError as exc:
                log.warning("retention: could not remove %s: %s", name, exc)


def _hour_key(t_wall_ns: int) -> str:
    return datetime.fromtimestamp(t_wall_ns / 1e9, tz=timezone.utc).strftime(_HOUR_FMT)


def _gzip_in_place(path: str, out_dir: str) -> None:
    """Compress ``path`` to ``path + .gz`` atomically, then remove ``path``.

    Writes to a temporary file and ``os.replace``s it into place so a crash
    mid-compression never yields a truncated ``.gz``; the source ``.jsonl``
    survives and will be retried on the next startup.
    """

    if not os.path.exists(path):
        return
    final = path + ".gz"
    tmp = final + ".tmp"
    try:
        with open(path, "rb") as src, gzip.open(tmp, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
            dst.flush()
            os.fsync(dst.fileno())
        os.replace(tmp, final)
        _fsync_dir(out_dir)
        os.remove(path)
        log.info("compressed %s -> %s", os.path.basename(path), os.path.basename(final))
    except OSError as exc:
        log.warning("gzip failed for %s: %s", path, exc)
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def _fsync_dir(path: str) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass
