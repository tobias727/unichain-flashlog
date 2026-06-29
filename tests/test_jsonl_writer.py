"""Unit tests for the JSONL sink: envelope, rotation, gzip, and raw round-trip."""

# Tests reach into writer internals to simulate crashes and force compression.
# pylint: disable=missing-function-docstring,protected-access,import-outside-toplevel

from __future__ import annotations

import gzip
import json
import os
from datetime import datetime, timezone

import pytest

from sink.jsonl_writer import JsonlWriter


def _hour_to_ns(hour: str) -> int:
    dt = datetime.strptime(hour, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1e9)


def _read_lines(path: str) -> list[str]:
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as fh:
        return [line for line in fh.read().splitlines() if line]


def test_envelope_format(tmp_path):
    writer = JsonlWriter(str(tmp_path), flush_every=1)
    writer.write(123, 456, '{"index":0}')
    writer.close()

    [gz] = list(tmp_path.glob("*.jsonl.gz"))
    (line,) = _read_lines(str(gz))
    obj = json.loads(line)
    assert obj == {"t_wall_ns": 123, "t_mono_ns": 456, "raw": '{"index":0}'}


def test_raw_roundtrip_preserves_exact_frame(tmp_path):
    # Frame with whitespace, unicode and nested quoting that must survive verbatim.
    frame = '{"index": 1, "base": null, "note": "héllo \\"q\\" ", "arr":[1, 2 ,3]}'
    writer = JsonlWriter(str(tmp_path), flush_every=1)
    writer.write(1, 2, frame)
    writer.close()

    [gz] = list(tmp_path.glob("*.jsonl.gz"))
    (line,) = _read_lines(str(gz))
    raw = json.loads(line)["raw"]
    assert raw == frame  # exact string preserved
    assert json.loads(raw)["base"] is None  # and still valid JSON


def test_hourly_rotation_creates_separate_files(tmp_path):
    writer = JsonlWriter(str(tmp_path), flush_every=1)
    writer.write(_hour_to_ns("2026-01-01T10"), 1, '{"h":10}')
    writer.write(_hour_to_ns("2026-01-01T11"), 2, '{"h":11}')
    writer.close()
    writer._compressor.shutdown(wait=True)  # ensure background gzip finished

    names = sorted(p.name for p in tmp_path.iterdir())
    assert "flashblocks_2026-01-01T10.jsonl.gz" in names
    assert "flashblocks_2026-01-01T11.jsonl.gz" in names


def test_gzip_on_rotation_removes_raw_and_is_readable(tmp_path):
    writer = JsonlWriter(str(tmp_path), flush_every=1)
    writer.write(_hour_to_ns("2026-01-01T10"), 1, '{"a":1}')
    writer.write(_hour_to_ns("2026-01-01T11"), 2, '{"b":2}')  # triggers rotation of T10
    writer.close()  # gzips current file and waits for all background compression

    raw_t10 = tmp_path / "flashblocks_2026-01-01T10.jsonl"
    gz_t10 = tmp_path / "flashblocks_2026-01-01T10.jsonl.gz"
    assert not raw_t10.exists()  # raw removed after gzip
    assert gz_t10.exists()
    assert json.loads(_read_lines(str(gz_t10))[0])["raw"] == '{"a":1}'


def test_restart_appends_to_existing_current_hour_file(tmp_path):
    hour_ns = _hour_to_ns("2026-01-01T10")

    w1 = JsonlWriter(str(tmp_path), flush_every=1)
    w1.write(hour_ns, 1, '{"n":1}')
    # Simulate a crash: fsync + close the file handle WITHOUT gzipping it.
    w1._fsync()
    w1._fh.close()
    w1._fh = None
    w1._compressor.shutdown(wait=True)

    raw_file = tmp_path / "flashblocks_2026-01-01T10.jsonl"
    assert raw_file.exists()

    # Restart in the same hour: should append, not overwrite, and not pre-gzip it.
    import sink.jsonl_writer as mod

    real_hour_key = mod._hour_key
    mod._hour_key = lambda _ns: "2026-01-01T10"
    try:
        w2 = JsonlWriter(str(tmp_path), flush_every=1)
        assert raw_file.exists()  # current-hour file left in place for append
        w2.write(hour_ns, 2, '{"n":2}')
        w2._fsync()
        lines = _read_lines(str(raw_file))
        assert [json.loads(line)["raw"] for line in lines] == ['{"n":1}', '{"n":2}']
        w2.close()
        w2._compressor.shutdown(wait=True)
    finally:
        mod._hour_key = real_hour_key


def test_stale_file_from_crash_is_compressed_on_startup(tmp_path):
    # A leftover past-hour .jsonl (crash before rotation) must be gzipped on start.
    stale = tmp_path / "flashblocks_2020-01-01T00.jsonl"
    stale.write_text(json.dumps({"t_wall_ns": 1, "t_mono_ns": 2, "raw": "{}"}) + "\n")

    writer = JsonlWriter(str(tmp_path), flush_every=1)
    writer._compressor.shutdown(wait=True)

    assert not stale.exists()
    assert (tmp_path / "flashblocks_2020-01-01T00.jsonl.gz").exists()
    writer.close()


def test_file_prefix_names_files_per_venue(tmp_path):
    # A venue-specific prefix must appear in the rotated/gzipped filename so two
    # venues sharing a directory never collide.
    writer = JsonlWriter(str(tmp_path), flush_every=1, file_prefix="flashblocks_base_")
    writer.write(123, 456, '{"index":0}')
    writer.close()
    writer._compressor.shutdown(wait=True)

    [gz] = list(tmp_path.glob("*.jsonl.gz"))
    assert gz.name.startswith("flashblocks_base_")
    assert json.loads(_read_lines(str(gz))[0])["raw"] == '{"index":0}'


def test_retention_deletes_old_gz(tmp_path):
    old = tmp_path / "flashblocks_2020-01-01T00.jsonl.gz"
    with gzip.open(old, "wt", encoding="utf-8") as fh:
        fh.write("{}\n")
    old_time = 0  # epoch, definitely older than any retention window
    os.utime(old, (old_time, old_time))

    writer = JsonlWriter(str(tmp_path), flush_every=1, retention_days=1)
    writer.close()

    assert not old.exists()


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
