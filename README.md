# unichain-flashlog

A small, robust 24/7 collector that captures the **Unichain + Base Flashblocks**
WebSocket stream and persists as gzipped JSONL.

> Part of a sub-block-resolution dataset for studying inter-flashblock arbitrage
> and LVR. This repo is the Flashblocks capture half; market-data capture lives
> alongside it.

> **Two connection models, one sink.** Unichain is collected from its public
> sequencer stream directly (`raw_ws`, brotli-compressed). Base has no public
> WebSocket for apps, so it is collected through a node provider via
> `eth_subscribe("newFlashblocks")` (`eth_subscribe`, plain JSON). See
> [Capturing Base flashblocks](#capturing-base-flashblocks).

## What this captures and why

Unichain produces a ~200 ms "flashblock" preconfirmation stream on top of its
1-second blocks (via Flashbots [Rollup-Boost](https://writings.flashbots.net/introducing-rollup-boost)).
This sub-block data is **real-time only** — Dune, archive nodes, and standard RPC
only expose *block-level* granularity, so flashblock-level history exists nowhere
unless you record it as it happens. This collector does exactly that: it
subscribes to the live stream and appends each raw WebSocket frame to disk,
**untouched**.

The captured form is sacred: **no parsing, no normalization, no dedup, no
reconciliation at capture time.** Each frame is stored as the exact string the
server sent. All of that is an analysis-time concern, done later from the raw
files. The capture path is **schema-agnostic** — it does not read any field of
the payload, so it keeps working even if Unichain changes the flashblock schema.

## Record schema

One JSON object per line (JSONL). Files are hourly and gzipped, prefixed with the
venue: `flashblocks_<venue>_YYYY-MM-DDTHH.jsonl.gz` (UTC), e.g.
`flashblocks_base_2026-06-29T14.jsonl.gz`. Each venue writes into its own
`/data/<venue>` subdir.

```json
{"t_wall_ns": 1750861457123456789, "t_mono_ns": 99887766554433, "raw": "<verbatim frame string>"}
```

| Field        | Meaning |
|--------------|---------|
| `t_wall_ns`  | `time.time_ns()` at the instant of receipt (wall clock, UTC epoch ns). |
| `t_mono_ns`  | `time.perf_counter_ns()` at receipt — monotonic, for precise inter-frame deltas (immune to clock adjustments). |
| `raw`        | The frame **as a string**, not re-serialized, so the exact bytes round-trip. |

Round-trip the original flashblock payload with a double parse:

```python
import json
obj   = json.loads(line)            # the envelope
frame = json.loads(obj["raw"])      # the original flashblock object
frame["index"], frame.get("metadata", {}).get("block_number")
```

A flashblock frame looks like `{"payload_id", "index", "base", "diff",
"metadata"}`, where `base` is present only on `index: 0` of each block
(`base.block_number` is a hex string) and `metadata.block_number` is an integer.
The collector does not depend on any of this.

## Run it (Docker Compose, from inside WSL2)

> Run everything from a **WSL** shell (Ubuntu), not PowerShell, so the bind mount
> lands on the Linux ext4 filesystem.

```bash
cp .env.example .env              # then set BASE_WS_URL to your provider WSS endpoint
mkdir -p ~/flashtape/data/base ~/flashtape/data/unichain   # host dirs on WSL ext4
docker compose up -d --build      # starts flashlog-base and flashlog-unichain
docker compose logs -f            # watch heartbeats / reconnects (both services)
```

You should see a growing `flashblocks_base_<hour>.jsonl` appear in
`~/flashtape/data/base` (and `flashblocks_unichain_<hour>.jsonl` in
`~/flashtape/data/unichain`), and on each hour boundary the previous file becomes
`…​.jsonl.gz`. To run only one venue, e.g. Base:
`docker compose up -d flashlog-base`.

Stop gracefully (flush + fsync + gzip current file):

```bash
docker compose down           # or: docker compose stop
```

### Where the data lands

- Inside each container: `/data/<venue>` (`/data/base`, `/data/unichain`).
- On the host: `~/flashtape/data/<venue>` on the **WSL2 ext4** filesystem (the
  compose mounts are `${HOME}/flashtape/data/base:/data/base` and
  `${HOME}/flashtape/data/unichain:/data/unichain`).
- **Do not** point this at `/mnt/c/...` — the Windows 9P bridge is slow and has
  file-locking quirks that can corrupt append/rotate.
- Reach the files from Windows via the UNC path
  `\\wsl$\Ubuntu\home\<you>\flashtape\data` (Explorer or
  `\\wsl.localhost\Ubuntu\...`).

### Permissions note

The container runs as a non-root user with **uid/gid 1000**, which matches the
default first WSL Ubuntu user, so it can write to your WSL home directory. If
`id -u` in WSL is not 1000, edit the `useradd`/`groupadd` uids in the
`Dockerfile` to match (or `chown` the data dir accordingly).

## Configuration (env)

Per-venue identity (`VENUE`, `CONNECTION_MODE`, `WS_URL`, `OUT_DIR`) is set per
service in `docker-compose.yml`; the WS URLs (incl. the Base provider key) come
from `.env` (see `.env.example`). The rest are optional shared tuning.

| Var | Default | Purpose |
|-----|---------|---------|
| `VENUE` | `unichain` | Names the output files (`flashblocks_<venue>_<hour>.jsonl`). Set per service. |
| `CONNECTION_MODE` | `raw_ws` | `raw_ws` = direct sequencer stream, brotli (Unichain). `eth_subscribe` = provider WS + `newFlashblocks` (Base). |
| `WS_URL` | `wss://mainnet-flashblocks.unichain.org/ws` | Flashblocks endpoint. For Base, a provider WSS (set via `BASE_WS_URL`). |
| `OUT_DIR` | `/data` | Output dir inside the container (per venue, e.g. `/data/base`). |
| `FLUSH_EVERY` | `50` | `fsync` to disk every N records (and on rotation/shutdown). |
| `STALL_S` | `5` | Warn if no frame arrives for this many seconds. |
| `HEARTBEAT_S` | `30` | Heartbeat log interval (cumulative count + rate). |
| `BACKOFF_CAP_S` | `30` | Reconnect backoff cap (exponential + jitter). |
| `DISK_MIN_FREE_MB` | `500` | Warn when free space on `OUT_DIR` drops below this. |
| `LOG_LEVEL` | `INFO` | `DEBUG`/`INFO`/`WARNING`/`ERROR`. |
| `RETENTION_DAYS` | *(unset)* | If set, delete `*.jsonl.gz` older than N days. Unset = keep everything. |

## Capturing Base flashblocks

Base needs a **node provider's WebSocket endpoint** — there is no usable public
one. The public Base RPC endpoints (`mainnet.base.org` / `sepolia.base.org`) are
**HTTP-only and expose no WebSocket**, and the raw sequencer flashblocks stream
(`wss://mainnet.flashblocks.base.org/ws`) is documented as *node-operators only,
not for applications*. So we collect Base through a Flashblocks-aware provider
that supports `eth_subscribe("newFlashblocks")` — get a WSS key from **Dwellir,
Alchemy, or QuickNode** (any Base-mainnet plan that advertises Flashblocks), and
put the full `wss://…` URL in `BASE_WS_URL` in your `.env`. On connect the
collector sends `{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newFlashblocks"]}`,
captures each notification's `params.result` (the Rollup-Boost frame) verbatim,
and re-subscribes automatically after any reconnect. (Unichain, by contrast, has
a public sequencer stream and needs no key.)

## Robustness / how it survives kills

- **WS disconnects:** automatic reconnect with exponential backoff + jitter
  (capped ~30 s). WebSocket keepalive pings detect dead links (e.g. after laptop
  sleep). Reconnects **do not backfill** — gaps are left visible in the data,
  because there is no source to backfill flashblocks from.
- **`docker stop` / SIGTERM / SIGINT:** handled gracefully — flush, fsync, gzip
  the current file, exit clean (`stop_grace_period: 30s`).
- **Hard kill / reboot / WSL shutdown:** the JSONL is line-oriented and
  `fsync`-ed periodically, so at worst a single partial trailing line is lost;
  readers simply skip an unparsable last line. On restart the collector
  **appends** to the current-hour file if it exists, and **gzips any leftover
  past-hour `.jsonl`** files from the crash. Compression is atomic
  (temp file + `os.replace`), so a `.gz` is never half-written.
- **Memory:** the stream is written frame-by-frame; nothing buffers the whole
  session. Hourly gzip runs on a background thread so capture never stalls.
- **`restart: unless-stopped`:** the container comes back after crashes and on
  Docker/laptop restart, but stays down if you deliberately `docker compose
  stop`.

A container `healthcheck` reports unhealthy if the current-hour file hasn't been
written within ~120 s (also surfaces a genuine upstream stall). With
`restart: unless-stopped` this is informational and does not by itself force a
restart.

## Running 24/7 on a laptop

- **Disable sleep/hibernate** (at minimum on AC): Windows → Settings → System →
  Power → Screen and sleep → "When plugged in, put my device to sleep" = Never.
  Sleep suspends WSL and the stream; the collector reconnects on wake but you
  lose the sleep window.
- **Start Docker Desktop on login** (Docker Desktop → Settings → General → Start
  Docker Desktop when you log in) and enable WSL integration for your distro, so
  `restart: unless-stopped` actually brings the container back after a reboot.
- Keep the laptop plugged in; check `docker compose logs --since 1h` occasionally
  for reconnect/stall warnings.

## Analysis quickstart (DuckDB)

The gzipped JSONL files are directly queryable later — e.g. extract the
flashblock `index` and `base.block_number` across a day:

```sql
-- DuckDB reads .jsonl.gz directly and auto-detects the envelope columns.
SELECT
    t_wall_ns,
    json_extract(raw, '$.index')                    AS flashblock_index,
    json_extract_string(raw, '$.base.block_number') AS base_block_number_hex,
    json_extract(raw, '$.metadata.block_number')    AS block_number
FROM read_json_auto('~/flashtape/data/base/flashblocks_base_*.jsonl.gz')
ORDER BY t_wall_ns
LIMIT 20;
```

Here `raw` comes back as a JSON string, so the `json_extract*` functions parse it
on the fly. `base` is null except on `index = 0`. To decode the hex block number:

```sql
SELECT
    CAST(json_extract(raw, '$.index') AS BIGINT) AS flashblock_index,
    from_hex(replace(json_extract_string(raw, '$.base.block_number'), '0x', '')) AS base_block_bytes
FROM read_json_auto('~/flashtape/data/base/flashblocks_base_*.jsonl.gz')
WHERE json_extract(raw, '$.index') = 0;
```

## Development

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt black pylint pytest
pytest          # unit tests for the sink
black src tests
pylint src tests
```

Layout:

```
src/
  config.py                 # env-driven configuration
  main.py                   # wire feed -> sink, run forever, handle signals
  healthcheck.py            # container healthcheck
  feeds/flashblock_feed.py  # schema-agnostic raw-frame source
  infra/ws.py               # reconnecting websocket manager (backoff + jitter)
  infra/monitoring.py       # stdout logging, heartbeat, stall + disk warnings
  sink/jsonl_writer.py      # append-only, hourly-rotating, gzip, fsync
tests/
  test_jsonl_writer.py
```
