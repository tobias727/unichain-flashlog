"""Container healthcheck: succeed if the current-hour file was written recently.

Exit 0 (healthy) when a ``flashblocks_<hour>.jsonl`` for the current UTC hour
exists and was modified within ``HEALTH_MAX_AGE_S`` seconds; exit 1 otherwise.

Note: a genuine upstream stall (no flashblocks arriving) will show as unhealthy,
which is the intended signal. With ``restart: unless-stopped`` this does not by
itself trigger a restart.
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone

from config import load_config


def main() -> int:
    """Return 0 if the current-hour file is fresh, 1 otherwise."""

    cfg = load_config()
    max_age = float(os.environ.get("HEALTH_MAX_AGE_S", "120"))
    hour = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H")
    path = os.path.join(cfg.out_dir, f"flashblocks_{hour}.jsonl")
    try:
        age = time.time() - os.path.getmtime(path)
    except OSError:
        print(f"unhealthy: {path} missing")
        return 1
    if age > max_age:
        print(f"unhealthy: {path} stale ({age:.0f}s > {max_age:.0f}s)")
        return 1
    print(f"healthy: {path} age={age:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
