"""Append-only JSONL audit log of every check: what was seen, who decided, what was done."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from ops.config import STATE_DIR


def write(record: dict) -> None:
    folder = STATE_DIR / "audit"
    folder.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    snapshot = json.dumps(record.get("snapshot", {}), sort_keys=True, default=str)
    record = {"at": now.isoformat(timespec="seconds"),
              "snapshot_sha256": hashlib.sha256(snapshot.encode()).hexdigest()[:16], **record}
    with open(folder / f"{now:%Y-%m-%d}.jsonl", "a") as f:
        f.write(json.dumps(record, default=str) + "\n")
