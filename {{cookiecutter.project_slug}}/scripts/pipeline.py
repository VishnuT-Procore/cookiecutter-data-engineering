#!/usr/bin/env python3
"""The pipeline's command-line contract. Make targets and every orchestrator plugin call this,
so swapping the orchestrator or the target never touches pipeline logic.

    python scripts/pipeline.py ingest deploy [--replace]   # start the Kafka -> Iceberg landing job
    python scripts/pipeline.py ingest status               # exit 0 if the landing job is running
    python scripts/pipeline.py ingest cancel
    python scripts/pipeline.py transform [dbt args...]     # dbt build: bronze -> silver -> gold + tests
    python scripts/pipeline.py check                       # generated files up to date + dbt parse
    python scripts/pipeline.py ops check|explain|test-alert  # AI ops: diagnose, alert, self-heal (docs/ai_ops.md)
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DBT = ["dbt", "--no-use-colors"]
DBT_ARGS = ["--project-dir", str(ROOT / "dbt"), "--profiles-dir", str(ROOT / "dbt")]


def run(cmd: list[str]) -> int:
    return subprocess.run(cmd, cwd=ROOT).returncode


def main(argv: list[str]) -> int:
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 0
    command, rest = argv[0], argv[1:]
    if command == "ingest":
        return run([sys.executable, str(ROOT / "ingestion" / "landing.py"), *rest])
    if command == "transform":
        return run([*DBT, "build", *DBT_ARGS, *rest])
    if command == "ops":
        return run([sys.executable, "-m", "ops", *rest])
    if command == "check":
        return run([sys.executable, str(ROOT / "scripts" / "generate.py"), "--check"]) or run([*DBT, "parse", *DBT_ARGS])
    print(f"unknown command {command!r}\n{__doc__}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
