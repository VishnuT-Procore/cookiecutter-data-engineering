"""Access to config/pipeline.yml for the Dagster definitions."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def load_pipeline() -> dict:
    return yaml.safe_load((ROOT / "config" / "pipeline.yml").read_text())
