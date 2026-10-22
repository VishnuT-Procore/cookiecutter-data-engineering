"""Loads config/ops.yml plus environment overrides."""

from __future__ import annotations

import os
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / ".ops"
MODES = ("off", "advise", "act")
SEVERITIES = ("info", "warning", "critical")


def load(path: Path | None = None) -> dict:
    cfg = yaml.safe_load((path or ROOT / "config" / "ops.yml").read_text()) or {}
    cfg["mode"] = os.environ.get("AI_OPS_MODE") or cfg.get("mode", "advise")
    if cfg["mode"] not in MODES:
        raise ValueError(f"ops mode must be one of {MODES}, got {cfg['mode']!r}")
    cfg.setdefault("llm", {})
    cfg["llm"].update(
        gateway_url=os.environ.get("LLM_GATEWAY_URL", "").rstrip("/"),
        api_key=os.environ.get("LLM_API_KEY", ""),
        model=os.environ.get("LLM_MODEL", ""),
    )
    cfg.setdefault("thresholds", {})
    cfg.setdefault("actions", {})
    cfg.setdefault("alerts", {})
    cfg["alerts"]["webhook_url"] = os.environ.get("ALERT_WEBHOOK_URL", "")
    return cfg


def severity_rank(severity: str) -> int:
    return SEVERITIES.index(severity) if severity in SEVERITIES else 0
