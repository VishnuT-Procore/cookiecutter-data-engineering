"""Removes secrets and bounds sizes before anything leaves the process (LLM prompt, alerts, audit)."""

from __future__ import annotations

import re

SECRET_KEY = re.compile(r"(pass(word)?|secret|token|api[_-]?key|credential|authorization|private[_-]?key)", re.I)
PATTERNS = [
    (re.compile(r"(?i)bearer\s+[a-z0-9._\-]+"), "Bearer ***"),
    (re.compile(r"://[^/\s:@]+:[^/\s@]+@"), "://***:***@"),
    (re.compile(r"\bAKIA[0-9A-Z]{16}\b"), "AKIA***"),
    (re.compile(r"(?i)((?:password|secret|token|api[_-]?key)\s*[=:]\s*)['\"]?[^\s'\",;]+"), r"\1***"),
]
MAX_STRING = 2000
MAX_ITEMS = 20


def text(value: str, limit: int = MAX_STRING) -> str:
    for pattern, replacement in PATTERNS:
        value = pattern.sub(replacement, value)
    return value if len(value) <= limit else value[:limit] + f"... [{len(value) - limit} chars truncated]"


def redact(value):
    if isinstance(value, dict):
        return {k: "***" if SECRET_KEY.search(str(k)) and isinstance(v, str) else redact(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        items = [redact(v) for v in value[:MAX_ITEMS]]
        return items + [f"... {len(value) - MAX_ITEMS} more"] if len(value) > MAX_ITEMS else items
    if isinstance(value, str):
        return text(value)
    return value
