#!/usr/bin/env python3
"""Validate config/pipeline.yml and (re)generate the target-specific files derived from it.

    python scripts/generate.py           # write generated files, remove stale ones
    python scripts/generate.py --check   # exit 1 if generated files are out of date (CI)

The target plugin provides ``scripts/plugin_generate.py`` with:
    MANAGED_GLOBS: list[str]                 globs (relative to the project root) owned by the generator
    render(pipeline: dict) -> dict[str, str] relative path -> file content
"""

from __future__ import annotations

import argparse
import importlib.util
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "pipeline.yml"

IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
COLUMN_TYPE = re.compile(
    r"^(string|integer|bigint|double|boolean|date|timestamp|timestamp_ms|timestamp_us|decimal\(\d+,\s*\d+\))$"
)
PAYLOAD_FORMATS = ("debezium", "json")
# Columns added by bronze; entity columns must not collide with them.
METADATA_COLUMNS = (
    "_op",
    "_is_deleted",
    "_source_ts_ms",
    "_kafka_topic",
    "_kafka_partition",
    "_kafka_offset",
    "_kafka_timestamp",
    "_ingested_at",
)


class ConfigError(ValueError):
    pass


def load_pipeline(path: Path = CONFIG_PATH) -> dict:
    """Load and validate pipeline.yml, returning a normalized dict."""
    with open(path) as f:
        cfg = yaml.safe_load(f) or {}

    errors: list[str] = []
    for key in ("project", "target", "payload_format", "raw_schema", "entities"):
        if key not in cfg:
            errors.append(f"missing top-level key '{key}'")
    if errors:
        raise ConfigError("; ".join(errors))

    if cfg["payload_format"] not in PAYLOAD_FORMATS:
        errors.append(f"payload_format must be one of {PAYLOAD_FORMATS}")
    if not IDENTIFIER.match(str(cfg["raw_schema"])):
        errors.append(f"raw_schema '{cfg['raw_schema']}' is not a valid identifier")
    if not cfg["entities"]:
        errors.append("at least one entity is required")

    names, topics = set(), set()
    for i, entity in enumerate(cfg["entities"] or []):
        where = f"entities[{i}]"
        name = entity.get("name")
        if not name or not IDENTIFIER.match(name):
            errors.append(f"{where}: name '{name}' is not a valid identifier")
            continue
        where = f"entity '{name}'"
        if name in names:
            errors.append(f"{where}: duplicate name")
        names.add(name)
        topic = entity.get("topic")
        if not topic:
            errors.append(f"{where}: topic is required")
        elif topic in topics:
            errors.append(f"{where}: topic '{topic}' is used by another entity")
        topics.add(topic)

        columns = entity.get("columns") or []
        if not columns:
            errors.append(f"{where}: at least one column is required")
        col_names = []
        for col in columns:
            cname, ctype = col.get("name"), str(col.get("type", "")).replace(" ", "")
            if not cname or not IDENTIFIER.match(cname):
                errors.append(f"{where}: column name '{cname}' is not a valid identifier")
            elif cname in METADATA_COLUMNS:
                errors.append(f"{where}: column '{cname}' collides with a bronze metadata column")
            if not COLUMN_TYPE.match(ctype):
                errors.append(f"{where}: column '{cname}' has unsupported type '{col.get('type')}'")
            col["type"] = ctype
            col.setdefault("description", "")
            col_names.append(cname)
        if len(col_names) != len(set(col_names)):
            errors.append(f"{where}: duplicate column names")

        pk = entity.get("primary_key") or []
        if not pk:
            errors.append(f"{where}: primary_key is required")
        for key in pk:
            if key not in col_names:
                errors.append(f"{where}: primary_key column '{key}' is not in columns")
        entity.setdefault("description", "")

    if errors:
        raise ConfigError("invalid config/pipeline.yml:\n  - " + "\n  - ".join(errors))

    # Ordering used to pick the latest change per key (most significant first).
    seq = ["_source_ts_ms", "_kafka_offset"] if cfg["payload_format"] == "debezium" else ["_kafka_offset"]
    for entity in cfg["entities"]:
        entity["sequence_by"] = seq
    return cfg


def _load_plugin():
    sys.dont_write_bytecode = True  # keep the project tree free of __pycache__
    path = ROOT / "scripts" / "plugin_generate.py"
    spec = importlib.util.spec_from_file_location("plugin_generate", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check", action="store_true", help="fail if generated files are stale")
    args = parser.parse_args(argv)

    try:
        pipeline = load_pipeline()
    except ConfigError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    plugin = _load_plugin()
    rendered = {Path(p): content for p, content in plugin.render(pipeline).items()}
    existing = {p.relative_to(ROOT) for g in plugin.MANAGED_GLOBS for p in ROOT.glob(g) if p.is_file()}
    stale = sorted(existing - set(rendered))
    changed = sorted(p for p, c in rendered.items() if not (ROOT / p).is_file() or (ROOT / p).read_text() != c)

    if args.check:
        if changed or stale:
            for p in changed:
                print(f"out of date: {p}", file=sys.stderr)
            for p in stale:
                print(f"stale:       {p}", file=sys.stderr)
            print("Run `make generate` and commit the result.", file=sys.stderr)
            return 1
        print("generated files are up to date")
        return 0

    for p in changed:
        (ROOT / p).parent.mkdir(parents=True, exist_ok=True)
        (ROOT / p).write_text(rendered[p])
        print(f"wrote   {p}")
    for p in stale:
        (ROOT / p).unlink()
        print(f"removed {p}")
    if not changed and not stale:
        print("generated files are up to date")
    return 0


if __name__ == "__main__":
    sys.exit(main())
