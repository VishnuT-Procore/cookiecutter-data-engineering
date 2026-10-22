"""Offline checks for the Snowflake target (no Snowflake account needed)."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
PIPELINE = yaml.safe_load((ROOT / "config" / "pipeline.yml").read_text())


def test_connector_covers_every_entity():
    connector = json.loads((ROOT / "ingestion" / "connector.json").read_text())
    config = connector["config"]
    assert config["topics"].split(",") == [e["topic"] for e in PIPELINE["entities"]]
    mapping = dict(pair.split(":") for pair in config["snowflake.topic2table.map"].split(","))
    assert mapping == {e["topic"]: e["name"].upper() for e in PIPELINE["entities"]}
    assert config["snowflake.schema.name"] == PIPELINE["raw_schema"].upper()


def test_every_entity_has_bronze_and_silver_models():
    for e in PIPELINE["entities"]:
        assert (ROOT / "dbt" / "models" / "bronze" / f"bronze_{e['name']}.sql").is_file()
        assert (ROOT / "dbt" / "models" / "silver" / f"silver_{e['name']}.sql").is_file()


def test_dbt_parse(monkeypatch):
    pytest.importorskip("dbt.adapters.snowflake")
    from dbt.cli.main import dbtRunner

    for var in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_PATH"):
        monkeypatch.setenv(var, os.environ.get(var, "offline-check"))
    result = dbtRunner().invoke(["parse", "--project-dir", str(ROOT / "dbt"), "--profiles-dir", str(ROOT / "dbt")])
    assert result.success, result.exception
