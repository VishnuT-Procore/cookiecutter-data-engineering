"""Offline checks for the Snowflake target (no Snowflake account needed)."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
PIPELINE = yaml.safe_load((ROOT / "config" / "pipeline.yml").read_text())


def test_every_entity_is_exposed_to_snowflake():
    sql = (ROOT / "snowflake" / "landing_tables.sql").read_text()
    for e in PIPELINE["entities"]:
        assert f"CATALOG_TABLE_NAME = '{e['name']}'" in sql
    assert sql.count("AUTO_REFRESH = TRUE") == len(PIPELINE["entities"])


def test_dbt_parse():
    from dbt.cli.main import dbtRunner

    result = dbtRunner().invoke(["parse", "--project-dir", str(ROOT / "dbt"), "--profiles-dir", str(ROOT / "dbt")])
    assert result.success, result.exception
