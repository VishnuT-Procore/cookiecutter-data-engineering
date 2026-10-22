"""Dagster definitions load and every layer is an asset (no Snowflake connection needed)."""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")

PIPELINE = yaml.safe_load((ROOT / "config" / "pipeline.yml").read_text())


def test_definitions_cover_every_layer(monkeypatch):
    import dagster as dg
    from dbt.cli.main import dbtRunner

    for var in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_PATH"):
        monkeypatch.setenv(var, os.environ.get(var, "offline-check"))
    result = dbtRunner().invoke(["parse", "--project-dir", str(ROOT / "dbt"), "--profiles-dir", str(ROOT / "dbt")])
    assert result.success, result.exception

    defs = importlib.import_module("orchestration.definitions").defs
    dg.Definitions.validate_loadable(defs)
    keys = {key.to_user_string() for key in defs.resolve_asset_graph().get_all_asset_keys()}
    for e in PIPELINE["entities"]:
        assert {f"kafka_raw/{e['name']}", f"bronze/{e['name']}", f"silver/{e['name']}"} <= keys
    assert {"gold/fct_orders", "gold/dim_customers", "gold/dim_customer_addresses"} <= keys
    assert defs.resolve_job_def("medallion")
    assert defs.resolve_schedule_def("medallion_schedule")
