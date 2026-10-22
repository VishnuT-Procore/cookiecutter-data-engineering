"""Dagster definitions load and every layer is an asset (no warehouse connection needed)."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")

PIPELINE = yaml.safe_load((ROOT / "config" / "pipeline.yml").read_text())


def test_definitions_cover_every_layer():
    import dagster as dg
    from dbt.cli.main import dbtRunner

    result = dbtRunner().invoke(["parse", "--project-dir", str(ROOT / "dbt"), "--profiles-dir", str(ROOT / "dbt")])
    assert result.success, result.exception

    defs = importlib.import_module("orchestration.definitions").defs
    dg.Definitions.validate_loadable(defs)
    graph = defs.resolve_asset_graph()
    keys = {key.to_user_string() for key in graph.get_all_asset_keys()}
    for e in PIPELINE["entities"]:
        assert {f"landing/{e['name']}", f"bronze/{e['name']}", f"silver/{e['name']}"} <= keys
        bronze = graph.get(dg.AssetKey(["bronze", e["name"]]))
        assert dg.AssetKey(["landing", e["name"]]) in bronze.parent_keys, "bronze should depend on landing"
    assert {"gold/fct_orders", "gold/dim_customers", "gold/dim_customer_addresses"} <= keys
    assert any(c.asset_key.to_user_string() == "gold/fct_orders" for c in graph.asset_check_keys)
    assert defs.resolve_job_def("medallion")
    assert defs.resolve_schedule_def("medallion_schedule")
    assert defs.resolve_sensor_def("ai_ops")
    assert defs.resolve_sensor_def("ai_ops_on_run_failure")
