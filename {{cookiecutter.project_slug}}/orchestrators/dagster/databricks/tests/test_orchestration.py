"""Dagster definitions load and every layer is an asset (no Databricks workspace needed)."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
pytest.importorskip("dagster")

PIPELINE = yaml.safe_load((ROOT / "config" / "pipeline.yml").read_text())


def test_definitions_cover_every_layer():
    import dagster as dg

    defs = importlib.import_module("orchestration.definitions").defs
    dg.Definitions.validate_loadable(defs)
    graph = defs.resolve_asset_graph()
    keys = {key.to_user_string() for key in graph.get_all_asset_keys()}
    for e in PIPELINE["entities"]:
        assert {f"bronze/{e['name']}", f"silver/{e['name']}"} <= keys
    assert {"gold/fct_orders", "gold/dim_customers", "gold/dim_customer_addresses", "gold/customer_order_summary"} <= keys
    assert defs.resolve_job_def("medallion")
    assert defs.resolve_schedule_def("medallion_schedule")
