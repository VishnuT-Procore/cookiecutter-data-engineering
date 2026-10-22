"""Dagster definitions: they load, every layer is an asset, and (batch mode) the job runs end to end."""

from __future__ import annotations

import importlib
import os
import sys
import uuid
from pathlib import Path

import psycopg
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")

import yaml  # noqa: E402

PIPELINE = yaml.safe_load((ROOT / "config" / "pipeline.yml").read_text())
FLINK = PIPELINE.get("streaming_engine") == "flink"


def load_defs():
    from dbt.cli.main import dbtRunner

    result = dbtRunner().invoke(["parse", "--project-dir", str(ROOT / "dbt"), "--profiles-dir", str(ROOT / "dbt")])
    assert result.success, result.exception
    module = importlib.import_module("orchestration.definitions")
    return importlib.reload(module).defs


def test_definitions_cover_every_layer():
    import dagster as dg

    defs = load_defs()
    dg.Definitions.validate_loadable(defs)
    keys = {key.to_user_string() for key in defs.resolve_asset_graph().get_all_asset_keys()}
    for e in PIPELINE["entities"]:
        assert f"bronze/{e['name']}" in keys
        assert f"silver/{e['name']}" in keys
        assert (f"flink/silver_{e['name']}" if FLINK else f"kafka_raw/{e['name']}") in keys
    assert {"gold/fct_orders", "gold/dim_customers", "gold/dim_customer_addresses"} <= keys
    assert defs.resolve_job_def("medallion")
    assert defs.resolve_schedule_def("medallion_schedule")
    checks = defs.resolve_asset_graph().asset_check_keys
    assert any(c.asset_key.to_user_string() == "gold/fct_orders" for c in checks), "dbt tests should be asset checks"


@pytest.mark.skipif(FLINK, reason="the Flink stack is covered by tests/test_flink_end_to_end.py")
def test_medallion_job_end_to_end():
    pytest.importorskip("testcontainers")
    try:
        from testcontainers.community.kafka import KafkaContainer
        from testcontainers.community.postgres import PostgresContainer
    except ImportError:  # testcontainers < 4.15
        from testcontainers.kafka import KafkaContainer
        from testcontainers.postgres import PostgresContainer

    spec = importlib.util.spec_from_file_location("produce_sample_events", ROOT / "scripts" / "produce_sample_events.py")
    producer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(producer)

    with KafkaContainer().with_kraft() as kafka, PostgresContainer(
        "postgres:16", username="dbt", password="dbt", dbname="warehouse"
    ) as pg:
        env = {
            "KAFKA_BOOTSTRAP_SERVERS": kafka.get_bootstrap_server(),
            "KAFKA_GROUP_ID": f"dagster-{uuid.uuid4().hex[:8]}",
            "POSTGRES_HOST": pg.get_container_host_ip(),
            "POSTGRES_PORT": str(pg.get_exposed_port(5432)),
            "POSTGRES_SCHEMA": "dg",
            "DBT_TARGET": "dev",
        }
        previous = {k: os.environ.get(k) for k in env}
        os.environ.update(env)
        try:
            producer.produce([1, 2], env["KAFKA_BOOTSTRAP_SERVERS"])
            result = load_defs().resolve_job_def("medallion").execute_in_process()
            assert result.success
            conninfo = f"host={env['POSTGRES_HOST']} port={env['POSTGRES_PORT']} user=dbt password=dbt dbname=warehouse"
            with psycopg.connect(conninfo) as conn:
                orders = dict(conn.execute("select order_id, status from dg_gold.fct_orders").fetchall())
            assert orders == {100: "created", 101: "shipped", 102: "created", 103: "created"}
        finally:
            for k, v in previous.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
