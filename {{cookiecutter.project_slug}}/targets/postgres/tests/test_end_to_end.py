"""End-to-end: Kafka -> raw -> bronze -> silver -> gold on real Kafka and Postgres (Testcontainers).

Runs the sample events in two phases with an incremental dbt build after each, then a third
build to prove the pipeline is idempotent. Requires Docker.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import uuid
from pathlib import Path

import psycopg
import pytest

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "ci"


def _load(name: str, rel_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # dataclasses resolve annotations through sys.modules
    spec.loader.exec_module(module)
    return module


producer = _load("produce_sample_events", "scripts/produce_sample_events.py")
loader = _load("kafka_to_raw", "ingestion/kafka_to_raw.py")


@pytest.fixture(scope="module")
def stack():
    pytest.importorskip("testcontainers")
    try:
        from testcontainers.community.kafka import KafkaContainer
        from testcontainers.community.postgres import PostgresContainer
    except ImportError:  # testcontainers < 4.15
        from testcontainers.kafka import KafkaContainer
        from testcontainers.postgres import PostgresContainer

    with KafkaContainer().with_kraft() as kafka, PostgresContainer(
        "postgres:16", username="dbt", password="dbt", dbname="warehouse"
    ) as pg:
        env = {
            "KAFKA_BOOTSTRAP_SERVERS": kafka.get_bootstrap_server(),
            "KAFKA_GROUP_ID": f"e2e-{uuid.uuid4().hex[:8]}",
            "POSTGRES_HOST": pg.get_container_host_ip(),
            "POSTGRES_PORT": str(pg.get_exposed_port(5432)),
            "POSTGRES_USER": "dbt",
            "POSTGRES_PASSWORD": "dbt",
            "POSTGRES_DB": "warehouse",
            "POSTGRES_SCHEMA": SCHEMA,
            "DBT_TARGET": "dev",
        }
        previous = {k: os.environ.get(k) for k in env}
        os.environ.update(env)
        try:
            yield env
        finally:
            for k, v in previous.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v


def ingest():
    assert loader.main(["--once"]) == 0


def dbt_build():
    from dbt.cli.main import dbtRunner

    result = dbtRunner().invoke(["build", "--project-dir", str(ROOT / "dbt"), "--profiles-dir", str(ROOT / "dbt")])
    assert result.success, result.exception


def query(sql: str) -> list[tuple]:
    with psycopg.connect(loader.postgres_conninfo()) as conn:
        return conn.execute(sql).fetchall()


def test_pipeline_end_to_end(stack):
    producer.produce([1], stack["KAFKA_BOOTSTRAP_SERVERS"])
    ingest()
    dbt_build()

    assert query(f"select count(*) from {SCHEMA}_silver.silver_customers")[0][0] == 3
    assert query(f"select count(*) from {SCHEMA}_gold.fct_orders")[0][0] == 3

    producer.produce([2], stack["KAFKA_BOOTSTRAP_SERVERS"])
    ingest()
    dbt_build()

    customers = {
        r[0]: r[1:]
        for r in query(f"select id, name, email, city, _is_deleted from {SCHEMA}_silver.silver_customers order by id")
    }
    assert customers[1] == ("Alice", "alice@new.example.com", "Shelbyville", False)
    assert customers[2] == ("Bob", "bob@example.com", "Portland", False)  # stale replay ignored
    assert customers[3][-1] is True  # soft-deleted

    assert [r[0] for r in query(f"select customer_id from {SCHEMA}_gold.dim_customers order by 1")] == [1, 2]
    orders = dict(query(f"select order_id, status from {SCHEMA}_gold.fct_orders"))
    assert orders == {100: "created", 101: "shipped", 102: "created", 103: "created"}
    summary = {r[0]: (r[1], float(r[2])) for r in query(
        f"select customer_id, order_count, total_amount from {SCHEMA}_gold.customer_order_summary")}
    assert summary == {1: (2, 125.49), 2: (2, 15.0)}

    bronze_changes = query(f"select count(*) from {SCHEMA}_bronze.bronze_customers")[0][0]
    snapshot = query(f"select * from {SCHEMA}_silver.silver_customers order by id")

    # Re-running without new data changes nothing.
    dbt_build()
    assert query(f"select count(*) from {SCHEMA}_bronze.bronze_customers")[0][0] == bronze_changes
    assert query(f"select * from {SCHEMA}_silver.silver_customers order by id") == snapshot
