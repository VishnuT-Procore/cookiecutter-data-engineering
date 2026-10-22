"""End-to-end on the local lakehouse: Kafka -> Flink -> Iceberg landing -> dbt (DuckDB) -> gold.

Starts compose.yaml (Kafka, Flink + SQL Gateway, Iceberg REST catalog, S3 storage), deploys the
generated landing job, streams the two sample phases and runs incremental dbt builds after each.
Requires Docker and free ports 9092, 9000, 8181, 8081, 8083.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load(name: str, rel_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


producer = _load("produce_sample_events", "scripts/produce_sample_events.py")
landing = _load("landing", "ingestion/landing.py")
jars = _load("fetch_jars", "ingestion/flink/fetch_jars.py")
PIPELINE = landing.load_pipeline()


@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    pytest.importorskip("testcontainers")
    from testcontainers.compose import DockerCompose

    jars.fetch()
    (ROOT / "ingestion" / "build").mkdir(parents=True, exist_ok=True)
    env = {
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "LOCAL_S3_ENDPOINT": "http://localhost:9000",
        "DUCKDB_PATH": str(tmp_path_factory.mktemp("warehouse") / "warehouse.duckdb"),
        "LANDING_DATABASE": "lake",
        "DBT_TARGET": "dev",
    }
    previous = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    try:
        with DockerCompose(str(ROOT), build=True, wait=True):
            yield env
    finally:
        for k, v in previous.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def lake():
    import duckdb

    con = duckdb.connect()
    for ext in ("httpfs", "avro", "iceberg"):
        con.execute(f"LOAD {ext}")
    con.execute("CREATE SECRET (TYPE s3, KEY_ID 'lakeadmin', SECRET 'lakeadmin-secret', ENDPOINT 'localhost:9000', "
                "URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')")
    con.execute("ATTACH 'warehouse' AS lake (TYPE iceberg, ENDPOINT 'http://localhost:8181', AUTHORIZATION_TYPE 'none')")
    return con


def landed(entity: str) -> int:
    try:
        return lake().execute(f"select count(*) from lake.{PIPELINE['landing_namespace']}.{entity}").fetchone()[0]
    except Exception:  # table not committed yet
        return 0


def wait_for_landing(expected: dict[str, int], timeout: float = 120) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all(landed(e) >= n for e, n in expected.items()):
            return
        time.sleep(2)
    raise AssertionError(f"landing did not reach {expected}: { {e: landed(e) for e in expected} }")


def produced(phases: list[int]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for phase in phases:
        for event in producer.sample_events(PIPELINE["payload_format"], phase):
            counts[event.entity] = counts.get(event.entity, 0) + 1
    return counts


def dbt_build() -> None:
    """Through the pipeline CLI contract, in its own process (DuckDB allows one writer per file)."""
    result = subprocess.run([sys.executable, str(ROOT / "scripts" / "pipeline.py"), "transform"],
                            cwd=ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-2000:]


def warehouse(sql: str) -> list[tuple]:
    import duckdb

    with duckdb.connect(os.environ["DUCKDB_PATH"], read_only=True) as con:
        return con.execute(sql).fetchall()


def test_lakehouse_end_to_end(stack):
    print(landing.deploy())
    assert landing.main(["status"]) == 0

    producer.produce([1], stack["KAFKA_BOOTSTRAP_SERVERS"])
    wait_for_landing(produced([1]))
    dbt_build()
    assert warehouse("select count(*) from dev_silver.silver_customers")[0][0] == 3

    producer.produce([2], stack["KAFKA_BOOTSTRAP_SERVERS"])
    wait_for_landing(produced([1, 2]))
    dbt_build()

    customers = {r[0]: r[1:] for r in warehouse(
        "select id, name, email, city, _is_deleted from dev_silver.silver_customers")}
    assert customers[1] == ("Alice", "alice@new.example.com", "Shelbyville", False)
    assert customers[2] == ("Bob", "bob@example.com", "Portland", False)  # stale replay ignored
    assert customers[3][-1] is True  # soft-deleted
    assert [r[0] for r in warehouse("select customer_id from dev_gold.dim_customers order by 1")] == [1, 2]
    orders = dict(warehouse("select order_id, status from dev_gold.fct_orders"))
    assert orders == {100: "created", 101: "shipped", 102: "created", 103: "created"}
    summary = {r[0]: (r[1], float(r[2])) for r in warehouse(
        "select customer_id, order_count, total_amount from dev_gold.customer_order_summary")}
    assert summary == {1: (2, 125.49), 2: (2, 15.0)}

    # Re-running without new data changes nothing (idempotent incremental models).
    bronze = warehouse("select count(*) from dev_bronze.bronze_customers")[0][0]
    snapshot = warehouse("select * from dev_silver.silver_customers order by id")
    dbt_build()
    assert warehouse("select count(*) from dev_bronze.bronze_customers")[0][0] == bronze
    assert warehouse("select * from dev_silver.silver_customers order by id") == snapshot
