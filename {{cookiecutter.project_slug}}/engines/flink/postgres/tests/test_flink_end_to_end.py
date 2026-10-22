"""End-to-end: Kafka -> Flink (bronze + silver in Postgres) -> dbt gold, on the Docker Compose stack.

Starts compose.yaml + compose.override.yaml + compose.flink.yaml (Kafka, Postgres, Flink), submits the
generated Flink SQL job, streams the two sample phases and waits for silver to converge, then builds
gold with dbt. Requires Docker and free ports 9092, 5432 and 8081.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

import psycopg
import pytest

ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILES = ["compose.yaml", "compose.override.yaml", "compose.flink.yaml"]
ENV = {
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "POSTGRES_HOST": "localhost",
    "POSTGRES_PORT": "5432",
    "POSTGRES_USER": "dbt",
    "POSTGRES_PASSWORD": "dbt",
    "POSTGRES_DB": "warehouse",
    "POSTGRES_SCHEMA": "ci",
    "DBT_TARGET": "dev",
}


def _load(name: str, rel_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


producer = _load("produce_sample_events", "scripts/produce_sample_events.py")
submit = _load("flink_submit", "flink/submit.py")
jars = _load("flink_fetch_jars", "flink/fetch_jars.py")
PIPELINE = submit.load_pipeline()
RAW = PIPELINE["raw_schema"]


@pytest.fixture(scope="module")
def stack():
    pytest.importorskip("testcontainers")
    from testcontainers.compose import DockerCompose

    jars.fetch()
    (ROOT / "flink" / "build").mkdir(parents=True, exist_ok=True)
    previous = {k: os.environ.get(k) for k in ENV}
    os.environ.update(ENV)
    compose = DockerCompose(str(ROOT), compose_file_name=COMPOSE_FILES, build=True, wait=True)
    try:
        with compose:
            yield compose
    finally:
        for k, v in previous.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def query(sql: str) -> list[tuple]:
    conninfo = "host=localhost port=5432 user=dbt password=dbt dbname=warehouse"
    with psycopg.connect(conninfo) as conn:
        return conn.execute(sql).fetchall()


def wait_for(description: str, predicate, timeout: float = 180, interval: float = 2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception:  # tables may not be visible yet
            pass
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {description}")


def running_jobs() -> int:
    with urllib.request.urlopen("http://localhost:8081/jobs/overview", timeout=5) as r:
        return sum(1 for j in json.load(r)["jobs"] if j["state"] == "RUNNING")


def silver_customers() -> dict:
    rows = query(f"select id, name, email, city, _is_deleted from {RAW}.silver_customers")
    return {r[0]: r[1:] for r in rows}


def order_status() -> dict:
    return dict(query(f"select order_id, status from {RAW}.silver_orders"))


def dbt_build():
    from dbt.cli.main import dbtRunner

    result = dbtRunner().invoke(["build", "--project-dir", str(ROOT / "dbt"), "--profiles-dir", str(ROOT / "dbt")])
    assert result.success, result.exception


def test_flink_streaming_pipeline(stack):
    submit.prepare("localhost:9092")
    stdout, stderr, code = stack.exec_in_container(
        ["/opt/flink/bin/sql-client.sh", "-f", "/opt/flink/build/pipeline.sql"], service_name="flink-jobmanager"
    )
    assert code == 0 and "[ERROR]" not in stdout, stdout + stderr
    wait_for("the Flink job to run", lambda: running_jobs() == 1, timeout=60)

    producer.produce([1], "localhost:9092")
    wait_for("phase 1 in silver", lambda: len(silver_customers()) == 3 and len(order_status()) == 3)

    producer.produce([2], "localhost:9092")
    wait_for(
        "phase 2 in silver",
        lambda: silver_customers()[1][1] == "alice@new.example.com"
        and silver_customers()[3][-1] is True
        and order_status() == {100: "created", 101: "shipped", 102: "created", 103: "created"},
    )
    customers = silver_customers()
    assert customers[1] == ("Alice", "alice@new.example.com", "Shelbyville", False)
    assert customers[2] == ("Bob", "bob@example.com", "Portland", False)  # stale replay ignored

    # Bronze keeps every change (Debezium tombstones excluded).
    expected_changes = sum(
        1
        for phase in (1, 2)
        for e in producer.sample_events(PIPELINE["payload_format"], phase)
        if e.entity == "customers" and (PIPELINE["payload_format"] == "json" or e.value is not None)
    )
    assert query(f"select count(*) from {RAW}.bronze_customers")[0][0] == expected_changes

    dbt_build()
    assert [r[0] for r in query("select customer_id from ci_gold.dim_customers order by 1")] == [1, 2]
    summary = {r[0]: (r[1], float(r[2])) for r in query(
        "select customer_id, order_count, total_amount from ci_gold.customer_order_summary")}
    assert summary == {1: (2, 125.49), 2: (2, 15.0)}
