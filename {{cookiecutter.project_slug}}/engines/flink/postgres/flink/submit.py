#!/usr/bin/env python3
"""Prepare and render the Flink streaming job.

    python flink/submit.py    # create Postgres tables + Kafka topics, render flink/build/pipeline.sql

`make flink-submit` then runs the rendered SQL with the Flink SQL client in the jobmanager container.
Placeholders ${NAME} in flink/sql/pipeline.sql are filled from the environment (defaults below).
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import psycopg
import yaml
from psycopg.conninfo import make_conninfo

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "flink" / "sql"
BUILD_DIR = ROOT / "flink" / "build"
PLACEHOLDER = re.compile(r"\$\{([A-Z0-9_]+)\}")


def load_pipeline() -> dict:
    return yaml.safe_load((ROOT / "config" / "pipeline.yml").read_text())


def settings(env: dict | None = None) -> dict[str, str]:
    """Values for the SQL placeholders. Flink reaches Kafka/Postgres by their container names."""
    env = dict(os.environ if env is None else env)
    project = load_pipeline()["project"]
    defaults = {
        "FLINK_JOB_NAME": f"{project}-medallion",
        "FLINK_KAFKA_BOOTSTRAP_SERVERS": "kafka:19092",
        "FLINK_KAFKA_GROUP_ID": f"{project}-flink",
        "FLINK_JDBC_URL": "jdbc:postgresql://postgres:5432/" + env.get("POSTGRES_DB", "warehouse"),
        "FLINK_CHECKPOINT_INTERVAL": "10s",
        "FLINK_STATE_TTL": "0",
        "POSTGRES_USER": "dbt",
        "POSTGRES_PASSWORD": "dbt",
    }
    return {**defaults, **{k: v for k, v in env.items() if k in defaults}}


def render(values: dict[str, str]) -> str:
    sql = (SQL_DIR / "pipeline.sql").read_text()
    missing = sorted(set(PLACEHOLDER.findall(sql)) - set(values))
    if missing:
        raise KeyError(f"no value for placeholders: {', '.join(missing)}")
    return PLACEHOLDER.sub(lambda m: values[m.group(1)], sql)


def create_tables() -> None:
    conninfo = make_conninfo(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ.get("POSTGRES_USER", "dbt"),
        password=os.environ.get("POSTGRES_PASSWORD", "dbt"),
        dbname=os.environ.get("POSTGRES_DB", "warehouse"),
    )
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute((SQL_DIR / "postgres_tables.sql").read_text())


def create_topics(bootstrap_servers: str | None = None) -> None:
    """The Kafka source needs its topics to exist when the job starts."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")})
    existing = set(admin.list_topics(timeout=30).topics)
    wanted = [e["topic"] for e in load_pipeline()["entities"] if e["topic"] not in existing]
    if wanted:
        for future in admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in wanted]).values():
            future.result()


def prepare(bootstrap_servers: str | None = None, env: dict | None = None) -> Path:
    create_tables()
    create_topics(bootstrap_servers)
    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    out = BUILD_DIR / "pipeline.sql"
    out.write_text(render(settings(env)))
    return out


if __name__ == "__main__":
    print(f"rendered {prepare().relative_to(ROOT)}")
    sys.exit(0)
