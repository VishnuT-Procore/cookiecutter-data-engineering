#!/usr/bin/env python3
"""Land Kafka records in Postgres raw tables (one table per topic).

Delivery is effectively-once: rows are inserted with ON CONFLICT DO NOTHING on
(kafka_partition, kafka_offset) and committed to Postgres *before* Kafka offsets are
committed, so a crash only causes harmless re-delivery. Messages whose value is not
valid JSON are written to <raw_schema>._dead_letter instead of blocking the topic.

    python ingestion/kafka_to_raw.py           # run continuously
    python ingestion/kafka_to_raw.py --once    # drain available messages, then exit

Configuration (environment):
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID, KAFKA_CONF_<KEY>=value (any librdkafka setting,
    e.g. KAFKA_CONF_SECURITY_PROTOCOL=SASL_SSL), POSTGRES_HOST/PORT/USER/PASSWORD/DB
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.conninfo import make_conninfo
import yaml
from psycopg import sql
from psycopg.types.json import Jsonb

LOG = logging.getLogger("kafka_to_raw")
ROOT = Path(__file__).resolve().parents[1]

RAW_TABLE_DDL = """
create table if not exists {table} (
    kafka_topic     text        not null,
    kafka_partition integer     not null,
    kafka_offset    bigint      not null,
    kafka_timestamp timestamptz,
    kafka_key       text,
    payload         jsonb,
    ingested_at     timestamptz not null default clock_timestamp(),
    primary key (kafka_partition, kafka_offset)
)
"""

DEAD_LETTER_DDL = """
create table if not exists {table} (
    kafka_topic     text        not null,
    kafka_partition integer     not null,
    kafka_offset    bigint      not null,
    kafka_key       text,
    raw_value       text,
    error           text,
    ingested_at     timestamptz not null default clock_timestamp(),
    primary key (kafka_topic, kafka_partition, kafka_offset)
)
"""


@dataclass(frozen=True)
class RawRecord:
    topic: str
    partition: int
    offset: int
    timestamp: datetime | None
    key: bytes | None
    value: bytes | None


def _decode(data: bytes | None) -> str | None:
    return None if data is None else data.decode("utf-8", errors="replace")


class RawWriter:
    """Writes batches of Kafka records to per-topic raw tables."""

    def __init__(self, conn: psycopg.Connection, schema: str, topic_to_table: dict[str, str]):
        self.conn = conn
        self.schema = schema
        self.topic_to_table = topic_to_table

    def ensure_tables(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("create schema if not exists {}").format(sql.Identifier(self.schema)))
            for table in self.topic_to_table.values():
                cur.execute(sql.SQL(RAW_TABLE_DDL).format(table=sql.Identifier(self.schema, table)))
            cur.execute(sql.SQL(DEAD_LETTER_DDL).format(table=sql.Identifier(self.schema, "_dead_letter")))
        self.conn.commit()

    def write(self, records: list[RawRecord]) -> tuple[int, int]:
        """Insert a batch in one transaction. Returns (landed, dead_lettered)."""
        rows: dict[str, list[tuple]] = {}
        dead: list[tuple] = []
        for r in records:
            table = self.topic_to_table.get(r.topic)
            if table is None:
                dead.append((r.topic, r.partition, r.offset, _decode(r.key), _decode(r.value), "unmapped topic"))
                continue
            try:
                payload = None if r.value is None else json.loads(r.value)
            except (ValueError, UnicodeDecodeError) as exc:
                dead.append((r.topic, r.partition, r.offset, _decode(r.key), _decode(r.value), f"invalid JSON: {exc}"))
                continue
            rows.setdefault(table, []).append(
                (r.topic, r.partition, r.offset, r.timestamp, _decode(r.key), None if payload is None else Jsonb(payload))
            )

        with self.conn.cursor() as cur:
            for table, batch in rows.items():
                cur.executemany(
                    sql.SQL(
                        "insert into {} (kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, kafka_key, payload) "
                        "values (%s, %s, %s, %s, %s, %s) on conflict (kafka_partition, kafka_offset) do nothing"
                    ).format(sql.Identifier(self.schema, table)),
                    batch,
                )
            if dead:
                cur.executemany(
                    sql.SQL(
                        "insert into {} (kafka_topic, kafka_partition, kafka_offset, kafka_key, raw_value, error) "
                        "values (%s, %s, %s, %s, %s, %s) on conflict do nothing"
                    ).format(sql.Identifier(self.schema, "_dead_letter")),
                    dead,
                )
        self.conn.commit()
        return sum(len(b) for b in rows.values()), len(dead)


def load_pipeline(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def kafka_config() -> dict:
    conf = {
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": os.environ.get("KAFKA_GROUP_ID", "medallion-raw-loader"),
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    for name, value in os.environ.items():
        if name.startswith("KAFKA_CONF_"):
            conf[name[len("KAFKA_CONF_"):].lower().replace("_", ".")] = value
    return conf


def postgres_conninfo() -> str:
    return make_conninfo(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ.get("POSTGRES_USER", "dbt"),
        password=os.environ.get("POSTGRES_PASSWORD", "dbt"),
        dbname=os.environ.get("POSTGRES_DB", "warehouse"),
    )


def run(args: argparse.Namespace) -> int:
    from confluent_kafka import TIMESTAMP_NOT_AVAILABLE, Consumer, KafkaError, KafkaException

    pipeline = load_pipeline(Path(args.pipeline))
    topic_to_table = {e["topic"]: e["name"] for e in pipeline["entities"]}

    stop = False

    def _stop(signum, _frame):
        nonlocal stop
        LOG.info("received signal %s, finishing current batch", signum)
        stop = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    with psycopg.connect(postgres_conninfo()) as conn:
        writer = RawWriter(conn, pipeline["raw_schema"], topic_to_table)
        writer.ensure_tables()

        assigned = False

        def _on_assign(_consumer, partitions):
            nonlocal assigned
            assigned = True
            LOG.info("assigned %d partitions", len(partitions))

        consumer = Consumer(kafka_config())
        consumer.subscribe(list(topic_to_table), on_assign=_on_assign)
        LOG.info("consuming %s", ", ".join(topic_to_table))
        idle_polls, total = 0, 0
        started = time.monotonic()
        try:
            while not stop:
                messages = consumer.consume(num_messages=args.batch_size, timeout=args.poll_timeout)
                records = []
                for m in messages:
                    if m.error():
                        if m.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise KafkaException(m.error())
                    ts_type, ts_ms = m.timestamp()
                    ts = None if ts_type == TIMESTAMP_NOT_AVAILABLE else datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    records.append(RawRecord(m.topic(), m.partition(), m.offset(), ts, m.key(), m.value()))

                if not records:
                    if not assigned:
                        # Group join takes a few seconds; don't count those polls as "drained".
                        if args.once and time.monotonic() - started > args.assign_timeout:
                            LOG.warning("no partitions assigned after %ss; do the topics exist?", args.assign_timeout)
                            break
                        continue
                    idle_polls += 1
                    if args.once and idle_polls >= args.idle_polls:
                        break
                    continue

                idle_polls = 0
                landed, dead = writer.write(records)
                consumer.commit(asynchronous=False)
                total += landed
                LOG.info("landed %d records (%d dead-lettered)", landed, dead)
        finally:
            consumer.close()
    LOG.info("done, %d records landed", total)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pipeline", default=str(ROOT / "config" / "pipeline.yml"))
    parser.add_argument("--once", action="store_true", help="exit after the topics are drained")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--poll-timeout", type=float, default=1.0, help="seconds")
    parser.add_argument("--idle-polls", type=int, default=5, help="empty polls before --once exits")
    parser.add_argument("--assign-timeout", type=float, default=60.0, help="seconds --once waits for partitions")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
