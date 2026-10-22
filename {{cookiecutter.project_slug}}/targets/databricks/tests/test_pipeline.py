"""Bronze -> silver -> gold on local Spark + Delta Lake, fed with the sample Kafka events.

Exercises the same code the Databricks jobs run: build_bronze, the silver foreachBatch stream
(availableNow) with Delta MERGE, and the gold SQL files.
"""

from __future__ import annotations

import argparse
import importlib.util
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load_producer():
    spec = importlib.util.spec_from_file_location("produce_sample_events", ROOT / "scripts" / "produce_sample_events.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


producer = _load_producer()


@pytest.fixture(scope="module")
def pipeline():
    from medallion.config import load_pipeline

    return load_pipeline(ROOT / "config" / "pipeline.yml")


@pytest.fixture(scope="module")
def tables(spark):
    from medallion.config import Tables

    t = Tables("", "t_bronze", "t_silver", "t_gold")
    for schema in t.schemas().values():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    return t


class FakeKafka:
    """Turns sample events into DataFrames shaped like Spark's Kafka source."""

    def __init__(self, spark, pipeline):
        self.spark = spark
        self.pipeline = pipeline
        self.offsets: dict[str, int] = {}

    def batch(self, phase: int, entity_name: str):
        from pyspark.sql import types as T

        entity = next(e for e in self.pipeline.entities if e.name == entity_name)
        rows = []
        for event in producer.sample_events(self.pipeline.payload_format, phase):
            if event.entity != entity_name:
                continue
            offset = self.offsets.get(entity_name, 0)
            self.offsets[entity_name] = offset + 1
            rows.append(
                (producer.encode(event.key), producer.encode(event.value), entity.topic, 0, offset,
                 datetime.now(timezone.utc).replace(tzinfo=None))
            )
        schema = T.StructType([
            T.StructField("key", T.BinaryType()),
            T.StructField("value", T.BinaryType()),
            T.StructField("topic", T.StringType()),
            T.StructField("partition", T.IntegerType()),
            T.StructField("offset", T.LongType()),
            T.StructField("timestamp", T.TimestampType()),
        ])
        return self.spark.createDataFrame(rows, schema)


def _land_bronze(kafka: FakeKafka, phase: int, tables) -> None:
    from medallion.bronze import build_bronze

    for entity in kafka.pipeline.entities:
        df = build_bronze(kafka.batch(phase, entity.name), entity, kafka.pipeline.payload_format)
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(tables.bronze(entity))


def test_end_to_end(spark, pipeline, tables, tmp_path):
    from medallion.cli import run_silver
    from medallion.gold import run_gold

    kafka = FakeKafka(spark, pipeline)
    args = argparse.Namespace(checkpoint_root=str(tmp_path / "checkpoints"), trigger="availableNow")

    _land_bronze(kafka, 1, tables)
    run_silver(spark, pipeline, tables, args)
    assert spark.table(tables.silver(pipeline.entities[0])).count() == 3

    _land_bronze(kafka, 2, tables)
    run_silver(spark, pipeline, tables, args)  # incremental: only phase-2 bronze rows are read
    run_gold(spark, ROOT / "gold", tables.schemas())

    gold = tables.schemas()["gold"]
    customers = {r.id: r for r in spark.table(tables.silver(pipeline.entities[0])).collect()}
    assert (customers[1].email, customers[1].city) == ("alice@new.example.com", "Shelbyville")
    assert customers[2].name == "Bob"  # stale replay ignored
    assert customers[3]._is_deleted

    assert sorted(r.customer_id for r in spark.table(f"{gold}.dim_customers").collect()) == [1, 2]
    orders = {r.order_id: r.status for r in spark.table(f"{gold}.fct_orders").collect()}
    assert orders == {100: "created", 101: "shipped", 102: "created", 103: "created"}
    summary = {r.customer_id: (r.order_count, float(r.total_amount))
               for r in spark.table(f"{gold}.customer_order_summary").collect()}
    assert summary == {1: (2, 125.49), 2: (2, 15.0)}
    assert spark.table(f"{gold}.dim_customer_addresses").count() == 2


def test_replayed_batch_does_not_overwrite_newer_rows(spark, pipeline, tables, tmp_path):
    from medallion.bronze import build_bronze
    from medallion.silver import upsert_silver

    customers = pipeline.entities[0]
    target = f"{tables.schemas()['silver']}.replay_customers"
    kafka = FakeKafka(spark, pipeline)
    phase1 = build_bronze(kafka.batch(1, "customers"), customers, pipeline.payload_format)
    phase2 = build_bronze(kafka.batch(2, "customers"), customers, pipeline.payload_format)

    upsert_silver(spark, phase1, customers, pipeline.sequence_by, target)
    upsert_silver(spark, phase2, customers, pipeline.sequence_by, target)
    expected = sorted(spark.table(target).drop("_ingested_at").collect())

    upsert_silver(spark, phase1, customers, pipeline.sequence_by, target)  # replay old changes
    assert sorted(spark.table(target).drop("_ingested_at").collect()) == expected
