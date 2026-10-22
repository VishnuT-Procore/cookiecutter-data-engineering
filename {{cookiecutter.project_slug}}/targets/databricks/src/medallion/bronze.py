"""Bronze: Kafka records -> append-only Delta change log, one table per entity."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from medallion.config import Entity

VALID_OPS = ("c", "u", "r", "d")


def build_bronze(kafka_df: DataFrame, entity: Entity, payload_format: str) -> DataFrame:
    """Parse a Kafka source DataFrame (key, value, topic, partition, offset, timestamp).

    Every message except Debezium tombstones is kept. Rows that cannot be parsed are kept with
    ``_is_valid = false`` (and the raw value), so nothing is lost silently; silver ignores them.
    """
    value = F.col("value").cast("string")
    key = F.col("key").cast("string")
    record_schema = entity.record_schema

    if payload_format == "debezium":
        envelope_schema = T.StructType(
            [
                T.StructField("before", record_schema),
                T.StructField("after", record_schema),
                T.StructField("op", T.StringType()),
                T.StructField("ts_ms", T.LongType()),
            ]
        )
        # JsonConverter with schemas.enable=true wraps the envelope in {schema, payload}.
        envelope_json = F.when(
            F.get_json_object(value, "$.schema").isNotNull() & F.get_json_object(value, "$.payload").isNotNull(),
            F.get_json_object(value, "$.payload"),
        ).otherwise(value)
        envelope = F.from_json(envelope_json, envelope_schema)
        df = (
            kafka_df.where(F.col("value").isNotNull())  # tombstones only exist for log compaction
            .withColumn("_envelope", envelope)
            .withColumn("_op", F.col("_envelope.op"))
            .withColumn(
                "_record", F.when(F.col("_op") == "d", F.col("_envelope.before")).otherwise(F.col("_envelope.after"))
            )
            .withColumn("_source_ts_ms", F.col("_envelope.ts_ms"))
        )
    elif payload_format == "json":
        df = (
            kafka_df.withColumn("_op", F.when(F.col("value").isNull(), F.lit("d")).otherwise(F.lit("u")))
            # A tombstone carries no value: recover the primary key from the JSON message key.
            .withColumn(
                "_record",
                F.when(F.col("value").isNull(), F.from_json(key, record_schema)).otherwise(
                    F.from_json(value, record_schema)
                ),
            )
            .withColumn("_source_ts_ms", F.lit(None).cast("long"))
        )
    else:
        raise ValueError(f"unknown payload_format {payload_format!r}")

    is_valid = F.col("_record").isNotNull() & F.col("_op").isin(*VALID_OPS)
    for k in entity.primary_key:
        is_valid = is_valid & F.col(f"_record.{k}").isNotNull()

    return df.select(
        *[c.convert(F.col(f"_record.{c.name}")).alias(c.name) for c in entity.columns],
        F.col("_op"),
        (F.col("_op") == "d").alias("_is_deleted"),
        F.col("_source_ts_ms"),
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
        F.current_timestamp().alias("_ingested_at"),
        F.coalesce(is_valid, F.lit(False)).alias("_is_valid"),
        key.alias("_raw_key"),
        value.alias("_raw_value"),
    )
