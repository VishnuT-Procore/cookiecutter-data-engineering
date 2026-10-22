"""Silver: latest change per primary key, maintained with Delta MERGE from the bronze stream."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from medallion.config import Entity

BRONZE_ONLY_COLUMNS = ("_is_valid", "_raw_key", "_raw_value")


def latest_per_key(bronze_df: DataFrame, entity: Entity, sequence_by: list[str]) -> DataFrame:
    """Keep the newest valid change for each primary key in a batch."""
    order = [F.col(c).desc_nulls_last() for c in sequence_by] + [F.col("_ingested_at").desc()]
    window = Window.partitionBy(*entity.primary_key).orderBy(*order)
    return (
        bronze_df.where("_is_valid")
        .withColumn("_rn", F.row_number().over(window))
        .where("_rn = 1")
        .drop("_rn", *BRONZE_ONLY_COLUMNS)
    )


def newer_condition(sequence_by: list[str], src: str = "s", tgt: str = "t") -> str:
    """SQL predicate: source row sorts after the target row (lexicographic, NULL sorts first)."""
    col, rest = sequence_by[0], sequence_by[1:]
    greater = f"({src}.{col} > {tgt}.{col} OR ({src}.{col} IS NOT NULL AND {tgt}.{col} IS NULL))"
    if not rest:
        return greater
    return f"({greater} OR ({src}.{col} <=> {tgt}.{col} AND {newer_condition(rest, src, tgt)}))"


def upsert_silver(spark: SparkSession, bronze_batch: DataFrame, entity: Entity, sequence_by: list[str], table: str) -> None:
    """MERGE a bronze micro-batch into silver. Older or replayed changes never win."""
    from delta.tables import DeltaTable

    latest = latest_per_key(bronze_batch, entity, sequence_by)
    if not spark.catalog.tableExists(table):
        latest.write.format("delta").saveAsTable(table)
        return
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in entity.primary_key)
    (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(latest.alias("s"), on)
        .whenMatchedUpdateAll(condition=newer_condition(sequence_by))
        .whenNotMatchedInsertAll()
        .execute()
    )
