"""Entry point for the Databricks jobs (and local runs).

    medallion bronze --pipeline config/pipeline.yml --catalog main --checkpoint-root /Volumes/...
    medallion silver --pipeline config/pipeline.yml --catalog main --checkpoint-root /Volumes/...
    medallion gold   --pipeline config/pipeline.yml --catalog main --sql-dir gold

--trigger availableNow (default) processes everything available and stops, so bronze -> silver -> gold
can run as one scheduled job. Pass an interval such as "1 minute" to run bronze/silver continuously.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from pyspark.sql import SparkSession

from medallion.bronze import build_bronze
from medallion.config import Pipeline, Tables, load_pipeline
from medallion.gold import run_gold
from medallion.silver import upsert_silver

LOG = logging.getLogger("medallion")


def _spark() -> SparkSession:
    spark = SparkSession.builder.getOrCreate()
    # Let MERGE / append add columns that were added to config/pipeline.yml.
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    return spark


def _trigger(value: str) -> dict:
    return {"availableNow": True} if value == "availableNow" else {"processingTime": value}


def _kafka_options(spark: SparkSession, args: argparse.Namespace) -> dict:
    options = {"kafka.bootstrap.servers": args.kafka_bootstrap_servers, "startingOffsets": args.starting_offsets}
    if args.kafka_secret_scope:
        # A JSON object of extra Kafka options, e.g.
        # {"kafka.security.protocol": "SASL_SSL", "kafka.sasl.mechanism": "PLAIN", "kafka.sasl.jaas.config": "..."}
        from pyspark.dbutils import DBUtils

        secret = DBUtils(spark).secrets.get(args.kafka_secret_scope, args.kafka_secret_key)
        options.update(json.loads(secret))
    return options


def _create_schemas(spark: SparkSession, tables: Tables) -> None:
    for schema in tables.schemas().values():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def _await(queries) -> None:
    for q in queries:
        q.awaitTermination()


def run_bronze(spark: SparkSession, pipeline: Pipeline, tables: Tables, args: argparse.Namespace) -> None:
    options = _kafka_options(spark, args)
    queries = []
    for entity in pipeline.entities:
        source = spark.readStream.format("kafka").options(**options).option("subscribe", entity.topic).load()
        queries.append(
            build_bronze(source, entity, pipeline.payload_format)
            .writeStream.format("delta")
            .queryName(f"bronze_{entity.name}")
            .option("checkpointLocation", f"{args.checkpoint_root}/bronze/{entity.name}")
            .option("mergeSchema", "true")
            .trigger(**_trigger(args.trigger))
            .toTable(tables.bronze(entity))
        )
    _await(queries)


def run_silver(spark: SparkSession, pipeline: Pipeline, tables: Tables, args: argparse.Namespace) -> None:
    queries = []
    for entity in pipeline.entities:
        target = tables.silver(entity)

        def merge_batch(batch_df, _batch_id, entity=entity, target=target):
            upsert_silver(batch_df.sparkSession, batch_df, entity, pipeline.sequence_by, target)

        queries.append(
            spark.readStream.table(tables.bronze(entity))
            .writeStream.queryName(f"silver_{entity.name}")
            .foreachBatch(merge_batch)
            .option("checkpointLocation", f"{args.checkpoint_root}/silver/{entity.name}")
            .trigger(**_trigger(args.trigger))
            .start()
        )
    _await(queries)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("layer", choices=["bronze", "silver", "gold"])
    parser.add_argument("--pipeline", required=True, help="path to config/pipeline.yml")
    parser.add_argument("--catalog", default="", help="Unity Catalog catalog (empty for local Spark)")
    parser.add_argument("--bronze-schema", default="bronze")
    parser.add_argument("--silver-schema", default="silver")
    parser.add_argument("--gold-schema", default="gold")
    parser.add_argument("--checkpoint-root", help="e.g. /Volumes/<catalog>/<schema>/checkpoints/<project>")
    parser.add_argument("--trigger", default="availableNow", help='"availableNow" or an interval like "1 minute"')
    parser.add_argument("--kafka-bootstrap-servers", default="localhost:9092")
    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument("--kafka-secret-scope", default="", help="Databricks secret scope with extra Kafka options")
    parser.add_argument("--kafka-secret-key", default="kafka-options")
    parser.add_argument("--sql-dir", default="gold", help="directory of gold *.sql files")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if args.layer in ("bronze", "silver") and not args.checkpoint_root:
        parser.error("--checkpoint-root is required for bronze and silver")

    pipeline = load_pipeline(args.pipeline)
    tables = Tables(args.catalog, args.bronze_schema, args.silver_schema, args.gold_schema)
    spark = _spark()
    _create_schemas(spark, tables)

    if args.layer == "bronze":
        run_bronze(spark, pipeline, tables, args)
    elif args.layer == "silver":
        run_silver(spark, pipeline, tables, args)
    else:
        LOG.info("gold: ran %s", ", ".join(run_gold(spark, args.sql_dir, tables.schemas())))
    return 0


if __name__ == "__main__":
    sys.exit(main())
