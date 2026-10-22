"""Dagster definitions for the Postgres target.

Batch (streaming_engine=none): kafka_raw/<entity> assets land Kafka records, then dbt builds
bronze -> silver -> gold. With Flink, bronze/silver tables are external assets written by the
Flink job; Dagster runs dbt (views + gold) and watches the Flink job with a sensor.
"""

import json
import os
import subprocess
import sys
import urllib.request

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from orchestration.dbt_translator import MedallionTranslator
from orchestration.pipeline import ROOT, load_pipeline

PIPELINE = load_pipeline()
ENTITIES = PIPELINE["entities"]
FLINK = PIPELINE.get("streaming_engine") == "flink"

DBT_PROJECT = DbtProject(project_dir=ROOT / "dbt", profiles_dir=ROOT / "dbt")
DBT_PROJECT.prepare_if_dev()  # `dbt parse` when running under `dagster dev`


@dbt_assets(manifest=DBT_PROJECT.manifest_path, project=DBT_PROJECT, dagster_dbt_translator=MedallionTranslator())
def dbt_medallion(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """bronze -> silver -> gold; dbt tests are reported as asset checks."""
    yield from dbt.cli(["build"], context=context).stream()


resources = {"dbt": DbtCliResource(project_dir=DBT_PROJECT)}

if FLINK:
    FLINK_JOB_NAME = os.environ.get("FLINK_JOB_NAME", f"{PIPELINE['project']}-medallion")

    flink_tables = [
        dg.AssetSpec(
            ["flink", f"{layer}_{e['name']}"],
            group_name="streaming",
            kinds={"flink", "postgres"},
            description=f"{layer.capitalize()} of Kafka topic {e['topic']}, written continuously by the Flink job.",
        )
        for e in ENTITIES
        for layer in ("bronze", "silver")
    ]

    @dg.sensor(minimum_interval_seconds=60, default_status=dg.DefaultSensorStatus.STOPPED)
    def flink_job_running(context: dg.SensorEvaluationContext):
        """Fails its tick (visible and alertable in Dagster) when the Flink job is not RUNNING."""
        url = os.environ.get("FLINK_REST_URL", "http://localhost:8081")
        with urllib.request.urlopen(f"{url}/jobs/overview", timeout=10) as response:
            jobs = json.load(response)["jobs"]
        if not any(j["name"] == FLINK_JOB_NAME and j["state"] == "RUNNING" for j in jobs):
            raise RuntimeError(f"Flink job '{FLINK_JOB_NAME}' is not RUNNING ({url})")
        return dg.SkipReason(f"Flink job '{FLINK_JOB_NAME}' is running")

    assets = [*flink_tables, dbt_medallion]
    medallion = dg.define_asset_job("medallion", selection=dg.AssetSelection.assets(dbt_medallion))
    schedules = [dg.ScheduleDefinition(job=medallion, cron_schedule="*/5 * * * *")]
    sensors = [flink_job_running]
else:

    @dg.multi_asset(
        specs=[
            dg.AssetSpec(
                ["kafka_raw", e["name"]],
                group_name="raw",
                kinds={"kafka", "postgres"},
                description=f"Raw records from Kafka topic {e['topic']}.",
            )
            for e in ENTITIES
        ]
    )
    def kafka_raw(context: dg.AssetExecutionContext):
        """Drain the Kafka topics into the raw tables (ingestion/kafka_to_raw.py --once)."""
        subprocess.run([sys.executable, str(ROOT / "ingestion" / "kafka_to_raw.py"), "--once"], check=True, cwd=ROOT)
        for spec in context.assets_def.specs:
            yield dg.MaterializeResult(asset_key=spec.key)

    assets = [kafka_raw, dbt_medallion]
    medallion = dg.define_asset_job("medallion", selection=dg.AssetSelection.all())
    schedules = [dg.ScheduleDefinition(job=medallion, cron_schedule="*/15 * * * *")]
    sensors = []

defs = dg.Definitions(assets=assets, jobs=[medallion], schedules=schedules, sensors=sensors, resources=resources)
