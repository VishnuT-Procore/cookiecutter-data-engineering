"""Dagster definitions for the Snowflake target.

Raw tables are loaded continuously by the Snowflake Kafka Connector (external assets); Dagster
runs dbt to build bronze -> silver -> gold, with dbt tests as asset checks.
"""

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from orchestration.dbt_translator import MedallionTranslator
from orchestration.pipeline import ROOT, load_pipeline

PIPELINE = load_pipeline()

DBT_PROJECT = DbtProject(project_dir=ROOT / "dbt", profiles_dir=ROOT / "dbt")
DBT_PROJECT.prepare_if_dev()  # `dbt parse` when running under `dagster dev`

kafka_raw = [
    dg.AssetSpec(
        ["kafka_raw", e["name"]],
        group_name="raw",
        kinds={"kafka", "snowflake"},
        description=f"Raw records from Kafka topic {e['topic']}, loaded by the Snowflake Kafka Connector.",
    )
    for e in PIPELINE["entities"]
]


@dbt_assets(manifest=DBT_PROJECT.manifest_path, project=DBT_PROJECT, dagster_dbt_translator=MedallionTranslator())
def dbt_medallion(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """bronze -> silver -> gold; dbt tests are reported as asset checks."""
    yield from dbt.cli(["build"], context=context).stream()


medallion = dg.define_asset_job("medallion", selection=dg.AssetSelection.assets(dbt_medallion))

defs = dg.Definitions(
    assets=[*kafka_raw, dbt_medallion],
    jobs=[medallion],
    schedules=[dg.ScheduleDefinition(job=medallion, cron_schedule="*/15 * * * *")],
    resources={"dbt": DbtCliResource(project_dir=DBT_PROJECT)},
)
