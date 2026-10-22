"""Dagster definitions: target-agnostic.

* landing/<entity>  external assets: Iceberg tables kept up to date by the Flink landing job
* bronze/silver/gold: every dbt model is an asset, every dbt test an asset check (dbt adapter = target)
* job `medallion` + schedule, and a sensor that fails its tick when the landing job is not running

Everything goes through the pipeline's own contract (dbt project, scripts/pipeline.py), so changing the
target or replacing Dagster needs no change here.
"""

import subprocess
import sys
from pathlib import Path

import dagster as dg
import yaml
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from orchestration.dbt_translator import MedallionTranslator

ROOT = Path(__file__).resolve().parents[1]
PIPELINE = yaml.safe_load((ROOT / "config" / "pipeline.yml").read_text())

DBT_PROJECT = DbtProject(project_dir=ROOT / "dbt", profiles_dir=ROOT / "dbt")
DBT_PROJECT.prepare_if_dev()  # `dbt parse` when running under `dagster dev`

landing_tables = [
    dg.AssetSpec(
        ["landing", e["name"]],
        group_name="landing",
        kinds={"kafka", "flink", "iceberg"},
        description=f"Raw records from Kafka topic {e['topic']}, landed continuously by the Flink job.",
    )
    for e in PIPELINE["entities"]
]


@dbt_assets(manifest=DBT_PROJECT.manifest_path, project=DBT_PROJECT, dagster_dbt_translator=MedallionTranslator())
def medallion_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """bronze -> silver -> gold; dbt tests are reported as asset checks."""
    yield from dbt.cli(["build"], context=context).stream()


@dg.sensor(minimum_interval_seconds=60, default_status=dg.DefaultSensorStatus.STOPPED)
def landing_job_running(context: dg.SensorEvaluationContext):
    """Fails its tick (visible and alertable in Dagster) when the landing job is not running."""
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "pipeline.py"), "ingest", "status"],
        cwd=ROOT, capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError((result.stdout + result.stderr).strip() or "landing job is not running")
    return dg.SkipReason(result.stdout.strip())


medallion = dg.define_asset_job("medallion", selection=dg.AssetSelection.assets(medallion_dbt))

defs = dg.Definitions(
    assets=[*landing_tables, medallion_dbt],
    jobs=[medallion],
    schedules=[dg.ScheduleDefinition(job=medallion, cron_schedule="*/5 * * * *")],
    sensors=[landing_job_running],
    resources={"dbt": DbtCliResource(project_dir=DBT_PROJECT)},
)
