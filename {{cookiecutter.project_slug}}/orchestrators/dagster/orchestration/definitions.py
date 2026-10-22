"""Dagster definitions: target-agnostic.

* landing/<entity>  external assets: Iceberg tables kept up to date by the Flink landing job
* bronze/silver/gold: every dbt model is an asset, every dbt test an asset check (dbt adapter = target)
* job `medallion` + schedule
* AI ops sensors (config/ops.yml): periodic health check and a check on every failed run; they alert
  and, in `act` mode, self-heal (redeploy landing, re-run failed models) through scripts/pipeline.py

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


def _ops_check(trigger: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "pipeline.py"), "ops", "check", "--trigger", trigger],
        cwd=ROOT, capture_output=True, text=True, timeout=1800,
    )


def _report(result: subprocess.CompletedProcess) -> dg.SkipReason:
    """Exit 0 = healthy, 1 = incidents handled by ai ops (alerted/acted), 2 = ai ops itself failed."""
    if result.returncode >= 2:
        raise RuntimeError((result.stdout + result.stderr).strip() or "ops check failed")
    return dg.SkipReason(result.stdout.strip())


@dg.sensor(minimum_interval_seconds=120, default_status=dg.DefaultSensorStatus.STOPPED)
def ai_ops(context: dg.SensorEvaluationContext):
    """Periodic health check: landing job, Kafka lag, last dbt run. Alerts and self-heals per config/ops.yml."""
    return _report(_ops_check("schedule"))


@dg.run_failure_sensor(default_status=dg.DefaultSensorStatus.STOPPED)
def ai_ops_on_run_failure(context: dg.RunFailureSensorContext):
    """A failed run triggers an immediate check (e.g. re-run failed dbt models, or alert with the cause)."""
    result = _ops_check(f"run_failed:{context.dagster_run.job_name}")
    if result.returncode >= 2:
        raise RuntimeError((result.stdout + result.stderr).strip() or "ops check failed")


medallion = dg.define_asset_job("medallion", selection=dg.AssetSelection.assets(medallion_dbt))

defs = dg.Definitions(
    assets=[*landing_tables, medallion_dbt],
    jobs=[medallion],
    schedules=[dg.ScheduleDefinition(job=medallion, cron_schedule="*/5 * * * *")],
    sensors=[ai_ops, ai_ops_on_run_failure],
    resources={"dbt": DbtCliResource(project_dir=DBT_PROJECT)},
)
