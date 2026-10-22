"""Dagster definitions for the Databricks target.

Bronze, silver and gold Delta tables are materialized by triggering the Databricks Asset Bundle job
(`databricks bundle deploy` creates it) and waiting for it with the Databricks SDK. Authentication
uses the standard SDK configuration (DATABRICKS_HOST/DATABRICKS_TOKEN or ~/.databrickscfg).
"""

import os

import dagster as dg

from orchestration.pipeline import ROOT, load_pipeline

PIPELINE = load_pipeline()
# Bundles deployed with `-t dev` prefix the job name, e.g. "[dev alice] <project>-medallion".
JOB_NAME = os.environ.get("DATABRICKS_JOB_NAME", f"{PIPELINE['project']}-medallion")

bronze = [
    dg.AssetSpec(["bronze", e["name"]], group_name="bronze", kinds={"databricks", "delta"},
                 description=f"Change log of Kafka topic {e['topic']}.")
    for e in PIPELINE["entities"]
]
silver = [
    dg.AssetSpec(["silver", e["name"]], group_name="silver", kinds={"databricks", "delta"},
                 deps=[["bronze", e["name"]]], description=f"Latest version of each {e['name']} row.")
    for e in PIPELINE["entities"]
]
gold = [
    dg.AssetSpec(["gold", path.stem.split("_", 1)[-1]], group_name="gold", kinds={"databricks", "delta"},
                 deps=[s.key for s in silver], description=f"Built by gold/{path.name}.")
    for path in sorted((ROOT / "gold").glob("*.sql"))
]


@dg.multi_asset(specs=[*bronze, *silver, *gold])
def medallion_tables(context: dg.AssetExecutionContext):
    """Run the bundle job (bronze -> silver -> gold) and wait for it to finish."""
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    jobs = [j for j in client.jobs.list(name=JOB_NAME)]
    if len(jobs) != 1:
        raise RuntimeError(f"expected one Databricks job named '{JOB_NAME}', found {len(jobs)}; set DATABRICKS_JOB_NAME")
    run = client.jobs.run_now(job_id=jobs[0].job_id).result()  # raises if the run fails
    context.log.info(f"Databricks run finished: {run.run_page_url}")
    for spec in context.assets_def.specs:
        yield dg.MaterializeResult(asset_key=spec.key, metadata={"run_url": dg.MetadataValue.url(run.run_page_url)})


medallion = dg.define_asset_job("medallion", selection=dg.AssetSelection.assets(medallion_tables))

defs = dg.Definitions(
    assets=[medallion_tables],
    jobs=[medallion],
    schedules=[dg.ScheduleDefinition(job=medallion, cron_schedule="*/15 * * * *")],
)
