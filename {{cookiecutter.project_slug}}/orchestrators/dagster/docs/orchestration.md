# Orchestration: Dagster

[Dagster](https://dagster.io/) (Apache-2.0, self-hosted; the paid Dagster+ is not required) runs the pipeline as
assets, with lineage from landing to gold:

| Assets | Materialized by |
|--------|-----------------|
| `landing/<entity>` | The Flink landing job (external assets, shown for lineage) |
| `bronze/*`, `silver/*`, `gold/*` | dbt, through `dagster-dbt`. Every dbt test becomes an **asset check** |

* Job `medallion` (dbt build), scheduled every 5 minutes.
* Sensor `landing_job_running` calls `scripts/pipeline.py ingest status` and fails its tick when the Flink job
  isn't running.
* Both start **stopped**. Turn them on in the UI (Automation tab).

Nothing here depends on the target: the dbt profile decides where models run.

```bash
make install
make dagster            # UI on http://localhost:3000
make dagster-validate   # CI-friendly check that definitions load
```

## Replacing Dagster

The orchestrator only calls the pipeline's contract:

- `python scripts/pipeline.py ingest deploy|status|cancel`
- `python scripts/pipeline.py transform`, or `dbt build` directly

Airflow (with astronomer-cosmos), Prefect, Databricks Jobs, Kubernetes CronJobs or plain cron can run the same
commands. Add them as `orchestrators/<name>/` in the template.

## Production

Run `dagster-webserver` and `dagster-daemon` with Postgres run storage, either with Docker or with the official Helm
chart (`dagster/dagster`). See https://docs.dagster.io/deployment/oss for details. The code location is this project
(`-m orchestration.definitions`).
