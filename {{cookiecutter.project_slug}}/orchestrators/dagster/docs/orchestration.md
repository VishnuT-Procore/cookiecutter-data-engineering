# Orchestration: Dagster

[Dagster](https://dagster.io/) (Apache-2.0, self-hosted; the paid Dagster+ is not required) runs the pipeline as
assets, with lineage from landing to gold:

| Assets | Materialized by |
|--------|-----------------|
| `landing/<entity>` | The Flink landing job (external assets, shown for lineage) |
| `bronze/*`, `silver/*`, `gold/*` | dbt, through `dagster-dbt`. Every dbt test becomes an **asset check** |

* Job `medallion` (dbt build), scheduled every 5 minutes.
* Sensor `ai_ops` runs `scripts/pipeline.py ops check` every 2 minutes, and `ai_ops_on_run_failure` runs it after
  any failed run. AI ops diagnoses the problem, alerts, and in `act` mode self-heals (see `docs/ai_ops.md`).
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
