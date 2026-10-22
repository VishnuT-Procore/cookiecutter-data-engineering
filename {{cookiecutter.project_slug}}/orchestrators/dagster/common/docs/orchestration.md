# Orchestration: Dagster

[Dagster](https://dagster.io/) (Apache-2.0, self-hosted) orchestrates the pipeline as **assets**, so bronze, silver
and gold show up as a lineage graph with run history. Only the open-source version is used; Dagster+ (the paid
hosted control plane) is optional and not required.

```bash
make install
make dagster            # UI on http://localhost:3000
make dagster-validate   # CI-friendly check that definitions load
```

Everything lives in `orchestration/definitions.py`:

| Target | Assets | Job / schedule |
|--------|--------|----------------|
| Postgres (batch) | `kafka_raw/<entity>` (runs `ingestion/kafka_to_raw.py --once`), then every dbt model as `bronze/*`, `silver/*`, `gold/*`. dbt tests become **asset checks**. | `medallion` job, every 15 min |
| Postgres + Flink | `flink/bronze_<e>`, `flink/silver_<e>` as external assets (written by the Flink job), dbt views and gold | `medallion` (dbt) every 5 min, plus sensor `flink_job_running` that fails its tick if the Flink job is not RUNNING |
| Snowflake | `kafka_raw/<entity>` as external assets (loaded by the Kafka connector), dbt models and checks | `medallion` job, every 15 min |
| Databricks | `bronze/*`, `silver/*`, `gold/*` materialized by triggering the bundle job `<project>-medallion` (databricks-sdk) | `medallion` job, every 15 min |

Schedules and sensors are created **stopped**. Turn them on in the UI (Automation tab) or set
`default_status=RUNNING` in the code.

On Databricks, leave the bundle job's own schedule paused (the default), so Dagster is the only scheduler. When
the bundle is deployed with `-t dev`, the job name gets a prefix; set `DATABRICKS_JOB_NAME` to match it.

`dagster dev` loads `.env` from the project directory, so the same variables as `make transform` apply.

## Production

Run the open-source deployment: `dagster-webserver` + `dagster-daemon` with a Postgres run storage, either
with Docker or with the official Helm chart (`dagster/dagster`). See
https://docs.dagster.io/deployment/oss for details. The code location is this project
(`-m orchestration.definitions`).
