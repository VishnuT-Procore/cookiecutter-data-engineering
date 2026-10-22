# Streaming engine: Apache Flink

```
Kafka ──► Flink SQL job ──► <raw_schema>.bronze_<e>   (every change, upsert on partition/offset)
                        └─► <raw_schema>.silver_<e>   (latest per key, upsert on primary key)
                                       │
            dbt: bronze_<e> / silver_<e> views + tests ──► gold models
```

[Apache Flink](https://flink.apache.org/) 1.20 (LTS) runs one continuous job, generated from `config/pipeline.yml`
as `flink/sql/pipeline.sql`. Silver is usually a few seconds behind Kafka. dbt still builds gold and runs the data
tests; bronze and silver are dbt *views* over the Flink tables, so gold models are unchanged.

| Piece | Details |
|-------|---------|
| Source | Kafka connector, raw key/value + `topic/partition/offset/timestamp` metadata. Tombstones arrive as NULL values. |
| Parsing | `JSON_VALUE`/`JSON_QUERY`. Debezium (`before`/`after`/`op`/`ts_ms`, optional `{schema,payload}` wrapper) or plain JSON with tombstone deletes. |
| Bronze | JDBC upsert sink keyed on `(_kafka_partition, _kafka_offset)`, so replays are idempotent. |
| Silver | `ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY <sequence> DESC) = 1` (Flink Top-N) into a JDBC upsert sink. Late or replayed events never win, and deletes stay as `_is_deleted = true`. |
| Delivery | Checkpoints every `FLINK_CHECKPOINT_INTERVAL` (10s). Idempotent upserts give effectively-once results. |
| State | Flink keeps the current row per key. Set `FLINK_STATE_TTL` (e.g. `30 d`) to bound it; `0` keeps state forever. |

## Run locally

```bash
make install
make demo          # flink-jars, docker compose up (Kafka, Postgres, Flink), submit job, sample events, dbt gold
make flink-status  # or open http://localhost:8081
make test          # end-to-end test on the compose stack
```

`make flink-submit` creates the Postgres tables (`flink/sql/postgres_tables.sql`) and the Kafka topics, renders the
job with values from the environment (`FLINK_*`, `POSTGRES_USER/PASSWORD`) into `flink/build/pipeline.sql`, and runs
it with the Flink SQL client.

## Change the pipeline

Edit `config/pipeline.yml`, run `make generate`, then redeploy the job. New columns need a `ALTER TABLE ... ADD
COLUMN` on the bronze/silver tables, since `create table if not exists` won't change existing tables.

Redeploy: stop with a savepoint (`flink stop --savepointPath ...`), then resubmit with
`SET 'execution.savepoint.path' = '...'`. Changing the SQL of a stateful query (silver) may need a fresh start. In
that case silver is rebuilt from Kafka, from the earliest retained offsets.

## Production

* Run Flink with the [Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/)
  (Apache-2.0), using a `FlinkSessionJob` or the SQL runner pattern, or use a managed Flink service.
* Use durable checkpoint storage (S3/GCS/ADLS) via `state.checkpoints.dir`, plus a state backend sized for the number
  of keys (RocksDB for large state).
* Pass credentials through the environment or Kubernetes secrets, never in the committed SQL. Placeholders are only
  filled when the job is rendered.
* Schedule dbt (gold) every few minutes, or with the Dagster option if enabled.
