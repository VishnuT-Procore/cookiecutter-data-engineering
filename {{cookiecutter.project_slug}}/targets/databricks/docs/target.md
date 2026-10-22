# Databricks target

```
Kafka ──► Structured Streaming ──► bronze.<entity> (Delta, append-only change log)
      ──► Structured Streaming foreachBatch + Delta MERGE ──► silver.<entity> (latest per key)
      ──► gold/*.sql (CREATE OR REPLACE TABLE ... AS SELECT) ──► gold.*
```

Built on open source: Apache Spark Structured Streaming and Delta Lake. It's deployed with a
[Databricks Asset Bundle](https://docs.databricks.com/en/dev-tools/bundles/index.html) (`databricks.yml`).

| Layer  | Code                         | Notes                                                                       |
|--------|------------------------------|-----------------------------------------------------------------------------|
| bronze | `src/medallion/bronze.py`    | One stream per topic. Every message is kept, typed via `from_json`, with `_op`, `_is_deleted`, Kafka metadata, `_raw_key` / `_raw_value`. Unparseable rows have `_is_valid = false`. Exactly-once through Delta and checkpoints. |
| silver | `src/medallion/silver.py`    | Streams bronze, keeps the newest change per key in each micro-batch, then `MERGE ... WHEN MATCHED AND <source is newer>`. Replays and out-of-order events never win. Deletes are soft (`_is_deleted`). |
| gold   | `gold/*.sql`                 | Run in name order by `medallion gold`. `${bronze}`, `${silver}` and `${gold}` are replaced with `<catalog>.<schema>`. |

## Run

```bash
python -m venv .venv && source .venv/bin/activate
make install && make test               # local Spark + Delta (Java 17 required)

databricks auth login --host <workspace-url>
# edit the variables in databricks.yml (catalog, kafka_bootstrap_servers, checkpoint_root, ...)
make validate deploy run                # BUNDLE_TARGET=dev by default
```

**Kafka credentials:** create a secret scope with a key `kafka-options` that holds a JSON object of extra Kafka source
options, for example `{"kafka.security.protocol": "SASL_SSL", "kafka.sasl.mechanism": "PLAIN", "kafka.sasl.jaas.config": "..."}`.
Then set the bundle variable `kafka_secret_scope`.

## Operate

* **Batch-style (default):** `trigger=availableNow`. The job runs bronze → silver → gold every 15 minutes
  (unpause the schedule).
* **Continuous:** set `trigger` to an interval (e.g. `"1 minute"`) and run bronze and silver as separate
  continuous jobs. Schedule gold on its own.
* **New columns:** add them to `config/pipeline.yml`. Schema auto-merge adds them to bronze and silver.
* **Reprocess:** delete the silver checkpoint directory and the silver table, then rerun silver. It replays bronze.
