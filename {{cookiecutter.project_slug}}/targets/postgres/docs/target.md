# Postgres target

```
Kafka ──► ingestion/kafka_to_raw.py ──► raw.<entity> (jsonb) ──► dbt: bronze ──► silver ──► gold
```

| Layer  | Where                       | How                                                                  |
|--------|-----------------------------|----------------------------------------------------------------------|
| raw    | `<raw_schema>.<entity>`     | Python consumer (confluent-kafka). PK `(kafka_partition, kafka_offset)`, `ON CONFLICT DO NOTHING`, DB commit before offset commit, so ingestion is effectively once. Bad JSON goes to `<raw_schema>._dead_letter`. |
| bronze | `<schema>_bronze.bronze_<e>` | dbt incremental (`delete+insert`) on `(partition, offset)`: typed change log with `_op`, `_is_deleted`, Kafka metadata. |
| silver | `<schema>_silver.silver_<e>` | dbt incremental on the primary key: the latest change per key; out-of-order events can't overwrite newer rows. |
| gold   | `<schema>_gold.*`           | Hand-written dbt models in `dbt/models/gold/`. |

On the `prod` dbt target the schemas are exactly `bronze`, `silver` and `gold`.

## Run locally

```bash
python -m venv .venv && source .venv/bin/activate
make install
cp .env.example .env
make demo          # docker compose up, 2 phases of sample events, ingest + dbt build after each
make test          # Testcontainers end-to-end test
```

## Operate

* **Continuous ingestion:** `make ingest-stream`. Run one or more replicas with the same `KAFKA_GROUP_ID`; Kafka
  balances the partitions across them.
* **Transform schedule:** run `make transform` (`dbt build`) from any scheduler (cron, Airflow, Dagster,
  Argo). Incremental models only read rows newer than their watermark, minus `lookback_minutes`.
* **Reprocess:** `dbt build --full-refresh --select silver_<e>+` rebuilds from bronze; bronze rebuilds from raw.
* **Kafka auth:** set `KAFKA_CONF_SECURITY_PROTOCOL`, `KAFKA_CONF_SASL_MECHANISM`, `KAFKA_CONF_SASL_USERNAME`,
  `KAFKA_CONF_SASL_PASSWORD` (any `KAFKA_CONF_*` variable is passed to librdkafka).
* **Retention:** raw and bronze grow forever. Partition or prune them by `ingested_at` once silver has caught up.
