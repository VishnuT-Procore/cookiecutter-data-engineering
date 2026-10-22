# Snowflake target

```
Kafka ──► Kafka Connect + Snowflake Kafka Connector (Snowpipe Streaming) ──► RAW.<ENTITY>
      ──► dbt: bronze ──► silver ──► gold
```

| Layer  | Where                         | How                                                                        |
|--------|-------------------------------|----------------------------------------------------------------------------|
| raw    | `<DB>.<RAW_SCHEMA>.<ENTITY>`  | [Snowflake Kafka Connector](https://github.com/snowflakedb/snowflake-kafka-connector) (Apache-2.0) using Snowpipe Streaming. Tables have `RECORD_METADATA` and `RECORD_CONTENT` VARIANT columns. Records that fail conversion go to the DLQ topic `<project>.dlq.snowflake`. |
| bronze | `<schema>_BRONZE.BRONZE_<E>`  | dbt incremental `merge` on `(partition, offset)`: typed change log with `_op`, `_is_deleted` and Kafka metadata. |
| silver | `<schema>_SILVER.SILVER_<E>`  | dbt incremental `merge` on the primary key: the latest change per key. Out-of-order events can't overwrite newer rows. |
| gold   | `<schema>_GOLD.*`             | Hand-written dbt models in `dbt/models/gold/`. |

## Set up

1. Run `ingestion/setup.sql` in Snowflake. It creates the database, raw schema, roles, service users and warehouse.
2. On your Kafka Connect cluster, install the Snowflake Kafka Connector jar (Maven Central:
   `com.snowflake:snowflake-kafka-connector`) and enable the file config provider (see
   `ingestion/snowflake.properties.example`).
3. Deploy the generated connector config: `KAFKA_CONNECT_URL=http://connect:8083 make deploy-connector`.
   `ingestion/connector.json` is regenerated from `config/pipeline.yml` by `make generate`.
4. `cp .env.example .env`, fill in the dbt credentials, then run `make install transform`.

Debezium sources should use `value.converter.schemas.enable=false`. Schema-wrapped envelopes are also handled.

## Operate

* **Schedule:** run `make transform` from any scheduler. Each run reads only raw rows newer than the
  bronze watermark (`SnowflakeConnectorPushTime`) minus `lookback_minutes`.
* **Reprocess:** `dbt build --full-refresh --select silver_<e>+`.
* **Lower latency:** run dbt more often, or turn silver into a Snowflake Dynamic Table over bronze using the same
  `qualify row_number()` logic.
