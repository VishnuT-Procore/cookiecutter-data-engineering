# Target: Snowflake

```
Flink ──► Iceberg landing (REST catalog + object storage)
              │  catalog integration + external volume, AUTO_REFRESH
              ▼
        <DB>.<LANDING>.<ENTITY>  (Snowflake Iceberg tables, read-only)
              │  dbt-snowflake
              ▼
        bronze ──► silver ──► gold   (native Snowflake tables)
```

## Setup

1. Point the landing job at a REST catalog Snowflake can reach: Apache Polaris / Snowflake Open Catalog, AWS Glue
   (Iceberg REST), Unity Catalog, and so on. Set `ICEBERG_CATALOG_URI`, `ICEBERG_WAREHOUSE` and
   `ICEBERG_CATALOG_PROPERTIES` in `.env`, then run `make ingest-deploy`.
2. Run `snowflake/setup.sql` once: database, warehouse, external volume, catalog integration, dbt role and user.
3. Run `snowflake/landing_tables.sql`. It is generated from `config/pipeline.yml` and has one
   `CREATE ICEBERG TABLE … CATALOG_TABLE_NAME … AUTO_REFRESH = TRUE` per entity. Re-run it after adding entities.
4. `cp .env.example .env`, fill in the dbt credentials, then run `make install transform`.

## Notes

* Landing tables are append-only, so Snowflake never has to read Iceberg row-level deletes.
* Freshness in Snowflake = Flink commit interval + catalog refresh interval (set `REFRESH_INTERVAL_SECONDS` on the
  integration if needed) + dbt schedule.
* Landing column names are lower-case Iceberg names and are quoted by `snowflake__landing_col`. If your
  integration resolves identifiers case-insensitively, set `vars: {quote_landing_columns: false}`.
* Verify the integration with `SELECT SYSTEM$VERIFY_CATALOG_INTEGRATION('<name>')`.
