# Target: Databricks

```
Flink ──► Unity Catalog Iceberg REST endpoint ──► <uc_catalog>.<landing>.<entity>  (managed Iceberg, append-only)
                                                        │  dbt-databricks
                                                        ▼
                                      bronze ──► silver ──► gold   (Delta tables)
```

## Setup

1. In Unity Catalog, enable external data access on the metastore and create the landing schema. Give the
   principal Flink uses `EXTERNAL USE SCHEMA` plus write privileges on it.
2. Point the landing job at Unity Catalog's Iceberg REST endpoint in `.env`:
   ```
   ICEBERG_CATALOG_URI=https://<workspace-host>/api/2.1/unity-catalog/iceberg-rest
   ICEBERG_WAREHOUSE=<uc_catalog>
   ICEBERG_CATALOG_PROPERTIES={"token": "<token>", "header.X-Iceberg-Access-Delegation": "vended-credentials"}
   ```
   For a service principal, use the Iceberg OAuth properties (`credential`, `oauth2-server-uri`, `scope`) instead of
   `token`. Then run `make ingest-deploy`.
3. `cp .env.example .env`, fill in `DATABRICKS_*` and `LANDING_DATABASE=<uc_catalog>`, then run
   `make install transform`.

## Notes

* External engines writing to managed Iceberg tables through Unity Catalog launched as Public Preview, and support
  is still changing. Check the current Databricks documentation for status and limitations.
* Landing tables are append-only, so Databricks never has to read Iceberg row-level deletes, which it doesn't
  support for Iceberg v2.
* Any other REST catalog that Unity Catalog can federate also works. Only the `ICEBERG_*` and `LANDING_*`
  variables change.
