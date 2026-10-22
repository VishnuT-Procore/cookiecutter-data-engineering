# Target: DuckDB (local)

DuckDB is the fully open-source, local target. It reads the Iceberg landing tables through the same REST catalog
the Flink job writes to (`ATTACH ... (TYPE iceberg)` in `dbt/profiles.yml`), and dbt builds bronze, silver and gold
into `warehouse.duckdb`. Use it for development and CI, and to try changes before running them on a cloud target.

```bash
make install       # Python deps + DuckDB extensions (from PyPI, so blocked extension hosts don't matter)
cp .env.example .env
make demo          # local lakehouse up, landing job, sample events, two incremental dbt builds
make test          # end-to-end test on the Docker Compose stack
```

| Variable | Meaning |
|----------|---------|
| `LANDING_CATALOG_URI`, `LANDING_WAREHOUSE` | Iceberg REST catalog as seen from this machine |
| `LANDING_S3_*` | Object storage credentials/endpoint for the data files |
| `LANDING_DATABASE` (`lake`), `LANDING_SCHEMA` | Where dbt finds the landing tables |
| `DUCKDB_PATH` | Output warehouse file (the Makefile sets an absolute path) |
