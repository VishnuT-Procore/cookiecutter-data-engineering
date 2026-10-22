# Target: DuckDB (local). Reads Iceberg landing via the REST catalog; bronze/silver/gold in warehouse.duckdb.
export DUCKDB_PATH ?= $(CURDIR)/warehouse.duckdb
export LANDING_DATABASE ?= lake

.PHONY: duckdb-extensions demo tables

install-plugins: duckdb-extensions

duckdb-extensions:
	$(PYTHON) -c "import duckdb_extensions as d; [d.import_extension(e) for e in ('httpfs', 'avro', 'iceberg')]"

demo: up ingest-deploy ## Local walkthrough: land sample events in Iceberg, then build bronze/silver/gold
	$(MAKE) sample-phase1
	@echo "waiting for the landing commit..." && sleep 20
	$(MAKE) transform
	$(MAKE) sample-phase2
	@echo "waiting for the landing commit..." && sleep 20
	$(MAKE) transform
	@echo "Done. Try: make tables, or open $(DUCKDB_PATH) with any DuckDB client."

tables: ## List the tables in the local DuckDB warehouse
	$(PYTHON) -c "import duckdb; c = duckdb.connect('$(DUCKDB_PATH)'); [print(r) for r in c.sql('show all tables').fetchall()]"
