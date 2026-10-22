# Postgres target: Python consumer lands Kafka records in raw tables, dbt builds bronze/silver/gold.
DBT := dbt --no-use-colors
DBT_ARGS := --project-dir dbt --profiles-dir dbt

.PHONY: install ingest ingest-stream transform demo test

install: ## Install Python dependencies
	$(PYTHON) -m pip install -r requirements-dev.txt

ingest: ## Land all available Kafka records in raw tables, then exit
	$(PYTHON) ingestion/kafka_to_raw.py --once

ingest-stream: ## Land Kafka records continuously
	$(PYTHON) ingestion/kafka_to_raw.py

transform: ## dbt build: bronze -> silver -> gold, with tests
	$(DBT) build $(DBT_ARGS)

demo: up ## Full local walkthrough: two phases of sample events through every layer
	$(MAKE) sample-phase1 ingest transform
	$(MAKE) sample-phase2 ingest transform
	@echo "Done. Try: psql -h localhost -U dbt warehouse -c 'select * from dev_gold.fct_orders'"

test: ## End-to-end test against Kafka + Postgres in Testcontainers (needs Docker)
	$(PYTHON) -m pytest -v tests
