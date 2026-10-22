# Postgres target: Python consumer lands Kafka records in raw tables, dbt builds bronze/silver/gold.
DBT := dbt --no-use-colors
DBT_ARGS := --project-dir dbt --profiles-dir dbt

.PHONY: install transform test

install: ## Install Python dependencies
	$(PYTHON) -m pip install -r requirements-dev.txt

transform: ## dbt build: bronze -> silver -> gold, with tests
	$(DBT) build $(DBT_ARGS)

-include batch.mk

test: ## End-to-end test against Kafka + Postgres in Testcontainers (needs Docker)
	$(PYTHON) -m pytest -v tests
