# Snowflake target: Snowflake Kafka Connector lands raw tables, dbt builds bronze/silver/gold.
DBT := dbt --no-use-colors
DBT_ARGS := --project-dir dbt --profiles-dir dbt
KAFKA_CONNECT_URL ?= http://localhost:8083
CONNECTOR_NAME := $(shell $(PYTHON) -c "import json;print(json.load(open('ingestion/connector.json'))['name'])")

.PHONY: install deploy-connector connector-status transform test

install: ## Install Python dependencies
	$(PYTHON) -m pip install -r requirements-dev.txt

deploy-connector: ## Create/update the Snowflake sink on Kafka Connect ($$KAFKA_CONNECT_URL)
	$(PYTHON) -c "import json,sys;json.dump(json.load(open('ingestion/connector.json'))['config'],sys.stdout)" \
	  | curl -fsS -X PUT -H 'Content-Type: application/json' --data @- \
	    $(KAFKA_CONNECT_URL)/connectors/$(CONNECTOR_NAME)/config
	@echo

connector-status: ## Show connector and task status
	curl -fsS $(KAFKA_CONNECT_URL)/connectors/$(CONNECTOR_NAME)/status
	@echo

transform: ## dbt build: bronze -> silver -> gold, with tests
	$(DBT) build $(DBT_ARGS)

test: ## Offline checks: generated files, connector config, dbt parse
	$(PYTHON) -m pytest -v tests
