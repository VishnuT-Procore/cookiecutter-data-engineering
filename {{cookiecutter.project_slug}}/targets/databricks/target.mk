# Databricks target: Spark Structured Streaming -> Delta bronze, MERGE -> silver, SQL -> gold.
BUNDLE_TARGET ?= dev

.PHONY: install test validate deploy run

install: ## Install the package with local Spark + Delta for tests
	$(PYTHON) -m pip install -r requirements-dev.txt

test: ## Run bronze/silver/gold on local Spark + Delta Lake (needs Java 17)
	$(PYTHON) -m pytest -v tests

validate: ## Validate the Databricks Asset Bundle
	databricks bundle validate -t $(BUNDLE_TARGET)

deploy: ## Build the wheel and deploy the bundle
	databricks bundle deploy -t $(BUNDLE_TARGET)

run: ## Run the bronze -> silver -> gold job now
	databricks bundle run medallion -t $(BUNDLE_TARGET)
