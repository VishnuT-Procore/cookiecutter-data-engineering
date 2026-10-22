# Orchestrator: Dagster (open source). Definitions in orchestration/definitions.py.
DAGSTER_HOME ?= $(CURDIR)/.dagster_home
export DAGSTER_HOME

.PHONY: dagster dagster-validate

dagster: ## Start the Dagster UI on http://localhost:3000 (schedule and sensor start stopped)
	@mkdir -p $(DAGSTER_HOME)
	dagster dev -m orchestration.definitions

dagster-validate: ## Check that the Dagster definitions load
	@mkdir -p $(DAGSTER_HOME)
	dbt --quiet parse --project-dir dbt --profiles-dir dbt
	dagster definitions validate -m orchestration.definitions
