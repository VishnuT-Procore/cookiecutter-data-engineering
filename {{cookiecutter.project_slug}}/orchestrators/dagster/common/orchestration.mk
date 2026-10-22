# Dagster (open source) orchestration: assets, schedules and sensors in orchestration/definitions.py.
DAGSTER_HOME ?= $(CURDIR)/.dagster_home
export DAGSTER_HOME

.PHONY: install-orchestration dagster dagster-validate

install: install-orchestration

install-orchestration:
	$(PYTHON) -m pip install -r requirements-orchestration.txt

dagster: ## Start the Dagster UI on http://localhost:3000 (schedules/sensors start stopped)
	@mkdir -p $(DAGSTER_HOME)
	dagster dev -m orchestration.definitions

dagster-validate: ## Check that the Dagster definitions load
	@mkdir -p $(DAGSTER_HOME)
	@if [ -d dbt ]; then dbt --quiet parse --project-dir dbt --profiles-dir dbt; fi
	dagster definitions validate -m orchestration.definitions
