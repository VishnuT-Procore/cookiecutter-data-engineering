SHELL := /bin/bash
PYTHON ?= python
PIPELINE := $(PYTHON) scripts/pipeline.py

-include .env
export

.DEFAULT_GOAL := help

.PHONY: help install install-plugins up down sample sample-phase1 sample-phase2 generate check-generated \
        ingest-deploy ingest-status ingest-cancel transform check test

help: ## Show available targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## Install Python dependencies (plugins hook extra steps into install-plugins)
	$(PYTHON) -m pip install -r requirements-dev.txt
	@$(MAKE) --no-print-directory install-plugins

install-plugins:

up: ## Start the local lakehouse (Kafka, Flink, Iceberg REST catalog, S3 storage)
	@mkdir -p ingestion/build
	$(PYTHON) ingestion/flink/fetch_jars.py
	docker compose up -d --wait --build

down: ## Stop local services and delete their volumes
	docker compose down -v

sample: ## Produce both phases of the sample change events to Kafka
	$(PYTHON) scripts/produce_sample_events.py --phase all

sample-phase1: ## Produce phase 1 (initial creates)
	$(PYTHON) scripts/produce_sample_events.py --phase 1

sample-phase2: ## Produce phase 2 (updates, delete, out-of-order replay)
	$(PYTHON) scripts/produce_sample_events.py --phase 2

generate: ## Regenerate files derived from config/pipeline.yml
	$(PYTHON) scripts/generate.py

check-generated: ## Fail if generated files are stale (CI)
	$(PYTHON) scripts/generate.py --check

ingest-deploy: ## Start the Kafka -> Iceberg landing job (idempotent)
	$(PIPELINE) ingest deploy

ingest-status: ## Is the landing job running?
	$(PIPELINE) ingest status

ingest-cancel: ## Stop the landing job
	$(PIPELINE) ingest cancel

transform: ## dbt build on the target: bronze -> silver -> gold, with tests
	$(PIPELINE) transform

check: ## Generated files up to date and dbt project parses
	$(PIPELINE) check

test: ## Run the project's tests
	$(PYTHON) -m pytest -v tests

include target.mk
-include orchestration.mk
