SHELL := /bin/bash
PYTHON ?= python

-include .env
export

.DEFAULT_GOAL := help

.PHONY: help up down sample sample-phase1 sample-phase2 generate check-generated

help: ## Show available targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

up: ## Start local Kafka (and target services) with Docker Compose
	docker compose up -d --wait

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

include target.mk
