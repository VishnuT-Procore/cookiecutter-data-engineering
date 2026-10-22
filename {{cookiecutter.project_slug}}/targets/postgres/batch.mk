# Batch ingestion (streaming_engine=none): Python consumer lands Kafka records in raw tables.
.PHONY: ingest ingest-stream demo

ingest: ## Land all available Kafka records in raw tables, then exit
	$(PYTHON) ingestion/kafka_to_raw.py --once

ingest-stream: ## Land Kafka records continuously
	$(PYTHON) ingestion/kafka_to_raw.py

demo: up ## Full local walkthrough: two phases of sample events through every layer
	$(MAKE) sample-phase1 ingest transform
	$(MAKE) sample-phase2 ingest transform
	@echo "Done. Try: psql -h localhost -U dbt warehouse -c 'select * from dev_gold.fct_orders'"
