# Apache Flink streaming engine: a continuous job keeps bronze/silver up to date in Postgres.
FLINK_UI := http://localhost:8081

.PHONY: flink-jars flink-submit flink-status flink-cancel demo

up: flink-jars

flink-jars: ## Download the pinned Flink connector jars (needed before `make up`)
	mkdir -p flink/build
	$(PYTHON) flink/fetch_jars.py

flink-submit: ## Create tables/topics and start the Flink job (bronze + silver)
	$(PYTHON) flink/submit.py
	$(COMPOSE) exec -T flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/build/pipeline.sql

flink-status: ## Show Flink jobs (UI: http://localhost:8081)
	@curl -fsS $(FLINK_UI)/jobs/overview | $(PYTHON) -m json.tool

flink-cancel: ## Cancel all running Flink jobs
	@for id in $$(curl -fsS $(FLINK_UI)/jobs/overview | $(PYTHON) -c "import json,sys;print(' '.join(j['jid'] for j in json.load(sys.stdin)['jobs'] if j['state']=='RUNNING'))"); do \
	  curl -fsS -X PATCH "$(FLINK_UI)/jobs/$$id?mode=cancel" && echo "cancelled $$id"; done

demo: up flink-submit ## Full local walkthrough: stream sample events through Flink, then build gold
	$(MAKE) sample-phase1
	sleep 15
	$(MAKE) sample-phase2
	sleep 15
	$(MAKE) transform
	@echo "Done. Flink UI: $(FLINK_UI). Try: psql -h localhost -U dbt warehouse -c 'select * from dev_gold.fct_orders'"
