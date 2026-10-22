# {{ cookiecutter.project_name }}

Kafka → Apache Iceberg landing → **{{ cookiecutter.target }}** medallion pipeline{% if cookiecutter.orchestrator != 'none' %}, orchestrated by **{{ cookiecutter.orchestrator }}**{% endif %}.
Generated from the data-engineering cookiecutter.

```mermaid
flowchart LR
    K[(Kafka)] -->|Flink, append-only<br/>seconds| L[(Iceberg landing<br/>open format)]
    L -->|dbt on {{ cookiecutter.target }}| B[bronze<br/>every change]
    B --> S[silver<br/>latest per key]
    S --> G[gold<br/>joins & breakdowns]
```

The pipeline is built from contracts, so the target and the orchestrator are replaceable plugins:

| Contract | Defined by | Pluggable part |
|----------|------------|----------------|
| **Landing**: one append-only Iceberg table per topic (raw key/value + Kafka metadata) | `ingestion/sql/landing.sql` (generated) | Object storage, Kafka cluster |
| **Catalog**: Iceberg REST API | `ICEBERG_CATALOG_*` in `.env` | Apache Polaris, Unity Catalog, AWS Glue, Snowflake Open Catalog, Lakekeeper, ... |
| **Transform**: one dbt project, bronze → silver → gold | `dbt/` | dbt adapter + `dbt/macros/adapter_<target>.sql` (5 small macros) |
| **Run**: `scripts/pipeline.py` (`ingest deploy\|status\|cancel`, `transform`, `check`) | `scripts/pipeline.py` | Any scheduler/orchestrator calls these |

| Layer  | Contents | Guarantees |
|--------|----------|------------|
| landing | Raw Kafka records in Iceberg, committed every `FLINK_CHECKPOINT_INTERVAL` (10 s) | Append-only, exactly-once (Flink checkpoints); never fails on payload changes |
| bronze | One typed row per message ({{ cookiecutter.payload_format }}) + `_op`, `_is_deleted`, `_source_ts_ms`, `_kafka_*`, `_ingested_at` | Idempotent on (partition, offset): re-runs never duplicate |
| silver | Latest change per primary key | Out-of-order and replayed events never overwrite newer data; deletes are soft (`_is_deleted`) |
| gold   | Hand-written models: joins (`fct_orders`, `customer_order_summary`) and breakdowns (`dim_customer_addresses`) | Exclude deleted rows |

Target details: **[docs/target.md](docs/target.md)**.
AI ops (alerting and self-healing through your LLM gateway, mode `{{ cookiecutter.ai_ops_mode }}`): **[docs/ai_ops.md](docs/ai_ops.md)**.
{%- if cookiecutter.orchestrator != 'none' %}
Orchestration: **[docs/orchestration.md](docs/orchestration.md)**.
{%- endif %}

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
make install
cp .env.example .env
make help
```

`make up` starts the local lakehouse: Kafka, Flink with its SQL Gateway, an Iceberg REST catalog and S3-compatible
storage. `make ingest-deploy` starts the landing job. `make sample-phase1` and `make sample-phase2` produce a
deterministic set of customer/order changes: creates, updates, a delete and an out-of-order replay.

## Add or change an entity

1. Edit `config/pipeline.yml`: topic, primary key, typed columns.
2. `make generate` regenerates the landing job and the dbt bronze/silver models. CI runs `make check-generated`.
3. Redeploy the landing job (`python scripts/pipeline.py ingest deploy --replace`) and add or adjust gold models.

Supported column types: `string, integer, bigint, double, boolean, date, timestamp` (ISO-8601),
`timestamp_ms`, `timestamp_us` (epoch), `decimal(p,s)`. The `customers` / `orders` entities and the gold models are
examples; replace them with your own.

## Layout

```
config/pipeline.yml          single source of truth (entities, payload format, landing namespace)
ingestion/                   Kafka -> Iceberg landing (Flink SQL, target-agnostic)
  landing.py                 deploy / status / cancel via Flink SQL Gateway + REST
  sql/landing.sql            generated Flink job
  flink/                     Flink image: pinned Kafka + Iceberg jars
dbt/                         bronze/silver (generated) and gold models, adapter interface macros
scripts/pipeline.py          the run contract used by make, orchestrators and AI ops
ops/, config/ops.yml         AI ops: signals -> rules -> LLM decision -> guarded actions -> alerts
scripts/generate.py          validates config, renders generated files
compose.yaml                 local lakehouse
target.mk, docs/target.md    target plugin
```
