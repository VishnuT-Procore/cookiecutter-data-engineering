# AI ops: alerting and self-healing

A thin decision layer on top of the pipeline. **The LLM only decides. Deterministic code observes, enforces the
guardrails and acts.** It runs through your own LLM gateway, and without one it falls back to built-in rules.

```
signals (Flink REST, Kafka lag, dbt run_results)
   └─► rules: incidents + baseline decision        always runs
         └─► LLM via LLM_GATEWAY_URL (optional)    refines severity, cause, summary, actions
               └─► guardrails                       allowlist, mode, cooldown, hourly cap, never silence critical
                     ├─► actions (act mode)          redeploy_landing | rerun_transform | rerun_failed_models
                     ├─► Slack-compatible alert      deduplicated, with a resolved notice
                     └─► audit log                   .ops/audit/YYYY-MM-DD.jsonl
```

## Configure

`.env`:

```bash
LLM_GATEWAY_URL=https://llm-gateway.example.com/v1   # any OpenAI-compatible /chat/completions endpoint
LLM_API_KEY=...                                      # sent as "Authorization: Bearer ..."
LLM_MODEL=claude-sonnet-5-5                          # whatever model name your gateway routes
ALERT_WEBHOOK_URL=https://hooks.slack.com/services/...
```

Gateways that expose that API include LiteLLM, Portkey, Kong AI Gateway, OpenRouter, Azure OpenAI and vLLM, so the
model can be Claude, GPT, Gemini or a local model. Policy lives in `config/ops.yml`: mode, thresholds, which actions
are allowed, cooldowns, hourly caps and alert severity.

| Mode | Behaviour |
|------|-----------|
| `off` | No checks |
| `advise` (default) | Diagnose and alert, with recommended actions. Never changes anything |
| `act` | Also runs allowlisted actions within cooldowns and hourly caps. The alert reports what was done |

## Run

```bash
make ops-explain      # print the snapshot, incidents and decision; no alert, no action
make ops-check        # the real check: decide, act (in act mode), alert, audit
make ops-test-alert   # verify the webhook
```

With Dagster, sensor `ai_ops` runs the check every 2 minutes and `ai_ops_on_run_failure` runs it after any failed
run. Without an orchestrator, schedule `python scripts/pipeline.py ops check` (cron, Kubernetes CronJob, …).

## What is detected

| Incident | Baseline severity | Baseline action |
|----------|-------------------|-----------------|
| `landing_down`: landing job not RUNNING | critical | `redeploy_landing` |
| `flink_unreachable` | critical | notify only |
| `checkpoint_stale`: no Iceberg commit for `checkpoint_max_age_minutes` | warning | `redeploy_landing` |
| `consumer_lag_high` | warning | notify only |
| `dbt_model_failed` | warning | `rerun_failed_models` |
| `dbt_test_failed`: data quality | warning | notify only |
| `transform_stale`: last dbt run older than `transform_max_age_minutes` | warning | `rerun_transform` |

## What the LLM sees and decides

* **Input:** the redacted health snapshot (job state, checkpoint ages, the last exception truncated, lag per topic,
  failed dbt nodes and their messages), the incidents and the baseline decision. **No data rows are sent.** Keys and
  patterns that look like secrets are removed (`ops/redact.py`).
* **Output:** one JSON object with severity, summary, root cause, actions (allowlisted names only), whether to alert,
  and a confidence. Anything else is rejected, and the rules decide instead.
* **Guardrails in code:**
  - unknown actions are dropped and logged;
  - actions only run in `act` mode and only if allowed;
  - cooldowns and hourly caps apply;
  - a critical incident is always alerted and never downgraded below warning.

## Extend

* **New signal:** add a collector in `ops/signals.py` and a rule in `ops/rules.py`.
* **New action:** add it to `ops/rules.py:CATALOG` and `ops/actions.py:RUNNERS`, and allow it in `config/ops.yml`.
  Actions should call `scripts/pipeline.py`, so they stay target-agnostic.
* **Other alert channels** (Teams, PagerDuty, email): add a sender in `ops/alerts.py`.
