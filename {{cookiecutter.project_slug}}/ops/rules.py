"""Deterministic incident detection. Always runs, so the layer works without an LLM.
Each incident suggests a baseline action from the catalog."""

from __future__ import annotations

from ops.config import severity_rank

# Actions the decision layer may choose from; executed by ops/actions.py.
CATALOG = {
    "redeploy_landing": "Cancel (if needed) and redeploy the Kafka -> Iceberg landing job: pipeline.py ingest deploy --replace",
    "rerun_transform": "Run the full dbt build again: pipeline.py transform",
    "rerun_failed_models": "Re-run only failed dbt nodes and their children: dbt build --select result:error+ result:fail+",
    "notify_only": "Take no action; humans need to look (e.g. data-quality test failures, infrastructure down)",
}


def incidents(snap: dict, thresholds: dict) -> list[dict]:
    found = []

    def add(code, severity, summary, action, evidence):
        found.append({"code": code, "severity": severity, "summary": summary, "suggested_action": action,
                      "evidence": evidence})

    job = snap.get("landing_job", {})
    if not job.get("reachable", True):
        add("flink_unreachable", "critical", "The Flink REST API is unreachable; landing status unknown.",
            "notify_only", {"error": job.get("error")})
    elif job.get("state") != "RUNNING":
        add("landing_down", "critical", f"Landing job '{job.get('name')}' is {job.get('state')}.",
            "redeploy_landing", {"state": job.get("state"), "last_exception": job.get("last_exception")})
    else:
        cp = job.get("checkpoints", {})
        age = cp.get("last_completed_age_seconds")
        limit = thresholds.get("checkpoint_max_age_minutes", 5) * 60
        if age is not None and age > limit:
            add("checkpoint_stale", "warning",
                f"No successful checkpoint (Iceberg commit) for {age // 60} min; landing is stalled.",
                "redeploy_landing", {"checkpoints": cp, "last_exception": job.get("last_exception")})

    kafka = snap.get("kafka", {})
    if kafka.get("total", 0) > thresholds.get("consumer_lag_warning", 10000):
        add("consumer_lag_high", "warning", f"{kafka['total']} Kafka messages not yet landed.",
            "notify_only", {"lag": kafka.get("lag")})

    transform = snap.get("transform", {})
    problems = transform.get("problems", [])
    errors = [p for p in problems if p["status"] in ("error", "runtime error")]
    failures = [p for p in problems if p["status"] == "fail"]
    if errors:
        add("dbt_model_failed", "warning", f"{len(errors)} dbt node(s) errored in the last run.",
            "rerun_failed_models", {"errors": errors})
    if failures:
        add("dbt_test_failed", "warning", f"{len(failures)} data test(s) failed in the last run.",
            "notify_only", {"failures": failures})
    age = transform.get("age_seconds")
    if transform.get("found") and age is not None and age > thresholds.get("transform_max_age_minutes", 30) * 60:
        add("transform_stale", "warning", f"Last dbt run was {age // 60} min ago; silver/gold are stale.",
            "rerun_transform", {"age_seconds": age})
    return found


def baseline_decision(found: list[dict]) -> dict:
    if not found:
        return {"severity": "info", "summary": "All checks passed.", "root_cause": "", "actions": [],
                "send_alert": False, "decided_by": "rules"}
    severity = max((i["severity"] for i in found), key=severity_rank)
    actions = []
    for i in found:
        if i["suggested_action"] != "notify_only" and i["suggested_action"] not in [a["name"] for a in actions]:
            actions.append({"name": i["suggested_action"], "reason": i["summary"]})
    return {
        "severity": severity,
        "summary": "; ".join(i["summary"] for i in found),
        "root_cause": "",
        "actions": actions,
        "send_alert": True,
        "decided_by": "rules",
    }
