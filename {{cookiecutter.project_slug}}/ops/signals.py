"""Collects a health snapshot through the pipeline's own contracts. Never raises: a collector that fails
reports `error`, which the rules treat as a signal of its own."""

from __future__ import annotations

import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def _pipeline() -> dict:
    return yaml.safe_load((ROOT / "config" / "pipeline.yml").read_text())


def _get(url: str, timeout: float = 10) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def landing_job() -> dict:
    """State of the Flink landing job, its checkpoints (= Iceberg commits) and last exception."""
    base = os.environ.get("FLINK_REST_URL", "http://localhost:8081").rstrip("/")
    name = os.environ.get("FLINK_JOB_NAME", f"{_pipeline()['project']}-landing")
    try:
        jobs = [j for j in _get(f"{base}/jobs/overview")["jobs"] if j["name"] == name]
    except Exception as exc:  # Flink unreachable
        return {"name": name, "reachable": False, "error": f"{type(exc).__name__}: {exc}"}
    if not jobs:
        return {"name": name, "reachable": True, "state": "NOT_FOUND"}
    job = max(jobs, key=lambda j: j.get("start-time", 0))
    info = {"name": name, "reachable": True, "state": job["state"], "job_id": job["jid"],
            "started_at_ms": job.get("start-time")}
    try:
        counts = _get(f"{base}/jobs/{job['jid']}/checkpoints")
        latest = (counts.get("latest") or {}).get("completed") or {}
        info["checkpoints"] = {
            "completed": counts.get("counts", {}).get("completed"),
            "failed": counts.get("counts", {}).get("failed"),
            "last_completed_age_seconds": (
                round(time.time() - latest["latest_ack_timestamp"] / 1000) if latest.get("latest_ack_timestamp") else None
            ),
        }
    except Exception as exc:
        info["checkpoints"] = {"error": str(exc)}
    try:
        exceptions = _get(f"{base}/jobs/{job['jid']}/exceptions?maxExceptions=3")
        root = (exceptions.get("exceptionHistory", {}).get("entries") or [{}])[0]
        if root.get("stacktrace"):
            info["last_exception"] = root.get("exceptionName", "") + ": " + root["stacktrace"][:1500]
    except Exception:
        pass
    return info


def kafka_lag() -> dict:
    """Messages in Kafka not yet committed by the landing job's consumer group, per topic."""
    try:
        from confluent_kafka import Consumer, TopicPartition

        pipeline = _pipeline()
        group = os.environ.get("FLINK_KAFKA_GROUP_ID", f"{pipeline['project']}-landing")
        consumer = Consumer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                             "group.id": group, "enable.auto.commit": False})
        try:
            lag = {}
            for entity in pipeline["entities"]:
                topic = entity["topic"]
                metadata = consumer.list_topics(topic, timeout=10).topics.get(topic)
                if metadata is None or metadata.error is not None:
                    lag[topic] = None
                    continue
                partitions = [TopicPartition(topic, p) for p in metadata.partitions]
                committed = consumer.committed(partitions, timeout=10)
                total = 0
                for tp in committed:
                    _, high = consumer.get_watermark_offsets(tp, timeout=10)
                    total += max(high - (tp.offset if tp.offset >= 0 else 0), 0)
                lag[topic] = total
            return {"group": group, "lag": lag, "total": sum(v for v in lag.values() if v)}
        finally:
            consumer.close()
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def last_transform() -> dict:
    """Outcome of the last dbt invocation (dbt/target/run_results.json)."""
    path = ROOT / "dbt" / "target" / "run_results.json"
    if not path.exists():
        return {"found": False}
    try:
        results = json.loads(path.read_text())
    except Exception as exc:
        return {"found": False, "error": str(exc)}
    generated = results.get("metadata", {}).get("generated_at")
    age = None
    if generated:
        when = datetime.fromisoformat(generated.replace("Z", "+00:00"))
        age = round((datetime.now(timezone.utc) - when).total_seconds())
    problems = [
        {"node": r["unique_id"], "status": r["status"], "message": (r.get("message") or "")[:500]}
        for r in results.get("results", [])
        if r.get("status") in ("error", "fail", "runtime error")
    ]
    return {"found": True, "command": results.get("args", {}).get("which"), "age_seconds": age,
            "nodes": len(results.get("results", [])), "problems": problems}


def snapshot() -> dict:
    return {
        "taken_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "landing_job": landing_job(),
        "kafka": kafka_lag(),
        "transform": last_transform(),
    }
