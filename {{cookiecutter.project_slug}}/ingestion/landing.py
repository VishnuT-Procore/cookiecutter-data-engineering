#!/usr/bin/env python3
"""Kafka -> Iceberg landing: deploy and operate the Flink job, independent of any target platform.

    python ingestion/landing.py render    # write ingestion/build/landing.sql from ingestion/sql/landing.sql
    python ingestion/landing.py deploy    # create topics (and the local bucket), submit via the Flink SQL Gateway
    python ingestion/landing.py status    # exit 0 if the landing job is RUNNING, 1 otherwise
    python ingestion/landing.py cancel

The job is plain Flink SQL, generated from config/pipeline.yml. Endpoints and credentials come from the
environment, so the same job runs against a local stack, a Kubernetes Flink cluster or a managed service:

    FLINK_SQL_GATEWAY_URL      SQL Gateway REST endpoint            (default http://localhost:8083)
    FLINK_REST_URL             Flink REST endpoint                  (default http://localhost:8081)
    FLINK_KAFKA_BOOTSTRAP_SERVERS  brokers as seen by Flink         (default kafka:19092)
    FLINK_KAFKA_PROPERTIES     JSON of extra Kafka source options, e.g. {"properties.security.protocol": "SASL_SSL"}
    FLINK_CHECKPOINT_INTERVAL  Iceberg commit interval = landing freshness (default 10s)
    ICEBERG_CATALOG_URI        Iceberg REST catalog                 (default http://iceberg-rest:8181)
    ICEBERG_WAREHOUSE          catalog warehouse                    (default s3://warehouse/)
    ICEBERG_CATALOG_PROPERTIES JSON of extra catalog properties (io-impl, s3.endpoint, credential, token, ...)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SQL_TEMPLATE = ROOT / "ingestion" / "sql" / "landing.sql"
BUILD = ROOT / "ingestion" / "build" / "landing.sql"
PLACEHOLDER = re.compile(r"\$\{([A-Z0-9_]+)\}")

LOCAL_CATALOG_PROPERTIES = {
    "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "s3.endpoint": "http://storage:9000",
    "s3.path-style-access": "true",
}


def load_pipeline() -> dict:
    return yaml.safe_load((ROOT / "config" / "pipeline.yml").read_text())


def job_name() -> str:
    return os.environ.get("FLINK_JOB_NAME", f"{load_pipeline()['project']}-landing")


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _options(properties: dict) -> str:
    """Render extra WITH-options, each on its own line and prefixed by a comma."""
    return "".join(f",\n  {_sql_string(k)} = {_sql_string(v)}" for k, v in properties.items())


def settings(env: dict | None = None) -> dict[str, str]:
    env = dict(os.environ if env is None else env)
    project = load_pipeline()["project"]
    catalog_properties = json.loads(env.get("ICEBERG_CATALOG_PROPERTIES") or json.dumps(LOCAL_CATALOG_PROPERTIES))
    kafka_properties = json.loads(env.get("FLINK_KAFKA_PROPERTIES") or "{}")
    return {
        "FLINK_JOB_NAME": env.get("FLINK_JOB_NAME", f"{project}-landing"),
        "FLINK_CHECKPOINT_INTERVAL": env.get("FLINK_CHECKPOINT_INTERVAL", "10s"),
        "FLINK_KAFKA_BOOTSTRAP_SERVERS": env.get("FLINK_KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
        "FLINK_KAFKA_GROUP_ID": env.get("FLINK_KAFKA_GROUP_ID", f"{project}-landing"),
        "FLINK_KAFKA_OPTIONS": _options(kafka_properties),
        "ICEBERG_CATALOG_URI": env.get("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181"),
        "ICEBERG_WAREHOUSE": env.get("ICEBERG_WAREHOUSE", "s3://warehouse/"),
        "ICEBERG_CATALOG_OPTIONS": _options(catalog_properties),
    }


def render(values: dict[str, str] | None = None) -> str:
    values = settings() if values is None else values
    sql = SQL_TEMPLATE.read_text()
    missing = sorted(set(PLACEHOLDER.findall(sql)) - set(values))
    if missing:
        raise KeyError(f"no value for placeholders: {', '.join(missing)}")
    return PLACEHOLDER.sub(lambda m: values[m.group(1)], sql)


def statements(sql: str) -> list[str]:
    """Split a script into statements: each ends with ';' at the end of a line, except inside an
    EXECUTE STATEMENT SET ... END block, which is one statement. Comment lines are dropped."""
    result, buffer = [], []
    for line in sql.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        buffer.append(line)
        in_statement_set = buffer[0].strip().upper().startswith("EXECUTE STATEMENT SET")
        if stripped.endswith(";") and (not in_statement_set or stripped.upper() == "END;"):
            result.append("\n".join(buffer).rstrip().rstrip(";"))
            buffer = []
    if buffer:
        result.append("\n".join(buffer))
    return result


# --- environment preparation -------------------------------------------------------------------

def create_topics() -> None:
    """The Kafka source needs its topics to exist when the job starts (no-op if they do)."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")})
    existing = set(admin.list_topics(timeout=30).topics)
    wanted = [e["topic"] for e in load_pipeline()["entities"] if e["topic"] not in existing]
    if wanted:
        for future in admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in wanted]).values():
            future.result()


def create_local_bucket() -> None:
    """Local stack only (LOCAL_S3_ENDPOINT set): create the warehouse bucket in the S3-compatible storage."""
    endpoint = os.environ.get("LOCAL_S3_ENDPOINT")
    if not endpoint:
        return
    import boto3

    bucket = os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse/").removeprefix("s3://").split("/")[0]
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("LOCAL_S3_ACCESS_KEY", "lakeadmin"),
        aws_secret_access_key=os.environ.get("LOCAL_S3_SECRET_KEY", "lakeadmin-secret"),
        region_name="us-east-1",
    )
    if bucket not in {b["Name"] for b in s3.list_buckets().get("Buckets", [])}:
        s3.create_bucket(Bucket=bucket)


# --- Flink REST / SQL Gateway ------------------------------------------------------------------

def _http(method: str, url: str, body: dict | None = None) -> dict:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            payload = response.read()
            return json.loads(payload) if payload else {}
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"{method} {url} failed ({exc.code}): {exc.read().decode(errors='replace')}") from None


def running_jobs(name: str | None = None) -> list[dict]:
    url = os.environ.get("FLINK_REST_URL", "http://localhost:8081")
    jobs = _http("GET", f"{url}/jobs/overview")["jobs"]
    return [j for j in jobs if j["state"] == "RUNNING" and (name is None or j["name"] == name)]


def execute(sql_statements: list[str], timeout: float = 300) -> None:
    """Run statements in one SQL Gateway session; raise with Flink's error message on failure."""
    gateway = os.environ.get("FLINK_SQL_GATEWAY_URL", "http://localhost:8083")
    session = _http("POST", f"{gateway}/v1/sessions", {"sessionName": job_name()})["sessionHandle"]
    try:
        for statement in sql_statements:
            op = _http("POST", f"{gateway}/v1/sessions/{session}/statements", {"statement": statement})["operationHandle"]
            deadline = time.monotonic() + timeout
            while True:
                status = _http("GET", f"{gateway}/v1/sessions/{session}/operations/{op}/status")["status"]
                if status in ("FINISHED", "ERROR", "CANCELED", "CLOSED", "TIMEOUT"):
                    break
                if time.monotonic() > deadline:
                    raise TimeoutError(f"statement did not finish: {statement[:80]}")
                time.sleep(0.5)
            if status != "FINISHED":
                # Fetching the result surfaces the error message.
                _http("GET", f"{gateway}/v1/sessions/{session}/operations/{op}/result/0")
                raise RuntimeError(f"statement {status}: {statement[:200]}")
    finally:
        try:
            _http("DELETE", f"{gateway}/v1/sessions/{session}")
        except RuntimeError:
            pass


def deploy(replace: bool = False) -> str:
    name = job_name()
    running = running_jobs(name)
    if running and not replace:
        return f"landing job '{name}' already RUNNING ({running[0]['jid']})"
    for job in running:
        cancel_job(job["jid"])
    create_local_bucket()
    create_topics()
    sql = render()
    BUILD.parent.mkdir(parents=True, exist_ok=True)
    BUILD.write_text(sql)
    execute(statements(sql))
    deadline = time.monotonic() + 120
    while not running_jobs(name):
        if time.monotonic() > deadline:
            raise RuntimeError(f"landing job '{name}' did not reach RUNNING; check the Flink UI")
        time.sleep(1)
    return f"landing job '{name}' RUNNING"


def cancel_job(jid: str) -> None:
    url = os.environ.get("FLINK_REST_URL", "http://localhost:8081")
    _http("PATCH", f"{url}/jobs/{jid}?mode=cancel")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["render", "deploy", "status", "cancel"])
    parser.add_argument("--replace", action="store_true", help="deploy: cancel a running job first")
    args = parser.parse_args(argv)

    if args.command == "render":
        BUILD.parent.mkdir(parents=True, exist_ok=True)
        BUILD.write_text(render())
        print(f"rendered {BUILD.relative_to(ROOT)}")
    elif args.command == "deploy":
        print(deploy(replace=args.replace))
    elif args.command == "status":
        jobs = running_jobs(job_name())
        print(f"landing job '{job_name()}': {'RUNNING' if jobs else 'NOT RUNNING'}")
        return 0 if jobs else 1
    else:
        for job in running_jobs(job_name()):
            cancel_job(job["jid"])
            print(f"cancelled {job['jid']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
