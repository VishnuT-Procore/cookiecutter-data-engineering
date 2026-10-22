"""Render the template for every target x payload format x orchestrator and check the generated project."""

from __future__ import annotations

import compileall
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from cookiecutter.exceptions import FailedHookException
from cookiecutter.main import cookiecutter

TEMPLATE = Path(__file__).resolve().parents[1]
TARGETS = ("duckdb", "databricks", "snowflake")
FORMATS = ("debezium", "json")
ORCHESTRATORS = ("dagster", "none")

COMMON_FILES = [
    "README.md", "Makefile", "target.mk", "compose.yaml", ".gitignore", ".env.example",
    "requirements.txt", "requirements-dev.txt", "config/pipeline.yml", "docs/target.md",
    ".github/workflows/ci.yml",
    "scripts/generate.py", "scripts/pipeline.py", "scripts/produce_sample_events.py",
    "ingestion/landing.py", "ingestion/sql/landing.sql", "ingestion/flink/Dockerfile", "ingestion/flink/fetch_jars.py",
    "dbt/dbt_project.yml", "dbt/profiles.yml", "dbt/macros/adapter_interface.sql", "dbt/macros/medallion.sql",
    "dbt/models/landing/_sources.yml", "dbt/models/bronze/bronze_customers.sql",
    "dbt/models/silver/silver_orders.sql", "dbt/models/gold/fct_orders.sql",
]
TARGET_FILES = {
    "duckdb": ["dbt/macros/adapter_duckdb.sql", "tests/test_end_to_end.py"],
    "databricks": ["dbt/macros/adapter_databricks.sql", "tests/test_databricks_offline.py"],
    "snowflake": ["dbt/macros/adapter_snowflake.sql", "snowflake/setup.sql", "snowflake/landing_tables.sql",
                  "scripts/plugin_generate.py", "tests/test_snowflake_offline.py"],
}
ADAPTER_PACKAGE = {"duckdb": "dbt-duckdb", "databricks": "dbt-databricks", "snowflake": "dbt-snowflake"}
DAGSTER_FILES = [
    "orchestration/__init__.py", "orchestration/definitions.py", "orchestration/dbt_translator.py",
    "orchestration.mk", "docs/orchestration.md", "tests/test_orchestration.py",
]
UNRENDERED = re.compile(r"\{\{\s*cookiecutter\.|\{%-?\s*(raw|endraw)")


def bake(tmp_path: Path, **context) -> Path:
    return Path(cookiecutter(str(TEMPLATE), no_input=True, output_dir=str(tmp_path), extra_context=context))


def run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)


@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
@pytest.mark.parametrize("payload_format", FORMATS)
@pytest.mark.parametrize("target", TARGETS)
def test_bake(tmp_path, target, payload_format, orchestrator):
    project = bake(tmp_path, target=target, payload_format=payload_format, orchestrator=orchestrator)

    for folder in ("targets", "orchestrators"):
        assert not (project / folder).exists(), folder
    for rel in COMMON_FILES + TARGET_FILES[target]:
        assert (project / rel).is_file(), rel
    for other in set(TARGETS) - {target}:
        for rel in TARGET_FILES[other]:
            if rel not in TARGET_FILES[target]:
                assert not (project / rel).exists(), f"{rel} from {other} leaked into {target}"
    for rel in DAGSTER_FILES:
        assert (project / rel).is_file() == (orchestrator == "dagster"), rel

    requirements = (project / "requirements.txt").read_text()
    assert ADAPTER_PACKAGE[target] in requirements
    assert ("dagster-dbt" in requirements) == (orchestrator == "dagster")
    for other in set(TARGETS) - {target}:
        assert ADAPTER_PACKAGE[other] not in requirements

    pipeline = yaml.safe_load((project / "config" / "pipeline.yml").read_text())
    assert (pipeline["target"], pipeline["payload_format"]) == (target, payload_format)

    for path in project.rglob("*"):
        if path.is_file() and "__pycache__" not in path.parts:
            text = path.read_text()
            assert not UNRENDERED.search(text), f"unrendered cookiecutter syntax in {path.relative_to(project)}"
            if path.suffix in (".yml", ".yaml"):
                yaml.safe_load(text)
            elif path.suffix == ".json":
                json.loads(text)

    assert compileall.compile_dir(project, quiet=1, force=True)
    result = run([sys.executable, "scripts/generate.py", "--check"], project)
    assert result.returncode == 0, result.stderr
    result = run(["make", "help"], project)
    assert result.returncode == 0, result.stderr
    assert "ingest-deploy" in result.stdout and "transform" in result.stdout


def test_landing_job_renders_for_any_catalog(tmp_path):
    project = bake(tmp_path, target="databricks")
    spec_env = {
        "ICEBERG_CATALOG_URI": "https://example.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
        "ICEBERG_WAREHOUSE": "main",
        "ICEBERG_CATALOG_PROPERTIES": json.dumps({"token": "t'k", "header.X-Iceberg-Access-Delegation": "vended-credentials"}),
        "FLINK_KAFKA_PROPERTIES": json.dumps({"properties.security.protocol": "SASL_SSL"}),
    }
    code = (
        "import json, os, sys; sys.path.insert(0, 'ingestion'); import landing; "
        "sql = landing.render(landing.settings(json.loads(os.environ['SPEC']))); "
        "print(len(landing.statements(sql))); print(sql)"
    )
    result = subprocess.run([sys.executable, "-c", code], cwd=project, capture_output=True, text=True,
                            env={"SPEC": json.dumps(spec_env), "PATH": ""})
    assert result.returncode == 0, result.stderr
    count, sql = result.stdout.split("\n", 1)
    assert "'uri' = 'https://example.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest'" in sql
    assert "'token' = 't''k'" in sql
    assert "'properties.security.protocol' = 'SASL_SSL'" in sql
    assert not [line for line in sql.splitlines() if "${" in line and not line.startswith("--")]
    # SET x3, CREATE CATALOG, CREATE DATABASE, 2 x (landing table + kafka source), one statement set
    assert int(count) == 3 + 1 + 1 + 2 * 2 + 1


def test_generator_tracks_config_changes(tmp_path):
    project = bake(tmp_path, target="snowflake")
    config = project / "config" / "pipeline.yml"
    original = config.read_text()
    pipeline = yaml.safe_load(original)
    pipeline["entities"].append(
        {"name": "payments", "topic": "app.public.payments", "primary_key": ["payment_id"],
         "columns": [{"name": "payment_id", "type": "bigint"}, {"name": "amount", "type": "decimal(18,2)"}]}
    )
    config.write_text(yaml.safe_dump(pipeline, sort_keys=False))

    assert run([sys.executable, "scripts/generate.py", "--check"], project).returncode == 1
    assert run([sys.executable, "scripts/generate.py"], project).returncode == 0
    assert (project / "dbt/models/silver/silver_payments.sql").is_file()
    assert "lake.landing.payments" in (project / "ingestion/sql/landing.sql").read_text()
    assert "CATALOG_TABLE_NAME = 'payments'" in (project / "snowflake/landing_tables.sql").read_text()

    config.write_text(original)
    assert run([sys.executable, "scripts/generate.py"], project).returncode == 0
    assert not (project / "dbt/models/silver/silver_payments.sql").exists()
    assert run([sys.executable, "scripts/generate.py", "--check"], project).returncode == 0


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda p: p["entities"][0].update(primary_key=["nope"]), "primary_key column 'nope'"),
        (lambda p: p["entities"][0]["columns"].append({"name": "x", "type": "varchar(10)"}), "unsupported type"),
        (lambda p: p["entities"][1].update(topic=p["entities"][0]["topic"]), "is used by another entity"),
        (lambda p: p.update(payload_format="avro"), "payload_format"),
    ],
)
def test_generator_rejects_invalid_config(tmp_path, mutate, message):
    project = bake(tmp_path, target="duckdb")
    config = project / "config" / "pipeline.yml"
    pipeline = yaml.safe_load(config.read_text())
    mutate(pipeline)
    config.write_text(yaml.safe_dump(pipeline, sort_keys=False))
    result = run([sys.executable, "scripts/generate.py"], project)
    assert result.returncode == 1
    assert message in result.stderr


def test_invalid_slug_is_rejected(tmp_path):
    with pytest.raises(FailedHookException):
        bake(tmp_path, project_slug="Not-A-Slug")
