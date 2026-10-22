"""Render the template for every target x payload format and check the generated project."""

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
TARGETS = ("postgres", "snowflake", "databricks")
FORMATS = ("debezium", "json")
ORCHESTRATORS = ("dagster", "none")
ENGINE_TARGETS = {"none": TARGETS, "flink": ("postgres",)}

COMMON_FILES = [
    "README.md", "Makefile", "target.mk", "compose.yaml", ".gitignore", ".env.example",
    "requirements.txt", "requirements-dev.txt", "config/pipeline.yml", "docs/target.md",
    "scripts/generate.py", "scripts/plugin_generate.py", "scripts/produce_sample_events.py",
    ".github/workflows/ci.yml",
]
TARGET_FILES = {
    "postgres": [
        "compose.override.yaml", "ingestion/kafka_to_raw.py", "dbt/dbt_project.yml", "dbt/profiles.yml",
        "dbt/models/bronze/_sources.yml", "dbt/models/bronze/bronze_customers.sql",
        "dbt/models/silver/silver_orders.sql", "dbt/models/gold/fct_orders.sql", "tests/test_end_to_end.py",
    ],
    "snowflake": [
        "ingestion/connector.json", "ingestion/setup.sql", "dbt/dbt_project.yml",
        "dbt/models/bronze/bronze_customers.sql", "dbt/models/silver/silver_customers.sql",
        "dbt/models/gold/dim_customer_addresses.sql", "tests/test_project.py",
    ],
    "databricks": [
        "databricks.yml", "pyproject.toml", "src/medallion/bronze.py", "src/medallion/silver.py",
        "gold/03_fct_orders.sql", "tests/test_pipeline.py",
    ],
}
# Files that exist only for one target; they must not appear in other targets' projects.
TARGET_MARKERS = {
    "postgres": ["ingestion/kafka_to_raw.py", "compose.override.yaml"],
    "snowflake": ["ingestion/connector.json", "ingestion/setup.sql"],
    "databricks": ["databricks.yml", "src/medallion/silver.py"],
}
DAGSTER_FILES = [
    "orchestration/__init__.py", "orchestration/definitions.py", "orchestration/pipeline.py",
    "orchestration.mk", "requirements-orchestration.txt", "docs/orchestration.md", "tests/test_orchestration.py",
]
FLINK_FILES = [
    "engine.mk", "compose.flink.yaml", "flink/Dockerfile", "flink/fetch_jars.py", "flink/submit.py",
    "flink/sql/pipeline.sql", "flink/sql/postgres_tables.sql", "docs/streaming.md", "tests/test_flink_end_to_end.py",
]
BATCH_ONLY_POSTGRES = ["ingestion/kafka_to_raw.py", "batch.mk", "tests/test_end_to_end.py"]
UNRENDERED = re.compile(r"\{\{\s*cookiecutter\.|\{%-?\s*(raw|endraw)")


def bake(tmp_path: Path, **context) -> Path:
    return Path(cookiecutter(str(TEMPLATE), no_input=True, output_dir=str(tmp_path), extra_context=context))


def run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)


COMBINATIONS = [
    (target, fmt, engine, orchestrator)
    for engine, targets in ENGINE_TARGETS.items()
    for target in targets
    for fmt in FORMATS
    for orchestrator in ORCHESTRATORS
]


@pytest.mark.parametrize("target, payload_format, streaming_engine, orchestrator", COMBINATIONS)
def test_bake(tmp_path, target, payload_format, streaming_engine, orchestrator):
    project = bake(tmp_path, target=target, payload_format=payload_format,
                   streaming_engine=streaming_engine, orchestrator=orchestrator)

    for folder in ("targets", "engines", "orchestrators"):
        assert not (project / folder).exists(), folder
    expected = list(COMMON_FILES)
    if streaming_engine == "flink":
        expected += FLINK_FILES + [f for f in TARGET_FILES[target] if f not in BATCH_ONLY_POSTGRES]
        for rel in BATCH_ONLY_POSTGRES:
            assert not (project / rel).exists(), f"{rel} should be removed with Flink"
    else:
        expected += TARGET_FILES[target]
        for rel in FLINK_FILES:
            assert not (project / rel).exists(), rel
    for rel in DAGSTER_FILES:
        assert (project / rel).is_file() == (orchestrator == "dagster"), rel
    for rel in expected:
        assert (project / rel).is_file(), rel
    for other in set(TARGETS) - {target}:
        for rel in TARGET_MARKERS[other]:
            assert not (project / rel).exists(), f"{rel} from {other} leaked into {target}"

    pipeline = yaml.safe_load((project / "config" / "pipeline.yml").read_text())
    assert (pipeline["target"], pipeline["payload_format"], pipeline["streaming_engine"]) == (
        target, payload_format, streaming_engine)

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
    assert "check-generated" in result.stdout


def test_generator_tracks_config_changes(tmp_path):
    project = bake(tmp_path, target="postgres")
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
    assert "payment_id" in (project / "dbt/models/silver/silver_payments.sql").read_text()

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
    project = bake(tmp_path, target="snowflake")
    config = project / "config" / "pipeline.yml"
    pipeline = yaml.safe_load(config.read_text())
    mutate(pipeline)
    config.write_text(yaml.safe_dump(pipeline, sort_keys=False))
    result = run([sys.executable, "scripts/generate.py"], project)
    assert result.returncode == 1
    assert message in result.stderr


@pytest.mark.parametrize("target", ["snowflake", "databricks"])
def test_flink_is_rejected_for_unsupported_targets(tmp_path, target, capfd):
    with pytest.raises(FailedHookException):
        bake(tmp_path, target=target, streaming_engine="flink")
    assert "not available for target" in capfd.readouterr().out


def test_invalid_slug_is_rejected(tmp_path):
    with pytest.raises(FailedHookException):
        bake(tmp_path, project_slug="Not-A-Slug")
