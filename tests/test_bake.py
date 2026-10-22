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
UNRENDERED = re.compile(r"\{\{\s*cookiecutter\.|\{%-?\s*(raw|endraw)")


def bake(tmp_path: Path, **context) -> Path:
    return Path(cookiecutter(str(TEMPLATE), no_input=True, output_dir=str(tmp_path), extra_context=context))


def run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)


@pytest.mark.parametrize("payload_format", FORMATS)
@pytest.mark.parametrize("target", TARGETS)
def test_bake(tmp_path, target, payload_format):
    project = bake(tmp_path, target=target, payload_format=payload_format)

    assert not (project / "targets").exists()
    for rel in COMMON_FILES + TARGET_FILES[target]:
        assert (project / rel).is_file(), rel
    for other in set(TARGETS) - {target}:
        for rel in TARGET_MARKERS[other]:
            assert not (project / rel).exists(), f"{rel} from {other} leaked into {target}"

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


def test_invalid_slug_is_rejected(tmp_path):
    with pytest.raises(FailedHookException):
        bake(tmp_path, project_slug="Not-A-Slug")
