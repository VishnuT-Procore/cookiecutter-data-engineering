import re
import sys

SLUG = "{{ cookiecutter.project_slug }}"
RAW_SCHEMA = "{{ cookiecutter.raw_schema }}"
TARGET = "{{ cookiecutter.target }}"
STREAMING_ENGINE = "{{ cookiecutter.streaming_engine }}"

IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")

# streaming engine -> targets it supports, and what to use elsewhere
ENGINE_SUPPORT = {
    "flink": (
        {"postgres"},
        {
            "snowflake": "there is no supported Flink -> Snowflake sink. For low latency use the Snowflake Kafka "
            "Connector (Snowpipe Streaming) with Dynamic Tables, i.e. streaming_engine=none.",
            "databricks": "Flink's Delta sink is append-only and cannot upsert silver. Spark Structured Streaming "
            "with a short trigger gives seconds-level latency on Databricks, i.e. streaming_engine=none.",
        },
    ),
}


def fail(message: str) -> None:
    print(f"ERROR: {message}")
    sys.exit(1)


for label, value in (("project_slug", SLUG), ("raw_schema", RAW_SCHEMA)):
    if not IDENTIFIER.match(value):
        fail(f"{label} '{value}' must match {IDENTIFIER.pattern} (lowercase letters, digits, underscores).")

if STREAMING_ENGINE in ENGINE_SUPPORT:
    supported, reasons = ENGINE_SUPPORT[STREAMING_ENGINE]
    if TARGET not in supported:
        fail(f"streaming_engine={STREAMING_ENGINE} is not available for target={TARGET}: {reasons[TARGET]}")
