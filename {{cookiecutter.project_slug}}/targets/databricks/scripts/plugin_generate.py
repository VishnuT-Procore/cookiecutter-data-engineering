"""Databricks plugin generator.

The Spark jobs read config/pipeline.yml at runtime (it is synced with the bundle), so there is
nothing to generate; scripts/generate.py still validates the config.
"""

MANAGED_GLOBS: list[str] = []


def render(pipeline: dict) -> dict[str, str]:
    return {}
