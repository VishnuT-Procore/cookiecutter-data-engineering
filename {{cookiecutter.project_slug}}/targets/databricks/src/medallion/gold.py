"""Gold: run the SQL files in gold/ in name order, substituting ${bronze}, ${silver} and ${gold}."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession


def render(sql: str, schemas: dict[str, str]) -> str:
    for name, qualified in schemas.items():
        sql = sql.replace("${" + name + "}", qualified)
    return sql


def statements(sql: str) -> list[str]:
    """Split a script on ';' and drop chunks that are only whitespace or -- comments."""
    result = []
    for chunk in sql.split(";"):
        code = [line for line in chunk.splitlines() if line.strip() and not line.strip().startswith("--")]
        if code:
            result.append(chunk.strip())
    return result


def run_gold(spark: SparkSession, sql_dir: str | Path, schemas: dict[str, str]) -> list[str]:
    executed = []
    for path in sorted(Path(sql_dir).glob("*.sql")):
        for statement in statements(render(path.read_text(), schemas)):
            spark.sql(statement)
        executed.append(path.name)
    return executed
