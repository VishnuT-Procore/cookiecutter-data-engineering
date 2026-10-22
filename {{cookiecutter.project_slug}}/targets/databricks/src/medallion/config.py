"""Pipeline configuration (config/pipeline.yml) as typed objects and Spark schemas."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import yaml
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T

DECIMAL = re.compile(r"^decimal\((\d+),\s*(\d+)\)$")


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    type: str

    @property
    def json_type(self) -> T.DataType:
        """Type used to read the field from the JSON payload."""
        t = self.type.lower().replace(" ", "")
        if m := DECIMAL.match(t):
            return T.DecimalType(int(m.group(1)), int(m.group(2)))
        return {
            "string": T.StringType(),
            "integer": T.IntegerType(),
            "bigint": T.LongType(),
            "double": T.DoubleType(),
            "boolean": T.BooleanType(),
            "date": T.StringType(),
            "timestamp": T.StringType(),
            "timestamp_ms": T.LongType(),
            "timestamp_us": T.LongType(),
        }[t]

    def convert(self, col: Column) -> Column:
        """Convert the JSON-typed value to its final column type."""
        t = self.type.lower()
        if t == "timestamp":
            return F.to_timestamp(col)
        if t == "timestamp_ms":
            return (col / F.lit(1_000)).cast("timestamp")
        if t == "timestamp_us":
            return (col / F.lit(1_000_000)).cast("timestamp")
        if t == "date":
            return F.to_date(col)
        return col


@dataclass(frozen=True)
class Entity:
    name: str
    topic: str
    primary_key: tuple[str, ...]
    columns: tuple[ColumnSpec, ...]

    @property
    def record_schema(self) -> T.StructType:
        return T.StructType([T.StructField(c.name, c.json_type) for c in self.columns])


@dataclass(frozen=True)
class Pipeline:
    project: str
    payload_format: str
    entities: tuple[Entity, ...]

    @property
    def sequence_by(self) -> list[str]:
        """Ordering that decides the latest change per key (most significant first)."""
        return ["_source_ts_ms", "_kafka_offset"] if self.payload_format == "debezium" else ["_kafka_offset"]


def load_pipeline(path: str | Path) -> Pipeline:
    with open(path) as f:
        raw = yaml.safe_load(f)
    entities = tuple(
        Entity(
            name=e["name"],
            topic=e["topic"],
            primary_key=tuple(e["primary_key"]),
            columns=tuple(ColumnSpec(c["name"], str(c["type"])) for c in e["columns"]),
        )
        for e in raw["entities"]
    )
    return Pipeline(project=raw["project"], payload_format=raw["payload_format"], entities=entities)


@dataclass(frozen=True)
class Tables:
    """Resolves <catalog>.<schema>.<table> names; catalog may be empty (local Spark)."""

    catalog: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str

    def _qualify(self, schema: str, table: str = "") -> str:
        parts = [p for p in (self.catalog, schema, table) if p]
        return ".".join(f"`{p}`" for p in parts)

    def bronze(self, entity: Entity) -> str:
        return self._qualify(self.bronze_schema, entity.name)

    def silver(self, entity: Entity) -> str:
        return self._qualify(self.silver_schema, entity.name)

    def schemas(self) -> dict[str, str]:
        return {
            "bronze": self._qualify(self.bronze_schema),
            "silver": self._qualify(self.silver_schema),
            "gold": self._qualify(self.gold_schema),
        }
