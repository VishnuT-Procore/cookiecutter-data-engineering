"""Maps dbt models to Dagster asset keys by medallion layer: bronze_customers -> bronze/customers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator


class MedallionTranslator(DagsterDbtTranslator):
    def get_asset_spec(self, manifest: Mapping[str, Any], unique_id: str, project) -> dg.AssetSpec:
        spec = super().get_asset_spec(manifest, unique_id, project)
        node = manifest["nodes"].get(unique_id)
        if node is None or node["resource_type"] != "model":
            return spec  # sources keep <source>/<table>, e.g. kafka_raw/customers
        layer = node["fqn"][1] if len(node["fqn"]) > 2 else "default"
        name = node["name"].removeprefix(f"{layer}_")
        return spec.replace_attributes(key=dg.AssetKey([layer, name]), group_name=layer)
