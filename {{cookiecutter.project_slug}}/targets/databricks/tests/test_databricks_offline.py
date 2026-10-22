"""Offline checks for the Databricks target (no workspace needed)."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_dbt_parse():
    from dbt.cli.main import dbtRunner

    result = dbtRunner().invoke(["parse", "--project-dir", str(ROOT / "dbt"), "--profiles-dir", str(ROOT / "dbt")])
    assert result.success, result.exception
