"""Offline defaults so dbt can parse the project without a Databricks workspace."""

import os

for var, value in {
    "DATABRICKS_HOST": "offline-check.cloud.databricks.com",
    "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/offline",
    "DATABRICKS_TOKEN": "offline-check",
}.items():
    os.environ.setdefault(var, value)
