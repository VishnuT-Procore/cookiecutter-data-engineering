"""Offline defaults so dbt can parse the project without a Snowflake account."""

import os

for var in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_PATH"):
    os.environ.setdefault(var, "offline-check")
