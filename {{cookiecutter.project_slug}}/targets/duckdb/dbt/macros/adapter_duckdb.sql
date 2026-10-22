{# DuckDB implementation of the adapter interface (dbt/macros/adapter_interface.sql). #}

{% macro duckdb__json_object(expr, key) %}json_extract({{ expr }}, '$.{{ key }}')::varchar{% endmacro %}

{% macro duckdb__json_text(expr, key) %}json_extract_string({{ expr }}, '$.{{ key }}'){% endmacro %}

{% macro duckdb__ts_from_iso(expr) %}cast({{ expr }} as timestamptz){% endmacro %}

{% macro duckdb__ts_from_epoch(expr, scale) %}to_timestamp(cast({{ expr }} as double) / {{ 10 ** scale }}){% endmacro %}
