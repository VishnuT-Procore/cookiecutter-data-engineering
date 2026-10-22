{# Snowflake implementation of the adapter interface (dbt/macros/adapter_interface.sql). #}

{# json_extract_path_text works on JSON stored as text and returns objects as JSON text. #}
{% macro snowflake__json_object(expr, key) %}json_extract_path_text({{ expr }}, '{{ key }}'){% endmacro %}

{% macro snowflake__json_text(expr, key) %}json_extract_path_text({{ expr }}, '{{ key }}'){% endmacro %}

{% macro snowflake__ts_from_iso(expr) %}try_to_timestamp_tz({{ expr }}){% endmacro %}

{% macro snowflake__ts_from_epoch(expr, scale) %}to_timestamp_tz(cast({{ expr }} as number(38, 0)), {{ scale }}){% endmacro %}

{#
  Columns of externally managed Iceberg tables keep their (lower-case) Iceberg names, so they are quoted.
  If your catalog integration resolves identifiers case-insensitively, set var quote_landing_columns: false.
#}
{% macro snowflake__landing_col(name) %}
  {%- if var('quote_landing_columns', true) -%}"{{ name }}"{%- else -%}{{ name }}{%- endif -%}
{% endmacro %}
