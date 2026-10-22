{# Databricks implementation of the adapter interface (dbt/macros/adapter_interface.sql). #}

{% macro databricks__json_object(expr, key) %}get_json_object({{ expr }}, '$.{{ key }}'){% endmacro %}

{% macro databricks__json_text(expr, key) %}get_json_object({{ expr }}, '$.{{ key }}'){% endmacro %}

{% macro databricks__ts_from_iso(expr) %}to_timestamp({{ expr }}){% endmacro %}

{% macro databricks__ts_from_epoch(expr, scale) -%}
  {%- if scale == 3 -%}timestamp_millis(cast({{ expr }} as bigint))
  {%- else -%}timestamp_micros(cast({{ expr }} as bigint))
  {%- endif -%}
{%- endmacro %}
