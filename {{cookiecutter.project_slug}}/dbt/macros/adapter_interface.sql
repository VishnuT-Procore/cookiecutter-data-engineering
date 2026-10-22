{#
  The adapter interface: the only SQL that differs between targets.
  A target plugin implements these as <adapter>__<name> in dbt/macros/adapter_<target>.sql.
  Inputs are SQL expressions; `expr` is always a JSON document stored as text.
#}

{# JSON text of the object/array at top-level `key` of `expr` (NULL if missing). #}
{% macro json_object(expr, key) %}{{ return(adapter.dispatch('json_object')(expr, key)) }}{% endmacro %}

{# Scalar at top-level `key` of `expr` as text, without JSON quotes (NULL if missing or null). #}
{% macro json_text(expr, key) %}{{ return(adapter.dispatch('json_text')(expr, key)) }}{% endmacro %}

{# ISO-8601 text (e.g. 2024-01-01T00:00:00Z) to a timestamp with time zone. #}
{% macro ts_from_iso(expr) %}{{ return(adapter.dispatch('ts_from_iso')(expr)) }}{% endmacro %}

{# Epoch text/number to a timestamp; scale 3 = milliseconds, 6 = microseconds. #}
{% macro ts_from_epoch(expr, scale) %}{{ return(adapter.dispatch('ts_from_epoch')(expr, scale)) }}{% endmacro %}

{# A column of the Iceberg landing table (some engines need quoting for lower-case names). #}
{% macro landing_col(name) %}{{ return(adapter.dispatch('landing_col')(name)) }}{% endmacro %}
{% macro default__landing_col(name) %}{{ name }}{% endmacro %}

{% macro _not_implemented(name) %}
  {{ exceptions.raise_compiler_error(
      "Target '" ~ target.type ~ "' does not implement " ~ name ~ "(); add it to dbt/macros/adapter_" ~ target.type ~ ".sql") }}
{% endmacro %}
{% macro default__json_object(expr, key) %}{{ _not_implemented('json_object') }}{% endmacro %}
{% macro default__json_text(expr, key) %}{{ _not_implemented('json_text') }}{% endmacro %}
{% macro default__ts_from_iso(expr) %}{{ _not_implemented('ts_from_iso') }}{% endmacro %}
{% macro default__ts_from_epoch(expr, scale) %}{{ _not_implemented('ts_from_epoch') }}{% endmacro %}
