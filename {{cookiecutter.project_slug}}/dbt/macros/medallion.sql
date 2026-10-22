{#
  Medallion building blocks, target-agnostic: all engine-specific SQL goes through adapter_interface.sql.

  Landing (Iceberg, written by Flink): kafka_topic, kafka_partition, kafka_offset, kafka_timestamp,
                                       kafka_key, kafka_value (raw text), ingested_at, ingest_date
#}

{% macro cast_field(expr, type) -%}
  {%- set t = type | lower | replace(' ', '') -%}
  {%- if t == 'string' -%} {{ expr }}
  {%- elif t == 'timestamp' -%} {{ ts_from_iso(expr) }}
  {%- elif t == 'timestamp_ms' -%} {{ ts_from_epoch(expr, 3) }}
  {%- elif t == 'timestamp_us' -%} {{ ts_from_epoch(expr, 6) }}
  {%- elif t == 'double' -%} cast({{ expr }} as {{ dbt.type_float() }})
  {%- else -%} cast({{ expr }} as {{ t }})
  {%- endif -%}
{%- endmacro %}


{% macro watermark() -%}
  {#- Newest _ingested_at already in this model, minus a lookback for late Iceberg commits. -#}
  select coalesce(
      {{ dbt.dateadd('minute', -1 * var('lookback_minutes', 60), 'max(_ingested_at)') }},
      {{ ts_from_iso("'1970-01-01T00:00:00Z'") }}
  )
  from {{ this }}
{%- endmacro %}


{#
  Bronze: one row per Kafka message (the full change log), typed and enriched with metadata.
  Idempotent: unique_key (_kafka_partition, _kafka_offset) + lookback window.
#}
{% macro bronze_model(source_table, payload_format, columns) %}
with landed as (
    select
        {{ landing_col('kafka_topic') }} as kafka_topic,
        {{ landing_col('kafka_partition') }} as kafka_partition,
        {{ landing_col('kafka_offset') }} as kafka_offset,
        {{ landing_col('kafka_timestamp') }} as kafka_timestamp,
        {{ landing_col('kafka_key') }} as kafka_key,
        {{ landing_col('kafka_value') }} as kafka_value,
        {{ landing_col('ingested_at') }} as ingested_at
    from {{ source('landing', source_table) }}
    {%- if is_incremental() %}
    where {{ landing_col('ingested_at') }} > ({{ watermark() }})
    {%- endif %}
),

deduplicated as (
    -- at-least-once landing may repeat a message; keep one row per (partition, offset)
    select * from (
        select
            landed.*,
            row_number() over (partition by kafka_partition, kafka_offset order by ingested_at) as _copy
        from landed
    ) as numbered
    where _copy = 1
),

{%- if payload_format == 'debezium' %}

unwrapped as (
    -- Debezium JsonConverter with schemas.enable=true wraps the envelope in {schema, payload}.
    select
        d.*,
        case
            when {{ json_object('kafka_value', 'schema') }} is not null
             and {{ json_object('kafka_value', 'payload') }} is not null
            then {{ json_object('kafka_value', 'payload') }}
            else kafka_value
        end as envelope
    from deduplicated as d
    where kafka_value is not null  -- tombstones only exist for log compaction
),

changes as (
    select
        u.*,
        {{ json_text('envelope', 'op') }} as _op,
        case
            when {{ json_text('envelope', 'op') }} = 'd' then {{ json_object('envelope', 'before') }}
            else {{ json_object('envelope', 'after') }}
        end as record,
        cast({{ json_text('envelope', 'ts_ms') }} as bigint) as _source_ts_ms
    from unwrapped as u
    where {{ json_text('envelope', 'op') }} in ('c', 'u', 'r', 'd')
)

{%- else %}

changes as (
    select
        d.*,
        case when kafka_value is null then 'd' else 'u' end as _op,
        -- a tombstone carries no value: the primary key comes from the JSON message key
        coalesce(kafka_value, kafka_key) as record,
        cast(null as bigint) as _source_ts_ms
    from deduplicated as d
)

{%- endif %}

select
    {%- for col in columns %}
    {{ cast_field(json_text('record', col['name']), col['type']) }} as {{ col['name'] }},
    {%- endfor %}
    _op,
    _op = 'd' as _is_deleted,
    _source_ts_ms,
    kafka_topic as _kafka_topic,
    kafka_partition as _kafka_partition,
    kafka_offset as _kafka_offset,
    kafka_timestamp as _kafka_timestamp,
    ingested_at as _ingested_at
from changes
where record is not null
{% endmacro %}


{#
  Silver: the latest change per primary key.
  New bronze rows are unioned with the current silver rows for the same keys and ranked again,
  so out-of-order or replayed events never overwrite a newer version.
#}
{% macro silver_model(bronze_relation, columns, primary_key, sequence_by) %}
{%- set metadata = ['_op', '_is_deleted', '_source_ts_ms', '_kafka_topic', '_kafka_partition',
                    '_kafka_offset', '_kafka_timestamp', '_ingested_at'] -%}
{%- set cols = (columns | map(attribute='name') | list) + metadata -%}
{%- set existing = [] -%}
{%- if is_incremental() and execute -%}
  {%- for c in adapter.get_columns_in_relation(this) -%}{%- do existing.append(c.name | lower) -%}{%- endfor -%}
{%- endif %}
with changes as (
    select {{ cols | join(', ') }}
    from {{ bronze_relation }}
    where {% for k in primary_key %}{{ k }} is not null{% if not loop.last %} and {% endif %}{% endfor %}
    {%- if is_incremental() %}
      and _ingested_at > ({{ watermark() }})
    {%- endif %}
),

candidates as (
    select * from changes
    {%- if is_incremental() %}
    union all
    select
        {%- for c in cols %}
        {% if c | lower in existing %}cur.{{ c }}{% else %}null{% endif %} as {{ c }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from {{ this }} as cur
    where exists (
        select 1 from changes as c
        where {% for k in primary_key %}c.{{ k }} = cur.{{ k }}{% if not loop.last %} and {% endif %}{% endfor %}
    )
    {%- endif %}
),

ranked as (
    select
        candidates.*,
        row_number() over (
            partition by {{ primary_key | join(', ') }}
            order by {% for s in sequence_by %}{{ s }} desc nulls last, {% endfor %}_ingested_at desc
        ) as _rank
    from candidates
)

select {{ cols | join(', ') }}
from ranked
where _rank = 1
{% endmacro %}
