{#
  Medallion building blocks for Snowflake.

  Raw tables are written by the Snowflake Kafka Connector (Snowpipe Streaming):
    RECORD_METADATA variant  (topic, partition, offset, CreateTime, key, SnowflakeConnectorPushTime, ...)
    RECORD_CONTENT  variant  (message value; tombstones arrive as NULL or {})
#}

{% macro json_field(record, name, type) -%}
  {%- set t = type | lower | replace(' ', '') -%}
  {%- set v = "get(" ~ record ~ ", '" ~ name ~ "')" -%}
  {%- if t == 'string' -%} {{ v }}::varchar
  {%- elif t == 'timestamp' -%} try_to_timestamp_tz({{ v }}::varchar)
  {%- elif t == 'timestamp_ms' -%} to_timestamp_tz({{ v }}::number, 3)
  {%- elif t == 'timestamp_us' -%} to_timestamp_tz({{ v }}::number, 6)
  {%- else -%} {{ v }}::{{ t }}
  {%- endif -%}
{%- endmacro %}


{% macro watermark_filter(column) -%}
  {#- Only read rows newer than what this model already has, minus a lookback for late arrivals. -#}
  {{ column }} > (
      select coalesce(dateadd(minute, -{{ var('lookback_minutes', 60) }}, max(_ingested_at)), '1970-01-01'::timestamp_tz)
      from {{ this }}
  )
{%- endmacro %}


{#
  Bronze: one row per Kafka message (the full change log), typed and enriched with metadata.
  Idempotent: merge on (_kafka_partition, _kafka_offset) + lookback window.
#}
{% macro bronze_model(source_table, payload_format, columns) %}
with raw as (
    select
        record_metadata:topic::varchar as kafka_topic,
        record_metadata:partition::integer as kafka_partition,
        record_metadata:offset::bigint as kafka_offset,
        to_timestamp_tz(record_metadata:CreateTime::number, 3) as kafka_timestamp,
        record_metadata:key::varchar as kafka_key,
        iff(record_content is null or (is_object(record_content) and array_size(object_keys(record_content)) = 0),
            null, record_content) as payload,
        coalesce(to_timestamp_tz(record_metadata:SnowflakeConnectorPushTime::number, 3),
                 to_timestamp_tz(record_metadata:CreateTime::number, 3)) as ingested_at
    from {{ source('kafka_raw', source_table) }}
),

new_records as (
    select * from raw
    {%- if is_incremental() %}
    where {{ watermark_filter('ingested_at') }}
    {%- endif %}
    -- the merge needs one source row per (partition, offset)
    qualify row_number() over (partition by kafka_partition, kafka_offset order by ingested_at) = 1
),

{%- if payload_format == 'debezium' %}

unwrapped as (
    -- Debezium JsonConverter with schemas.enable=true wraps the envelope in {schema, payload}.
    select
        r.*,
        iff(payload:schema is not null and payload:payload is not null, payload:payload, payload) as envelope
    from new_records as r
    where payload is not null  -- tombstones only exist for log compaction
),

changes as (
    select
        u.*,
        envelope:op::varchar as _op,
        iff(envelope:op::varchar = 'd', envelope:before, envelope:after) as record,
        envelope:ts_ms::bigint as _source_ts_ms
    from unwrapped as u
    where envelope:op::varchar in ('c', 'u', 'r', 'd')
)

{%- else %}

changes as (
    select
        r.*,
        iff(payload is null, 'd', 'u') as _op,
        -- A tombstone carries no value: recover the primary key from the JSON message key.
        iff(payload is null, try_parse_json(kafka_key), payload) as record,
        null::bigint as _source_ts_ms
    from new_records as r
)

{%- endif %}

select
    {%- for col in columns %}
    {{ json_field('record', col['name'], col['type']) }} as {{ col['name'] }},
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
where is_object(record)
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
      and {{ watermark_filter('_ingested_at') }}
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
)

select {{ cols | join(', ') }}
from candidates
qualify row_number() over (
    partition by {{ primary_key | join(', ') }}
    order by {% for s in sequence_by %}{{ s }} desc nulls last, {% endfor %}_ingested_at desc
) = 1
{% endmacro %}
