{#
  Medallion building blocks for Postgres.

  Raw tables (landed by ingestion/kafka_to_raw.py):
    kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, kafka_key (text), payload (jsonb), ingested_at
#}

{% macro json_field(record, name, type) -%}
  {%- set t = type | lower | replace(' ', '') -%}
  {%- set txt = "(" ~ record ~ " ->> '" ~ name ~ "')" -%}
  {%- if t == 'string' -%} {{ txt }}
  {%- elif t == 'timestamp' -%} {{ txt }}::timestamptz
  {%- elif t == 'timestamp_ms' -%} to_timestamp({{ txt }}::numeric / 1000.0)
  {%- elif t == 'timestamp_us' -%} to_timestamp({{ txt }}::numeric / 1000000.0)
  {%- elif t == 'double' -%} {{ txt }}::double precision
  {%- else -%} {{ txt }}::{{ t }}
  {%- endif -%}
{%- endmacro %}


{% macro watermark_filter(column) -%}
  {#- Only read rows newer than what this model already has, minus a lookback for late commits. -#}
  {{ column }} > (
      select coalesce(max(_ingested_at) - interval '{{ var("lookback_minutes", 60) }} minutes', '-infinity'::timestamptz)
      from {{ this }}
  )
{%- endmacro %}


{#
  Bronze: one row per Kafka message (the full change log), typed and enriched with metadata.
  Idempotent: unique_key (_kafka_partition, _kafka_offset) + lookback window.
#}
{% macro bronze_model(source_table, payload_format, columns) %}
with new_records as (
    select * from {{ source('kafka_raw', source_table) }}
    {%- if is_incremental() %}
    where {{ watermark_filter('ingested_at') }}
    {%- endif %}
),

{%- if payload_format == 'debezium' %}

unwrapped as (
    -- Debezium JsonConverter with schemas.enable=true wraps the envelope in {schema, payload}.
    select
        r.*,
        case when payload ? 'schema' and payload ? 'payload' then payload -> 'payload' else payload end as envelope
    from new_records as r
    where payload is not null  -- tombstones only exist for log compaction
),

changes as (
    select
        u.*,
        envelope ->> 'op' as _op,
        case when envelope ->> 'op' = 'd' then envelope -> 'before' else envelope -> 'after' end as record,
        (envelope ->> 'ts_ms')::bigint as _source_ts_ms
    from unwrapped as u
    where envelope ->> 'op' in ('c', 'u', 'r', 'd')
)

{%- else %}

changes as (
    select
        r.*,
        case when payload is null then 'd' else 'u' end as _op,
        -- A tombstone carries no value: recover the primary key from the JSON message key.
        case when payload is null then kafka_key::jsonb else payload end as record,
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
where jsonb_typeof(record) = 'object'
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
),

ranked as (
    select
        *,
        row_number() over (
            partition by {{ primary_key | join(', ') }}
            order by {% for s in sequence_by %}{{ s }} desc nulls last, {% endfor %}_ingested_at desc
        ) as _rn
    from candidates
)

select {{ cols | join(', ') }}
from ranked
where _rn = 1
{% endmacro %}
