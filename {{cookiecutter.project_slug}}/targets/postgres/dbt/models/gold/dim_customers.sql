-- Gold: current customers (soft-deleted rows excluded).
select
    id as customer_id,
    name,
    email,
    country,
    updated_at
from {{ ref('silver_customers') }}
where not _is_deleted
