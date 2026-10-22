-- Gold: breaks the address attributes of silver_customers out into their own table.
select
    id as customer_id,
    street,
    city,
    country,
    updated_at
from {{ ref('silver_customers') }}
where not _is_deleted
  and coalesce(street, city, country) is not null
