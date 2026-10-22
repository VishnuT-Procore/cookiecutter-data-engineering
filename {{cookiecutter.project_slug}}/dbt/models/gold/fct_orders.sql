-- Gold: orders joined with their (current) customer.
select
    o.order_id,
    o.customer_id,
    c.name as customer_name,
    c.country as customer_country,
    o.status,
    o.amount,
    o.updated_at as order_updated_at
from {{ ref('silver_orders') }} as o
left join {{ ref('silver_customers') }} as c
    on c.id = o.customer_id
   and not c._is_deleted
where not o._is_deleted
