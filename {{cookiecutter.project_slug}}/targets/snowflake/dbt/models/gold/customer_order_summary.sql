-- Gold: one row per current customer with order totals (join of two gold models).
select
    c.customer_id,
    c.name,
    count(o.order_id) as order_count,
    coalesce(sum(o.amount), 0) as total_amount,
    max(o.order_updated_at) as last_order_at
from {{ ref('dim_customers') }} as c
left join {{ ref('fct_orders') }} as o
    on o.customer_id = c.customer_id
group by c.customer_id, c.name
