-- Gold: one row per current customer with order totals (join of two gold tables).
CREATE OR REPLACE TABLE ${gold}.customer_order_summary USING DELTA AS
SELECT
    c.customer_id,
    c.name,
    count(o.order_id) AS order_count,
    coalesce(sum(o.amount), 0) AS total_amount,
    max(o.order_updated_at) AS last_order_at
FROM ${gold}.dim_customers AS c
LEFT JOIN ${gold}.fct_orders AS o
    ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.name
