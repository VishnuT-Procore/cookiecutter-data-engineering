-- Gold: orders joined with their (current) customer.
CREATE OR REPLACE TABLE ${gold}.fct_orders USING DELTA AS
SELECT
    o.order_id,
    o.customer_id,
    c.name AS customer_name,
    c.country AS customer_country,
    o.status,
    o.amount,
    o.updated_at AS order_updated_at
FROM ${silver}.orders AS o
LEFT JOIN ${silver}.customers AS c
    ON c.id = o.customer_id
   AND NOT c._is_deleted
WHERE NOT o._is_deleted
