-- Gold: current customers (soft-deleted rows excluded).
CREATE OR REPLACE TABLE ${gold}.dim_customers USING DELTA AS
SELECT
    id AS customer_id,
    name,
    email,
    country,
    updated_at
FROM ${silver}.customers
WHERE NOT _is_deleted
