-- Gold: breaks the address attributes of silver customers out into their own table.
CREATE OR REPLACE TABLE ${gold}.dim_customer_addresses USING DELTA AS
SELECT
    id AS customer_id,
    street,
    city,
    country,
    updated_at
FROM ${silver}.customers
WHERE NOT _is_deleted
  AND coalesce(street, city, country) IS NOT NULL
