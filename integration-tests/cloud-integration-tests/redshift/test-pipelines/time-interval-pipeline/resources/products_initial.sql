/* @bruin

name: public.products_time_interval
type: rs.sql

materialization:
    type: table
    strategy: time_interval
    time_granularity: date
    incremental_key: dt

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for the product"
  - name: product_name
    type: VARCHAR
    description: "Name of the product"
  - name: price
    type: FLOAT
    description: "Price of the product in USD"
  - name: dt
    type: DATE
    description: "Date when the product was added"

@bruin */

SELECT 1 AS product_id, 'Laptop' AS product_name, 999.99 AS price, DATE '2024-01-01' AS dt
UNION ALL
SELECT 2 AS product_id, 'Smartphone' AS product_name, 699.99 AS price, DATE '2024-01-02' AS dt
UNION ALL
SELECT 3 AS product_id, 'Headphones' AS product_name, 199.99 AS price, DATE '2024-01-03' AS dt
UNION ALL
SELECT 4 AS product_id, 'Monitor' AS product_name, 299.99 AS price, DATE '2024-01-04' AS dt
UNION ALL
SELECT 5 AS product_id, 'Keyboard' AS product_name, 89.99 AS price, DATE '2024-01-05' AS dt
