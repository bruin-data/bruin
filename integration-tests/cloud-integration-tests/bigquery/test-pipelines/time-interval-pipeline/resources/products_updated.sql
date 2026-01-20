/* @bruin

name: test.products_time_interval
type: bq.sql

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
    type: INTEGER
    description: "Price of the product in cents"
  - name: dt
    type: DATE
    description: "Date when the product was added"

@bruin */

SELECT 6 AS product_id, 'Tablet' AS product_name, 49999 AS price, DATE '2024-01-15' AS dt
UNION ALL
SELECT 7 AS product_id, 'Mouse' AS product_name, 2999 AS price, DATE '2024-01-16' AS dt
UNION ALL
SELECT 8 AS product_id, 'Webcam' AS product_name, 7999 AS price, DATE '2024-01-18' AS dt

