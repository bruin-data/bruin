/* @bruin

name: public.products_delete_insert
type: rs.sql

materialization:
    type: table
    strategy: delete+insert
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

SELECT 5 AS product_id, 'Tablet' AS product_name, 499.99 AS price, DATE '2024-01-15' AS dt
UNION ALL
SELECT 6 AS product_id, 'Mouse' AS product_name, 29.99 AS price, DATE '2024-01-15' AS dt
