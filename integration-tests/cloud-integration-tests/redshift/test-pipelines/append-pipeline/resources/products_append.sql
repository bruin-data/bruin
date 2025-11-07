/* @bruin

name: public.products_append
type: rs.sql

materialization:
    type: table
    strategy: append

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

@bruin */

SELECT 4 AS product_id, 'Monitor' AS product_name, 299.99 AS price
UNION ALL
SELECT 5 AS product_id, 'Keyboard' AS product_name, 89.99 AS price
