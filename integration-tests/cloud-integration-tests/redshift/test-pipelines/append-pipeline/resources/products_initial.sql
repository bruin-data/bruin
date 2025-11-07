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

SELECT 1 AS product_id, 'Laptop' AS product_name, 999.99 AS price
UNION ALL
SELECT 2 AS product_id, 'Smartphone' AS product_name, 699.99 AS price
UNION ALL
SELECT 3 AS product_id, 'Headphones' AS product_name, 199.99 AS price
