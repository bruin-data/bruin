/* @bruin

name: test.products_append
type: databricks.sql

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
    type: INTEGER
    description: "Price of the product in cents"

@bruin */

SELECT 1 AS product_id, 'Laptop' AS product_name, 99999 AS price
UNION ALL
SELECT 2 AS product_id, 'Smartphone' AS product_name, 69999 AS price
UNION ALL
SELECT 3 AS product_id, 'Headphones' AS product_name, 19999 AS price

