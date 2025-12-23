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

SELECT 4 AS product_id, 'Monitor' AS product_name, 29999 AS price
UNION ALL
SELECT 5 AS product_id, 'Keyboard' AS product_name, 8999 AS price


