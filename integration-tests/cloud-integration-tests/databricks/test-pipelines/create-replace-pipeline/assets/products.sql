/* @bruin

name: test.products_create_replace
type: databricks.sql

materialization:
    type: table
    strategy: create+replace

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for the product"
  - name: product_name
    type: VARCHAR
    description: "Name of the product"
  - name: price
    type: FLOAT
    description: "Price of the product"

@bruin */

SELECT 1 AS product_id, 'Laptop' AS product_name, 999.99 AS price
UNION ALL
SELECT 2 AS product_id, 'Smartphone' AS product_name, 699.99 AS price
UNION ALL
SELECT 3 AS product_id, 'Headphones' AS product_name, 199.99 AS price

