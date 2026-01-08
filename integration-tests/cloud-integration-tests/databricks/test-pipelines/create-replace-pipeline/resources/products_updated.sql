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

-- Completely different data - should fully replace
SELECT 10 AS product_id, 'Monitor' AS product_name, 299.99 AS price
UNION ALL
SELECT 11 AS product_id, 'Keyboard' AS product_name, 79.99 AS price

