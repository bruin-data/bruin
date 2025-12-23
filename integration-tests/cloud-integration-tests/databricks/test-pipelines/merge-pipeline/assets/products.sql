/* @bruin

name: test.products_merge
type: databricks.sql

materialization:
    type: table
    strategy: merge

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for the product"
    primary_key: true
  - name: product_name
    type: VARCHAR
    description: "Name of the product"
    update_on_merge: true
  - name: price
    type: INTEGER
    description: "Price of the product in cents"
    update_on_merge: false

@bruin */

SELECT 1 AS product_id, 'Laptop' AS product_name, 99900 AS price
UNION ALL
SELECT 2 AS product_id, 'Smartphone' AS product_name, 69900 AS price
UNION ALL
SELECT 3 AS product_id, 'Headphones' AS product_name, 19900 AS price

