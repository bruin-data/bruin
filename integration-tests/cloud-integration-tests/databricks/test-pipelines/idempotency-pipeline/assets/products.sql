/* @bruin

name: test.products_idempotency
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
    type: FLOAT
    description: "Price of the product"
    update_on_merge: true
  - name: stock
    type: INTEGER
    description: "Stock count"
    update_on_merge: true

@bruin */

SELECT 1 AS product_id, 'Laptop' AS product_name, 999.99 AS price, 10 AS stock
UNION ALL
SELECT 2 AS product_id, 'Smartphone' AS product_name, 699.99 AS price, 50 AS stock
UNION ALL
SELECT 3 AS product_id, 'Headphones' AS product_name, 199.99 AS price, 100 AS stock

