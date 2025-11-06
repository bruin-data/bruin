/* @bruin
name: test.orders
type: duckdb.sql

materialization:
  type: table
  strategy: create+replace

columns:
  - name: order_id
    type: INTEGER
    description: "Unique identifier for order"
    primary_key: true
  - name: product_name
    type: VARCHAR
    description: "Name of the product"
  - name: quantity
    type: INTEGER
    description: "Quantity ordered"
  - name: total_amount
    type: DOUBLE
    description: "Total order amount"
@bruin */

SELECT 1 AS order_id, 'Laptop' AS product_name, 2 AS quantity, 1999.98 AS total_amount
UNION ALL
SELECT 2 AS order_id, 'Mouse' AS product_name, 5 AS quantity, 124.95 AS total_amount
UNION ALL
SELECT 3 AS order_id, 'Keyboard' AS product_name, 3 AS quantity, 269.97 AS total_amount
