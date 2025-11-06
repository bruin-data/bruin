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

SELECT 10 AS order_id, 'Monitor' AS product_name, 1 AS quantity, 399.99 AS total_amount
UNION ALL
SELECT 20 AS order_id, 'Webcam' AS product_name, 2 AS quantity, 159.98 AS total_amount
