/* @bruin
name: test.orders
type: redshift.sql
materialization:
  type: table
  strategy: create+replace
@bruin */

SELECT 4 AS order_id, 'Monitor' AS product_name, 599.99 AS price
UNION ALL
SELECT 5, 'Webcam', 129.99
