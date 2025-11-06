/* @bruin
name: test.orders
type: redshift.sql
materialization:
  type: table
  strategy: create+replace
@bruin */

SELECT 1 AS order_id, 'Laptop' AS product_name, 1999.99 AS price
UNION ALL
SELECT 2, 'Mouse', 49.99
UNION ALL
SELECT 3, 'Keyboard', 149.99
