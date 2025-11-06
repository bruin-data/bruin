/* @bruin
name: test.products
type: duckdb.sql
materialization:
  type: table
  strategy: delete+insert
  incremental_key: product_id
@bruin */

SELECT 1 AS product_id, 'Laptop' AS product_name, 'Electronics' AS category, 1999.99 AS price
UNION ALL
SELECT 2 AS product_id, 'Mouse' AS product_name, 'Electronics' AS category, 25.99 AS price
UNION ALL
SELECT 3 AS product_id, 'Desk' AS product_name, 'Furniture' AS category, 499.99 AS price
