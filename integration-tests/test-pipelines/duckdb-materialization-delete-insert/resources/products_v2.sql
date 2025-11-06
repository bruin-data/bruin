/* @bruin
name: test.products
type: duckdb.sql
materialization:
  type: table
  strategy: delete+insert
  incremental_key: product_id
@bruin */

SELECT 1 AS product_id, 'Laptop Pro' AS product_name, 'Electronics' AS category, 1599.99 AS price
UNION ALL
SELECT 2 AS product_id, 'Mouse' AS product_name, 'Electronics' AS category, 25.99 AS price
UNION ALL
SELECT 4 AS product_id, 'Chair' AS product_name, 'Furniture' AS category, 299.99 AS price
