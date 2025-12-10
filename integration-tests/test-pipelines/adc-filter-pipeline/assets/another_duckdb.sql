/* @bruin
name: duckdb_categories
type: duckdb.sql

materialization:
  type: table

tags:
  - duckdb
  - include

columns:
  - name: category_id
    type: INTEGER
    primary_key: true
  - name: category_name
    type: VARCHAR
@bruin */

SELECT 1 AS category_id, 'Electronics' AS category_name
UNION ALL
SELECT 2 AS category_id, 'Clothing' AS category_name

