/* @bruin
name: duckdb_products
type: duckdb.sql

materialization:
  type: table

tags:
  - duckdb
  - include

columns:
  - name: id
    type: INTEGER
    primary_key: true
  - name: name
    type: VARCHAR
@bruin */

SELECT 1 AS id, 'Product A' AS name
UNION ALL
SELECT 2 AS id, 'Product B' AS name

