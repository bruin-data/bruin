/* @bruin
name: relationship_test.customers
type: duckdb.sql

materialization:
  type: table

columns:
  - name: id
    type: integer
    checks:
      - name: unique

@bruin */

SELECT 1 AS id
UNION ALL
SELECT 2 AS id
UNION ALL
SELECT NULL AS id;
