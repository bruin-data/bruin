/* @bruin

name: sp_mart.report
type: duckdb.sql
materialization:
  type: table

@bruin */

SELECT a.src AS a_src, b.src AS b_src, '{{ schema_prefix }}' AS prefix
FROM sp_raw.dev_and_prod a
CROSS JOIN sp_raw.prod_only b
