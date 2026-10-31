/* @bruin

name: sp_mart.report_append
type: duckdb.sql
materialization:
  type: table
  strategy: append

@bruin */

SELECT a.src
FROM sp_raw.dev_and_prod a
WHERE NOT EXISTS (SELECT 1 FROM sp_mart.report_append)
