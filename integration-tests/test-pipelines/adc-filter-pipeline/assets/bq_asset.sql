/* @bruin
name: bq_summary
type: bq.sql

materialization:
  type: table

tags:
  - bigquery
  - exclude-from-test

depends:
  - duckdb_products

columns:
  - name: total_count
    type: INTEGER
@bruin */

SELECT COUNT(*) AS total_count FROM duckdb_products

