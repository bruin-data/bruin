/* @bruin

name: test_truncate_insert_strategy
type: duckdb.sql

materialization:
  type: table
  strategy: truncate+insert

@bruin */

SELECT
    1 AS id,
    'test_data' AS description,
    CURRENT_DATE AS created_date
