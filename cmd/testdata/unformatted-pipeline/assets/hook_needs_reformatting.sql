/* @bruin

name: format_test.hook_asset
type: duckdb.sql

hooks:
  pre:
    - query: " select 1 "
  post:
    - query: |
        SELECT
          2

@bruin */

SELECT 42 AS value;
