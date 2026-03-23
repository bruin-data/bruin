/* @bruin

name: asset_with_start_date
type: duckdb.sql
start_date: 2020-01-01
materialization:
  type: table

@bruin */

SELECT
    '{{ start_date }}' as captured_start_date,
    '{{ end_date }}' as captured_end_date;
