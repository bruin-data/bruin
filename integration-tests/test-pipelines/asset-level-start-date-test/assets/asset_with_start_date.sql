/* @bruin

name: asset_with_start_date
type: duckdb.sql
connection: duckdb-variables
start_date: "2024-06-01"
materialization:
  type: table

@bruin */

SELECT
    '{{ start_date }}' as captured_start_date,
    '{{ end_date }}' as captured_end_date,
    'has_asset_start_date' as asset_type;
