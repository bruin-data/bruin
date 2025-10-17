/* @bruin

name: asset_invalid_start_date
type: duckdb.sql
start_date: "invalid-date-format"
materialization:
  type: table

@bruin */

SELECT
    '{{ start_date }}' as captured_start_date,
    '{{ end_date }}' as captured_end_date,
    'invalid_asset_start_date' as asset_type;
