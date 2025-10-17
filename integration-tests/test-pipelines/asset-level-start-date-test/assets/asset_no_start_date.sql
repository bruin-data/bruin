/* @bruin

name: asset_no_start_date
type: duckdb.sql
materialization:
  type: table

@bruin */

SELECT
    '{{ start_date }}' as captured_start_date,
    '{{ end_date }}' as captured_end_date,
    'no_asset_start_date' as asset_type;
