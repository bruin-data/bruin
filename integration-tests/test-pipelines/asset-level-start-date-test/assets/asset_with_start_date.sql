/* @bruin

type: duckdb.sql
start_date: "2024-06-01"
connection: duckdb-variables

materialization:
  type: table

@bruin */

SELECT
    '{{ start_date }}' AS captured_start_date,
    '{{ end_date }}' AS captured_end_date,
    'has_asset_start_date' AS asset_type;
