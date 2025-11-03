/* @bruin

type: duckdb.sql
connection: duckdb-variables

materialization:
  type: table

@bruin */

SELECT
    '{{ start_date }}' AS captured_start_date,
    '{{ end_date }}' AS captured_end_date,
    'no_asset_start_date' AS asset_type;
