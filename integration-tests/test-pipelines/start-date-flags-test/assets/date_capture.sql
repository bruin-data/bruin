/* @bruin

type: duckdb.sql

materialization:
  type: table

@bruin */

SELECT
    '{{ start_date }}' AS captured_start_date,
    '{{ end_date }}' AS captured_end_date,
    current_timestamp AS execution_time;
