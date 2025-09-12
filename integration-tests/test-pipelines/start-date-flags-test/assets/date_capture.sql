/* @bruin

name: date_capture
type: duckdb.sql
materialization:
  type: table

@bruin */

SELECT 
    '{{ start_date }}' as captured_start_date,
    '{{ end_date }}' as captured_end_date,
    current_timestamp as execution_time;