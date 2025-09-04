/* @bruin

name: test.future_start_date_skip
type: duckdb.sql
start_date: "2030-01-01"

materialization:
  type: table

@bruin */

select 'This should be skipped in full-refresh mode' as message, '{{ start_date }}' as start_date_value