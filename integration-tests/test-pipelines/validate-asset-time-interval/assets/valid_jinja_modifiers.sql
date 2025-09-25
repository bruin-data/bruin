/* @bruin
name: jinja.example
interval_modifiers:
  start: '{{ start_timestamp }}'
  end: '{{ end_timestamp }}'
type: duckdb.sql
materialization:
   type: table
@bruin */

SELECT 1 