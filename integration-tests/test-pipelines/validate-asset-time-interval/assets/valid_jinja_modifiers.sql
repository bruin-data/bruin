/* @bruin
name: valid_jinja.example
interval_modifiers:
  start: '{{ "-1d" }}'
  end: '{{ "1d" }}'
type: duckdb.sql
materialization:
   type: table
@bruin */

SELECT 1 