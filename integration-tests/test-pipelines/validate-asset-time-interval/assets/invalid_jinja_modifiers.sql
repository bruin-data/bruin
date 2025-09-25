/* @bruin
name: invalid_jinja.example
interval_modifiers:
  start: '{% if start_timestamp[11:13] == "00" %}1d{% else %}-30d{% endif %}'
  end: '{% if end_timestamp[11:13] == "00" %}-1d{% else %}0h{% endif %}'
type: duckdb.sql
materialization:
   type: table
@bruin */

SELECT 1 