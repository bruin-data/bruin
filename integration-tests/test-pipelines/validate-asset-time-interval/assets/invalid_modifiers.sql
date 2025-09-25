/* @bruin
name: invalid_modifiers.example
interval_modifiers:
  start: 24h
  end: -25h
type: duckdb.sql
materialization:
   type: table
@bruin */

SELECT 1 