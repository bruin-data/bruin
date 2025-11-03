/* @bruin

name: invalid_modifiers.example
type: duckdb.sql

materialization:
  type: table
interval_modifiers:
  start: 1d
  end: -1d

@bruin */

SELECT 1
