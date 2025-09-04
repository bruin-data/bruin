/* @bruin

name: test.start_date_basic
type: duckdb.sql
start_date: "2026-12-01"

materialization:
  type: table

@bruin */

select '{{ start_date }}' as col1