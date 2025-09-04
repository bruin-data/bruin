/* @bruin

name: test.start_date_pipeline_default
type: duckdb.sql

materialization:
  type: table

@bruin */

select '{{ start_date }}' as col1