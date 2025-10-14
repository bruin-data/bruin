/* @bruin
name: test_aggregation_macro
type: duckdb.sql

materialization:
  type: table

depends:
  - test_basic_macro

@bruin */

-- Test using the aggregation macro
{{ count_by_column('test_basic_macro', 'name') }}
