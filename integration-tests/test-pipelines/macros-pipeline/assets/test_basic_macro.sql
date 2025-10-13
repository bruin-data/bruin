/* @bruin
name: test_basic_macro
type: duckdb.sql

materialization:
  type: table

@bruin */

-- Test using a simple macro
{{ simple_select('1 as id, \'test\' as name') }}
