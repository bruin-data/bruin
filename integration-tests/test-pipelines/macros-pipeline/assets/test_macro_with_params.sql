/* @bruin

type: duckdb.sql

materialization:
  type: table

depends:
  - test_basic_macro

@bruin */

-- Test using a macro with parameters
{{ filter_by_date('test_basic_macro', 'id', '1') }}
