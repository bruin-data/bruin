/* @bruin

name: schema_prefix_test.check_prefix_sql
type: duckdb.sql
materialization:
  type: table

@bruin */

SELECT '{{ schema_prefix }}' as prefix
