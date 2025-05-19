/* @bruin

name: render_this.my_asset
type: duckdb.sql
materialization:
  type: table

custom_checks:
  - name: verify this resolves in custom checks
    value: 1
    query: SELECT '{{this}}' = 'render_this.my_asset'

@bruin */

SELECT '{{this}}' AS rendered_name