/* @bruin

name: render_this.my_asset_2
type: duckdb.sql

depends:
   - render_this.my_asset

custom_checks:
  - name: verify this resolves in query
    value: 1
    query: SELECT COUNT(*) = 1 AS has_expected_row FROM RENDER_THIS.my_asset WHERE rendered_name = 'render_this.my_asset';

@bruin */

SELECT 1