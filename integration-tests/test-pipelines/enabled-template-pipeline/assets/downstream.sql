/* @bruin
name: templated_downstream
type: duckdb.sql
depends:
  - templated_upstream
materialization:
  type: table
@bruin */

select 2 as id;
