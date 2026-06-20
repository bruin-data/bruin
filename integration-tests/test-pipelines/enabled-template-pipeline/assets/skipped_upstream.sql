/* @bruin
name: templated_upstream
type: duckdb.sql
enabled: "{{ var.upstream_enabled }}"
materialization:
  type: table
@bruin */

select * from upstream_should_not_run;
