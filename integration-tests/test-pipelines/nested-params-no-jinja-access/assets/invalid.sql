/* @bruin
name: nested_params_no_jinja_access.invalid
type: duckdb.sql
parameters:
  scalar: hidden
  nested:
    should_fail: "{{ parameters.scalar }}"
@bruin */

SELECT 1 AS id;
