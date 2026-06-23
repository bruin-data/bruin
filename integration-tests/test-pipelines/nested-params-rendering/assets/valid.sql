/* @bruin
name: nested_params_rendering.valid
type: duckdb.sql
parameters:
  plain: "{{ this }}"
  nested:
    asset_name: "{{ this }}"
    window:
      start: "{{ start_date }}"
      end: "{{ end_date }}"
    list:
      - "{{ this }}"
      - 3
      - full_refresh: "{{ full_refresh }}"
        pipeline: "{{ pipeline }}"
@bruin */

SELECT 1 AS id;
