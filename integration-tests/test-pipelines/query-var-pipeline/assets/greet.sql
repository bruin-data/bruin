/* @bruin
name: greet
type: duckdb.sql

materialization:
  type: table

columns:
  - name: greeting
    type: VARCHAR
    description: "A greeting message"
@bruin */

SELECT '{{ greeting }}' AS greeting;
