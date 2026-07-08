/* @bruin
name: bruin_test.sensor_table
type: doris.sql

materialization:
  type: table
  strategy: create+replace
@bruin */

SELECT 1 AS id, 'ready' AS status
