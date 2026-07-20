/* @bruin
name: bruin_test.table_sensor_table
type: starrocks.sql

materialization:
  type: table
  strategy: create+replace
@bruin */

SELECT 1 AS id, 'ready' AS status
