/* @bruin
name: local.bruin_test.query_sensor_table
type: spark.sql

materialization:
  type: table
  strategy: create+replace
@bruin */

SELECT 1 AS id, 'ready' AS status
