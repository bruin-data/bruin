/* @bruin
name: local.bruin_test.query_sensor
type: spark.sensor.query

depends:
  - local.bruin_test.query_sensor_table

parameters:
  query: SELECT 1 FROM local.bruin_test.query_sensor_table WHERE status = 'ready' LIMIT 1
  poke_interval: 1
  timeout: 30s
@bruin */
