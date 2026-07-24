/* @bruin
name: local.bruin_test.table_sensor
type: spark.sensor.table

depends:
  - local.default.table_sensor_table

parameters:
  table: table_sensor_table
  poke_interval: 1
  timeout: 30s
@bruin */
