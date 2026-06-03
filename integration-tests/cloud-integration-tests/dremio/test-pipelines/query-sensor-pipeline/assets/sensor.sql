/* @bruin
name: bruin_test.sensor
type: flight.sensor.query

depends:
  - bruin_test.sensor_table

parameters:
    query: SELECT 1 FROM "bruin_test"."sensor_table" WHERE status = 'ready' LIMIT 1
@bruin */
