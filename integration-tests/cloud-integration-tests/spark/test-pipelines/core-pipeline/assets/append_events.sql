/* @bruin
name: local.bruin_test.append_events
type: spark.sql

materialization:
  type: table
  strategy: append
@bruin */

SELECT 1 AS event_id, 'one' AS event_name
UNION ALL
SELECT 2 AS event_id, 'two' AS event_name
