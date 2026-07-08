/* @bruin
name: bruin_test.append_events
type: doris.sql

materialization:
  type: table
  strategy: append
@bruin */

SELECT 1 AS event_id, 'initial-one' AS event_name
UNION ALL
SELECT 2 AS event_id, 'initial-two' AS event_name
