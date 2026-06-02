/* @bruin
name: bruin_test.events
type: flight.sql

materialization:
  type: table
  strategy: append
@bruin */

SELECT 1 AS event_id, 'login' AS event_name
UNION ALL
SELECT 2 AS event_id, 'click' AS event_name
UNION ALL
SELECT 3 AS event_id, 'logout' AS event_name
