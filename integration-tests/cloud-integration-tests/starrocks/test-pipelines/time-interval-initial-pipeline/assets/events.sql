/* @bruin
name: bruin_test.time_interval_events
type: starrocks.sql

materialization:
  type: table
  strategy: time_interval
  incremental_key: dt
  time_granularity: date
@bruin */

SELECT 1 AS event_id, 'old-before' AS event_name, CAST('2024-01-10' AS DATE) AS dt
UNION ALL
SELECT 2 AS event_id, 'old-middle' AS event_name, CAST('2024-01-16' AS DATE) AS dt
UNION ALL
SELECT 3 AS event_id, 'old-after' AS event_name, CAST('2024-01-20' AS DATE) AS dt
