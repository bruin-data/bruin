/* @bruin
name: bruin_test.time_interval_events
type: doris.sql

materialization:
  type: table
  strategy: time_interval
  incremental_key: dt
  time_granularity: date
@bruin */

SELECT *
FROM (
    SELECT 2 AS event_id, 'updated-middle' AS event_name, CAST('2024-01-16' AS DATE) AS dt
    UNION ALL
    SELECT 4 AS event_id, 'new-middle' AS event_name, CAST('2024-01-17' AS DATE) AS dt
    UNION ALL
    SELECT 5 AS event_id, 'outside-filtered' AS event_name, CAST('2024-01-25' AS DATE) AS dt
) AS updates
WHERE dt BETWEEN '{{ start_date }}' AND '{{ end_date }}'
