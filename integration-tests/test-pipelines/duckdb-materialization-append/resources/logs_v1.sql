/* @bruin
name: test.logs
type: duckdb.sql
materialization:
  type: table
  strategy: append
@bruin */

SELECT 1 AS log_id, 'User login' AS event, '2024-01-01' AS event_date
UNION ALL
SELECT 2 AS log_id, 'Page view' AS event, '2024-01-01' AS event_date
UNION ALL
SELECT 3 AS log_id, 'User logout' AS event, '2024-01-01' AS event_date
