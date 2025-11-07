/* @bruin
name: test.logs
type: duckdb.sql
materialization:
  type: table
  strategy: append
@bruin */

SELECT 4 AS log_id, 'Button click' AS event, '2024-01-02' AS event_date
UNION ALL
SELECT 5 AS log_id, 'Form submit' AS event, '2024-01-02' AS event_date
