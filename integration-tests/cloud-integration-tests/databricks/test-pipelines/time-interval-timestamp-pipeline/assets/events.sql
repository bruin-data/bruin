/* @bruin

name: test.events_time_interval
type: databricks.sql

materialization:
    type: table
    strategy: time_interval
    time_granularity: timestamp
    incremental_key: event_timestamp

columns:
  - name: event_id
    type: INTEGER
    description: "Unique identifier for the event"
  - name: event_name
    type: VARCHAR
    description: "Name of the event"
  - name: user_id
    type: INTEGER
    description: "User who triggered the event"
  - name: event_timestamp
    type: TIMESTAMP
    description: "Timestamp when the event occurred"

@bruin */

SELECT 1 AS event_id, 'login' AS event_name, 100 AS user_id, TIMESTAMP '2024-01-01 10:00:00' AS event_timestamp
UNION ALL
SELECT 2 AS event_id, 'purchase' AS event_name, 100 AS user_id, TIMESTAMP '2024-01-01 11:30:00' AS event_timestamp
UNION ALL
SELECT 3 AS event_id, 'logout' AS event_name, 100 AS user_id, TIMESTAMP '2024-01-01 12:00:00' AS event_timestamp
UNION ALL
SELECT 4 AS event_id, 'login' AS event_name, 101 AS user_id, TIMESTAMP '2024-01-02 09:00:00' AS event_timestamp
UNION ALL
SELECT 5 AS event_id, 'view' AS event_name, 101 AS user_id, TIMESTAMP '2024-01-02 09:15:00' AS event_timestamp

