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

-- Updated events for Jan 2nd with new data
SELECT 6 AS event_id, 'purchase' AS event_name, 101 AS user_id, TIMESTAMP '2024-01-02 10:00:00' AS event_timestamp
UNION ALL
SELECT 7 AS event_id, 'logout' AS event_name, 101 AS user_id, TIMESTAMP '2024-01-02 11:00:00' AS event_timestamp

