/* @bruin
name: static.invalid
type: duckdb.sql

materialization:
  type: table
  strategy: create+replace
  
interval_modifiers:
  start: "25h"   # Shift start forward 25 hours
  end: "-2h"     # Shift end backward 2 hours

columns:
  - name: id
    type: INTEGER
    description: "Unique identifier"
    primary_key: true
  - name: name
    type: VARCHAR
    description: "Name of the item"
  - name: created_at
    type: TIMESTAMP
    description: "When the item was created"
@bruin */

SELECT 1 AS id, 'Test Item' AS name, TIMESTAMP '2025-01-15 12:00:00' AS created_at;
-- SELECT
--     1 AS id,
--     'Test Item' AS name,
--     TIMESTAMP '2025-01-15 12:00:00' AS created_at
-- WHERE created_at BETWEEN '{{start_timestamp}}' AND '{{end_timestamp}}'
